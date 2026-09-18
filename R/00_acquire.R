# =============================================================================
# 00_acquire.R - the one downloader (issue #29 item 3)
#
#     source("R/00_paths.R"); source("R/00_acquire.R")
#     atlas_require("mapspam-2020v1r2")     # fetch only if not already here
#     atlas_acquire("faostat-bulk", force = TRUE)
#
# Two rules this file exists to enforce.
#
#   1. ONE downloader per dataset, shared. The recipe lives in the dataset's
#      catalogue record (metadata/catalogue/<id>.json, "acquire" block) and the
#      implementation lives here, once per METHOD. Nothing hand-rolls its own
#      download loop any more. Where a dataset is genuinely acquired by an
#      existing script - the python ingests, the hazards_upstream downloaders -
#      method "script" REGISTERS that command rather than reimplementing it, so
#      the catalogue still names the single reproducible way to recreate it.
#
#   2. ON DEMAND, not by default. Nothing is fetched because setup ran. A
#      dataset is fetched because something asked for it. atlas_require() is the
#      normal entry point and is a no-op when the data is already present.
#
# RECIPE vs RECEIPT - the distinction the whole design turns on:
#
#   * The RECIPE is in git, in the catalogue record. It is how to get the data,
#     it is versioned, and it is the same on every host.
#   * The RECEIPT is written next to the data, at
#     <working_dir>/Data/_acquisition/<id>.jsonl. It is what actually happened
#     ON THIS HOST: when, from where, how many files, how many bytes, by whom.
#     It is append-only and never travels with the repo, because it describes
#     one machine's copy.
#
#   That is what makes the data recreatable: the recipe says what to run, the
#   receipt says what you already ran and when it drifted from the recipe.
# =============================================================================

if (!isTRUE(getOption("atlas.acquire_loaded"))) {

  if (!isTRUE(getOption("atlas.paths_loaded"))) {
    stop("source R/00_paths.R before R/00_acquire.R")
  }

  `%||%` <- function(a, b) if (is.null(a) || length(a) == 0L) b else a

  .acq_t0 <- new.env(parent = emptyenv())
  .acq_log <- function(..., level = "INFO") {
    now <- Sys.time()
    if (is.null(.acq_t0$t0)) .acq_t0$t0 <- now
    cat(sprintf("[%s] +%6.1fs %-5s | %s\n",
                format(now, "%Y-%m-%d %H:%M:%S"),
                as.numeric(difftime(now, .acq_t0$t0, units = "secs")),
                level, paste0(..., collapse = "")))
    flush.console()
  }

  # ---- catalogue access -----------------------------------------------------
  atlas_catalogue <- function(id = NULL) {
    d <- file.path(atlas_repo_root(), "metadata", "catalogue")
    if (is.null(id)) {
      fs <- list.files(d, pattern = "^[^_].*\\.json$", full.names = TRUE)
      out <- lapply(fs, jsonlite::fromJSON, simplifyVector = FALSE)
      names(out) <- vapply(out, function(r) r$id, character(1))
      return(out)
    }
    f <- file.path(d, paste0(id, ".json"))
    if (!file.exists(f)) stop("no catalogue record for '", id, "' (expected ", f, ")")
    jsonlite::fromJSON(f, simplifyVector = FALSE)
  }

  # Where a dataset lives on THIS host.
  atlas_dataset_dir <- function(rec) {
    loc <- rec$acquire$target %||% rec$location
    base <- if (!is.null(loc$key)) {
      atlas_dir(loc$key, absolute = TRUE)
    } else if (!is.null(loc$path)) {
      file.path(atlas_working_dir(), loc$path)
    } else {
      stop("dataset '", rec$id, "' has no resolvable location")
    }
    if (!is.null(loc$subpath)) base <- file.path(base, loc$subpath)
    base
  }

  atlas_dataset_present <- function(rec) {
    d <- atlas_dataset_dir(rec)
    if (!dir.exists(d)) return(FALSE)
    loc <- rec$location
    n <- length(list.files(d, pattern = utils::glob2rx(loc$glob %||% "*"),
                           recursive = isTRUE(loc$recursive)))
    n >= (rec$completeness$min_files %||% 1L)
  }

  # ---- receipts -------------------------------------------------------------
  .acq_receipt_path <- function(id) {
    file.path(atlas_working_dir(), "Data", "_acquisition", paste0(id, ".jsonl"))
  }

  .acq_write_receipt <- function(id, entry) {
    p <- .acq_receipt_path(id)
    dir.create(dirname(p), recursive = TRUE, showWarnings = FALSE)
    entry$timestamp <- format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
    entry$host <- atlas_host_id()
    entry$user <- Sys.info()[["user"]] %||% NA_character_
    entry$dataset <- id
    cat(jsonlite::toJSON(entry, auto_unbox = TRUE, null = "null"), "\n",
        file = p, append = TRUE, sep = "")
    invisible(p)
  }

  #' What this host has actually done to acquire a dataset.
  atlas_acquire_status <- function(id) {
    p <- .acq_receipt_path(id)
    if (!file.exists(p)) {
      return(data.frame(dataset = id, timestamp = NA, files = NA, bytes = NA,
                        status = "no receipt", stringsAsFactors = FALSE))
    }
    lines <- readLines(p, warn = FALSE)
    lines <- lines[nzchar(lines)]
    do.call(rbind, lapply(lines, function(l) {
      e <- jsonlite::fromJSON(l)
      data.frame(dataset = e$dataset %||% id,
                 timestamp = e$timestamp %||% NA,
                 method = e$method %||% NA,
                 files = e$files %||% NA,
                 bytes = e$bytes %||% NA,
                 status = e$status %||% NA,
                 stringsAsFactors = FALSE)
    }))
  }

  # ---- s3 handle (anonymous; the Atlas bucket is public for read) -----------
  .acq_s3 <- function() {
    h <- getOption("atlas.s3_anon")
    if (is.null(h)) {
      h <- s3fs::S3FileSystem$new(anonymous = TRUE)
      options(atlas.s3_anon = h)
    }
    h
  }

  # ---- one implementation per METHOD ---------------------------------------
  # Each returns a data.frame(src, dest, bytes, action) describing what it did
  # (or, under dry_run, what it would do).

  .acq_download_one <- function(src, dest, overwrite, dry_run, downloader) {
    if (file.exists(dest) && !overwrite) {
      return(data.frame(src = src, dest = dest, bytes = file.size(dest), action = "skip"))
    }
    if (dry_run) {
      return(data.frame(src = src, dest = dest, bytes = NA_real_, action = "would-fetch"))
    }
    dir.create(dirname(dest), recursive = TRUE, showWarnings = FALSE)
    downloader(src, dest)
    data.frame(src = src, dest = dest,
               bytes = if (file.exists(dest)) file.size(dest) else NA_real_,
               action = "fetch")
  }

  .acq_s3_get <- function(src, dest) .acq_s3()$file_download(src, dest, overwrite = TRUE)
  .acq_http_get <- function(src, dest) utils::download.file(src, dest, mode = "wb", quiet = TRUE)

  .acq_method <- list(

    # Mirror an S3 prefix into the target directory.
    "s3-dir" = function(rec, target, overwrite, dry_run) {
      a <- rec$acquire
      keys <- .acq_s3()$dir_ls(a$source, recurse = isTRUE(a$recurse))
      if (!is.null(a$pattern)) keys <- keys[grepl(a$pattern, keys)]
      if (!is.null(a$exclude)) keys <- keys[!grepl(a$exclude, keys)]
      if (!length(keys)) stop("no objects under ", a$source)
      dest <- if (isTRUE(a$flatten)) {
        file.path(target, basename(keys))
      } else {
        file.path(target, sub(paste0("^", a$source), "", keys))
      }
      do.call(rbind, Map(function(s, d) {
        .acq_download_one(s, d, overwrite, dry_run, .acq_s3_get)
      }, keys, dest))
    },

    # Expand a glue-style path_pattern over enumerated params.
    "s3-glue" = function(rec, target, overwrite, dry_run) {
      a <- rec$acquire
      grid <- expand.grid(lapply(a$params, unlist), stringsAsFactors = FALSE)
      keys <- vapply(seq_len(nrow(grid)), function(i) {
        s <- a$source
        for (nm in names(grid)) s <- gsub(paste0("\\{", nm, "\\}"), grid[[nm]][i], s)
        s
      }, character(1))
      do.call(rbind, Map(function(s, d) {
        .acq_download_one(s, d, overwrite, dry_run, .acq_s3_get)
      }, keys, file.path(target, basename(keys))))
    },

    "s3-file" = function(rec, target, overwrite, dry_run) {
      a <- rec$acquire
      do.call(rbind, lapply(unlist(a$files), function(k) {
        .acq_download_one(k, file.path(target, basename(k)), overwrite, dry_run, .acq_s3_get)
      }))
    },

    "http-file" = function(rec, target, overwrite, dry_run) {
      a <- rec$acquire
      do.call(rbind, lapply(a$files, function(f) {
        .acq_download_one(f$url, file.path(target, f$dest %||% basename(f$url)),
                          overwrite, dry_run, .acq_http_get)
      }))
    },

    # Download a zip, unpack it, drop the archive. Presence is judged on the
    # EXPECTED member, not on the zip, so a half-unpacked tree re-fetches.
    "http-zip" = function(rec, target, overwrite, dry_run) {
      a <- rec$acquire
      do.call(rbind, lapply(a$files, function(f) {
        expect <- file.path(target, f$expect)
        if (file.exists(expect) && !overwrite) {
          return(data.frame(src = f$url, dest = expect, bytes = file.size(expect), action = "skip"))
        }
        if (dry_run) {
          return(data.frame(src = f$url, dest = expect, bytes = NA_real_, action = "would-fetch"))
        }
        dir.create(target, recursive = TRUE, showWarnings = FALSE)
        zp <- file.path(target, basename(f$url))
        utils::download.file(f$url, zp, mode = "wb", quiet = TRUE)
        utils::unzip(zp, exdir = target)
        unlink(zp)
        data.frame(src = f$url, dest = expect,
                   bytes = if (file.exists(expect)) file.size(expect) else NA_real_,
                   action = "fetch")
      }))
    },

    # Scrape an index page for links matching a pattern.
    "http-scrape" = function(rec, target, overwrite, dry_run) {
      a <- rec$acquire
      if (!requireNamespace("rvest", quietly = TRUE)) stop("method http-scrape needs 'rvest'")
      links <- rvest::html_attr(rvest::html_nodes(rvest::read_html(a$source), "a"), "href")
      links <- grep(a$pattern, links, value = TRUE)
      if (!length(links)) stop("no links matching ", a$pattern, " at ", a$source)
      urls <- file.path(a$source, links)
      do.call(rbind, Map(function(s, d) {
        .acq_download_one(s, d, overwrite, dry_run, .acq_http_get)
      }, urls, file.path(target, basename(links))))
    },

    # The dataset is acquired by an existing script. We do NOT reimplement it -
    # we register it, so the catalogue still names the one reproducible command,
    # and the receipt records that it was run.
    "script" = function(rec, target, overwrite, dry_run) {
      a <- rec$acquire
      cmd <- a$command
      if (dry_run || !isTRUE(a$runnable)) {
        .acq_log("dataset '", rec$id, "' is acquired by a registered script.")
        .acq_log("  run: ", cmd)
        return(data.frame(src = cmd, dest = target, bytes = NA_real_,
                          action = if (dry_run) "would-run" else "manual"))
      }
      .acq_log("running registered acquisition command: ", cmd)
      st <- system(cmd)
      data.frame(src = cmd, dest = target, bytes = NA_real_,
                 action = if (st == 0L) "ran" else paste0("failed(", st, ")"))
    }
  )

  # ---- the entry points -----------------------------------------------------

  #' Acquire a dataset using its registered recipe, and write a receipt.
  atlas_acquire <- function(id, force = FALSE, dry_run = FALSE) {
    rec <- atlas_catalogue(id)
    a <- rec$acquire
    if (is.null(a) || is.null(a$method)) {
      stop("dataset '", id, "' has no acquire block in its catalogue record.\n",
           "  Nothing can fetch it automatically. See the record's `gaps`.")
    }
    fn <- .acq_method[[a$method]]
    if (is.null(fn)) {
      stop("unknown acquire method '", a$method, "' for '", id, "'. Known: ",
           paste(names(.acq_method), collapse = ", "))
    }
    target <- atlas_dataset_dir(rec)
    .acq_log("acquire '", id, "' method=", a$method, " -> ", target,
             if (dry_run) "  [DRY RUN]" else "")

    res <- tryCatch(fn(rec, target, overwrite = isTRUE(force), dry_run = isTRUE(dry_run)),
                    error = function(e) {
                      .acq_write_receipt(id, list(method = a$method, status = "error",
                                                  error = conditionMessage(e),
                                                  source = a$source %||% a$command %||% NA))
                      stop("acquire '", id, "' failed: ", conditionMessage(e), call. = FALSE)
                    })

    n_fetch <- sum(res$action %in% c("fetch", "ran"))
    n_skip <- sum(res$action == "skip")
    bytes <- sum(res$bytes, na.rm = TRUE)
    .acq_log("  ", nrow(res), " object(s): ", n_fetch, " fetched, ", n_skip,
             " already present, ", format(bytes, big.mark = ","), " bytes")

    if (!dry_run) {
      .acq_write_receipt(id, list(
        method = a$method,
        source = a$source %||% a$command %||% NA,
        version = rec$version %||% NA,
        files = nrow(res), fetched = n_fetch, skipped = n_skip,
        bytes = bytes, target = target,
        status = if (n_fetch + n_skip == nrow(res)) "ok" else "partial"
      ))
    }
    invisible(res)
  }

  #' Ensure a dataset is present, acquiring it only if it is not.
  #' This is the on-demand entry point - the one stage scripts should call.
  atlas_require <- function(id, force = FALSE, quiet = FALSE) {
    rec <- atlas_catalogue(id)
    if (!force && atlas_dataset_present(rec)) {
      if (!quiet) .acq_log("'", id, "' already present at ", atlas_dataset_dir(rec))
      return(invisible(atlas_dataset_dir(rec)))
    }
    atlas_acquire(id, force = force)
    invisible(atlas_dataset_dir(rec))
  }

  #' Acquire everything a pipeline stage needs, and nothing else.
  atlas_require_stage <- function(stage_id, force = FALSE, dry_run = FALSE) {
    recs <- atlas_catalogue()
    need <- Filter(function(r) stage_id %in% unlist(r$consumed_by), recs)
    if (!length(need)) {
      .acq_log("no catalogue record declares stage '", stage_id, "' as a consumer")
      return(invisible(character(0)))
    }
    .acq_log("stage '", stage_id, "' needs ", length(need), " dataset(s): ",
             paste(names(need), collapse = ", "))
    for (r in need) {
      if (is.null(r$acquire)) {
        .acq_log("  SKIP '", r$id, "' - no acquire recipe (see gaps)", level = "WARN")
        next
      }
      if (dry_run) atlas_acquire(r$id, dry_run = TRUE) else atlas_require(r$id, force = force)
    }
    invisible(names(need))
  }

  options(atlas.acquire_loaded = TRUE)
}
