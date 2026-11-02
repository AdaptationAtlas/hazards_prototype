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
#
# MANIFEST - what the origin holds, pinned in git (issue #58 item 1):
#
#   * metadata/catalogue/_manifests/<id>.tsv, named by the record's
#     origin.checksum_manifest, lists every object: src, dest (relative to the
#     target dir), bytes, S3 ETag and/or md5. Built from listings and HEADs by
#     R/checks/75_catalogue_manifest.R - no data downloaded.
#   * Where one exists, atlas_acquire() fetches exactly the listed objects,
#     re-fetches any local copy of the wrong size, and hashes every file it
#     downloads; atlas_require() counts a dataset present only when every listed
#     object is here at the right size; atlas_verify() audits a host's copy.
#
# S3 objects are read over anonymous HTTPS with base R, not through an S3
# client: the bucket is public-read and PASCAL has no s3fs/paws/boto3.
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
  # option atlas.catalogue_dir exists for fixtures; nothing else should set it.
  .acq_catalogue_dir <- function() {
    getOption("atlas.catalogue_dir", file.path(atlas_repo_root(), "metadata", "catalogue"))
  }

  atlas_catalogue <- function(id = NULL) {
    d <- .acq_catalogue_dir()
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
    files <- .acq_manifest_files(rec)
    if (NROW(files)) {
      # Pinned: present means every object the origin holds is here at the
      # right size. A file count alone let 36 of 48 MapSPAM tiles pass on PASCAL.
      st <- vapply(seq_len(nrow(files)), function(i) {
        .acq_check(file.path(d, files$dest[i]), files[i, ], hash = FALSE)
      }, character(1))
      return(all(st == "ok-size"))
    }
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

  # ---- manifests ------------------------------------------------------------
  # origin.checksum_manifest is either a URL of a published checksum list (only
  # nexgddp-cmip6-raw, consumed by its own downloader) or a repo-relative path
  # to a manifest TSV. Only the second is read here.
  atlas_manifest_path <- function(rec) {
    m <- rec$origin$checksum_manifest
    if (is.null(m) || !nzchar(m) || grepl("^[a-z][a-z0-9+.-]*://", m)) return(NA_character_)
    cands <- if (grepl("^(/|[A-Za-z]:)", m)) m else {
      c(file.path(atlas_repo_root(), m), file.path(.acq_catalogue_dir(), m))
    }
    hit <- cands[file.exists(cands)]
    if (!length(hit)) stop("dataset '", rec$id, "' names checksum_manifest '", m,
                           "' but no such file exists")
    hit[1]
  }

  atlas_manifest <- function(rec) {
    if (is.character(rec)) rec <- atlas_catalogue(rec)
    p <- atlas_manifest_path(rec)
    if (is.na(p)) return(NULL)
    m <- utils::read.delim(p, colClasses = "character", na.strings = c("", "NA"),
                           quote = "", comment.char = "")
    miss <- setdiff(c("src", "dest", "bytes", "etag", "md5", "role"), names(m))
    if (length(miss)) stop("manifest ", p, " lacks column(s): ", paste(miss, collapse = ", "))
    m$bytes <- as.numeric(m$bytes)
    m
  }

  # The rows that land on disk as themselves (archives are unpacked and dropped).
  .acq_manifest_files <- function(rec) {
    m <- atlas_manifest(rec)
    if (is.null(m)) return(NULL)
    m[m$role == "file", , drop = FALSE]
  }

  # ---- hashes (base R only: tools::md5sum) ----------------------------------
  .acq_md5 <- function(path) unname(tools::md5sum(path))

  # S3 multipart ETag = md5 of the concatenated binary md5s of each part, then
  # "-<parts>". The part size is not recorded, so try the common client
  # defaults that give exactly <parts> parts for this size. NA = could not tell.
  .acq_multipart_etag <- function(path, part_size) {
    con <- file(path, "rb")
    tmp <- tempfile()
    on.exit({ close(con); unlink(tmp) })
    digests <- raw(0)
    n <- 0L
    repeat {
      b <- readBin(con, "raw", n = part_size)
      if (!length(b)) break
      writeBin(b, tmp)
      h <- .acq_md5(tmp)
      digests <- c(digests, as.raw(strtoi(substring(h, seq(1, 31, 2), seq(2, 32, 2)), 16L)))
      n <- n + 1L
    }
    writeBin(digests, tmp)
    paste0(.acq_md5(tmp), "-", n)
  }

  .acq_s3_etag_matches <- function(path, etag) {
    etag <- gsub('"', "", etag, fixed = TRUE)
    if (!grepl("-", etag, fixed = TRUE)) return(identical(.acq_md5(path), etag))
    n <- as.integer(sub(".*-", "", etag))
    mib <- 1024^2
    cands <- c(8, 16, 5, 10, 15, 32, 50, 64, 100, 128, 256, 512) * mib
    cands <- cands[ceiling(file.size(path) / cands) == n]
    if (!length(cands)) return(NA)
    for (ps in cands) if (identical(.acq_multipart_etag(path, ps), etag)) return(TRUE)
    FALSE
  }

  # One local file against one manifest row: "ok" (hash matched), "ok-size"
  # (only size could be or was checked), "missing", "size" or "hash".
  .acq_check <- function(path, row, hash = TRUE) {
    if (!file.exists(path)) return("missing")
    if (!is.na(row$bytes) && file.size(path) != row$bytes) return("size")
    if (!hash) return("ok-size")
    if (!is.na(row$md5)) return(if (identical(.acq_md5(path), row$md5)) "ok" else "hash")
    if (!is.na(row$etag) && startsWith(row$src, "s3://")) {
      m <- .acq_s3_etag_matches(path, row$etag)
      if (is.na(m)) return("ok-size")
      return(if (m) "ok" else "hash")
    }
    "ok-size"
  }

  # ---- transport: anonymous HTTPS for S3 and HTTP alike ---------------------
  # option atlas.s3_endpoint (sprintf template on the bucket) exists for fixtures.
  .acq_s3_split <- function(uri) {
    rest <- sub("^s3://", "", uri)
    list(bucket = sub("/.*$", "", rest), key = sub("^[^/]*/?", "", rest))
  }
  .acq_s3_endpoint <- function(bucket) {
    sprintf(getOption("atlas.s3_endpoint", "https://%s.s3.amazonaws.com"), bucket)
  }
  .acq_s3_https <- function(uri) {
    s <- .acq_s3_split(uri)
    paste0(.acq_s3_endpoint(s$bucket), "/", utils::URLencode(s$key, reserved = FALSE))
  }

  .acq_http_get <- function(src, dest) {
    # R's default 60 s timeout kills a 48 MB tile at PASCAL's ~0.5 MB/s.
    old <- options(timeout = max(3600, getOption("timeout")))
    on.exit(options(old))
    st <- utils::download.file(src, dest, mode = "wb", quiet = TRUE)
    if (st != 0L) stop("download failed (status ", st, "): ", src)
  }
  .acq_get <- function(src, dest) {
    .acq_http_get(if (startsWith(src, "s3://")) .acq_s3_https(src) else src, dest)
  }

  .acq_xml_unescape <- function(x) {
    x <- gsub("&quot;", '"', x, fixed = TRUE)
    x <- gsub("&apos;", "'", x, fixed = TRUE)
    x <- gsub("&lt;", "<", x, fixed = TRUE)
    x <- gsub("&gt;", ">", x, fixed = TRUE)
    gsub("&amp;", "&", x, fixed = TRUE)
  }

  # One ListObjectsV2 page -> list(objects = data.frame(key, bytes, etag),
  # token = continuation token or NULL). Pure, so a fixture can feed it XML.
  .acq_s3_ls_parse <- function(x) {
    if (grepl("<Error>", x, fixed = TRUE)) {
      stop("S3 list refused: ", sub(".*<Code>(.*?)</Code>.*", "\\1", x, perl = TRUE))
    }
    items <- regmatches(x, gregexpr("<Contents>.*?</Contents>", x, perl = TRUE))[[1]]
    tag <- function(t) {
      .acq_xml_unescape(sub(paste0(".*<", t, ">(.*?)</", t, ">.*"), "\\1", items, perl = TRUE))
    }
    objs <- if (length(items)) {
      data.frame(key = tag("Key"), bytes = as.numeric(tag("Size")),
                 etag = gsub('"', "", tag("ETag"), fixed = TRUE), stringsAsFactors = FALSE)
    } else {
      data.frame(key = character(0), bytes = numeric(0), etag = character(0))
    }
    token <- if (grepl("<IsTruncated>true</IsTruncated>", x, fixed = TRUE)) {
      sub(".*<NextContinuationToken>(.*?)</NextContinuationToken>.*", "\\1", x, perl = TRUE)
    }
    list(objects = objs, token = token)
  }

  # Anonymous listing of an S3 prefix over HTTPS. Not every prefix permits an
  # anonymous LIST (afr_highlands/ does not); such records use s3-file.
  # Directory-marker keys (trailing "/") are dropped.
  .acq_s3_ls <- function(prefix_uri) {
    s <- .acq_s3_split(prefix_uri)
    out <- list()
    token <- NULL
    repeat {
      q <- paste0(.acq_s3_endpoint(s$bucket), "/?list-type=2&prefix=",
                  utils::URLencode(s$key, reserved = TRUE),
                  if (!is.null(token)) paste0("&continuation-token=",
                                              utils::URLencode(token, reserved = TRUE)))
      tmp <- tempfile(fileext = ".xml")
      got <- tryCatch(utils::download.file(q, tmp, mode = "wb", quiet = TRUE),
                      error = function(e) stop("S3 list failed for ", prefix_uri, ": ",
                                               conditionMessage(e), call. = FALSE))
      x <- paste(readLines(tmp, warn = FALSE), collapse = "")
      unlink(tmp)
      pg <- .acq_s3_ls_parse(x)
      out[[length(out) + 1L]] <- pg$objects
      token <- pg$token
      if (is.null(token)) break
    }
    o <- do.call(rbind, out)
    o <- o[!endsWith(o$key, "/"), , drop = FALSE]
    o$src <- paste0("s3://", s$bucket, "/", o$key)
    o
  }

  # ---- plans: what each method fetches, as data.frame(src, dest, bytes, etag,
  # md5, role) with dest relative to the target dir. Shared with
  # R/checks/75_catalogue_manifest.R, so a manifest's dest column comes from the
  # same code that downloads. bytes/etag/md5 are NA unless a listing gave them.
  .acq_plan_df <- function(src, dest, bytes = NA_real_, etag = NA_character_, role = "file") {
    md5 <- ifelse(!is.na(etag) & grepl("^[0-9a-f]{32}$", etag), etag, NA_character_)
    data.frame(src = src, dest = dest, bytes = bytes, etag = etag, md5 = md5,
               role = role, stringsAsFactors = FALSE)
  }

  .acq_plan <- list(

    # Mirror an S3 prefix into the target directory.
    "s3-dir" = function(rec) {
      a <- rec$acquire
      prefix <- if (endsWith(a$source, "/")) a$source else paste0(a$source, "/")
      ls <- .acq_s3_ls(prefix)
      rel <- substring(ls$src, nchar(prefix) + 1L)
      keep <- isTRUE(a$recurse) | !grepl("/", rel, fixed = TRUE)
      if (!is.null(a$pattern)) keep <- keep & grepl(a$pattern, ls$src)
      if (!is.null(a$exclude)) keep <- keep & !grepl(a$exclude, ls$src)
      ls <- ls[keep, , drop = FALSE]
      rel <- rel[keep]
      if (!nrow(ls)) stop("no objects under ", a$source)
      .acq_plan_df(ls$src, if (isTRUE(a$flatten)) basename(rel) else rel, ls$bytes, ls$etag)
    },

    # Expand a glue-style path_pattern over enumerated params.
    "s3-glue" = function(rec) {
      a <- rec$acquire
      grid <- expand.grid(lapply(a$params, unlist), stringsAsFactors = FALSE)
      keys <- vapply(seq_len(nrow(grid)), function(i) {
        s <- a$source
        for (nm in names(grid)) s <- gsub(paste0("\\{", nm, "\\}"), grid[[nm]][i], s)
        s
      }, character(1))
      .acq_plan_df(keys, basename(keys))
    },

    "s3-file" = function(rec) {
      k <- unlist(rec$acquire$files)
      .acq_plan_df(k, basename(k))
    },

    "http-file" = function(rec) {
      f <- rec$acquire$files
      .acq_plan_df(vapply(f, function(x) x$url, character(1)),
                   vapply(f, function(x) x$dest %||% basename(x$url), character(1)))
    },

    # The archive is fetched, unpacked and dropped; `expect` is the member whose
    # presence says the unpack happened.
    "http-zip" = function(rec) {
      f <- rec$acquire$files
      .acq_plan_df(vapply(f, function(x) x$url, character(1)),
                   vapply(f, function(x) basename(x$url), character(1)), role = "archive")
    },

    # Scrape an index page for links matching a pattern.
    "http-scrape" = function(rec) {
      a <- rec$acquire
      if (!requireNamespace("rvest", quietly = TRUE)) stop("method http-scrape needs 'rvest'")
      links <- rvest::html_attr(rvest::html_nodes(rvest::read_html(a$source), "a"), "href")
      links <- grep(a$pattern, links, value = TRUE)
      if (!length(links)) stop("no links matching ", a$pattern, " at ", a$source)
      .acq_plan_df(file.path(a$source, links), basename(links))
    }
  )

  # ---- fetch one object -----------------------------------------------------
  .acq_row <- function(src, dest, bytes, action, verify) {
    data.frame(src = src, dest = dest, bytes = bytes, action = action, verify = verify,
               stringsAsFactors = FALSE)
  }

  # Fetch src to dest through a .part file, so an interrupted transfer never
  # leaves a truncated file that looks present (the likely origin of PASCAL's
  # two truncated MapSPAM tiles). With a manifest row (`expect`):
  #   * an existing file of the wrong size is re-fetched, not skipped;
  #   * a fetched file is hashed (md5, or the S3 multipart ETag) and fetched once
  #     more on a mismatch. A second mismatch stops, unless the origin is
  #     declared mutable (origin.manifest.mutable), where the new bytes are kept
  #     and reported as drift.
  .acq_download_one <- function(src, dest, overwrite, dry_run, expect = NULL, mutable = FALSE) {
    have <- file.exists(dest)
    if (have && !overwrite) {
      if (is.null(expect)) return(.acq_row(src, dest, file.size(dest), "skip", "none"))
      if (.acq_check(dest, expect, hash = FALSE) != "size") {
        return(.acq_row(src, dest, file.size(dest), "skip", "size"))
      }
      .acq_log("  wrong size, re-fetching: ", dest, " (", format(file.size(dest), big.mark = ","),
               " bytes, manifest ", format(expect$bytes, big.mark = ","), ")", level = "WARN")
    }
    if (dry_run) {
      return(.acq_row(src, dest, NA_real_, if (have) "would-refetch" else "would-fetch", NA))
    }
    dir.create(dirname(dest), recursive = TRUE, showWarnings = FALSE)
    part <- paste0(dest, ".part")
    for (attempt in 1:2) {
      unlink(part)
      .acq_get(src, part)
      v <- if (is.null(expect)) "none" else .acq_check(part, expect, hash = TRUE)
      if (v %in% c("none", "ok", "ok-size")) break
      .acq_log("  verify: ", v, " mismatch on attempt ", attempt, ": ", src, level = "WARN")
    }
    if (!v %in% c("none", "ok", "ok-size")) {
      if (!isTRUE(mutable)) {
        unlink(part)
        stop("verification failed (", v, ") for ", src, " after 2 attempts. Either the ",
             "transfer is corrupt or the origin changed since the manifest was listed; ",
             "check with Rscript R/checks/75_catalogue_manifest.R --check", call. = FALSE)
      }
      .acq_log("  origin drift (mutable origin, new bytes kept): ", src, level = "WARN")
      v <- "drift"
    }
    if (have) unlink(dest)
    if (!file.rename(part, dest)) stop("could not move ", part, " into place")
    .acq_row(src, dest, file.size(dest), if (have) "refetch" else "fetch", v)
  }

  # ---- the non-plan methods -------------------------------------------------
  .acq_http_zip <- function(rec, target, man, overwrite, dry_run, mutable) {
    do.call(rbind, lapply(rec$acquire$files, function(f) {
      member <- file.path(target, f$expect)
      if (file.exists(member) && !overwrite) {
        return(.acq_row(f$url, member, file.size(member), "skip", "none"))
      }
      if (dry_run) return(.acq_row(f$url, member, NA_real_, "would-fetch", NA))
      row <- if (!is.null(man)) man[man$src == f$url, , drop = FALSE]
      zp <- file.path(target, basename(f$url))
      r <- .acq_download_one(f$url, zp, overwrite = TRUE, dry_run = FALSE,
                             expect = if (NROW(row) == 1L) row, mutable = mutable)
      utils::unzip(zp, exdir = target)
      unlink(zp)
      if (!file.exists(member)) stop("archive ", f$url, " did not contain ", f$expect)
      .acq_row(f$url, member, file.size(member), "fetch", r$verify)
    }))
  }

  # The dataset is acquired by an existing script. We do NOT reimplement it -
  # we register it, so the catalogue still names the one reproducible command,
  # and the receipt records that it was run.
  .acq_script <- function(rec, target, dry_run) {
    a <- rec$acquire
    cmd <- a$command
    if (dry_run || !isTRUE(a$runnable)) {
      .acq_log("dataset '", rec$id, "' is acquired by a registered script.")
      .acq_log("  run: ", cmd)
      return(.acq_row(cmd, target, NA_real_, if (dry_run) "would-run" else "manual", "none"))
    }
    .acq_log("running registered acquisition command: ", cmd)
    st <- system(cmd)
    .acq_row(cmd, target, NA_real_, if (st == 0L) "ran" else paste0("failed(", st, ")"), "none")
  }

  # ---- the entry points -----------------------------------------------------

  #' Acquire a dataset using its registered recipe, and write a receipt.
  atlas_acquire <- function(id, force = FALSE, dry_run = FALSE) {
    rec <- atlas_catalogue(id)
    a <- rec$acquire
    if (is.null(a) || is.null(a$method)) {
      stop("dataset '", id, "' has no acquire block in its catalogue record.\n",
           "  Nothing can fetch it automatically. See the record's `gaps`.")
    }
    known <- c(names(.acq_plan), "script")
    if (!a$method %in% known) {
      stop("unknown acquire method '", a$method, "' for '", id, "'. Known: ",
           paste(known, collapse = ", "))
    }
    target <- atlas_dataset_dir(rec)
    man <- if (a$method == "script") NULL else atlas_manifest(rec)
    mutable <- isTRUE(rec$origin$manifest$mutable)
    .acq_log("acquire '", id, "' method=", a$method, " -> ", target,
             if (dry_run) "  [DRY RUN]" else "")
    if (!is.null(man)) {
      .acq_log("  pinned manifest: ", nrow(man), " object(s), ",
               format(sum(man$bytes, na.rm = TRUE), big.mark = ","), " bytes",
               if (mutable) " (mutable origin)" else "")
    }

    run <- function() {
      overwrite <- isTRUE(force)
      if (a$method == "script") return(.acq_script(rec, target, isTRUE(dry_run)))
      if (a$method == "http-zip") {
        return(.acq_http_zip(rec, target, man, overwrite, isTRUE(dry_run), mutable))
      }
      # Pinned objects when a manifest exists; otherwise what the origin lists
      # now (an s3-dir listing still carries sizes and ETags to check against).
      plan <- if (!is.null(man)) man[man$role == "file", , drop = FALSE] else .acq_plan[[a$method]](rec)
      do.call(rbind, lapply(seq_len(nrow(plan)), function(i) {
        p <- plan[i, , drop = FALSE]
        has <- !is.na(p$bytes) || !is.na(p$etag) || !is.na(p$md5)
        .acq_download_one(p$src, file.path(target, p$dest), overwrite, isTRUE(dry_run),
                          expect = if (has) p, mutable = mutable)
      }))
    }
    res <- tryCatch(run(),
                    error = function(e) {
                      .acq_write_receipt(id, list(method = a$method, status = "error",
                                                  error = conditionMessage(e),
                                                  source = a$source %||% a$command %||% NA))
                      stop("acquire '", id, "' failed: ", conditionMessage(e), call. = FALSE)
                    })

    n_fetch <- sum(res$action %in% c("fetch", "refetch", "ran"))
    n_skip <- sum(res$action == "skip")
    bytes <- sum(res$bytes, na.rm = TRUE)
    vt <- table(factor(res$verify, levels = c("ok", "ok-size", "size", "drift", "none")))
    .acq_log("  ", nrow(res), " object(s): ", n_fetch, " fetched (",
             sum(res$action == "refetch"), " re-fetched), ", n_skip,
             " already present, ", format(bytes, big.mark = ","), " bytes")
    .acq_log("  verified: hash ", vt[["ok"]], ", size only ", vt[["ok-size"]] + vt[["size"]],
             ", drift ", vt[["drift"]], ", no manifest ", vt[["none"]])

    if (!dry_run) {
      .acq_write_receipt(id, list(
        method = a$method,
        source = a$source %||% a$command %||% NA,
        version = rec$version %||% NA,
        files = nrow(res), fetched = n_fetch, skipped = n_skip,
        refetched = sum(res$action == "refetch"),
        verified_hash = vt[["ok"]], verified_size = vt[["ok-size"]] + vt[["size"]],
        drift = vt[["drift"]],
        manifest = rec$origin$checksum_manifest %||% NA,
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

  #' Audit this host's copy of a dataset against its pinned manifest.
  #' hash = FALSE stats files only (fast); hash = TRUE also checks md5 / S3 ETag
  #' (reads every byte). "extra" = a local file matching the record's glob that
  #' the origin does not hold. Returns the per-file table invisibly.
  atlas_verify <- function(id, hash = FALSE) {
    rec <- atlas_catalogue(id)
    files <- .acq_manifest_files(rec)
    if (is.null(files)) stop("dataset '", id, "' has no checksum manifest to verify against")
    if (!nrow(files)) stop("manifest for '", id, "' lists only archives; nothing on disk to check")
    d <- atlas_dataset_dir(rec)
    st <- vapply(seq_len(nrow(files)), function(i) {
      .acq_check(file.path(d, files$dest[i]), files[i, ], hash = hash)
    }, character(1))
    glob <- rec$location$glob %||% "*"
    local <- if (dir.exists(d)) list.files(d, pattern = utils::glob2rx(glob), recursive = TRUE) else character(0)
    extra <- setdiff(local, files$dest)
    out <- data.frame(dest = c(files$dest, extra),
                      bytes_manifest = c(files$bytes, rep(NA_real_, length(extra))),
                      bytes_local = file.size(file.path(d, c(files$dest, extra))),
                      status = c(st, rep("extra", length(extra))),
                      stringsAsFactors = FALSE)
    tab <- table(out$status)
    .acq_log("verify '", id, "' at ", d, if (hash) "  [hash]" else "  [size]")
    .acq_log("  manifest ", nrow(files), " object(s); local ", length(local),
             " file(s) matching '", glob, "'")
    .acq_log("  ", paste(sprintf("%s=%d", names(tab), as.integer(tab)), collapse = "  "))
    bad <- out[!out$status %in% c("ok", "ok-size"), , drop = FALSE]
    for (i in seq_len(min(nrow(bad), 60L))) {
      .acq_log(sprintf("  %-7s %s  (local %s, manifest %s)", bad$status[i], bad$dest[i],
                       format(bad$bytes_local[i], big.mark = ","),
                       format(bad$bytes_manifest[i], big.mark = ",")),
               level = if (bad$status[i] == "extra") "INFO" else "WARN")
    }
    if (nrow(bad) > 60L) .acq_log("  ... ", nrow(bad) - 60L, " more")
    invisible(out)
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
