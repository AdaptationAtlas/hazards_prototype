#!/usr/bin/env Rscript
# =============================================================================
# 71_stage_ready.R - "can this host run stage X?" (issue #29 item 4)
#
# Answers the question BEFORE a long run starts, instead of eight hours in.
# Reads metadata/stages.json, resolves every declared input through
# R/00_paths.R, and reports PASS/FAIL per input. Exits non-zero on any FAIL.
#
#   Rscript R/checks/71_stage_ready.R --stage 0.4.4
#   Rscript R/checks/71_stage_ready.R --stage 3 --shard timeframe=annual
#   Rscript R/checks/71_stage_ready.R --all
#   Rscript R/checks/71_stage_ready.R --list
#
# Pure read. Creates nothing, downloads nothing, and does not source
# 0_server_setup.R - it only needs the resolver, so it costs about a second.
#
# Readiness is answered PER SHARD, not just per stage: a host may hold
# everything stage X needs for one GCM and nothing for another, and that is the
# question a scheduler asks before dispatching. --shard also checks that the
# stage can actually be filtered on that axis (stages.json shard_env), because
# an axis that partitions the output is not the same as an axis the script
# accepts a filter for.
# =============================================================================

options(stringsAsFactors = FALSE)

args <- commandArgs(TRUE)
has <- function(f) f %in% args
val <- function(f, default = NULL) {
  i <- match(f, args)
  if (is.na(i) || i == length(args)) default else args[i + 1L]
}
vals <- function(f) {
  out <- character(0)
  idx <- which(args == f)
  for (i in idx) if (i < length(args)) out <- c(out, args[i + 1L])
  out
}

.t0 <- Sys.time(); .tl <- .t0
say <- function(...) {
  now <- Sys.time()
  cat(sprintf("[%s] +%5.1fs (Δ%4.1fs) | %s\n",
              format(now, "%Y-%m-%d %H:%M:%S"),
              as.numeric(difftime(now, .t0, units = "secs")),
              as.numeric(difftime(now, .tl, units = "secs")),
              paste0(..., collapse = "")))
  .tl <<- now
  flush.console()
}

repo_root <- local({
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  d <- if (length(fa)) dirname(normalizePath(sub("^--file=", "", fa[1]), mustWork = FALSE)) else getwd()
  for (. in 1:5) {
    if (file.exists(file.path(d, "metadata", "data.json"))) return(d)
    d <- dirname(d)
  }
  stop("cannot locate repo root from ", getwd())
})

source(file.path(repo_root, "R", "00_paths.R"))
spec <- jsonlite::fromJSON(file.path(repo_root, "metadata", "stages.json"),
                           simplifyVector = FALSE)

if (has("--list")) {
  for (s in spec$stages) {
    ax <- unlist(s$shard_axes)
    cat(sprintf("  %-7s %-42s shard_axes: %s\n", s$id, s$script,
                if (length(ax)) paste(ax, collapse = ",") else "-"))
  }
  quit(save = "no", status = 0L)
}

wanted <- if (has("--all")) {
  vapply(spec$stages, `[[`, "", "id")
} else {
  v <- val("--stage")
  if (is.null(v)) {
    stop("usage: 71_stage_ready.R --stage <id> [--shard axis=value ...] | --all | --list")
  }
  v
}

shards <- vals("--shard")

# Resolve one declared input to an absolute directory + glob.
resolve_input <- function(inp) {
  base <- if (!is.null(inp$key)) {
    atlas_dir(inp$key, absolute = TRUE)
  } else if (!is.null(inp$path)) {
    file.path(atlas_working_dir(), inp$path)
  } else {
    stop("stages.json input has neither 'key' nor 'path'")
  }
  if (!is.null(inp$subpath)) base <- file.path(base, inp$subpath)
  list(
    dir = base,
    glob = inp$glob %||% "*",
    min_files = inp$min_files %||% 1L,
    optional = isTRUE(inp$optional),
    # Several trees are hive-partitioned (variable=.../*.tif), so a flat count
    # of the root is legitimately zero even when the data is all present.
    recursive = isTRUE(inp$recursive)
  )
}
`%||%` <- function(a, b) if (is.null(a)) b else a

say("host=", atlas_host_id(), " climdat_source=", atlas_climdat_source(),
    " working_dir=", atlas_working_dir())
shard_env <- Sys.getenv("ATLAS_SHARD", unset = "")
if (nzchar(shard_env)) say("ATLAS_SHARD=", shard_env)

overall_ok <- TRUE

for (sid in wanted) {
  st <- Filter(function(s) identical(s$id, sid), spec$stages)
  if (!length(st)) {
    ids <- vapply(spec$stages, `[[`, "", "id")
    stop("unknown stage '", sid, "'. Known: ", paste(ids, collapse = ", "))
  }
  st <- st[[1]]

  cat("\n")
  say("stage ", st$id, "  (", st$script, ")")

  # Shard axis validation. An axis can partition the outputs while no filter
  # exists to select a slice - say so plainly rather than letting a scheduler
  # assume the split is safe.
  if (length(shards)) {
    axes <- unlist(st$shard_axes)
    for (sh in shards) {
      parts <- strsplit(sh, "=", fixed = TRUE)[[1]]
      ax <- parts[1]
      if (!ax %in% axes) {
        cat(sprintf("  WARN  shard axis '%s' does not partition stage %s (axes: %s)\n",
                    ax, st$id, if (length(axes)) paste(axes, collapse = ",") else "none"))
        overall_ok <- FALSE
      } else {
        ev <- st$shard_env[[ax]]
        if (is.null(ev)) {
          cat(sprintf("  WARN  axis '%s' partitions the output but stage %s has no filter wired for it;\n",
                      ax, st$id))
          cat("        splitting on it needs a code change, not just an env var.\n")
        } else {
          cat(sprintf("  ok    axis '%s' selectable via %s=%s\n", ax, ev, parts[2]))
        }
      }
    }
  }

  for (inp in st$inputs) {
    r <- resolve_input(inp)
    n <- if (dir.exists(r$dir)) {
      length(list.files(r$dir, pattern = utils::glob2rx(r$glob), recursive = r$recursive))
    } else {
      NA_integer_
    }

    status <- if (is.na(n)) {
      "MISSDIR"
    } else if (n < r$min_files) {
      "SHORT"
    } else {
      "PASS"
    }
    if (status != "PASS" && !r$optional) overall_ok <- FALSE
    if (status != "PASS" && r$optional) status <- paste0(status, "*")

    cat(sprintf("  %-8s %-6s %s\n", status,
                if (is.na(n)) "-" else sprintf("n=%d", n),
                file.path(r$dir, if (r$recursive) paste0("**/", r$glob) else r$glob)))
  }
}

cat("\n")
if (overall_ok) {
  say("READY")
  quit(save = "no", status = 0L)
} else {
  say("NOT READY - see MISSDIR/SHORT/WARN rows above")
  say("* = optional input, does not block")
  quit(save = "no", status = 1L)
}
