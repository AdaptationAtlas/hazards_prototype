#!/usr/bin/env Rscript
# =============================================================================
# 72_crosshost_fingerprint.R - cross-host equivalence proof (issue #29)
#
# Fingerprints the FINAL artefacts of a stage so the same stage run on two hosts
# can be compared. Emits one TSV per host; the two get diffed.
#
#   Rscript R/checks/72_crosshost_fingerprint.R --stage 0.4 --out /tmp/fp_cglabs.tsv
#   diff -u /tmp/fp_cglabs.tsv /tmp/fp_mac.tsv
#
# SHAPE BEFORE VALUE. Rasters are fingerprinted on dim / ext / res / crs /
# nodata FIRST, then on cell statistics; parquets on row count and the ordered
# column set with types, then per-column checksums. A value-only gate has
# passed here before while the artefact extent was wrong, so shape is not
# optional and is reported as its own columns rather than folded into a hash.
#
# Pure read. Writes only the TSV named by --out.
# =============================================================================

options(stringsAsFactors = FALSE)
suppressPackageStartupMessages({
  library(terra)
  library(arrow)
})

args <- commandArgs(TRUE)
val <- function(f, default = NULL) {
  i <- match(f, args)
  if (is.na(i) || i == length(args)) default else args[i + 1L]
}

.t0 <- Sys.time(); .tl <- .t0
say <- function(..., level = "INFO") {
  now <- Sys.time()
  cat(sprintf("[%s] +%5.1fs (Δ%4.1fs) %-5s| %s\n",
              format(now, "%Y-%m-%d %H:%M:%S"),
              as.numeric(difftime(now, .t0, units = "secs")),
              as.numeric(difftime(now, .tl, units = "secs")),
              level, paste0(..., collapse = "")))
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

stage <- val("--stage", "0.4")
out <- val("--out")
if (is.null(out)) stop("usage: 72_crosshost_fingerprint.R --stage <id> --out <file>")

# Which artefacts define "this stage produced the right thing". Deliberately
# the FINAL products, not intermediates - an intermediate can match while the
# artefact a consumer reads does not.
targets <- switch(
  stage,
  "0.4" = list(
    list(key = "exposure", glob = "*.parquet", recursive = FALSE),
    list(path = "Data/mapspam/2020V1r2_SSA/processed", glob = "*.tif", recursive = TRUE),
    list(key = "GLW4_2020", subpath = "processed", glob = "*.tif", recursive = TRUE)
  ),
  stop("no fingerprint target set defined for stage '", stage, "'")
)

resolve <- function(t) {
  base <- if (!is.null(t$key)) atlas_dir(t$key, absolute = TRUE) else file.path(atlas_working_dir(), t$path)
  if (!is.null(t$subpath)) base <- file.path(base, t$subpath)
  base
}

# A truncated TIFF opens fine - the header is intact - and only fails when pixels
# are actually read. So rast() succeeding proves nothing; the stats call is where
# corruption surfaces, and it must not kill the run. A fingerprint that dies on
# the first bad file cannot tell you WHICH files are bad, which is most of the
# point. Unreadable is a first-class result, reported and counted.
fp_raster <- function(f) {
  r <- try(terra::rast(f), silent = TRUE)
  if (inherits(r, "try-error")) {
    return(list(shape = "<unopenable>", value = "<unopenable>", bad = TRUE))
  }
  shape <- paste(
    sprintf("dim=%s", paste(dim(r), collapse = "x")),
    sprintf("ext=%s", paste(round(as.vector(terra::ext(r)), 6), collapse = ",")),
    sprintf("res=%s", paste(signif(terra::res(r), 10), collapse = ",")),
    sprintf("crs=%s", terra::crs(r, describe = TRUE)$code %||% "NA"),
    sprintf("nlyr=%d", terra::nlyr(r)),
    sep = " "
  )
  # Per-layer summary. Rounded so a cross-platform ULP difference in the last
  # bits does not masquerade as a real divergence, but tight enough that a real
  # one still shows.
  stats <- tryCatch({
    st <- suppressWarnings(terra::global(r, fun = "sum", na.rm = TRUE)[, 1])
    nn <- suppressWarnings(terra::global(!is.na(r), fun = "sum", na.rm = TRUE)[, 1])
    paste(
      sprintf("sum=%s", paste(signif(st, 10), collapse = "|")),
      sprintf("n_nonNA=%s", paste(nn, collapse = "|")),
      sep = " "
    )
  }, error = function(e) {
    paste0("<CORRUPT: ", sub("\\s+", " ", substr(conditionMessage(e), 1, 120)), ">")
  })
  list(shape = shape, value = stats, bad = startsWith(stats, "<CORRUPT"))
}

fp_parquet <- function(f) {
  d <- try(arrow::read_parquet(f), silent = TRUE)
  if (inherits(d, "try-error")) {
    return(list(shape = "<unreadable>", value = "<unreadable>", bad = TRUE))
  }
  cols <- names(d)
  types <- vapply(d, function(x) class(x)[1], character(1))
  shape <- paste(
    sprintf("nrow=%d", nrow(d)),
    sprintf("ncol=%d", ncol(d)),
    sprintf("cols=%s", paste(sprintf("%s:%s", cols, types), collapse = ",")),
    sep = " "
  )
  num <- vapply(d, is.numeric, logical(1))
  sums <- vapply(cols, function(cn) {
    if (!num[[cn]]) return(NA_real_)
    sum(as.numeric(d[[cn]]), na.rm = TRUE)
  }, numeric(1))
  value <- paste(sprintf("%s=%s", cols[num], signif(sums[num], 10)), collapse = "|")
  list(shape = shape, value = value, bad = FALSE)
}

`%||%` <- function(a, b) if (is.null(a) || length(a) == 0L || is.na(a[1])) b else a

say("host=", atlas_host_id(), " working_dir=", atlas_working_dir(), " stage=", stage)

rows <- list()
for (t in targets) {
  dir <- resolve(t)
  if (!dir.exists(dir)) {
    say("MISSING target dir: ", dir)
    next
  }
  fs <- list.files(dir, pattern = utils::glob2rx(t$glob),
                   recursive = isTRUE(t$recursive), full.names = TRUE)
  fs <- sort(fs)
  say("fingerprinting ", length(fs), " file(s) under ", dir)
  for (f in fs) {
    rel <- sub(paste0("^", gsub("([.|()\\^{}+$*?\\[\\]])", "\\\\\\1", atlas_working_dir()), "/"), "", f)
    fpr <- if (grepl("\\.parquet$", f)) fp_parquet(f) else fp_raster(f)
    if (isTRUE(fpr$bad)) say("  UNREADABLE: ", rel, level = "WARN")
    rows[[length(rows) + 1L]] <- data.frame(
      artefact = rel, bytes = file.size(f), shape = fpr$shape, value = fpr$value
    )
  }
}

if (!length(rows)) stop("no artefacts fingerprinted - has the stage been run on this host?")

df <- do.call(rbind, rows)
df <- df[order(df$artefact), ]
con <- file(out, "w")
writeLines(paste(names(df), collapse = "\t"), con)
writeLines(do.call(paste, c(unname(as.list(df)), sep = "\t")), con)
close(con)
bad <- grepl("^<", df$value)
say("wrote ", nrow(df), " artefact rows to ", out)
if (any(bad)) {
  say(sum(bad), " artefact(s) could not be read - listed below and marked in the TSV.",
      level = "WARN")
  for (a in df$artefact[bad]) cat("    ", a, "\n", sep = "")
  say("A short/truncated tile means the local copy is damaged, not that the hosts ",
      "disagree. Re-fetch before treating any diff as meaningful.", level = "WARN")
}
say("compare with: diff -u <other-host>.tsv ", out)
