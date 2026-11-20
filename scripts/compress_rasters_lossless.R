#!/usr/bin/env Rscript
# compress_rasters_lossless.R - rewrite uncompressed GeoTIFFs with lossless ZSTD, in place (issue #58).
#
# For each path in --list (one absolute path per line):
#   1. skip it if the file already carries a COMPRESSION tag (so a stopped run can simply be restarted);
#   2. gdal_translate it (via sf::gdal_utils, so all GDAL metadata, band descriptions and nodata are copied)
#      to <path>.dup2tmp.tif in the SAME folder, with COMPRESS=ZSTD, ZSTD_LEVEL=9, TILED=YES and
#      PREDICTOR=3 for float / 2 for integer types;
#   3. check the copy against the original: same dimensions, extent, CRS, layer names, data type, NA flag
#      and terra metadata tags; every cell equal (max |a - b| = 0) and the NA pattern identical;
#   4. only if every check passes: copy the original's mode and mtime onto the copy and rename it over the
#      original (an atomic replace on one filesystem). Otherwise delete the copy, log FAIL and STOP the run.
#
# Writes one TSV row per file to --log (appending). Nothing is uploaded anywhere.
#
# Usage:
#   Rscript scripts/compress_rasters_lossless.R --list files.txt --log out.tsv [--limit N] [--tmpdir DIR] [--dry-run]

suppressPackageStartupMessages({ library(terra); library(sf) })

args <- commandArgs(trailingOnly = TRUE)
opt <- function(name, default = NULL) {
  i <- match(name, args)
  if (is.na(i)) return(default)
  if (i == length(args) || startsWith(args[i + 1], "--")) return(TRUE)
  args[i + 1]
}
list_file <- opt("--list"); log_file <- opt("--log")
limit <- as.integer(opt("--limit", "0")); dry <- isTRUE(opt("--dry-run", FALSE))
tmpdir <- opt("--tmpdir", tempdir())
if (is.null(list_file) || is.null(log_file)) stop("--list and --log are required")
dir.create(tmpdir, showWarnings = FALSE, recursive = TRUE)
terraOptions(tempdir = tmpdir, progress = 0)

compression_of <- function(p) {
  m <- grep("COMPRESSION=", describe(p), value = TRUE)
  if (length(m)) sub(".*COMPRESSION=", "", trimws(m[1])) else "NONE"
}

same_structure <- function(a, b) {
  checks <- c(
    dims  = identical(dim(a), dim(b)),
    ext   = isTRUE(all.equal(as.vector(ext(a)), as.vector(ext(b)), tolerance = 0)),
    crs   = identical(crs(a), crs(b)),
    names = identical(names(a), names(b)),
    type  = identical(datatype(a), datatype(b)),
    naflg = identical(NAflag(a), NAflag(b)),
    tags  = identical(metags(a), metags(b))
  )
  checks
}

paths <- readLines(list_file)
paths <- paths[nzchar(paths)]
if (limit > 0) paths <- head(paths, limit)
if (!file.exists(log_file)) {
  cat("time\tstatus\tpath\tbytes_before\tbytes_after\tpct\tsecs\tdetail\n", file = log_file)
}
logrow <- function(status, p, b0 = NA, b1 = NA, secs = NA, detail = "") {
  pct <- if (is.na(b0) || is.na(b1)) NA else round(100 * b1 / b0, 2)
  cat(sprintf("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), status, p,
              b0, b1, pct, if (is.na(secs)) NA else round(secs, 1), detail),
      file = log_file, append = TRUE)
}

n_ok <- 0L; n_skip <- 0L; saved <- 0
for (p in paths) {
  t0 <- Sys.time()
  if (!file.exists(p)) { logrow("MISSING", p); next }
  b0 <- file.size(p)
  cmp <- compression_of(p)
  if (cmp != "NONE") { logrow("skip-compressed", p, b0, b0, detail = cmp); n_skip <- n_skip + 1L; next }
  if (dry) { logrow("dry-run", p, b0); next }

  tmp <- paste0(p, ".dup2tmp.tif")
  if (file.exists(tmp)) unlink(tmp)                      # left over from an interrupted run
  a <- rast(p)
  dt <- datatype(a)[1]
  pred <- if (grepl("^FLT", dt)) "PREDICTOR=3" else "PREDICTOR=2"
  ok_tr <- tryCatch({
    sf::gdal_utils("translate", p, tmp, options = c(
      "-of", "GTiff", "-co", "COMPRESS=ZSTD", "-co", "ZSTD_LEVEL=9", "-co", pred,
      "-co", "TILED=YES", "-co", "BIGTIFF=IF_SAFER"), quiet = TRUE)
    TRUE
  }, error = function(e) conditionMessage(e))
  if (!isTRUE(ok_tr)) {
    unlink(tmp); logrow("FAIL", p, b0, detail = paste("translate:", ok_tr))
    stop("translate failed on ", p, ": ", ok_tr)
  }

  b <- rast(tmp)
  st <- same_structure(a, b)
  d <- if (all(st)) suppressWarnings(max(global(abs(a - b), "max", na.rm = TRUE)[, 1], na.rm = TRUE)) else NA
  na_mis <- if (all(st)) sum(global(is.na(a) != is.na(b), "sum")[, 1]) else NA
  if (identical(na_mis, 0) && !is.na(d) && is.infinite(d)) d <- 0   # every cell NA in both: nothing to differ
  if (!all(st) || !identical(d, 0) || !identical(na_mis, 0)) {
    detail <- sprintf("structure-fail=[%s] max|diff|=%s na-mismatch=%s",
                      paste(names(st)[!st], collapse = ","), d, na_mis)
    rm(a, b); unlink(tmp); logrow("FAIL", p, b0, detail = detail)
    stop("lossless check failed on ", p, ": ", detail)
  }
  rm(a, b); tmpFiles(remove = TRUE)

  Sys.chmod(tmp, file.mode(p), use_umask = FALSE)
  Sys.setFileTime(tmp, file.mtime(p))
  if (!file.rename(tmp, p)) {
    unlink(tmp); logrow("FAIL", p, b0, detail = "rename failed")
    stop("rename failed on ", p)
  }
  b1 <- file.size(p)
  logrow("ok", p, b0, b1, as.numeric(difftime(Sys.time(), t0, units = "secs")), cmp)
  n_ok <- n_ok + 1L; saved <- saved + (b0 - b1)
}
cat(sprintf("done: %d compressed, %d already compressed, %.2f GB saved\n", n_ok, n_skip, saved / 1e9))
