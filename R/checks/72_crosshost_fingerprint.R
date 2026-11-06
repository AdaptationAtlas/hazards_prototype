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
#   Rscript R/checks/72_crosshost_fingerprint.R --stage 0.4.1 --res res-25 --out fp_pascal.tsv
#   Rscript R/checks/72_crosshost_fingerprint.R --compare fp_cglabs.tsv fp_pascal.tsv [--rtol 1e-6]
#
# --stage 0.4.1 fingerprints only what R/0.4.1 writes for one grid (--res
# res-25 | res-05): the six VoP rasters, livestock numbers, the masks, the
# shoat proportions and the fao_vop prices-used CSVs. Older products in the
# same directories are left out, so they cannot create one-sided rows.
# Every TSV also carries _env/* rows (R, terra, GDAL, PROJ and key package
# versions), so a value difference can be traced to a library difference.
#
# --compare is the VALUE gate (issue #58 phase B):
#   * shape must match exactly;
#   * n_nonNA must match exactly;
#   * every sum must agree within --rtol (relative tolerance, default 1e-6).
# It exits 1 on any SHAPE-DIFF / VALUE-DIFF in a shared artefact, or on an
# artefact present on one side only. _env rows are reported, never gated.
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

`%||%` <- function(a, b) if (is.null(a) || length(a) == 0L || is.na(a[1])) b else a

# ---- --compare: the value gate ------------------------------------------------
if (!is.na(match("--compare", args))) {
  i <- match("--compare", args)
  fa <- args[i + 1L]; fb <- args[i + 2L]
  rtol <- as.numeric(val("--rtol", "1e-6"))
  rd <- function(f) utils::read.delim(f, colClasses = "character", quote = "", na.strings = character(0))
  A <- rd(fa); B <- rd(fb)
  nums <- function(v, key) {
    # "sum=1|2 n_nonNA=3|4" or "col=1|col2=2"; returns a named numeric vector
    if (startsWith(v, "<")) return(NULL)
    if (grepl("^sum=", v)) {
      m <- regmatches(v, regexec(paste0(key, "=([^ ]*)"), v))[[1]]
      if (length(m) < 2L) return(NULL)
      return(as.numeric(strsplit(m[2], "|", fixed = TRUE)[[1]]))
    }
    if (key != "sum") return(numeric(0))
    kv <- strsplit(strsplit(v, "|", fixed = TRUE)[[1]], "=", fixed = TRUE)
    stats::setNames(as.numeric(vapply(kv, `[`, "", 2)), vapply(kv, `[`, "", 1))
  }
  rel <- function(a, b) ifelse(a == b, 0, abs(a - b) / pmax(abs(a), abs(b)))
  keys <- union(A$artefact, B$artefact)
  res <- do.call(rbind, lapply(keys, function(k) {
    a <- A[A$artefact == k, ]; b <- B[B$artefact == k, ]
    env <- startsWith(k, "_env/")
    st <- if (!nrow(a)) "ONLY-B" else if (!nrow(b)) "ONLY-A" else if (env) {
      if (identical(a$value, b$value)) "env-same" else "env-diff"
    } else if (!identical(a$shape, b$shape)) "SHAPE-DIFF" else if (startsWith(a$value, "md5=")) {
      if (identical(a$value, b$value)) "same" else "VALUE-DIFF"
    } else {
      sa <- nums(a$value, "sum"); sb <- nums(b$value, "sum")
      na <- nums(a$value, "n_nonNA"); nb <- nums(b$value, "n_nonNA")
      if (is.null(sa) || is.null(sb)) "UNREADABLE"
      else if (length(sa) != length(sb) || !identical(names(sa), names(sb)) || !identical(na, nb)) "VALUE-DIFF"
      else {
        r <- max(rel(sa, sb), 0, na.rm = TRUE)
        if (any(is.na(sa) != is.na(sb))) "VALUE-DIFF" else if (r == 0) "same" else if (r <= rtol) "within-tol" else "VALUE-DIFF"
      }
    }
    mx <- if (nrow(a) && nrow(b) && !env && !startsWith(a$value, "<") && !startsWith(a$value, "md5=")) {
      sa <- nums(a$value, "sum"); sb <- nums(b$value, "sum")
      if (length(sa) == length(sb) && length(sa)) signif(max(rel(sa, sb), na.rm = TRUE), 3) else NA
    } else NA
    data.frame(artefact = k, status = st, max_rel_diff = mx,
               a = if (env && nrow(a)) a$value else "", b = if (env && nrow(b)) b$value else "")
  }))
  say("compare ", basename(fa), " (A) vs ", basename(fb), " (B), rtol=", rtol)
  tab <- table(res$status)
  say(paste(sprintf("%s=%d", names(tab), as.integer(tab)), collapse = "  "))
  for (j in which(res$status == "env-diff")) say(sprintf("  env-diff  %-26s A=%s  B=%s", sub("^_env/", "", res$artefact[j]), res$a[j], res$b[j]))
  bad <- res[res$status %in% c("SHAPE-DIFF", "VALUE-DIFF", "UNREADABLE", "ONLY-A", "ONLY-B"), ]
  for (j in seq_len(nrow(bad))) say(sprintf("  %-10s %s  (max rel diff %s)", bad$status[j], bad$artefact[j], bad$max_rel_diff[j]), level = "WARN")
  wt <- res[res$status == "within-tol", ]
  if (nrow(wt)) say("within tolerance, largest relative difference: ", max(wt$max_rel_diff, na.rm = TRUE))
  if (!is.null(val("--out"))) utils::write.table(res[, 1:3], val("--out"), sep = "\t", quote = FALSE, row.names = FALSE)
  say(if (nrow(bad)) sprintf("FAIL - %d artefact(s) differ or are one-sided", nrow(bad)) else "PASS - every shared artefact matches; no one-sided artefacts")
  quit(save = "no", status = if (nrow(bad)) 1L else 0L)
}

stage <- val("--stage", "0.4")
out <- val("--out")
if (is.null(out)) stop("usage: 72_crosshost_fingerprint.R --stage <id> --out <file>  |  --compare <a.tsv> <b.tsv>")
tag <- val("--res", "res-25")

# Which artefacts define "this stage produced the right thing". Deliberately
# the FINAL products, not intermediates - an intermediate can match while the
# artefact a consumer reads does not.
targets <- switch(
  stage,
  # Every input R/0.4.1 reads, as md5 (repo files such as the base grids come
  # with the git commit). Compare these FIRST: when FAOSTAT differs between
  # hosts, the VoP outputs are expected to differ too.
  "0.4.1-inputs" = list(
    list(key = "GLW4_2020", glob = "*.tif", recursive = FALSE, md5 = TRUE),
    list(key = "afr_highlands", glob = "*.asc", recursive = FALSE, md5 = TRUE),
    list(dataset = "boundaries-gaul2024", glob = "*_a0_*.parquet", recursive = FALSE, md5 = TRUE),
    list(key = "fao", glob = "*.csv", recursive = FALSE, md5 = TRUE,
         keep = "^(Value_of_Production_E_Africa|Value_of_Production_E_All_Area_Groups|Prices_E_Africa_NOFLAG|Prices_E_All_Data_\\(Normalized\\)|Production_Crops_Livestock_E_Africa_NOFLAG)\\.csv$")
  ),
  "0.4.1" = list(
    list(key = "GLW4_2020", subpath = "processed", glob = "*", recursive = TRUE,
         keep = paste0("(glw4-2020_vop_.*_", tag, "\\.tif|livestock_number_number_", tag, "\\.tif|-prices-used\\.csv)$")),
    list(key = "GLW4_2020", subpath = "intermediate", glob = "*.tif", recursive = FALSE,
         keep = paste0("(livestock_masks|shoat_prop)_", tag, "\\.tif$"))
  ),
  "0.4" = list(
    list(key = "exposure", glob = "*.parquet", recursive = FALSE),
    list(path = "Data/mapspam/2020V1r2_SSA/processed", glob = "*.tif", recursive = TRUE),
    list(key = "GLW4_2020", subpath = "processed", glob = "*.tif", recursive = TRUE)
  ),
  stop("no fingerprint target set defined for stage '", stage, "'")
)

resolve <- function(t) {
  if (!is.null(t$dataset)) {                      # where the catalogue record says it lives
    if (!exists("atlas_dataset_dir")) source(file.path(repo_root, "R", "00_acquire.R"))
    return(atlas_dataset_dir(atlas_catalogue(t$dataset)))
  }
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

# The prices-used CSVs: row count + ordered columns, then per-column sums (the
# same form as parquets, so --compare treats them alike).
fp_csv <- function(f) {
  d <- try(utils::read.csv(f, check.names = FALSE), silent = TRUE)
  if (inherits(d, "try-error")) return(list(shape = "<unreadable>", value = "<unreadable>", bad = TRUE))
  types <- vapply(d, function(x) class(x)[1], character(1))
  shape <- sprintf("nrow=%d ncol=%d cols=%s", nrow(d), ncol(d),
                   paste(sprintf("%s:%s", names(d), types), collapse = ","))
  num <- vapply(d, is.numeric, logical(1))
  sums <- vapply(names(d)[num], function(cn) sum(as.numeric(d[[cn]]), na.rm = TRUE), numeric(1))
  list(shape = shape, value = paste(sprintf("%s=%s", names(sums), signif(sums, 10)), collapse = "|"), bad = FALSE)
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
  if (!is.null(t$keep)) fs <- fs[grepl(t$keep, basename(fs))]
  fs <- sort(fs)
  say("fingerprinting ", length(fs), " file(s) under ", dir)
  for (f in fs) {
    rel <- sub(paste0("^", gsub("([.|()\\^{}+$*?\\[\\]])", "\\\\\\1", atlas_working_dir()), "/"), "", f)
    fpr <- if (isTRUE(t$md5)) list(shape = sprintf("bytes=%.0f", file.size(f)), value = paste0("md5=", unname(tools::md5sum(f))), bad = FALSE)
    else if (grepl("\\.parquet$", f)) fp_parquet(f) else if (grepl("\\.csv$", f)) fp_csv(f) else fp_raster(f)
    if (isTRUE(fpr$bad)) say("  UNREADABLE: ", rel, level = "WARN")
    rows[[length(rows) + 1L]] <- data.frame(
      artefact = rel, bytes = file.size(f), shape = fpr$shape, value = fpr$value
    )
  }
}

if (!length(rows)) stop("no artefacts fingerprinted - has the stage been run on this host?")

# Environment rows: a value difference with a library difference beside it is
# a different finding from one without.
pv <- function(p) tryCatch(as.character(utils::packageVersion(p)), error = function(e) "absent")
env <- c(R = paste(R.version$major, R.version$minor, sep = "."),
         terra = pv("terra"), sf = pv("sf"), data.table = pv("data.table"), dplyr = pv("dplyr"),
         tidyr = pv("tidyr"), rlang = pv("rlang"), vctrs = pv("vctrs"), arrow = pv("arrow"),
         GDAL = tryCatch(terra::gdal(), error = function(e) "?"),
         PROJ = tryCatch(as.character(terra::gdal(lib = "proj")), error = function(e) "?"),
         platform = R.version$platform, git = tryCatch(system2("git", c("-C", repo_root, "rev-parse", "--short", "HEAD"), stdout = TRUE), error = function(e) "?"))
rows[[length(rows) + 1L]] <- data.frame(artefact = paste0("_env/", names(env)), bytes = NA,
                                        shape = "", value = unname(env))

df <- do.call(rbind, rows)
df <- df[order(df$artefact), ]
con <- file(out, "w")
writeLines(paste(names(df), collapse = "\t"), con)
writeLines(do.call(paste, c(unname(as.list(df)), sep = "\t")), con)
close(con)
bad <- grepl("^<", df$value) & !startsWith(df$artefact, "_env/")
say("wrote ", nrow(df), " artefact rows to ", out)
if (any(bad)) {
  say(sum(bad), " artefact(s) could not be read - listed below and marked in the TSV.",
      level = "WARN")
  for (a in df$artefact[bad]) cat("    ", a, "\n", sep = "")
  say("A short/truncated tile means the local copy is damaged, not that the hosts ",
      "disagree. Re-fetch before treating any diff as meaningful.", level = "WARN")
}
say("compare with: diff -u <other-host>.tsv ", out)
