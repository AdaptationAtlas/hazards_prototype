#!/usr/bin/env Rscript
# =============================================================================
# 75_catalogue_manifest.R - pin what each origin holds (issue #58 item 1)
#
#   Rscript R/checks/75_catalogue_manifest.R --coverage                 # who has a manifest
#   Rscript R/checks/75_catalogue_manifest.R --build [--dataset a,b]    # list origins, write
#   Rscript R/checks/75_catalogue_manifest.R --check [--dataset a,b]    # list origins, diff only
#   Rscript R/checks/75_catalogue_manifest.R --verify <id> [--hash]     # audit THIS host's copy
#
# A manifest is metadata/catalogue/_manifests/<id>.tsv: one row per object the
# recipe fetches - src, dest (relative to the target dir), bytes, S3 ETag, md5,
# role (file | archive). It is built from S3 listings, HEAD requests and the
# Dataverse file API. NO DATA IS DOWNLOADED: --build and --check read headers
# only. R/00_acquire.R verifies against it at download time.
#
# Checksum sources, best first:
#   md5   S3 single-part ETag, GCS x-goog-hash md5, Dataverse dataFile.md5
#   etag  S3 multipart ETag ("<md5>-<parts>"), checked by recomputing it
#   size  everything else (FAOSTAT and GRDC archives, which change in place, so
#         their records carry origin.manifest.mutable = true and a mismatch is
#         reported as drift, not a failure)
#
# --build rewrites origin.checksum_manifest / origin.manifest and appends a
# changelog entry when the listing changed. --check exits 1 on drift in a
# non-mutable record. --verify exits 1 on any missing / size / hash row.
# Needs network for --build/--check; uses the curl CLI for HEADs.
# =============================================================================

options(stringsAsFactors = FALSE)

args <- commandArgs(TRUE)
has <- function(f) f %in% args
val <- function(f, default = NULL) {
  i <- match(f, args)
  if (is.na(i) || i == length(args)) default else args[i + 1L]
}
`%||%` <- function(a, b) if (is.null(a) || length(a) == 0L) b else a

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
source(file.path(repo_root, "R", "00_acquire.R"))

cat_dir <- file.path(repo_root, "metadata", "catalogue")
man_rel <- function(id) file.path("metadata", "catalogue", "_manifests", paste0(id, ".tsv"))
recs <- atlas_catalogue()
eligible <- Filter(function(r) !is.null(r$acquire$method) && r$acquire$method %in% names(.acq_plan), recs)
COLS <- c("src", "dest", "bytes", "etag", "md5", "role")

# ---- --verify: this host's copy ---------------------------------------------
if (!is.null(val("--verify"))) {
  out <- atlas_verify(val("--verify"), hash = has("--hash"))
  bad <- sum(out$status %in% c("missing", "size", "hash"))
  cat(if (bad) sprintf("FAIL - %d object(s) missing or wrong\n", bad) else "PASS - every manifest object present and matching\n")
  quit(save = "no", status = if (bad) 1L else 0L)
}

# ---- --coverage -------------------------------------------------------------
if (has("--coverage")) {
  cm <- vapply(recs, function(r) r$origin$checksum_manifest %||% NA_character_, character(1))
  cat(sprintf("records: %d   with checksum_manifest: %d   null: %d\n",
              length(recs), sum(!is.na(cm)), sum(is.na(cm))))
  cat(sprintf("S3/HTTPS-acquired (manifest-eligible): %d   with manifest: %d\n\n",
              length(eligible), sum(!is.na(cm[names(eligible)]))))
  for (r in eligible) {
    m <- r$origin$manifest
    cat(sprintf("  %-26s %-11s %s\n", r$id, r$acquire$method,
                if (is.null(m)) "-" else sprintf("%d obj  %s bytes  verify=%s%s  listed %s",
                  m$objects, format(m$bytes, big.mark = ","), m$verify,
                  if (isTRUE(m$mutable)) " (mutable)" else "", m$listed)))
  }
  quit(save = "no", status = 0L)
}

if (!has("--build") && !has("--check")) {
  cat("usage: --coverage | --build [--dataset a,b] | --check [--dataset a,b] | --verify <id> [--hash]\n")
  quit(save = "no", status = 2L)
}

# ---- listing helpers (headers only) -----------------------------------------
curl_headers <- function(url, range = FALSE) {
  a <- c("-s", "-L", "-m", "60", if (range) c("-r", "0-0", "-D", "-", "-o", "/dev/null") else "-I", url)
  h <- suppressWarnings(system2("curl", shQuote(a), stdout = TRUE, stderr = FALSE))
  h <- sub("\r$", "", h)
  starts <- grep("^HTTP/", h)
  if (!length(starts)) return(NULL)
  last <- h[max(starts):length(h)]            # the final hop after redirects
  code <- as.integer(strsplit(last[1], " ")[[1]][2])
  kv <- strsplit(last[-1][grepl(":", last[-1])], ":\\s*")
  hdr <- list()
  for (p in kv) {
    k <- tolower(p[1]); v <- paste(p[-1], collapse = ":")
    hdr[[k]] <- c(hdr[[k]], v)
  }
  list(code = code, h = hdr)
}

b64_to_hex <- function(b) paste(sprintf("%02x", as.integer(jsonlite::base64_dec(b))), collapse = "")

# Fill bytes / etag / md5 for one plan row that has none (s3-dir rows arrive filled).
describe <- function(src) {
  url <- if (startsWith(src, "s3://")) .acq_s3_https(src) else src
  dv <- regmatches(url, regexec("dataverse\\.harvard\\.edu/api/access/datafile/([0-9]+)", url))[[1]]
  if (length(dv) == 2L) {
    j <- jsonlite::fromJSON(sprintf("https://dataverse.harvard.edu/api/files/%s", dv[2]))
    f <- j$data$dataFile
    return(list(bytes = as.numeric(f$filesize), etag = NA_character_, md5 = f$md5 %||% NA_character_))
  }
  r <- curl_headers(url)
  if (is.null(r) || r$code >= 400 || is.null(r$h[["content-length"]])) {
    r2 <- curl_headers(url, range = TRUE)       # GRDC refuses HEAD (400) but honours Range
    cr <- r2$h[["content-range"]]
    if (is.null(cr)) stop("no size obtainable for ", url, " (HTTP ", r$code %||% NA, ")")
    return(list(bytes = as.numeric(sub(".*/", "", cr)),
                etag = gsub('"', "", r2$h[["etag"]][1] %||% NA_character_), md5 = NA_character_))
  }
  etag <- gsub('"', "", r$h[["etag"]][1] %||% NA_character_)
  gh <- grep("^md5=", r$h[["x-goog-hash"]], value = TRUE)
  md5 <- if (length(gh)) b64_to_hex(sub("^md5=", "", gh[1])) else NA_character_
  if (startsWith(src, "s3://") && is.na(md5) && grepl("^[0-9a-f]{32}$", etag)) md5 <- etag
  list(bytes = as.numeric(tail(r$h[["content-length"]], 1)), etag = etag, md5 = md5)
}

build_one <- function(r) {
  p <- .acq_plan[[r$acquire$method]](r)
  for (i in seq_len(nrow(p))) {
    if (!is.na(p$bytes[i])) next
    d <- describe(p$src[i])
    p$bytes[i] <- d$bytes; p$etag[i] <- d$etag; p$md5[i] <- d$md5
  }
  # Only S3 ETags and real md5s are checkable; an opaque HTTP ETag is kept for
  # the record but never compared.
  p <- p[order(p$dest), COLS]
  rownames(p) <- NULL
  p
}

verify_level <- function(p) {
  f <- p[p$role == "file", , drop = FALSE]
  hashable <- !is.na(f$md5) | (!is.na(f$etag) & startsWith(f$src, "s3://"))
  if (!nrow(f) || !any(hashable)) "size" else if (all(hashable)) "hash" else "mixed"
}

read_committed <- function(id) {
  f <- file.path(repo_root, man_rel(id))
  if (!file.exists(f)) return(NULL)
  m <- utils::read.delim(f, colClasses = "character", na.strings = c("", "NA"), quote = "", comment.char = "")
  m$bytes <- as.numeric(m$bytes)
  m[, COLS]
}

diff_manifest <- function(old, new) {
  if (is.null(old)) return(list(added = new$dest, removed = character(0), changed = character(0)))
  k <- function(m) paste(m$dest, m$bytes, m$etag, m$md5, sep = "|")
  both <- intersect(old$dest, new$dest)
  changed <- both[k(old[match(both, old$dest), ]) != k(new[match(both, new$dest), ])]
  list(added = setdiff(new$dest, old$dest), removed = setdiff(old$dest, new$dest), changed = changed)
}

ids <- if (!is.null(val("--dataset"))) strsplit(val("--dataset"), ",")[[1]] else names(eligible)
unknown <- setdiff(ids, names(eligible))
if (length(unknown)) stop("not manifest-eligible (no S3/HTTPS acquire recipe): ", paste(unknown, collapse = ", "))

today <- format(Sys.Date())
drift_fail <- 0L
for (id in ids) {
  r <- recs[[id]]
  mutable <- r$acquire$method == "http-zip"
  new <- tryCatch(build_one(r), error = function(e) e)
  if (inherits(new, "error")) {
    cat(sprintf("%-26s SKIP  origin not listable from here: %s\n", id, conditionMessage(new)))
    next
  }
  old <- read_committed(id)
  d <- diff_manifest(old, new)
  n_diff <- length(d$added) + length(d$removed) + length(d$changed)
  cat(sprintf("%-26s %3d obj  %15s bytes  verify=%-5s %s\n", id, nrow(new),
              format(sum(new$bytes), big.mark = ","), verify_level(new),
              if (is.null(old)) "NEW" else if (!n_diff) "unchanged" else
                sprintf("DRIFT +%d -%d ~%d%s", length(d$added), length(d$removed),
                        length(d$changed), if (mutable) " (mutable)" else "")))
  if (n_diff && !is.null(old)) {
    for (x in head(c(if (length(d$added)) paste("+", d$added), if (length(d$removed)) paste("-", d$removed), if (length(d$changed)) paste("~", d$changed)), 20)) cat("      ", x, "\n")
  }

  if (has("--check")) {
    if (is.null(old) || (n_diff && !mutable)) drift_fail <- drift_fail + 1L
    next
  }

  # --build
  if (!is.null(old) && !n_diff) next
  dir.create(file.path(repo_root, dirname(man_rel(id))), showWarnings = FALSE)
  w <- new
  w$bytes <- format(w$bytes, scientific = FALSE, trim = TRUE)
  utils::write.table(w, file.path(repo_root, man_rel(id)), sep = "\t", quote = FALSE,
                     row.names = FALSE, na = "")
  f <- file.path(cat_dir, paste0(id, ".json"))
  rj <- jsonlite::read_json(f)
  rj$origin$checksum_manifest <- man_rel(id)
  rj$origin$manifest <- list(objects = nrow(new), bytes = sum(new$bytes),
                             verify = verify_level(new), mutable = mutable, listed = today)
  rj$changelog[[length(rj$changelog) + 1L]] <- list(
    date = today,
    change = sprintf("%s checksum manifest: %d object(s), %s bytes, verify=%s%s. Listed from the origin by R/checks/75_catalogue_manifest.R (headers only); R/00_acquire.R checks every download against it.",
                     if (is.null(old)) "Added" else "Rebuilt", nrow(new),
                     format(sum(new$bytes), big.mark = ","), verify_level(new),
                     if (mutable) " (origin changes in place, so a mismatch is drift, not failure)" else ""),
    ref = "#58")
  writeLines(jsonlite::toJSON(rj, auto_unbox = TRUE, pretty = 2, null = "null", digits = NA), f)
}

if (has("--check")) {
  cat(if (drift_fail) sprintf("\nFAIL - %d record(s) missing a manifest or drifted\n", drift_fail) else "\nPASS - every manifest matches its origin\n")
  quit(save = "no", status = if (drift_fail) 1L else 0L)
}
