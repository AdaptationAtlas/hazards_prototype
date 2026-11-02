#!/usr/bin/env Rscript
# R/checks/fixture_acquire_manifest.R
# ===================================
# Issue #58 item 1: download-time verification in R/00_acquire.R. No network, seconds.
#   real     every manifest shipped under metadata/catalogue/_manifests parses through the real
#            atlas_manifest(), and agrees with its record (object count, bytes, mutable flag, roles)
#   etag     the S3 multipart-ETag check agrees with an independent implementation (digest), and
#            rejects a one-byte change
#   list     the ListObjectsV2 parser unescapes &quot;/&amp; and returns the continuation token
#   s3-dir   end to end through the real atlas_require()/atlas_acquire()/atlas_verify() against a
#            file:// "bucket": fetch + hash; a truncated local file makes the dataset not-present and
#            is re-fetched; a corrupted origin object stops the fetch and leaves the good local copy;
#            no .part file survives; extras are reported, not failed; dry_run writes nothing
#   zip      a mutable archive whose size drifted is kept as "drift"; the same drift on an
#            immutable record stops
# Usage: Rscript R/checks/fixture_acquire_manifest.R
root <- if (nzchar(Sys.getenv("ATLAS_PROJECT_DIR"))) Sys.getenv("ATLAS_PROJECT_DIR") else { fa <- grep("^--file=", commandArgs(FALSE), value = TRUE); if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd() }
ok <- function(cond, msg) { if (!isTRUE(cond)) stop("FIXTURE FAIL: ", msg) else cat("  ok  ", msg, "\n") }
quietly <- function(expr) { capture.output(r <- expr); r }

source(file.path(root, "R", "00_paths.R"))
source(file.path(root, "R", "00_acquire.R"))

## ---- real: the shipped manifests --------------------------------------------------------------
recs <- atlas_catalogue()
withm <- Filter(function(r) !is.null(r$origin$checksum_manifest) && !grepl("://", r$origin$checksum_manifest), recs)
ok(length(withm) >= 13, sprintf("at least 13 records carry a repo manifest (found %d)", length(withm)))
for (r in withm) {
  m <- atlas_manifest(r)
  mm <- r$origin$manifest
  ok(!is.null(m) && nrow(m) == mm$objects && sum(m$bytes) == mm$bytes && !anyNA(m$bytes) &&
       !anyDuplicated(m$dest) && all(m$role %in% c("file", "archive")) &&
       isTRUE(mm$mutable) == (r$acquire$method == "http-zip") &&
       all((m$role == "archive") == (r$acquire$method == "http-zip")),
     sprintf("%-24s manifest parses; %d obj / %s bytes agree with the record", r$id, nrow(m), format(sum(m$bytes), big.mark = ",")))
}

## ---- etag: independent reference ------------------------------------------------------------
td <- tempfile("acqfx_"); dir.create(td)
set.seed(58)
big <- file.path(td, "big.bin")
writeBin(as.raw(sample(0:255, 9 * 1024^2 + 12345, TRUE)), big)
ref_etag <- function(path, ps) {
  b <- readBin(path, "raw", file.size(path))
  starts <- seq(1, length(b), by = ps)
  parts <- lapply(starts, function(s) b[s:min(s + ps - 1, length(b))])
  d <- unlist(lapply(parts, function(p) digest::digest(p, "md5", serialize = FALSE, raw = TRUE)))
  paste0(digest::digest(d, "md5", serialize = FALSE), "-", length(parts))
}
if (requireNamespace("digest", quietly = TRUE)) {
  e8 <- ref_etag(big, 8 * 1024^2)
  ok(isTRUE(.acq_s3_etag_matches(big, e8)), sprintf("multipart ETag %s matches the digest reference (8 MiB parts)", e8))
  e5 <- ref_etag(big, 5 * 1024^2)
  ok(isTRUE(.acq_s3_etag_matches(big, e5)), "a 5 MiB-part ETag is also recognised (part-size search)")
  ok(isTRUE(.acq_s3_etag_matches(big, digest::digest(big, "md5", file = TRUE))), "single-part ETag = plain md5")
  bad <- file.path(td, "bad.bin"); file.copy(big, bad)
  con <- file(bad, "r+b"); seek(con, 5e6, rw = "write"); writeBin(as.raw(0x00), con); close(con)
  if (identical(readBin(big, "raw", 5e6 + 1)[5e6 + 1], as.raw(0x00))) {
    con <- file(bad, "r+b"); seek(con, 5e6, rw = "write"); writeBin(as.raw(0x01), con); close(con)
  }
  ok(isFALSE(.acq_s3_etag_matches(bad, e8)), "a one-byte change fails the multipart ETag")
} else cat("  skip  digest not installed - etag reference checks skipped\n")

## ---- list: parser ---------------------------------------------------------------------------
xml <- paste0('<?xml version="1.0"?><ListBucketResult><IsTruncated>true</IsTruncated>',
              '<Contents><Key>p/a&amp;b.tif</Key><Size>10</Size><ETag>&quot;0123456789abcdef0123456789abcdef&quot;</ETag></Contents>',
              '<Contents><Key>p/sub/c.tif</Key><Size>2048</Size><ETag>&quot;0123456789abcdef0123456789abcdef-3&quot;</ETag></Contents>',
              '<NextContinuationToken>tok/en==</NextContinuationToken></ListBucketResult>')
pg <- .acq_s3_ls_parse(xml)
ok(identical(pg$objects$key, c("p/a&b.tif", "p/sub/c.tif")) && identical(pg$objects$bytes, c(10, 2048)) &&
     identical(pg$objects$etag[2], "0123456789abcdef0123456789abcdef-3") && identical(pg$token, "tok/en=="),
   "ListObjectsV2 page parsed: keys unescaped, quotes stripped, continuation token returned")
ok(inherits(tryCatch(.acq_s3_ls_parse("<Error><Code>AccessDenied</Code></Error>"), error = identity), "error"),
   "an <Error> listing stops rather than returning zero objects")

## ---- s3-dir end to end ----------------------------------------------------------------------
bucket <- file.path(td, "bucket", "fxbucket", "fx")
dir.create(file.path(bucket, "sub"), recursive = TRUE)
invisible(file.copy(big, file.path(bucket, "a.bin")))
writeLines("bee", file.path(bucket, "b.txt"))
writeLines(c("sea", "two"), file.path(bucket, "sub", "c.txt"))
catdir <- file.path(td, "catalogue"); dir.create(file.path(catdir, "_manifests"), recursive = TRUE)
md5 <- function(p) unname(tools::md5sum(p))
man <- data.frame(
  src = paste0("s3://fxbucket/fx/", c("a.bin", "b.txt", "sub/c.txt")),
  dest = c("a.bin", "b.txt", "sub/c.txt"),
  bytes = file.size(file.path(bucket, c("a.bin", "b.txt", "sub/c.txt"))),
  etag = c(if (exists("e8")) e8 else NA, md5(file.path(bucket, "b.txt")), md5(file.path(bucket, "sub/c.txt"))),
  md5 = c(NA, md5(file.path(bucket, "b.txt")), md5(file.path(bucket, "sub/c.txt"))),
  role = "file")
man$bytes <- format(man$bytes, scientific = FALSE, trim = TRUE)
write.table(man, file.path(catdir, "_manifests", "fx.tsv"), sep = "\t", quote = FALSE, row.names = FALSE, na = "")
rec <- list(schema_version = "1.0.0", id = "fx",
            origin = list(type = "public-s3", url = "s3://fxbucket/fx/",
                          checksum_manifest = "_manifests/fx.tsv",
                          manifest = list(objects = 3, mutable = FALSE)),
            location = list(key = NULL, path = "Data/fx", glob = "*", recursive = TRUE),
            completeness = list(min_files = 1),
            acquire = list(method = "s3-dir", source = "s3://fxbucket/fx/", recurse = TRUE))
writeLines(jsonlite::toJSON(rec, auto_unbox = TRUE, null = "null", pretty = 2), file.path(catdir, "fx.json"))

wd <- file.path(td, "wd"); dir.create(wd)
Sys.setenv(ATLAS_WORKING_DIR = wd)
options(atlas.catalogue_dir = catdir, atlas.s3_endpoint = paste0("file://", file.path(td, "bucket"), "/%s"))
tgt <- file.path(wd, "Data", "fx")

r0 <- quietly(atlas_acquire("fx", dry_run = TRUE))
ok(all(r0$action == "would-fetch") && !dir.exists(tgt), "dry_run plans 3 fetches and writes nothing")
ok(!atlas_dataset_present(atlas_catalogue("fx")), "absent before the first fetch")
r1 <- quietly(atlas_acquire("fx"))
ok(nrow(r1) == 3 && all(r1$action == "fetch") && all(r1$verify == "ok"), "3 objects fetched, every one hash-verified (multipart ETag, md5, md5)")
ok(atlas_dataset_present(atlas_catalogue("fx")), "present after the fetch")
ok(!length(list.files(tgt, "\\.part$", recursive = TRUE)), "no .part file left behind")

writeBin(readBin(file.path(tgt, "a.bin"), "raw", 1e6), file.path(tgt, "a.bin"))   # truncate
ok(!atlas_dataset_present(atlas_catalogue("fx")), "a truncated tile makes the dataset NOT present (a file count would have passed)")
v <- quietly(atlas_verify("fx"))
ok(v$status[v$dest == "a.bin"] == "size", "atlas_verify flags it as size")
r2 <- quietly(atlas_require("fx"))
rr <- quietly(atlas_acquire("fx"))
ok(all(rr$action == "skip"), "after atlas_require the copy is whole (second acquire skips all 3)")
ok(identical(md5(file.path(tgt, "a.bin")), md5(big)), "the re-fetched tile is byte-identical to the origin")

writeLines("extra", file.path(tgt, "stray.tif"))
v <- quietly(atlas_verify("fx", hash = TRUE))
ok(sum(v$status == "ok") == 3 && v$status[v$dest == "stray.tif"] == "extra", "atlas_verify(hash) = 3 ok + 1 extra; the extra is reported, not failed")

writeLines(c("SEA", "two"), file.path(bucket, "sub", "c.txt"))                    # corrupt origin, same size
before <- md5(file.path(tgt, "sub", "c.txt"))
e <- tryCatch(quietly(atlas_acquire("fx", force = TRUE)), error = identity)
ok(inherits(e, "error") && grepl("verification failed \\(hash\\)", conditionMessage(e)),
   "a same-size corrupted origin object stops the fetch with a hash failure")
ok(identical(md5(file.path(tgt, "sub", "c.txt")), before) && !file.exists(file.path(tgt, "sub", "c.txt.part")),
   "the good local copy is untouched and the bad .part is removed")
rc <- jsonlite::fromJSON(tail(readLines(file.path(wd, "Data", "_acquisition", "fx.jsonl")), 1))
ok(identical(rc$status, "error"), "the failure is written to the receipt")

## ---- zip: mutable vs immutable ------------------------------------------------------------------
zsrc <- file.path(td, "zsrc"); dir.create(zsrc)
writeLines("x,y\n1,2", file.path(zsrc, "member.csv"))
owd <- setwd(zsrc); utils::zip("pack.zip", "member.csv", flags = "-q"); setwd(owd)
zurl <- paste0("file://", file.path(zsrc, "pack.zip"))
write.table(data.frame(src = zurl, dest = "pack.zip", bytes = "999", etag = "", md5 = "", role = "archive"),
            file.path(catdir, "_manifests", "fz.tsv"), sep = "\t", quote = FALSE, row.names = FALSE, na = "")
zrec <- function(mutable) list(id = "fz", origin = list(checksum_manifest = "_manifests/fz.tsv", manifest = list(mutable = mutable)),
                               location = list(path = "Data/fz", glob = "*.csv"),
                               acquire = list(method = "http-zip", files = list(list(url = zurl, expect = "member.csv"))))
writeLines(jsonlite::toJSON(zrec(TRUE), auto_unbox = TRUE, null = "null"), file.path(catdir, "fz.json"))
z1 <- quietly(atlas_acquire("fz"))
ok(z1$verify == "drift" && file.exists(file.path(wd, "Data", "fz", "member.csv")) && !file.exists(file.path(wd, "Data", "fz", "pack.zip")),
   "mutable archive with a drifted size: kept, unpacked, reported as drift")
writeLines(jsonlite::toJSON(zrec(FALSE), auto_unbox = TRUE, null = "null"), file.path(catdir, "fz.json"))
e2 <- tryCatch(quietly(atlas_acquire("fz", force = TRUE)), error = identity)
ok(inherits(e2, "error") && grepl("verification failed \\(size\\)", conditionMessage(e2)), "the same drift on an immutable record stops")

unlink(td, recursive = TRUE)
cat("\nALL ACQUIRE-MANIFEST FIXTURE ASSERTIONS PASSED\n")
