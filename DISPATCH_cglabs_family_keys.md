# Dispatch: per-unit exposure family keys — publish the 2021 pair, retire usd-2015 (#30 follow-up)

**Status:** code on `develop`. Block A is read-only and runnable now. Blocks B and C write to S3 and
are **GO-gated**. Run this **after** `DISPATCH_cglabs_r3_res25_rerun.md` Block D has published, so the
node is not carrying two live writes at once - nothing here depends on that re-bake, it is sequencing
only.

**Why.** Under `domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/`
three per-unit keys sit next to the canonical `crop-livestock_all*`:

| key | live since | `unit` in the live file | producer | consumer |
|---|---|---|---|---|
| `variable=vop_intld15-2021.parquet` | 2025-11-03 | `intld15` (pre-#30 label) | none until now | `R/checks/19_*` |
| `variable=vop_nominal-usd-2021.parquet` | 2025-11-03 | `usd` | none until now | **KE-ENSO ROI notebook, live** |
| `variable=vop_nominal-usd-2015.parquet` | 2026-01-28 | `usd15` | **none at all** | `R/checks/19_*`, `24_*` (closed analyses) |

Decision (p.steward 2026-09-26): the 2021 pair gets a publisher - 0.4.4 §3.2 / §3.3 already write
their local twins at both resolutions - and `vop_nominal-usd-2015` is retired: nothing produces it,
no vintage in the chain maintains it, and #23 documents a defect in it nobody can fix.

**What changed in code.** `scripts/r3_publish_tiers.R --family` / `--family-only` publishes
`vop_nominal-usd-2021` and `vop_intld15-2021` through the **same** `publish_table()` path and gates as
`--reference`: `--res` required, key `variable=<name>_<res-tag>.parquet`, unsuffixed key rewritten as
the deprecated res-05 alias, columns identical to live (14 = 14), `distinct(exposure/unit/stat)`
identical or a 1:1 unit vintage move under `--allow-unit-vintage-change`, rows within 25 % of live
(res-05 should be **identical**: the live files were built from the same 0.05° extraction), first
res-25 publish gates against the legacy key with `--allow-res-change`. Sidecars ship. Backups go to
`s3://digital-atlas/sandbox/backup/issue9_<STAMP>/`. The script has **no delete path**; the
retirement in Block C is an explicit, GO-gated `s3fs` call.

**Consumer-visible change, relayed before GO (macbook's job, not yours):** the ROI notebook builds
its label as `CONCAT(exposure, '_', unit)`, so `vop_usd` becomes `vop_nominal-usd-2021`; row count
and columns unchanged, values change where #30 corrected them.

---

## Block A - read-only audit (~2 min, paste)

```bash
cd <hazards_prototype> && git fetch origin && git checkout develop && git pull --ff-only && git log -1 --oneline
Rscript -e '
  suppressPackageStartupMessages({library(arrow); library(dplyr)}); source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")
  for (f in c("vop_nominal-usd-2021", "vop_intld15-2021")) for (r in c("res-05", "res-25")) {
    p <- file.path(exposure_dir, sprintf("%s_adm_sum_spam20_glw420_%s.parquet", f, r)); d <- open_dataset(p)
    u <- (d |> distinct(unit) |> collect())$unit
    cat(sprintf("%-22s %s rows %8d cols %2d unit [%s] sidecar %s mtime %s\n", f, r, nrow(d), length(names(d$schema)), paste(u, collapse=","),
        file.exists(paste0(p, ".json")), format(file.mtime(p), "%Y-%m-%d %H:%M"))) }'
Rscript scripts/r3_publish_tiers.R --family-only --res 0.05 --allow-unit-vintage-change --dry-run
Rscript scripts/r3_publish_tiers.R --family-only --res 0.25 --allow-unit-vintage-change --allow-res-change --dry-run
```
**Expect:** four local twins present, 14 columns each, a single vintage-ful `unit` each
(`nominal-usd-2021` / `intld15-2021`), sidecars present, mtimes from the Block I 0.4.4 runs
(2026-09-24). Dry-runs: for each family table `ok: 14 columns identical`, `distinct(exposure)` and
`distinct(stat)` identical, `unit vintage change ALLOWED ... 1 -> 1 units` naming `usd` →
`nominal-usd-2021` and `intld15` → `intld15-2021`, and for **res-05 `rows local N vs live N (identical)`**;
res-25 rows `INFORMATIONAL` against the legacy baseline. Any FAIL line: **STOP**, paste.

---

## Block B - publish the 2021 pair at both resolutions (LIVE WRITE - GO-gated)

> **SUPERSEDED.** Block B was executed as part of `DISPATCH_cglabs_r3_res25_rerun.md` C0-4 on 2026-09-28 (GO p.steward): both 2021 keys published at `_res-05` / `_res-25` with the unsuffixed aliases, verified from S3 on both machines. Nothing to run here.

```bash
cd <hazards_prototype>; STAMP=$(date +%Y%m%d_%H%M%S)
Rscript scripts/r3_publish_tiers.R --family-only --res 0.05 --allow-unit-vintage-change 2>&1 | tee logs/publish_family_res-05_$STAMP.log
Rscript scripts/r3_publish_tiers.R --family-only --res 0.25 --allow-unit-vintage-change --allow-res-change 2>&1 | tee logs/publish_family_res-25_$STAMP.log
```
Verify **from S3** (re-download; s3fs may multipart, so ETag is not md5):
```bash
Rscript -e '
  suppressPackageStartupMessages({library(arrow); library(s3fs)}); source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")
  base <- "s3://digital-atlas/domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/"
  chk <- function(local, key) { tmp <- tempfile(fileext = ".parquet"); s3_file_download(paste0(base, key), tmp)
    cat(sprintf("%-42s md5 %s | rows S3 %d local %d | sidecar %s\n", key, if (tools::md5sum(local) == tools::md5sum(tmp)) "MATCH" else "MISMATCH",
        nrow(read_parquet(tmp)), nrow(read_parquet(local)), if (s3_file_exists(paste0(base, key, ".json"))) "present" else "MISSING")) }
  for (f in c("vop_nominal-usd-2021", "vop_intld15-2021")) {
    chk(file.path(exposure_dir, sprintf("%s_adm_sum_spam20_glw420_res-05.parquet", f)), sprintf("variable=%s_res-05.parquet", f))
    chk(file.path(exposure_dir, sprintf("%s_adm_sum_spam20_glw420_res-05.parquet", f)), sprintf("variable=%s.parquet", f))
    chk(file.path(exposure_dir, sprintf("%s_adm_sum_spam20_glw420_res-25.parquet", f)), sprintf("variable=%s_res-25.parquet", f)) }'
for k in vop_nominal-usd-2021_res-05 vop_nominal-usd-2021_res-25 vop_nominal-usd-2021 vop_intld15-2021_res-05 vop_intld15-2021_res-25 vop_intld15-2021; do
  curl -sI -r 0-99 "https://digital-atlas.s3.amazonaws.com/domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/variable=$k.parquet" | grep -E '^HTTP|Last-Modified' | tr '\n' ' '; echo " <- $k"; done
```
**Expect:** six `MATCH`, sidecars present, `HTTP/1.1 206` ×6 with today's `Last-Modified`. Then the
ROI notebook's own query must still return rows - macbook runs it over HTTPS with duckdb; you need not.

**STOP.** Paste.

---

## Block C - retire `vop_nominal-usd-2015` (LIVE DELETE - GO-gated, after Block B is verified)

> **GO given by p.steward 2026-10-01 for Block C.** Prerequisite met: the ROI notebook (`atlas_nb-KE-enso`, our repo) was moved to the `_res-05` key the same day. Run as written, paste the backup key and the 404, STOP. Macbook then closes #23.

Backup first, then delete, then prove it is gone. The publisher never deletes; this is the only place
a delete happens, and it is one key.

```bash
Rscript -e '
  suppressPackageStartupMessages(library(s3fs)); source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")
  key <- "s3://digital-atlas/domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/variable=vop_nominal-usd-2015.parquet"
  bak <- sub("^s3://digital-atlas/", paste0("s3://digital-atlas/sandbox/backup/issue9_", format(Sys.time(), "%Y%m%d_%H%M%S"), "/"), key)
  stopifnot(s3_file_exists(key)); tmp <- tempfile(fileext = ".parquet"); s3_file_download(key, tmp)
  s3_file_upload(tmp, bak, ACL = "public-read", overwrite = TRUE); stopifnot(s3_file_exists(bak))
  cat("backup:", bak, "size", s3_file_info(bak)$size, "\n")
  s3_file_delete(key); cat("deleted:", key, "| still exists?", s3_file_exists(key), "\n")'
curl -sI "https://digital-atlas.s3.amazonaws.com/domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/variable=vop_nominal-usd-2015.parquet" | head -1
```
**Expect:** backup written with the same size as the live object (2,248,114 bytes on 2026-09-25,
informational), `still exists? FALSE`, `HTTP/1.1 404`. Paste the backup key. Then macbook closes #23
(defect in a retired object) and records the retirement in the R/checks headers (already on develop).
