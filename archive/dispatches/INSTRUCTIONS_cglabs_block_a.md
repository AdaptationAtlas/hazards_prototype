# Instructions for cglabs: Execute Block A of DISPATCH_cglabs_pop_denominator_fixes.md

**Context:** Commit `bb7788e` is on `origin/develop`. Block B remains PARKED (needs Pete's decision on #43). Block A is strictly value-neutral (removes leaked `i.pop_source` column from `exposure_jrc_rp.parquet` and `exposure_totals.parquet` and republishes tier 16 to S3).

Execute Block A step by step, stop at every gate, and report what you see:

---

### Step 1: Sync and Gate A0 Preflight
```bash
cd /home/jovyan/atlas/hazards_prototype && git pull --ff-only && git log -1 --oneline
pgrep -af Rscript || echo "no Rscript running"
env | grep -E '^(POP_|APPLY|IN_DIR|EXP_ROOT)' || echo "no POP_ env set (good)"
Rscript -e '
  source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")
  suppressPackageStartupMessages(library(arrow)); library(data.table)
  d <- file.path(dirname(chirts_chirps_hist_dir), "exposure", "intersect")
  for (f in c("exposure_gfm_seasonal.parquet","exposure_jrc_rp.parquet","exposure_totals.parquet")) {
    n <- names(read_parquet(file.path(d, f), as_data_frame = FALSE))
    cat(sprintf("%-30s i.cols=%-16s mtime=%s\n", f,
        paste(grep("^i\\.", n, value = TRUE), collapse = ",") , format(file.mtime(file.path(d,f)))))
  }
  t <- as.data.table(read_parquet(file.path(d, "exposure_totals.parquet")))
  cat(sprintf("rows=%d adm1=%d national pop_total=%.0f pop_total_grid=%.0f\n",
      nrow(t), uniqueN(t$adm1_pcode), sum(t$pop_total), sum(t$pop_total_grid)))
  cat(sprintf("pop_source=%s pop_method=%s pop_year=%s\n", t$pop_source[1], t$pop_method[1], t$pop_year[1]))'
```
* **Expectations:**
  - `exposure_jrc_rp` and `exposure_totals` report only `i.pop_source`; `exposure_gfm_seasonal` reports none.
  - 290 rows, 47 counties, `pop_method = county-level`.
  - Baseline `national pop_total` is expected to be `52,837,534` (grid sum: `55,119,798`).
  - **STOP** if GFM carries an `i.` column, or any other `i.` column appears, or `pop_method != county-level`.

---

### Step 2: Gate A1 Dry Run
```bash
cd /home/jovyan/atlas/hazards_prototype
POP_SOURCE=knbs-projection POP_YEAR=2026 POP_METHOD=county-level \
POP_YEAR_MATCH=1 POP_REF_YEAR=2026 \
Rscript R/observational/7b_relevel_exposure_pop.R 2>&1 | tee logs/relevel42_dryrun_$(date +%Y%m%d_%H%M%S).log
```
* **Expectations:**
  - `dropped stale join artefact: i.pop_source (#42)` logged for `jrc` and `totals`, not GFM.
  - All three tables report **x1.0000**.
  - `national pop_total` equals the A0 figure (`52,837,534`).
  - **STOP** if any factor is not x1.0000 or national total shifts.

---

### Step 3: Gate A2 Apply
```bash
cd /home/jovyan/atlas/hazards_prototype
POP_SOURCE=knbs-projection POP_YEAR=2026 POP_METHOD=county-level \
POP_YEAR_MATCH=1 POP_REF_YEAR=2026 APPLY=1 \
Rscript R/observational/7b_relevel_exposure_pop.R 2>&1 | tee logs/relevel42_apply_$(date +%Y%m%d_%H%M%S).log
```
* **Expectations:**
  - Same x1.0000 lines, concludes with `WROTE -> ...`.

---

### Step 4: Gate A3 Local Gates & Idempotence Check
```bash
cd /home/jovyan/atlas/hazards_prototype
Rscript -e '
  source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")
  suppressPackageStartupMessages(library(arrow)); library(data.table)
  d <- file.path(dirname(chirts_chirps_hist_dir), "exposure", "intersect")
  ok <- TRUE
  for (f in c("exposure_gfm_seasonal.parquet","exposure_jrc_rp.parquet","exposure_totals.parquet")) {
    t <- as.data.table(read_parquet(file.path(d, f)))
    leaked <- grep("^i\\.", names(t), value = TRUE)
    dev <- max(abs(t$pop_total - t$pop_total_grid * t$pop_scale_census * t$pop_growth_county), na.rm = TRUE)
    cat(sprintf("%-30s i.cols=%-10s identity_dev=%.3g\n", f,
                if (length(leaked)) paste(leaked, collapse=",") else "none", dev))
    if (length(leaked) || !is.finite(dev) || dev > 1e-6) ok <- FALSE
  }
  cat(if (ok) "GATE PASS\n" else "GATE FAIL\n")'
```
* **Expectations:**
  - `i.cols=none` across all three tables, `identity_dev < 1e-6`, output is `GATE PASS`.
  - Re-run the A1 dry run command: must report **no** `dropped stale join artefact` lines and **x1.0000** on all three.

---

### Step 5: Gate A4 Publish Tier 16 (Backgrounded)
```bash
cd /home/jovyan/atlas/hazards_prototype
export STAMP=$(date +%Y%m%d_%H%M%S)
nohup Rscript R/observational/6_publish_obs_to_s3.R --full --tier 16 --overwrite \
  > logs/publish_t16_$STAMP.log 2>&1 &
echo $! > logs/publish_t16_$STAMP.pid
sleep 30; tail -20 logs/publish_t16_$STAMP.log
```
* **Expectations:**
  - 3/3 objects uploaded, zero errors.

---

### Step 6: Gate A5 S3 Verification & Response Prepend
```bash
cd /home/jovyan/atlas/hazards_prototype
Rscript -e '
  suppressPackageStartupMessages(library(arrow)); library(data.table)
  b <- "https://digital-atlas.s3.amazonaws.com/domain=exposure/type=intersect/region=kenya/processing=analysis-ready/"
  for (f in c("exposure_gfm_seasonal.parquet","exposure_jrc_rp.parquet","exposure_totals.parquet")) {
    tf <- tempfile(fileext=".parquet"); download.file(paste0(b,f), tf, quiet=TRUE, mode="wb")
    t <- as.data.table(read_parquet(tf))
    cat(sprintf("%-30s ncol=%3d i.cols=%s\n", f, ncol(t),
        if (length(grep("^i\\.", names(t)))) paste(grep("^i\\.", names(t), value=TRUE), collapse=",") else "none"))
    if (f == "exposure_totals.parquet")
      cat(sprintf("  national pop_total=%.0f pop_source=%s pop_method=%s\n",
          sum(t$pop_total), t$pop_source[1], t$pop_method[1]))
  }'
```
* Prepend the `### RESPONSE` block at the top of `DISPATCH_cglabs_pop_denominator_fixes.md` with:
  - Side-by-side A0 and A5 national totals (`52,837,534`).
  - A1 and A3 x-factors (`x1.0000`).
  - Tail of the publish log.
* Commit, push to `origin/develop`, and confirm `git log origin/develop..HEAD` is clean.
