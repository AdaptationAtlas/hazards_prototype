# DISPATCH — issue #26: R/2.1 GCM pin + historic collapse + baseline mislabel → full re-bake

**Status: WAITING FOR CGLABS. Newest block on top; append replies as dated blocks.**

---

## [macbook 2026-09-17] Code fixed on develop — run the audit, then the FORCE re-bake

Issue: https://github.com/AdaptationAtlas/hazards_prototype/issues/26
Fix commit: see `git log --oneline -1 -- R/2.1_create_monthly_haz_tables.R` after `git pull origin develop`.

### What changed (why the old outputs must all die)

1. **5-GCM pin removed** (old `R/2.1:189-191`). Default = ALL GCMs found in
   `indices_dir`. `R21_GCMS="A,B"` env var subsets for dev runs; an
   ensemble-evenness gate hard-stops when scenario × timeframe GCM counts are
   uneven (`R21_ALLOW_UNEVEN_ENSEMBLE=1` bypasses — do NOT set without a GO).
2. **Historic folder parse fixed**: `historical_<gcm>_<y1>_<y2>` now parses
   per-GCM like the ssp folders. Intermediates become
   `historic_<gcm>_<window>_<hazard>_<stat>.parquet` — no more
   `historic_historic_historic` collapse, historic no longer dropped.
3. **Baselines are window-keyed**: one baseline per historic window on disk
   (both `1981-2014` and `1995-2014` kept — Pete's call, window in the name).
   Anomaly outputs are `*_anomaly-<window>_*`; the interim `anomaly-historic`
   naming is gone. This also kills the mislabel (anomalies computed vs
   1981-2014 under a 1995-2014 label).
4. **Futures leak fixed**: historic-window files no longer enter `file_combos`
   as futures.
5. Guards: stale `historic_historic_historic_*` intermediates hard-stop §2.5;
   `scripts/r21_publish_to_s3.R` hard-stops on any `_anomaly-historic_` file.
6. Publishers updated to window names: `scripts/r21_publish_to_s3.R` (now
   publishes BOTH baselines: `baseline=1981-2014` keys are new + additive,
   `baseline=1995-2014` keys unchanged for the notebook), `R/republish_A.R`,
   `R/publish_B.R`, `R/build_publish_C.R` (these three stay 1995-2014-only).

Validated locally: 24-assertion synthetic probe on the parse/baseline/combo
logic (18 GCMs × 16 ssp + 2 historic windows, R21_GCMS paths, gate trip/bypass,
stale-guard trip, file_combos = 10 unique, §3.4 `.path_single_source` holds).
All PASS. `Rscript -e parse()` clean on all 5 R files, `bash -n` clean on the
wrapper.

### Step 0 — pull + verify

```bash
cd ~/atlas/hazards_prototype
git pull origin develop
git log --oneline -3
```

### Step 1 — AUDIT indices_dir completeness (fast, do this first)

The evenness gate will stop the run if any window/scenario is short of GCMs.
The 1981-2014 window has a history of being partial (~8 GCMs in
indices_seasonal at one point). Audit before burning a run:

```bash
IND=/home/jovyan/common_data/atlas_nex-gddp_hazards/cmip6/indices
ls -d $IND/historical_*_1981_2014 | wc -l   # expect 18
ls -d $IND/historical_*_1995_2014 | wc -l   # expect 18
for s in ssp126 ssp245 ssp370 ssp585; do for p in 2021_2040 2041_2060 2061_2080 2081_2100; do
  echo -n "$s $p: "; ls -d $IND/${s}_*_${p} 2>/dev/null | wc -l
done; done                                   # expect 18 each
# per-folder hazard completeness for the 9 R/2.1 hazards (monthly tifs):
for d in $IND/historical_*_1981_2014; do
  for h in HSH TMAX TAVG NDWL0 NDWS NTx35 PTOT THI NDD; do
    n=$(ls $d/$h*/*.tif 2>/dev/null | wc -l); [ "$n" -eq 0 ] && echo "EMPTY: $d $h"
  done
done
```

**If 1981-2014 is short anywhere: STOP and report the table in this file.**
Pete decides whether to (a) bridge/re-bake the missing window upstream first or
(b) drop the 1981-2014 baseline for this bake. Do NOT set
`R21_ALLOW_UNEVEN_ENSEMBLE=1` on your own.

### Step 2 — back up + clear the output dir

Everything in `hazard_timeseries_mean_month` is stale-named after this fix
(collapsed intermediates, `anomaly-historic` outputs, window-named orphans from
pre-2025 runs). Move aside, don't rm:

```bash
source_dir=/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data
mv $source_dir/hazard_timeseries_mean_month $source_dir/hazard_timeseries_mean_month_pre_issue26_bak
mkdir -p $source_dir/hazard_timeseries_mean_month/intermediate
```

(Check first with `du -sh` that there is disk headroom for old + new side by side.)

### Step 3 — launch

```bash
bash scripts/r21_rerun.sh          # FORCE_OVERWRITE=1 is the wrapper default
```

**Early kill-gate (first ~2 min of log):** the script now prints
`GCMs per scenario x timeframe` and `Ensemble members ( N ) = ...` before any
extraction. Confirm `N = 18` and 4-token folder names in `Folders included`.
Expect **324 folders** (18 GCM × [16 ssp + 2 historic]) vs the old 80. If the
evenness gate stops it, that's the Step-1 decision — report, don't bypass.

**Runtime:** old 5-GCM run: §2 ≈ 1 h, §3.2/3.3 ≈ 1-2 h each, §3.4 ≈ 9 h/timeframe
pre-kernel. Folder count is 4× and baselines 2× (§3.2+ combos 10 vs 7), so
expect roughly half a day to a day wall-clock. Timestamped section markers are
in the log; per feedback_r3_silent_41_failures check per-section elapsed, not
just exit 0.

### Step 4 — post-run validation (before ANY publish)

```r
# Rscript from project root after run completes
source("R/0_server_setup.R")
library(arrow); library(data.table)
dir <- atlas_dirs$data_dir$hazard_timeseries_mean_month

# 1) intermediates: no collapse, per-GCM historic, both windows
f <- data.table(file=list.files(file.path(dir,"intermediate"), ".parquet$"))
f[, c("scenario","model","timeframe") := tstrsplit(file, "_", keep=1:3)]
stopifnot(f[model=="historic", .N] == 0)
print(f[scenario=="historic", .N, by=.(timeframe)])            # 2 windows, equal N
print(f[, uniqueN(model), by=.(scenario,timeframe)])           # all 18

# 2) canonical ensembles: n_models uniform 18, both baselines, no 5/13/0 residue
for (b in c("1981-2014","1995-2014")) {
  p <- file.path(dir, sprintf("haz_3months_adm_mean_2041-2060_anomaly-%s_ensemble_seasons.parquet", b))
  d <- data.table(read_parquet(p))
  cat(b, ": n_models table:\n"); print(d[, .N, by=n_models])   # expect single row: 18
  stopifnot(d[, all(n_models == 18)])
  cat(b, ": baseline_name:", d[, unique(baseline_name)], "\n") # == the window
}

# 3) the two baselines genuinely differ (mislabel is dead)
d1 <- data.table(read_parquet(file.path(dir,"haz_3months_adm_mean_2041-2060_anomaly-1981-2014_seasons.parquet")))
d2 <- data.table(read_parquet(file.path(dir,"haz_3months_adm_mean_2041-2060_anomaly-1995-2014_seasons.parquet")))
m <- merge(d1[hazard=="PTOT", .(b1=mean(baseline_value)), by=.(iso3,season)],
           d2[hazard=="PTOT", .(b2=mean(baseline_value)), by=.(iso3,season)])
cat("PTOT baseline windows differ in", m[, mean(abs(b1-b2) > 0)]*100, "% of iso3 x season\n")  # expect >0
```

Gate on SHAPE too (feedback_smoke_full_dir_collision): 10 `_seasons`, 10
`_ensemble_seasons`, 10 `_ensemble`, plus the trends set, all mtimes > run
start.

### Step 5 — publish (canonical first, then B and C)

```bash
Rscript scripts/probe_r21_outputs.R             # rewritten for #26: window names, n_models uniformity, CR-119 schema — must exit 0
Rscript scripts/r21_publish_to_s3.R --dry-run   # expect 10 keys: 5 periods x baseline=1995-2014 + 5 x 1981-2014
Rscript scripts/r21_publish_to_s3.R
CONFIRM=  Rscript R/publish_B.R                 # dry-run first
CONFIRM=1 Rscript R/publish_B.R
Rscript R/build_publish_C.R                     # dry-run first
CONFIRM=1 Rscript R/build_publish_C.R
```

Then local-vs-S3 verify (feedback_s3_uploader_no_verify): for each uploaded key,
`aws s3 ls` size == local size; duckdb httpfs `COUNT(*)` + `SELECT DISTINCT
n_models` on the canonical 2041-2060/baseline=1995-2014 key (expect only 18).

### Step 6 — report

Append a dated block at the top of this file: audit table, gate output, per-
section elapsed, validation results, published keys + sizes. Push to develop.
Do NOT create branches (commit directly to develop, own paths only).
