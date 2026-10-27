# DISPATCH — pascal — rangeland FPAR track: full Africa ingest (2005-2025) + S6 rangeland drought V1

**Append-only; newest block on top. Prepend `### RESPONSE — pascal` to answer.**

### MACBOOK 2026-10-27 — run the rangeland track on PASCAL while CGlabs runs the crops chain

**What this is.** PLAN_obs_hazard_pipeline.md §3 (decision D4): JRC ASAP FPAR dekads → 0.25° rangeland-weighted
means (`python/obs/fpar_ingest.py`) → per-season cumulative FPAR anomaly, percentile classes and an R/3 livestock
bundle (`R/obs/s6_rangeland_fpar.R`). Both have synthetic selftests; the Kenya smoke of the ingest passed on the
macbook. This is the first full-Africa run. It is independent of CGlabs' crops chain; the bundles get merged there
later (S6 is cheap to re-run once the THI/NDWL0 stacks exist).

**Rules.** Writes only under a new root `FR=<bulk>/atlas_obs_hazards_pascal` (absolute; say what you used). Read-only
otherwise. Background with logs in `logs/obs_fpar_<step>_<STAMP>.log`; `ionice -c3 nice -n19` on the bulk mount.
JRC served this node at ~1.75 MB/s in the S0 probe: the Africa window is ~50 GB of range reads, so expect ~8 h for the
ingest; it is resumable (re-run the same command; written dekads are skipped). Large profile or SSH; say which.
Public repo: no usernames, hostnames, IPs, mount identifiers.

#### F0 — env + selftests
```bash
cd <repo> && git pull --ff-only && git log -1 --format='%h %s'
PY=~/.conda/envs/obs/bin/python          # the obs env (DISPATCH_pascal_obs_env.md); the S1 repro proved rasterio/requests work in it
$PY python/obs/fpar_ingest.py selftest            # ALL FPAR SELFTEST ASSERTIONS PASSED (7)
Rscript R/obs/s6_rangeland_fpar.R --selftest     # ALL S6 SELFTEST ASSERTIONS PASSED (11)   [terra 1.8.93 / GDAL 3.4.3 is fine: GeoTIFF only]
```
**Stop** on any failure.

#### F1 — full ingest (background, resumable)
```bash
FR=<bulk>/atlas_obs_hazards_pascal; mkdir -p $FR
nohup ionice -c3 nice -n19 $PY python/obs/fpar_ingest.py ingest --out-root $FR --years 2005:2025 --workers 4 --min-age 90 \
      > logs/obs_fpar_ingest_$STAMP.log 2>&1 &
```
Watch for `FAIL` lines; the run stops on the first (range or NaN-mask invariant) and records it in
`$FR/manifests/fpar_manifest_*.csv` as a `FAIL` row. Static layers (rangeland fraction, phenology modes) are written
first; the phenology mode is slow (up to 72 passes over the window) — report its wall-clock separately.
Then:
```bash
$PY python/obs/fpar_ingest.py validate --out-root $FR --years 2005:2025 --min-age 90     # PASS; 756 dekads (2025 dekads younger than 90 d are skipped by design)
```
Report: dekads written, wall-clock, MB/s achieved, `du -sh $FR/Data/obs_fpar`, the validate domain-mean line, and the
static layers' `gdalinfo -stats` (rl_frac min/mean/max; pheno_n distribution of 1 vs 2; pheno_s1/e1 ranges).

#### F2 — S6 on the full window
```bash
nohup Rscript R/obs/s6_rangeland_fpar.R --obs-root $FR --years 2005:2025 --token chirps-chirts-agera5 --rl-min 0.1 \
      > logs/obs_fpar_s6_$STAMP.log 2>&1 &
```
(No THI/NDWL0 stacks exist on this node: the heat and wet slots are 0 by design; the log says so.) Report the log's
`season 1/season 2`, `baseline P(moderate)` (incl. the **tied-quantile cell count**) and `consecutive-failure` lines,
and `ls $FR/Data/hazard_risk/ggcmi-crop/*FPARz*_int.tif | wc -l` (expect 30).

#### F3 — diagnostics (PLAN §3b, §3 V1): report, do not gate
Over rangeland cells (rl_frac ≥ 0.1), from `$FR/Data/obs_fpar/seasonal/2005-2025/`:
1. `z_year.tif`: per start year the domain-mean z and the share of cells with z ≤ Q20 — a time series of 20 numbers
   (which years were bad continent-wide; 2010-11 Horn of Africa and 2016-17 should stand out).
2. `thresholds_z.tif`: distribution of q20/q10/q05 (should sit near −0.84/−1.28/−1.64 if z were normal; report the medians).
3. `consecutive_failure.tif`: domain mean per year.
4. The share of rangeland cells with **constant or near-zero FPAR** (sd of the dekad series < 1): these are the tie
   candidates and hyper-arid cells; report the count and where they cluster (Sahara margin? Namib?).
5. Cross-check against the Kenya FEWS rangeland WRSI (already on S3, `eastafrica-wrsi-fews`): for the Kenya box and
   seasons 2005-2025, the correlation between the season-1 cumulative FPAR anomaly and the end-of-season rangeland
   WRSI (MAM window) over rangeland cells — one number plus a scatter description. **A low correlation is a finding, not a stop.**

**Stop and describe** on: a selftest failure; a `FAIL` row in the ingest manifest; validate FAIL; S6 stopping; more than
1 % tied cells (S6 prints a WARN).

#### Then
Prepend `### RESPONSE — pascal` (F0-F3), commit only this file, push, verify on `origin/develop`. Keep `$FR`.
