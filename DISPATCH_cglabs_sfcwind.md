# Dispatch: fetch + preprocess sfcWind, then validate calc_PET (FAO-56 PM)

**Goal:** get `sfcWind` (near-surface wind) into the pipeline so FAO-56 Penman-Monteith ET₀ (`calc_PET.R`) can run on real data. sfcWind confirmed available for all 18 Atlas GCMs × scenarios in NEX-GDDP-CMIP6 (r1i1p1f1). Head = origin/develop `2f94535`.

## Mechanism (no new tooling, no credentials)
`download_manual_nex_gddpCMIP6_v2_0.R` fetches from the **public AWS Open Data** bucket over HTTPS (`nex-gddp-cmip6.s3.us-west-2.amazonaws.com`, `curl`, anonymous, free egress). Now parallel (`DL_WORKERS` default 32) with per-file retry/backoff (`DL_TRIES` default 3). S3 won't throttle — only the local link matters, and cglabs has good bandwidth.

## Pull
```bash
cd <hazards_prototype>/hazards_upstream/R
git checkout develop && git pull        # head 2f94535; DO NOT create branches
export COMMON_DATA=<your real data root>
```

## 1. Download sfcWind (historical + future)
The download var list + filter now include sfcWind. **Delete the stale manifest CSV first** so sfcWind rows get URL-probed (otherwise it reads the cached manifest with no sfcWind):
```bash
rm -f "$COMMON_DATA"/nex-gddp-cmip6_raw/cmip6_baseline_v2.0_files_to_download.csv \
      "$COMMON_DATA"/nex-gddp-cmip6_raw/cmip6_future_v2.0_files_to_download.csv
# historical (1995-2014 needed; script probes 1981-2014)
SCENARIO=historical DL_WORKERS=32 Rscript 01_download_data/download_manual_nex_gddpCMIP6_v2_0.R
# future (only if you need future PET; scope SSPs by editing line ~42 to c('ssp245','ssp585') if not all 4)
SCENARIO=future     DL_WORKERS=32 Rscript 01_download_data/download_manual_nex_gddpCMIP6_v2_0.R
```
Writes raw `.nc` to `"$COMMON_DATA"/nex-gddp-cmip6_raw/sfcWind2/<scenario>/<gcm>/`.

⚠️ **Size-skip check:** the script treats files `< 1e8` bytes (100 MB) as incomplete and retries. If sfcWind annual `.nc` are genuinely smaller than 100 MB, this loops forever — after the first few land, check actual size (`ls -la .../sfcWind2/historical/ACCESS-ESM1-5/`) and lower the `1e8` threshold in the script if needed.

## 2. Stage raw to the preprocess input mount
Download writes to `…/nex-gddp-cmip6_raw/sfcWind2/…` (under COMMON_DATA), but preprocess reads from a hardcoded `/home/jovyan/shared-data-premium/nex-gddp-cmip6_raw/<var>/…` (no `2`). Move/sync (drop the `2`):
```bash
for sc in historical ssp245 ssp585; do
  rsync -a "$COMMON_DATA"/nex-gddp-cmip6_raw/sfcWind2/$sc/ \
           /home/jovyan/shared-data-premium/nex-gddp-cmip6_raw/sfcWind/$sc/ 2>/dev/null
done
```
(This download→preprocess path mismatch affects all vars, not just sfcWind — flagged for the portability cleanup, not fixing here.)

## 3. Preprocess sfcWind → daily tifs
`preprocess_nex-gddp-cmip6_daily_data_v2_0.R` `vrs` now includes sfcWind (no unit conversion — m/s passes through like hurs). It writes `"$COMMON_DATA"/nex-gddp-cmip6/sfcWind/<scenario>/<gcm>/sfcWind_<date>.tif`. (Set its `scenario` at the bottom; it also lists `pr` — already-present pr tifs are skipped.)
```bash
Rscript 02_preprocess_data/preprocess_nex-gddp-cmip6_daily_data_v2_0.R
```

## 4. Validate calc_PET on one GCM/month
```bash
GCMS=ACCESS-ESM1-5 SCENARIO=historical YRS=1995:1995 MONTHS=1 \
  Rscript 04_indices/calc_PET.R 2>&1 | tee /tmp/pet_smoke.log
```
PASS = `nex-gddp-cmip6_indices/historical_ACCESS-ESM1-5/PET/PET-1995-01.tif` written, no path/object errors, values physically plausible (monthly PET ~30–250 mm depending on region/season). Spot-check: `Rscript -e 'library(terra); summary(values(rast("…/PET-1995-01.tif")))'`.

## Report back (edit this file + commit)
- download: # files, any failures from the loud end-of-run report, actual sfcWind file size (re: the 1e8 threshold)
- staging + preprocess: sfcWind tifs present under `nex-gddp-cmip6/sfcWind/…`?
- calc_PET smoke: pass/fail + PET value range
Do not push code fixes without flagging the diff first.
