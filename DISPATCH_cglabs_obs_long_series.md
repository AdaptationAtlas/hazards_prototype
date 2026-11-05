# DISPATCH — cglabs — observed hazards, long series 1981–2025 (for trends, normals and anomalies)

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### MACBOOK 2026-11-05 — X0–X6: same pipeline, 1981–2025, in a NEW root; nothing published

**Why.** Pete decided (2026-11-05) to extend the observed baseline back to the start of the source data, so that:
- **trends** use the full series (43 seasons, 1982–2024, instead of 19);
- **anomalies** can be taken against the WMO normal **1991–2020**;
- observed **1995–2014** can be compared like-for-like with the NEX-GDDP historical baseline.

The CDH cubes start on 1981-01-01, so the data years are **1981–2025**. One spin-up year gives seasons **1982–2024**
(timeframe label `1982-2025`). The published 2006–2025 product is not touched and stays reproducible from `$OR`.
The trend/anomaly products are a later block; this one only builds the inputs.

**Rules.** Code is unchanged; this is the same S1 → S2 → S3-S5 chain with new years and a new root.
- Write only under **`OL={common_data}/atlas_obs_hazards_1981`**. Never under `$OR`, `$SM` or the NEX trees.
- Every long step runs in the background with `nohup`, logging to `logs/obs_long_<step>_$STAMP.log` plus a `.pid`. Track it with
  `while kill -0 $(cat x.pid) 2>/dev/null; do sleep 60; done` or the log tail, not `wait`.
- `env | grep -E 'FORCE_OVERWRITE|DAYS_CHECK|AVAIL_SEED|R3_|ATLAS_CLIMDAT'` must be empty before each launch.
- Obs python: `env -u GDAL_DRIVER_PATH PROJ_DATA=<env>/share/proj GDAL_DATA=<env>/share/gdal $PY …` (or `pyobs`).
- No R/3 and no publish in this dispatch.

#### X0 — pull, selftests
```bash
cd $REPO && git pull --ff-only && git log -1 --format='%h %s'
$PY python/obs/s1_ingest.py selftest        # 23
$PY python/obs/soil_fill.py selftest        # 9
Rscript R/obs/s3_s5_obs_hazards.R --selftest  # 24
```

#### X1 — new root: link, then soil fill (same fill as #56)
```bash
mkdir -p $OL
$PY python/obs/s1_ingest.py link --out-root $OL --common-data {common_data}
$PY python/obs/soil_fill.py fill --src-dir {common_data}/atlas_hazards/soils --out-root $OL \
    --check-pr $OR/nex-gddp-cmip6/pr/historical/chirps-chirts-agera5/pr_2015-06-15.tif > logs/obs_long_soilfill_$STAMP.log 2>&1
```
Expect the same numbers as L1 (filled 638,524 cells per layer; land without soil 2,494 → 49). The shared soils' md5
must be unchanged.

#### X2 — S1 ingest 1981–2025 (two processes, as I3)
```bash
nohup $PY python/obs/s1_ingest.py ingest --out-root $OL --years 1981:2025 --vars pr,tasmax,tasmin --rh none > logs/obs_long_cubes_$STAMP.log 2>&1 &
nohup $PY python/obs/s1_ingest.py ingest --out-root $OL --years 1981:2025 --vars rsds --rh both --workers 8  > logs/obs_long_agera5_$STAMP.log 2>&1 &
```
Then `validate --out-root $OL --years 1981:2025` for the main token, and `--vars hurs --token chirps-chirts-agera5-rh15`.
Expect 16,436 days per variable.

Report:
- days per variable;
- wall-clock;
- the domain-mean lines;
- digests;
- **every FLAG line**, counted per year and per variable. Tmax repaired above 57 °C, Tmin above 43 °C, and AgERA5 RH
  with fewer than 5 hours.

For the overlap 2005–2025, the daily files must be **byte-identical** to `$OR`: same code, same pinned sources.
State the per-variable md5-list digest for 2005–2025 in both roots.

**Stop on:**
- any S1Stop;
- any day with more than 100 repaired native pixels;
- an overlap mismatch.

The flagged-day *count* is not a stop by itself: Tmin had 222 days in 2005–2025, so expect roughly twice that.

#### X3 — S2, all five scripts, seeded at the first data year
```bash
cd $REPO/hazards_upstream/R/04_indices
export COMMON_DATA=$OL SCENARIO=historical YRS=1981:2025 DAYS_CHECK=strict AVAIL_SEED_MONTHS=1981-01
GCMS=chirps-chirts-agera5      nohup Rscript fast_calc_NDWS.R  > $REPO/logs/obs_long_s2_ndws_$STAMP.log 2>&1 &
GCMS=chirps-chirts-agera5      nohup Rscript fast_calc_NDWL0.R > $REPO/logs/obs_long_s2_ndwl0_$STAMP.log 2>&1 &
GCMS=chirps-chirts-agera5      nohup Rscript calc_NTx.R        > $REPO/logs/obs_long_s2_ntx_$STAMP.log 2>&1 &
GCMS=chirps-chirts-agera5      nohup Rscript calc_THI.R        > $REPO/logs/obs_long_s2_thi_$STAMP.log 2>&1 &
GCMS=chirps-chirts-agera5-rh15 nohup Rscript calc_THI.R        > $REPO/logs/obs_long_s2_thi_rh15_$STAMP.log 2>&1 &
unset COMMON_DATA SCENARIO YRS DAYS_CHECK AVAIL_SEED_MONTHS
```
Expect **540** monthly files per index, 0 WARN and 0 `daily inputs missing`. NDWS took 50 min and NDWL0 83 min for 21
years, so expect about 2–3.5 h here.

#### X4 — S3-S5, both commands
```bash
nohup Rscript R/obs/s3_s5_obs_hazards.R --obs-root $OL --token chirps-chirts-agera5 --years 1981:2025 --spinup 1 \
      --ggcmi-dir $NWD/Data/ggcmi > logs/obs_long_s35_$STAMP.log 2>&1 &
nohup Rscript R/obs/s3_s5_obs_hazards.R --obs-root $OL --token chirps-chirts-agera5 --thi-token chirps-chirts-agera5-rh15 \
      --years 1981:2025 --spinup 1 --sets livestock --ggcmi-dir $NWD/Data/ggcmi > logs/obs_long_s35_rh15_$STAMP.log 2>&1 &
```
Expect:
- `seasons kept=43 (1982..2024) timeframe=1982-2025`;
- 129 + 30 bundles under `$OL/Data/hazard_risk/ggcmi-crop/`. They are not used yet and will not be published from here;
- the seasonal stacks under `$OL/Data/obs_seasonal/<token>_1982-2025/<window>/` with **43 layers each**.

#### X5 — overlap gate against the published run (judge the new product against its own inputs, then against `$OR`)
For the 19 seasons 2006–2024, compare `$OL/Data/obs_seasonal/chirps-chirts-agera5_1982-2025/<window>/<index>_<stat>.tif`
with the same layer in `$OR/Data/obs_seasonal/chirps-chirts-agera5_2006-2025/…`, for every window and index.
- **NTx35 / THI_max:** the months are computed independently, so they must be **identical**. Report max |diff|;
  anything above 0 is a stop.
- **NDWS / NDWL0:** these carry soil-water state month to month, and the run now starts from a 1981 seed instead of
  2005. The difference should be largest in the first season and vanish within a year or two. Report max |diff| per
  start year 2006…2024. **It should be 0, or float noise, from about 2007 on.** If a difference persists beyond 2008,
  that is a stop: the state is not converging.
- `PTOT` (if present): identical.

#### X6 — report
Prepend `### RESPONSE` with X0–X5, the disk use of `$OL` (`du -sh`), and the total wall-clock. Commit only this file,
push, verify, delivery line. **Keep `$OL`.** The trend/normal/anomaly step reads it next.
