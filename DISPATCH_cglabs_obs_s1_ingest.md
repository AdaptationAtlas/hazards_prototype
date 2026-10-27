# DISPATCH — cglabs — observed pipeline S1: Python env, Kenya smoke, then the full 2005-2025 ingest

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### MACBOOK 2026-10-27 — build the `obs` env, smoke S1 on the Kenya box, then run the full Africa ingest + gates

**What S1 is.** `python/obs/s1_ingest.py` (docstring explains every subcommand). It reads the CDH Hugging
Face Zarr cubes (CHIRTS-ERA5, CHIRPS v3, pinned revisions) and the FAO AgERA5 v2.0 mirror, crops to Africa,
aggregates to 0.25°, and writes daily GeoTIFFs **on the full 1440×400 NEX-GDDP lattice** in the layout the
upstream index scripts read. Invariants are enforced while writing (plausible ranges, identical NaN mask
every day, every requested day present in the source); a violation stops the run and is recorded in
`manifests/s1_failed_*.csv`. Resumable; revision-aware skip. Macbook: selftest 17/17, Kenya 2015 smoke
1,825 files, 0 failures, validate PASS; Tmax matches the CHC GeoTIFF within 0.022 °C; rsds equals the exact
area-weighted mean.

**Rules.**
- Writes go **only** under `OBS_ROOT`, a new directory under `{common_data}`. Nothing under the live
  NEX-GDDP trees. Suggested: `OBS_ROOT={common_data}/atlas_obs_hazards` (full) and
  `{common_data}/atlas_obs_hazards_smoke` (smoke). Say what you used.
- Background everything; logs in `logs/obs_s1_<step>_<STAMP>.log`.
- `env | grep -E 'FORCE_OVERWRITE|DAYS_CHECK|AVAIL_SEED'` empty before every launch.
- Public repo: no IPs, hostnames, mount identifiers or usernames in the RESPONSE.

#### I0 — Python env (no root; per-user)
```bash
cd <repo> && git pull --ff-only
# EITHER uv (if present)            OR conda, conda-forge only, ONE fetch thread (PASCAL saw silent failures otherwise)
uv venv -p 3.12 ~/.venvs/obs && VIRTUAL_ENV=~/.venvs/obs uv pip install -r python/obs/requirements.txt
#   conda create -p ~/.conda/envs/obs -c conda-forge --override-channels python=3.12 "zarr>=3.1" xarray numpy fsspec aiohttp requests "rasterio>=1.4"   # with CONDA_FETCH_THREADS=1
PY=<env>/bin/python
$PY -c "import rasterio, zarr, xarray; print(rasterio.__gdal_version__, zarr.__version__, xarray.__version__)"
$PY python/obs/s1_ingest.py selftest        # must end ALL S1 SELFTEST ASSERTIONS PASSED
```
**Verify the env exists** (`test -x $PY`) rather than trusting the installer's exit code.

#### I1 — Kenya smoke (about 15 min)
```bash
SM={common_data}/atlas_obs_hazards_smoke; mkdir -p $SM
$PY python/obs/s1_ingest.py link    --out-root $SM --common-data {common_data} --smoke
nohup $PY python/obs/s1_ingest.py ingest  --smoke --out-root $SM --years 2015 --rh both > logs/obs_s1_smoke_$STAMP.log 2>&1 &
$PY python/obs/s1_ingest.py validate --smoke --out-root $SM --years 2015                     # PASS
$PY python/obs/s1_ingest.py validate --smoke --out-root $SM --years 2015 --vars hurs                                  # variant (i), main token
$PY python/obs/s1_ingest.py validate --smoke --out-root $SM --years 2015 --vars hurs --token chirps-chirts-agera5-rh15  # variant (ii)
```
Report: wall-clock per variable (the log prints block/day timings), `manifests/` contents, validate output.

#### I2 — S2 on the smoke output (proves the hand-off to the unchanged upstream scripts)
```bash
cd <repo>/hazards_upstream/R/04_indices
export COMMON_DATA=$SM GCMS=chirps-chirts-agera5 SCENARIO=historical YRS=2015 DAYS_CHECK=strict AVAIL_SEED_MONTHS=2015-01
nohup Rscript fast_calc_NDWS.R  > ../../../logs/obs_s2_smoke_ndws_$STAMP.log 2>&1 &
nohup Rscript calc_NTx.R        > ../../../logs/obs_s2_smoke_ntx_$STAMP.log  2>&1 &
nohup Rscript calc_PTOT.R       > ../../../logs/obs_s2_smoke_ptot_$STAMP.log 2>&1 &
GCMS=chirps-chirts-agera5-rh15 nohup Rscript calc_THI.R > ../../../logs/obs_s2_smoke_thi_rh15_$STAMP.log 2>&1 &
unset COMMON_DATA GCMS SCENARIO YRS DAYS_CHECK AVAIL_SEED_MONTHS
```
Outputs land in `$SM/nex-gddp-cmip6_indices/historical_<token>/`. Report per index: files written (expect 12
months; NTx 31 dirs × 12), zero `WARN`/`daily inputs missing`, and over the Kenya box:
- NDWS monthly mean 0-31 and **not** saturated (an un-converted radiation unit would make every day a stress day);
- **PET sanity**: the log or a quick `terra` read of ETMAX is not available from NDWS outputs — instead check
  NDWS Kenya-box mean per month is in the range 2-28 days and varies seasonally;
- NTx35 January Kenya mean ≈ 0-10 days; PTOT monthly totals plausible (0-400 mm).
**Stop and describe** if any script stops, or NDWS is 0 or ≥ 28 everywhere.

#### I3 — full Africa ingest (about 1 h; two processes)
```bash
OR={common_data}/atlas_obs_hazards; mkdir -p $OR
$PY python/obs/s1_ingest.py link --out-root $OR --common-data {common_data}
nohup $PY python/obs/s1_ingest.py ingest --out-root $OR --years 2005:2025 --vars pr,tasmax,tasmin --rh none > logs/obs_s1_cubes_$STAMP.log 2>&1 &
nohup $PY python/obs/s1_ingest.py ingest --out-root $OR --years 2005:2025 --vars rsds --rh both --workers 8 > logs/obs_s1_agera5_$STAMP.log 2>&1 &
```
Then `validate --out-root $OR --years 2005:2025` for the main token (4 vars + hurs) and for `--token
chirps-chirts-agera5-rh15 --vars hurs`. Report wall-clock, file counts (expect 7,670 per variable), `du -sh`,
the manifest JSON `libs` block, and `s1_failed_*` (expect none).

**Invariants:** validate PASS; 7,670 files per variable; no `s1_failed_*.csv` rows; domain-mean Tmax over
Africa per day between 20 and 40 °C.

#### I4 — AgERA5 v1.0 vs v2.0 radiation (PLAN §4 gate; the node has v1.0 dailies 1981-2022)
On the native 0.1° grid over the Africa window, for every 15th of the month 2006-2022 (204 days):
`{common_data}/ecmwf_agera5/<solar var dir>/…_final-v1.0.nc` (J m-2 d-1) vs the FAO v2.0 COG for the same
day (`/vsicurl/https://storage.googleapis.com/fao-gismgr-c3s-data/DATA/C3S/MAPSET/AGERA5-SRF/C3S.AGERA5-SRF.<date>.tif`).
Report per year: mean bias (MJ), 99th percentile of |diff|, max |diff|, and the linear trend of the yearly
mean bias. **Invariants:** |mean bias| < 0.1 MJ, p99 |diff| < 0.5 MJ, no trend beyond ±0.02 MJ/yr. A
systematic difference means v1.0 and v2.0 are not the same radiation and the plan's "one series" claim
needs revisiting. Script it in R or Python; keep it in `scripts/obs_agera5_v1_v2_gate.*` and commit it.

#### Then
1. Prepend `### RESPONSE` with I0-I4.
2. Commit **only** this file and the I4 script; push; verify on `origin/develop`.
