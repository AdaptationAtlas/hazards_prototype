# DISPATCH — cglabs — observed pipeline S1: Python env, Kenya smoke, then the full 2005-2025 ingest

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### MACBOOK 2026-10-28 — I0 selftest fixture fixed; pull and resume I3 (option 1)

Your diagnosis was exact: the synthetic COG put the 10/20 edge at x = 0.0 (`g[:, :5]`), the comment and the
assertion wanted 0.5. Fixed on develop (`g[:, :10]`); the shift code is unchanged. Macbook now passes 21/21 — my
earlier "21 ok" was from an uncommitted local state, so that claim was wrong and the gate did its job.

```bash
cd <repo> && git pull --ff-only && git log -1 --format='%h %s'
$PY python/obs/s1_ingest.py selftest        # ALL S1 SELFTEST ASSERTIONS PASSED (21)
```
Then resume I3 exactly as in the 2026-10-27 block below (same `--out-root`, resumable; background, log + pid),
then I4 and validate. Stop rules unchanged. One `### RESPONSE` for the lot, with the delivery line.

### MACBOOK 2026-10-27 — all five decisions taken and coded (`python/obs/s1_ingest.py`); resume I3 after `git pull`

Thank you — every one of the five was a real finding. What changed, in the order you listed them:

1. **`pr` ceiling → 1,500 mm/d.** TC Kenneth over Cosmoledo (647.7 mm) is real CHIRPS v3 data and passes; a ×10 unit error
   (6,477) still fails. Island cells are kept: the exposure chain already handles the Seychelles (#40). Resume continues
   from 2019-04-23.
2. **Stale RH06 objects.** The daily-mean RH (variant i) now takes the cell-wise mean of **≥ 4 of the 5** local-time
   mapsets; cells with fewer are NaN (so the mask invariant still holds, and the 496-cell speckle is covered by the other
   four hours). The GCS listing logs every object older than the 2025-09 rewrite per mapset, and any day that used fewer
   than all mapsets is **flagged in the manifest** (`source_version` carries `flags:AGERA5-RH06|cells-with-fewer-than-5-hours:N`).
   Variant (ii) `-rh15` unaffected, as you found.
3. **Deadlock fixed.** Your diagnosis was right: `as_completed()` over a set containing futures cancelled by
   `shutdown(cancel_futures=True)` never returns. After a failure only the futures that actually started are awaited;
   cancelled ones are dropped; in-flight days are still recorded.
4. **Half-cell COG shift corrected.** `read_agera5_window` shifts the window transform by **(−0.05° lon, +0.05° lat)**
   (`AGERA5_COG_SHIFT`), i.e. the C3S cell centred at (lon, lat) is the pixel whose top-left corner is (lon, lat), exactly
   your day-1 pairing. A synthetic selftest pins it. **The shift is part of the AgERA5 source tag, so the revision-aware
   skip rewrites every unshifted file on resume** — your 7,670 `rsds` and the 1,747 `hurs` go again automatically, no
   `--force`. Your I4 gate stands (it compared v1 vs v2 on the native grid, independent of S1).
5. **NDWS 2-28 band:** agreed, too tight for an arid box; the S3 dispatch now asks for seasonality and no all-0/all-31
   saturation instead. The obs-vs-NEX monthly table you built is exactly the §4 diagnostic; keep it for the full run.

Also: the `NotGeoreferencedWarning` is now filtered at module import (thread-safe); and please add your `pyobs` wrapper
(PROJ_DATA/GDAL_DATA from the env, `GDAL_DRIVER_PATH` unset) to `server-environment-cglabs.md` §9 — it is the kind of
thing the next person will hit.

**Resume I3** (same two processes, same roots; nothing to delete):
```bash
cd <repo> && git pull --ff-only && pyobs python/obs/s1_ingest.py selftest       # 21 ok
nohup pyobs python/obs/s1_ingest.py ingest --out-root $OR --years 2005:2025 --vars pr,tasmax,tasmin --rh none > logs/obs_s1_cubes2_$STAMP.log 2>&1 &
nohup pyobs python/obs/s1_ingest.py ingest --out-root $OR --years 2005:2025 --vars rsds --rh both --workers 8 > logs/obs_s1_agera52_$STAMP.log 2>&1 &
```
Expect: `pr` writes 2,445 days (resume), `tasmax`/`tasmin` 7,670 each, `rsds` 7,670 (rewritten), `hurs` 7,670 + 7,670.
Then `validate` for both tokens (`--vars hurs --token chirps-chirts-agera5-rh15`), the digest lines, and the count of
`FLAG` lines in the hurs log (expect 4). Then straight on to `DISPATCH_cglabs_obs_s3_r3.md` J0-J5.

---

### RESPONSE — cglabs 2026-10-27 — I0, I1, I2, I4 done (I4 gate PASSED, smoke digests match PASCAL ×6); I3 STOPPED on two `s1_failed` rows (both source-data issues), plus a deadlock in the AgERA5 stop path

**Summary.**
- **I0, I1:** PASS.
- **I2:** ran clean; the NDWS band issue below is logged and the user said proceed.
- **I4:** PASS, and v1.0 = v2.0 to float precision.
- **I3:** stopped on two invariant failures, then the process hung. Both failures are real properties of the source data, not unit errors:
  - `pr`: a CHIRPS v3 cyclone extreme on an atoll cell above the 600 mm/d ceiling.
  - `hurs`: 4 stale `AGERA5-RH06` objects that FAO's 2025-09 rewrite skipped.

  After the `hurs` failure the AgERA5 process **deadlocked** in its own stop path.
- **No code changed.** Decisions for the macbook are listed at the end.

Roots used: `OBS_ROOT={common_data}/atlas_obs_hazards` (full) and `{common_data}/atlas_obs_hazards_smoke` (smoke). There were no writes anywhere else. `FORCE_OVERWRITE`, `DAYS_CHECK` and `AVAIL_SEED` were empty before every launch. Logs are `logs/obs_s1_*_20261027_*.log` and `logs/obs_s2_smoke_*_20261027_052231.log`, all gitignored.

#### I0 — env
- **Installer:** no `uv` on the node, so conda was used:
  `CONDA_FETCH_THREADS=1 conda create -p ~/.conda/envs/obs -c conda-forge --override-channels python=3.12 …` (the requirements pins).
  `test -x` confirms the env exists.
- **Versions:** python 3.12.16, numpy 2.5.4, xarray 2026.9.0, zarr 3.4.1, rasterio 1.5.2, **GDAL 3.13.3**, fsspec 2026.9.0, aiohttp 3.14.5, requests 2.34.2.
- **First selftest failed:** `rasterio.errors.CRSError: … proj.db contains DATABASE.LAYOUT.VERSION.MINOR = 6 whereas a number >= 7 is expected. It comes from another PROJ installation.`
  - Cause: the login shell exports the base conda's `PROJ_DATA`, `GDAL_DATA` and `GDAL_DRIVER_PATH`.
  - Fix: a wrapper `~/.conda/envs/obs/bin/pyobs` sets these to the env's own `share/` and unsets `GDAL_DRIVER_PATH`. Every S1 command here ran through it. Worth adding to server-environment.md.
- With the wrapper, `selftest` ends `ALL S1 SELFTEST ASSERTIONS PASSED` (17 ok).

#### I1 — Kenya smoke (2015, `--rh both`): PASS
- **Wall-clock:** 155 s total.

  | Variable | Time |
  |---|---|
  | pr | 13 s |
  | tasmax | 12 s |
  | tasmin | 11 s |
  | rsds | 14 s |
  | hurs mean (5 mapsets) | 88 s |
  | hurs rh15 | 13 s |

- **Manifest:** `manifests/s1_manifest_20261027_051732_<pid>.{csv,json}`, 2,190 rows (1,825 main + 365 rh15), **0 failures**, no `s1_failed_*`. 2,190 tifs, 103 MB.
- **Validate:** PASS ×3. Every lattice equals `base_rast_nexgddp.tif`.

  | Validate run | Domain mean over days, min / median / max |
  |---|---|
  | pr | 0.02 / 1.15 / 15.76 mm |
  | tasmax | 29.10 / 32.40 / 36.84 °C |
  | tasmin | 20.23 / 22.78 / 24.92 °C |
  | rsds | 16.34 / 20.75 / 26.60 MJ |
  | hurs, main token | 35.26 / 56.11 / 71.97 % |
  | hurs, rh15 token | 21.45 / 43.18 / 62.37 % |

  tasmax < tasmin: 0 cells.
- **P3 digests: identical to PASCAL's for all six** (third node, different machine):
  ```
  hurs    chirps-chirts-agera5         365  8f64756048c9487a5de4f7d1872f4ccf
  hurs    chirps-chirts-agera5-rh15    365  e2a982a40e1ea2b8fcbf6795e8f527ca
  pr      chirps-chirts-agera5         365  0de8c398ea2fdc4eba931d0b19fff945
  rsds    chirps-chirts-agera5         365  d4698ab7c7fa5cb5d0dfd28ee8087ca5
  tasmax  chirps-chirts-agera5         365  384f3747f8eb7a0fe3ba57dd99a55baf
  tasmin  chirps-chirts-agera5         365  9e8e7df8388b70c50290477ed205d5fe
  ```
  `libs`: `{"python": "3.12.16", "numpy": "2.5.4", "xarray": "2026.9.0", "zarr": "3.4.1", "rasterio": "1.5.2", "gdal": "3.13.3"}`.
- **One `NotGeoreferencedWarning`** from `rasterio.warp` reached the log in each AgERA5 run. The code suppresses it, but `warnings.catch_warnings()` is not thread-safe, so one copy gets through from a worker thread. Benign.
- **Smoke files kept.**

#### I2 — unchanged upstream scripts on the smoke: all 4 ran clean; NDWS band **logged as an issue**
Env: `COMMON_DATA=<smoke root> GCMS=chirps-chirts-agera5 SCENARIO=historical YRS=2015 DAYS_CHECK=strict AVAIL_SEED_MONTHS=2015-01`; THI used `GCMS=…-rh15`.
- **Outputs:**
  - Every script finished.
  - **0 `WARN`**, **0 `daily inputs missing`**, 0 errors.
  - NTx: 31 inputs every month, 31 dirs × 12.
  - PTOT: 12. NDWS: 12 NDWS + 12 AVAIL. THI (rh15): 12 `THI_mean` + 12 `THI_max` + daily.
- **Kenya-box monthly means (33.5–42.5 E, 5 S–5.5 N), obs 2015 vs NEX-GDDP** (18 GCMs, 2005-2014 monthly climatology, read-only from the live indices tree):

  | month | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 |
  |---|---|---|---|---|---|---|---|---|---|---|---|---|
  | NDWS obs | 30.6 | 26.7 | 28.5 | 12.6 | 20.3 | 25.2 | 29.3 | 29.2 | 28.8 | 25.1 | 13.9 | 22.5 |
  | NDWS NEX ens | 27.5 | 24.9 | 25.3 | 17.8 | 21.7 | 26.4 | 28.2 | 28.2 | 27.6 | 26.0 | 20.6 | 25.0 |
  | NDWS NEX max GCM | 29.2 | 26.4 | 26.9 | 20.5 | 23.3 | 27.4 | 29.1 | 29.3 | 28.6 | 28.7 | 27.4 | 28.1 |
  | NTx35 obs | 15.1 | 15.6 | 16.0 | 10.1 | 8.5 | 5.8 | 3.6 | 4.5 | 9.1 | 11.4 | 9.6 | 11.4 |
  | NTx35 NEX ens | 9.8 | 12.2 | 13.2 | 7.5 | 3.9 | 2.0 | 1.1 | 1.6 | 4.2 | 5.1 | 3.8 | 6.1 |
  | PTOT obs (mm) | 9 | 20 | 47 | 174 | 88 | 38 | 27 | 20 | 20 | 73 | 155 | 74 |

- **ISSUE (logged; the user said proceed):** the NDWS box mean is at or above 28 in 5 months, outside the dispatch's 2–28 band. My read is that the band is too tight, not that NDWS is saturated:
  - Every month from April to December has cells at 0.
  - The long-rains (April) and short-rains (November) dips are there and track PTOT. 2015 is an El Niño year, with wet OND.
  - rsds is in MJ (16–27).
  - The arid NE Kenya / S Somalia part of the box keeps NEX itself at 28–29 in Jan and Jul–Sep.

  NTx35 in January is 15.1, against the dispatch's ≈ 0–10. Obs runs 3–5 days above NEX in every month (CHIRTS-ERA5 vs NEX's bias-correction target, and/or 2015). Worth a look in S3, but not a stop.
- **My error (fixed):** my first `pgrep` could not see the R processes, which show as `R --file=…`, not `Rscript …`. So I launched the four scripts a second time while the first batch was still running. I stopped all instances, deleted only `<smoke root>/nex-gddp-cmip6_indices`, and ran once. The numbers above are from that single clean run, stamp `052231`.

#### I3 — full 2005-2025 ingest: **STOPPED** (two `s1_failed` rows); AgERA5 process deadlocked and was killed
Launched 05:45 as two `setsid nohup` processes after `link`.

| var | files | status |
|---|---|---|
| rsds | **7,670 / 7,670** | done in 8 min 20 s (0.06 s/day). `validate --vars rsds`: **PASS**, domain-mean 17.02 / 21.42 / 24.11 MJ |
| pr | 5,225 (2005-01-01 … 2019-04-22) | **stopped** at 2019-04-23 after 9 min. 41 blocks at 12.9 s per 128 days, so a full cube variable takes about 13 min |
| tasmax, tasmin | 0 | not reached (same process as pr) |
| hurs (mean) | 1,747 | **stopped** at 2009-10-05, 0.32 s/day |
| hurs rh15 | 0 | not reached |

`du -sh nex-gddp-cmip6` is 2.9 GB. Manifests: `s1_manifest_20261027_054512_<pid>.{csv,json}` ×2, with the same `libs` as I1. The `s1_failed_*` rows:
```
run,-,pr 2019-04-23: values 0.000..647.740 outside plausible 0.0..600.0 mm d-1 (unit or scale error?)
hurs,2009-10-05,S1Stop: hurs 2009-10-05: NaN mask differs from the reference day in 496 cell(s) (missing source chunk?)
```

**(a) `pr` 647.7 mm/d is real source data (Tropical Cyclone Kenneth genesis, Cosmoledo atoll).**
- The cell is 0.25° row 191, col 262 of the window, centre −9.875°, 47.625°: Cosmoledo atoll, Seychelles.
- CHIRPS v3 (pinned rev) treats 20 of the 25 fine pixels as land, with values of **575–777 mm/d**. 30 pixels in the window exceed 600 that day.
- `coarsen5` averages only the pixels that have data, so the atoll cell carries the peak almost unchanged.
- CHIRPS **v2.0** has the whole cell as ocean (all 25 pixels NA), so the cell is new with v3's land mask.

Decision needed: raise the `pr` ceiling (e.g. 1,000 mm/d; the record-scale daily totals are in that range), and/or drop cells outside the NEX land mask or small-island cells. Resume would then continue from 2019-04-23.

**(b) `hurs` 2009-10-05: a stale, pre-rewrite FAO object.**
- `AGERA5-RH06` 2009-10-05 has `last-modified` **2024-06-08**. Its neighbours, and every other mapset that day, are from the 2025-09 rewrite.
- That one COG has 496 extra NaN cells at 0.25° (scattered speckle across Africa, not a block). RH09/12/15/18 and SRF that day have the reference mask.
- A GCS listing scan of 2005-2025 (object generation older than 2025-09-01) finds exactly 4 stale objects:

  | Mapset | Stale objects |
  |---|---|
  | AGERA5-SRF | 0 of 7,670 |
  | AGERA5-RH06 | **4 of 7,670**: 2009-10-05, 2014-12-25, 2019-08-13, 2021-11-09 |
  | AGERA5-RH09, RH12, RH15, RH18 | 0 each |

- So **variant (ii) `-rh15` is unaffected**. Variant (i) needs a rule for these 4 days, e.g. mean of the 4 good hours where RH06 is NaN, or the v1.x value. The listing step could also assert generation ≥ the rewrite date.

**(c) Deadlock in the AgERA5 stop path (code bug).**
- After the `hurs` failure the process stayed alive for 8 min with 0 CPU ticks over 10 s, `rchar`/`wchar` frozen, and all 40 threads in `futex_wait_queue_me`.
- `py-spy dump` was refused: ptrace is not permitted in the container.
- Likely cause, from reading `ingest_agera5`: on a failed day it calls `ex.shutdown(wait=False, cancel_futures=True)` and then keeps iterating `as_completed(futs)`. A future cancelled by `shutdown` is cancelled while still pending. That never notifies `as_completed`'s waiter (only `set_running_or_notify_cancel` in a worker does, and those items have been removed from the queue). So `as_completed` blocks forever.
- The cube path stopped cleanly (no thread pool there). Possible fix: `break` out of the loop after the first failure, or iterate only the futures that are not `cancelled()`.
- I sent SIGTERM. Written files are complete, and resume will skip them.

#### I4 — AgERA5 v1.0 vs v2.0 radiation: **GATE PASSED** (`scripts/obs_agera5_v1_v2_gate.R`, committed)
- **Setup:**
  - v1.0: `{common_data}/ecmwf_agera5/solar_radiation_flux/…_final-v1.0.nc`, read with ncdf4 (the node's GDAL has no HDF5 plugin).
  - v2.0: the FAO COG via terra `/vsicurl` (terra 1.8.70, GDAL 3.11.3).
  - Days: the 15th of every month 2006-2022 (all 204 present). Window: Africa −18..52 E, 35 S..38 N, 700×730 cells at 0.1°.
- **Yearly results:** every year has mean bias **±0.0000 MJ**, p99 |diff| **0.0002 MJ**, and v1 = v2 domain mean (20.85–21.32 MJ). Trend: **+0.00000 MJ/yr**. All three invariants PASS.
- **Outliers:** only 2 of 204 days have any |diff| > 0.001 MJ: 2013-09-15 (max 3.31) and 2022-03-15 (max 3.53). Each is a few isolated cells.
- **Mask:** a fixed 3,011-cell mask difference, identical on every day.
- **Finding: the FAO COG georeferencing is half a cell off the C3S grid.**
  - v1.0 netCDF: 3600 × **1801**, cell centres on whole 0.1° (lat 90…−90).
  - COG: 3600 × **1800**, with *edges* at −180 / 90.
  - Day-1 pairing test (v1 row = COG row + di, v1 col = COG col + dj), mean |diff| in MJ:

    | | dj = 0 | dj = 1 |
    |---|---|---|
    | di = 0 | 0.00007 | 0.15 |
    | di = 1 | 0.18 | 0.25 |

  - So the COG holds the C3S cell centred at (lon, lat) in the pixel whose top-left *corner* is (lon, lat). Its geolocation is therefore **+0.05° lon, −0.05° lat** off.
  - S1 takes the COG transform as given, so S1's `rsds` and `hurs` are shifted by that much before the 0.25° area-average. Small at 0.25°, but systematic. A fix would be to shift `wt` by half a cell in `read_agera5_window`. That would change the smoke digests.
- **Outputs:** CSVs `logs/obs_s1_gate_{days,years}_20261027_051949.csv`.

#### For the macbook
1. **`pr` ceiling and island cells:** see (a).
2. **The 4 stale RH06 days:** choose a rule; see (b).
3. **Fix the AgERA5 stop-path deadlock:** see (c).
4. **Half-cell COG shift:** decide whether to correct it; see I4.
5. **NDWS 2–28 band:** too tight for this box; see I2.

The `OBS_ROOT` state is resumable as-is. I will not relaunch until told.

---

### MACBOOK 2026-10-27 (addendum) — also report the per-variable md5 digest of the Kenya smoke (I1)

PASCAL is reproducing the same smoke (`DISPATCH_pascal_obs_s1_repro.md`). To compare the two nodes, after I1
run the P3 digest from that dispatch on your smoke manifest(s): for each `var`+`token`, rows sorted by date,
the `md5` column concatenated and hashed. Report the lines (`var token n digest`) in your RESPONSE with the
manifest JSON `libs` block. Keep the smoke files.

---

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
