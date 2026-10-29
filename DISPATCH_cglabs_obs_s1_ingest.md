# DISPATCH — cglabs — observed pipeline S1: Python env, Kenya smoke, then the full 2005-2025 ingest

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### RESPONSE — cglabs 2026-10-29 — selftest 22/22; `tasmax` 7,670 complete with 6 repaired days, validate PASS, Tmax invariant PASS; cube process STOPPED again, this time on a CHIRTS-ERA5 **Tmin** artifact (45.1 °C, Red Sea coast, 2005-09-04) that the 57 °C cap cannot catch; S3 J0 PASS

**Pull:** `d94b598`. Stamp `20261029_090553`; FORCE_OVERWRITE / DAYS_CHECK / AVAIL_SEED empty at each launch.

**Selftest:** `logs/obs_s1_selftest4_<stamp>.log`, 22 `ok` lines, `ALL S1 SELFTEST ASSERTIONS PASSED`.

**Cube resume** (`logs/obs_s1_cubes3_<stamp>.log`, same command as before): `pr: 7670 days requested, 0 to write`,
`tasmax: 7670 days requested, 7670 to write`, as expected.

- **`tasmax`: complete, 7,670 written.** Every `FLAG native-pixels-above-57C-repaired` line:

  | day | repaired native pixels |
  |---|---|
  | 2009-07-20 | 14 |
  | 2011-07-01 | 6 |
  | 2015-08-12 | 2 |
  | 2015-08-20 | 4 |
  | 2016-06-09 | 3 |
  | 2017-08-24 | 20 |

  6 days, max 20 pixels: inside both thresholds (≤ ~20 days, ≤ ~100 pixels). The manifest carries
  `;flags:repaired-above-57C:<n>` on exactly these 6 rows. 2009-07-20 has 14, not the 16 I counted above 56 °C;
  the difference is the 2 pixels between 56 and 57. No `tasmax` S1Stop.
- **`tasmin`: STOPPED on day 247.** It wrote 2005-01-01..2005-09-03 (246 days), then
  `FAIL run -: tasmin 2005-09-04: values 0.528..45.100 outside plausible -45.0..45.0 degC (unit or scale error?)`
  (row in `manifests/s1_failed_20261029_090603_*.csv`). This is a **high-side** stop: a 0.25° cell mean of 45.1 °C.

**Diagnosis: the same class of CHIRTS-ERA5 coastal artifact, in Tmin, and below the repair cap.**
- Location: the coastal strip of the Saudi Red Sea coast near Yanbu, about 23.9–24.1 °N, 38.1–38.3 °E. That is
  inside the Africa bbox but outside Africa. Its 0.25° cell holds only a few land pixels, so the cell mean equals
  the hot pixels.
- Native CHIRTS-ERA5 Tmin on 2005-09-04: max 45.1, with 9 pixels > 44 and 68 > 40 across the window. The days either
  side peak at 40.2 (09-02), 39.6 (09-03), 43.1 (09-05) and 39.7 (09-06).
- The same block's mean by day, 09-01..09-07: 32.7, 33.1, 38.2, **45.1**, 43.0, 36.8, 32.2. That is a +12 °C one-day jump in a
  *minimum* temperature.
- CHIRTS v1.0 (`{common_data}/chirts/Tmin/2005/`), box 23.8–24.2 °N, 38.0–38.4 °E: Tmin max 31.9 / **35.4** / 34.8
  on 09-03 / 04 / 05. ERA5 overshoots v1.0 by about 10 °C. v1.0 Tmax for the same box is 45.6. So it was a hot day,
  but ERA5's Tmin 45.1 is about v1.0's *Tmax* for that box, and above the world-record highest daily minimum (about 44 °C).
- ERA5's Tmax that day: Africa-window max 52.8 (the days either side reach 47.7 and 50.5). I did not localise it. It is
  probably the same strip and is below 57, so the cap leaves it unrepaired.
- `TPHYS_CAP["tasmin"] = 57` cannot fire on a Tmin artifact: this one sits at 44–45 °C.

**Decision needed (macbook). Not improvised here.** Options:
- (a′) A separate, lower Tmin cap, e.g. `TPHYS_CAP["tasmin"] = 44` (about the record highest minimum). The same
  neighbour-median repair and FLAG apply. The 5×5 neighbourhood here is mostly sea (NaN) or other hot coastal pixels,
  so the median may still be about 44. Consider a larger window or "inland neighbours only".
- (a″) A temporal-jump test (|Δ| vs the ±3-day median > ~8 °C) as a second repair trigger. It would catch both the
  Safi and the Yanbu cases.
- Raising the tasmin range to 46. **Not recommended:** the value is non-physical.

A full-cube Tmin scan (days with any native pixel > 42 °C) would size the problem before choosing. About 13 min;
I have not run it. On resume, `tasmin` restarts with whatever `src_note` the fix sets; `pr` and `tasmax` write 0
unless the `tasmax` rule also changes.

**Validate `tasmax`** (`logs/obs_s1_validate3_<stamp>.log`): lattice True, `tasmax: 7670/7670 days present`,
domain-mean over days **min 24.36 median 32.77 max 35.74 °C**. **Tmax invariant (20–40 °C): PASS.** `VALIDATE PASS`.
`tasmin` was not validated (246/7,670), and the `tasmax < tasmin` check is pending with it.

**Digests** (latest manifest row per file, sorted by date):
```
hurs   chirps-chirts-agera5       7670 5af44fa1bc3be95f178207ca6c1564ba   (unchanged)
hurs   chirps-chirts-agera5-rh15  7670 1ac4b86e13c7f15f9c5595cc4bea8d2c   (unchanged)
pr     chirps-chirts-agera5       7670 fa64e4167601d3ee0c828916292d7c08   (unchanged)
rsds   chirps-chirts-agera5       7670 542071f0497cdab868637b4e057d899c   (unchanged)
tasmax chirps-chirts-agera5       7670 2af8fb25b20b4fcd395b53f5a223d8e4   (complete, repair-cap57)
tasmin chirps-chirts-agera5        246 43c4d93c6841ea45998150f04bc952f7   (partial, 2005-01-01..2005-09-03)
```

**`DISPATCH_cglabs_obs_s3_r3.md` J0: PASS** (logs `logs/obs_s3_{fx_*,selftest}_<stamp>.log`):
- `PASS R/checks/fixture_publish_tier_gates.R` (43 `ok`, `ALL PUBLISH-GATE FIXTURE ASSERTIONS PASSED`)
- `PASS R/checks/fixture_upstream_run_params.R` (53 `ok`, `ALL UPSTREAM RUN-PARAMETER FIXTURE ASSERTIONS PASSED`)
- `PASS selftest` (`R/obs/s3_s5_obs_hazards.R --selftest`: 23 `ok`, `ALL S3-S5 SELFTEST ASSERTIONS PASSED`; the
  dispatch said 20/20, so it has grown since)

J1 onward was not started: J1 only touches the smoke root and could run, but the dispatch says "as written, J0",
and J6 needs a complete `tasmin`.

**`server-environment-cglabs.md` §9:** added the `obs` conda env + `pyobs` wrapper note (same commit).

Nothing is running. The cube process will not be relaunched until the Tmin decision.

### MACBOOK 2026-10-28 (2) — Tmax decision: (a) in code, deterministic and flagged; pull and resume the cube process

Good catch and a clean diagnosis. Decision **(a)**, implemented so it is reproducible rather than a one-off edit:
`s1_ingest.py` now treats any **native** CHIRTS-ERA5 `tasmax`/`tasmin` pixel above **57 °C** (`TPHYS_CAP`; the
world record is 56.7) as non-physical and replaces it with the **median of its valid, sub-cap 5×5 neighbours**
*before* the 0.25° aggregation (cap itself if a pixel has no valid neighbour). The NaN mask is untouched. Every
repaired day logs `FLAG native-pixels-above-57C-repaired:<n>` and carries `;flags:repaired-above-57C:<n>` in the
manifest's `source_version`, so the repair is auditable per day. Not (b): CHIRTS v1.0 ends in 2016 and splicing
sources inside one variable is worse than a flagged local repair. Not (c). Selftest now **22**.

The cube `src_note` for `tasmax`/`tasmin` gained ` repair-cap57`, so on resume **all 1,661 written `tasmax` days
are rewritten** (the rule applies uniformly; ~13 min), then 2009-07-20 onward, then `tasmin`. `pr` is untouched
(0 to write).

```bash
cd <repo> && git pull --ff-only && git log -1 --format='%h %s'
$PY python/obs/s1_ingest.py selftest        # ALL S1 SELFTEST ASSERTIONS PASSED (22)
# resume: the SAME cube command as before (same --out-root, --vars pr,tasmax,tasmin), background, log + pid
```
Expect in the log: `tasmax: 7670 days requested, 7670 to write`, `pr: ... 0 to write`. Report:
- every `FLAG native-pixels-above-57C-repaired` line (date + count) for `tasmax` and `tasmin` — this is the scan
  you proposed, for free; I expect 2009-07-20 and few or no others. **More than ~20 flagged days, or any day with
  more than ~100 repaired pixels, is a stop**: that would mean a systematic problem, not an artifact.
- any `S1Stop` (the −25/62 and −45/45 ranges still apply after repair; a low-side stop is a new finding).
- validate for `tasmax` and `tasmin` (days present, domain-mean min/median/max), plus the Tmax invariant from the
  2026-10-27 block (domain mean over days within 20-40 °C), and the two digests.
Then start `DISPATCH_cglabs_obs_s3_r3.md` J0 as written there. The `pyobs` wrapper note may go into
`server-environment-cglabs.md` §9 in the same commit as this file's RESPONSE (two files, both yours).

### RESPONSE — cglabs 2026-10-28 — I0 21/21; I3 resume: `pr`, `rsds`, `hurs` ×2 complete and validate PASS; cube process STOPPED on a CHIRTS-ERA5 Tmax artifact (68.4 °C, Moroccan coast, 2009-07-20); I4 PASS again

Code: `d81b9e0`. `OBS_ROOT={common_data}/atlas_obs_hazards` (same root as before; nothing deleted).
`FORCE_OVERWRITE`, `DAYS_CHECK` and `AVAIL_SEED` were empty at launch. Logs:
- `logs/obs_s1_{selftest3,cubes2,agera52,validate2,gate2}_20261028_125*.log`
- pid files `logs/obs_s1_{cubes2,agera52}_20261028_125605.pid`, holding the python pids (the first write held `setsid`'s pid; corrected).

#### I0 — selftest
`ALL S1 SELFTEST ASSERTIONS PASSED`, 21 ok.

#### I3 — resume: two processes, launched 12:56

**Cube process (`pr,tasmax,tasmin`) — STOPPED.**
- `pr`: 2,445 days written (resume from 2019-04-23 as expected), in 4 min 17 s. 2019-04-23 (TC Kenneth, 647.7) now passes the 1,500 ceiling.
- `tasmax`: started at 2005-01-01 and wrote 1,661 days to **2009-07-19**, then stopped:
  ```
  run,-,tasmax 2009-07-20: values 11.696..68.400 outside plausible -25.0..62.0 degC (unit or scale error?)
  ```
  (`manifests/s1_failed_20261028_125614_<pid>.csv`; the process exited cleanly.)
- `tasmin`: not reached.

**Diagnosis (read-only): CHIRTS-ERA5 artifact on one day, at one stretch of coast.**
- Pinned cube rev `c44a0b5`, 2009-07-20: 16 native 0.05° pixels exceed 56 °C (8 exceed 60). All lie in **32.33–32.83 °N, 9.28–8.93 °W**, the Atlantic coast around Safi, Morocco. The native maximum is **68.7 °C** at 32.575 °N, −9.275 °E; the 0.25° cell centred at 32.625 °N, −9.375 °E averages 68.4.
- The pixels are coastal edge cells (NaN to the west), and the next pixels inland drop to 50–54 °C.

  | Day | Africa-wide native max |
  |---|---|
  | 2009-07-18 | 48.6 °C |
  | 2009-07-19 | 49.1 °C |
  | 2009-07-21 | 48.7 °C |
  | 2009-07-22 | 48.4 °C |

  No pixel exceeds 56 °C on those days.
- There *was* a real heat event. CHIRTS v1.0 locally (`{common_data}/chirts/Tmax/2009/`), same box:

  | Day | CHIRTS v1.0 Tmax |
  |---|---|
  | 2009-07-19 | 28.8–37.7 °C |
  | 2009-07-20 | **38.3–48.2 °C** |
  | 2009-07-21 | 29.5–37.0 °C |

  So CHIRTS-ERA5 overshoots by about 20 °C on the coastal pixels, to values beyond the world record (about 57 °C). The 62 °C ceiling did its job.
- **Decision needed:**
  - (a) Clip or NaN Tmax pixels above, e.g., 60 °C. NaN would break the NaN-mask invariant, so clip, or replace with the median of the 3×3 inland neighbours.
  - (b) Replace this day and box with CHIRTS v1.0, which ends in 2016, so only for ≤ 2016.
  - (c) Raise the ceiling (not recommended: 68 °C is non-physical and would put 68 °C into NTx and THI).

  It is worth scanning the whole cube for other days above 56 °C before choosing. That costs about 13 min per variable at the cube rate; I have not run it.

**AgERA5 process (`rsds`, `--rh both`) — complete, 0 failures** (`manifest … 23010 rows, 0 failures`):

| var / token | files | time | note |
|---|---|---|---|
| rsds | 7,670 (all rewritten: shift tag) | 8 min 44 s | 0.06 s/day |
| hurs `chirps-chirts-agera5` | 7,670 | 42 min | 0.33 s/day |
| hurs `chirps-chirts-agera5-rh15` | 7,670 | 7 min | — |

- **Stale-object log line:** `hurs: AGERA5-RH06 has 4 object(s) older than the 2025-09 rewrite: 2009-10-05, 2014-12-25, 2019-08-13, 2021-11-09`.
- **FLAG lines: 4** (as expected), each `FLAG AGERA5-RH06 cells-with-fewer-than-5-hours:504`. The 504 vs my earlier 496 is presumably the half-cell shift moving the speckle across 0.25° cells. All 4 days carry `flags:…` in the manifest's `source_version`.
- The `NotGeoreferencedWarning` is gone from the log, and the stop-path fix was not exercised (no AgERA5 failure).

**Validate** (`logs/obs_s1_validate2_*.log`). Every lattice equals `base_rast_nexgddp.tif`.

| var / token | days present | domain-mean over days (min / median / max) | result |
|---|---|---|---|
| pr | 7,670/7,670 | 0.54 / 1.78 / 4.53 mm | **PASS** |
| rsds | 7,670/7,670 | 17.03 / 21.42 / 24.12 MJ | **PASS** |
| hurs (main) | 7,670/7,670 | 38.74 / 46.02 / 55.58 % | **PASS** |
| hurs (`-rh15`) | 7,670/7,670 | 29.81 / 36.49 / 45.11 % | **PASS** |

`tasmax` and `tasmin` were not validated (incomplete). The Tmax-domain-mean invariant (20–40 °C) is pending for the same reason. `du -sh nex-gddp-cmip6`: 6.8 GB.

**Digests (full run;** latest manifest row per file, sorted by date, `md5` concatenated and hashed):
```
hurs    chirps-chirts-agera5         7670  5af44fa1bc3be95f178207ca6c1564ba
hurs    chirps-chirts-agera5-rh15    7670  1ac4b86e13c7f15f9c5595cc4bea8d2c
pr      chirps-chirts-agera5         7670  fa64e4167601d3ee0c828916292d7c08
rsds    chirps-chirts-agera5         7670  542071f0497cdab868637b4e057d899c
tasmax  chirps-chirts-agera5         1661  19b9a564631c4e761618552c88531e1f   (partial, to 2009-07-19)
```
`libs` is unchanged from 2026-10-27: python 3.12.16, numpy 2.5.4, xarray 2026.9.0, zarr 3.4.1, rasterio 1.5.2, GDAL 3.13.3.

#### I4 — rerun on `d81b9e0`: GATE PASSED, identical to 2026-10-27
All 204 days, yearly mean bias ±0.0000 MJ, p99 |diff| 0.0002 MJ, trend +0.00000 MJ/yr. The day-1 pairing is again di=0, dj=0 (mean |diff| 0.00007 MJ). Outliers are the same 2 days (3.31 and 3.53 MJ), and the mask difference is the same fixed 3,011 cells. CSVs: `logs/obs_s1_gate_{days,years}_20261028_125730.csv`.

#### Not done
- `pyobs` wrapper into `server-environment-cglabs.md` §9: not done, because this run's instruction was to commit only this dispatch file. The wrapper body:
  ```bash
  #!/bin/bash
  E=~/.conda/envs/obs
  export PROJ_DATA=$E/share/proj PROJ_LIB=$E/share/proj GDAL_DATA=$E/share/gdal
  unset GDAL_DRIVER_PATH
  exec $E/bin/python "$@"
  ```
- `DISPATCH_cglabs_obs_s3_r3.md`: not started (needs `tasmax`/`tasmin`).

**State:** resumable. After the Tmax decision, the same cube command resumes at `tasmax` 2009-07-20, then does `tasmin` (about 13 min each at 12.9 s per 128 days). Nothing will be relaunched until told.

---

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
