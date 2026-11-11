# DISPATCH — cglabs — observed hazards, long series 1981–2025 (for trends, normals and anomalies)

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### MACBOOK 2026-11-11 (2) — Y3 multi-core fix on develop; resume Y3–Y4 (weights come from your cache)

Right again, and thanks for the precise diagnosis. Fixed by shipping local copies of `mk_sen` and `normal_stats` in the
closures `terra::app` hands to the PSOCK workers (both call sites). The selftest now runs `run()` with `cores = 2` and
checks the result equals the single-core result (18 assertions).

```bash
cd $REPO && git pull --ff-only && Rscript R/obs/s7_trends_anomalies.R --selftest   # 18
```
Then Y3 as written (main token, then `-rh15`), then Y4. Both runs should log `admin weights from cache`.

### RESPONSE — cglabs 2026-11-11 (1) — **STOP at Y3 again: the admin fix works (weights built and cached, `admin units: admin0 55, admin1 719, admin2 6670`), but the first stack fails under `--cores 16`: `could not find function "mk_sen"` on the PSOCK workers.** No stack output written; `-rh15` not launched. No fix improvised

Stamp `20261111_054848`. HEAD `fc6d730`. Env grep empty.

**Y0 rerun:** `s7_trends_anomalies.R --selftest` → **17** ok, `ALL S7 SELFTEST ASSERTIONS PASSED`.

**Y3, main token, command as written** (`logs/obs_long_s7_<stamp>.log`), 05:48:48 → 06:08:26 (**~20 min**), peak memory **1.76 GB** (R + children, 5 s samples).
```
[S7] src=…/obs_seasonal/chirps-chirts-agera5_1982-2025 windows=19 years=1982-2024 (43) normal=1991-2020 cores=16
[S7] admin weights admin0: 44881 unit-cell pairs in 0.2 min (cached)
[S7] admin weights admin1: 54862 unit-cell pairs in 2.0 min (cached)
[S7] admin weights admin2: 80408 unit-cell pairs in 17.0 min (cached)
[S7] admin units: admin0 55, admin1 719, admin2 6670
Error in checkForRemoteErrors(val) :
  1440 nodes produced errors; first error: could not find function "mk_sen"
Calls: run ... clusterApply -> staticClusterApply -> checkForRemoteErrors
Execution halted
```
(Plus the known `CPL_gdal_init` LIBTIFF notices; the "11 warnings" were not printed.) The weights took 19 min this time, not 36.

**Where:** `R/obs/s7_trends_anomalies.R:81`, in `do_stack()`:
```r
tr <- app(r, function(v) mk_sen(v, t, min_trend), cores = cores)
```
With `cores > 1`, terra's `app()` runs the function on a fresh PSOCK cluster. The anonymous function is shipped with its `do_stack` frame (`t`, `min_trend`), but `mk_sen` lives in the master's global env, which is not shipped, so every worker fails. Line 86 (`normal_stats`) has the same pattern and would fail next.
**Why the selftest passed:** every selftest `run()` and `do_stack()` call uses the default `cores = 1L`, where `app()` stays in-process. So the `cores > 1` path has no test.
**Suggested fix (your call):** pass the helper through `app()`'s `...`, which terra forwards to the workers. For example `app(r, function(v, f, t, m) f(v, t, m), f = mk_sen, t = t, m = min_trend, cores = cores)`, and the same for `normal_stats`. Then add one selftest `do_stack()` call with `cores = 2` that checks the result is identical to `cores = 1`.

**State:**
- `$OL/Data/obs_trends/` holds only `_admin_weights/admin{0,1,2}_<key>.rds` (184 KB, 358 KB, 679 KB). No window dir, stack or parquet was written. The weights are valid, so the rerun reads them from cache: no ~20 min rebuild for either token.
- The `-rh15` sensitivity run was not launched; Y4 not done. Nothing running. Nothing published.

---

### MACBOOK 2026-11-11 — Y3 fix on develop (nrow type + cached admin weights); resume Y3–Y4

Right stop, and a clean diagnosis. The bug is fixed in `R/obs/s7_trends_anomalies.R`: the unit count is now taken as an
integer. The selftest now runs the **full `run()` with admin units**, the path that crashed (17 assertions, was 15).

**Admin weights are now cached** in `$OL/Data/obs_trends/_admin_weights/<level>_<key>.rds`. The key covers the grid
and the unit geometry. The 36-minute build happens once; the second token and any rerun read it back (the log
says `admin weights from cache`).

```bash
cd $REPO && git pull --ff-only && Rscript R/obs/s7_trends_anomalies.R --selftest   # 17
```
Then Y3 as written: the main token first, then `--token chirps-chirts-agera5-rh15`; then Y4. Nothing in `$OL` needs
cleaning, because the crashed run wrote nothing.

### RESPONSE — cglabs 2026-11-06 (3) — **STOP at Y3: S7 halted on its admin-unit log line, after 36 min of building admin weights and before the first stack.** `vapply(units, nrow, 1L)`: `nrow()` of a terra `SpatVector` is a double. Nothing written under `obs_trends`; the `-rh15` run not started. Y0-Y2 stand. No fix improvised

**What ran:** the Y3 command as written (`--token chirps-chirts-agera5 … --boundaries-dir $NWD/Data/boundaries --cores 16`), `logs/obs_long_s7_<stamp>.log`,
12:08:18 → 12:44:30. Env grep empty.

```
[S7] src=…/obs_seasonal/chirps-chirts-agera5_1982-2025 windows=19 years=1982-2024 (43) normal=1991-2020 cores=16
Error in vapply(units, nrow, 1L) : values must be type 'integer',
 but FUN(X[[1]]) result is type 'double'
Calls: run ... sprintf -> paste0 -> sprintf -> paste -> sprintf -> vapply
Execution halted
```
(The eight `CPL_gdal_init` warnings with it are the known `/opt/conda/lib/gdalplugins` LIBTIFF notice, see the X4 final block.)

**Where:** `R/obs/s7_trends_anomalies.R:155`, in `run()`:
```r
lg(sprintf("admin units: %s", paste(sprintf("%s %d", names(units), vapply(units, nrow, 1L)), collapse = ", ")))
```
`read_units()` (l.95-102) returns `terra::vect(...)` objects, and terra's `nrow()` method returns a double, so `vapply(…, 1L)` refuses it.
The run had already spent **~36 min** in `unit_weights()` (l.103-108: `terra::extract(…, weights = TRUE)` over GAUL admin0/1/2, on one core)
when it reached this line, so the cost is in time only. Suggested one-token fix: `vapply(units, nrow, numeric(1))`, or `as.integer(nrow(x))`.
**Why the selftest passed:** its `run()` calls (l.201, l.214) never pass `boundaries_dir`, and the admin path is tested through
`admin_stack()` with a data.frame directly. So the `run()` → `read_units()` → `vect` path has no test. One assertion that calls `run()`
with a tiny boundaries dir of three `_a0_/_a1_/_a2_` parquets would cover it.

**State:**
- `$OL/Data/obs_trends` does not exist; nothing to clean up. The RH15 sensitivity run was not launched.
- Peak memory over the 36 min: **1.6 GB**, total RSS of the R process and its children sampled every 5 s
  (`logs/obs_long_s7_peakmem_<stamp>.txt`). `--cores 16` had not engaged yet, since `app()` is per stack.
- Y0-Y2 stand as reported below. Nothing running.

**Not done (waits on the fix):** the rest of Y3 and Y4. Already done for Y4: the Kenya PTOT sanity check (interim 1). **For the rerun, note** that
`unit_weights()` costs ~36 min **per S7 invocation**, so the two token runs together cost ~72 min before any stack. Caching the weights
(e.g. a `.rds` beside `dst`, keyed on the template grid) would halve that; your call.

---

### RESPONSE — cglabs 2026-11-06 (interim 2) — **Y2 PASS: 19 new `PTOT_sum.tif` stacks, 44 layers each; 0 existing stacks or bundles rewritten.** Y3 (S7, main token) launched 12:08:18

`logs/obs_long_s35_ptot_<stamp>.log`, 11:53:30 → 12:07:46, **14 min**, 0 WARN, 0 errors.
- The log reports `seasons kept=43 (1982..2024) timeframe=1982-2025` and the same window diagnostics as X4. It ends with `S3 <window>/PTOT: 44 seasons` for
  each window, every `S4/S5 … done` reusing its bundle, and **`done: 0 bundles`**.
- **The mtime gate**, over every file under `$OL/Data/obs_seasonal` and `$OL/Data/hazard_risk`: before **254** (95 stacks + 159 bundles), after
  **273**. **0 changed** (mtime or size), 0 gone, **19 new, all `PTOT_sum.tif`**: one per window of the main token, 44 layers
  (1981 … 2024) each.
- The `-rh15` token's 19 window dirs hold only its THI stacks, so they have no `PTOT_sum.tif`. That is expected, because the command runs on the
  main token only.
- Lists: `logs/obs_long_y2_mtimes_{before,after}_<stamp>.txt`.

---

### RESPONSE — cglabs 2026-11-06 (interim 1) — **Y0 + Y1 PASS.** Selftests 25 / 15; monthly PTOT 1981-2025 is 540 files, 0 WARN, 0 missing, 0 errors; the Kenya-box climatology shows both rains. Y2 (rainfall stacks) running

Stamp `20261106_111807`. HEAD `3ccda81`, branch `develop`. Env grep empty before each launch.

#### Y0 — selftests. PASS
`s3_s5_obs_hazards.R --selftest` **25** ok; `s7_trends_anomalies.R --selftest` **15** ok. `$NWD/Data/boundaries` holds exactly
one of each: `atlas_gaul24_a0_africa.parquet`, `…_a1_…`, `…_a2_…`.

#### Y1 — S2 monthly PTOT. PASS
`calc_PTOT.R` as written (`logs/obs_long_s2_ptot_<stamp>.log`), 11:18 → 11:53, **35 min**. **540** `PTOT-YYYY-MM.tif` (nothing else in
the dir), 0 WARN, 0 `daily inputs missing`, 0 errors.

Sanity check: the Kenya box (33.9..41.9 E, 4.7 S..5.0 N) land-mean monthly PTOT, averaged over 1981-2025, in mm:
```
Jan 36  Feb 32  Mar 78  Apr 139  May 83  Jun 32  Jul 30  Aug 29  Sep 26  Oct 60  Nov 103  Dec 68
```
Both rains are there: the long rains peak in April (139), the short rains in November (103), and June-September and January-February are dry.

---

### MACBOOK 2026-11-06 — long series accepted; Y0–Y4: rainfall stack + S7 trends, 1991–2020 normals, anomalies (nothing published)

**Accepted.** X0–X6 passed every gate, and the overlap result is exact: NDWS/NDWL0 are identical from 2006, so the
published product is free of its seed. Your note on 44 layers (43 seasons + the spin-up) is right; S7 selects seasons
by name and drops 1981.

**New on develop (pull):**
- `R/obs/s3_s5_obs_hazards.R --extra-indices PTOT` builds seasonal **rainfall-total** stacks (`PTOT_sum.tif`, sum
  over each window). These are stacks only: never classified, never in a bundle. Selftest 25.
- **`R/obs/s7_trends_anomalies.R`** (selftest 15). Per window × index stack it computes:
  - trend: Theil-Sen slope per decade; Mann-Kendall p with the Hamed-Rao autocorrelation correction; FDR
    (Benjamini-Hochberg) significance across cells;
  - the 1991–2020 normal: mean, sd, p10, p50, p90;
  - per-season anomalies: difference and z.

  With `--boundaries-dir` it also writes area-weighted admin0/1/2 series and trends, computed on each unit's own
  mean series. Outputs go under `$OL/Data/obs_trends/<token>_1982-2024/`.

#### Y0 — pull, selftests
```bash
cd $REPO && git pull --ff-only && git log -1 --format='%h %s'
Rscript R/obs/s3_s5_obs_hazards.R --selftest   # 25
Rscript R/obs/s7_trends_anomalies.R --selftest # 15
```
#### Y1 — S2 monthly PTOT, 1981–2025 (background)
```bash
cd $REPO/hazards_upstream/R/04_indices
COMMON_DATA=$OL SCENARIO=historical YRS=1981:2025 DAYS_CHECK=strict GCMS=chirps-chirts-agera5 \
  nohup Rscript calc_PTOT.R > $REPO/logs/obs_long_s2_ptot_$STAMP.log 2>&1 &   # + .pid
```
Expect 540 `PTOT-YYYY-MM.tif`, 0 WARN, 0 `daily inputs missing`. Sanity check: the Kenya-box land mean for each
month shows the March–May and October–December rains.

#### Y2 — S3 rainfall stacks only (no --overwrite: the existing stacks and bundles are reused)
```bash
nohup Rscript R/obs/s3_s5_obs_hazards.R --obs-root $OL --token chirps-chirts-agera5 --years 1981:2025 --spinup 1 \
      --ggcmi-dir $NWD/Data/ggcmi --extra-indices PTOT > logs/obs_long_s35_ptot_$STAMP.log 2>&1 &
```
Expect new `PTOT_sum.tif` in every window dir (44 layers), and the log reusing the existing stacks and bundles.
**Any other stack or bundle rewritten is a stop**: check mtimes before and after.

#### Y3 — S7 (background)
```bash
nohup Rscript R/obs/s7_trends_anomalies.R --obs-root $OL --token chirps-chirts-agera5 --timeframe 1982-2025 \
      --years 1982:2024 --normal 1991:2020 --boundaries-dir $NWD/Data/boundaries --cores 16 \
      > logs/obs_long_s7_$STAMP.log 2>&1 &
```
This covers all windows and all stacks (NDWS, NDWL0, NTx35, THI_max, PTOT). Run it again with
`--token chirps-chirts-agera5-rh15` for the RH15 THI stacks (livestock windows), as a sensitivity run. If the
boundaries dir does not hold exactly one `*_a0_*`, `*_a1_*` and `*_a2_*` parquet, S7 stops; say what is there.

#### Y4 — report
- Wall-clock and peak memory for Y3.
- Per index, the log's summary line: cells with a trend, % p < 0.05, % FDR-significant, median slope per decade.
  Report it for `ggcmi-mai` (maize) and `annual12` (livestock THI).
- The admin trend summary per level.
- **Sanity checks (report, not gates):**
  - the 1991–2020 normal of maize-window NDWS for KEN and ETH;
  - the 2011 and 2015 anomaly-z of maize-window NDWS for KEN admin0. The 2010–11 Horn of Africa and 2015–16 El Niño
    droughts should show as positive dry anomalies;
  - the national THI_max trend in Nigeria, Kenya and Ethiopia.
- `du -sh $OL/Data/obs_trends`.

Then push one `### RESPONSE`. **Nothing is published**: the keys and the publish are a later block with Pete's GO.

### RESPONSE — cglabs 2026-11-05 (final) — **X0-X6 complete, every gate PASS.** S3-S5 built 129 + 30 = 159 bundles with `seasons kept=43 (1982..2024) timeframe=1982-2025`. **X5 overlap against `$OR`, 2006-2024: NTx35 and THI_max identical (max \|diff\| 0); NDWS and NDWL0 exactly 0 from the first published season (2006) on.** `$OL` = 33 GB, kept. Nothing published, no R/3.

Stamp `20261105_091344`. X0-X2 are in interim 1 and X3 in interim 2, both below. **Total wall-clock 09:13 → 15:59, 6 h 45 min.**

#### X4 — S3-S5, both commands as written. PASS
Started 14:26:56 (`logs/obs_long_s35{,_rh15}_<stamp>.log`). The main command took **74 min** (done 15:40:57) and the `-rh15` livestock command **26 min** (done 14:52:41). 0 WARN.
- Both print `seasons kept=43 (1982..2024) timeframe=1982-2025`.
- `done: 129 bundles` + `done: 30 bundles` = **159** `*_int.tif` in `$OL/Data/hazard_risk/ggcmi-crop/`. No FPAR bundle: S6 was not run.
- The window diagnostics equal L2's exactly: `annual12 … =12: 165134; NA on land: 0`, and `NA on land: 110` on every GGCMI window.
- **Seasonal stacks: 95 files, each with 44 layers named 1981 … 2024, not 43.** This is the same shape as `$OR`: its stacks hold
  **20** layers (2005 … 2024) for `seasons kept=19`. S3 writes the spin-up season into the stack (`layer per season, named by start
  year`, `R/obs/s3_s5_obs_hazards.R:15`), and the bundles use the 43 kept seasons. **So 43 + 1 spin-up layer is the expected
  invariant;** the block's "43 layers" counts kept seasons. I did not stop on it, because `$OR` shows the same relationship and
  X5 reads layers by name.
- The 8 `GDAL error 1` lines (`libtiff.so.6: version LIBTIFF_4.6.1 not found (required by /opt/conda/lib/gdalplugins/../libgdal.so.36)`)
  are a long-standing node warning, already in the May `cr068_*` logs. It comes from the image's `GDAL_DRIVER_PATH` (server-environment
  §9): terra tries the `/opt/conda` plugins, fails to load them, and keeps its built-in GTiff driver. The outputs are unaffected, as
  X5 shows. (The base conda history confirms the 2026-11-05 `gh` install added only `gh`.)

#### X5 — overlap gate against `$OR`. PASS
`logs/obs_long_x5_overlap_<stamp>.R`, per-layer table in `…csv`. For every window (19) × index stack in
`$OR/Data/obs_seasonal/<token>_2006-2025/` (`NDWS_mean`, `NDWL0_mean`, `NTx35_mean`, `THI_max_max`, plus `THI_max_max` for `-rh15`),
I matched each layer in `$OL/…_1982-2025/` by name: **1,900 layer pairs, 0 files missing, 0 cells where one root is NA and the other
is not.**

max |diff| over all windows and cells, per season start year:

| year | NDWS | NDWL0 | NTx35 | THI_max (both tokens) |
|---|---|---|---|---|
| 2005 (`$OR`'s spin-up season, not kept there) | 4.67 | 2.75 | **0** | **0** |
| **2006 … 2024 (each year)** | **0** | **0** | **0** | **0** |

- **NTx35 / THI_max: identical** in every season, including 2005.
- **NDWS / NDWL0: converged before the first published season.** The only difference is in `$OR`'s own seed season 2005, where
  `$OR` started from `AVAIL_SEED_MONTHS=2005-01` and `$OL` carries 24 years of state. **From 2006 on the difference is exactly 0, not
  float noise.** The bucket forgets its seed within the 2005 season, so the published 2006-2025 product is also free of its seed.
- `PTOT`: no stack in either root, so there is nothing to compare.

#### X6 — wrap
- `du -sh $OL` = **33 GB**. **`$OL` kept**; the trend/normal/anomaly block reads it.
- Nothing written outside `$OL` and `logs/`. `$OR`, `$SM`, the NEX trees and S3 untouched.
- **Correction to interim 2:** the S2 rates were not "matching" the 21-year run; they were **faster**. NDWS ~8.8 s/month here against
  ~11.9 s in L2, NDWL0 ~15.7 against ~19.7.

---

### RESPONSE — cglabs 2026-11-05 (interim 2) — **X3 PASS: S2 complete, 540 monthly files per index, 0 WARN, 0 `daily inputs missing`, 0 errors.** X4 (S3-S5) launched 14:26:56

All five scripts launched together at 11:37:52 with the block's environment (`COMMON_DATA=$OL SCENARIO=historical YRS=1981:2025
DAYS_CHECK=strict AVAIL_SEED_MONTHS=1981-01`), unset afterwards; `logs/obs_long_s2_{ndws,ndwl0,ntx,thi,thi_rh15}_<stamp>.log`.
Peak memory stayed low: 14 GB used of 376 GB with all five running.

| script | token | end (log) | wall | monthly files | WARN | missing | error |
|---|---|---|---|---|---|---|---|
| `fast_calc_NDWS.R` | main | 12:57:20 | **79 min** | NDWS **540** (+540 `AVAIL`) | 0 | 0 | 0 |
| `fast_calc_NDWL0.R` | main | 13:58:43 | **141 min** | NDWL0 **540** (+540 `AVAIL`) | 0 | 0 | 0 |
| `calc_NTx.R` | main | 14:26:08 | **168 min** | NTx20 … NTx50, **540 each** (31 thresholds) | 0 | 0 | 0 |
| `calc_THI.R` | main | 12:16:49 | 39 min | THI_max **540**, THI_mean 540 | 0 | 0 | 0 |
| `calc_THI.R` | `-rh15` | 12:16:49 | 39 min | THI_max **540**, THI_mean 540 | 0 | 0 | 0 |

540 = 45 years × 12. As in RESPONSE (6), NDWS/NDWL0 directories hold twice that because of the `AVAIL-YYYY-MM.tif` carry-over
state. The THI dirs also hold a `daily/` subdirectory, the same as `$OR`. The NDWS/NDWL0 rate is ~8.5 s and ~15 s per month,
matching the 21-year run's pace scaled to 45 years.

---

### RESPONSE — cglabs 2026-11-05 (interim 1) — **X0-X2 PASS.** Selftests 23/9/24; soil fill identical to L1 (2,494 → 49) and byte-identical to `$OR`'s filled soils; S1 ingest 1981-2025 complete, 16,436 days per variable, 0 failures, no S1Stop; worst repaired day 65 px (limit 100); **2005-2025 overlap byte-identical to `$OR` on all six series**. X3 (S2) running.

Stamp `20261105_091344`. HEAD `8c9ee59`, branch `develop`. Env grep empty before each launch. `OL={common_data}/atlas_obs_hazards_1981`; nothing written outside `$OL` and `logs/`.

(Step 1 of the hand-off prompt, the M/P RESPONSE in `DISPATCH_cglabs_obs_s3_r3.md`, was already pushed as `7624c02` on 2026-11-05 and is on origin; nothing redone.)

#### X0 — selftests. PASS
`s1_ingest.py selftest` **23** ok, `soil_fill.py selftest` **9** ok, `s3_s5_obs_hazards.R --selftest` **24** ok (all via `pyobs`).

#### X1 — link + soil fill. PASS
`link` made the two symlinks (`chirps_wrld`, `atlas_hazards/soils`). The fill then replaced the soils symlink with filled copies, as in L1:

| layer | res | max px | valid | filled | still NA | filled range | source range |
|---|---|---|---|---|---|---|---|
| `sscp_world.tif` | 0.05000 deg | 10 | 3,712,662 | **638,524** | 10,044,814 | 7.392 .. 107.25 | 4.97 .. 111 |
| `ssat_world.tif` | 0.05000 deg | 10 | 3,712,662 | **638,524** | 10,044,814 | 10.127 .. 183.05 | 6.78 .. 200 |

`--check-pr`: obs land cells 48,576, without soil **2,494 → 49** for both layers, the same as L1. Shared soils: md5 of all six files identical before and after (`logs/obs_long_soil_md5_{before,after}_<stamp>.txt`). Extra check: the md5 of every file in `$OL/atlas_hazards/soils` equals `$OR/atlas_hazards/soils`.

#### X2 — S1 ingest 1981-2025. PASS
Both processes as written (`logs/obs_long_{cubes,agera5}_<stamp>.log`). Wall-clock: cubes (pr, tasmax, tasmin) 09:14 → 10:26, **73 min**; AgERA5 (rsds, hurs mean, hurs rh15) 09:14 → 11:27, **133 min**. Manifests 49,308 rows each, **0 failures**, no S1Stop, no traceback. Sources pinned as in `$OR`: CHIRPS cube rev `e9466d3`, CHIRTS cube rev `c44a0b5`, AgERA5 v2.0 on the FAO GISMGR mirror.

**validate**, `--years 1981:2025`, both **VALIDATE PASS**, lattice equals `base_rast_nexgddp.tif`:

| variable (token) | days | domain-mean over days, min / median / max |
|---|---|---|
| pr | 16,436 / 16,436 | 0.54 / 1.77 / 4.53 mm d-1 |
| tasmax | 16,436 / 16,436 | 23.54 / 32.35 / 35.74 degC |
| tasmin | 16,436 / 16,436 | 12.95 / 20.76 / 23.85 degC |
| rsds | 16,436 / 16,436 | 16.77 / 21.37 / 24.12 MJ m-2 d-1 |
| hurs (`-rh15`) | 16,436 / 16,436 | 29.81 / 37.05 / 48.20 % |

`tasmax < tasmin` in sampled days: 0 cells. (The main-token `validate` covers pr/tasmax/tasmin/rsds by default; hurs main has 16,436 files in its manifest.)

**Digests** (md5 of the sorted `md5  filename` list per series, from the manifests, 1981-2025):
```
pr      chirps-chirts-agera5       16436  fb7db5b64a46d04f4003cb1643bff672
tasmax  chirps-chirts-agera5       16436  e35857fd757602f270d149161dbc94c1
tasmin  chirps-chirts-agera5       16436  e6601bc4f455d337c19f6b993bddf75e
rsds    chirps-chirts-agera5       16436  f9edbc51506e70bd426c2a8838b474fd
hurs    chirps-chirts-agera5       16436  29fe6672f32c32a3995ab83423775dfe
hurs    chirps-chirts-agera5-rh15  16436  0dc10df7b80dfd7e57d0962d91e042e2
```

**Overlap 2005-2025, byte-identical to `$OR`.** Every daily file hashed in both roots (`logs/obs_long_overlap_md5_<stamp>.py`, summary `…_summary.txt`), same digest definition:
```
series                                     n(OR)  n(OL)  digest OR = digest OL               mismatches
pr/historical/chirps-chirts-agera5          7670   7670  d48c8a05328fb2610380b10796923fbc   0
tasmax/historical/chirps-chirts-agera5      7670   7670  41300d604fc09ae0fd977fdcaa57f500   0
tasmin/historical/chirps-chirts-agera5      7670   7670  7ea92f4318de917c97ec4890b65659da   0
rsds/historical/chirps-chirts-agera5        7670   7670  fd50cc39c653ae0ec8cde295d976ed5a   0
hurs/historical/chirps-chirts-agera5        7670   7670  daef4b9041a867a6d7a505f0a930849b   0
hurs/historical/chirps-chirts-agera5-rh15   7670   7670  08ff7ca2997800a0f970b30aee622e07   0
```

**FLAG lines: 299 in total.** tasmax repaired above 57 °C on **7** days, tasmin above 43 °C on **277** days, and AgERA5 RH06 with fewer than 5 hours on **15** days. **The most repaired native pixels on any one day is 65** (tasmin, 2023), under the 100-pixel stop. Tmin for 2005-2025 is **222 days, exactly the `$OR` count**; 1981-2004 adds 55. That is fewer than "roughly twice" because the early decades have few hot nights above the cap. The 15 RH06 days are the AgERA5 objects older than the archive's 2025-09 rewrite, which the ingest names: 1985-05-11, 1989-11-08, 1989-12-17, 1989-12-31, 1990-11-21, 1991-11-19, 4 × 2000, 2004, 2009-10-05, 2014-12-25, 2019-08-13, 2021-11-09. Each flags **504 cells** with fewer than 5 hours. The four from 2009 on are the same four days `$OR` named.

Per year: days flagged, and the most pixels repaired on one day (tasmax/tasmin only). Years not listed have no flags.

| year | tasmax days | tasmin days | hurs RH06 days | max px/day |
|---|---|---|---|---|
| 1981 | 0 | 1 | 0 | 2 |
| 1983 | 0 | 1 | 0 | 3 |
| 1984 | 0 | 3 | 0 | 6 |
| 1985 | 0 | 0 | 1 | 0 |
| 1987 | 0 | 1 | 0 | 12 |
| 1988 | 1 | 8 | 0 | 16 |
| 1989 | 0 | 0 | 3 | 0 |
| 1990 | 0 | 5 | 1 | 19 |
| 1991 | 0 | 0 | 1 | 0 |
| 1992 | 0 | 1 | 0 | 4 |
| 1993 | 0 | 2 | 0 | 5 |
| 1995 | 0 | 4 | 0 | 13 |
| 1996 | 0 | 1 | 0 | 9 |
| 1997 | 0 | 1 | 0 | 2 |
| 1998 | 0 | 9 | 0 | 25 |
| 1999 | 0 | 4 | 0 | 8 |
| 2000 | 0 | 9 | 4 | 14 |
| 2001 | 0 | 4 | 0 | 3 |
| 2003 | 0 | 1 | 0 | 4 |
| 2004 | 0 | 0 | 1 | 0 |
| 2005 | 0 | 4 | 0 | 24 |
| 2006 | 0 | 9 | 0 | 11 |
| 2007 | 0 | 2 | 0 | 23 |
| 2008 | 0 | 6 | 0 | 8 |
| 2009 | 1 | 2 | 1 | 14 |
| 2010 | 0 | 11 | 0 | 20 |
| 2011 | 1 | 6 | 0 | 29 |
| 2012 | 0 | 7 | 0 | 28 |
| 2013 | 0 | 3 | 0 | 15 |
| 2014 | 0 | 5 | 1 | 19 |
| 2015 | 2 | 9 | 0 | 29 |
| 2016 | 1 | 10 | 0 | 28 |
| 2017 | 1 | 14 | 0 | 31 |
| 2018 | 0 | 6 | 0 | 10 |
| 2019 | 0 | 10 | 1 | 10 |
| 2020 | 0 | 11 | 0 | 27 |
| 2021 | 0 | 19 | 1 | 26 |
| 2022 | 0 | 6 | 0 | 32 |
| 2023 | 0 | 29 | 0 | 65 |
| 2024 | 0 | 43 | 0 | 51 |
| 2025 | 0 | 10 | 0 | 27 |

#### X3 — S2, launched 11:37:52, all five scripts as written (PIDs logged). Report follows.

---

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
