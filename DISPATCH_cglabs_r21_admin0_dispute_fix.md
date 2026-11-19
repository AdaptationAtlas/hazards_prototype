# DISPATCH — R/2.1 national rows for EGY/KEN/SDN/SSD, and first-year DJF/NDJ: rebuild and republish 19 keys

Append-only. Newest block on top. Respond by prepending a `### RESPONSE` block.

---

### MACBOOK 2026-11-19 [M-20261119-r21a0-1] — confirm cause, rebuild R/2.1 from §2.4 (intermediates reused), gate, republish time series + trends + variability

(Written 00:xx EAT on 2026-11-20, which is still 2026-11-19 UTC; the code keeps the date the originating
session assigned.)

**Context.** Three CDH records (season time series, trends, variability) are on hold until this lands.
On the live files (checked 2026-11-19):

- **National rows broken.** In every `…/processing=timeseries_mean_month/timeframe=3months/period=*/baseline=*/variable=ensemble_season_timeseries.parquet`,
  the national rows (`admin1_name IS NULL`) of **EGY, KEN, SDN, SSD** have **1 year** (the window's first) instead
  of the full set. Their values are sums over the whole window: KEN PTOT MAM ssp245 2041-2060 = 10,278 mm, against
  160-837 mm for its first-level units. The trends and variability files carry NaN on those national rows. Every
  other zone has the full year set.
- **Second fault, all zones.** For the cross-year seasons, the **first year of every window is about 2×**:
  AGO national PTOT ssp245 2041-2060 DJF 2041 = 955 against 481-533 for 2042-2059; NDJ 2041 = 961 against 474-544.
  The anomaly baselines (historic DJF/NDJ means) and every DJF/NDJ trend and variability value inherit that.

**Cause — one mechanism, two triggers (`R/2.1_create_monthly_haz_tables.R` §3.1).** `find_consecutive_pattern()`
(`R/haz_functions.R`) marks months that do not fit the season pattern with **0, not NA**. §3.1 then kept them
(`dt[!is.na(seq)]`), so every unmatched month landed in one group, which took the first row's year.
- **Trigger 1, the 4 countries.** Their national groups hold 2-4 rows per (year, month). §3.1 groups by
  `admin0_name, admin1_name` (iso3 and gaul codes are not in the key). The extra rows come from the GAUL 2024 disputed
  areas (`metadata/disputed_areas.csv`: Abyei 100, Bir Tawil 110, Hala'ib 133, Ilemi 135; claimants are exactly
  these 4 countries). They reach the admin0 extraction labelled with a claimant's iso3/admin0_name. The repo's own
  record of this: `R/misc/ke_eastern_rainfall_trends.R` l.62-65 ("Kenya adm0 carries two rows per year: gaul0_code
  137 … 135 = the disputed Ilemi Triangle sliver … Keep 137"). The R/2.2 commit `254b307` also notes that "GAUL2024
  ships a few multipart/dup polygons per admin level". With duplicated months the pattern never matches. The whole
  window then becomes one summed "year".
- **Trigger 2, all zones.** At the window edges a cross-year season is incomplete: DJF has the first Jan-Feb with no
  Dec before it, and the last Dec with nothing after it. Those months were marked 0 and lumped into the first year.

The public S3 admin0 boundary file has 55 clean rows, so the exact form of the node's duplicates is not visible from
macbook. **Step 2 confirms it on the node before anything moves.**

**Fix (this commit).**
1. **§2.4 combine:** drop admin0 rows coded as a disputed area (gaul0 ∈ `disputed_areas.csv`), then exact duplicate
   admin0 rows. The disputed areas stay as admin1 rows under both claimants, unchanged. Then a **hard stop** if any
   (iso3, admin0/1/2_name, scenario, model, timeframe, hazard, year, month) key still has more than one row. The log
   prints `2.4) <timeframe>: dropped <n> disputed/duplicate admin0 rows`.
2. **§3.1:** `dt[seq > 0]` instead of `dt[!is.na(seq)]`. Incomplete edge seasons are dropped, not lumped. The DJF/NDJ
   year sets stay the same (they already had one year fewer); only the first year's value changes.
3. **`scripts/r21_publish_to_s3.R`** now backs up each live key to `….preFix-<stamp>.bak` (public-read) before
   overwriting, as publish_B/C already do. The dry-run prints the planned backup name.

**Rebuild scope.** It cannot be national-only. The national rows live in the same files as everything else, and
fault 2 touches every zone's DJF/NDJ. No env switch skips "extraction but not combine". **The per-file existence
gates do it:**
- Leave `intermediate/` (per-GCM extractions, untouched by the fix) in place.
- Park every top-level output in `hazard_timeseries_mean_month/`.
- Run R/2.1 with **no SKIP flags and `FORCE_OVERWRITE` unset**. §2 extraction finds every intermediate and writes
  nothing. §2.4 combine and §3.1-§3.4 rebuild.

The 2026-09-18 full run took about 12 h, of which §2 extraction was about 3.5 h and §3.4 about 1 h. Expect roughly
8 h. **Do not use `scripts/r21_rerun.sh`.** It defaults `FORCE_OVERWRITE=1` (that would re-extract everything) and
auto-commits.

**Scope.** Rebuild R/2.1 as above. Publish the 19 keys (10 time series, 4 trends, 5 variability) through their own
scripts. Do not touch any other prefix, the older `.bak` clutter (#59), R/2.2, R/3, or the obs pipeline. Macbook
updates pins, changelogs and records from your RESPONSE.

**Rules that bite here:** `git pull` first; absolute-path sourcing in `Rscript -e`; the R/2.1 run and every publish
in the background (`nohup … &`, `logs/`); keep `Data/_parked_r21a0_*` and every existing `_parked_*` /
`sandbox/backup/` / `*_pre_issue26_bak` dir; verify the RESPONSE landed on `origin/develop`.
**At any deviation from a gate: STOP, describe, publish nothing. Do not improvise a fix.**

Paths: `TSM = atlas_dirs$data_dir$hazard_timeseries_mean_month` (the dir R/2.1 calls `output_dir`), `INT = TSM/intermediate`.
`BASE = s3://digital-atlas/domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=timeseries_mean_month/timeframe=3months`.
The 19 keys:
- **Time series:** `BASE/period=<p>/baseline=<b>/variable=ensemble_season_timeseries.parquet` for b ∈ {1981-2014,
  1995-2014}, p ∈ {b, 2021-2040, 2041-2060, 2061-2080, 2081-2100}.
- **Trends:** `…/period=<future>/baseline=1995-2014/variable=ensemble_season_trends.parquet` (4 keys).
- **Variability:** `…/period=<1995-2014 or future>/baseline=1995-2014/variable=ensemble_season_variability.parquet` (5 keys).

---

**Step 0 — node quiet. G0 (invariant):** no Rscript running, no live `logs/*.pid`, nothing writing under `Data/`.
Also report free space on the `Data/` volume. The rebuild rewrites about 16 GB beside the parked copy.

**Step 1 — code is current.** `git pull`; `grep -c 'M-20261119-r21a0-1'` → `R/2.1_create_monthly_haz_tables.R` 2,
`scripts/r21_publish_to_s3.R` 1. Report HEAD. Report the md5 of the local admin0 boundary file
(`geo_files_local["admin0"]`) and of `file.path(boundaries_int_dir, "admin0_zonal.tif")`, the cached zonal raster
R/2.1 reuses.

**Step 2 — G1, confirm the cause on node data (read-only).**
- (a) **Boundary index.** Read the admin0 attributes from `geo_files_local["admin0"]` exactly as R/2.1 l.97-108
  does, including `zone_id`. Report row count, and every row for EGY/KEN/SDN/SSD or with gaul0 ∈ {100,110,133,135}
  (iso3, admin0_name, gaul0_code, zone_id), and any duplicated `zone_id`.
- (b) **Combined monthly table.** In the current `TSM/haz_monthly_adm_mean_2041-2060.parquet`, take
  `admin1_name IS NULL` rows and count rows per (iso3, scenario, model, hazard, year, month). For each iso3 with
  count > 1, list its distinct `gaul0_code` values and whether the extra rows are exact copies.
- (c) **Per-model, before the ensemble.** In `TSM/haz_3months_adm_mean_2041-2060_anomaly-1995-2014_seasons.parquet`:
  distinct `year` for KEN national rows (MAM), and for AGO national DJF the `value` for the first two years of one
  model.

**G1 (invariants):**
- (b) finds duplicates **only** for EGY, KEN, SDN, SSD, and only at admin0. Every extra row either has gaul0 ∈
  {100,110,133,135} or is an exact copy of the country's own row.
- (c) KEN national has 1 distinct year (the per-model series is already wrong, so it is not an ensemble fault). AGO
  DJF year 1 is about 2× year 2.

If duplicates appear for any other iso3 or at admin1, or extra rows are neither disputed-coded nor exact copies,
STOP. The fix would not cover them.

**Step 3 — reference and park.**
- **Live state.** `head-object` (size, ETag, LastModified, VersionId) for the 19 keys → `logs/r21a0_s3_before_<STAMP>.txt`.
- **Time-series reference = live.** For each of the 10, md5 of the local `TSM/haz_3months_adm_mean_<p>_anomaly-<b>_ensemble_seasons.parquet`
  = live ETag. For a multipart ETag (`-N`), recompute it or compare a downloaded copy. Any mismatch: STOP.
- **Trends/variability reference.** Run the two dry-runs on the current (pre-fix) files:
  `Rscript R/publish_B.R` and `Rscript R/build_publish_C.R`, with env clean of `CONFIRM`. Copy the slim/output
  parquets they write under `/tmp` to `logs/r21a0_ref_<STAMP>/{B,C}/`. Each must be **content-identical** to its
  live key (rows joined on all non-value columns, every value equal, NaN/NULL in the same places). This proves live
  = what these inputs produce, so live can serve as the G4/G5 reference. Any difference: STOP.
- **Park.** Record the file list and count of `INT` (expect about 2,916 parquets). **Move** every top-level file of
  `TSM` (parquets and `.json` sidecars, not `intermediate/`) to `Data/_parked_r21a0_<STAMP>/`. After the move: no
  `.parquet` at `TSM` top level; `INT` count and newest mtime unchanged.

**Step 4 — rebuild.** From the repo root, `FORCE_OVERWRITE` explicitly unset, no SKIP flags:
```bash
STAMP=$(date +%Y%m%d_%H%M%S); nohup env -u FORCE_OVERWRITE -u SKIP_R2_1_SEC2 -u SKIP_R2_1_SEC3_1 -u SKIP_R2_1_SEC3_2 -u SKIP_R2_1_SEC3_3 -u SKIP_R2_1_SEC3_4 Rscript -e 'options(error = function() { traceback(2); quit(status = 1, save = "no") }); source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); source("/home/jovyan/atlas/hazards_prototype/R/2.1_create_monthly_haz_tables.R")' > logs/r21a0_run_${STAMP}.log 2>&1 & echo $! > logs/r21a0_run_${STAMP}.pid
```
**Early kill-check (first minutes of the log):** `GCMs per scenario x timeframe` = 18 everywhere, no evenness stop.
§2 must write **no** intermediate: `INT` gets no file newer than the run start. If it starts re-extracting, kill the
job and STOP.

**G2 (invariants, end of run):**
- Exit 0, and the log has no error lines.
- One `2.4) <tf>: dropped <n> …` line per timeframe (6), with n > 0 for each. The new stop() did not fire.
- `INT` unchanged (count, newest mtime).
- `TSM` holds the same file set as the parked dir (same names; 10 `_ensemble_seasons`, 10 `_seasons`, 60 `_trends*`,
  …), all newer than the run start.
- §3.4 completes all 10 combos.

**Step 5 — G3, the fix took (new files alone; the 10 `_ensemble_seasons` + the 10 per-model `_seasons`).**
**G3 (invariants):**
- **Completeness.** In every file, for each season, every (iso3, admin1_name) zone, national rows included, has
  the same year set as the file's modal zone. Within-year seasons have the full window; DJF and NDJ have exactly one
  year fewer (as before).
- **No duplicate keys** (iso3, admin0/1_name, scenario, timeframe, year, hazard, season[, model]).
- **The 4 national rows are plausible (per-model `_seasons`, where the bound is exact).** For every country, compute
  the share of national rows outside [min, max] of that country's admin1 rows (same model, scenario, hazard, season,
  year). Use a tolerance of 1 % of the admin1 range, because border cells fall to one zone per level under
  `touches = TRUE`. Do it for hazards whose seasonal function (`haz_meta$function`) is sum or mean, and for max-type
  hazards on the upper bound only. **Each of EGY/KEN/SDN/SSD ≤ the largest share among the other 51 countries.**
  Report the four shares, that maximum, and KEN PTOT MAM ssp245 2041-2060 national ensemble `mean` (expect
  hundreds of mm, not 10,278).

**Step 6 — G4, time series: product vs input (new `_ensemble_seasons` vs parked = live, joined on all non-value columns).**
**G4 (invariants), per file:**
- **Rows:** new = parked + 4 × (rows of one normal country's national block − rows of the collapsed block). In the
  future files the collapsed block is 468 rows (9 hazards × 13 seasons × 4 scenarios × 1 year). Report the numbers.
- **Outside the exemptions, everything identical.** Every row that is **not** (EGY/KEN/SDN/SSD national) and **not**
  season ∈ {DJF, NDJ} is identical in every column, NaN/NULL in the same places. 0 differing rows.
- **DJF/NDJ, not first year:** `mean`, `sd`, `q17`, `q83`, `n_models` identical. The `*_anomaly` columns may shift,
  because the historic DJF/NDJ baseline changed. Report how many shifted.
- **DJF/NDJ, first year:** for hazards whose seasonal function is sum or max, new `mean` ≤ parked `mean` in every row
  (months were removed). Report the median of new first year / median of the zone's other years (expect ≈ 1; parked
  ≈ 2 for PTOT).
- **4 national blocks:** any change is allowed. Report it.

**Step 7 — G5, trends and variability (new dry-runs vs `logs/r21a0_ref_<STAMP>/`).** Run `Rscript R/publish_B.R`
and `Rscript R/build_publish_C.R` (env clean, no CONFIRM) and keep their outputs under `logs/r21a0_new_<STAMP>/{B,C}/`.
**G5 (invariants):**
- Each plans exactly its keys (B: 4, C: 5), and each script's own assertions pass.
- **No NaN on national rows with model data.** In every file, 0 NaN/NULL `mean` on admin0 rows of EGY/KEN/SDN/SSD.
  The admin0 NaN count for other countries equals the reference.
- **Identical outside the exemptions.** Every row that is not (the 4 national) and not season ∈ {DJF, NDJ} is
  identical to the reference. DJF/NDJ rows may differ; report how many.
- Row counts = reference.

**Step 8 — G6, validators.** Run the probe used in the issue #26 rebake if it is still in the repo
(`scripts/probe_r21_outputs.R`), plus the dry-run of the time-series publisher: `Rscript scripts/r21_publish_to_s3.R --dry-run`.
**G6 (invariants):** the probe passes, or fails only on the known n_models ∈ {0,18} all-NaN rows (issue #26
diagnosis, unchanged count); the dry-run lists **exactly the 10** time-series keys, each with a planned `.preFix-<stamp>.bak`.

**Step 9 — publish (19 keys), each in the background, one after another.**
```bash
STAMP=$(date +%Y%m%d_%H%M%S); nohup Rscript scripts/r21_publish_to_s3.R > logs/r21a0_publish_ts_${STAMP}.log 2>&1 &
```
Then, after it finishes: `nohup env CONFIRM=1 Rscript R/publish_B.R > logs/r21a0_publish_B_<STAMP>.log 2>&1 &`.
After that one finishes: `nohup env CONFIRM=1 Rscript R/build_publish_C.R > logs/r21a0_publish_C_<STAMP>.log 2>&1 &`.

**Step 10 — verify.** `head-object` for the 19 → `logs/r21a0_s3_after_<STAMP>.txt`. List the whole
`processing=timeseries_mean_month/` prefix before (from Step 3) and after.
**Invariants:**
- **Each of the 19 keys:** S3 size = size of the file uploaded; ETag = MD5 of that file. For a multipart ETag,
  recompute it, or download the key and compare content to the Step-6/7 file.
- A new `.preFix-<stamp>.bak` exists for each of the 19 and carries the Step-3 live size/ETag.
- Public `curl -sI https://digital-atlas.s3.amazonaws.com/<key>` → 200.
- Re-read 2 keys over HTTPS: KEN national in `period=2041-2060/baseline=1995-2014` time series has the full year
  set, and its trends file has no NaN on the 4 national rows.
- **Nothing else under the prefix changed.** The only differences between the before and after listings are the 19
  keys and 19 new `.bak` objects.

**Step 11 — RESPONSE.** G0-G6 with the numbers asked for, the Step-2 diagnosis (what form the duplicates took), a
19-row pin table (key, size, ETag, LastModified UTC, x-amz-version-id), log paths, the parked dir. Commit, push,
verify on `origin/develop`. End with `Answers: M-20261119-r21a0-1 (complete)` or `(stopped at <gate>)`.
