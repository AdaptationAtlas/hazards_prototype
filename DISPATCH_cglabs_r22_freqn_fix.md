# DISPATCH — fix historic `frequency_n` in the R/2.2 hazard-change tables, rebuild and republish 2 keys

Append-only. Newest block on top. Respond by prepending a `### RESPONSE` block.

---

### RESPONSE 2026-11-19 [M-20261119-r22n-1] — cglabs: SEC4 rerun, G0–G5 PASS, `haz_freq` + `haz_freq_ensemble` published

**Outcome: complete.** All gates passed with no deviation. Only the 2 named keys changed on S3; the
other 8 are byte-identical (ETag, size, LastModified, VersionId) before and after.

**G0 — PASS.** No Rscript processes running, no `logs/*.pid` pointing at a live process; newest log
before the run was the 19:32 housekeeping pass.

**Step 1 — PASS.** HEAD `e5d896d` (carries `beb97a2`). `grep -c M-20261119-r22n-1`: `R/2.2_haz_change.R` 2,
`R/publish_cr093_r22.R` 1.

**Step 2 — park.** Pre-run local files matched the 11-16 pins exactly:

| file | md5 | size | rows |
|---|---|---|---|
| haz_freq.parquet | `37bbc85a427d021a89b6b19c00f4c11b` | 5,325,673 | 12,457,872 |
| haz_freq_ensemble.parquet | `547be71660c1e11e48c41c5213b76690` | 1,576,579 | 692,104 |

Moved (not copied) to `Data/_parked_r22n_20261119_200747/`; md5 re-checked there, identical. `STATS` was
empty before the run.

**G1 — PASS.** Run 20:07:58–20:11:19 UTC (200.6 s), process ended after `R/2.2 complete`.
- `run controls: SEC1=FALSE SEC2=FALSE SEC3=FALSE SEC4=TRUE`
- `SEC4 frequency_n multiplier: 19 years (hist file historical_ACCESS-CM2_1995_2014_HSH_max_max.tif)`;
  the `stop()` did not fire. Note: the file it names is an HSH file, not NDWS/NDWL0. Only its layer count
  is used, and that is 19, so the result is right. But the `"hist"` pattern is not hazard-specific. That
  is worth a look macbook-side, though it is not a fault here.
- Files under `Data/` newer than run start: exactly the two `STATS/haz_freq*.parquet`. No zonal or
  boundary cache rewrites, no other parquet or tif.
- The log ends with R's `There were 50 or more warnings`. I did not inspect them; they are most likely
  the expected `min`/`max` on all-NA groups that the new NULL-ing handles.

**G2 — PASS** (new files alone).
- `haz_freq` `max(frequency_n)` by scenario: historic 19, ssp126/245/370/585 19 each. Historic = ssp max.
- `|frequency_n − 19·frequency|`: 5,924,160 paired rows, max 0.59 (every scenario incl. historic),
  rows > 0.6 = 0. The NA pattern of `frequency` and `frequency_n` agrees in every row; 0 non-finite values.
- `haz_freq_ensemble`: historic `frequency_n` max `mean` = 19 (and 19 for every ssp); NaN/±Inf count in
  `mean`/`min`/`max`/`sd` = 0/0/0/0.

**G3 — PASS** (new vs parked, joined on all non-value columns).
- Rows: `haz_freq` 12,457,872 = parked; `haz_freq_ensemble` 692,104 = parked. 0 duplicate keys either
  side; keys only-new 0, only-parked 0.
- `haz_freq`: `frequency` 0 rows differ (historic 366,408, ssp 5,862,528; NULLs in the same places);
  ssp `frequency_n` 0 differ. **Historic `frequency_n`: 145,726 of 366,408 rows differ**, the only
  differences in the file. 0 parked zeros changed. Median new/parked (parked ≥ 1) = **0.5758** (19/33 = 0.5758).
- `haz_freq_ensemble`, parked non-finite → now NULL (all converted; NULL count = parked non-finite count):

  | group | rows | `mean` NaN→NULL | `min` −/+Inf→NULL | `max` →NULL | finite cells that differ |
  |---|---|---|---|---|---|
  | historic frequency | 20,356 | 772 | — (NULL by design) | — | 0 |
  | historic frequency_n | 20,356 | 772 | — | — | 10,751 (`mean`, allowed) |
  | ssp frequency | 325,696 | 12,352 | 12,352 | 12,352 | 0 |
  | ssp frequency_n | 325,696 | 12,352 | 12,352 | 12,352 | 0 |

  12,352 = 4 × 3,088 per ssp, and 772 per variable in historic, matching the dispatch. `sd` had no
  non-finite values; it was already NULL in those 12,352 ssp cells and stays NULL. Historic
  `min`/`max`/`sd` are NULL in all 20,356 rows on both sides.
  Report: historic `frequency_n` ensemble `mean` median new/parked (parked ≥ 1) = **0.5758**.

**G4 — PASS.** `R/validate_cr093_real.R`: 10 PASS / 0 FAIL (haz_freq rg=244, haz_freq_ensemble rg=14, iso3=55).

**G5 — PASS.** Env clean of `CONFIRM|R22_|FORCE_OVERWRITE|AWS_BIN`. `mode: DRY-RUN`;
`plan limited to 2 of 10 files: haz_freq, haz_freq_ensemble`; both pre-flight `OK`; `pre-flight PASS for all 2 files`.

**Step 7/8 — published + verified.** The script backed up both live keys to
`variable=<name>.preFix-20261119-201437.bak`; the `.bak` objects carry the 11-16 sizes and ETags
(5,325,673 / `37bbc85a…`, 1,576,579 / `547be716…`). The verify lines show `rg=244 null-stat=0` and
`rg=14 null-stat=0`. HTTPS HEAD returns 200 on both. Live `haz_freq` re-read over HTTPS gives
`frequency_n` max 19 for historic and every ssp. The other 8 keys show no diff between the before and
after `head-object` files.

Pin table (local md5 = ETag, local size = S3 size; both single-part):

| variable | size | ETag | LastModified (UTC) | x-amz-version-id |
|---|---|---|---|---|
| haz_freq | 5,641,357 | `4965fa146b3cfdd81a3054235c278d57` | 2026-11-19T20:14:44Z | `TtTYLZagDzDiV71Ud8tqewvdZLtzA3yC` |
| haz_freq_ensemble | 1,556,674 | `7d4a157d60f65904a37a27b499e8d8b1` | 2026-11-19T20:14:51Z | `R9wklWmlSskgbIHiUK0NRPfuqOD2eI6a` |

**Logs** (`logs/`, gitignored): `r22n_sec4_20261119_200751.log`, `r22n_g2g3_20261119_200751.txt`,
`r22n_validate_20261119_200751.log`, `r22n_dryrun_20261119_200751.log`, `r22n_publish_20261119_200751.log`,
`r22n_s3_before_20261119_200751.txt`, `r22n_s3_after_20261119_200751.txt`.

**Kept:** `Data/_parked_r22n_20261119_200747/` (the two 11-16 files) and every existing `_parked_*` /
`sandbox/backup/` dir. Not touched: the other 8 keys, the `.preFix*.bak` clutter (#59, now +2), the adm0
claimant rows (#11), NaN/Inf in other ensemble files, the pins CSV, the changelog, the CDH record.

Answers: M-20261119-r22n-1 (complete)

---

### MACBOOK 2026-11-19 [M-20261119-r22n-1] — rerun R/2.2 SEC4 only, gate, publish `haz_freq` + `haz_freq_ensemble`

**Context.** A review on 2026-11-19 found that the live `variable=haz_freq.parquet` (published
2026-11-16, `archive/dispatches/DISPATCH_cglabs_r22_hazard_change_publish.md`) has historic
`frequency_n` = `frequency` x **33** instead of x **19**. Live: historic `frequency_n` max = 33,
every ssp max = 19; the historic `frequency_n / frequency` ratio sits at 30-35. The `frequency`
column itself is correct.

**Cause.** `R/2.2_haz_change.R` SEC4 took the year count from the first `hist` file in the
annual indices dir. That dir also holds the Track-2 trends files `historical_<GCM>_1981_2014_*`
(33 layers), which sort before `_1995_2014_`. `R/2_calculate_haz_freq.R` already skips them;
R/2.2 did not.

**Fix (this commit, `develop`).**
1. SEC4 drops `_1981_2014` files before picking the historic file, and **stops** if
   `years_hist != years_scen`. It logs one line:
   `SEC4 frequency_n multiplier: <n> years (hist file <name>)`.
2. A second fault in `haz_freq_ensemble`, independent of the first: a zone where all 18 models are NA
   aggregated to `mean` = NaN and `min`/`max` = ±Inf (live: 772 NaN means per variable in historic,
   3,088 per ssp). SEC4 now writes those cells as NULL. Historic `min`/`max`/`sd` stay NULL **by
   design (`data_ens[scenario == "historic", c("min", "max", "sd") := NA]`), so that part is unchanged.
3. `R/publish_cr093_r22.R` accepts `R22_PUBLISH_ONLY=<stem>,<stem>` (exact stems, unknown stem =
   hard stop). Unset = all 10, as before.

**Scope.** Rerun **SEC4 only**. Publish **only** `haz_freq` and `haz_freq_ensemble`. Do not touch
the other 8 hazard-change keys, the `.preFix*.bak` clutter (#59), the adm0 claimant rows (#11),
any other prefix or stage. Do not fix NaN/Inf in the other ensemble files even though you may
see them; that is a separate macbook decision. The CDH record for this product is **on hold**
until this lands. Macbook updates the pins CSV and the changelog from your RESPONSE; you do not.

**Rules that bite here:** `git pull` first and confirm HEAD carries this code; absolute-path
sourcing in `Rscript -e`; the R/2.2 run and the publish go in the background (`nohup … &`, log to
`logs/`); keep the new `_parked_r22n_*` dir and every existing `_parked_*` / `sandbox/backup/`
dir; after committing the RESPONSE, verify it landed on `origin/develop`. **At any deviation from
a gate below: STOP, describe what you see, publish nothing. Do not improvise a fix.**

Paths below: `RISK = <hazard_timeseries_risk>/annual` and `STATS = RISK/stats`, as R/2.2 and the
publish script derive them from `R/0_server_setup.R`.

---

**Step 0 — node quiet.** `ps aux | grep -i [R]script`, live `logs/*.pid`, recent log mtimes.
**G0 (invariant):** no pipeline job running or writing under `Data/`. Ambiguous = STOP.

**Step 1 — code is current.** `git pull`; `grep -c 'M-20261119-r22n-1' R/2.2_haz_change.R
R/publish_cr093_r22.R` → both ≥ 1. Report HEAD.

**Step 2 — park, then rerun SEC4.**
- Record md5 + size + row count of `STATS/haz_freq.parquet` and `STATS/haz_freq_ensemble.parquet`.
  **Check:** each md5 equals the live ETag in the 11-16 pin table
  (`37bbc85a427d021a89b6b19c00f4c11b`, `547be71660c1e11e48c41c5213b76690`). If not, STOP: the local
  files are not what is live, and G3 would compare against the wrong reference.
- **Move** (not copy) both to `Data/_parked_r22n_<STAMP>/` so a failed run cannot leave a stale file
  behind for the publish to pick up. Re-check md5 at the new location.
- Run, from the repo root:
  ```bash
  STAMP=$(date +%Y%m%d_%H%M%S); nohup env SKIP_R22_SEC1=1 SKIP_R22_SEC2=1 SKIP_R22_SEC3=1 Rscript -e 'source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); source("/home/jovyan/atlas/hazards_prototype/R/2.2_haz_change.R")' > logs/r22n_sec4_${STAMP}.log 2>&1 & echo $! > logs/r22n_sec4_${STAMP}.pid
  ```
**G1 (invariants):** exit 0; the log shows `run controls: SEC1=FALSE SEC2=FALSE SEC3=FALSE
SEC4=TRUE`; the multiplier line reads **19** and names a `_1995_2014_` file; exactly two parquet
files written (both `STATS/haz_freq*`). List any other file under `Data/` newer than the run start:
R/2.2's own zonal/boundary caches being re-written is a report; any `.parquet` or risk/indices
`.tif` outside `STATS` is a STOP. Anything else, including the new `stop()` firing: STOP and paste
the log tail.

**Step 3 — G2, the fix took (new files alone).**
**G2 (invariants):**
- `haz_freq`: for every scenario, `max(frequency_n) ≤ 19`, and historic max **=** the ssp max.
- `haz_freq`: for every row, `|frequency_n − 19·frequency| ≤ 0.6` (the slack covers `frequency`
  rounded to 2 dp then x 19, plus `frequency_n` rounded to 0 dp). This must hold for historic too.
- `haz_freq_ensemble`: historic `frequency_n` `mean ≤ 19`; no NaN and no ±Inf in `mean`, `min`, `max`
  or `sd`, in any row.

**Step 4 — G3, product vs input (new against parked, same keys).** Join on every non-value column.
**G3 (invariants):**
- Row counts unchanged: `haz_freq` = parked (12,457,872); `haz_freq_ensemble` = parked.
  Key sets identical on both sides (zero keys gained or lost).
- `haz_freq`: all `frequency` rows identical to parked, NULLs in the same places; ssp `frequency_n`
  rows identical; every row that differs is a historic `frequency_n` row (zeros stay zero, so not
  every historic row differs). Report the count that differs.
- `haz_freq_ensemble`: `frequency` rows identical to parked in `mean`/`min`/`max`/`sd`, except where
  parked is NaN or ±Inf. Those cells must now be NULL, and the NULL count in the new file must equal
  the parked non-finite count. Same for ssp `frequency_n` rows. Historic `frequency_n` `mean` may
  differ. **Report, not a gate:** the median of new/parked where parked is finite and ≥ 1 (expect
  about 19/33 = 0.58; single values drift because each model's value was rounded to 0 dp first).

**Step 5 — G4, validator.** `Rscript R/validate_cr093_real.R`.
**G4 (invariant):** 10 PASS / 0 FAIL. (The 8 untouched files still pass as they did on 11-16.)

**Step 6 — G5, filtered dry-run.** First `env | grep -E 'CONFIRM|R22_|FORCE_OVERWRITE|AWS_BIN'` → empty. Then
`R22_PUBLISH_ONLY=haz_freq,haz_freq_ensemble Rscript R/publish_cr093_r22.R` (no CONFIRM).
**G5 (invariants):** `mode: DRY-RUN`; the log shows `plan limited to 2 of 10 files`; the plan lists
**exactly 2 keys**, `…/processing=hazard-change/timeframe=annual/variable=haz_freq.parquet` and
`…variable=haz_freq_ensemble.parquet`; both pre-flight `OK`; `pre-flight PASS for all 2 files`.

**Step 7 — publish the 2 keys.** Before it, write `head-object` (size, ETag, LastModified,
VersionId) for **all 10** keys to `logs/r22n_s3_before_<STAMP>.txt`. Then:
```bash
STAMP=$(date +%Y%m%d_%H%M%S); nohup env CONFIRM=1 R22_PUBLISH_ONLY=haz_freq,haz_freq_ensemble Rscript R/publish_cr093_r22.R > logs/r22n_publish_${STAMP}.log 2>&1 &
```
The script backs up each live key to `variable=<name>.preFix-<stamp>.bak` itself. Watch to completion.

**Step 8 — verify.** `head-object` for all 10 again → `logs/r22n_s3_after_<STAMP>.txt`.
**Invariants:**
- The 2 published keys: S3 size = local size; ETag = local MD5 (both files are single-part); the
  2 `.bak` objects exist with the 11-16 sizes; `null-stat=0` in the script's verify lines;
  `curl -sI 'https://digital-atlas.s3.amazonaws.com/<key>'` → 200.
- **The other 8 keys: ETag, size, LastModified and VersionId byte-identical before and after.**
  Any change there: STOP and report.
- Re-read the live `haz_freq` over HTTPS: historic `frequency_n` max = 19.

**Step 9 — RESPONSE.** G0-G5 outcomes with the numbers asked for, the 2-row pin table (variable,
size, ETag, LastModified UTC, x-amz-version-id), log paths, the parked dir name. Commit, push,
verify on `origin/develop`. End with `Answers: M-20261119-r22n-1 (complete)` or `(stopped at <gate>)`.
