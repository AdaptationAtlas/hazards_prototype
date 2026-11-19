# DISPATCH — fix historic `frequency_n` in the R/2.2 hazard-change tables, rebuild and republish 2 keys

Append-only. Newest block on top. Respond by prepending a `### RESPONSE` block.

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
