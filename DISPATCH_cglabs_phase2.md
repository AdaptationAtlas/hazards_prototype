# Dispatch: Stage-0 Phase-2 — bake hazard variables (02→04, hazards_upstream)

**Scope narrowed (Pete, 2026-06-25): we only need the HAZARD VARIABLES (monthly index tifs). NO long-term stats, NO annual stats, NO metadata.**
So this bake runs **02→04 only. SKIP 05_final_maps (long-term), 06_metadata, 07_bucket_uploads.**
Head = origin/develop `0d1e036`. Two-session rule: macbook fixes code, cglabs runs (real `~/common_data`).

## Deliverable
`04_indices` per-month hazard-variable rasters:
`$COMMON_DATA/nex-gddp-cmip6_indices/<scenario>_<gcm>/<INDEX>/<INDEX>-YYYY-MM.tif`
(NDD, NTx35/40, HSH, HSM_NTx35, THI, NDWS, NDWL0/50, PTOT, TAI, TAVG, TMAX, TMIN). That's it — the month-resolution index files ARE the month summaries. `05_final_maps`/`06_metadata` produce the long-term/annual/metadata products we explicitly do **not** need.

## Pull
```bash
cd <hazards_prototype>/hazards_upstream/R
git checkout develop && git pull        # head 0d1e036; DO NOT create branches (standing rule)
export COMMON_DATA=<your real data root>
```
04 reads bias-corrected daily NEX-GDDP under `$COMMON_DATA/nex-gddp-cmip6/<var>/<scenario>/<gcm>/`. Run 02/03 first **only if** those inputs are missing; if present (smoke used them), go straight to 04.

## 2a — runtime gate FIRST (mandatory, ~1 GCM / 1 month)
The existing 04 gate validates the migration on live Data/ at minimal scope (NDWS, ACCESS-ESM1-5, 1995-01) and checks compute + should_skip + FORCE_OVERWRITE + loud-fail:
```bash
bash 04_indices/gate_phase2_ndws.sh        # GATE_GCM=... COMMON_DATA=... to override
```
Exit 0 = PASS (migration good, sweep 04). Non-zero = FAIL: capture the gate log, STOP, report. Do not patch blind.
(Note: the gate's own summary line still says "sweep 01/02/03/05/06" — ignore; per this dispatch we sweep 02→04 only.)

## 2b — sweep 04_indices (only if 2a passes)
Run every `04_indices/calc_*.R` / `fast_calc_*.R` across the full set, both scenarios:
```bash
# historical
SCENARIO=historical Rscript 04_indices/calc_NDD.R    # ... and each other index
# future (expands to all SSPs via cfg_ssps)
SCENARIO=future     Rscript 04_indices/calc_NDD.R    # ...
```
Use your normal full GCM set (unset GCMS = the script default). NDWS/NDWL have an AVAIL state dependency — run their months in order (the gate covers the 1995-01 seed). Watch per-stage for residual hardcoded paths / missing-object errors; logs are timestamped — note per-script elapsed.

## Report back (edit this file + commit)
- 2a gate: PASS/FAIL + summary line
- 2b: which indices completed, any errors with file:line
- spot-check a few output tifs exist under `$COMMON_DATA/nex-gddp-cmip6_indices/`

Do not push code fixes without flagging the diff first.

---
## ✅ 2a PASS + 2b SWEEP PASS (cglabs 2026-06-26, HEAD 45159bd)
- **2a gate** (`gate_phase2_ndws.sh`): `GATE SUMMARY: 9 passed, 0 failed`, exit 0.
- **2b — full historical sweep, all 12 `04_indices` scripts, default 18-GCM set,
  no FORCE:** every script exit 0, 0 error lines.
  | script | exit | elapsed | script | exit | elapsed |
  |---|---|---|---|---|---|
  | calc_NDD | 0 | 13s | calc_TAI | 0 | 13s |
  | calc_NTx | 0 | 42s | calc_HSH | 0 | 15s |
  | calc_PTOT | 0 | 12s | calc_THI | 0 | 16s |
  | calc_TAVG | 0 | 13s | fast_calc_NDWS | 0 | 835s |
  | calc_TMAX | 0 | 13s | fast_calc_NDWL0 | 0 | 827s |
  | calc_TMIN | 0 | 12s | fast_calc_NDWL50 | 0 | 820s |
  The 3 `fast_calc_*` did real fresh compute across all 18 GCMs (~46s/GCM,
  AVAIL-ordered) — not just skips; calc_* fast where outputs already existed.
- **Outputs verified** under `$COMMON_DATA/nex-gddp-cmip6_indices/`: e.g.
  `historical_TaiESM1/NDWS` 816 tifs, `…/PTOT` 480.
- **Future code path validated** (`SCENARIO=future` calc_PTOT, ACCESS-ESM1-5):
  exit 0, `scenario=future yrs=2021:2100`, SSP expansion works. Future indices
  already populated (ssp126/245/370/585 NDWS = 1920 tifs each /GCM).
- **INCOMPLETE-GCM concern (coverage.csv) did NOT bite** historical: all 18 GCMs
  ran clean (sufficient historical coverage / existing outputs skip).
- **Migration VALIDATED on real data, 02→04.** Note: indices were already
  comprehensively baked (prior run, `indice_completion_2025-07-22`), so this was
  a clean re-validation + gap-fill, not a from-scratch bake.
- **NOT done (by design / optional):** a full FORCE re-bake, and a full *future*
  fast_calc re-sweep across 18 GCMs × 4 SSPs (hours of NDWS/NDWL fresh compute;
  outputs already present). Run as a scheduled background job only if a full
  refresh is actually wanted — say the word.

## Log (newest first)
- 2026-06-25 — **scope narrowed to 02→04** (hazard variables only; drop 05/06). Dispatch rewritten.
- 2026-06-25 `0d1e036` (macbook) — fix `hazards.r_root` clobber: ofile-scan now matches the `00_setup.R` frame (not any outer ofile), so a sibling re-sourcing setup can't re-root to its subdir. Fixes the 2a path-doubling. (Affected 05/06 path; now out of scope but fixed.)
- 2026-06-25 (cglabs) — 2a (meta_NDWS, old 06-scoped gate) FAILED: `hazards.r_root` clobber doubled `05_final_maps/`. Scoping itself worked. → fixed in 0d1e036; gate replaced by the 04 gate above.
- 2026-06-25 `63c7362` (macbook) — run-controls fix: 05/06 calc honor GCMS/SCENARIO; meta block-2 setdiff + guarded historical row; unset = byte-identical legacy.
- 2026-06-25 `96061c5` (cglabs) — SMOKE RE-RUN PASS (meta_NDWS via repo-relative getOption; real compute; hit 400s cap mid-block-1, so block-2 was never reached → the clobber hid until 2a scoped it fast enough).
- 2026-06-25 `cfc4039` (macbook) — cross-stage source() fix: `00_setup.R` self-locates + stores `hazards.r_root` option (survives rm(list=ls())).
- 2026-06-25 (cglabs) — first SMOKE FAILED: meta_*.R sourced `~/Repositories/hazards` sibling-clone path (absent). → cfc4039.
- earlier — 01–06 migrated to 00_setup.R; `5fbb877` dropped rm(list=ls()) that wiped setup in 4× 02_preprocess.
- Non-blocking: cglabs box `libtiff.so.6: LIBTIFF_4.6.1 not found` GDAL warning on terra load — box env issue, didn't stop runs.
