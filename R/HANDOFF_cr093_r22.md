# CR-093 — R/2.2 revival handoff (for the cglabs claude session)

Date: 2026-06-23. Goal: make `R/2.2_haz_change.R` outputs a single canonical,
notebook-consumable source (iso3-prunable + pushdown). R/2.2 was unused, so its
filename handling + admin API are stale vs current `R/2` output and
`haz_functions.R`. Run claude IN `~/atlas/hazards_prototype` on cglabs (live
`Data/`, live `haz_functions.R`, `Rscript` — validate in-place, no git round-trip).

## State
develop @ origin: `f683377` (SEC1 dir/pairing/parse fix) + `b799707` (R/2 perf).
SEC1 stage-test (`SKIP_R22_SEC2=1 SKIP_R22_SEC3=1 SKIP_R22_SEC4=1`) now gets PAST
the old leaked-global crash and builds change/diff stacks, then hits the bugs below.

## DONE (SEC1, committed)
- Self-contained input dirs in section 0 at the `annual` axis (override
  `R22_TIMEFRAME`): `haz_mean_dir`, `haz_time_risk_dir`, `haz_timeseries_dir` were
  leaked globals from R/2's per-timeframe loop (R/2:656-664).
- SEC1: filter to canonical hyphen-year `YYYY-YYYY` (drops ~18 stale `YYYY_YYYY`
  historic dups); pair historic->future by GCM token; suffix-strip parse for
  `PTOT-sum_mean`.

## DONE 2026-06-23 (committed to develop: 0b82b4e, c3e13b5) — SEC1 GATE PASSED
SEC1 runs end-to-end on live Data/; all 4 ptot_* outputs PASS
`Rscript R/validate_cr093_real.R` (4 PASS / 6 FAIL = SEC2/3/4 not yet produced).
1. **SEC1 `historical_` prefix** — DONE. `.extract_gcm` now strips `historical`
   too. Both `historic_historic_historic_<GCM>_` and stale `historical_<GCM>_`
   name the SAME 18 GCMs (full overlap), so the naive "add historical" fix would
   DOUBLE-count — added GCM dedup after parse (keeps canonical hhh form).
2. **SEC1 `_sd` files** — DONE, but the suggested `grepl("_mean", files)` is a
   NO-OP: `files` are full paths and the parent dir is `hazard_timeseries_mean`,
   so it matches everything. Anchored on the suffix: `grep("_mean[.]tif$", files)`.
   (This was the real row-doubling + NaN/Inf source, not just cosmetic.)
3. **admin_extract API MIGRATION** — DONE. boundaries_zonal/boundaries_index
   built mirroring R/2.1:111-132; all 8 calls switched. Extra vs R/2.1: must
   dedup boundaries_index by zone_id (GAUL2024 dup polygons would double rows ->
   cartesian join) and join area by gaul code, not admin name (names not unique).
   merge_admin_extract is now a pass-through; write_chg_parquet strips gaul*.

### Known pre-existing artifact (NOT a regression; decide before publish)
ptot_change_by_model has ~17k NaN/Inf `value` rows in zero-precip deserts
(e.g. EGY Al-Kharga Oasis): the change raster is `round(100*d/past,1)` and
`past≈0` blows up. Untouched science (R/2.2 ~L207). Gate doesn't check ranges.
Also ~1728 rows look like dup keys when grouped by admin NAME only — genuine
GAUL2024 duplicate names across distinct gaul codes, not a pipeline bug.

## TODO (fix + validate against real Data/, then commit each)
Items 1-3 DONE + validated above (commits 0b82b4e, c3e13b5). Remaining:

2b. **SEC1 ENSEMBLE filter** — DONE (parity with SEC3/4). SEC1 now drops
   `ENSEMBLE` files before pairing. On current Data/ the by-model output was
   already clean (no historic ENSEMBLEmean exists, so the future ENSEMBLEmean
   never paired), but the filter is added defensively so a future producer
   change can't leak model="ENSEMBLEmean" into ptot_change_by_model.
2c. **terra-probe on cglabs** — OPEN (unrelated to R/2.2). `USE_R2_5_2_VEC`
   defaults ON; the §5.2 vectorize probe (`Rscript R/probe_r2_5_2_vec.R`) only
   ran on macbook terra. Run ONCE on cglabs to confirm terra::mean/stdev parity
   before a multi-hour §5.2 bake; fallback `USE_R2_5_2_VEC=0`.
4. **SEC2/3/4 stale parsers + naming** — DONE + validated (commit 203bac9).
   Risk dir is dash-delimited with a 1-token `historic` prefix
   (`historic_ACCESS-CM2_1995-2014_THI-max-max-G82.tif`). Fixed: file patterns
   (`THI-max`), threshold codes (`THI-max-max-G`, `NTxNN-mean-G`, `NDWS-mean-G`),
   a shared `.parse_risk_vars()` (plain `_`-split, take scenario/model/timeframe
   + trailing severity — kills the `.G`->`_` field-shift that made severity="1"),
   `"historical"` scenario literals -> `"historic"`, `seq_along(choices)` ->
   `seq_len(nrow(choices))`, and SEC2 now drops ENSEMBLE. NOTE: ntx_perc_by_model
   still carries extra `area`/`total_area` columns (pre-existing, harmless,
   notebook uses `value`); could be trimmed later.

## STATUS 2026-06-23: FULL GATE PASSED (10/10)
Full run (all sections) on live Data/ -> `Rscript R/validate_cr093_real.R` =
**10 PASS / 0 FAIL**. All R/2.2 outputs are iso3-bearing + prunable + ensembles
carry mean/min/max/sd. Safe to wire publish to the canonical `domain=` path.
Remaining open item: 2c (terra §5.2 probe on cglabs — unrelated to R/2.2).

### Known pre-existing artifact carried into all % outputs
ptot/thi/ntx %-area and haz_freq `frequency_n` have NaN in zero-precip/zero-area
zones (the `100*x/total` and `100*d/past` patterns with total/past≈0; ~15-30k
rows each). Untouched science. Gate doesn't check value ranges. Decide whether to
NA-clean before publish.

## Gate before publish (procedure)
Full run (drop SKIP flags) -> `Rscript R/validate_cr093_real.R` -> require
`GATE PASSED` (iso3 present/non-NA/>=2 distinct, 0 null row-group stats, ensembles
have mean/min/max/sd) -> only then wire publish to canonical `domain=` path.
Validate-real-artifact + early-kill-gate discipline applies (no >10-min run on
changed code without validation). Commit each fix to develop so the macbook stays
in sync.
