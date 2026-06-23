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

## TODO (fix + validate against real Data/, then commit each)
1. **SEC1 `historical_` prefix** — TWO historic conventions coexist:
   `historic_historic_historic_<GCM>_...` AND `historical_<GCM>_...` (36 files).
   `.extract_gcm` only strips `historic_historic_historic|ssp[0-9]+` → the 36
   `historical_` files mis-parse and get skipped. FIX: add `historical` to the
   prefix-strip alternation in `.extract_gcm`.
2. **SEC1 `_sd` files** — SEC1 ingests `PTOT-sum_sd` alongside `_mean`; PTOT %
   area change is mean-only. FIX: `files <- files[grepl("_mean", files)]`.
3. **admin_extract API MIGRATION (deep, the real blocker)** — `haz_functions.R`
   (github main, line 1527) changed
   `admin_extract(data, Geographies=, FUN=)` ->
   `admin_extract(data, boundaries_zonal, boundaries_index, FUN=, max_cells_in_memory=)`.
   R/2.2 errors: `unused argument (Geographies = Geographies)`. FIX: mirror
   **R/2.1_create_monthly_haz_tables.R:111-132** — build `boundaries_zonal`
   (per-geography rasterized `<name>_zonal.tif` cached in `boundaries_int_dir`) +
   `boundaries_index` (data.frame zone_id->iso3/admin_name/gaul_code per geography),
   then update ALL admin_extract calls in R/2.2 (SEC1 x3, SEC2 x2, SEC3 x2, SEC4 x1)
   and verify `merge_admin_extract` output handling (zone_id join) matches R/2.1.
4. **SEC2/3/4 stale parsers** — `gsub("historical", "historical_historical_historical")`
   at ~354/447/530 should target `historic` (real prefix); suffix-strip + `[,c(1:3,5)]`
   field-selection assume old naming. Validate each vs real risk filenames. Filename
   grammar is already documented — `scenario_model_timeframe_<haz-dashed>[_stat].tif`,
   `_`-split, GCMs dashed, years `YYYY-YYYY`, historic prefix 1 or 3 tokens. Don't
   re-dump to rediscover.

## Gate before publish
Full run (drop SKIP flags) -> `Rscript R/validate_cr093_real.R` -> require
`GATE PASSED` (iso3 present/non-NA/>=2 distinct, 0 null row-group stats, ensembles
have mean/min/max/sd) -> only then wire publish to canonical `domain=` path.
Validate-real-artifact + early-kill-gate discipline applies (no >10-min run on
changed code without validation). Commit each fix to develop so the macbook stays
in sync.
