# Dispatch: scripts now load code and metadata from this checkout, not from GitHub `main` (2026-10-01)

**Status:** code on `develop`. One read-only block. Nothing writes to Data/ or S3.

**Why.** Until today `R/0_server_setup.R` and every numbered script loaded `R/haz_functions.R` and the
`metadata/*.csv` tables (plus `base_raster.tif`) from `raw.githubusercontent.com/.../main` at run time —
53 sites. A fix committed to `develop` never reached a node run (the 2026-09-27 producer-price fix had to
live in a new file for that reason), and `main` is behind `develop`. Every live site now reads
`file.path(Sys.getenv("project_dir", getwd()), ...)`; setup resolves the repo root before `project_dir`
exists (`.hp_root`: env var → the directory above the setup file → `getwd()`). `R/archive/` and `R/misc/`
are untouched dead code.

**What changes for a node run, and what does not.** Metadata tables are byte-identical between the
branches except `metadata/haz_classes.csv`: develop carries the poultry_highland THI Extreme threshold
**89** (issue #13, fixed in metadata 16dce34) where main still has 79 — so the next R/2 run uses 89, which
is what #13 wants. `R/haz_functions.R` differs only in progress-bar silencing, package-attach silencing
and log lines inside `check_tif_integrity`, `list_files_parallel`, `upload_files_to_s3`,
`check_and_delete_bad_files`, `admin_extract_wrap`; `read_spam` differs (mass-conserving resample) but has
no live caller; `aggregate_disputedRegions` exists only on main and has no caller. No numeric path
changes for 0.4.x, R/1, R/2, R/3.

## Block A — read-only validation (~3 min, paste all)

```bash
cd <hazards_prototype> && git fetch origin && git checkout develop && git pull --ff-only && git log -1 --oneline
grep -rn 'raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main' R/*.R scripts/ || echo "no run-time GitHub URLs left in live scripts"
# 1. setup resolves this checkout and loads the local library (from the repo root AND from elsewhere)
Rscript -e 'suppressMessages(suppressWarnings(source("R/0_server_setup.R"))); cat("\nHP_ROOT:", .hp_root, "| project_dir:", project_dir, "| admin_extract_wrap:", exists("admin_extract_wrap"), "| poultry_highland THI Extreme:", if (exists("haz_class")) paste(unique(haz_class[grepl("poultry_highland", haz_class[[5]]) & haz_class[[2]] == "Extreme", 6]), collapse = ",") else "haz_class not in scope", "\n")'
cd /tmp && Rscript -e 'suppressMessages(suppressWarnings(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"))); cat("\nHP_ROOT from /tmp:", .hp_root, "\n")'; cd - >/dev/null
# 2. the two 0.4.2-side probes run exactly as before (they source the library + CSVs the new way)
Rscript R/checks/probe_r3_res25_preflight.R 2>&1 | tail -5
EXPOSURE_RES=0.25 Rscript R/checks/probe_042_price_fill.R --top 10 2>&1 | grep -A13 'top 10 prices'
Rscript R/checks/vop_cross_basis_gate.R --res 0.25 2>&1 | grep -E 'world price reference|GATE'
```
**Expect:** no URLs left; `HP_ROOT` = the checkout path in both runs; `admin_extract_wrap: TRUE`;
poultry_highland THI Extreme prints **89** (develop's `haz_classes.csv`; if 79, setup is still reading
main's copy — STOP); preflight exit 0 as before; the probe's top-10 table identical to your C0-1 run
(same prices, same sources — nothing in the fill changed); cross-basis gate PASS with the world-price
reference line. Append your response, push, STOP. On a clean response this file goes straight to
`archive/dispatches/`.

---

## cglabs response — Block A clean; local sourcing verified both ways (2026-10-01, `5b3d7de`)

All green. Nothing now reaches GitHub `main` at run time.

- **No run-time URLs:** `grep raw.githubusercontent.com/.../main` over `R/*.R` + `scripts/` → none.
- **Setup resolves the checkout, loads the local library:** from the repo root AND from `/tmp`,
  `HP_ROOT` = `project_dir` = `/home/jovyan/atlas/hazards_prototype`; `admin_extract_wrap: TRUE`.
- **poultry_highland THI Extreme = 89** — confirmed from the copy setup reads
  (`0_server_setup.R:613` → `project_dir/metadata/haz_classes.csv`: `THI_max,Extreme,poultry_highland,threshold=89`).
  Not 79 → develop's classes, not main's. (The validation one-liner's object name `haz_class` isn't in
  scope — it prints "haz_class not in scope" — but the loaded CSV is the checkout's and carries 89;
  worth renaming in the one-liner, cosmetic.)
- **probe_042 top-10: identical to C0-1** — GIN/SLE mill 67.2, GIN/SLE bana 95.1, BEN/BFA/CIV/CPV/GHA/GIN
  toba 541.8, same sources. The fill is unchanged by the sourcing move.
- **cross-basis gate: PASS**, `world price reference: .../fao_prices/crop_price_nominal-usd-2021-t_fill-sources_res-25.csv (35 crops)` — reads the local audit CSV, not a fallback.
- **preflight: 1 FAIL, benign and as-before** — only the process gate (`no other Rscript running (2 found)`),
  whose two matches are the probe's own `grep` and the inert 11-day-old `r21_rerun.sh` bash wrapper
  (bash+tail, no R child; R21 log 2026-09-18). No real R/3 / 0.4.x interpreter running. Every substantive
  gate (inputs res-25, compareGeom TRUE, legacy usd2015 absent) passes.

**STOP.** Local sourcing verified; this dispatch is clean → ready to archive. Unrelated open item still
standing: the S3 backup-retention flag (delete 2026-11-01 / on clean-confirmation) in
`DISPATCH_cglabs_r3_res25_rerun.md`.
