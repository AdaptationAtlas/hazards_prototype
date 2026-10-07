# Archived dispatches, plans and handovers

Closed threads. Kept because they carry the reasoning behind decisions that are still
load-bearing, and because the RESPONSE blocks are the only record of what was actually
run on the node. Nothing here is live — do not act on instructions in these files.

Anything still in flight lives at the repository root.

| File | Thread | Outcome |
|---|---|---|
| `DISPATCH_cglabs_avail_fix.md` | Track-1 NDWS de-saturation → live Atlas (Jun–Jul 2026) | Pipeline recovery complete; its publish half was superseded by the issue-9 dispatch, which shipped 2026-09-16 |
| `DISPATCH_cglabs_server_environment.md` | CGlabs environment note | Delivered as `server-environment-cglabs.md` |
| `DISPATCH_cglabs_pop_denominator_fixes.md` | Tier-16 exposure denominator: stray `i.pop_source` column (#42) and the `pop_method` mislabel (#44), Oct 2026 | Both closed. Tier 16 re-levelled and republished 2026-10-06, verified on S3: national `pop_total` 52,837,534, `pop_method` `county-growth-from-2020`, no `i.` columns. No numeric value moved. Block B (#43, move the default to `county-level`) stays parked there, gated on #33 |
| `INSTRUCTIONS_cglabs_block_a.md` | Peer-session wrapper inlining Block A of the denominator dispatch, Oct 2026 | Superseded twice over (Block A stopped at its gate; A2 and A3 replaced it) and left pointing at a path that moved into this directory. Archived with its thread — the dispatch itself was always the handoff |
| `DISPATCH_cglabs_ke39_exposure.md` | KE-39 exposure layers | 7/7 layers live (population, admin, roads, facilities, grid) |
| `DISPATCH_cglabs_zonal_exposure.md` | Pre-cooked flood × exposure adm2 tables | Live, tier 16 |
| `DISPATCH_cglabs_knbs_population.md` | KNBS 2019 census + 2020-2045 projections, tier-16 re-level onto official denominators (issue #28) | Live; tiers 16/17/18. Tier 16 final = year-matched (`POP_YEAR_MATCH=1`). Follow-ups: #32 (WorldPop vs census), #33 (licence), #34 (season-year) |
| `DISPATCH_cglabs_issue26_r21_rebake.md` | R/2.1 full re-bake: GCM-pin + historic-collapse + baseline-mislabel (issue #26) | Live; 18-member ensembles, both baselines, 19 S3 keys published + verified 2026-09-20. Gate relaxed to accept `n_models ∈ {0,full}` (0 = all-NaN structural) |
| `DISPATCH_cglabs_issue30_intld_vintage.md` | intld15 vintage mismatch on the exposure reference (issue #30) | RESOLVED 2026-09-25. Reference republished at both resolutions (`crop-livestock_all_res-05` / `_res-25` + legacy alias), verified from S3; live gate PASS (livestock 1.198 → 1.000, unit `intld15-2021`). Root cause was the 0.25° zonal grid (~70% row deficit); fix ran 0.4.4's zonal step on the 0.05° base regardless of `climdat_source`, kept `unit_full`, reverted path-synthesised hive columns, refreshed a stale adm0 parquet, and made §2 refuse untagged glw tifs. Full A→J run trail in the RESPONSE blocks |
| `DISPATCH_cglabs_gfm_flood.md` | Sentinel-1 GFM observed flood | Live, tier 14 |
| `DISPATCH_cglabs_flood_ingest.md` · `FLOOD_ingest_plan.md` | JRC + GFD flood | Live, tiers 6–7 |
| `DISPATCH_cglabs_ndvi_ingest.md` · `NDVI_ingest_plan.md` | MODIS NDVI | Live, tier 5 |
| `DISPATCH_cglabs_wrsi_ingest.md` · `WRSI_ingest_plan.md` | FEWS WRSI cropland + rangeland | Live, tier 8 |
| `DISPATCH_cglabs_seasonal_rasters.md` · `DISPATCH_cglabs_ptot_overviews.md` | Seasonal PTOT, COG overviews | Live; overviews fixed and republished |
| `DISPATCH_cglabs_coverage_probe.md` | Global vs Africa coverage audit | Every upstream source is global; the Africa cut is ours, at three code sites — **node half (Q1-Q6) never run** |
| `DISPATCH_cglabs_phase2.md` · `DISPATCH_cglabs_sfcwind.md` | Upstream Stage-0 migration, wind availability | Validated on real data; `sfcWind` present for all 18 GCMs |
| `DISPATCH_poultry_thi_rebake.md` | poultry_highland THI threshold 79 → 89 | Metadata fixed; published outputs still need the re-bake, tracked as issue #13 |
| `DISPATCH_desert_mask.md` | Desert masking | Closed |
| `DISPATCH_cglabs_issue9_none_publish.md` | Issue #9: `hazard='none'` on every combination + publish the notebook-facing hazard_exposure (Sep 2026) | Published 2026-09-16, verified; #9 and #12 closed 2026-10-01 once the denominator existed on the product's grid (`crop-livestock_all_res-25`, #30): zero exceedances against it at every admin level. `Data/_parked_issue9/` deleted 2026-10-07 (clean-up Block C) |
| `DISPATCH_cglabs_local_sourcing.md` | Scripts load `haz_functions.R` and metadata from the checkout, not GitHub `main` (2026-10-01) | Verified on the node the same day: no run-time URLs, root resolved from repo root and elsewhere, develop's `haz_classes.csv` (THI 89) read, probe outputs unchanged |
| `HANDOVER_2026-10-01.md` | Session handover 2026-10-01 → 10-07 (#30 follow-ups: local sourcing, item-2 exposure pass, price method, clean-up) | Superseded by `HANDOVER_2026-10-07.md` (root) |
| `DISPATCH_pascal_issue29_profile.md` | PASCAL / Afrilabs host profile for #29 | Done: `metadata/hosts.json` afrilabs `verified` 2026-09-18 (63dc78a); #29 closed 2026-09-23 |
| `DISPATCH_cglabs_issue29_paths.md` | #29 path resolver on CGlabs (Blocks A-D) | A-C superseded by the delivered resolver + stage-ready gate; Block D (regeneration sizing) never run; #29 closed 2026-09-23 |
| `DISPATCH_cglabs_track1_ndws_resume.md` | Track-1 NDWS / hazard_exposure resume (2026-09-18) | Gap A (sidecars) shipped; C (intld publish) rides #13; B/D/E (period=annual, historic/ENSEMBLE models, value_sd) are publish-scope questions carried into `HANDOVER_2026-10-07.md` §2D |
| `HANDOVER_2026-09-17_ke-enso-population-schema.md` | Topic briefing: KE-ENSO population schema | Status block superseded by the 2026-10-06 re-level (`DISPATCH_cglabs_pop_denominator_fixes.md`, `HANDOVER_2026-10-06_ke-enso-exposure-denominator-answers.md`) |
| `HANDOVER_2026-09-18_cdh-standard-for-ke-enso-notebook.md` | Topic briefing: CDH standard for the KE-ENSO notebook | PR list stale; the live table is `metadata/cdh/README.md` |
| `DISPATCH_cglabs_wrsi_inversion_rebake.md` | WRSI cropland/rangeland inversion (REGION_MAP FEWS codes swapped) re-bake | COMPLETE 2026-09-22, 93/93 md5 verified on S3 |
| `ISSUE_26_republish_baseline_naming.md` | #26 R/2.1 GCM pin: republish + baseline naming plan | #26 closed 2026-09-20 (rebaked, published, verified) |
| `ISSUE_cr093_nan_zeroprecip.md` | CR-093 NaN at zero precipitation in R/2.2 | Closed 2026-06-24: 100 mm/yr desert mask live and published |
| `ISSUE_cr119_canonical_regression.md` | CR-119 canonical-parquet regression | Resolved 2026-06-12 (republished, pruned); the optional Phase-2 per-iso3 hive partitioning was never done |
| `DISPATCH_cglabs_cleanup_2026-10.md` | One clean-up pass for the #30 / item-2 threads: six retained `sandbox/backup/issue9_*` prefixes, the retired `vop_nominal-usd-2015` key, three node parked dirs (2026-10-07) | S3 deletes done (found to be PERMANENT with s3fs — AGENTS.md §2), 2015 key retired with a copy at `sandbox/backup/retired_20261007_064952/`, #23 closed; node freed ~123 GB, one 19 MB `.nfs` shell left under `Data/_parked_intld_fixes_20261004_173417` to `rm -rf` once released |
| `DISPATCH_cglabs_r3_res25_rerun.md` | R/3 full re-bake on native 0.25° exposure + usd tier republish behind the G6 value-drift gate (#30 follow-ups, 2026-09-26 → 10-01) | usd tiers live 2026-09-30 (superseded 2026-10-06 by the item-2 re-bake); parked set released 2026-10-01; backups deleted in the 2026-10-07 clean-up |
| `DISPATCH_cglabs_family_keys.md` | Producer + publisher for the per-unit family keys `vop_nominal-usd-2021` / `vop_intld15-2021` at both resolutions; retirement of `vop_nominal-usd-2015` (#23) | Family keys live 2026-09-28 (republished 2026-10-06); 2015 key retired 2026-10-07 (clean-up Block B), #23 closed |
| `DISPATCH_cglabs_exposure_intld_fixes.md` | Item 2 of the #30 follow-ups: #38 millet split, #39 coverage guard, #40 Seychelles, and the nominal price method (implied price, stale-price test, basis guard, cited pins incl. coffee) (2026-10-01 → 10-06) | Exposure reference + family republished 2026-10-06 at both resolutions (9/9 md5), usd hazard tiers re-baked and republished the same day (3/3 md5, hazard ÷ reference ≈ 1 except three named border pairs, #18); #38/#39/#40 closed; method text `docs/methods/nominal_price_method.md` |
| `HANDOVER_2026-10-01_exposure-intld-fixes.md` | Topic briefing for the item-2 pass: #38/#39/#40 evidence, the price-method deep dive and decisions, the evidence reviews (eight low-side pairs; coffee) | Closed with the dispatch above; its evidence sections are cited by `docs/methods/nominal_price_method.md` and `metadata/price_pins.csv` |
| `HANDOVER_*.md` | Session handovers and topic briefings, Jun–Sep 2026 | Superseded by the current handover at the repository root |
