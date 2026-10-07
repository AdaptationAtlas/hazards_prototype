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
| `DISPATCH_cglabs_coverage_probe.md` | Global vs Africa coverage audit | Every upstream source is global; the Africa cut is ours, at three code sites |
| `DISPATCH_cglabs_phase2.md` · `DISPATCH_cglabs_sfcwind.md` | Upstream Stage-0 migration, wind availability | Validated on real data; `sfcWind` present for all 18 GCMs |
| `DISPATCH_poultry_thi_rebake.md` | poultry_highland THI threshold 79 → 89 | Metadata fixed; published outputs still need the re-bake, tracked as issue #13 |
| `DISPATCH_desert_mask.md` | Desert masking | Closed |
| `DISPATCH_cglabs_issue9_none_publish.md` | Issue #9: `hazard='none'` on every combination + publish the notebook-facing hazard_exposure (Sep 2026) | Published 2026-09-16, verified; #9 and #12 closed 2026-10-01 once the denominator existed on the product's grid (`crop-livestock_all_res-25`, #30): zero exceedances against it at every admin level. `Data/_parked_issue9/` released in the 2026-11-01 clean-up pass |
| `DISPATCH_cglabs_local_sourcing.md` | Scripts load `haz_functions.R` and metadata from the checkout, not GitHub `main` (2026-10-01) | Verified on the node the same day: no run-time URLs, root resolved from repo root and elsewhere, develop's `haz_classes.csv` (THI 89) read, probe outputs unchanged |
| `DISPATCH_cglabs_exposure_intld_fixes.md` | Item 2 of the #30 follow-ups: #38 millet split, #39 coverage guard, #40 Seychelles, and the nominal price method (implied price, stale-price test, basis guard, cited pins incl. coffee) (2026-10-01 → 10-06) | Exposure reference + family republished 2026-10-06 at both resolutions (9/9 md5), usd hazard tiers re-baked and republished the same day (3/3 md5, hazard ÷ reference ≈ 1 except three named border pairs, #18); #38/#39/#40 closed; method text `docs/methods/nominal_price_method.md` |
| `HANDOVER_2026-10-01_exposure-intld-fixes.md` | Topic briefing for the item-2 pass: #38/#39/#40 evidence, the price-method deep dive and decisions, the evidence reviews (eight low-side pairs; coffee) | Closed with the dispatch above; its evidence sections are cited by `docs/methods/nominal_price_method.md` and `metadata/price_pins.csv` |
| `HANDOVER_*.md` | Session handovers, Jun–Sep 2026 | Superseded by the current handover at the repository root |
