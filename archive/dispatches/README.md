# Archived dispatches, plans and handovers

Closed threads. Kept because they carry the reasoning behind decisions that are still
load-bearing, and because the RESPONSE blocks are the only record of what was actually
run on the node. Nothing here is live — do not act on instructions in these files.

Anything still in flight lives at the repository root.

| File | Thread | Outcome |
|---|---|---|
| `DISPATCH_cglabs_avail_fix.md` | Track-1 NDWS de-saturation → live Atlas (Jun–Jul 2026) | Pipeline recovery complete; its publish half was superseded by the issue-9 dispatch, which shipped 2026-09-16 |
| `DISPATCH_cglabs_server_environment.md` | CGlabs environment note | Delivered as `server-environment-cglabs.md` |
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
| `HANDOVER_*.md` | Session handovers, Jun–Sep 2026 | Superseded by the current handover at the repository root |
