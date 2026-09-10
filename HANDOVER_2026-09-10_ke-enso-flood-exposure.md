# HANDOVER — KE-ENSO flood + exposure workstream (2026-08 → 2026-09-10)

Session role: **macbook / hazards_prototype** (authors ingest + publish + zonal engine; cglabs runs on-node).
Branch `develop`. This workstream is **DONE pipeline-side** — the notebook now reads live layers + pre-cooked
tables. Remaining items are external (CDH contribution, another session's coverage probe) or offered-not-asked.

## What shipped (all LIVE on `s3://digital-atlas/`, 206 + CORS)

**1. GFM flood replaces GFD.** Copernicus CEMS GFM (Sentinel-1 SAR) observed flood, Kenya, 2018→2025.
- `python/ingest_flood_gfm.py` (v2) — monthly aggregate built DIRECTLY from EODC STAC tiles at ~111 m
  (`rasterizeGeom`/`resampleAlg=max`), parallel across months; overpass 20 m is opt-in (`--stage overpass`).
  Perf lesson: first design (per-raster 20 m mosaics) = 9 days/113 GB → re-arch to 111 m-direct = ~3 h.
- Publish **tier 14** (`6_publish_obs_to_s3.R`, `name_fn_gfm`, recursive tree). Live at
  `domain=climate/type=flood/source=glofas-gfm/region=kenya/processing={monthly,seasonal,history}/…`.
  382 files verified. history `frequency`+`footprint` published but not yet wired in the notebook.
- Coding gotcha: `flooded` = 0/1/255 (255 = SAR not-observed, NOT dry); `nobs` companion. Seasonal
  PTOT-aligned (`variable=flooded/season={SEASON}/flooded_{SEASON}_{YYYY}.tif`).
- Notebook swapped (dev_rainfall_maps.qmd v0.24, `b819148`); **GFD deleted** from S3 (cglabs, 15 COGs).

**2. KE-39 exposure = 7/7 layers.** `python/ingest_exposure_*.py`, publish tiers 9–15:
WorldPop pop (t9), GRID3/WOPR pop (t11), IEBC COD-AB admin adm1/adm2 (t10), OSM roads (t12),
HOTOSM health+schools (t13), KPLC electricity grid 5-voltage (t15). All under `domain=exposure/…`.

**3. Pre-cooked flood × exposure tables (the extract).** `R/observational/7_zonal_exposure.R`, publish **tier 16**.
Per-adm2 (290) zonal intersect → 3 parquets at `domain=exposure/type=intersect/region=kenya/processing=analysis-ready/`:
`exposure_gfm_seasonal.parquet` (27,260 rows), `exposure_jrc_rp.parquet` (2,030), `exposure_totals.parquet` (290).
Perf lesson: line `st_intersection` = 68 h → **rasterised line-length** (`rasterizeGeom` once/grid + `zonal`) = ~3 h.
Notebook reads these + a 179 KB adm2 topojson (client-side intersect retired).

**4. CDH metadata — 13 records, all validate.** `metadata/cdh/*.yaml` + `README.md` manifest.
`uvx check-jsonschema --schemafile <v0.1.0 profile-url> metadata/cdh/*.yaml` → ok. Real-schema shape
captured in memory [[reference-cgiar-cdh-metadata-standard]] (authoring-guide summary was wrong on
extensions[]/cdh-block/temporal/dimensions/keywords).

## Cross-session handovers written (filesystem-local channel — notebook session reads the worktree)
`atlas_nb-KE-enso/playbook/handovers/KE-enso-explorer/dispatches/`:
`2026-08-31_reply-gfm-flood-live.md`, `2026-09-01_reply-ke39-exposure-all-live.md`,
`2026-09-09_reply-precooked-exposure-tables-live.md`.

## Open threads (nothing blocking pipeline-side)
- **CDH contribution** — GitHub issue **CGIAR-Climate-Data-Hub/cdh-metadata-standard#32** filed (13 records +
  federation-of-extracts spec question for @bjyberg). Email draft at `scratchpad/brayden_email_draft.md`
  (recipient `B.Youngberg@cgiar.org` GUESSED — verify; Outlook connector was unavailable). Await Brayden:
  contribution route + a canonical "federate-to-parent" link convention (then add it to the intersect record).
- **Offered, not asked** — wire GFM `history` freq/footprint static layer; GRID3 `pop_source` A/B in the
  exposure tables. Hold until the notebook requests.
- **Coverage probe** — `DISPATCH_cglabs_coverage_probe.md`, another session's thread, cglabs on-disk half pending.

## Resume pointers
- Live dispatch logs (newest-on-top): `DISPATCH_cglabs_gfm_flood.md`, `DISPATCH_cglabs_ke39_exposure.md`,
  `DISPATCH_cglabs_zonal_exposure.md`, `DISPATCH_cglabs_ptot_overviews.md` — all with completed RESPONSEs.
- Publish tiers 1–16 in `R/observational/6_publish_obs_to_s3.R` (`--tier N`, overview gate on .tif).
- Memory: [[project_gfm_flood_and_cdh_backfill]] carries the full resume state.
