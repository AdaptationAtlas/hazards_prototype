# DISPATCH — cglabs ⇄ macbook · KE-39 flood/drought EXPOSURE ingest

_Append-only. Newest on top. cglabs runs, appends RESPONSE, pushes; macbook reads._

Workstream: KE-39 (KE-ENSO) — make flood/drought hazard actionable by intersecting with where people
+ assets are, at Kenya admin-2. **Routed to cglabs (Pete 2026-08-22).** New = the exposure layers (COGs +
vectors on Atlas S3, same conventions as PTOT/SPEI/NDVI/flood/WRSI — NOT GEE). Notebook side is PAUSED on
the intersect UI until cglabs delivers layers.

Full detail on the KE-ENSO thread (`atlas_notebooks` dev/KE-enso-explorer/…/dispatches/):
`2026-08-22_cglabs-ke39-exposure-kickoff.md` + `2026-08-22_cglabs-ke39-boundaries-addendum.md`. This file
mirrors it for the hazards_prototype (macbook) side.

---

## [cglabs 2026-08-23 #1] cglabs kickoff — access probe done, boundary authority resolved, needs macbook decisions

### 🔑 BOUNDARIES — use nationally-approved IEBC (COD-AB), NOT GAUL (Pete's steer, verified)
On-node check of the Atlas backbone `Data/boundaries/atlas_gaul24_a2_africa.parquet`: Kenya GAUL24 = **48
admin-1 / 291 admin-2** (names ARE IEBC sub-counties — Nairobi's 17, Marsabit's 4, etc.), BUT it carries the
disputed **Ilemi Triangle** as an extra a1+a2 and has **no official p-codes**. The authoritative source is
**HDX `cod-ab-ken`** (source = IEBC, org = OCHA, CC-BY-IGO): **47 counties / 290 sub-counties with official
`adm1_pcode`/`adm2_pcode`** (+ ward p-codes as points). GeoJSON/GDB/SHP/XLSX, 6.9 MB, no auth.
- **Decision:** serve the KE-39 exposure admin backbone from **IEBC COD-AB** (publish a1/a2 GeoJSON+topojson
  to S3), keep **GAUL24 as the climate/zonal backbone** (don't disturb the hazard pipeline). Build
  `adm2_pcode↔gaul2_code` crosswalk ONLY if joining GAUL-climate stats onto IEBC units in tables (not needed
  for the map intersect — it's raster×geometry in the notebook).
- My earlier "GAUL a2 fine, no crosswalk" was **wrong for an official product** — walked back.

### Source access + licence matrix (all probed from cglabs, non-GEE, no auth unless noted)
| Layer | Route | Reach | Licence | Format | Note |
|---|---|---|---|---|---|
| **Admin (official)** | HDX `cod-ab-ken` (IEBC) | 200 | CC-BY-IGO | GeoJSON/GDB/SHP | 47 cty / 290 sub-cty + p-codes |
| **Population** | WorldPop constrained 2020 100 m `data.worldpop.org/.../2020/maxar_v1/KEN/ken_ppp_2020_constrained.tif` | 200 (34 MB) | CC BY 4.0 | plain GeoTIFF | COG-ify + overviews on ingest; NoData ~-99999 |
| Population (alt) | GRID3 Kenya (data.grid3.org) | 200 | CC BY (per-asset — verify each) | GeoTIFF | KNBS-census-tuned |
| **Roads** | OSM Geofabrik `africa/kenya-latest.osm.pbf` | 302→file | ODbL | .osm.pbf | extract highways → GeoJSON |
| **Health** | KMHFR API **DOWN (000)** → HDX HOTOSM "Health Facilities of Kenya" | HDX 200 | ODbL | GeoPackage/GeoJSON | official KMHFR needs creds/allowlist |
| **Schools** | HDX HOTOSM "Education Facilities of Kenya" | 200 | ODbL | GPKG/GeoJSON/SHP/KML | GIGA API DOWN (000) |
| **Electricity** | energydata.info "Kenya Electricity Network" (CC0) + gridfinder (CC-BY) | 200 | CC0 / CC-BY-4.0 | vector | KPLC grid CC0 = cleanest |
| **Settlement extents** | GRID3 Kenya Settlement Extents | 200 | per-asset (verify) | polygon/point | flood ∩ built-up |

**2 endpoints failed from node (flag):** KMHFR (`api.kmhfr.health.go.ke`) + GIGA (`api.giga.global`) = 000
(connect fail — need token/allowlist). Both have working open HDX HOTOSM substitutes (ODbL) → health + schools
NOT blocked; only matters if you want the *official* registries.

### Proposed S3 layout (mirror existing tiers)
- rasters: `domain=exposure/type=exposure/source={worldpop|grid3}/region=east-africa/…` — COG + overviews (or ≤512 exemption), CORS `*` + range.
- vectors: GeoJSON/topojson on S3 (roads, health, schools, grid, admin) — overlay, not window-read.

### DECISIONS NEEDED (macbook / Pete) before any bake
1. **Confirm IEBC COD-AB** as the exposure admin source (my strong rec — matches Pete's steer). cglabs then ingests COD-AB a1/a2 → S3.
2. **Population: WorldPop 100 m (CC BY, one clean file) vs GRID3 Kenya (KNBS-tuned, per-asset licence)?**
3. **Health: HDX HOTOSM (ODbL, works now) vs official KMHFR (needs creds — node can't reach the API)?**
4. **Producer scripts:** none exist in hazards_prototype. Do you (macbook) ship `ingest_exposure_*.py` + a
   `type=exposure` publish tier (like NDVI/flood/WRSI), or authorise cglabs to author them?

### RESPONSE block (macbook: answer 1–4 + push; cglabs then bakes)
```
1 admin source = IEBC-CODAB / GAUL / other
2 population = worldpop-100m / grid3 / both
3 health = hdx-hotosm / official-kmhfr(+creds) / both
4 scripts = macbook-ships / cglabs-authors
notes:
```
No ingest/publish run yet — read-only probe + boundary verification only. Ready to bake on your answers.

## [macbook 2026-08-23 #1] RESPONSE — decisions confirmed (Pete). macbook ships scripts; then cglabs bakes.

```
1 admin source = IEBC-CODAB   (official 47/290 + p-codes; GAUL stays climate/zonal backbone; Ilemi/no-pcode = disqualifies GAUL for the official product)
2 population = both            (WorldPop 100m 2020 CC-BY = v1 clean baseline; + GRID3 Kenya KNBS-tuned — verify each GRID3 asset licence before publish)
3 health = both/official-later (HDX HOTOSM ODbL now for health + schools; add official KMHFR/GIGA later once creds/allowlist sorted — flag those 2 endpoints as blocked, not dropped)
4 scripts = macbook-ships      (I author ingest_exposure_*.py + a type=exposure publish tier, same pattern as NDVI/flood/WRSI incl the overview gate + <=512 exemption + count-verify; cglabs runs)
notes:
 - S3 layout OK: rasters domain=exposure/type=exposure/source={worldpop|grid3}/region=east-africa/... (COG+overviews or <=512 exempt, CORS+range);
   vectors (admin/roads/health/schools/grid/settlement) as GeoJSON(+topojson) on S3 for overlay (not window-read).
 - Admin: publish IEBC COD-AB a1/a2 GeoJSON+topojson. Crosswalk adm2_pcode<->gaul2_code = DEFER (map intersect is raster x geometry; only needed if joining GAUL-climate stats onto IEBC units in tables).
 - Licence tracking: WorldPop CC-BY-4.0, COD-AB CC-BY-IGO, HOTOSM/roads ODbL, KPLC grid CC0, gridfinder CC-BY, GRID3 per-asset (VERIFY). Carry licence per layer into the Brayden/CDH note.
 - Sequence: macbook writes scripts -> smoke-gate each (like flood/NDVI/WRSI) -> cglabs runs full + publishes -> relay to KE-ENSO + add type=exposure to Brayden dm#2.
 - Health/schools: HOTOSM reachable now = not blocked; only the *official* registries wait on creds.
```
Writing the ingest/publish scripts next (population COG ×2, vector publishers, type=exposure tier). Will dispatch per-layer smoke gates as they land — no full bake until each smokes clean.
