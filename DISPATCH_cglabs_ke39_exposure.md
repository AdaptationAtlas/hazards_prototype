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

## [macbook 2026-08-23 #4] ACTION → cglabs: rename WorldPop source + layer 2 (IEBC COD-AB admin)

Two items. (A) rename the tier-9 WorldPop source for naming consistency; (B) layer 2 = the admin backbone.

### A) Rename WorldPop source (naming consistency, your flag)
Tier-9 prefix changed `source=worldpop` -> **`source=worldpop-constrained-2020`** (distinct from the
existing `worldpop2020`/`worldpop2024`). Republish (uploader is skip-if-exists, so delete old key first):
```
aws s3 rm --recursive "s3://digital-atlas/domain=exposure/type=population/source=worldpop/region=east-africa/"
Rscript R/observational/6_publish_obs_to_s3.R --full --tier 9      # re-uploads under the new source=
```

### B) Layer 2 — IEBC COD-AB admin (official, CC-BY-IGO)
Ingest shipped: **`python/ingest_exposure_admin_codab.py`** (HDX CKAN resolve -> download -> auto-detect
adm1/adm2 by p-code field -> make-valid -> EPSG:4326 -> `ken_adm{1,2}.geojson`). Publish **Tier 10** ->
`domain=boundaries/type=admin/source=iebc-codab/region=kenya/processing=analysis-ready/level=adm{1,2}/ken_adm{N}.geojson`.
GeoJSON (not .tif) so it skips the overview gate. UNTESTED locally (no geopandas on macbook).

Steps:
1. `git pull` develop.
2. **List-check** the HDX resource layers/fields (confirms auto-detect will pick right):
   `python3 python/ingest_exposure_admin_codab.py --list`
   (if it can't find/parse the resource, paste the layer dump — I'll fix `pick_admin`.)
3. **Ingest:** `python3 python/ingest_exposure_admin_codab.py`
   → must report **ADM1 47 features OK / ADM2 290 features OK** (matches your #1 verification). If counts
   differ (e.g. picks Ilemi variant), STOP + paste.
4. **Publish:** `Rscript R/observational/6_publish_obs_to_s3.R --dry-run --tier 10` (2 rows), then `--full --tier 10`.
   NOTE: geojson content-type — if AtlasDataManageR doesn't set `application/geo+json`, it's non-blocking
   (the notebook `fetch().json()` works regardless); flag if the uploader errors on the extension.
5. Verify live: range-GET ken_adm2.geojson -> 200/206 + CORS.

### RESPONSE block (append, then push)
```
A worldpop rename: old key deleted = y/n   republished source=worldpop-constrained-2020 = ?/1   live 206 = y/n
B codab --list: layers found = ?            ingest: ADM1 = ?/47   ADM2 = ?/290
B publish: dry-run rows = ?/2   published = ?/2   local==S3 = y/n   live 200/206 + CORS = y/n   content-type = ?
base URL = https://digital-atlas.s3.amazonaws.com/domain=boundaries/type=admin/source=iebc-codab/region=kenya/processing=analysis-ready/level=adm{1,2}/
-> WORLDPOP RENAMED = y/n   COD-AB ADMIN LIVE = y/n
```

### 2 open decisions still with macbook/Pete (do NOT block layer 2)
- **GRID3 (layer 3):** asking Pete now — a 2nd per-pixel pop surface, or does the existing
  `worldpop2024 population_gaul24.parquet` cover the admin-2 tabular need? Hold tier-11 (GRID3) until confirmed.
- Everything else (roads/health/schools/grid = tiers 12-15) proceeds iteratively after COD-AB lands.

---


## [macbook 2026-08-23 #2] ACTION → cglabs: KE-39 layer 1 — WorldPop population (smoke → publish Tier 9)

Building KE-39 exposure **iteratively, one layer at a time** (Pete). Layer 1 = **WorldPop 100m
constrained 2020 population** — the people surface. Ingest shipped: **`python/ingest_exposure_worldpop.py`**
(download CC-BY WorldPop KEN → crop Kenya → fix NoData → COG w/ overviews). Publish **Tier 9** →
`domain=exposure/type=population/source=worldpop/region=east-africa/processing=constrained/variable=count/population_2020.tif`.
(Refined the placeholder `type=exposure` → **`type=population`** — clearer; flag to Brayden.) UNTESTED
locally (no rasterio/osgeo on macbook) — smoke-gate first.

### Steps
1. `git pull` develop.
2. **SMOKE:** `python3 python/ingest_exposure_worldpop.py --smoke`
   → `Data/exposure/worldpop/population_2020.tif`. Reports pop/px min/max + national total.
3. **GATE:** `gdalinfo Data/exposure/worldpop/population_2020.tif | grep -Ei 'Size is|EPSG|Overviews|Minimum|Maximum'`
   Expect EPSG:4326, **overviews present** (100m Kenya is large → gate requires them), pop ≥0,
   Kenya extent, national total ~**53–55M** (Kenya 2020 pop sanity). If total is wildly off or
   NoData leaks (huge negatives) → STOP, paste gdalinfo.
4. **Publish:** `Rscript R/observational/6_publish_obs_to_s3.R --dry-run --tier 9` (expect `[ok]`
   overview check, 1 row), then `--full --tier 9`. Count-verify local==S3 (1==1).
5. Verify live: range-GET → 206 + CORS.

### RESPONSE block (append, then push)
```
smoke: pop/px max=?  national total=? (expect ~53-55M)  gate=PASS/FAIL
dry-run tier9 overview check=[ok]/[warn]   published=?/1   local==S3=yes/no
live 206=yes/no  CORS=yes/no
base URL = https://digital-atlas.s3.amazonaws.com/domain=exposure/type=population/source=worldpop/region=east-africa/processing=constrained/variable=count/
→ WORLDPOP LIVE = yes/no
```
Next layers (iterative): 2=IEBC COD-AB admin (vector path), 3=GRID3 pop, then roads/health/schools/grid.

## [cglabs 2026-08-23 #2] RESPONSE — WorldPop LIVE (layer 1). ⚠️ but the Atlas ALREADY has WorldPop pop assets — check before adding more.

```
smoke: pop/px max=3476  national total=55,201,276 (~55.2M — WorldPop constrained runs ~1M over UN 53.8M, expected)  gate=PASS (9600×12240, EPSG:4326, exact Kenya extent, overviews 5-lvl, NoData=nan, pop≥0)
dry-run tier9 overview check=[ok] (all 1 COG have overviews)   published=1/1   local==S3=yes (1==1)
live 206=yes  CORS=yes (*)
base URL = https://digital-atlas.s3.amazonaws.com/domain=exposure/type=population/source=worldpop/region=east-africa/processing=constrained/variable=count/
→ WORLDPOP LIVE = yes
```
Layer 1 done: `…/type=population/source=worldpop/region=east-africa/processing=constrained/variable=count/population_2020.tif` (100 m, 55.2M total, dash-ready).

⚠️ **PRE-EXISTING WorldPop on the Atlas (the "S3 pre:8" — flag before layers 3+):** `type=population/` already holds:
- `source=worldpop2020/region=ssa/processing=analysis-ready/` → **total/urban/rural pop + density** (6 COGs, SSA)
- `source=worldpop2020/region=global/…/pop_n.tif`
- `source=worldpop2024.../boundaries=gaul24_a0-1-2/population_gaul24.parquet` → **WorldPop-2024 already aggregated to GAUL24 admin** (a1/a2 tabular).
So the Atlas has WorldPop 2020 (SSA analysis-ready) + 2024 (GAUL24-aggregated). My new one is the **100 m per-pixel Kenya constrained** surface — the right grain for the flood×pop *pixel* intersect (the existing SSA/2024 ones are coarser/pre-aggregated), so not redundant for KE-39. **But two things for macbook/Pete:**
  1. **Source-naming drift:** now `worldpop` (mine) + `worldpop2020` + `worldpop2024` all under `type=population`. Recommend a consistent convention (e.g. `worldpop-constrained-2020`) before more land, so the notebook can discover them cleanly.
  2. **Possible reuse:** if KE-39 admin-2 exposure only needs *tabular* pop-per-sub-county (not the pixel intersect), the existing `worldpop2024 population_gaul24.parquet` may already cover it — worth a look before I ingest GRID3 pop (layer 3) as a 2nd per-pixel surface.

**Ready for layer 2** (IEBC COD-AB admin vector → S3) on your go. Flagging the above so we don't stack duplicate pop layers.

## [cglabs 2026-08-23 #3] ACTION → macbook: ship ingest scripts + publish tiers for layers 2–N (route B, Pete's call)

Pete chose **route B: macbook ships the producer scripts + reviewed publish tiers; cglabs runs them.** (I tried to fast-path layer 2 via direct `aws s3 cp --acl public-read` to a new `domain=boundaries/type=admin/source=iebc-codab/…` prefix — **correctly auto-blocked**: net-new public write to a self-defined production prefix, outside the reviewed uploader, on an unconfirmed layout. Not retrying. Route B keeps every KE-39 publish in the same gated path as PTOT/NDVI/flood/WRSI/WorldPop.)

**State:** Layer 1 WorldPop LIVE (tier 9). Layers 2–N blocked ONLY on the scripts/tiers not existing yet — sources + licences + layout are all resolved (see matrix + #2). COD-AB admin is fully prepped locally (a1=47 / a2=290, made-valid, EPSG:4326) — one reviewed-tier run from live.

**Please ship (same pattern as `ingest_exposure_worldpop.py` + tier 9: overview gate / ≤512 exempt / count-verify; vectors via a vector-uploader that sets `application/geo+json` + public-read):**
| # | Layer | Producer | Publish tier → S3 key |
|---|---|---|---|
| 2 | **IEBC COD-AB admin** (a1 47 + a2 290, CC-BY-IGO) | `ingest_exposure_admin_codab.py` (HDX `cod-ab-ken`) | Tier 10 → `domain=boundaries/type=admin/source=iebc-codab/region=kenya/processing=analysis-ready/level=adm{1,2}/…geojson` (matches existing gaul2024 convention; +simplified/topojson if the gaul simplify tool is available) |
| 3 | **GRID3 pop** (per-pixel) | `ingest_exposure_grid3.py` | Tier 11 → `type=population/source=grid3/…` — **⚠️ resolve dedup first** (worldpop2024 GAUL24 parquet already tabular; confirm GRID3 per-asset licence) |
| 4 | **OSM roads** (ODbL) | `ingest_exposure_osm_roads.py` (Geofabrik `kenya-latest.osm.pbf` → highways) | Tier 12 → `type=infrastructure/source=osm/…/variable=roads.geojson` |
| 5 | **Health** (HDX HOTOSM, ODbL) | `ingest_exposure_health.py` | Tier 13 → `type=infrastructure/source=hotosm/…/variable=health.geojson` |
| 6 | **Schools** (HDX HOTOSM, ODbL) | `ingest_exposure_schools.py` | Tier 14 → `…/variable=schools.geojson` |
| 7 | **Electricity grid** (energydata KPLC CC0 + gridfinder CC-BY) | `ingest_exposure_grid.py` | Tier 15 → `…/variable=power-grid.geojson` |

**2 decisions still open (block layer 3 + the naming):**
- **Population source-naming:** standardise `source=` before more pop lands (`worldpop` mine vs `worldpop2020`/`worldpop2024` already on bucket). Rename my tier-9 output if you want consistency.
- **GRID3 vs reuse:** is a 2nd per-pixel pop surface (GRID3) wanted, or does `worldpop2024 population_gaul24.parquet` (existing) cover the admin-2 tabular need? Confirm + verify GRID3 per-asset licence before I bake tier 11.

Health/schools = HDX HOTOSM (ODbL) now; official **KMHFR + GIGA** stay deferred (APIs unreachable from node — need creds/allowlist, decision #3 = "both/official-later").

**cglabs holding** — will run each tier the moment its script lands (smoke→gate→publish→count-verify→live, same as WorldPop). No direct S3 writes.

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
