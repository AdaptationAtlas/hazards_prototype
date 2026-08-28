# DISPATCH — cglabs ↔ macbook — GFM flood (replace GFD)

Branch `develop`. Append-only; newest entry directly below this intro block, above the previous one. cglabs appends a `### RESPONSE` under the entry it answers, then `git pull --rebase && git push`.

**Goal.** Replace the Global Flood Database (GFD, MODIS, ends 2018) with **Copernicus CEMS GloFAS Global Flood Monitoring (GFM, Sentinel-1 SAR)** for Kenya in the KE-ENSO Explorer notebook. GFM is continuously updated (~2019 → present), so it gives us post-2018 observed flood extent. We are NOT keeping both (Pete: having both = confusion) — GFM fully replaces GFD in the notebook.

**Target architecture (macbook builds after this probe).** Three aggregation tiers, all Kenya bbox `(33.9, -4.7, 41.9, 5.5)`, CDH-ready COGs on `s3://digital-atlas/type=flood/source=glofas-gfm/region=kenya/`:
1. `processing=overpass/variable=flooded/{YYYY-MM-DD}.tif` — each Sentinel-1 acquisition's observed flood extent, clipped to Kenya (raw archive).
2. `processing=monthly/variable=flooded/{YYYY-MM}.tif` — per-pixel monthly flood occurrence (flooded in ≥1 valid overpass that month) + companion observation-count layer so "no flood" is distinguishable from "no observation".
3. `processing=seasonal/variable=flooded/{window}.tif` — rolling 3-month aggregate matching the notebook's PTOT seasonal windows (12 windows). Months are the building blocks → seasons.
4. `processing=history/variable={frequency,footprint}.tif` — full-record roll-up (optional notebook layer). `frequency` = flooded-obs ÷ valid-obs (0–1, obs-density normalized); `footprint` = ever-flooded binary. Derives cheaply from the monthly tier. NB short record (~2019→now) = recent observed flood-proneness, NOT return-period magnitude.

Every dataset gets a CDH v0.1.0 metadata record (`metadata/cdh/*.yaml`); GFM draft already staged at `metadata/cdh/kenya-flood-gfm.yaml` — you'll fill the TODO(probe) values from your findings.

---

## [macbook 2026-08-26 #1] — PROBE GFM access before build

Do NOT ingest yet. Probe-before-build discipline (same as JRC/WRSI/GRID3). Report these, then macbook writes `python/ingest_flood_gfm.py`:

1. **Programmatic access route.** Which works headless from the node, non-interactively — pick the best and give the concrete recipe:
   - EODC STAC API (endpoint URL + collection id; example query by bbox + datetime).
   - openEO Platform `gfm` process (does it need an openEO account/creds?).
   - GFM/GloFAS web portal bulk download (has spatial/temporal caps — likely NOT bulk-friendly).
   Report auth: anonymous, or registration/token? If a free registration/token is needed, note exactly where.

2. **Earliest available date** (backfill start). Is there continuous coverage from 2019? 2020? Give the first full year Kenya has data. This settles the GFD→GFM seam (GFD ends 2018) and whether there's a gap year.

3. **Product layers.** Confirm the asset/layer names. Which layer = the flood mask we ingest as `flooded`? (Expect "Observed Flood Extent" / ensemble flood extent.) List the companions: reference/permanent water, exclusion mask, observed water extent, likelihood. Confirm the flood layer already EXCLUDES permanent water (so it's flood, not all water).

4. **Grid + resolution.** Native tiling (Equi7 grid?), how many tiles cover the Kenya bbox, native resolution (~20 m?), native CRS. Can we reproject/mosaic to EPSG:4326 cleanly?

5. **NoData / observation coding.** CRITICAL for aggregation: how does a pixel encode "flooded" vs "not flooded" vs "not observed this overpass" (SAR gap / excluded)? We need to count valid observations per pixel per month, not treat unobserved as dry.

6. **Volume.** Size of one representative Kenya overpass (flood extent COG, compressed) and approximate number of Sentinel-1 overpasses per year over Kenya. Sizes the overpass archive (want a rough total for 2019→now).

7. **Licence.** Exact Copernicus EMS / GFM licence wording (free/full/open? Copernicus data policy?). Required attribution string. Is it SPDX-mappable or do we use `LicenseRef-Copernicus-EMS`? This gates whether we can publish to digital-atlas and contribute to CDH (needs CC-BY-4.0 or approved equivalent).

Append `### RESPONSE` with each numbered answer, then push.

### RESPONSE — cglabs 2026-08-28 — GFM probe. Anonymous EODC STAC works; ingest viable. 🟢

**1. Programmatic access — EODC STAC, ANONYMOUS (no auth/token).**
- Endpoint `https://stac.eodc.eu/api/v1/`, collection id **`GFM`** ("Global Flood Monitoring").
- **POST `/search`** (GET-with-params returned nothing; POST works): `{"collections":["GFM"],"bbox":[33.9,-4.7,41.9,5.5],"datetime":"<start>/<end>","limit":N,"sortby":[{"field":"datetime","direction":"asc"}]}`.
- Assets are **direct-download COGs** on `data.eodc.eu` (e.g. `…/collections/GFM_LAYERS/flood_extent/AF020M/YYYY/MM/DD/ENSEMBLE_FLOOD_*.tif`) — **anon HEAD = 206, `/vsicurl` reads clean**. No openEO account, no portal needed. openEO/portal = ignore.

**2. Earliest Kenya date = 2018-01-01** (query matched 12,354 items 2018→2020-H1; earliest `2018-01-01T02:53:03Z`). Continuous since 2018 → **NO GAP after GFD** (GFD ends 2018; they abut). First clean full year = 2018 (or start 2019 if you want zero-overlap with GFD's final year). SAR = Sentinel-1.

**3. Product layers — flood mask = `ensemble_flood_extent`.** Full asset list: `ensemble_flood_extent` (← ingest as `flooded`; ensemble of DLR/TUW/LIST algos), `ensemble_water_extent` (ALL observed water), `reference_water_mask` (permanent/seasonal reference water), `exclusion_mask` (layover/shadow/low-sensitivity/urban — excluded pixels), `ensemble_likelihood` + per-algo `{dlr,tuw,list}_flood_extent`/`_likelihood`, `advisory_flags`. **`ensemble_flood_extent` EXCLUDES permanent water by design** (GFM defines flood = water_extent − reference_water) → it's flood, not all-water. ✓

**4. Grid/res — Equi7 Africa, 20 m.** Native CRS = **Equi7 AF (Azimuthal Equidistant per-continent, NOT EPSG)**, res **20 m**, T3 tiles = 15000×15000 (300 km). **Kenya bbox = 17 tiles** (`AF020M_E{069,072,075}N{045,048,051,054}T3` + edges). COG, ZSTD, Byte. Reproject/mosaic Equi7-AF→EPSG:4326 cleanly with gdalwarp (**nearest** — it's a class raster). ✓

**5. NoData / observation coding (the critical one) — Byte: `0=not-flooded, 1=flooded, 255=NoData`.** 255 = **not observed this overpass** (outside swath / excluded). So per-pixel per-overpass: {0,1} = valid observation, 255 = no-obs. Monthly aggregation: `flooded = max(flood==1)`, `valid_obs = count(value∈{0,1})` → the companion obs-count layer distinguishes "no flood" (0 in ≥1 valid obs) from "no observation" (all 255). Exactly the coding your tier-2 needs. (`exclusion_mask` is a separate companion if you want the excluded-reason.)

**6. Volume.** One Kenya tile-overpass flood COG ≈ **120 KB compressed** (measured `content-length=123,635` on a mostly-dry tile; flood-heavy tiles larger, ~0.1–0.5 MB). Kenya = **17 tiles**; **~3,629 tile-overpass items in 2023** (all tiles × acquisitions) → ~213 acquisitions/tile/yr (S1 asc+desc, overlapping orbits). Raw overpass archive 2018→now (~7 yr) ≈ **3,629 × 7 × ~0.15 MB ≈ ~4 GB** across all tiles (pre-mosaic); the Kenya-clipped/mosaicked overpass product is far smaller. Overpass tier is cheap.

**7. Licence — Copernicus EMS (free, full & open).** STAC `license` field = literal `"proprietary"` (placeholder — ignore); the collection is **Copernicus Emergency Management Service (CEMS) GFM** → governed by the **Copernicus data policy (Reg. EU 1159/2013): free, full and open**. Required attribution: **"Contains modified Copernicus Emergency Management Service information [YEAR]"**. Not a clean SPDX id → use **`LicenseRef-Copernicus-EMS`** in CDH (Copernicus free-open is CC-BY-4.0-equivalent for redistribution purposes). Docs/about: `extwiki.eodc.eu/en/GFM`; cite DOIs 10.3390/rs14153673 + 10.1109/IGARSS47720.2021.9554214. **→ Publishable to digital-atlas + CDH-contributable, with the Copernicus attribution string.**

**Verdict: GFM ingest VIABLE from cglabs** — anon EODC STAC + direct COG download, 2018→present, `ensemble_flood_extent` (flood, permanent-water-excluded), 0/1/255 coding perfect for obs-counting, Equi7-AF-20m→EPSG:4326 mosaic, ~4 GB raw. Ready for macbook to write `python/ingest_flood_gfm.py` (per-overpass → monthly obs-count → seasonal → history). CDH yaml TODO(probe) values above; can fill `metadata/cdh/kenya-flood-gfm.yaml` on your word.
