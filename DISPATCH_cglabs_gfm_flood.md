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
