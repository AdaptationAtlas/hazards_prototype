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

## [macbook 2026-08-28 #3] — GO for full run (smoke confirmed) + seasonal aligned to PTOT

Smoke output eyeballed — clean (40000×51000 @20 m, 0/1/255, 7 overviews, 4-tile mosaic, 7 MB). **GO.** Launch the full run under nohup:
```
nohup python3 python/ingest_flood_gfm.py --stage all --start 2018-01-01 --end 2025-12-31 &> gfm_ingest.log &
```
(or stage-by-stage `overpass → monthly → seasonal → history` to checkpoint). ~10 GB overpass tier is fine.

**Seasonal aligned to PTOT (committed).** Ingest now writes `seasonal/{flooded,nobs}/{SEASON}_{YYYY}.tif`; the publish tier-14 `name_fn_gfm` emits `processing=seasonal/variable={flooded|nobs}/season={SEASON}/{var}_{SEASON}_{YYYY}.tif` — i.e. `variable=flooded/season=MAM/flooded_MAM_2020.tif`, matching your PTOT layout so the notebook swaps `source=`/`variable=` in one URL builder. Same 12 window codes.

**Publish tier 14 is added** (recursive `processing=/variable=` walk, overview gate applies — smoke COG had 7 overviews so it passes). After ingest completes:
```
Rscript R/observational/6_publish_obs_to_s3.R --full --tier 14
```
Then **count-verify** (published == local .tif count) AND a **local-vs-S3 diff** (uploader has no built-in verify — objects can silently drop). Report the counts.

**Overpass filename** stays `{YYYYMMDD}T{HHMMSS}.tif` (archive; keeps sub-daily overpasses distinct). Notebook reads monthly/seasonal, not overpass, so date-only isn't needed there.

**One alignment question before publish — confirm the PTOT MONTHLY S3 path.** GFM monthly is currently `processing=monthly/variable=flooded/{YYYY-MM}.tif` (+ `variable=nobs/`). If PTOT monthly uses a different filename/partition (e.g. `variable=PTOT/PTOT_{YYYY}_{MM}.tif`), tell me the exact pattern and I'll align GFM monthly the same way before you publish tier 14. Seasonal is the notebook's display unit so it's the priority; monthly alignment is for URL-builder consistency.

Append `### RESPONSE` with the PTOT monthly path + (after the run) the ingest tallies + publish count-verify, then push.

### RESPONSE — cglabs 2026-08-28 — GO'd; full run LAUNCHED (+ one code fix). PTOT monthly path below.

**PTOT MONTHLY S3 path (confirmed live):** `…/processing=monthly/variable=PTOT/**PTOT-{YYYY}-{MM}.tif**` (e.g. `PTOT-1981-01.tif` … `PTOT-2026-04.tif`) — flat under `variable=PTOT/`, filename `{VAR}-{YYYY}-{MM}.tif`, NO `season=`-style partition at monthly.
→ **To align GFM monthly:** rename `processing=monthly/variable=flooded/{YYYY-MM}.tif` → `processing=monthly/variable=flooded/**flooded-{YYYY}-{MM}.tif**` (and `variable=nobs/nobs-{YYYY}-{MM}.tif`). i.e. prefix the filename with `{var}-`, matching PTOT's `{VAR}-{YYYY}-{MM}.tif`. Then monthly + seasonal both mirror PTOT and the notebook swaps `source=`/`variable=` in one builder. Please tweak the tier-14 `name_fn_gfm` for monthly before publish.

**Full run LAUNCHED** (`--stage all 2018-01-01..2025-12-31`, background) — running clean now, but:

⚠️ **CODE FIX applied (committed `6759ae2`, flag ratify):** the full run **KeyError'd immediately** at `stac_search` L118 `url = nxt["url"]` — the STAC `next` pagination link key is **`href`, not `url`** (+ its `body` is a partial paging token). Smoke never hit it (its late-Apr-2020 window = <500 items = single page). Fixed: `url = nxt.get("href") or url` + merge the token onto the current body so `collections/bbox/datetime` survive across pages. Verified: paged 450 items across 3 page-boundaries cleanly, then relaunched. Please ratify.

**Ingest tallies + publish count-verify → I'll append when the run finishes** (Stage-A ≈ ~1,400 overpass mosaics 2018→now + monthly/seasonal/history — multi-hour). Will then run `--full --tier 14` (after you align the monthly name_fn) + count-verify (local .tif == S3) + local-vs-S3 diff, and report. No publish until the run completes + monthly alignment lands.

---

## [macbook 2026-08-28 #2] — ingest script live: SMOKE first, then hold

Probe answers folded in. `python/ingest_flood_gfm.py` is committed (anon EODC STAC, `ensemble_flood_extent`, Equi7-AF 20 m → EPSG:4326, 0/1/255 coding, 4 tiers). CDH record updated at `metadata/cdh/kenya-flood-gfm.yaml`.

**STEP 1 — smoke (cheap, do first, report back):**
```
python3 python/ingest_flood_gfm.py --smoke
```
Runs ONE overpass end-to-end over a tight late-Apr-2020 Kenya-flood window (mosaic the intersecting Equi7 tiles → EPSG:4326 → clip Kenya → COG), then a gdalinfo gate. Report: output size (WxH), nodata, min/max, #overviews, #tiles mosaicked, file size on disk. This proves the Equi7→4326 mosaic + 0/1/255 coding + overviews before any long run. **Then HOLD** — let macbook eyeball the smoke output before the multi-year Stage-A run.

**STEP 2 — full (only after macbook confirms smoke):** run under nohup (Stage A = ~1400 overpass mosaics over 2018→now, the long one). Timestamped logging + skip-if-exists are built in. Either:
```
nohup python3 python/ingest_flood_gfm.py --stage all --start 2018-01-01 --end 2025-12-31 &> gfm_ingest.log &
```
or stage-by-stage (`--stage overpass` → `monthly` → `seasonal` → `history`) to checkpoint.

Notes: (a) aggregates land at ~111 m (0.001°) to match the pop/exposure grid; overpass stays native ~20 m. (b) seasonal keys are `YYYY-WWW` rolling 3-month (e.g. `2020-MAM`, 12 windows) — **flag if the PTOT seasonal COGs used a different naming/window convention so I align the notebook**. (c) macbook adds the GFM publish tier to `6_publish_obs_to_s3.R` after smoke confirms the on-disk `processing=/variable=` layout. Do NOT publish yet.

Append `### RESPONSE` with the smoke gdalinfo numbers, then push.

### RESPONSE — cglabs 2026-08-28 — SMOKE PASS. Holding for your confirm before the multi-year run. 🟢

```
smoke overpass: 2020-04-24T03:10:11Z (4 Equi7 tiles mosaicked)
WxH        = 40000 x 51000   (Kenya bbox @ ~20 m native / 0.0002° — overpass stays native ✓)
nodata     = 255
min/max    = 0 / 1           (0=not-flooded, 1=flooded, 255=NoData — coding confirmed ✓)
#overviews = 7
#tiles     = 4               (this overpass's swath ∩ Kenya; full-Kenya coverage = 17 tiles across overpasses)
file size  = 7.0 MB on disk  (ZSTD COG; mostly-dry overpass)
out path   = Data/exposure/gfm_flood/overpass/20200424T031011.tif
```
Equi7-AF→EPSG:4326 mosaic + clip + 0/1/255 + overviews all confirmed. **HOLDING — not starting Stage-A / --stage all until you confirm.** (At 7 MB × ~1,400 overpasses the overpass tier ≈ ~10 GB native-20 m; the 111 m aggregates are far smaller.)

**⚠️ SEASONAL NAMING — YES, PTOT differs from your `YYYY-WWW`; align before the seasonal tier:**
The 12 tri-month **window codes are identical** (JFM FMA MAM AMJ MJJ JJA JAS ASO SON OND NDJ DJF) — good. But the **layout/filename differ**:
- **PTOT (live, what I baked):** S3 `…/processing=seasonal/variable=PTOT/**season={SEASON}/**PTOT_{SEASON}_{YYYY}_sum.tif` (e.g. `season=MAM/PTOT_MAM_2020_sum.tif`) — **partitioned by a `season={SEASON}/` sub-dir**, filename `{SEASON}_{YYYY}` order.
- **Your GFM proposal:** flat `processing=seasonal/variable=flooded/{YYYY}-{WWW}.tif` (e.g. `2020-MAM.tif`) — no `season=` partition, `{YYYY}-{SEASON}` order.
- **Recommend GFM match PTOT:** `…/processing=seasonal/variable=flooded/season={SEASON}/flooded_{SEASON}_{YYYY}.tif`. Then the notebook swaps rainfall↔flood by changing only `variable=`/`source=` in the same URL builder; your `YYYY-MM-DD` flat scheme would need a separate path builder. Your call — flagging so you align the notebook + the publish tier's name_fn.

**Also (minor) overpass filename:** tier-1 spec said `{YYYY-MM-DD}.tif`, script writes `{YYYYMMDD}T{HHMMSS}.tif` (`20200424T031011.tif`) — keeps sub-daily overpasses distinct (good, since >1 overpass/day possible), but ≠ the dispatch's `YYYY-MM-DD`. Align if the notebook expects date-only.

Smoke is clean; say go and I launch `--stage all 2018→2025` under nohup (or stage-by-stage). No publish yet (tier not added).

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
