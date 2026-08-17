# DISPATCH — cglabs ⇄ macbook · riverine flood ingest (KE-ENSO)

_Append-only. Newest on top. cglabs runs, appends RESPONSE, pushes; macbook reads._

Workstream: two flood products for the KE-ENSO Explorer — **JRC GloFAS flood hazard** (static
return-period) + **Global Flood Database** (observed events, ENSO-composable). Plan:
`FLOOD_ingest_plan.md`. This first dispatch is an **access probe** — settle where each source is
reachable from the node before writing ingests. No ingest yet.

---

## [macbook 2026-08-17 #3] ACTION → cglabs: GFD observed-flood ingest (smoke → all years → publish Tier 7)

Global Flood Database v1.4 (observed MODIS inundation, 2000–2018, ENSO-composable). Ingest shipped:
**`python/ingest_flood_gfd.py`** (list 913 events from public `gfd_v1_4` GCS → cheap Kenya extent
filter via `/vsizip//vsicurl` byte-range → warp `flooded` band to fixed Kenya grid → per-year UNION
of flooded → COG w/ overviews). Structure verified by macbook probe (band1=flooded, EPSG:4326,
~250 m). **No auth. UNTESTED end-to-end locally (no osgeo/rasterio on macbook — list+extent+year
logic verified) — smoke-gate first.** Publish **Tier 7** → `domain=climate/type=flood/source=global-flood-db/region=east-africa/processing=annual/variable=flooded/`.

### Steps
1. `git pull` develop.
2. **SMOKE** (year 2015 only — 39 events, filters to Kenya ones): `python3 python/ingest_flood_gfd.py --smoke`
   → `Data/flood_gfd/GFD/flooded_2015.tif`. Watch the log: "+DFO_… (Kenya event N)" lines then a
   flooded-px count + coverage.
3. **GATE:** `gdalinfo Data/flood_gfd/GFD/flooded_2015.tif | grep -Ei 'Size is|EPSG|Overviews|Minimum|Maximum'`
   Expect EPSG:4326, overviews, values **0/1** (min 0, max 1), Kenya extent. If 0 Kenya events in
   2015 → try another year (e.g. `--smoke` edited, or note it); if values not 0/1 → STOP, paste.
4. **Full:** `python3 python/ingest_flood_gfd.py`  (2000–2018 → up to 19 year-COGs; years with no
   Kenya event are skipped). Report per-year Kenya-event counts + COGs written.
5. **Publish + count-verify** (uploader doesn't self-verify):
   ```bash
   Rscript R/observational/6_publish_obs_to_s3.R --dry-run --tier 7
   Rscript R/observational/6_publish_obs_to_s3.R --full --tier 7
   ls Data/flood_gfd/GFD/*.tif | wc -l                                  # local
   aws s3 ls --recursive "s3://digital-atlas/domain=climate/type=flood/source=global-flood-db/region=east-africa/" | wc -l   # must equal local
   ```
6. Verify live: range-GET one → 206 + CORS.

### RESPONSE block (append, then push)
```
smoke 2015: Kenya events = ?   flooded px = ?   gate = PASS/FAIL (min/max)
full: year-COGs written = ?  (list years + Kenya-event counts)
dry-run rows = ?   published = ?   local==S3 = yes/no
live 206 = yes/no  CORS = yes/no
base URL = https://digital-atlas.s3.amazonaws.com/domain=climate/type=flood/source=global-flood-db/region=east-africa/processing=annual/variable=flooded/
→ GFD FLOOD LIVE = yes/no
```
Note: some years may have 0 Kenya events (skipped) — that's expected, report which years produced COGs.

---

## [macbook 2026-08-17 #2] ACTION → cglabs: JRC flood ingest (smoke → 7 RP → publish Tier 6)

Access verified (#1 🟢). Ingest shipped: **`python/ingest_flood_jrc.py`** (mosaic 4 Kenya tiles
from source.coop via /vsicurl → crop Kenya → clamp <0→NaN → COG w/ overviews). Publish **Tier 6**
(`--tier 6`, opt-in) → `domain=climate/type=flood/source=jrc-glofas/region=east-africa/processing=return-period/variable=flood-depth/rp={RP}/`.
**UNTESTED locally — smoke-gate first.**

### Steps
1. `git pull` develop.
2. **SMOKE** (RP100 only): `python3 python/ingest_flood_jrc.py --smoke`
   → `Data/flood_jrc/JRC/flood-depth_rp100.tif`.
3. **GATE:** `gdalinfo Data/flood_jrc/JRC/flood-depth_rp100.tif | grep -Ei 'Size is|EPSG|Overviews|Minimum|Maximum'`
   Expect EPSG:4326, overviews, depth **≥0** (no negatives — clamp applied), max ~tens of m, Kenya
   extent. If negatives remain or CRS wrong → STOP, paste gdalinfo.
4. **Full:** `python3 python/ingest_flood_jrc.py`  (7 RP: 10/20/50/75/100/200/500).
5. **Publish:** `Rscript R/observational/6_publish_obs_to_s3.R --dry-run --tier 6` (expect 7 rows),
   then `Rscript R/observational/6_publish_obs_to_s3.R --full --tier 6`.
6. **VERIFY (incl. count — the uploader doesn't self-verify, NDVI MAM-2008 dropped silently):**
   ```bash
   aws s3 ls --recursive "s3://digital-atlas/domain=climate/type=flood/source=jrc-glofas/region=east-africa/" | wc -l   # expect 7
   curl -s -o /dev/null -w '%{http_code}\n' -r 0-0 "https://digital-atlas.s3.amazonaws.com/domain=climate/type=flood/source=jrc-glofas/region=east-africa/processing=return-period/variable=flood-depth/rp=100/flood-depth_rp100.tif"  # 206
   ```
   Confirm local (7) == S3 (7); if short, re-run `--full --tier 6`.

### RESPONSE block (append, then push)
```
smoke gate = PASS/FAIL (depth min/max=?)
full ingest = ?/7 RP
dry-run rows = ?/7   published = ?/7   S3 count == local = yes/no
live 206 = yes/no   CORS = yes/no
base URL = https://digital-atlas.s3.amazonaws.com/domain=climate/type=flood/source=jrc-glofas/region=east-africa/processing=return-period/variable=flood-depth/rp={RP}/
→ JRC FLOOD LIVE = yes/no
```
GFD (observed events, `gfd_v1_4` GCS) = macbook's next follow-up ingest (bigger). Not this dispatch.

---

## [macbook 2026-08-17 #1] ACTION → cglabs: flood-source access probe (JRC + GFD)

Report only what you verify. Kenya bbox = `33.9,-4.7,41.9,5.5` (W,S,E,N).

### A) JRC GloFAS Flood Hazard v2.1 (return-period flood depth/hazard, 90 m, public CC-BY)
Find a **working public download** for one return period (RP100) — try in order, report which works:
1. JRC Data Catalogue collection id-0054 / data.europa.eu (dataset `floodMapGL_rp100y` or the v2.1
   equivalent). Search the live URL (the old `cidportal.jrc.ec.europa.eu/ftp/...FLOODS/GlobalMaps/`
   path now 301→jeodpp→404, so find the current one).
2. source.coop COG: `nlebovits/jrc-glofas` (repo lists RP 10..500) — get the object URL.
3. GEE `JRC/CEMS_GLOFAS/FloodHazard/v2_1` (only if GEE ends up provisioned).
Once you have a URL, verify + crop Kenya:
```bash
gdalinfo "/vsicurl/<URL>" 2>/dev/null | grep -Ei 'Size is|EPSG|Pixel Size|Band|Unit|Minimum|Maximum' | head
gdalwarp -q -te 33.9 -4.7 41.9 5.5 -t_srs EPSG:4326 -of COG \
  "/vsicurl/<URL>" /tmp/flood_rp100_KEN.tif
gdalinfo /tmp/flood_rp100_KEN.tif | grep -Ei 'Size is|EPSG|Minimum|Maximum'
```
Report: the working URL, native res/CRS/units (depth m? hazard class 0–4?), and that the Kenya
crop succeeded + its value range.

### B) Global Flood Database v1 (observed inundation, ~2000–2018, for the ENSO composite)
Which access route works from this node?
1. **GEE** `GLOBAL_FLOOD_DB/MODIS_EVENTS/V1` — needs the GEE auth we didn't set up (#1 NDVI probe
   = needs-auth). Confirm still absent (`python3 -c "import ee"`), and whether a **service-account**
   route is acceptable (we can provision one, like the Earthdata login).
2. **Non-GEE:** Cloud to Street / Dartmouth Flood Observatory data portal
   (global-flood-database.cloudtostreet.info) or a Zenodo/figshare mirror — is the event raster
   collection downloadable directly (egress test + any download URL)? Report what you find.

### RESPONSE block (append, then push)
```
JRC working URL = ?    native res/CRS = ?    units = depth-m / hazard-class 0-4
JRC Kenya crop = ok/fail   value range = ?
GFD via GEE = needs-auth/ok    GFD non-GEE download = found(url)/none
→ JRC INGEST VIABLE = yes/no    GFD ROUTE = gee-serviceaccount / portal / none
```

Once this lands: macbook writes the **JRC ingest** (crop 7 RP → COG → new `type=flood` tier) —
small, unblocked — and scopes GFD off whichever route you report.

## [cglabs 2026-08-17 #1] RESPONSE — both sources reachable. JRC viable + verified. GFD via public GCS (non-GEE). 🟢

```
JRC working URL = source.coop nlebovits/jrc-glofas (anon S3 + https)   native res/CRS = 90 m (0.000833°) / EPSG:4326 Float32   units = depth-m (flood depth, RP-tagged)
JRC Kenya crop = ok   value range = ~0–42.2 m (mean 1.26; min -1.02 = nodata/resample edge, clamp on ingest)
GFD via GEE = needs-auth (ee still absent)    GFD non-GEE download = FOUND: gs://gfd_v1_4 public bucket
→ JRC INGEST VIABLE = yes    GFD ROUTE = portal (public GCS gfd_v1_4, no GEE, no auth)
```

**A) JRC GloFAS Flood Hazard (source.coop `nlebovits/jrc-glofas`) — VERIFIED:**
- Route 1 (JRC/data.europa) skipped — route 2 works cleanly. Anon access both ways:
  - HTTPS: `https://data.source.coop/nlebovits/jrc-glofas/depth-rp{RP}/{TILE}/{TILE}_RP{RP}_depth.tif`
  - S3 (listable): `aws s3 ls --no-sign-request s3://us-west-2.opendata.source.coop/nlebovits/jrc-glofas/`
- Layout: `depth-rp{RP}/ID{n}_{N/S}{lat}_{E/W}{lon}/…_RP{RP}_depth.tif`, **10° tiles**, RP = **10/20/50/75/100/200/500** (7 return periods). Native **90 m (0.000833°), EPSG:4326, Float32, flood DEPTH in metres**. One tile = 11999×11999, HTTP 206, depth 0.1–7.8 m.
- **Kenya = 4 tiles:** `ID150_N10_E30, ID151_N0_E30, ID161_N10_E40, ID162_N0_E40`. gdalwarp mosaic+crop to Kenya bbox → **9600×12240, EPSG:4326, 90 m, depth ~0–42.2 m, mean 1.26 m** (min -1.02 = a nodata/resample edge sliver — clamp `<0→0` or set NoData on ingest). Crop **succeeded**. So the JRC ingest is unblocked: for each of 7 RP → mosaic the 4 Kenya tiles → COG (+overviews) → `type=flood` tier.

**B) Global Flood Database v1 — non-GEE route FOUND:**
- GEE (`GLOBAL_FLOOD_DB/MODIS_EVENTS/V1`): still **needs-auth** — `import ee` = ModuleNotFoundError (unchanged since NDVI #1). A service account would work (like Earthdata) but **not needed** — see below.
- **Non-GEE (recommended): `https://storage.googleapis.com/gfd_v1_4/` — public, anon-listable, no auth.** **913 per-event objects** (`DFO_{id}_From_{start}_to_{end}.zip`), HEAD = 206. This is the Cloud-to-Street GFD v1.4 event archive (each zip = per-event MODIS inundation raster). Also mirrored on **Zenodo DOI 10.5281/zenodo.7545697**. Egress to both = 200. So GFD ingest can run here entirely non-GEE (pull event zips → extract inundation rasters → filter to Kenya bbox + ENSO years → composite).

**Ready for macbook:** JRC ingest is small + fully unblocked (write the 7-RP crop→COG→`type=flood` tier, I run it). GFD scoping off the `gfd_v1_4` GCS bucket — the 913 events span 2000–2018, ENSO-composable; needs a per-event Kenya-filter + reliability handling (own follow-up). Left the probe crop at `/tmp/flood_rp100_KEN.tif` (scratch, not committed).

---
