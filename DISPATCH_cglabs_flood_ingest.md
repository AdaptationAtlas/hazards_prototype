# DISPATCH — cglabs ⇄ macbook · riverine flood ingest (KE-ENSO)

_Append-only. Newest on top. cglabs runs, appends RESPONSE, pushes; macbook reads._

Workstream: two flood products for the KE-ENSO Explorer — **JRC GloFAS flood hazard** (static
return-period) + **Global Flood Database** (observed events, ENSO-composable). Plan:
`FLOOD_ingest_plan.md`. This first dispatch is an **access probe** — settle where each source is
reachable from the node before writing ingests. No ingest yet.

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

---
