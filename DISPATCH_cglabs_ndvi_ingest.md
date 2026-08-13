# DISPATCH — cglabs ⇄ macbook · MODIS NDVI ingest (KE-ENSO rangeland)

_Append-only. Newest on top. cglabs runs, appends RESPONSE, pushes; macbook reads._

Workstream: per-pixel MODIS NDVI (MOD13Q1, 250 m, seasonal OND/MAM, 2000–) → COG w/ overviews →
publish to `domain=climate/type=vegetation/source=modis-mod13q1/…`. Plan: `NDVI_ingest_plan.md`.
This first dispatch is a **capability probe** — where can the GEE ingest run? No ingest yet.

---

## [macbook 2026-08-13 #2] ACTION → cglabs: NON-GEE path — earthaccess + LP DAAC auth/discovery gate

**Dropping GEE.** The #1 probe showed GEE needs auth/provisioning we don't want — but egress to
**LP DAAC = 200** and rasterio 1.4.3 + gdal 3.10.3 are present. So pull **MOD13Q1 direct from NASA
LP DAAC via `earthaccess`** (only "auth" = a **free NASA Earthdata Login** — much lighter than a GEE
service account). This dispatch is a cheap gate: prove auth + discovery, and capture the HDF
subdataset metadata I need to write the ingest correctly. **No full ingest yet.**

### Prereq
Free **NASA Earthdata Login** (https://urs.earthdata.nasa.gov). Provide creds via `~/.netrc`
(`machine urs.earthdata.nasa.gov login <u> password <p>`) OR `EARTHDATA_USERNAME` / `EARTHDATA_PASSWORD`
env. **If no account exists, STOP and report — we'll get one provisioned (30 s, free).**

### Steps
1. `git pull` develop. `pip install earthaccess` (conda env is writable).
2. **Auth + discovery test (no bulk download):**
   ```python
   python3 - <<'PY'
   import earthaccess
   earthaccess.login()  # ~/.netrc or EARTHDATA_* env
   r = earthaccess.search_data(short_name="MOD13Q1", version="061",
         bounding_box=(33.9,-4.7,41.9,5.5), temporal=("2015-11-01","2015-11-16"))
   print("granules:", len(r))
   print("first:", r[0]["meta"]["native-id"] if r else "NONE")
   PY
   ```
   Expect ~4 granules (Kenya spans MODIS tiles h21v08/09, h22v08/09).
3. **Download ONE granule + dump its subdatasets** (this is what I need to build the ingest —
   the exact NDVI subdataset name + native CRS/res):
   ```python
   python3 - <<'PY'
   import earthaccess
   earthaccess.login()
   r = earthaccess.search_data(short_name="MOD13Q1", version="061",
         bounding_box=(36.5,-1.5,38.0,0.5), temporal=("2015-11-01","2015-11-16"))
   f = earthaccess.download(r[:1], local_path="./ndvi_probe_dl")
   print("file:", f)
   PY
   gdalinfo ./ndvi_probe_dl/*.hdf 2>/dev/null | grep -iE 'SUBDATASET_.*_NAME|NDVI|Coordinate System is|PROJCRS|sinusoidal' | head -30
   ```
   Paste the `SUBDATASET_*_NAME` lines (esp the "250m 16 days NDVI" one) + the CRS line.

### RESPONSE block (append, then push)
```
earthaccess installed = y/n     Earthdata login = ok/no-account
granules (Kenya 2015-11 window) = ?    first native-id = ?
one granule downloaded = y/n
NDVI subdataset name = HDF4_EOS:EOS_GRID:"...":MODIS_Grid_16DAY_250m_500m_VI:"250m 16 days NDVI"
native CRS = ? (expect MODIS Sinusoidal)   native res = ? (~231 m)
→ NON-GEE NDVI INGEST VIABLE HERE = yes / no / needs-earthdata-account
```

Once this returns green + the subdataset name, macbook writes the full ingest: search per
16-day window → mosaic tiles → reproject Sinusoidal→EPSG:4326 → seasonal (OND/MAM) mean →
COG w/ overviews → publish tier in `6_publish_obs_to_s3.R` (`type=vegetation`).

---

## [macbook 2026-08-13 #1] ACTION → cglabs: GEE capability probe (does this node have Earth Engine?)

MODIS NDVI comes from Google Earth Engine (`MODIS/061/MOD13Q1`). Before writing the export
script, confirm whether THIS node can reach GEE, or whether ingest must run elsewhere (a
GEE-enabled node / macbook one-off). Report only what you verify.

### Checks
1. **earthengine-api installed?**
   `python3 -c "import ee; print('ee', ee.__version__)"` — report version or ImportError.
2. **Auth available?** Try, in order, and report which (if any) works:
   ```python
   import ee
   try:
       ee.Initialize()                      # persisted user creds
       print("INIT ok (user creds)")
   except Exception as e:
       print("user-init failed:", str(e)[:120])
   ```
   Also report: is there a **service-account key** on the node?
   `ls ~/.config/earthengine/ 2>/dev/null; env | grep -iE 'EARTHENGINE|GOOGLE_APPLICATION_CRED' | sed 's/=.*/=<set>/'`
3. **Live read (only if Initialize worked):**
   ```python
   ic = ee.ImageCollection("MODIS/061/MOD13Q1").filterDate("2015-10-01","2015-12-31")
   print("MOD13Q1 OND-2015 images:", ic.size().getInfo())   # expect ~6 (16-day)
   ```
4. **Export path:** can the node write a GCS bucket (for `Export.image.toCloudStorage`)?
   `gsutil ls 2>&1 | head` (report if gsutil present + any bucket). If no GCS, note it — we'd
   pull arrays locally via `geemap`/`xee` instead.
5. **Local COG toolchain:** `python3 -c "import rasterio; print('rasterio', rasterio.__version__)"`
   (gdal already confirmed 3.10.3). Report rasterio present y/n.
6. **Alt path if no GEE:** if ee is absent/unauthable here, note whether the node has plain
   internet egress (so a non-GEE ingest via NASA LP DAAC / AppEEARS could run here instead).

### RESPONSE block (append, then push)
```
ee installed = ? (version)      Initialize = ok/failed (mode)
service-account key present = y/n
MOD13Q1 OND-2015 image count = ?   (proves live GEE read)
GCS/gsutil = present/absent (bucket?)
rasterio = y/n
egress for non-GEE ingest = y/n
→ CAN RUN GEE INGEST HERE = yes / no / needs-auth
```

If **no** — say what's missing; we route the ingest to a GEE-enabled node or a macbook one-off
export, then hand cglabs only the finished COGs to publish (Tier reuse in `6_publish_obs_to_s3.R`).

## [cglabs 2026-08-13 #1] RESPONSE — GEE NOT set up here (needs-auth). Egress + COG toolchain present.

```
ee installed = NO (ModuleNotFoundError: No module named 'ee'; geemap/xee also absent)   Initialize = n/a (no ee)
service-account key present = n   (no ~/.config/earthengine/; no EARTHENGINE_* / GOOGLE_APPLICATION_CREDENTIALS env)
MOD13Q1 OND-2015 image count = n/a (no ee to read)
GCS/gsutil = present (/usr/bin/gsutil) but UNCONFIGURED (no project id, unauthed)
rasterio = y (1.4.3)   [gdal 3.10.3 already confirmed]
egress for non-GEE ingest = y  (LP DAAC e4ftl01.cr.usgs.gov = 200; earthengine.googleapis.com reachable = 404 = server hit, no auth)
→ CAN RUN GEE INGEST HERE = needs-auth
```

**What's missing (all fixable on this node, none are hard blockers):**
1. `earthengine-api` not installed — pip-installable (`pip install earthengine-api geemap` — conda env is writable, we installed `gh` earlier).
2. **No GEE credentials** — no persisted user creds, no service-account key. Needs either `earthengine authenticate` (interactive, browser device-code like the gh login) OR a service-account JSON dropped at `~/.config/earthengine/` + `GOOGLE_APPLICATION_CREDENTIALS`.
3. **No GCS project** for `Export.image.toCloudStorage` — gsutil is present but unconfigured. **Workaround:** egress works, so `geemap`/`xee` can pull arrays **directly to local** (no GCS needed) → write COGs with the rasterio/gdal already here.

**Recommendation:** this node is a viable GEE ingest host *once authed* — it has internet egress + the full COG toolchain (rasterio 1.4.3 + gdal 3.10.3) + ample compute. Fastest route: `pip install earthengine-api geemap` + a **service-account key** (non-interactive, works in background runs — preferable to interactive `authenticate`), then pull MOD13Q1 via `geemap`/`xee` to local + COG-ify here. **Alt:** if you'd rather not provision GEE creds on cglabs, route the export to a GEE-enabled node / macbook one-off and hand cglabs the finished arrays/COGs to publish (Tier reuse in `6_publish_obs_to_s3.R`) — egress=yes means a **non-GEE path (LP DAAC / AppEEARS, MOD13Q1 direct)** could also run here entirely without GEE. Your call on GEE-creds vs non-GEE ingest.

---
