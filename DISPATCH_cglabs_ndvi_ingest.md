# DISPATCH — cglabs ⇄ macbook · MODIS NDVI ingest (KE-ENSO rangeland)

_Append-only. Newest on top. cglabs runs, appends RESPONSE, pushes; macbook reads._

Workstream: per-pixel MODIS NDVI (MOD13Q1, 250 m, seasonal OND/MAM, 2000–) → COG w/ overviews →
publish to `domain=climate/type=vegetation/source=modis-mod13q1/…`. Plan: `NDVI_ingest_plan.md`.
This first dispatch is a **capability probe** — where can the GEE ingest run? No ingest yet.

---

## [macbook 2026-08-16 #3] ACTION → cglabs: NDVI ingest — SMOKE GATE then seasonal full run

Account + specs verified (#2b 🟢). Ingest script shipped: **`python/ingest_ndvi_modis.py`**
(earthaccess → MOD13Q1 → mask fill/reliability → scale /10000 → mosaic 4 Kenya tiles → reproject
Sinusoidal→EPSG:4326 → window mean → COG w/ overviews). Region default = East-Africa/Kenya bbox
(the 4 tiles). **UNTESTED locally (no earthaccess/HDF4 on macbook) — smoke-gate first.**

**Answers to your #2b questions:**
- Creds: use **`~/.netrc` (chmod 600)** for the multi-hour full run (survives non-interactively).
  Personal `earprs` account is fine for this one-off historical bake; a service account is only
  worth it if we make NDVI recurring — note it, don't block.
- Reliability mask: **ON** (script default — keeps pixel-reliability 0/1, drops cloud/snow).
- Probe HDF: **delete** `ndvi_probe_dl/` (229 MB), no longer needed.
- Run **from working_dir** (so `Data/ndvi_modis/NDVI/` resolves), or pass `--out`.

### Steps
1. `git pull` develop. Ensure `~/.netrc` has the urs entry (chmod 600). `rm -rf ndvi_probe_dl/`.
2. **SMOKE GATE** (1 year × OND, ~1–2 min + one window download):
   `python3 python/ingest_ndvi_modis.py --smoke`
   → writes `Data/ndvi_modis/NDVI/NDVI_OND_2015_mean.tif`.
3. **GATE checks** on that COG:
   ```bash
   gdalinfo Data/ndvi_modis/NDVI/NDVI_OND_2015_mean.tif \
     | grep -Ei 'Size is|Coordinate System|EPSG|Pixel Size|Overviews|STATISTICS_(MIN|MEAN|MAX)'
   ```
   Expect: **EPSG:4326**, overviews present, pixel ~0.002° (~250 m), NDVI stats in ~**−0.2 … 0.95**,
   extent covering Kenya (~34–42°E, −5…5.5°N). If NDVI looks like raw DN (thousands) or CRS is
   Sinusoidal or overviews missing → STOP, paste the gdalinfo.
4. If green → **seasonal full run** (OND + MAM, 2000–2025):
   `python3 python/ingest_ndvi_modis.py --seasons OND,MAM --years 2000:2025`
   ~52 COGs. Heavier: ~4 tiles × ~6 composites × 2 seasons × 26 yr HDF downloads (~200+ GB
   transient; auto-cleaned per window via tempdir). Report elapsed + COG count.
   **Defer `--annual`** for now (full-year composite download ~2× cost) — flag if you want it.
5. Report the local COG list; macbook then adds the `type=vegetation` publish tier to
   `6_publish_obs_to_s3.R` and dispatches the S3 upload.

### RESPONSE block (append, then push)
```
netrc set = y/n    probe hdf removed = y/n
smoke COG: EPSG=?  overviews=?  pixelsize=?  NDVI min/mean/max=?
gate = PASS/FAIL (if fail, gdalinfo pasted)
seasonal full: COGs written = ?/52   elapsed = ?
→ NDVI INGEST DONE (ready to publish) = yes/no
```
Note: if a window errors mid-run, the script logs it and continues (idempotent skip on re-run);
report any ERROR lines.

## [cglabs 2026-08-16 #3] RESPONSE — NDVI INGEST DONE. 52/52 COGs, gate PASS. Ready to publish. 🟢

```
netrc set = y (chmod 600, ~/.netrc, home dir — outside repo)    probe hdf removed = y
smoke COG: EPSG=4326  overviews=yes (5 lvls)  pixelsize=0.002099° (~233 m)  NDVI min/mean/max = -0.20 / 0.48 / 0.999
gate = PASS
seasonal full: COGs written = 52/52 (51 written + 1 skip = smoke OND-2015 reused; 0 empty, 0 ERROR)   elapsed = ~3h25m (07:24→10:49)
→ NDVI INGEST DONE (ready to publish) = yes
```
Layout: `Data/ndvi_modis/NDVI/NDVI_{OND|MAM}_{YYYY}_mean.tif` — **OND 26 + MAM 26 = 52**, 2000–2025. Spot-checked OND-2000 / MAM-2025 / OND-2015: all EPSG:4326, overviews present, NDVI ∈ [-0.20, 1.00], mean 0.45–0.49 (scaled /10000, NoData applied, reliability-masked). Extent 30–50.8°E / -10–10°N (full 4-tile footprint, covers Kenya). ~275 MB/COG.

⚠️ **CODE FIX applied on cglabs (flagged for ratification):** `python/ingest_ndvi_modis.py` L152-154 — `earthaccess.download()` returns the `.hdf` **plus BROWSE `.jpg` + `.cmr.xml`**; the `keep` filter checked date but **not extension**, so a browse jpg reached the HDF4 subdataset opener → `RasterioIOError` on the first smoke attempt. Added an `.hdf` extension guard (`str(f).endswith(".hdf")`). One line; re-ran smoke → PASS → full run clean. Please ratify.

**Ready for macbook:** add the `type=vegetation` / `source=modis-mod13q1` publish tier to `6_publish_obs_to_s3.R` (name_fn for `NDVI_{SEASON}_{YEAR}_mean.tif` → `…/domain=climate/type=vegetation/source=modis-mod13q1/region=africa/processing=seasonal/variable=NDVI/season={SEASON}/`), then dispatch the S3 upload — I'll run it. `--annual` deferred (say the word to add). netrc stays on cglabs for that run (flag if you want it removed after).

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

## [cglabs 2026-08-13 #2] RESPONSE — discovery WORKS anon; download blocked on the free Earthdata account. STOP per prereq.

```
earthaccess installed = y (0.18.0)     Earthdata login = NO-ACCOUNT (no ~/.netrc urs entry, no EARTHDATA_* env; login(env)+login(netrc) both fail)
granules (Kenya 2015-11 window) = 4    first native-id = MOD13Q1.A2015305.h22v08.061.2021336135713
one granule downloaded = NO (protected bucket → 302 redirect to URS login; needs Earthdata auth)
NDVI subdataset name = UNVERIFIED (can't dump without downloading the .hdf). Documented v061 expected:
    HDF4_EOS:EOS_GRID:"<file>.hdf":MODIS_Grid_16DAY_250m_500m_VI:"250m 16 days NDVI"  ← confirm post-account
native CRS = UNVERIFIED (expected MODIS Sinusoidal SR-ORG:6974)   native res = UNVERIFIED (expected ~231.66 m)
→ NON-GEE NDVI INGEST VIABLE HERE = needs-earthdata-account
```

**What works (everything except the login):**
- `earthaccess` installs + imports clean (0.18.0). CMR **discovery is public/anon** — searched MOD13Q1.061 over the Kenya bbox (33.9,-4.7,41.9,5.5), 2015-11-01→16 → **exactly 4 granules**, the expected tiles: **h21v08, h21v09, h22v08, h22v09**. native-ids captured (`MOD13Q1.A2015305.{tile}.061…`).
- Egress confirmed to LP DAAC cloud (`data.lpdaac.earthdatacloud.nasa.gov`).
- COG toolchain present (rasterio 1.4.3 + gdal 3.10.3).

**The one blocker:** the `.hdf` sits in `lp-prod-protected/…` → anon fetch = **302 to URS login**. earthaccess `login()` fails (no netrc, no env). So I could **not** download a granule, hence could **not** verify the NDVI subdataset name / native CRS / res (dispatch rule = report only what's verified; the values above are the documented v061 expectation, to be confirmed once auth lands).

**To unblock (matches your prereq — 30 s, free):** provision a **NASA Earthdata Login** and drop it on the node as either
`~/.netrc` → `machine urs.earthdata.nasa.gov login <u> password <p>` (chmod 600), or `EARTHDATA_USERNAME`/`EARTHDATA_PASSWORD` env. Then I re-run step 3 (download 1 granule + `gdalinfo` subdataset dump) and return the verified subdataset name + CRS/res. Everything else is ready — this node **can** run the full non-GEE ingest once the login exists.

## [cglabs 2026-08-16 #2b] RESPONSE — ACCOUNT LIVE. Auth + download + subdataset dump ALL VERIFIED. Ingest viable here. 🟢

Earthdata account provisioned (`earprs`) + activated (first login was `invalid_account_status: temporary` → after web password-reset it authenticated). One 250 m granule downloaded (229 MB) + subdatasets dumped. **Needed one toolchain add: `conda install -c conda-forge libgdal-hdf4`** (conda gdal 3.10.3 shipped without the HDF4 plugin — GDAL named the fix; installed, HDF4 driver now present).

```
earthaccess login = ok (environment strategy)   granules (full Kenya window 33.9,-4.7,41.9,5.5) = 4 (tiles h21v08/09, h22v08/09)
one granule downloaded = yes (MOD13Q1.A2015305.h21v09.061…hdf, 229 MB)
NDVI subdataset name = HDF4_EOS:EOS_GRID:"<file>.hdf":MODIS_Grid_16DAY_250m_500m_VI:"250m 16 days NDVI"   (SDS 1 of 12; EVI=SDS2, VI Quality=SDS3, pixel reliability=SDS12)
native CRS = MODIS Sinusoidal (PROJCRS unnamed, METHOD "Sinusoidal", sphere R=6371007.181)
native res = 231.656 m   grid = 4800×4800 per tile   origin x = 3335851.559
scale_factor = 10000  (real NDVI = DN / 10000)   add_offset = 0   NoData = -3000
NDVI DN range this granule = min -2000 (-0.20) / max 9996 (0.9996) / mean 4034 (0.40) — proper NDVI ✓
→ NON-GEE NDVI INGEST VIABLE HERE = YES
```

**For the ingest script (verified specs):** search per 16-day window → for each of the 4 tiles pull SDS "250m 16 days NDVI" → apply `NoData=-3000`, scale `/10000` → mosaic the 4 tiles → reproject **Sinusoidal→EPSG:4326** → seasonal (OND/MAM) mean → COG **with overviews** (unlike the 5b seasonal COGs — 4800×4800×4 tiles is big, overviews matter for the renderer) → publish tier in `6_publish_obs_to_s3.R` under `domain=climate/type=vegetation/source=modis-mod13q1/…`. Recommend also masking on SDS "250m 16 days pixel reliability" (keep 0/1 = good/marginal) to drop cloud/snow.

**Creds handling (flag):** I used the password **env-only in-process — NOT written to `~/.netrc` or the repo** (no plaintext secret on disk / in git). The full ingest run will need creds available non-interactively — tell me whether to (a) write `~/.netrc` (chmod 600) on cglabs for the ingest, or (b) you'd rather set env each run. Also: the account is a **personal** Earthdata login; for a recurring/automated ingest a dedicated service account is cleaner. Left the 229 MB probe HDF at `ndvi_probe_dl/` (gitignored path) — say if you want it removed.

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
