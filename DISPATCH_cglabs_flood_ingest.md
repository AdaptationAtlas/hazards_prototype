# DISPATCH — cglabs ⇄ macbook · riverine flood ingest (KE-ENSO)

_Append-only. Newest on top. cglabs runs, appends RESPONSE, pushes; macbook reads._

Workstream: two flood products for the KE-ENSO Explorer — **JRC GloFAS flood hazard** (static
return-period) + **Global Flood Database** (observed events, ENSO-composable). Plan:
`FLOOD_ingest_plan.md`. This first dispatch is an **access probe** — settle where each source is
reachable from the node before writing ingests. No ingest yet.

---

## [macbook 2026-08-18 #4] ACTION → cglabs: validate the new overview publish-gate (+ ratify makedirs fix)

Both flood tiers LIVE (JRC 7/7, GFD 15/15 🟢) — thanks. Two housekeeping items, then flood is closed.

- **Ratified:** your `makedirs`-before-warp fix in `ingest_flood_jrc.py` (correct — my bug). No action.
- **New publish-gate shipped** (`8177ad7`): `6_publish_obs_to_s3.R --full` now HARD-STOPS before upload
  if any COG lacks internal overviews (dash requirement); `--dry-run` warns. I couldn't run it
  locally (no gdalinfo-on-store on macbook) — **validate it on real data:**

### Steps
1. `git pull` develop.
2. **Gate sanity — expect PASS** on the fine tiers (they have overviews):
   `Rscript R/observational/6_publish_obs_to_s3.R --dry-run --tier 5`  (NDVI)
   `Rscript R/observational/6_publish_obs_to_s3.R --dry-run --tier 6`  (JRC flood)
   `Rscript R/observational/6_publish_obs_to_s3.R --dry-run --tier 7`  (GFD flood)
   → each prints `[ok] overview check: all N COG(s) have overviews.`
3. **Gate catches the known gap — expect WARNING** on monthly PTOT (shipped pre-fix, no overviews):
   `Rscript R/observational/6_publish_obs_to_s3.R --dry-run --tier 3`
   → expect `[!] OVERVIEW WARNING: N/M COG(s) lack internal overviews`. This is EXPECTED (monthly
   PTOT is the coarse 5 km laggard; forward-fix only per p.steward — do NOT re-COG it now). Just
   confirm the gate correctly flags it. (If you ever re-publish `--tier 3`, `--full` will now block
   until re-COGed with `recog_overviews.R`, or `ALLOW_NO_OVERVIEWS=1`.)

### RESPONSE block (append, then push)
```
tier5 (NDVI) gate = ok/warn(?)     tier6 (JRC) = ok/warn(?)     tier7 (GFD) = ok/warn(?)
tier3 (monthly PTOT) gate = warn (N/M lack overviews)  [expected]
--full gate behaviour confirmed = yes/no
→ OVERVIEW GATE VALIDATED = yes/no
```
After this: flood + overview-gate closed. macbook relays flood URLs to KE-ENSO + flags
`type=vegetation`/`type=flood` to Brayden. Nothing else queued for cglabs.

## [cglabs 2026-08-18 #4] RESPONSE — overview gate VALIDATED on real data. Works. ⚠️ but it catches MORE than PTOT — 256 live SPEI COGs also lack overviews.

```
tier5 (NDVI) gate = ok (all 52 have overviews)     tier6 (JRC) = ok (all 7)     tier7 (GFD) = ok (all 15)
tier3 (monthly) gate = WARN 800/1632 lack overviews  [expected for PTOT — but see flag]
--full gate behaviour confirmed = yes (--full --tier 3 → "PUBLISH GATE FAILED … Execution halted", 0 uploads, blocked before any upload)
→ OVERVIEW GATE VALIDATED = yes
```
Gate logic correct: `[ok]` on the fine tiers, WARN on `--dry-run`, HARD-STOP on `--full` (verified no upload occurred — 0 `done:` lines). makedirs fix ratification noted, thanks.

⚠️ **FLAG — the 800 is NOT just PTOT.** Tier 3 = `PTOT + SPEI-03 + SPEI-12` (1632 COGs). Breakdown of the 800 lacking overviews:
- **PTOT: 544/544** — the known 5 km laggard (forward-fix only, per p.steward — left alone ✓).
- **SPEI-03: 101/544** + **SPEI-12: 155/544** = **256 SPEI COGs lack overviews** — and these are **LIVE on S3** (published #5/#6, before the gate existed). So the SPEI drought layer has 256 files the dash can't render zoomed-out. Likely the `fix_spei_inf.R` (#6) rewrite dropped overviews on the files it touched (it re-COGed via `write_seasonal_cog` — may not have added pyramids), or those specific SPEI COGs never got them. **Not caught earlier because the gate didn't exist at #5/#6 publish time.**

**Recommend (macbook call):** run `recog_overviews.R` over the SPEI store (256 files) + re-publish `--tier 3` (gate will then pass for SPEI; PTOT stays the known laggard, use `ALLOW_NO_OVERVIEWS=1` or leave tier-3 PTOT unpublished-as-is). Small job, I'll run it on your word. NDVI/JRC/GFD/climatology-seasonal all clean. Otherwise flood + gate closed.

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

## [cglabs 2026-08-18 #3] RESPONSE — GFD FLOOD LIVE. 15/15 year-COGs published + verified. 🟢

```
smoke 2015: Kenya events = 1 (DFO_4317, Dec-2015 El Niño)   flooded px = 13739   gate = PASS (min 0 / max 1, EPSG:4326 + overviews)
full: year-COGs written = 15 (14 written + smoke 2015; 4 years empty/skipped)
  years+Kenya-events: 2001·2002·2003·2005·2006·2007·2008·2011·2012(5)·2013(3)·2014(4)·2015(1)·2016(2)·2017(1)·2018(2)
  (no COG for 2000/2004/2009/2010/2019 = 0 Kenya events in GFD — expected)
dry-run rows = 15   published = 15/15   local==S3 = yes (15 == 15)
live 206 = yes   CORS = yes (*)
base URL = https://digital-atlas.s3.amazonaws.com/domain=climate/type=flood/source=global-flood-db/region=east-africa/processing=annual/variable=flooded/
→ GFD FLOOD LIVE = yes
```
Each COG 3562×4542, EPSG:4326, overviews, values **0/1** (flooded union per year). Live: 2015 gdalinfo via /vsicurl = EPSG:4326 + overviews; range-GET 206 + CORS `*`; 2001 + 2018 both 206. **count-verify 15==15 first try.** Script ran clean end-to-end (no code fix needed — `/vsizip//vsicurl` byte-range extent-filter over the 913-event bucket worked as designed).

**ENSO signal is visible in the data:** big flooded-coverage years line up with El Niño — **2015** (Dec El Niño floods), **2012** (5 events, 253k px), **2006** — good for the ENSO composite. Note per-year `flooded px` varies a lot (2014 only 7k px / 4 events vs 2012 253k) = real event-footprint variation, not a bug.

**Both flood tiers now live:** JRC return-period (`source=jrc-glofas`, 7 RP) + GFD observed (`source=global-flood-db`, 15 yr). Ready for macbook to flag `type=flood` to Brayden + relay both base URLs to KE-ENSO (RP hazard slider + observed-event/ENSO layer). Left `Data/flood_gfd/GFD/` (15 COGs) on cglabs.

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

## [cglabs 2026-08-17 #2] RESPONSE — JRC FLOOD LIVE. 7/7 RP published + verified. 🟢

```
smoke gate = PASS (depth min 0.10 / mean 1.25 / max 41.27 m; EPSG:4326 + overviews, ≥0 clamp applied)
full ingest = 7/7 RP (10/20/50/75/100/200/500)
dry-run rows = 7/7   published = 7/7   S3 count == local = yes (7 == 7)
live 206 = yes   CORS = yes (*)
base URL = https://digital-atlas.s3.amazonaws.com/domain=climate/type=flood/source=jrc-glofas/region=east-africa/processing=return-period/variable=flood-depth/rp={RP}/
→ JRC FLOOD LIVE = yes
```
Each COG 9600×12240, EPSG:4326, 90 m, overviews (5 lvl), depth ≥0 (min 0.10). **Physically sane — mean depth monotonic ↑ with return period:** rp10 1.00 → rp20 1.08 → rp50 1.18 → rp75 1.22 → rp100 1.25 → rp200 1.32 → rp500 1.40 m; max 40.1 → 46.2 m (rarer = deeper). Live: RP100 gdalinfo via /vsicurl = EPSG:4326 + overviews; range-GET 206 + CORS `*`; rp10 + rp500 both 206. **count-verify passed first try (7==7, no silent drop this run).**

⚠️ **CODE FIX applied on cglabs (flagged for ratification):** `python/ingest_flood_jrc.py` — `os.makedirs(out_dir)` was at L65, **after** the L56 `gdal.Warp(tmp, …)` that writes the tmp *into* `out_dir` → first smoke failed `Attempt to create … .tmp_rp100.tif: No such file or directory`. Moved `makedirs` to the top of `build_rp()` (before the warp); removed the now-redundant later call. One-line move; re-ran smoke → PASS → full 7/7 clean. Please ratify.

**Done — ready for macbook** to flag `type=flood` to Brayden + relay the base URL to KE-ENSO (RP slider 10–500). GFD (`gfd_v1_4`, 913 events) = the bigger follow-up ingest, its own dispatch. Left `Data/flood_jrc/JRC/` (7 COGs, ~1.8 GB) on cglabs.

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
