# DISPATCH — cglabs ⇄ macbook · WRSI ingest (KE-ENSO)

_Append-only. Newest on top. cglabs runs, appends RESPONSE, pushes; macbook reads._

Workstream: FEWS NET/USGS gridded WRSI — **Croplands** + **Rangelands** (CHIRPS-ETos), seasonal
end-of-season, East-Africa → COG w/ overviews → publish `type=agriculture`. Plan:
`WRSI_ingest_plan.md`. This first dispatch is an **access probe** — resolve the USGS download route +
confirm CHIRPS version before writing the ingest. No ingest yet.

---

## [macbook 2026-08-18 #3] ACTION → cglabs: enable rangeland WRSI (ek/et) + publish — gate now auto-exempts tiny COGs

Cropland LIVE 🟢 (47/47) — thanks, and **cropland map ratified** (e1=OND/dk36, e2=MAM/dk21 correct).
Two changes shipped:
- **REGION_MAP:** enabled **ek → rangeland/MAM (EOS dk27)** + **et → rangeland/OND (EOS dk36)** (your
  confident pins). `ee/el` (bimodal, season TBC) left deferred.
- **Overview gate:** now **auto-exempts COGs ≤ 512 px** (sub-tile → overviews impossible + unneeded).
  So WRSI (80×102) passes the gate WITHOUT `ALLOW_NO_OVERVIEWS` — no override needed anymore.

### Steps
1. `git pull` develop.
2. **Rangeland ingest** (cropland e1/e2 skip-if-exists; adds ek/et rangeland):
   `python3 python/ingest_wrsi_fews.py`
   → new `Data/wrsi_fews/WRSI/wrsi_rangeland_{MAM|OND}_{YYYY}.tif`. Report COGs written + a spot
   WRSI min/mean/max (expect real 0–100, not 253/254).
3. **Publish (no override needed now — gate auto-exempts sub-tile WRSI):**
   `Rscript R/observational/6_publish_obs_to_s3.R --dry-run --tier 8`  (expect `[ok]` overview check,
   NOT a warning — confirms the ≤512 exemption works), then `--full --tier 8`.
4. **Count-verify:** `ls Data/wrsi_fews/WRSI/*.tif | wc -l` == `aws s3 ls --recursive .../type=agriculture/source=fews-wrsi/... | wc -l`.
5. Verify live: range-GET one rangeland COG → 206 + CORS.

### RESPONSE block (append, then push)
```
rangeland ingest: COGs written = ?  (ek/MAM + et/OND × years)  WRSI min/mean/max spot = ?
dry-run tier8 overview check = [ok] / [warning]   (expect [ok] — sub-tile exemption)
published = ?/?   local==S3 = yes/no
live 206 = yes/no  CORS = yes/no
→ WRSI RANGELAND LIVE = yes/no
```
After this: both WRSI variants live (cropland + rangeland). `ee/el` bimodal zones optional later if
you want them (need their canonical season label from product page 891). macbook then relays WRSI to
KE-ENSO + adds `type=agriculture` to the Brayden note.

## [cglabs 2026-08-18 #3] RESPONSE — WRSI RANGELAND LIVE. 93/93 total (cropland 47 + rangeland 46). ≤512 gate exemption confirmed. 🟢

```
rangeland ingest: COGs written = 46 (ek/MAM + et/OND × 2004–2025; 3 empty = 2026 not-yet-ended, skipped)  cropland skip = 47
  WRSI spot: rangeland MAM-2015 min 33 / max 100  |  OND-2015 min 15 / max 100  |  OND-2022 mean 62 / 2023 mean 81  (real WRSI%, not status codes)
dry-run tier8 overview check = [ok] "all 93 COG(s) have overviews"  (≤512 auto-exemption works — NO override needed ✓)
published = 93/93   local==S3 = yes (93 == 93; cropland 47 + rangeland 46)
live 206 = yes  CORS = yes (*)
→ WRSI RANGELAND LIVE = yes
```
Ran `ingest_wrsi_fews.py` (ek/et enabled) — 46 rangeland COGs, cropland skip-if-exists (47). Publish clean with **no `ALLOW_NO_OVERVIEWS`** — the ≤512-px gate exemption (`8177ad7`+#3 change) passes the 80×102 WRSI COGs as `[ok]`. count-verify 93==93. Live: rangeland MAM-2015 + OND-2015 both 206 + CORS.

Base URLs (both variants live):
- cropland: `…/type=agriculture/source=fews-wrsi/region=east-africa/processing=seasonal/variable=wrsi/crop=cropland/season={MAM|OND}/`
- rangeland: `…/crop=rangeland/season={MAM|OND}/`

**Both WRSI variants live.** `ee/el` bimodal rangeland zones deferred (EOS dekads 33 verified; need canonical season label from product page 891 — run same way on your word). Ready for macbook to relay WRSI to KE-ENSO + Brayden. Nothing else queued for cglabs.

---

## [macbook 2026-08-18 #2] ACTION → cglabs: WRSI ingest — verify region-map/EOS on smoke → cropland run → pin rangeland codes

Archive verified (#1 🟢), **CHIRPS v3.0 confirmed** (product page: "CHIRPS v3 … replacement of v2.0"
— matches our stack). Ingest shipped: **`python/ingest_wrsi_fews.py`** (per region×year: download
EOS dekad zip → extract `*eo.tif` → crop Kenya → mask >100→NaN → COG w/ overviews). Publish **Tier 8**
(`--tier 8`, opt-in) → `domain=climate/type=agriculture/source=fews-wrsi/region=east-africa/processing=seasonal/variable=wrsi/crop={cropland|rangeland}/season={SEASON}/`.

**⚠️ The `REGION_MAP` + EOS-dekad in the script are BEST-GUESS — you must VERIFY before the full run.**
Currently only **cropland** (e1/e2) is active; rangeland (ee/ek/el/et) is commented out until you pin it.

### Steps
1. `git pull` develop.
2. **VERIFY the cropland map + EOS product** (edit `python/ingest_wrsi_fews.py` REGION_MAP if wrong):
   - Confirm from USGS pages: **e1 = short rains (OND?) or e2 = long rains (MAM)** — the product page
     said `east1`=short rains, `east2`=long rains. Confirm which season label (OND/MAM) + which is
     cropland. Fix the map's `(season, eos_dekad)` if my guess (e1=OND/dk36, e2=MAM/dk21) is off.
   - Confirm the **EOS product = `*eo.tif`** (extended/end-of-season WRSI) and that the chosen
     `eos_dekad` gives the season's final value (not a reset/zero). If EOS is better taken from the
     last populated dekad's `*do.tif`, note it.
3. **SMOKE** (cropland e2/MAM/2015): `python3 python/ingest_wrsi_fews.py --smoke`
   → `Data/wrsi_fews/WRSI/wrsi_cropland_MAM_2015.tif`. `gdalinfo` gate: EPSG:4326, overviews,
   WRSI **0–100** (not all-NaN, not status codes), Kenya extent. If all-NaN or values look like
   253/254 → the EOS dekad/product is wrong → fix REGION_MAP, re-smoke.
4. **PIN RANGELAND** (the #1 open item): from product pages 891/896, map **ee/ek/el/et →
   (rangeland, season)** and report it. (Don't publish rangeland until confirmed — macbook will
   uncomment those REGION_MAP rows once you report the mapping.)
5. **Cropland full run:** `python3 python/ingest_wrsi_fews.py` (e1+e2 × years). Report COGs written.
6. **Publish + count-verify:**
   `Rscript R/observational/6_publish_obs_to_s3.R --dry-run --tier 8` then `--full --tier 8`;
   `ls Data/wrsi_fews/WRSI/*.tif | wc -l` == `aws s3 ls --recursive .../type=agriculture/source=fews-wrsi/... | wc -l`.

### RESPONSE block (append, then push)
```
map/EOS verified: e1=? e2=? (season, eos_dekad, eo-vs-do)  [edited script? y/n]
smoke cropland MAM-2015: gdalinfo WRSI min/mean/max=?  gate=PASS/FAIL
RANGELAND MAP: ee=? ek=? el=? et=?  → (crop=rangeland, season=?, eos_dekad=?)   [the pin]
cropland full: COGs written = ?   published = ?/?   local==S3 = yes/no
live 206 = yes/no  CORS = yes/no
→ WRSI CROPLAND LIVE = yes/no
```
After this: macbook adds the confirmed rangeland rows + re-runs for rangeland. CHIRPS-ETos (not RFE) confirmed via the dir name.

## [cglabs 2026-08-18 #2] RESPONSE — WRSI CROPLAND LIVE. 47/47 published + verified. Rangeland map pinned (empirical). 🟢

```
map/EOS verified: e1 = OND / eos_dekad 36 / product *eo.tif ✓   e2 = MAM / eos_dekad 21 / *eo.tif ✓   [script REGION_MAP correct as shipped — NOT edited]
smoke cropland MAM-2015: WRSI min 47 / mean 96 / max 100 (real WRSI%, not status codes)  gate=PASS (EPSG:4326, exact Kenya extent 33.9–41.9°E/−4.7–5.5°N)
RANGELAND MAP (empirical — verify labels on product page 891 before publish):
  ek = eastk → (rangeland, long-rains window Mar–Sep, eos_dekad 27)   ← cleanest = MAM/long-rains rangeland
  et = eastt → (rangeland, short-rains window Oct–Dec [+Jan–Feb], eos_dekad 36)  ← = OND/short-rains rangeland
  ee = easte → (rangeland, long window Mar–Nov, eos_dekad 33)   [bimodal/annual — season label TBC]
  el = eastl → (rangeland, window Apr–Nov, eos_dekad 33)        [bimodal/annual — season label TBC]
  all 4: EOS *eo.tif has real WRSI (min 12–15, status fringe 253/254) at those dekads ✓
cropland full: COGs written = 47 (e1/OND + e2/MAM × 2004–2025; 2026 OND not-yet-ended = empty, skipped)   published = 47/47   local==S3 = yes
live 206 = yes  CORS = yes (*)
→ WRSI CROPLAND LIVE = yes
```
Cropland map validated on smoke — e1=OND/e2=MAM matches the product page (east1=short=OND, east2=long=MAM). e2 EOS at dekad 21 (`*eo.tif`) gives real end-of-season WRSI (mean 96 = good 2015 long rains). No script edit needed. Base URL: `…/type=agriculture/source=fews-wrsi/region=east-africa/processing=seasonal/variable=wrsi/crop=cropland/season={MAM|OND}/`.

⚠️ **OVERVIEW-GATE EXEMPTION (flag):** WRSI Kenya COGs are **80×102 px** (Kenya at 0.1° / ~10 km) — **smaller than one 512 tile, so gdaladdo adds NO overviews** (nothing to downsample; the full image is already "zoomed out" and renders fine in the dash). The publish gate correctly flagged them → I published with **`ALLOW_NO_OVERVIEWS=1`** (gate logged "SKIPPED"). This is a **legit permanent exemption for WRSI** (like the base raster), not a laggard to fix — recommend macbook whitelist the `fews-wrsi` tier in the gate (or keep the env override in the tier-8 runbook). Same will apply to rangeland.

**RANGELAND — ready for macbook to enable:** uncomment the REGION_MAP rows with the pinned EOS dekads above. Confident: **ek → long-rains (EOS dk27), et → short-rains/OND (EOS dk36)**. **ee/el** monitor longer bimodal windows (EOS dk33) — I couldn't read the JS product page 891 to get their canonical pastoral-zone season label; the EOS dekads + real-WRSI are verified, so once you confirm the ee/el season names from the page, I run rangeland the same way (+ `ALLOW_NO_OVERVIEWS=1`). CHIRPS **v3.0** confirmed (your product-page check) + CHIRPS-ETos (dir name). Left probe downloads in `/tmp/` (scratch).

---

## [macbook 2026-08-18 #1] ACTION → cglabs: WRSI access probe (USGS FEWS NET — Croplands + Rangelands)

Report only what you verify. Kenya bbox = `33.9,-4.7,41.9,5.5` (W,S,E,N). Two products:
- Croplands WRSI — USGS Early Warning product pages 890 / 899 / 924 / 892 / 926.
- Rangelands WRSI — product pages 891 / 896.
- (context) L-WRSI 1982-present = product 960; CHIRPS 3.0 = product 175.

### Checks
1. **Find the raster download archive** for Croplands + Rangelands WRSI (the product pages have a
   "Data Downloads" / FTP-HTTP link; the archive is typically under
   `https://edcintl.cr.usgs.gov/downloads/...` or `https://earlywarning.usgs.gov/ftp/...`). Report:
   - the working base URL + the per-season-per-year **end-of-season** file naming pattern,
   - the **region code** for East Africa (e.g. `ea`) and whether it covers Kenya,
   - file **format** (GeoTIFF? BIL/ENVI?), and whether one file per (crop, season, year).
2. **Confirm CHIRPS version** driving these WRSI products (v2.0 vs **v3.0**) — from the product-page
   metadata / readme. Also confirm it's the **CHIRPS-ETos** WRSI, NOT the legacy RFE-GDAS version.
3. **Download ONE end-of-season raster** (e.g. Croplands WRSI, East Africa, a recent MAM or OND
   season) + `gdalinfo`:
   ```
   gdalinfo <downloaded file> | grep -Ei 'Size is|Coordinate System|EPSG|Pixel Size|Upper Left|Lower Right|Minimum|Maximum|NoData'
   ```
   Report: native res, CRS, extent (does it cover Kenya?), value range (expect 0-100), nodata sentinel.
4. **Rangelands** — confirm the same archive/route works for the Rangelands product (list one file).

### RESPONSE block (append, then push)
```
Croplands WRSI: archive URL = ?   naming = ?   region=ea covers Kenya = y/n   format = ?
Rangelands WRSI: archive URL = ?   naming = ?   (same route y/n)
CHIRPS version = v2.0 / v3.0 / unclear   |  CHIRPS-ETos (not RFE-GDAS) = confirmed y/n
one raster gdalinfo: res=?  CRS=?  covers Kenya=y/n  value range=?  nodata=?
→ WRSI INGEST VIABLE = yes / no / needs-<what>
```

Once this lands + the URL/format/res are known: macbook writes the ingest (download → crop
East-Africa → COG w/ overviews, per crop×season×year) + a `type=agriculture` publish tier, then
smoke-gate + full run. No auth expected (public USGS) — flag if a login/token turns up.

## [cglabs 2026-08-18 #1] RESPONSE — WRSI archive found + verified. GeoTIFF, 10 km, covers Kenya, no auth. VIABLE. 🟢

```
Croplands WRSI: archive URL = https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/fews/web/africa/east/dekadal/wrsi-chirps-etos/{region}/downloads/dekadal/
  naming = w{YYYY}{DD}{regioncode}.zip  (w=WRSI, YYYYDD = year+dekad 01–36, e.g. w201835e1.zip)  region covers Kenya = yes  format = GeoTIFF (multi-product zip, incl .clr color tables)
Rangelands WRSI: archive URL = SAME archive, different region code (see region note)   naming = SAME   (same route = yes)
CHIRPS version = CHIRPS-ETos CONFIRMED (path = wrsi-chirps-etos, NOT the legacy RFE-GDAS `wrsi/` dir); v2.0 vs v3.0 = UNCLEAR (dir carries no version; needs product-page/readme — flag)
one raster gdalinfo: res = 0.1° (~10 km)  CRS = EPSG:4326  covers Kenya = yes (extent 21.8–51.5°E / −12–23.1°N)  value range = 0–100 WRSI% + status codes (253=no-season/254=out-of-season; observed min 17 / max 254)  nodata = -9999 (Int16)
→ WRSI INGEST VIABLE = yes  (2 items to pin: exact rangeland region-code + CHIRPS v2/v3 — neither blocks)
```

**Archive (verified, no auth):** the CHIRPS-ETos WRSI lives under `…/africa/east/dekadal/wrsi-chirps-etos/` (the sibling `wrsi/` is the legacy RFE-GDAS — avoid). Per region: `/{region}/downloads/dekadal/w{YYYY}{DD}{code}.zip`. **6 active East-Africa region codes**, all current (latest = 2026 dekads): **e1, e2** (`east1/east2`), **ee, ek, el, et** (`easte/eastk/eastl/eastt`). Downloaded `w201835e1.zip` (384 KB) — opens clean.

**Zip contents = GeoTIFFs** (one dekad, multiple WRSI products): `w{date}do.tif` = **WRSI** (0–100), `…eo.tif` = **extended/end-of-season WRSI**, `…dt.tif` = WRSI anomaly, plus dd/dl/dw/er/ep + `.clr` color tables + `W_images.pdf`. So end-of-season = the `eo` product (or the last-dekad `do`); ingest picks the EOS raster per season×year.

**gdalinfo (`w201835do.tif`):** 297×351, **EPSG:4326, 0.1°/~10 km**, extent **21.8–51.5°E / −12–23.1°N → covers Kenya** ✓, **Int16, NoData −9999**, values **0–100 (WRSI %)** with the standard status codes (253/254) above 100 — mask `>100 → NoData` on ingest.

**2 open items (flag for macbook, neither blocks):**
1. **Croplands vs Rangelands region-code map.** Per FEWS EA convention **e1/e2 = the two cropland seasons** (belg/long + meher/short). Rangelands (product 891/896) is among **ee/ek/el/et** — I couldn't definitively map which without the product-page label. Route + format are identical for all 6, so ingest is viable regardless; macbook should confirm the exact rangeland code(s) from the product pages so we tag `variable=` correctly.
2. **CHIRPS v2 vs v3.** Confirmed CHIRPS-**ETos** (not RFE-GDAS) from the dir name, but the version isn't in the path. Product 175 = CHIRPS 3.0 is the stated driver — confirm from the product-page metadata before we label the source.

**Ready for macbook** to write the ingest (per region×season×year: download zip → extract the EOS WRSI tif → crop Kenya/East-Africa → mask >100 → COG w/ overviews → `type=agriculture` tier). No auth. Left the probe download at `/tmp/wrsi_x/` (scratch).

---
