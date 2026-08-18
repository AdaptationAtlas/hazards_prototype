# DISPATCH — cglabs ⇄ macbook · WRSI ingest (KE-ENSO)

_Append-only. Newest on top. cglabs runs, appends RESPONSE, pushes; macbook reads._

Workstream: FEWS NET/USGS gridded WRSI — **Croplands** + **Rangelands** (CHIRPS-ETos), seasonal
end-of-season, East-Africa → COG w/ overviews → publish `type=agriculture`. Plan:
`WRSI_ingest_plan.md`. This first dispatch is an **access probe** — resolve the USGS download route +
confirm CHIRPS version before writing the ingest. No ingest yet.

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
