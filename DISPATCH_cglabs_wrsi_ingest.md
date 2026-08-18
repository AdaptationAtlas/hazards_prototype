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

---
