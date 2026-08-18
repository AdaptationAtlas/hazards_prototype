# WRSI ingest plan — FEWS NET Croplands + Rangelands (KE-ENSO)

_Scoping doc, 2026-08-18. Water Requirement Satisfaction Index for the KE-ENSO Explorer — a
crop/pasture water-satisfaction signal, ENSO-composable. Ingest the operational FEWS NET/USGS
product (NOT derive: climate-toolkit's WRSI is a point-based partial FAO-56 spec, unfinished for
gridded)._

## 1. Two products (domain-correct by land use)

Maize WRSI over rangeland is misleading (no maize, wrong calendar). FEWS NET solves this by making
**two separate gridded WRSI products** — publish both:

| product | for | USGS product | season windows (East Africa) |
|---|---|---|---|
| **CHIRPS Croplands WRSI** | cropland (maize/grain calendar) | 890/899/924/892/926 | MAM (long rains) + OND / Apr-Sep / Apr-Nov |
| **CHIRPS Rangelands WRSI** | **pastoral / ASAL** (pasture calendar) | 891/896 | long + short rains (Oct-Jan etc.) |

- Cropland → Croplands WRSI; **Rangeland → Rangelands WRSI** (purpose-built; NOT maize).
- **NDVI (already live)** stays as the observed rangeland forage cross-check.
- WRSI value classes: <50 failure · 50-60 poor · 60-80 mediocre · 80-95 average · 95-99 good ·
  99-100 very good (standard ramp).

## 2. Backbone — likely CHIRPS v3.0 (confirm)
FEWS NET transitioned WRSI off legacy RFE-GDAS onto **CHIRPS + NOAA ETos**; the USGS portal now
serves **CHIRPS 3.0** (prod/175). So the "CHIRPS - WRSI" products plausibly align with our v3.0
rainfall/SPEI backbone. **⚠️ Confirm exact CHIRPS version from product metadata** (v2.0 vs v3.0) in
the access probe — and ingest the **CHIRPS-ETos** version, NOT the legacy RFE-GDAS (USGS says the
two are not directly compatible).

## 3. Product shape
End-of-season WRSI per year → ENSO-composable (compose by ENSO/IOD phase with the notebook's
year-sets). Seasons MAM + OND. Record: rangeland operational since ~2005; L-WRSI 1982-present.

## 4. S3 layout (NEW `type=agriculture` — Brayden convention call)
```
domain=climate/type=agriculture/source=fews-wrsi/region=east-africa/
  processing=seasonal/variable=wrsi/crop={cropland|rangeland}/season={MAM|OND}/wrsi_{crop}_{SEASON}_{YYYY}.tif
```
COG conventions = the other tiers (EPSG:4326, tiled, OVERVIEWS=AUTO, CORS `*`, range) — the publish
overview-gate will enforce pyramids. New publish tier in `6_publish_obs_to_s3.R`.

## 5. Ingest mechanism (non-GEE)
Download the FEWS NET/USGS gridded WRSI rasters from the USGS Early Warning data portal
(earlywarning.usgs.gov / edcintl.cr.usgs.gov) → crop East-Africa/Kenya → COG w/ overviews. No auth
expected (public USGS). Access probe resolves the exact archive URL + format + resolution first.

## 6. Gates / open items (access probe #1)
1. **Download route:** the exact USGS raster archive URL pattern for Croplands + Rangelands WRSI
   (format GeoTIFF/BIL, region code for East Africa, per-season-per-year end-of-season file naming).
2. **CHIRPS version** (v2.0 vs v3.0) from product metadata — for the "consistent backbone" claim.
3. **Native res / CRS / extent / nodata / value range** (expect 0-100 + a nodata sentinel).
4. `type=agriculture` convention → Brayden.

## 7. Sequence
1. cglabs access probe (#1) — resolve URLs/format/res + CHIRPS version, download one raster, gdalinfo.
2. macbook writes the ingest (crop → COG) + publish tier; smoke-gate; full run; publish.
3. Relay to KE-ENSO + flag `type=agriculture` to Brayden.
