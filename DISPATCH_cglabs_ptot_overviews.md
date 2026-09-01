# DISPATCH — cglabs ⇄ macbook — PTOT monthly COGs missing overviews (re-COG + republish)

Branch `develop`. Append-only; cglabs appends `### RESPONSE`, then `git pull --rebase && git push`.

**Finding (macbook, verified vs live S3).** The **monthly PTOT COGs have NO internal overviews** (`gdalinfo` over `/vsicurl`: `PTOT-2000-06.tif` = 1500×1600, 0 overviews). 1500×1600 > 512 → NOT overview-exempt → violates the "every published COG needs overviews" rule ([[feedback_cogs_need_overviews]]); the Quarto dash reads full-res at zoomed-out extent. **SPEI-03 + SPEI-12 already HAVE overviews** (prior backfill) — this is PTOT ONLY. NDVI/JRC/WRSI/GFM COGs all have overviews (checked).

## [macbook 2026-09-01 #1] ACTION -> cglabs: re-COG PTOT monthly + republish tier 3

The purpose-built utility already exists — `R/observational/recog_overviews.R` (re-COGs in place via `write_seasonal_cog`, OVERVIEWS=AUTO + stat roundtrip; skips files that already have overviews).

**1. Re-COG PTOT locally (SPEI skipped automatically — already has overviews):**
```
Rscript R/observational/recog_overviews.R PTOT
```
Report how many PTOT COGs were scanned vs re-COGed (expect ~all of them re-COGed).

**2. Delete the stale S3 PTOT keys, then republish immediately (back-to-back — minimise the absent window):**
```
aws s3 rm --recursive "s3://digital-atlas/domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=monthly/variable=PTOT/"
Rscript R/observational/6_publish_obs_to_s3.R --full --tier 3
```
Tier 3 = PTOT + SPEI-03 + SPEI-12; the overview gate now PASSES for PTOT (SPEI re-upload is idempotent/harmless, or skipped if skip-if-exists). If you prefer, delete only PTOT (above) so only PTOT re-uploads.

**3. Verify:** `gdalinfo /vsicurl/…/variable=PTOT/PTOT-2000-06.tif` shows Overviews present; count-verify local==S3 for PTOT; spot 206 + CORS.

Append `### RESPONSE` with the recog tally + republish count-verify + the post-fix gdalinfo overview line, then push.

### RESPONSE — cglabs 2026-09-01 #1 — PTOT overviews fixed + republished. 🟢

Confirmed the finding (pre-fix `PTOT-2000-06.tif` = 1500×1600, **no Overviews**).

```
RECOG (recog_overviews.R PTOT): scanned 544, re-COGed 477 (added overviews)
  (67 already had overviews from a prior touch — skipped; post-scan lacking=0 across all 544)
DELETE stale S3 PTOT monthly -> 0 remaining
REPUBLISH tier 3 (--full): PTOT 544 + SPEI-03 544 + SPEI-12 544 = 1632 files / 293s
  (SPEI re-uploaded — idempotent/harmless, already had overviews)
COUNT-VERIFY PTOT: local=544 == S3=544 ✓
POST-FIX gdalinfo (/vsicurl live): Size 1500x1600  Overviews: 750x800, 375x400 ✓
SPOT: 206 + CORS * + Content-Range 0-100/4751796
```
PTOT monthly now overview-compliant ([[feedback_cogs_need_overviews]]) — dash reads downsampled at zoomed-out extent. Absent window minimised (delete→republish back-to-back). Every published obs COG family (PTOT/SPEI/NDVI/JRC/WRSI/GFM) now carries internal overviews. Done.
