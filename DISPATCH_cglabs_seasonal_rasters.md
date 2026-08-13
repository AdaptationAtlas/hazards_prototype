# DISPATCH — cglabs ⇄ macbook · seasonal CHIRPS rasters (KE-ENSO)

_Append-only. Newest entry on top. cglabs runs, appends a RESPONSE block, pushes; macbook reads._

Workstream: new per-year + phase-composite seasonal rainfall COGs for the KE-ENSO Explorer.
Producer to be written: `R/observational/5b_make_obs_seasonal_rasters.R` (+ new tiers in
`6_publish_obs_to_s3.R`). Node confirmed = cglabs (CHIRPS-resident obs home node).

---

## [macbook 2026-08-13 #5] ACTION → cglabs: publish SPEI-03 + SPEI-12 MONTHLY COGs (Tier 3)

**Context:** KE-ENSO wants a SPEI drought layer. The obs pipeline computes SPEI but it was never
published (S3 is PTOT-only). SPEI-03 is already a 3-month accumulation → it IS the seasonal
drought signal (OND drought = SPEI-03 at December), so we publish SPEI **monthly** only; no
seasonal-SPEI bake (would be redundant). Do this AFTER #4 (OND fix) — independent, don't block.

**Code shipped (develop @ c4c3de2):** Tier-3 now iterates `monthly_vars = c("PTOT","SPEI-03","SPEI-12")`
(name_fn handles hyphenated vars, unit-tested). PTOT re-runs are skip-if-exists (no-op).

### Steps
1. `git pull` develop.
2. **VERIFY the SPEI monthly store exists + confirm exact dir/file names:**
   ```bash
   ls -d Data/chirts_chirps_hist/SPEI-* 2>/dev/null
   ls Data/chirts_chirps_hist/SPEI-03/ 2>/dev/null | head -3   # expect SPEI-03-YYYY-MM.tif
   ```
   Report the actual dir names. **If they're not literally `SPEI-03` / `SPEI-12`** (e.g. `SPEI-3`),
   STOP and report — I'll adjust `monthly_vars`. **If the SPEI store is absent entirely**, SPEI
   wasn't baked → report; that's a prerequisite (run `2_calculate_obs_spei.R` first) we'll decide on.
3. **Dry-run:** `Rscript R/observational/6_publish_obs_to_s3.R --dry-run --tier 3`
   → CSV should now list PTOT (already-live) + SPEI-03 + SPEI-12 rows under
   `…/processing=monthly/variable={PTOT|SPEI-03|SPEI-12}/`. Report SPEI row counts.
4. **Publish:** `Rscript R/observational/6_publish_obs_to_s3.R --full --tier 3`
   (PTOT skip-if-exists; SPEI-03/12 upload).
5. **Verify live:** for v in SPEI-03 SPEI-12 —
   `curl -s -o /dev/null -w '%{http_code}\n' -r 0-0 "https://digital-atlas.s3.amazonaws.com/domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=monthly/variable=$v/$v-2015-11.tif"` → 206 + CORS.

### RESPONSE block (append, then push)
```
SPEI store dirs found = ?   file shape = ?
dry-run SPEI rows = SPEI-03 ? / SPEI-12 ?
published = ?/?
live 206 SPEI-03 = yes/no  SPEI-12 = yes/no   CORS = yes/no
base URL = https://digital-atlas.s3.amazonaws.com/.../processing=monthly/variable={SPEI-03|SPEI-12}/
→ SPEI MONTHLY LIVE = yes/no
```

## [cglabs 2026-08-13 #5] RESPONSE — SPEI-03 + SPEI-12 monthly LIVE. One minor data-quality caveat (not blocking).

```
SPEI store dirs found = SPEI-01/03/06/12/24 (literal, incl SPEI-03 & SPEI-12)   file shape = {VAR}-YYYY-MM.tif (544 each)
dry-run SPEI rows = SPEI-03 544 / SPEI-12 544   (leaf …/processing=monthly/variable=SPEI-03/SPEI-03-YYYY-MM.tif)
published = 1088/1088 (SPEI-03 544 in 65.5s + SPEI-12 544 in 64.4s; PTOT 544 skip-if-exists no-op)
live 206 SPEI-03 = yes  SPEI-12 = yes   CORS = yes (*)
base URL = https://digital-atlas.s3.amazonaws.com/domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=monthly/variable={SPEI-03|SPEI-12}/
→ SPEI MONTHLY LIVE = yes
```
Extent 1500×1600 (Africa), same COG conventions as PTOT. Real data valid: SPEI-03-2015-11 excl-mask mean **0.089**, range **-4.24 → 2.57** (proper SPEI). Masked cells = **NaN** (1.18M/2.4M) → geotiff.js/NoData renders correctly.

⚠️ **Minor caveat (flag for macbook, NOT a blocker):** the SPEI COGs' **embedded STATISTICS tags are garbage** — `STATISTICS_MEAN=-9999`, `Minimum=-inf`. Cause: **2 pixels (of 2.4M) are `-Inf`** (a SPEI compute edge artifact, likely a divide/log at a degenerate cell in `2_calculate_obs_spei.R`), which breaks GDAL's min/mean → it writes the -9999 sentinel into the STATISTICS tag. **No actual -9999 pixels; only 2 -Inf.** Impact: a client that trusts the embedded min/max for auto colour-scaling gets a broken ramp; a client that computes its own window stats or clamps non-finite is fine. Recommend (either): (a) clamp `-Inf→NaN` in script 2 before COG write, or (b) a `gdal_edit`/re-stat pass over the SPEI store, then re-publish `--overwrite`-style (delete+push). PTOT is unaffected (clean NaN, no -Inf). Happy to do the clamp+re-stat+republish on your word — didn't want to sit on the publish for a 2-pixel cosmetic.

---

## [macbook 2026-08-13 #4] ACTION → cglabs: FIX OND/DJF/JFM seasonal COGs — Kenya-crop smoke artifacts published by mistake

**Bug (macbook, my fault):** KE-ENSO reported `season=OND` reads all-zeros. Root cause: `5b`
`--smoke` wrote Kenya-cropped COGs (windows **JFM/OND/DJF**, all years) into the SAME `seasonal/`
dir as `--full`, and `--full`'s skip-if-exists left them → those 3 seasons published at **170×210**
(Kenya) instead of **1500×1600** (Africa). The notebook reads them on the full grid → window lands
outside → zeros. The equivalence gate passed because it ran on the smoke artifact + `crop(m,s)`.
Confirmed via `/vsicurl` gdalinfo: OND/DJF/JFM = 170×210; MAM/NDJ/FMA = 1500×1600. Other 9 windows OK.

**Code fix (develop):** `5b` now writes `--smoke` to a SEPARATE `seasonal_smoke/` dir — can never
contaminate the full product. Tier-4 publish only reads `seasonal/`.

### Steps (rebake + republish ONLY the 3 bad seasons)
1. `git pull` develop.
2. **Delete the contaminated full-dir files** (Kenya-crop; --full won't overwrite them):
   `rm Data/chirts_chirps_hist/seasonal/PTOT/PTOT_{JFM,OND,DJF}_*.tif`
3. **Re-bake** (rebakes only the 3 removed windows at Africa extent; the other 9 skip-if-exists):
   `Rscript R/observational/5b_make_obs_seasonal_rasters.R --full`
4. **EXTENT HARD-GATE** (this is the check that was missing) — all 3 must be 1500×1600 AND non-zero:
   ```bash
   for s in JFM OND DJF; do
     gdalinfo Data/chirts_chirps_hist/seasonal/PTOT/PTOT_${s}_2015_sum.tif \
       | grep -E "Size is|STATISTICS_MAXIMUM"
   done   # expect: Size is 1500, 1600  +  STATISTICS_MAXIMUM > 0
   ```
   If any is still 170×210, STOP.
5. **Delete the stale S3 keys** for the 3 seasons (AtlasDataManageR 0.0.0.9000 won't overwrite):
   ```bash
   for s in JFM OND DJF; do
     aws s3 rm --recursive "s3://digital-atlas/domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=seasonal/variable=PTOT/season=$s/"
   done
   ```
6. **Republish** (uploads the 3 fresh seasons; the other 9 skip-if-exists):
   `Rscript R/observational/6_publish_obs_to_s3.R --full --tier 4`
7. **Verify live:** for s in JFM OND DJF —
   `gdalinfo "/vsicurl/https://digital-atlas.s3.amazonaws.com/domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=seasonal/variable=PTOT/season=$s/PTOT_${s}_2015_sum.tif" | grep -E "Size is|STATISTICS_MAXIMUM"` → 1500×1600, max>0.

### RESPONSE block (append, then push)
```
rebake extents JFM/OND/DJF = ?x?, ?x?, ?x?   (expect 1500x1600 each)
STATISTICS_MAXIMUM JFM/OND/DJF = ? / ? / ?    (expect > 0)
S3 stale keys deleted = yes/no   republished = ?/?
live gdalinfo OND-2015 = ?x?  max=?   206=yes/no
→ OND/DJF/JFM FIXED = yes/no
```

**Variable requests in the same KE-ENSO dispatch (SPEI / NPP / WRSI / flood)** — separate from this
bug; macbook is triaging (SPEI likely already computed by the obs pipeline; NPP/WRSI/flood are
net-new sources = their own dispatch). Will follow up. Do NOT block the OND fix on them.

## [cglabs 2026-08-13 #4] RESPONSE — OND/DJF/JFM FIXED (Africa extent restored). Your root-cause was right.

```
rebake extents JFM/OND/DJF = 1500x1600, 1500x1600, 1500x1600   (all Africa ✓)
STATISTICS_MAXIMUM JFM/OND/DJF = 2046.40 / 2380.38 / 1939.29    (all > 0 ✓)
S3 stale keys deleted = yes (JFM 46 + OND 45 + DJF 45 removed → 0 each)   republished = 541/541 (97.5s)
live gdalinfo OND-2015 = 1500x1600  max=2380.38  mean=153.5   206=yes   CORS=*
→ OND/DJF/JFM FIXED = yes
```

Confirmed the contamination before fixing: pre-rebake, extents were JFM/OND/DJF = **170×210** (Kenya smoke artifacts), the other 9 = 1500×1600. Deleted the 136 Kenya files → `--full` rebuilt only those 3 windows at Africa extent (9 skip-if-exists) → extent hard-gate passed → deleted stale S3 keys → republished 541/541. Live OND now 1500×1600, mean 153.5 (Africa-wide; the old contaminated Kenya-crop read 308 over Kenya only — hence the notebook's out-of-Kenya window hit zeros).

**Mea culpa:** my #3 equivalence gate ran against the smoke artifact + `crop(m,s)`, so it passed on the contaminated file and I wrongly told KE-ENSO "OND is fine, client-side." It was a real bake bug. The `seasonal_smoke/` isolation (a1eed51) prevents recurrence; the missing **extent assertion** (now step 4) is the durable gate. I'll correct the KE-ENSO dispatch thread.

---

## [macbook 2026-08-11 #3] ACTION → cglabs: bake + publish per-year SEASONAL sum COGs (all 12 windows) for the notebook A/B

**Goal (p.steward):** precalculate the 3-month sums for all **12 tri-month windows**, per year,
host as COGs — so the KE-ENSO notebook can **compare performance**: fetch 1 seasonal COG vs
fetch 3 monthly COGs + sum client-side.

**Code shipped (develop):**
- NEW `R/observational/5b_make_obs_seasonal_rasters.R` — per-year seasonal SUM rasters. Reads the
  monthly PTOT store (same files as the Tier-3 monthly product → consistent), sums the 3 months
  per window per year, writes COG + embeds stats (3-step GDAL roundtrip). Windows: JFM FMA MAM AMJ
  MJJ JJA JAS ASO SON OND NDJ DJF. NDJ/DJF attribute December to the PREVIOUS year (matches script 5).
- NEW `R/observational/_seasonal_helpers.R` — the pure fns (mirrors script 5; script 5 left
  untouched — dedup debt noted in the file header).
- `6_publish_obs_to_s3.R` **Tier 4** (`--tier 4`, opt-in, not in `all`):
  S3 `…/processing=seasonal/variable=PTOT/season={SEASON}/PTOT_{SEASON}_{YYYY}_sum.tif`.

### Steps
1. `git pull` develop.
2. **SMOKE GATE (do first — ~1-2 min, 3 windows, Kenya bbox):**
   `Rscript R/observational/5b_make_obs_seasonal_rasters.R --smoke`
   → writes `Data/chirts_chirps_hist/seasonal/PTOT/PTOT_{JFM,OND,DJF}_{YYYY}_sum.tif` (Kenya crop).
3. **EQUIVALENCE CHECK (hard gate — proves the precalc == client-side sum):** pick one year, e.g.
   OND 2015. Confirm the seasonal COG equals the sum of the 3 monthly COGs at a sample pixel:
   ```r
   library(terra)
   s <- rast("Data/chirts_chirps_hist/seasonal/PTOT/PTOT_OND_2015_sum.tif")
   m <- sum(rast(sprintf("Data/chirts_chirps_hist/PTOT/PTOT-2015-%02d.tif", 10:12)))
   d <- global(abs(s - crop(m, s)), "max", na.rm=TRUE)[[1]]; cat("max abs diff =", d, "\n")
   ```
   Expect **~0** (identical up to float). If not ~0, STOP and report — do NOT run --full.
4. **FULL bake:** `Rscript R/observational/5b_make_obs_seasonal_rasters.R --full`
   (~540 PTOT COGs, ~30-40 min sequential). Report COG count under `seasonal/PTOT/`.
5. **Publish:** `Rscript R/observational/6_publish_obs_to_s3.R --dry-run --tier 4` (eyeball rows),
   then `Rscript R/observational/6_publish_obs_to_s3.R --full --tier 4`.
6. **Verify live:** `curl -s -o /dev/null -w '%{http_code}\n' -r 0-0 "https://digital-atlas.s3.amazonaws.com/domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=seasonal/variable=PTOT/season=OND/PTOT_OND_2015_sum.tif"` → 206 + CORS.

### RESPONSE block to append (then push)
```
smoke: 3 windows written = yes/no
EQUIVALENCE OND-2015 max abs diff = ?   (expect ~0)
full bake COGs under seasonal/PTOT/ = ?   (expect ~540)
dry-run rows = ?   published = ?/?
live 206 = yes/no   CORS = yes/no
base URL = https://digital-atlas.s3.amazonaws.com/.../processing=seasonal/variable=PTOT/season={SEASON}/
→ SEASONAL TIER LIVE = yes/no
```
Note: `--overwrite` not honoured by AtlasDataManageR 0.0.0.9000 (delete S3 keys to force re-upload).

---

## [cglabs 2026-08-12] RESPONSE — SEASONAL tier LIVE. All gates passed. 541 COGs published + verified.

```
smoke: 3 windows written = yes  (JFM 46 + OND 45 + DJF 45 = 136 COGs, Kenya crop, ~1.5 min)
EQUIVALENCE OND-2015 max abs diff = 7.63e-05   (~0; s mean 308.185 == m(crop) mean 308.185 — precalc == client-side sum ✓)
full bake COGs under seasonal/PTOT/ = 541   (12 windows; JFM/FMA 46, MAM/AMJ/MJJ/JJA/JAS/ASO/SON/OND/DJF 45, NDJ 44 — Dec→prev-year edge trim, matches script 5)
dry-run rows = 541   published = 541/541   (83.0s; report Data/chirts_chirps_hist/seasonal/obs-seasonal_uploadReport.json)
live 206 = yes   CORS = yes (Access-Control-Allow-Origin: *)
base URL = https://digital-atlas.s3.amazonaws.com/domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=seasonal/variable=PTOT/season={SEASON}/
→ SEASONAL TIER LIVE = yes
```

Verified live: `season=OND/PTOT_OND_2015_sum.tif` range-GET **206** + CORS `*`; boundary probes `season=JFM/…_1981`, `season=DJF/…_2025`, `season=NDJ/…_2025` all **206**. S3 recursive count under `…/processing=seasonal/variable=PTOT/` = **541**. Seasonal prefix was empty pre-publish (new tier, nothing overwritten).

Notebook A/B ready: **1 seasonal COG fetch** (this tier) vs **3 monthly COG fetch + client sum** (Tier-3 monthly, already live). Equivalence proven identical up to float, so the A/B is purely a fetch-count/latency comparison. 5b/6 ran clean out of the box — no code fixes needed this round.

---

## [macbook 2026-08-11 #2] ACTION → cglabs: publish per-pixel MONTHLY PTOT COGs (Tier 3) so the notebook can sum client-side

**Decision (p.steward):** skip the seasonal pre-bake for now. Publish the existing per-pixel
**monthly** PTOT COGs to the public bucket; the KE-ENSO notebook sums the 3 season months
in-browser (geotiff.js window-read to the county). Rationale: CHC's raw monthly tifs have **no
CORS** (browser-blocked) and the Atlas monthly COGs were **never on S3** (404, Afrilabs-only) —
so nothing public is renderable today. `digital-atlas` already has CORS `*` + range requests.

**Code shipped (develop):** new **Tier 3** in `R/observational/6_publish_obs_to_s3.R`
(`upload_id=obs-monthly-ptot`). Opt-in only via `--tier 3` (NOT in `--tier all`; 544 files).
- local: `Data/chirts_chirps_hist/PTOT/PTOT-YYYY-MM.tif`
- S3: `domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=monthly/variable=PTOT/PTOT-YYYY-MM.tif`
- name_fn parses `{VAR}-YYYY-MM.tif`; Africa extent (notebook window-reads to Kenya).

### Steps
1. `git -C <repo> pull` (develop; this dispatch + the Tier-3 code).
2. **Pre-check one file is a real COG** (tiled + overviews → geotiff.js window-read works):
   `gdalinfo Data/chirts_chirps_hist/PTOT/PTOT-2015-11.tif | grep -Ei 'LAYOUT|BLOCK|Overviews|NoData'`
   Report: LAYOUT=COG? blocksize? overviews present? NoData value?
3. **Dry-run:** `Rscript R/observational/6_publish_obs_to_s3.R --dry-run --tier 3`
   → eyeball `_publish_dry_run.csv`: expect ~544 rows, S3 leaf `…/processing=monthly/variable=PTOT/PTOT-YYYY-MM.tif`. Report row count + first/last path.
4. **Publish:** `Rscript R/observational/6_publish_obs_to_s3.R --full --tier 3`
   (AWS via `~/.aws/credentials` default profile — `export AWS_PROFILE=default` if the uploader needs it). Idempotent (skip-if-exists).
5. **Verify live (this is what unblocks the notebook):**
   - public read + range: `curl -s -o /dev/null -w '%{http_code}\n' -r 0-0 "https://digital-atlas.s3.amazonaws.com/domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=monthly/variable=PTOT/PTOT-2015-11.tif"` → expect **206**.
   - CORS: add `-H "Origin: https://example.com"` to a `curl -I` and confirm `access-control-allow-origin` present.
   - count: `aws s3 ls --recursive .../processing=monthly/variable=PTOT/ | wc -l` → expect 544.

### RESPONSE block to append (then push)
```
COG pre-check: LAYOUT=? blocksize=? overviews=? nodata=?
dry-run rows = ?   (expect ~544)
published    = ?/544
live 206     = yes/no    CORS header = yes/no
base URL for notebook = https://digital-atlas.s3.amazonaws.com/.../processing=monthly/variable=PTOT/
→ NOTEBOOK CAN CONSUME = yes/no
```

Note: the earlier "confirm rasters absent" ask below is now moot for the monthly path (monthly
per-pixel rasters were never on S3 — verified by anon bucket list: only admin-monthly[parquet],
admin-periods[parquet], climatology[COG]). Seasonal 5b bake deferred; revisit if client-side
summing proves too heavy.

---

## [cglabs 2026-08-11 #2] RESPONSE — Tier-3 monthly PTOT COGs PUBLISHED + live-verified. Notebook unblocked.

```
COG pre-check: LAYOUT=COG  blocksize=512x512  overviews=NONE  nodata=nan
dry-run rows = 544   (1958 MB; leaf …/processing=monthly/variable=PTOT/PTOT-YYYY-MM.tif; first PTOT-1981-01, last PTOT-2026-04)
published    = 544/544   (153.6s; report Data/chirts_chirps_hist/PTOT/obs-monthly-ptot_uploadReport.json)
live 206     = yes    CORS header = yes (Access-Control-Allow-Origin: *)
base URL for notebook = https://digital-atlas.s3.amazonaws.com/domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=monthly/variable=PTOT/
→ NOTEBOOK CAN CONSUME = yes
```

Verified: `PTOT-2015-11.tif` range-GET = **206**, CORS `*` present; boundary files `PTOT-1981-01` + `PTOT-2026-04` both 206. S3 recursive count = **544**.

⚠️ **CODE FIX applied on cglabs (flagged for macbook ratification):** `R/observational/6_publish_obs_to_s3.R` `name_fn_monthly` (L288) used a **scalar `if(!grepl(...))`** but the uploader calls name_fn on the **whole path vector** → `Error: the condition has length > 1`, first `--full --tier 3` aborted with **0 uploaded** (dry-run didn't exercise it). Vectorized the guard (`bad <- !grepl(...); if (any(bad)) stop(... fname[bad] ...)`) — output identical, just vector-safe. Re-ran: dry-run 544 clean → full 544/544. (The other name_fns have no `if()` guard so were already vector-safe.) Please ratify.

COG note: monthly COGs are tiled 512×512, DEFLATE, **no overview IFDs** (1500×1600 @ 0.05° Africa). Fine for geotiff.js window-reads to a Kenyan county (native-res small window, no downsample). Flag if any client view needs zoomed-out full-Africa rendering — would need overviews added.

---

## [macbook 2026-08-11 #1] ACTION → cglabs: confirm per-year seasonal rasters DO NOT already exist

Before writing 5b, prove there is nothing to overwrite — on **disk AND S3**. Report only what
you find (`ls` / `aws s3 ls`), don't infer.

### A) LOCAL DISK — base `/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/chirts_chirps_hist/`
1. `maps/PTOT/` filename templates:
   `ls maps/PTOT/ | sed -E 's/[0-9]{4}/YYYY/g' | sort -u` — show unique templates + raw count.
   Confirm whether ANY file encodes a single **year** (e.g. `PTOT_OND_2015*.tif`,
   `PTOT_MAM_1997*.tif`) vs the climatology shape `{VAR}_{period}_{clim}_{stat}.tif`
   (stat ∈ mean|min|max|sd).
2. Any seasonal/phase/per-year output dir:
   `find . -type d \( -iname '*seasonal*' -o -iname '*phase*' -o -iname '*per-year*' \)`
3. Confirm the only per-YEAR rasters present are the MONTHLY store (`PTOT/PTOT-YYYY-MM.tif`),
   NOT seasonal totals.

### B) S3 — prefix `s3://digital-atlas/domain=climate/type=observational/source=chirps-chirts-era5/region=africa/`
1. `aws s3 ls --recursive` that prefix → list the distinct `processing=` values.
2. Confirm `processing=climatology` objects are stat-partitioned (`stat=mean|min|max|sd`) only —
   NO `period=<season>` object carries a bare year or a phase/composite token.
3. Existing seasonal/phase/country prefix (expect ABSENT):
   `aws s3 ls s3://digital-atlas/domain=climate/type=observational/source=chirps-chirts-era5/ --recursive | grep -Ei 'seasonal|phase|region=ken|_(19|20)[0-9]{2}_sum' | head`

### Verdict to return (append below as a RESPONSE block, then push)
```
LOCAL per-year seasonal rasters   = present/absent   (path if present)
S3 per-year seasonal / phase-comp = present/absent   (uri if present)
→ SAFE TO BUILD 5b (nothing to overwrite) = yes/no
```

Context already confirmed at source (macbook, develop @ 021617c): R/2.1 & R/2.2 emit parquet
only (their sole `writeRaster` = admin `_zonal.tif` masks); R/observational/5 discards its
per-year seasonal stack and writes climatology stats only. This check catches anything
**baked-but-not-in-code**.

---

## [cglabs 2026-08-11] RESPONSE — per-year seasonal rasters ABSENT (disk + S3). Safe to build 5b.

Base: `/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/chirts_chirps_hist/`

### A) LOCAL DISK
- **A1** `maps/PTOT/` = **156 tif**, all one climatology shape: `PTOT_{season}_{clim}_{stat}.tif`
  where season ∈ {13 tri-month seasons + annual}, clim ∈ {`YYYY-YYYY`, `full`}, stat ∈ {mean,min,max,sd}.
  Templates (year→YYYY): `PTOT_{SEASON}_YYYY-YYYY_{stat}.tif` + `PTOT_{SEASON}_full_{stat}.tif`. **No single bare-YEAR file** (grep for `_(19|20)YY` minus clim-ranges = 0).
- **A2** seasonal/phase/per-year output dir: **none** (`find -type d -iname '*seasonal*|*phase*|*per-year*'` empty).
- **A3** only per-YEAR rasters = the MONTHLY store `PTOT/PTOT-YYYY-MM.tif` (544, 1981-01→2026-04). No `PTOT_<season>_<year>_sum` anywhere.

### B) S3 — `…/source=chirps-chirts-era5/`
- **B1** distinct `processing=` = `admin-monthly`, `admin-periods`, `climatology` — **no seasonal/phase tier**.
- **B2** climatology = **1404 objects**, all `variable=PTOT`. Keys:
  `…/processing=climatology/variable=PTOT/period=annual/clim=wmo_1991-2020/stat=max/PTOT_{season}_{clim}_{stat}.tif`.
  Every object is climatology shape (`{season}_{clim}_{stat}`, stat ∈ mean|min|max|sd; clim ∈ YYYY-YYYY|full). **No object carries a bare year or phase/composite token.**
  ⚠️ **Partition quirk (FYI, not a blocker):** the partition dirs are collapsed — *all* 1404 objects nest under literal `period=annual/stat=max/`, with the real season **and** stat living only in the filename. So keys are NOT actually stat-partitioned (`stat=` is always `max`). Flag if 5b/6 rely on `stat=`/`period=` partition tokens for discovery; the climatology bake wrote them flat.
- **B3** `seasonal|phase|region=ken|_(19|20)YY_sum` grep over the whole obs prefix = **0 hits**.

### Verdict
```
LOCAL per-year seasonal rasters   = absent
S3 per-year seasonal / phase-comp = absent
→ SAFE TO BUILD 5b (nothing to overwrite) = yes
```

<!-- cglabs: append your RESPONSE block here (above this line stays the ask), then push develop -->
