# DISPATCH — cglabs ⇄ macbook · seasonal CHIRPS rasters (KE-ENSO)

_Append-only. Newest entry on top. cglabs runs, appends a RESPONSE block, pushes; macbook reads._

Workstream: new per-year + phase-composite seasonal rainfall COGs for the KE-ENSO Explorer.
Producer to be written: `R/observational/5b_make_obs_seasonal_rasters.R` (+ new tiers in
`6_publish_obs_to_s3.R`). Node confirmed = cglabs (CHIRPS-resident obs home node).

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
