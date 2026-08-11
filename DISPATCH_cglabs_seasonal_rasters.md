# DISPATCH — cglabs ⇄ macbook · seasonal CHIRPS rasters (KE-ENSO)

_Append-only. Newest entry on top. cglabs runs, appends a RESPONSE block, pushes; macbook reads._

Workstream: new per-year + phase-composite seasonal rainfall COGs for the KE-ENSO Explorer.
Producer to be written: `R/observational/5b_make_obs_seasonal_rasters.R` (+ new tiers in
`6_publish_obs_to_s3.R`). Node confirmed = cglabs (CHIRPS-resident obs home node).

---

## [macbook 2026-08-11] ACTION → cglabs: confirm per-year seasonal rasters DO NOT already exist

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

<!-- cglabs: append your RESPONSE block here (above this line stays the ask), then push develop -->
