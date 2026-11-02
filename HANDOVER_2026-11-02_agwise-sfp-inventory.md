# Topic briefing — AgWise outputs on the CGlabs node, for the SFP data hub

**Date:** 2026-11-02 · **Node:** CGlabs · **Mode:** read-only, first pass
**Consumer:** whoever scopes the SFP data hub's AgWise holdings.
**Not a session handover.** Entry point stays `HANDOVER_2026-10-23.md`.

Companion thread: `DISPATCH_macbook_agwise_sfp.md`.

---

## 1. Question asked

Which AgWise (`github.com/CGIAR-AgWise`) outputs are on CGlabs storage and could feed the SFP
data hub? Clues to look for: fertilizer/yield maps, planting-date and cultivar outputs, climate
NetCDFs, DSSAT inputs, ERA–Carob data.

## 2. Method and its limits

High-level folder identification only. Immediate children of candidate roots, bounded output,
short timeouts. **No storage-wide recursive search, no recursive listing, no `du`, no file counts,
no content scans** — the bulk share holds billions of files, and truncating output does not bound
the cost of producing it. Nothing was downloaded, modified or published.

Where a name search was needed, the existing one was reused rather than repeated: the
`-maxdepth 8` sweep recorded in `DISPATCH_obs_find_local_copies.md` §F1 (2026-10-26) covered all
four mount paths of the bulk share and the separate home volume, finishing rc 0 on all 95
top-level directories.

## 3. Headline

**No AgWise working tree is on this node.** The landing path AgWise's own scripts write to does
not exist here. Present instead: two AgWise-authored download scripts, and several
AgWise-adjacent datasets owned by other projects.

## 4. Confirmed AgWise ownership

| path | what | evidence |
|---|---|---|
| `{common_data}/download_chirps_v3_rnl.R` | CHIRPS v3.0 daily rainfall NetCDF downloader, UCSB CHC, by year | header `Project : AgWise Data Sourcing / Agroclimate DataOps`; Alliance of Bioversity International and CIAT; CGIAR Climate Action Science Program; created 2026-05-28 |
| `{common_data}/download_chirts_era5_temperature.R` | CHIRTS-ERA5 daily Tmax/Tmin NetCDF downloader | same header block; created 2026-05-29 |

Author name and contact are in the script headers on the node; not reproduced here, this repo is
public.

Both write to a **relative** path:

```
agwise-datasourcing/dataops/datasourcing/Data/Global_GeoData/Landing/
    Rainfall/chirps_v3/<year>.nc
    TemperatureMax/CHIRTS-ERA5/<year>.nc
```

matching the `CGIAR-AgWise/agwise-datasourcing` repo layout. **That tree does not exist under any
searched root** (§F1 above found no `chirps_v3` and no `CHIRTS-ERA5` directory anywhere). Either
the downloads never ran, or they ran on another machine. Being relative paths, a run from the
wrong working directory would also have scattered them — none were found either way.

## 5. The two `agwise` directories

Both sit at the **home-directory level**, the same pane that lists `common_data` in the file
browser — not inside the bulk share:

```
drwxr-xr-x 1 root root 0 Nov  9  2022  ~/AgWise
drwxr-xr-x 1 root root 0 Jan 10  2023  ~/agwise
```

- Genuinely empty. `ls -la` returns only `.` and `..`; not permission-masked (0755, readable).
- On the **home volume (30 TB)**, a different filesystem from the bulk share (192 TB) that carries
  `common_data`. So not on the shared data volume at all.
- `root:root`, mtime equals creation date, never modified since. `Links: 1`, so no subdirectory
  ever existed.

Read: placeholder or stale mount-point stubs from the container image — most likely the AgWise
share is mounted under one of these names on a different host profile and was never wired up
here. Consistent with §4: the scripts expect a tree this node has never had.

**Nothing to harvest from them.** They are, however, two concrete mount-target names to put to
AgWise when asking where the real tree lives.

## 6. AgWise-adjacent candidates — ownership NOT confirmed AgWise

Ranked by apparent value to the hub.

| path | why relevant | source / intermediate / final |
|---|---|---|
| `{common_data}/era/` | **ERA** (Evidence for Resilient Agriculture) — the ERA side of the ERA–Carob clue. `data/`: `era.compiled.parquet`, `era_data_v1.0_2018.parquet`, `era_data_v1.1_2020.parquet`, versioned RData/csv (V1.0 2023-11-22, V1.1 2023-06-27), `MASTER CODES.gsheet`, `Unique Spatial Locations & Buffers.csv`, version histories `v1.0_history_2018`, `v1.1_history_2020_majestic_hippo`, `v1.2_history_2022_skinny_cow`. `geodata/`: site-joined covariates `CHIRPS.parquet`, `POWER.parquet`, `AgMERRA.parquet`, `ERA_SOS.RData`, landcover / topography / soils. `ancillary_datasets/`: chirps, dem, power, soilgrids download staging | **final** (versioned releases) + source staging + an `errors/` and `data_entry/` working layer |
| `{common_data}/isda/` | iSDA Africa soil, **30 m, v0.13, 2001-2017, WGS84**. `raw/` ~346 GB: `bdr`, `clay_tot_psa`, `db_od`, `fcc`, `log.al`, `log.c`, `log.ca`, `log.ecec.f`, `log.fe`, `log.k`, `log.mg`, at `0..20cm` / `20..50cm` / `0..200cm`. Metadata present: `fcc_attributes.tab`, `ph_metadata.json`. `intermediate/`: per-property `.vrt` + `_index.txt` tile indexes, built 2024-07-09 | **source** (third-party redistribution) + **intermediate** (VRT mosaics). No final layer |
| `{common_data}/suitability/` | `CropSuite`, `CropSuiteV2`, `climatedata`, `soilgrids` — crop suitability, cultivar-adjacent | mixed, uninspected |
| `{common_data}/atlas_crop_calendar/` | `raw_gaez2015`, `raw_jagermeyr`, `intermediate` — planting / harvest windows | source + intermediate |
| `{common_data}/atlas_sos/` | `LTavg`, `seasonal_mean` — start of season, i.e. a planting-date surface | Atlas product |
| `{common_data}/wapor/` | `aeti`, `tb` — WaPOR ET and biomass, yield proxy | source |
| `{common_data}/rhomis_v3/` | household survey, EiA-adjacent; `raw`, `download_rhomis.R`, `rhomis_vs_mapspam.R` | source + analysis |
| `{common_data}/soilgrids/` | `raw`, `intermediate`, 2024-06-26, owned by another uid | source + intermediate |
| `{common_data}/atlas_yield/` | `daily_gcm_extract` — yield-named, but a GCM extract | intermediate |
| climate NetCDFs: `{common_data}/ecmwf_agera5/`, `chirps_af/`, `chirps_wrld/`, `chirts/` | already inventoried with products, versions, date ranges and completeness in `DISPATCH_obs_find_local_copies.md` §F2 — do not re-measure | source |

**DSSAT: nothing found.** The soil holdings are gridded property rasters, not DSSAT profiles — no
`.SOL`, no profile-keyed table at depth 1. They are the ingredients an AgWise DSSAT workflow builds
from, which is how AgWise uses iSDA, but no AgWise-built profile set is on this node.

**Carob: nothing found.** `era/` is ERA proper. No `carob` directory at any inspected level.

## 7. Ruled out

- `{common_data}/EiA_pub/` — the name says EiA, the contents are `NTx20..NTx44[_mean].tif`, i.e.
  **Atlas hazard outputs** published for EiA. Not an AgWise product.
- `{common_data}/codes/`, `~/1000farms-course/`, `~/gap-analysis-workshop/` — empty.
- `~/shared-data-premium/` → `nex-gddp-cmip6_raw` only. `~/file-manager/`, `~/examples/` → scratch.

## 8. Metadata availability

Thin on the volume. **No README and no manifest at the `common_data` root** — only loose `.R` and
`.csv` files. Per-dataset metadata exists only where noted above (`ph_metadata.json`,
`fcc_attributes.tab`, ERA's `MASTER CODES`). The usable manifests live in this repo, not on the
share:

- `DISPATCH_obs_find_local_copies.md` §F2 — climate product inventory, versions and completeness
- `metadata/hosts.json` — path registry
- `HANDOVER_2026-10-22_hazard-pipeline-map-and-obs-baseline.md` — pipeline map

## 9. Open questions for whoever picks this up

1. **Where does `agwise-datasourcing/.../Landing/` actually live?** Ask AgWise; cheaper than any
   further search here. Name `~/AgWise` and `~/agwise` as the stub mount targets on this node.
2. **Licensing.** `isda/raw/` is a third-party redistribution; ERA has its own terms. Neither can
   be listed in a hub without checking the upstream licence first. Nothing here was published.
3. **Is `era/` the holding the hub wants**, or does the hub want AgWise's *use* of ERA? Different
   answers, different owners.

## 10. Proposed next narrow inspection

`{common_data}/era/` — depth-1 `ls -l` for sizes and mtimes, plus **parquet schemas only** (column
names and types, no row scans); separate `data/data_entry/` and `errors/` from the versioned
releases. Bounded, read-only, minutes.
