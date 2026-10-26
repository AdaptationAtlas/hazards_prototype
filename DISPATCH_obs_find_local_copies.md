# DISPATCH — cglabs + pascal — is there a local CHIRPS v3 / CHIRTS-ERA5 / AgERA5 daily copy anywhere?

**Append-only; newest block on top. Each node prepends `### RESPONSE — <node>`.**

### RESPONSE — cglabs 2026-10-26 — no local CHIRPS v3 or CHIRTS-ERA5. AgERA5 has SRF and RH to 2025-12-31 locally, but stitched: v1.0-era stacks to 2022 + **v1.1.1** from 2023 (RH hour may differ across the seam)

**The answer.** There is **no** local CHIRPS v3 (daily or otherwise) and **no** CHIRTS-ERA5 anywhere on
this node's shared storage. Every local CHIRPS is **v2.0**, and every local CHIRTS is CHIRTS-daily
**v1.0, 1983-2016**. For AgERA5 there is **no v2.0**. There *is* a local AgERA5 with SRF + RH beyond
2022: yearly netCDF to **2025-12-31**, Africa-wide (120°W-120°E, 50°S-50°N). But 2023-2025 was
built in Feb 2026 from **AgERA5 v1.1.1** dailies, while 1979-2022 is a 2023-built stack that records
no source version. That leaves a version seam at 2023-01-01 and, for RH, possibly a different
time of day (details below). S1 should keep the Hugging Face cubes for CHIRTS-ERA5 and CHIRPS v3.

#### F1 — search
- `locate`/`plocate`: not installed, so `find` was used. `ionice -c3 nice -n19`, `-maxdepth 8`, case-insensitive,
  dirs + files; pruned `.snapshot`, `node_modules`, `.git`, `.ipynb_checkpoints`. Patterns exactly as
  given. Logs: `logs/obs_find_*_20261026_061310.log` and `logs/obs_find_split_20261026_061310/`.
- Roots: `{common_data}`, plus the three other mount paths of the same 192 TB bulk volume
  (`<bulk>/file-manager`, `<bulk>/examples`, `<bulk>/shared-data-premium`; **0 hits** in each), and the
  separate 30 TB home volume (`-xdev`; the only hits were two `download_AgERA5.R` scripts in repo checkouts).
- `{common_data}`: a single walk was too slow on NFS (>80 min), so each of the 95 top-level dirs was
  searched in parallel at `-maxdepth 7` (= 8 from the root); all 95 finished with rc 0.
  The exceptions:
  - `ecmwf_agera5_cmip6_africa` was completed by the single walk.
  - In `nex-gddp-cmip6`, the single walk file-checked `pr`, `pr2`, `pr_v1` and `hurs` and was stopped
    while in `rsds`. All 607 directory names to depth 3 were checked, with 0 hits. The remaining files
    are uniformly `<var>_<YYYY-MM-DD>.tif`.
- The patterns do not catch bare `chirps*` / `chirts*` dirs, so I also listed the top-level
  `chirps*`, `chirts*` and `chc_*` dirs. A non-matching `chirts` tree inside `ecmwf_agera5/netcdf/`
  surfaced through the AgERA5 hit.
- Also at the root: `download_chirps_v3_rnl.R` and `download_chirts_era5_temperature.R` (2026-05-28).
  They target a relative `agwise-datasourcing/.../Landing/{Rainfall/chirps_v3, TemperatureMax/CHIRTS-ERA5}`
  path that does **not** exist under any searched root, and the search found no `chirps_v3` or
  `CHIRTS-ERA5` directory. Either the downloads never ran or they landed on another machine.

#### F2 — hit directories
Hits grouped by parent directory. `{cd}` = `{common_data}`.

| path | product / version (source) | step | variables | first .. last | files | size | newest mtime | complete? |
|---|---|---|---|---|---|---|---|---|
| `{cd}/ecmwf_agera5/<var>/` (6 dirs) | **AgERA5 v1.0** (`…_final-v1.0.nc`) | daily | SRF, RH (`Relative-Humidity-2m-12h`), Tmax, Tmin, Tmean, wind 10 m | 1981-01-01 .. **2022-12-31** | 15 340 per var (= every day) | 81-162 GB per var, ~620 GB | 2023-04 .. 2025-01 | yes, to 2022; Tmean has 2 097 duplicate-date extra files |
| `{cd}/ecmwf_agera5/netcdf/<Var>/AgEra/` (RH, SRF, Tmax, Tmin, Tmean, wind) | AgERA5: **1979-2022** = yearly stacks written 2023-05 by terra, no units, long_name or source version recorded; **2023-2025** = ncrcat of **`final-v1.1.1`** dailies (SRF history) | daily, one file per year | as named | **1979 .. 2025-12-31** (2025: 365 days) | 47 per var | RH 26 GB, SRF 57 GB, Tmax 13 GB, Tmin 13 GB, Tmean 12 GB, wind 9.9 GB | 2026-02-10 | yearly series complete; see seam below |
| `{cd}/ecmwf_agera5/netcdf/Rainfall/AgEra/` | AgERA5 precipitation (v1.1.1 era) | daily, yearly files | precip | 2023 .. 2025 | 3 | 465 MB | 2026-02-09 | 2023-2025 only |
| `{cd}/ecmwf_agera5/netcdf/TemperatureMax/chirts/` | **CHIRTS-daily v1.0** (`title: dataVersion 1.0`), not ERA5 | daily, yearly files | Tmax | 1983 .. 2016 | 34 | 112 GB | 2023-10-04 | complete for v1 |
| `{cd}/ecmwf_agera5/chunks/` | AgERA5 RH yearly chunks, scratch-like | daily | RH | 1985 .. 1994 + 365 daily tmp files | 11 + 365 | 28 GB | 2023-05-01 | partial / intermediate |
| `{cd}/AgERA5_RH_2015_2022/`, `{cd}/AgERA5_Temp_2015_2022/Tmax/` | AgERA5 v1.0 subsets | daily | RH, Tmax (+ 1 Kenya tif each) | 2015-01-01 .. 2022-12-31 | 2 922 | 19 GB, 14 GB | 2023-04, 2024-06 | complete subset |
| `{cd}/chirps_af/zarr/chirps_prec_<yyyy>.zarr` (+ `netcdf/`, `raw/`) | **CHIRPS v2.0** (`title`/`version` attrs) | daily, one store per year | precip | 1981 .. 2025-12-31 (2025: 365 days); `raw/` tifs to 2023-12-31; `netcdf/chirps_prec_2026.nc` is **0 bytes** | 45 stores | Zarr 57 GB, nc 48 GB, raw 141 GB | Zarr 2026-03-19 | complete for v2.0 to 2025. Grid 7200 x 2000 at 0.05° (50°S-50°N, all longitudes) despite the `_af` name |
| `{cd}/chirps_wrld/` (configured `chirps_raw`) | CHIRPS v2.0 tifs | daily | precip | 1981-01-01 .. 2023-12-31 | 15 705 | 845 GB | 2024-06-27 | complete to 2023 |
| `{cd}/chirts/{Tmax,Tmin,RHum}/<yyyy>/`, `{cd}/chirts/netcdf/{tmax,tmin}/africa/`, `{cd}/chirts_africa/Tmax/` (configured `chirts_raw`) | **CHIRTS-daily v1.0** | daily | Tmax, Tmin, RHum | 1983 .. 2016 | 34 years | ~867 GB per var as reported by `du`; netcdf 223 GB | 2024-06 | complete for v1 (ends 2016) |
| `{cd}/ecmwf_agera5_cmip6_africa/*`, `{cd}/chirps_cmip6_africa*`, `{cd}/chirts_cmip6_africa` | CMIP6 future fields named after the obs products, **not observations** | daily | SRF, precip, RHum, … | 2021-2100 | ~1.76 M | — | — | n/a |
| `{cd}/atlas_hazards/cmip6/indices/historical-AgERA5/…`, `{cd}/hazards_prototype/Data/hazard_timeseries_mean_month/*AgERA5*`, `{cd}/rainfall_variability/AgERA5/` | pipeline **outputs** (long-term means, anomalies, annual means) | — | — | — | — | — | — | n/a |
| `{cd}/chc_cmip6/` | empty except `readme.txt`: "deleted due to space limitations" | — | — | — | — | — | — | n/a |

No `*.zarr` store exists other than the 45 CHIRPS v2.0 yearly stores.

**The AgERA5 seam, in detail.** For SRF/RH/Tmax, the 2021 and 2022 files have one variable named after
the year, plus `crs`, on a 3600 x 1000 grid (180°W-180°E). The 2023 files have `Solar_Radiation_Flux` /
`rhum` / `Temperature_Air_2m_Max_24h` on a 2400 x 1000 grid (120°W-120°E), with CF attributes. The
2025 SRF `history` names `Solar-Radiation-Flux_C3S-glob-agric_AgERA5_<date>_final-v1.1.1.area-subset…`.
The 2023+ RH says `long_name: Relative humidity at 18LT`, whereas the v1.0 RH dailies on disk are the
`-12h` layer, and the pre-2023 yearly RH stacks record no hour. If the 1979-2022 RH stack came from
those 12h dailies, then RH switches from 12LT to 18LT at 2023-01-01. That should be checked before
anyone uses this RH as one series. SRF is a daily sum, so the time of day does not apply, but it
still changes product version at 2023.

#### F3 — one table
| product | node | path (templated) | daily? | years | version | complete? |
|---|---|---|---|---|---|---|
| CHIRPS v3 | cglabs | — | — | — | **none found** | — |
| CHIRTS-ERA5 | cglabs | — | — | — | **none found** | — |
| CHIRPS (for reference) | cglabs | `{cd}/chirps_af/zarr/`, `{cd}/chirps_wrld/` | yes | 1981-2025 / 1981-2023 | **v2.0** | yes for v2.0 |
| CHIRTS (for reference) | cglabs | `{cd}/chirts/`, `{cd}/ecmwf_agera5/netcdf/TemperatureMax/chirts/` | yes | 1983-2016 | **v1.0** | yes for v1 (ends 2016) |
| AgERA5 dailies | cglabs | `{cd}/ecmwf_agera5/<var>/` | yes | 1981-2022 | **v1.0** | yes (SRF + RH stop at 2022) |
| AgERA5 yearly stacks | cglabs | `{cd}/ecmwf_agera5/netcdf/<Var>/AgEra/` | yes | **1979-2025** | v1.0-era to 2022, **v1.1.1** from 2023; **no v2.0** | yearly series complete, but versions are mixed and RH may change hour at 2023 |

**So, against the question:**
- A local, complete **CHIRPS v3 daily** for 2005-2025: **no**.
- A local, complete **CHIRTS-ERA5 daily** for 2005-2025: **no**.
- **AgERA5 v2.0**: **no**.
- Any AgERA5 with SRF + RH beyond 2022: **yes**, in `{cd}/ecmwf_agera5/netcdf/{SolarRadiation,RelativeHumidity}/AgEra/` to 2025-12-31, as a v1.0/v1.1.1 stitch with the RH caveat above.


### MACBOOK 2026-10-24 — READ-ONLY name search of the shared storage, both nodes (~minutes, background)

**Why.** The S0 probe and the pipeline-map inventory only looked in the *configured* raw trees
(`hosts.json` `chirps_raw` / `chirts_raw`, `ecmwf_agera5`) plus a shallow search. A copy made by
another user or project could sit elsewhere on the shared disks, as tifs, netCDF or Zarr. If a
complete, current copy exists locally, S1 should read it, not Hugging Face. Pete asked us to be sure.

**Scope.**
- Read-only: `find`, `ls`, `du`, `gdalinfo`/`ncdump` on **one** sample file per hit. No copying, no
  writes outside `logs/`.
- Run in the background with a log in `logs/obs_find_<STAMP>.log`. Use `ionice -c3 nice -n19` on shared
  NFS. Prune `.snapshot`, `node_modules` and `.git`.
- Public repo: report paths in `{common_data}/…` or `<bulk>/…` form; no usernames, hostnames, IPs or
  mount identifiers.

#### F1 — search
Roots:
- **CGlabs:** `{common_data}` and the bulk data root.
- **PASCAL:** the shared bulk root that holds `Data`, `Projects` and `Workspace`.

Use `-maxdepth 8` and **directories and files**, case-insensitive. Report hits grouped by parent directory, not every file:

```
*chirps*v3* *chirps-v3* *chirps3*            # CHIRPS v3
*chirts*era5* *chirts-era5* *CHIRTS_ERA5*    # CHIRTS-ERA5
*agera5* *AgERA5* *agrometeorological*       # AgERA5
*.zarr                                       # any Zarr store (may be one of the above)
```

If `locate`/`plocate` exists and its database is under 7 days old, it may be used instead. Say so,
and give the DB date.

#### F2 — for each distinct hit directory
Report:
- **product and version**, from the file name, any readme and one file's metadata
  (`gdalinfo` / `ncdump -h`, look for `version`, `title`, `source`);
- **daily or monthly or other**;
- variables;
- **first and last date**, and the file count;
- size (`du -sh`);
- extent and resolution of one sample;
- the newest mtime (when it was last updated);
- whether it looks like a complete series or a partial download.

For AgERA5 specifically: which **version** (v1.0 / v1.1 / v2.0), and are `solar_radiation_flux`
and the **relative-humidity** layers present, to what date?

#### F3 — answer in one table
| product | node | path (templated) | daily? | years | version | complete? |

**The question:** is there a local, complete **CHIRPS v3 daily** or **CHIRTS-ERA5 daily** covering
2005-2025, or an **AgERA5 v2.0** / any AgERA5 with SRF + RH beyond 2022?

Prepend the RESPONSE, commit only this file, push and verify.
