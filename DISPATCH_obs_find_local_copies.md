# DISPATCH — cglabs + pascal — is there a local CHIRPS v3 / CHIRTS-ERA5 / AgERA5 daily copy anywhere?

**Append-only; newest block on top. Each node prepends `### RESPONSE — <node>`.**

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
