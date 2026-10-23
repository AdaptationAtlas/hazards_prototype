# DISPATCH — cglabs + pascal — observed pipeline S0: where should ingest run?

**Append-only; newest block on top. Each node prepends its own `### RESPONSE — <node>` block.**

### MACBOOK 2026-10-23 — READ-ONLY probe on BOTH nodes. Measure network and resources; on PASCAL, also inventory the CHIRTS/CHIRPS trees.

**Why.** `PLAN_obs_hazard_pipeline.md` (v2, decisions D1-D5) needs about 390 GiB of CHIRTS-ERA5 daily
data, 25-30 GB of AgERA5 and about 50 GB of ASAP FPAR, streamed and reduced to 0.25° Africa. Compute
is small (S2-S5 under ~1 h). **Download bandwidth decides the node.** PASCAL's `chirts_raw` points at a
`chirts_era5` tree; if that tree already holds CHIRTS-ERA5 *daily*, the largest download disappears.

**Scope.**
- Read-only. Test downloads go to a temporary directory **under your bulk data root** (not `/`, not
  `/home`), and are deleted at the end.
- No pipeline runs, no S3 writes.
- On PASCAL, use an SSH or RStudio session, or say which JupyterHub profile you are in and read the
  `cpu.max` value (`server-environment.md` §3).
- Background anything over about a minute; logs go in `logs/obs_s0_probe_<STAMP>.log`.
- Public repo: no IPs, hostnames or mount identifiers in the RESPONSE (`AGENTS.md` §3 grep before
  committing).

#### A — bandwidth (both nodes)

Time each download with `curl -s -o "$TMP/<name>" -w '%{size_download} %{time_total} %{speed_download}\n' <url>`.
Run each twice and report both.

- **A1. CHIRTS-ERA5 daily Tmax**, about 26 MiB:
  `https://data.chc.ucsb.edu/experimental/CHIRTS-ERA5/tmax/tifs/daily/2015/CHIRTS-ERA5.daily_Tmax.2015.06.15.tif`
- **A2. AgERA5 v2.0 SRF**, about 12.8 MB:
  `https://storage.googleapis.com/fao-gismgr-c3s-data/DATA/C3S/MAPSET/AGERA5-SRF/C3S.AGERA5-SRF.2015-06-15.tif`
- **A3. ASAP FPAR, one dekad**, about 305 MB. Pick the file nearest mid-2015 from
  `https://agricultural-production-hotspots.ec.europa.eu/data/indicators_fpar/fpar/` and report the
  name.
- **A4. Parallel throughput:** 8 concurrent A1-type downloads (8 different days in 2015-06),
  `xargs -P 8`. Report aggregate MB/s.
- **A5. Windowed read:** time
  `gdal_translate -projwin -18 38 52 -35 /vsicurl/<A2 url> $TMP/af.tif`
  and report `gdal-config --version`.

#### B — resources (both nodes)

`nproc`; usable CPUs (on PASCAL from `cpu.max`); `free -g`; `uptime` (the load now); free space on the
bulk data root (`df -h`); R `packageVersion("terra")` and the GDAL that terra reports
(`terra::gdal()`).

#### C — PASCAL only: what is in the raw climate trees?

The `chirts_raw` and `chirps_raw` roots are in `metadata/hosts.json` (pascal profile). For each, report:
- the product and version, from file names, any readme and the tif metadata;
- **daily or monthly**;
- which variables (Tmax, Tmin, RH, other);
- the first and last date;
- file count and approximate size;
- the extent and resolution of one file (`gdalinfo`).

**The question that matters: is CHIRTS-ERA5 daily Tmax/Tmin present for 2005-2025, and is CHIRPS v3
daily present?**

#### Then

1. Delete `$TMP`.
2. Prepend `### RESPONSE — <node> 2026-10-xx` with the A/B (and C) tables.
3. Commit and push only this file.
4. Verify on `origin/develop` (`git log origin/develop..HEAD` empty).

**Stop and report** if any URL returns non-200, or `/vsicurl` fails, rather than working around it.
