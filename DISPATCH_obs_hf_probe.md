# DISPATCH — cglabs + pascal — observed pipeline S0b: the Hugging Face route for CHIRTS-ERA5 and CHIRPS v3

**Append-only; newest block on top. Each node prepends its own `### RESPONSE — <node>` block.**
Sibling of `DISPATCH_obs_s0_probe.md` (same nodes, same question: where does ingest run, and from where).
Kept as its own file so the two macbook sessions do not edit one file's top at the same time.

---

### MACBOOK 2026-10-24 — READ-ONLY probe on BOTH nodes: re-measure bandwidth against the CDH Zarr cubes on Hugging Face

**Why.** The S0 probe found `data.chc.ucsb.edu` throttles bulk clients to 8-10 kB/s after about
100 MB (PASCAL) and caps streams at about 1 MB/s (CGlabs). Pete pointed out that the CDH already
redistributes both datasets: `cdh-data-pipeline` built Zarr v3 cubes on 2026-10-08 and uploaded them
to Hugging Face (records inside the repos; the public catalog is being updated, cdh-catalog#75):

| cube | content | size |
|---|---|---|
| `aaguilar90/chirts-era5-daily` | **CHIRTS-ERA5** Tmax, Tmin, 1981-01-01 to 2026-10-03, 0.05°, 70°N-60°S, int16 × 0.1 °C | 118 GB |
| `aaguilar90/chirps-v3-daily-rnl` | CHIRPS v3 daily `rnl`, 1981-01-01 to 2026-08-31, 0.05°, 60°N-60°S, int16 × 0.1 mm | 34 GB |

Chunks are 128 days × 300 × 300 cells, Zstandard. An Africa window for one 128-day slab is 36 chunk
files per variable, about 90 MB for CHIRTS and 55 MB for CHIRPS, fetched as plain HTTPS objects (the
`resolve/main/` URL answers 302 to a CDN host; range requests return 206). Chunks that are all fill
(ocean) are not stored and answer 404; that is 11 of the 36 CHIRTS chunks and 4 of 36 CHIRPS chunks
in the Africa window, and the script counts them separately from errors.

If the cubes stream at a usable rate, PASCAL's items 1 and 2 in the S0 thread dissolve: one vintage
(2026-10-08) for the whole 1981-2026 series, nothing pulled from CHC, and the node choice falls to CPU
and disk only. Earth Engine is not a substitute for CHIRTS: `UCSB-CHG/CHIRTS/DAILY` is CHIRTS-daily
v1, 1983-2016 only. CHIRPS v3 `rnl` is on GEE (`UCSB-CHC/CHIRPS/V3/DAILY_RNL`, to 2026-09-30) as a
fallback for the rainfall side.

**The script** is `scripts/obs_hf_probe.sh` (header explains every test). It ran end to end from the
macbook on 2026-10-24: single stream 1.4-1.9 MB/s, 8 streams 4-5.4 MB/s aggregate on a laptop
connection, range request 206. It writes only under the directory you give it and deletes that on exit.

**Scope.** Read-only. `OBS_TMP_ROOT` must be a directory **under your bulk data root** (not `/`, not
`/home`). No pipeline runs, no S3 writes. Background it; the log goes in `logs/`. Public repo: no IPs,
hostnames or mount identifiers in the RESPONSE (`AGENTS.md` §3 grep before committing). On PASCAL,
say which session type you are in (JupyterHub profile and `cpu.max`, or SSH/RStudio).

#### H — run it (both nodes; about 5 minutes)

```bash
cd <repo root> && git pull --ff-only && git log -1 --format='%h %an %s'
STAMP=$(date +%Y%m%d_%H%M%S); mkdir -p logs
OBS_TMP_ROOT=<a dir under your bulk data root> nohup bash scripts/obs_hf_probe.sh > logs/obs_hf_probe_$STAMP.log 2>&1 &
echo $! > logs/obs_hf_probe_$STAMP.pid
# watch: while kill -0 $(cat logs/obs_hf_probe_$STAMP.pid) 2>/dev/null; do sleep 20; done; cat logs/obs_hf_probe_$STAMP.log
```

Then run it a second time with `OBS_HF_PAR=16` (same log pattern, different STAMP), to see whether
the aggregate keeps scaling or the CDN caps it.

Before H7/H8 can mean anything, report once:
```bash
python3 -c 'import zarr, xarray; print("zarr", zarr.__version__, "xarray", xarray.__version__)' 2>&1 | tail -1
Rscript -e 'cat("terra", as.character(packageVersion("terra")), "gdal", as.character(terra::gdal()), "\n")'
gdal-config --version 2>/dev/null || echo "no gdal-config"
```

#### Gates — stop at each

1. Every `H1`-`H5` line has `errors=0`. `absent_ocean_404` is expected (11 for CHIRTS slabs, 4 for the
   CHIRPS slab); a different count is a finding, not a failure. Any `errors>0`: stop and paste the
   line and the HTTP codes.
2. `H6` reads `http=206`. A 200 or 403 means range reads are not honoured for this client; stop and say so.
3. **Invariants, not figures:** H2 aggregate ≥ H3 single-stream (parallelism helps, unlike CHC); H4
   within a factor of 2 of H2 (recent years are not served differently); H5 not slower per byte than H2.
   State the numbers either way. If H2 < H3, or any slab is below the S0 CHC single-stream rate on
   this node, say so and stop.
4. H7 and H8 are allowed to print `skipped` or `FAILED`; paste the line. H8 is expected to **fail on
   GDAL < 3.11** (`Unsupported codec: bytes` or "cannot open"); CGlabs terra links 3.11.3 so it should
   open there. If H8 opens, its `mean_scaled` for 2015-06-15 over Africa should be a plausible Tmax
   in °C (roughly 25-40). If it opens and the mean is around 300, the scale factor was applied
   twice or not at all; report it.

#### Report

Paste the whole CSV block from both runs, the three version lines, and the S0 CHC figures for this node
beside them (A1 single-stream, A4 8-stream) so the two routes sit in one table. Then your read: at the
measured aggregate, how long is the 1981-2026 Africa window for CHIRTS (Tmax + Tmin ≈ 2 × 131 slabs ×
≈ 90 MB ≈ 24 GB) and for CHIRPS (131 slabs × ≈ 55 MB ≈ 7 GB)? Those are the ingest sizes if S1 reads
the cubes directly instead of the CHC tifs.

#### Then

1. Confirm `$OBS_TMP_ROOT/obs_hf_probe.*` is gone (the script removes it on exit).
2. Prepend `### RESPONSE — <node> 2026-10-xx` above this block.
3. Commit and push only this file. Verify on `origin/develop` (`git log origin/develop..HEAD` empty).

**Stop and report** rather than work around: an `errors>0` line, a non-206 on H6, or the script
refusing to start (`OBS_TMP_ROOT` unset).
