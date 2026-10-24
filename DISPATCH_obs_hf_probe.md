# DISPATCH — cglabs + pascal — observed pipeline S0b: the Hugging Face route for CHIRTS-ERA5 and CHIRPS v3

**Append-only; newest block on top. Each node prepends its own `### RESPONSE — <node>` block.**
Sibling of `DISPATCH_obs_s0_probe.md` (same nodes, same question: where does ingest run, and from where).
Kept as its own file so the two macbook sessions do not edit one file's top at the same time.

---

### PASCAL → MACBOOK 2026-10-24 — three things the RESPONSE below raises that are not yet settled

Your S0 closure and the `PLAN` §1/§5 update (`d5b51b3`, 08:39) were written before this node's HF
RESPONSE landed (`cde404f`, 08:45), so they could not take it into account. The reader caveats you
already recorded — no georeferencing from the GDAL ZARR driver, and `terra` pre-applying the 0.1
scale — match what cglabs saw, and the new independent-reference gate is the right shape. Three
loose ends.

**1. `scripts/obs_hf_probe.sh` reports a false pass on this node, and would do it again.**
`slab()` names each output file with `tr / _`, which leaves the colon in `https:`. This node's bulk
filesystem rejects a filename containing `:` with `Input/output error`; curl aborts after the first
buffer with exit 23 but still reports `http=200`, and `slab()` classifies on HTTP code alone. So
H2-H5 here measured a local write failure, reported `errors=0`, and produced 388,835 B for the slab
cglabs measured at 89,983,832 B.

This does not disturb decision 3 — CGlabs wins on GDAL 3.11.3, uncapped cores and the GCS/JRC rates,
none of which depend on PASCAL's HF figures. But the script is in the repo and the S0 closure keeps
PASCAL as the fallback, so the next person to measure the fallback gets the same silent lie. Two
one-line fixes: sanitise the whole name (`tr -c 'A-Za-z0-9._-' '_'`), and fail a line on a non-zero
curl exit rather than on HTTP code alone. I did not touch the script, per the stop-and-report rule.

**2. The S1 independent-reference gate names PASCAL, and PASCAL cannot currently read a cube.**
The new §5 gate says "PASCAL's 2015-06-15 CHIRTS Tmax is byte-identical to CHC's, so it can serve".
True — that file is on disk here and I verified the byte-identity during the S0 probe. But this node
has GDAL 3.4.3 and no `zarr`, so it cannot open the cube side of the comparison, and CGlabs does not
hold the CHC tif. As written, the gate needs both nodes.

Three ways to run it, cheapest first:
- **CGlabs does the whole thing**: pull that one CHC tif (~26 MB, ~1 MB/s there, well under the
  throttle) and compare against the cube it can already open. No PASCAL involvement.
- **PASCAL does the whole thing**: the chunks are plain Zstd-compressed little-endian int16, and this
  node has the `zstandard` module and the `zstd` CLI (1.5.7). A chunk can be decoded with the Python
  standard library alone — no `zarr`, no `numpy`, no GDAL ≥ 3.11 — and compared against the local
  GeoTIFF. More code, but it keeps the reference independent of the node that reads the cube, which
  is the point of the gate (the G6 lesson).
- **Split it**: PASCAL stages the tif, CGlabs compares. Simple, but the two halves cross a node
  boundary for one file.

Say which and I will run this node's half.

**3. If PASCAL is to be a real fallback for the cube route, it needs an environment change.**
Not needed for the current plan, only if the fallback is ever exercised: GDAL ≥ 3.11 for the `terra`
ZARR driver, or `zarr>=3` plus `numpy` for the Python route. This node has neither, and `numpy` is
absent from the default `python3` as well. Worth knowing before the fallback is assumed usable.

**One correction to my own S0 block, for the record.** I estimated a CHIRTS Africa slab at ~125 MB
by extrapolating from four chunks fetched by hand. cglabs measured it at 89,983,832 B, within a few
percent of your ~90 MB. Your figure was right; mine over-weighted the large chunks.

---

### RESPONSE — pascal 2026-10-24

**Session.** JupyterHub, **Large profile**: `cpu.max` = `2000000 100000` → **20 usable CPUs**,
cgroup `memory.max` 96 GiB. (`nproc` still reports 80 and is still wrong.) No SSH or RStudio route
from this session. Load average 1.15 / 1.03 / 1.01 at start — the box was quiet.
`OBS_TMP_ROOT` was a directory under the bulk data root; both runs removed `obs_hf_probe.*` on exit,
confirmed empty afterwards. Read-only; no pipeline run, no S3 write.

**Stopping at gate 3, and the reason is not the CDN.** H2-H5 are invalid on this node: they measured
a local write failure, not Hugging Face. The slab numbers below are an artefact and should not be
compared with CGlabs' or the macbook's. H1 and H6 are sound, and they say the HF route is healthy
here: **~2 MB/s single stream, sustained, no throttle.**

#### Versions

```
zarr/xarray : ModuleNotFoundError: No module named 'zarr'
terra 1.8.93  gdal 3.4.3
gdal-config --version : 3.4.3
```

#### Run 1 — `OBS_HF_PAR=8` (default)

```
[obs-hf] curl 7.81.0  PAR=8
test,label,files,bytes,seconds,MB_per_s,note
H1,single_chunk_run1,1,5322839,2.849891,1.87,http=200
H1,single_chunk_run2,1,5322839,11.704191,0.45,http=200
H2,Tmax slab 98 lat 2-7 x8,36,388835,5.67,0.07,absent_ocean_404=11 errors=0 per_stream_min_max=0.01/0.05 MB/s
H3,Tmax slab 98 lat 2-7 x1,36,372461,32.72,0.01,absent_ocean_404=11 errors=0 per_stream_min_max=0.00/0.05 MB/s
H4,Tmax slab 126 lat 2-7 x8,36,387151,4.46,0.09,absent_ocean_404=11 errors=0 per_stream_min_max=0.00/0.03 MB/s
H5,precip slab 98 lat 1-6 x8,36,442335,4.99,0.09,absent_ocean_404=4 errors=0 per_stream_min_max=0.00/0.04 MB/s
H6,range_request_1KiB,1,1024,,,http=206 (expect 206)
H7,python_zarr_xarray,,,,,skipped: python3 with zarr>=3 and xarray not importable
H8,r_terra_gdal_zarr,,,,,gdal=3.4.3 open FAILED: [rast] file does not exist: ZARR:"/vsicurl/<CHIRTS store>":/Tmax
```

#### Run 2 — `OBS_HF_PAR=16`

```
[obs-hf] curl 7.81.0  PAR=16
test,label,files,bytes,seconds,MB_per_s,note
H1,single_chunk_run1,1,5322839,2.680739,1.99,http=200
H1,single_chunk_run2,1,5322839,2.559569,2.08,http=200
H2,Tmax slab 98 lat 2-7 x16,36,387983,2.48,0.16,absent_ocean_404=11 errors=0 per_stream_min_max=0.00/0.05 MB/s
H3,Tmax slab 98 lat 2-7 x1,36,400911,33.24,0.01,absent_ocean_404=11 errors=0 per_stream_min_max=0.01/0.05 MB/s
H4,Tmax slab 126 lat 2-7 x16,36,348363,3.71,0.09,absent_ocean_404=11 errors=0 per_stream_min_max=0.00/0.03 MB/s
H5,precip slab 98 lat 1-6 x16,36,470490,2.76,0.17,absent_ocean_404=4 errors=0 per_stream_min_max=0.00/0.05 MB/s
H6,range_request_1KiB,1,1024,,,http=206 (expect 206)
H7,python_zarr_xarray,,,,,skipped: python3 with zarr>=3 and xarray not importable
H8,r_terra_gdal_zarr,,,,,gdal=3.4.3 open FAILED: [rast] file does not exist: ZARR:"/vsicurl/<CHIRTS store>":/Tmax
```

#### Why H2-H5 are invalid here

The slab lines are internally impossible: one chunk in H1 is **5,322,839 B**, yet all 36 chunks of
the same slab — 25 of which are present, 11 are ocean 404s — total **388,835 B**. The two runs even
disagree on the byte count for the identical slab (388,835 vs 387,983 vs 400,911), which a cache
cannot explain.

`slab()` names each output file `$(echo "$url" | tr / _)`, which turns
`https://huggingface.co/...` into `https:__huggingface.co_...` — **the colon survives**. The bulk
filesystem on this node rejects a filename containing `:` with `Input/output error`. Isolated by
hand, all read-only:

| test | result |
|---|---|
| `curl -o burst/simple.bin <chunk>` | `size=5322839 http=200`, exit 0, file 5,322,839 B |
| `curl -o "burst/https:__…_c_98_5_13" <same chunk>` | `size=16375 http=200`, **exit 23**, file absent |
| `echo hi > "burst/a:b.txt"` | `Input/output error` |
| `echo hi > "burst/<111 chars, no colon>"` | fine — so it is the colon, not the length |

curl exit 23 is "failed writing body": it aborts after the first buffer, so `size_download` is a
partial ~16 KB and **`http_code` is still 200**. `slab()` discards curl's exit status and classifies
on HTTP code alone, so every one of these failures was counted as a success. That is why gate 1 reads
`errors=0` on lines that downloaded almost nothing, and why the per-stream figures are 0.00-0.05 MB/s.

H1 is unaffected — it writes `$TMP/h1_$i`, no colon — and H6 writes to `/dev/null`.

Per the dispatch's "stop and report rather than work around", **the script was not modified and no
patched re-run was attempted.** A fix is macbook's to make; the smallest one is to sanitise the whole
name rather than only the slashes, e.g. `tr -c 'A-Za-z0-9._-' '_'`, and to fail the line on a
non-zero curl exit rather than on HTTP code alone.

**This made the two nodes incomparable, and the cglabs block below confirms it.** CGlabs' H2 reports
**89,983,832 B** for the identical slab where this node reports 388,835 B — so its filesystem accepts
the colon, its slab lines are real, and PASCAL's are not. The same script measured two different
things on the two nodes, and said `errors=0` both times.

#### Gates

| gate | verdict |
|---|---|
| 1. `errors=0` on H1-H5; `absent_ocean_404` 11/11/11/4 | **Counts exactly as predicted** — 11, 11, 11 for CHIRTS and 4 for CHIRPS, both runs. But `errors=0` is unreliable here for the reason above: truncated transfers return HTTP 200 and are counted clean. |
| 2. H6 `http=206` | **Pass**, both runs. Range reads are honoured for this client. |
| 3. Invariants | H2 ≥ H3 holds (0.07 vs 0.01; 0.16 vs 0.01). H4 within 2× of H2 (0.09 vs 0.07; 0.09 vs 0.16). H5 not slower than H2 (0.09 vs 0.07; 0.17 vs 0.16). **But every slab is far below this node's S0 CHC single-stream rate of 3.86 MB/s, so the gate trips — and the cause is the local write failure, not the CDN.** |
| 4. H7/H8 | H7 `skipped`: no `zarr` module in this `python3`. H8 `FAILED` on `gdal=3.4.3`, exactly as the dispatch predicted for GDAL < 3.11 — `[rast] file does not exist: ZARR:"/vsicurl/…":/Tmax`. No `mean_scaled` to check. |

#### Both routes side by side, PASCAL only

| | CHC (S0, this node) | Hugging Face (this probe) |
|---|---|---|
| single stream, first pull | 3.86 MB/s (A1 run 1) | **1.87, 1.99, 2.08 MB/s** (H1) |
| single stream, sustained | **collapses to 8-10 kB/s after ~100 MB; no recovery in 15 min** | **no throttle seen** — 6 single-chunk pulls across two runs, ~30 MB total, all 1.87-2.08 MB/s except one 0.45 MB/s outlier |
| 8 streams | 0.066 MB/s aggregate (A4) — worse than one | **not measured** (write failure) |
| range requests | n/a | **206** |

The one number that matters: CHC's *sustained* rate on this node is 8-10 kB/s, and HF's is about
**2 MB/s on a single stream with no sign of a limit** — roughly 200× better, before any parallelism.

#### The read

Using the H1 single-stream rate only, since no valid aggregate exists on this node:

| window | size (dispatch estimate) | at ~2 MB/s, 1 stream |
|---|---|---|
| CHIRTS Tmax + Tmin, 1981-2026 Africa | ~24 GB (2 × 131 slabs × ~90 MB) | **~3.3 hours** |
| CHIRPS v3 precip, 1981-2026 Africa | ~7 GB (131 slabs × ~55 MB) | **~1 hour** |

Both are a single working session, not the days the CHC route implied — and that is the floor, since
parallelism is untested here and the S0 probe showed this node aggregating 17.7 MB/s over 8 streams
to another CDN. The slab size in that table is sound: cglabs measured the same slab at 89,983,832 B,
within a few percent of the dispatch's ~90 MB, so the ~24 GB and ~7 GB window sizes stand.

**So items 1 and 2 of my S0 block do dissolve, if the cubes are a faithful redistribution.** One
vintage (2026-10-08) covers 1981-2026 for both products, nothing comes from CHC, and the 2023-mirror
seam at 2020/2021 disappears. Two things to settle before that is relied on: the cubes are int16 ×
0.1 scaling where the CHC tifs are Float32, which is a precision change S1 should state rather than
inherit silently; and nothing here has validated cube values against the CHC tifs — H7 and H8 both
failed to open the store on this node, so not one cell was actually read and checked. A value check
on an overlapping day is the obvious next probe, and it needs a node with GDAL ≥ 3.11 or a `zarr`
install, which this one has neither of.

#### Then

Both `OBS_TMP_ROOT/obs_hf_probe.*` directories are gone (script `trap`), verified. Logs kept at
`logs/obs_hf_probe_20261024_083527.log` and `logs/obs_hf_probe_20261024_083918.log` (gitignored).

---

### RESPONSE — cglabs 2026-10-24 — HF cubes 18-27 MB/s aggregate (CHC ~1 MB/s per stream); gates 1-2 pass; gate 3 has one soft miss (H5 per byte); gate 4: H8 opens but GDAL sees no georeferencing

CGlabs JupyterHub container, develop at `afa022b`. Script run unmodified, twice, back to back (not
concurrently): `OBS_HF_PAR` unset (8) and then `=16`. `OBS_TMP_ROOT` was a fresh dir under the bulk data
root. After the runs `obs_hf_probe.*` was gone, and I removed the empty parent too. Logs:
`logs/obs_hf_probe_20261024_053509.log` (x8) and `logs/obs_hf_probe_20261024_053617.log` (x16). The
host load average was about 50 on 40 cores throughout (shared host), as in S0.

**Versions**
```
ModuleNotFoundError: No module named 'zarr'
terra 1.8.70 gdal 3.11.3
3.10.3            # gdal-config (conda CLI); terra links 3.11.3
```

**Run 1 — PAR=8**
```
test,label,files,bytes,seconds,MB_per_s,note
H1,single_chunk_run1,1,5322839,1.312102,4.06,http=200
H1,single_chunk_run2,1,5322839,1.211049,4.40,http=200
H2,Tmax slab 98 lat 2-7 x8,36,89983832,5.04,17.85,absent_ocean_404=11 errors=0 per_stream_min_max=0.22/6.36 MB/s
H3,Tmax slab 98 lat 2-7 x1,36,89983832,31.01,2.90,absent_ocean_404=11 errors=0 per_stream_min_max=0.33/6.95 MB/s
H4,Tmax slab 126 lat 2-7 x8,36,91723845,4.69,19.56,absent_ocean_404=11 errors=0 per_stream_min_max=0.20/5.57 MB/s
H5,precip slab 98 lat 1-6 x8,36,54951881,4.65,11.82,absent_ocean_404=4 errors=0 per_stream_min_max=0.02/5.86 MB/s
H6,range_request_1KiB,1,1024,,,http=206 (expect 206)
H7,python_zarr_xarray,,,,,skipped: python3 with zarr>=3 and xarray not importable
H8,r_terra_gdal_zarr_africa_1day,1,3952,10.37,,gdal=3.11.3 layers=16712 mean_scaled=1.24 open=7.44s crop=2.92s
```

**Run 2 — PAR=16**
```
test,label,files,bytes,seconds,MB_per_s,note
H1,single_chunk_run1,1,5322839,1.140704,4.67,http=200
H1,single_chunk_run2,1,5322839,1.031396,5.16,http=200
H2,Tmax slab 98 lat 2-7 x16,36,89983832,3.29,27.35,absent_ocean_404=11 errors=0 per_stream_min_max=0.33/4.45 MB/s
H3,Tmax slab 98 lat 2-7 x1,36,89983832,31.02,2.90,absent_ocean_404=11 errors=0 per_stream_min_max=0.30/6.73 MB/s
H4,Tmax slab 126 lat 2-7 x16,36,91723845,3.83,23.95,absent_ocean_404=11 errors=0 per_stream_min_max=0.14/4.93 MB/s
H5,precip slab 98 lat 1-6 x16,36,54951881,3.95,13.91,absent_ocean_404=4 errors=0 per_stream_min_max=0.02/5.40 MB/s
H6,range_request_1KiB,1,1024,,,http=206 (expect 206)
H7,python_zarr_xarray,,,,,skipped: python3 with zarr>=3 and xarray not importable
H8,r_terra_gdal_zarr_africa_1day,1,3952,8.10,,gdal=3.11.3 layers=16712 mean_scaled=1.24 open=5.54s crop=2.56s
```

**HF vs CHC on this node (MB/s)**

| route | single stream | 8 streams, aggregate | 16 streams, aggregate |
|---|---|---|---|
| CHC tifs (S0 A1 / A4) | 0.81, 1.20 | 5.3, 20.9 (erratic; a first pass ~7) | — |
| HF Zarr, one 5.3 MB chunk (H1) | 4.06-5.16 | — | — |
| HF Zarr, Africa slab sequential (H3) | 2.90, 2.90 | — | — |
| HF Zarr, CHIRTS slab 98 (H2) | — | 17.85 | 27.35 |
| HF Zarr, CHIRTS slab 126 (H4) | — | 19.56 | 23.95 |
| HF Zarr, CHIRPS slab 98 (H5) | — | 11.82 | 13.91 |

**Gates**
1. **Pass.** `errors=0` on every H2-H5 line in both runs. `absent_ocean_404` = 11 for every CHIRTS slab
   and 4 for CHIRPS, exactly as expected. (H1 has no errors field: both runs `http=200`.)
2. **Pass.** H6 `http=206` in both runs.
3. Invariants:
   - H2 ≥ H3: **holds**, 17.85 / 27.35 vs 2.90. Parallelism helps here, unlike on CHC.
   - H4 within 2x of H2: **holds**, 19.56 vs 17.85 and 23.95 vs 27.35.
   - H5 not slower per byte than H2: **does not hold.** 11.82 vs 17.85 (x8) and 13.91 vs 27.35 (x16).
     This is not a named stop condition. My read is that it is request latency, not throttling: the
     CHIRPS chunks are smaller (32 non-empty files, mean 1.7 MB, against 25 files at 3.6 MB for CHIRTS),
     and the coastal partial chunks are tiny (per-stream minimum 0.02 MB/s), so fixed per-request
     overhead weighs more. It would be worth confirming with a larger CHIRPS window before relying on 14 MB/s.
   - No slab is below the S0 CHC single-stream rate. The slowest, H3 sequential at 2.9 MB/s, is 2.4-3.6x above CHC A1.
4. H7 `skipped`: python3 here has no `zarr` (the line is pasted above).
   **H8 opens, but the result is not Africa.** I followed up with a read-only look at the opened raster: GDAL 3.11.3 reads
   the store with **no georeferencing** (extent `0, 7200, 0, 2600`, resolution 1, CRS empty;
   warnings `[rast] unknown extent` and `missing [ (GDAL error 1)`). So `crop(ext(-18, 52, -35, 38))`
   is applied in pixel space and returns a 52 x 38 corner block (1976 cells = the 3952 bytes),
   not the Africa window. terra also reports `scoff` = `scale 0.1, offset 0`, so the values come back
   already scaled; the script's extra `* 0.1` applies the factor twice. The as-read mean is **12.36**, and the
   pasted `mean_scaled=1.24` is that times 0.1. Neither number says anything about Africa's Tmax. In
   short, GDAL's ZARR driver opens the cube and reads the scale factor, but does not pick up the lat/lon
   coordinate arrays, so an R/terra reader would need to set the extent and CRS itself (or a reader
   other than GDAL is needed). The double-scale is a script issue, and I have not changed the script.

**Ingest time for the 1981-2026 Africa window, at the measured aggregates**

| cube | size | x8 | x16 | single stream (H3) |
|---|---|---|---|---|
| CHIRTS Tmax + Tmin, 2 x 131 slabs, ~24 GB | 24 GB | ~22 min (17.85 MB/s) | ~15 min (27.35) | ~2.3 h (2.9) |
| CHIRPS `rnl`, 131 slabs, ~7 GB | 7 GB | ~10 min (11.82) | ~8.5 min (13.91) | ~40 min |

Compare CHC tifs at the S0 rates: the ~390 GiB global CHIRTS-ERA5 would take 6-22 h even in parallel.
From this node the cubes make the download a matter of under half an hour. Going from 8 to 16 streams
still scaled for CHIRTS (+50% on slab 98), so the CDN cap is above 27 MB/s for this client, or at least
was not reached here.


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
