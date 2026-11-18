# DISPATCH — pascal — a per-user `obs` conda env so PASCAL can read the CDH Zarr cubes

**Append-only; newest block on top. Prepend a `### RESPONSE — pascal` block to answer.**

### RESPONSE — pascal 2026-10-24

**Session.** JupyterHub, **Large profile**: `cpu.max` = `2000000 100000` → 20 usable CPUs,
`memory.max` 96 GiB. No SSH or RStudio route. No root, no system packages, nothing under `/opt/conda`
touched, `~/.conda/envs/sci` untouched (mtime still 2026-09-15), no Jupyter kernel registered, no
`~/.Renviron` or `RETICULATE_PYTHON` change, no `~/.condarc` change.

**Headline. E4 passes on all four cases**: max |diff| is exactly the half-quantum and not one cell
exceeds it, so the cube is the CHC data, quantised. **But two things in the plan need correcting:**
GDAL 3.13.3 still reports no georeferencing, so that caveat is not a GDAL-version problem and will
not age out; and `~/.conda/envs/sci` already had everything needed except `aiohttp`, so the new env
may be redundant.

#### E1 — inspect

| item | value |
|---|---|
| `conda` | 26.7.1, from the system install. **No `mamba`, no `micromamba`.** |
| `~/.condarc` | `conda-forge` only, `channel_priority: strict`, `default_channels: []`; envs and pkgs dirs already under the user's home. Nothing to change. |
| existing envs | `sci`, `tools`, and base |
| **`~/.conda/envs/sci`** | activatable. Python 3.12.14, numpy 2.5.3, **GDAL 3.13.3**, zarr 3.3.0, xarray 2026.7.0, and `gdalinfo --formats` lists the Zarr driver. |
| home free space | 1.8 TB total, **501 GB free** (72 % used) |
| package cache | 3.6 GB |

Home has 501 GB free, far above the ~20 GB threshold in the dispatch, so the env and its cache stayed
in the home tree as configured. Nothing was put on the bulk root.

**`sci` already met the requirement.** The dispatch's premise — "the default `python3` has no `zarr`
or `numpy`" — is true of the `/opt/conda` interpreter on `PATH`, which is what the HF probe tested.
It is not true of `sci`, which carries GDAL 3.13.3, zarr 3.3.0, xarray and dask. The one thing `sci`
lacks is `aiohttp`. So the choice was really between `conda install -p ~/.conda/envs/sci aiohttp` and
a new env; the dispatch said do not modify `sci` and create a new one, so that is what I did. Worth
deciding which you want to keep — see the note at the end.

#### E2 — build

**Dry run.** `libgdal-zarr` is **not a conda-forge package** and the solve fails with
`PackagesNotFoundInChannelsError`. Dropped it, as the dispatch anticipated; the driver is in
`libgdal-core`. The solve then succeeded:

| package | version |
|---|---|
| python | 3.12.15 |
| gdal / libgdal-core | **3.13.3** |
| zarr | **3.4.1** |
| xarray | 2026.9.0 |
| numpy | 2.5.4 |
| fsspec | 2026.9.0, aiohttp 3.14.5, requests 2.34.2 |

122 packages, **110.0 MB** to download. Built env is **669 MB** at `~/.conda/envs/obs`.
`gdalinfo --formats` in it lists `Zarr -raster,multidimensional raster- (rw+uvs)`.

**Two builds failed first, and failed silently.** Both died with
`ConnectionResetError(104, 'Connection reset by peer')` while fetching packages — and **conda still
printed `done` and exited 0 while creating no environment at all**. Only an explicit
`test -x <env>/bin/python` caught it. A plain "it said done" would have been wrong twice.

The host was fine: `curl` of the exact package that reset (`proj-9.9.0-h3767062_0.conda`) returned
HTTP 200 at 2.08 MB/s in the same minute. It is conda's **parallel** fetcher that gets reset — the
same concurrency sensitivity this node showed against CHC in the S0 probe. The third build, with
`CONDA_FETCH_THREADS=1` (plus raised retries and timeouts, all as one-shot environment variables, no
config file touched), completed with **zero resets**. Worth knowing for anyone building an env here.

#### E3 — cube read

Store opens over HTTPS at the pinned revision `c44a0b5…`.

| | |
|---|---|
| variables | `Tmax`, `Tmin`, `spatial_ref` |
| dims | time 16716, lat 2600, lon 7200 |
| lat | 69.975 → −59.975, **north→south: yes** |
| lon | −179.975 → 179.975 |
| time | 1981-01-01 → 2026-10-07 |
| encoding | `scale_factor` **0.1**, `add_offset` 0.0, `_FillValue` **−32768**, dtype int16, units `degC` |

**Tmax, 2015-06-15, Africa box** (lon −18…52, lat −35…38), after xarray's decode, not scaled again:

```
shape (1460, 1400)   min 11.1000   mean 34.4925   max 48.7000
NaN 871,948 of 2,044,000 cells (ocean)
```

Mean **34.49 °C** — inside the 25-35 invariant, sitting at its top edge. That is a land-only mean for
a June day over a box reaching the Sahara, so it looks right, but the invariant has almost no room
above it and a slightly different box would breach it. Worth widening to ~25-38 if it is meant to
hold for any date.

**`chunks={}` does not work in this env**: it raises `ImportError: chunk manager 'dask' is not
available`. `dask` is not in the E2 package list. `chunks=None` works and is what I used. The same
call appears in `scripts/obs_hf_probe.sh` H7, so H7 would fail here for that reason rather than for a
missing `zarr` — worth either adding `dask` to the env recipe or changing H7.

**GDAL 3.13.3 does not fix the georeferencing.** `gdalinfo 'ZARR:"/vsicurl/<store>":/Tmax'` reports:

```
Size is 7200, 2600
Corner Coordinates:
Upper Left  (    0.0,    0.0)      Lower Right ( 7200.0, 2600.0)
Band 1 Block=300x300 Type=Int16 ... NoData Value=-32768  Unit Type: degC  Offset: 0,  Scale:0.1
```

Pixel space, no CRS — **identical to CGlabs' 3.11.3**. So `PLAN` §1 caveat (i) is not a "CGlabs is on
an older GDAL" problem; it is how the driver handles this store, and S1 must set the grid by hand on
any node and any GDAL version. It also emits `ERROR 1: missing [` four times on open, and reports
`DIM_time_VALUE=0` on *every* band, so the time axis is not usable through GDAL either.

**One trap on the time axis.** The store's time units are `days since 1980-01-01` while `time[0]` is
**1981-01-01** (raw value 366). Anyone treating the stored integer as an offset from the series start
is 366 days out. Confirmed against the plan's assumption: index **12583 = 2015-06-15**, as the script
and `PLAN` have it, and 2020-12-31 is index 14609. Selecting by `time=` label avoids the whole issue.

#### E4 — the S1 independent-reference gate

Cube vs the node's CHC GeoTIFFs, Africa box, cell by cell on the 0.05° grid:

```
case                     n_cells  max|diff|   mean_diff  NA_cube_only  NA_tif_only  verdict
Tmax 2015-06-15          1172052     0.0500     0.00003             0            0     PASS
    max|diff| exact=0.05000000000000426  cells over 0.05+1e-9: 0  |diff| q99=0.049493
Tmin 2015-06-15          1172052     0.0500     0.00004             0            0     PASS
    max|diff| exact=0.05000000000000071  cells over 0.05+1e-9: 0  |diff| q99=0.049506
Tmax 2020-12-31          1172052     0.0500     0.00003             0            0     PASS
    max|diff| exact=0.05000000000000426  cells over 0.05+1e-9: 0  |diff| q99=0.049501
Tmin 2020-12-31          1172052     0.0500    -0.00002             0            0     PASS
    max|diff| exact=0.05000000000000071  cells over 0.05+1e-9: 0  |diff| q99=0.049502
```

**Alignment is by coordinate, not index.** For each side I built the cell-centre arrays — from the
GeoTIFF's geotransform, and from the cube's `lat`/`lon` coordinate variables — selected the same
closed box by value from each, then asserted the two axes match before comparing a single cell. They
do, to `max|dlon| = 3.46e-06°` and `max|dlat| = 1.56e-06°` (about 0.4 m). That residual is the
GeoTIFF's float32 geotransform: its pixel size is stored as 0.050000000745058, not 0.05. I had set my
own tolerance at 1e-6° first and it tripped on exactly that; 1e-4° (~11 m, 1/500 of a cell) is the
honest threshold for "same grid".

**NA agrees exactly**: 1,172,052 valid cells on both sides, 871,948 NA on both, and **zero** cells NA
in one but not the other, in all four cases. The GeoTIFFs declare no NoData, so I treated values
≤ −9000 as fill (the CHIRTS −9999 convention); the cube uses −32768 → NaN.

**Verdict: pass, and the gate is working as intended.** |diff| is distributed uniformly across
[0, 0.05] with q99 ≈ 0.0495, cube values are exact multiples of 0.1, and the GeoTIFF values are
continuous float32 — which is precisely Float32 → int16 × 0.1 rounding and nothing else. Mean diff is
+0.00003 °C, i.e. no bias. The cube is the CHC data, quantised to 0.1 °C. No scale applied twice, no
flip, no offset.

**One wording fix for `PLAN` §4.** The gate says `|diff| ≤ 0.05`. The true maximum *is* 0.05, so in
float64 it lands on `0.05000000000000426` and a literal `<=` test **fails** — my first run printed
FAIL on all four cases for that reason alone, and only full-precision output showed why. This is the
"invariants, not exact figures" rule in `AGENTS.md` §1 biting at the last decimal place. Suggest
stating it as `|diff| ≤ 0.05 + 1e-9`, or "no cell differs by more than one int16 quantum (0.1 °C)".

#### Env summary

| | |
|---|---|
| path | `~/.conda/envs/obs` (per-user; `conda env remove -p ~/.conda/envs/obs` undoes it) |
| size | 669 MB, 122 packages, conda-forge only |
| key versions | python 3.12.15, GDAL 3.13.3 (Zarr driver), zarr 3.4.1, xarray 2026.9.0, numpy 2.5.4, fsspec 2026.9.0, aiohttp 3.14.5 |
| not included | `dask` — so `xr.open_zarr(..., chunks={})` fails; use `chunks=None` |
| build note | needs `CONDA_FETCH_THREADS=1` on this node |

Added to `server-environment.md` §5.

**Worth a decision before this is relied on.** `obs` and `sci` now overlap almost completely: `sci`
has GDAL 3.13.3, zarr 3.3.0, xarray and **dask**, and lacks only `aiohttp`; `obs` has `aiohttp` and
newer zarr but no `dask`. Two near-identical 600 MB+ envs is a maintenance trap, and the next person
will not know which to use. Either add `aiohttp` to `sci` and drop `obs`, or add `dask` to `obs` and
declare it the cube env. I have not done either — say which.

---

### MACBOOK 2026-10-24 — build `~/.conda/envs/obs` (no root, no system change), verify the cube read, then run the S1 reference gate on this node alone

**Why.** PASCAL cannot open the cubes: system GDAL is 3.4.3 and the default `python3` has no `zarr` or
`numpy` (`DISPATCH_obs_hf_probe.md`, pascal block items 2-3). A per-user env fixes both without
touching the system R/GDAL stack, which `server-environment.md` §8.4 says to leave alone because
rebuilding it would shadow ~1,680 site-library packages. With the env in place, PASCAL becomes a real
fallback for the cube route and can run the S1 independent-reference gate **by itself**. It has the CHC
GeoTIFF (2015-06-15 Tmax, byte-identical to CHC) on disk; the env reads the cube side.

**Rules.**
- **No root, no system packages, no edits to `/opt/conda`.**
- **Do not modify `~/.conda/envs/sci`.** Other kernels use it. Create a **new** env.
- conda-forge only, as already set in `~/.condarc` (`server-environment.md` §6).
- Everything reversible: `conda env remove -p ~/.conda/envs/obs` or `rm -rf` undoes it.
- Background anything over about a minute, with logs in `logs/pascal_obs_env_<STAMP>.log`.
- Public repo: no IPs, hostnames, mount identifiers or usernames in the RESPONSE.
- Say which session type and profile you are in.

#### E1 — inspect (no change)
- `which conda mamba micromamba`.
- Is `~/.conda/envs/sci` present and activatable? Report its `gdalinfo --version`, `python -V` and `numpy`
  version.
- Home free space (`df -h ~`). `/home` is about 71 % full; the env should be about 1-2 GB, so say
  whether the env and its package cache should go under the bulk data root instead (`conda create -p
  <bulk>/...`, `CONDA_PKGS_DIRS=<bulk>/...`). **Prefer the bulk root if home has under ~20 GB free.**

#### E2 — build (dry-run first)
```bash
conda create -p <ENV> -c conda-forge --override-channels \
  "python=3.12" "zarr>=3" xarray numpy fsspec aiohttp requests "gdal>=3.11" "libgdal-zarr" --dry-run
```
- `libgdal-zarr` may be bundled into `libgdal` on conda-forge. If the solver rejects it, drop it and
  confirm the driver with `gdalinfo --formats | grep -i zarr` after the build.
- Report the solver's GDAL / zarr / numpy versions and the download size. Then run it for real.
- Do **not** register a Jupyter kernel or touch `~/.Renviron` / `RETICULATE_PYTHON`.

#### E3 — verify the cube read (pinned revisions)
CHIRTS cube: `https://huggingface.co/datasets/aaguilar90/chirts-era5-daily/resolve/c44a0b58772e32f7cbb2b5a740d5e6586ceeb054/chirts-era5-daily.zarr`.

In the env, with Python + xarray (`zarr.storage.FsspecStore` / `xr.open_zarr(..., storage_options=...)`
over HTTPS):
- open the store and print the variables, dims, coordinate ranges and the `scale_factor` attributes;
- select **Tmax, 2015-06-15**, Africa box lon −18…52, lat −35…38, and report:
  - shape;
  - min / mean / max **after** xarray's decode, which applies the scale; do not scale again;
  - whether lat runs north→south.

  **Invariant:** mean Africa Tmax is roughly 25-35 °C.

Then GDAL 3.13 from the env: `gdalinfo 'ZARR:"/vsicurl/<store>":/Tmax'`. **Does it report a real
geotransform and CRS?** CGlabs' GDAL 3.11.3 did not; it reported extent `0,7200,0,2600` and no CRS.
Report what 3.13 does.

#### E4 — the S1 independent-reference gate (`PLAN_obs_hazard_pipeline.md` §4)
Compare the cube's Tmax for 2015-06-15 against the node's CHC GeoTIFF for the same day
(`chirts_raw/tmax/…/CHIRTS-ERA5.daily_Tmax.2015.06.15.tif`) over the Africa box, cell by cell on the
0.05° grid:
- report n_cells, max |diff| and mean diff;
- align by **coordinates**, not array index; state how;
- NA handling: report cells NA in one and not the other.

**Invariant:** max |diff| ≤ 0.05 °C (the int16 × 0.1 quantum, rounded) and NA patterns agree except
ocean.

Repeat for one more date (2020-12-31, the end of the node's mirror) and for **Tmin**.

**Stop and describe** if any |diff| > 0.05. That would mean the cube is not the CHC data, or the
alignment or scale is wrong; we need to know which.

#### Then
1. Prepend `### RESPONSE — pascal` with E1-E4 and the env's location.
2. Add an `obs` env entry to `server-environment.md` §5 (path pattern, purpose, key versions; no
   username).
3. Commit **only** this file and `server-environment.md`, then push.
4. Verify on `origin/develop`.
