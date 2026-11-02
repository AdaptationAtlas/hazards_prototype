# 🖥️ PASCAL Server – Environment and Capabilities

*Last updated: September 2025; host facts from the 2026-10 observed-pipeline dispatches folded in 2026-11-02 (#58)*\
*Project*: Africa Agriculture Adaptation Atlas (AAAA)\
*Prepared by*: Pete Steward ([p.steward\@cgiar.org](mailto:p.steward@cgiar.org){.email})\
*Host*: `PASCAL` (internal CGIAR network; address withheld — public repo)\
*Repository*: <https://github.com/AdaptationAtlas/hazards_prototype>

This note records the hardware, storage and software available on the PASCAL
server, so that anyone running the `hazards_prototype` workflow knows what the
machine can do, where the data sits, and which tooling is already in place. It
is a companion to [haz-processing-notes.md](haz-processing-notes.md) and to the
setup performed in [R/0_server_setup.R](R/0_server_setup.R).

------------------------------------------------------------------------

## Table of Contents

-   [1) Hardware](#1-hardware)
-   [2) Storage](#2-storage)
-   [3) Shared use and thread etiquette](#3-shared-use-and-thread-etiquette)
-   [4) R environment](#4-r-environment)
-   [5) Python environment](#5-python-environment)
-   [6) Containers and other tooling](#6-containers-and-other-tooling)
-   [7) Access](#7-access)
-   [8) Known limitations](#8-known-limitations)
-   [9) Operating facts for dispatches](#9-operating-facts-for-dispatches)

------------------------------------------------------------------------

## 1) Hardware {#1-hardware}

| Item | Value |
|------|-------|
| CPU | 2 × Intel Xeon Gold 6258R @ 2.70 GHz |
| Cores | 80 physical (40 per socket, hyperthreading disabled) |
| NUMA | 2 nodes — cores 0–39 and 40–79 |
| Cache | 320 MiB L2, 32 MiB L3 |
| RAM | 397 GiB |
| Swap | 4 GiB (file-backed) |
| OS | Ubuntu 22.04.5 LTS, kernel 5.15 |
| Virtualisation | KVM guest |

There is **no GPU**. The only display device is a QEMU virtual VGA adapter, and
neither the NVIDIA driver nor the CUDA toolkit is installed. Any workflow
requiring GPU acceleration must run elsewhere. Note that backup directories on
the shared storage refer to a sibling host named `ampere`, which may be the
GPU-equipped machine; confirm with IT before planning GPU work.

The practical strength of this machine is a large core count combined with a
very high memory ceiling. Datasets that would normally force out-of-core or
tiled strategies can often be held entirely in RAM.

## 2) Storage {#2-storage}

| Mount | Size | Free | Type | Notes |
|-------|------|------|------|-------|
| `/` | 118 G | 56 G | ext4 (LVM) | System only |
| `/home` | 1.8 T | 516 G | ext4 (LVM) | All user home directories |
| `/cluster01` | 50 T | 23 T | NFS (internal) | Shared project and data storage |
| `/cluster01-bkp` | 20 T | 16 T | NFS (internal) | Backups, not user-readable |

All volumes are rotational. `/cluster01` holds the shared `Data`, `Projects`
and `Workspace` trees; this is where bulk hazard and exposure data belongs, not
in `/home`. No disk quotas are enforced, so please be considerate — `/home` is
already 71% full.

**Write under `/cluster01/Workspace/atlas/`, not at the top of `Workspace`.**
The top-level `Workspace` directory is not writable by an ordinary account
(`Permission denied`, 2026-10-27). The account-writable area one level down is
where `working_dir` already sits (`metadata/hosts.json`), and where the observed
pipeline's smoke and FPAR roots were put. Any new output root on this host goes
under it.

## 3) Shared use and thread etiquette {#3-shared-use-and-thread-etiquette}

PASCAL has around 69 user accounts and **no batch scheduler** (no Slurm, no
PBS). Nothing stops a single job from saturating the machine, so resource
sharing is by convention.

**Your effective limit depends on how you connected.** A JupyterHub session
runs inside a systemd cgroup and is capped by the profile chosen at spawn
time, regardless of what `nproc` reports:

| Profile | vCPU | RAM | `cpu.max` | `memory.max` |
|---------|------|-----|-----------|--------------|
| Small | 8 | 32 GiB | `800000 100000` | 34359738368 (verified 2026-10-27/29) |
| Medium | 16 | 64 GiB | not yet observed | |
| Large | 20 | 96 GiB | `2000000 100000` | 96 GiB (verified 2026-10-24) |

**Pete starts the hub session, so the profile is fixed before a node agent sees
the dispatch.** Every PASCAL dispatch therefore states the profile it needs on
its first line, and asks the node to report `cpu.max` and `memory.max`. A
heavy dispatch should ask for Large up front: Small (32 GiB) has already forced
one ingest to be re-chunked, and caps parallel R jobs at about two.

Inside such a session `nproc` still reports 80 and CPU affinity still shows
0–79, but `cpu.max` throttles actual throughput to the profile value. Check
what you really have with:

``` bash
cat /sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)/cpu.max
```

A first value of `800000` against a period of `100000` means 8 CPUs.

RStudio Server and plain SSH sessions are **not** capped, so those are the
routes to the full 80 cores and 397 GiB.

Before launching heavy work, check the current load with `uptime` and pick a
worker count deliberately rather than defaulting to all 80 cores:

``` r
# R
future::plan(future::multisession, workers = 8)   # raise only outside JupyterHub
data.table::setDTthreads(8)
terra::gdalCache(60000)  # already set in R/0_server_setup.R
```

``` bash
# Python / OpenBLAS — one careless matrix multiply will otherwise use all 80 cores
export OMP_NUM_THREADS=8 OPENBLAS_NUM_THREADS=8 MKL_NUM_THREADS=8
```

No global thread limits are imposed at the system or profile level. This is
deliberate — when the machine is idle, using it fully is the right thing to do.
Judge per job.

## 4) R environment {#4-r-environment}

> **Found 2026-10-28 (R was 4.5.1 on 2026-10-24): the system R was upgraded 4.5.1 → 4.6.1 underneath the site
> library, and much of the R stack no longer loads.** About 1,650 packages in
> `/usr/local/lib/R/site-library` were compiled for 4.5. Any of them whose
> compiled code uses the R internal `SETLENGTH` fails to load with
> `undefined symbol: SETLENGTH`. Confirmed broken: `data.table` 1.18.0, and
> `rlang`, which takes most of the tidyverse (`dplyr` …) with it. Confirmed
> loading: `terra` 1.8-93 and `sf` 1.0-24. The site library is not writable
> without root.
>
> - **IT ticket:** Pete owns it. The proper fix is a node-wide rebuild of
>   the site library.
> - **Authorised workaround:** a per-user rebuild into `R_LIBS_USER`
>   (`~/R/x86_64-pc-linux-gnu-library/4.6`), which R searches before the site
>   library. Install with
>   `install.packages("<pkg>", lib = Sys.getenv("R_LIBS_USER"))`. It is
>   account-scoped and reversible (`rm -rf` the directory).
> - **Rebuilt so far:** `data.table` 1.18.6.1 only (2026-10-28). Nothing else.
> - **Older per-user installs:** the September 2025 installs listed below sit in
>   the **4.5** per-user library, which R 4.6 does not search. Treat them as
>   absent until the issue #58 A3(a) audit reports otherwise.
>
> Until that audit (`DISPATCH_pascal_issue29_followups.md`) reports, assume that
> any pipeline R script touching `data.table` or the tidyverse will halt here.

The inventory below dates from September 2025 (R 4.5.1). R 4.6.1 is now
installed system-wide with roughly 1,650 packages in
`/usr/local/lib/R/site-library` built for 4.5, including the geospatial and
modelling stack used by this workflow: `terra` 1.8-93, `sf` 1.0-24, `stars`,
`gdalcubes`, `exactextractr`, `ncdf4`, `data.table` 1.18.0, `tidyverse`,
`tidymodels`, `xgboost`, `ranger`, `brms`, `lme4`, `future`/`furrr` and
`reticulate`. See the note above for which of these still load.

**RStudio Server** is available on port **8787**.

The following were added to the personal library at
`~/R/x86_64-pc-linux-gnu-library/4.5` in September 2025 (the 4.5 library, which R 4.6 no longer searches), as they were missing
from the site library and are useful for the parquet-based outputs this
workflow produces:

| Package | Version | Notes |
|---------|---------|-------|
| `arrow` | 25.0.1 | Parquet, Dataset, Acero, JSON, S3 and GCS support all enabled |
| `duckdb` | 1.5.5 | Query parquet directly without loading into memory |
| `duckdbfs` | 0.1.2 | Remote and S3 parquet via DuckDB |
| `RPostgres` | 1.4.10 | Built against system libpq 14.24 |

DuckDB extensions are cached in `~/.duckdb`, so `httpfs` does not re-download
each session.

These are per-user installations. Other users will need to repeat them.

## 5) Python environment {#5-python-environment}

The Python available on `PATH` by default comes from `/opt/conda` and carries
**no scientific packages at all** — no numpy, pandas, rasterio or geopandas. It
is not usable for analysis as shipped. The bare system interpreter is
`/usr/bin/python3.10`.

A working environment was therefore built at `~/.conda/envs/sci`:

``` bash
conda activate ~/.conda/envs/sci
```

| Package | Version | | Package | Version |
|---------|---------|-|---------|---------|
| python | 3.12 | | rasterio | 1.5.1 |
| numpy | 2.5.3 | | geopandas | 1.1.4 |
| pandas | 3.0.5 | | shapely | 2.1.2 |
| scipy | 1.18.1 | | pyproj | 3.8.0 |
| scikit-learn | 1.9.1 | | rioxarray | 0.23.0 |
| statsmodels | 0.15.0 | | xarray | 2026.7.0 |
| matplotlib | 3.11.1 | | zarr | 3.3.0 |
| dask | 2026.8.0 | | netCDF4 | 1.7.4 |
| pyarrow | 25.0.0 | | duckdb | 1.5.5 |
| polars | 1.44.2 | | jupyterlab | 4.6.3 |

Python 3.12 was chosen rather than the 3.14 that conda defaults to, because
several geospatial packages do not yet build against 3.14.

Notably this environment carries **GDAL 3.13.3 and PROJ 9.8.1**, against the
system GDAL 3.4.3 and PROJ 8.2.1 that R's `terra` and `sf` are linked to. For
work involving cloud-optimised GeoTIFFs, Zarr or remote raster access, the
Python side is substantially more capable.

`reticulate` is pointed at this environment via `RETICULATE_PYTHON` in
`~/.Renviron`, so `import()` from R resolves to it.

**JupyterHub** is running, and the environment is registered as the kernel
`Python 3.12 (sci)`. Restart your Hub server to see it.

### The `obs` environment — reading the CDH Zarr cubes

A second, deliberately small environment was built at `~/.conda/envs/obs` so the node can read the
CDH Hugging Face Zarr v3 cubes of CHIRTS-ERA5 and CHIRPS v3 (`DISPATCH_pascal_obs_env.md`). The
system GDAL that R's `terra` links to is 3.4.3 and cannot open them, and the `python3` on `PATH`
from `/opt/conda` carries no scientific packages at all.

``` bash
conda activate ~/.conda/envs/obs
```

| Package | Version | | Package | Version |
|---------|---------|-|---------|---------|
| python | 3.12.15 | | fsspec | 2026.9.0 |
| gdal / libgdal-core | 3.13.3 | | aiohttp | 3.14.5 |
| zarr | 3.4.1 | | requests | 2.34.2 |
| xarray | 2026.9.0 | | numpy | 2.5.4 |

669 MB, 122 packages, conda-forge only. Removable with
`conda env remove -p ~/.conda/envs/obs`.

Three things to know before using it:

- **No `dask`.** `xr.open_zarr(url, chunks={})` raises `ImportError: chunk manager 'dask' is not
  available`; use `chunks=None`.
- **`libgdal-zarr` is not a conda-forge package.** The Zarr driver ships inside `libgdal-core`;
  confirm with `gdalinfo --formats | grep -i zarr`.
- **Build conda environments on this node with `CONDA_FETCH_THREADS=1`.** conda's parallel
  package fetcher gets `ConnectionResetError(104)` here, and on failure it still prints `done` and
  exits 0 **having created no environment** — always check `test -x <env>/bin/python` afterwards. A
  single fetch thread completed the same build with zero resets, while a plain `curl` of the package
  that had just reset ran at 2 MB/s.

Both nodes now carry an env named `obs` (python 3.12, GDAL 3.13, rasterio,
xarray, zarr). On CGlabs it is run through the `pyobs` wrapper
(`server-environment-cglabs.md` §9); here it is activated directly.

This env overlaps `sci` heavily: `sci` already carries GDAL 3.13.3, zarr, xarray and `dask`, and
lacks only `aiohttp`. Decide which of the two is the cube environment rather than growing both.

## 6) Containers and other tooling {#6-containers-and-other-tooling}

There is **no usable Docker**. The CLI is present but the daemon socket does
not exist, and ordinary users are not in the `docker` group.

`apptainer` 1.5.3 is installed at `~/.conda/envs/tools` and symlinked into
`~/.local/bin`. It runs rootless and can pull Docker images directly:

``` bash
apptainer exec docker://ghcr.io/osgeo/gdal:ubuntu-small-latest gdalinfo --version
```

This covers most container needs, though there is no daemon and no
`docker compose`.

Node.js was upgraded for the same user from the system's end-of-life v12.22.9
to v26.8.2 (npm 11.19.1), again via `~/.local/bin` symlinks.

Conda is configured in `~/.condarc` to use **conda-forge only** with strict
channel priority, with `default_channels` emptied so that the Anaconda
`repo.anaconda.com` terms-of-service gate is not triggered. Environments and
package caches are kept under the user's home.

## 7) Access {#7-access}

| Service | Port | Authentication | Resource cap |
|---------|------|----------------|--------------|
| JupyterHub | 8000 | CGIAR directory (LDAP) | Yes — per spawn profile |
| RStudio Server | 8787 | Local account | None |
| SSH | 22 | Local account | None |

JupyterHub is published to an external address and is the normal way in; the
other two are reachable from the office network only.

Home directories all live on `/home`. Authentication differs by route.
**JupyterHub authenticates against the CGIAR directory over LDAP**, so the hub
login is your normal CGIAR username and password, and the hub creates a
matching local Unix account and home directory on first login. **SSH and
RStudio Server authenticate against the local account** in `/etc/passwd`
instead, whose password is set per machine and is not the directory one. This
catches people out: the credentials that work in the browser will not
necessarily work over SSH. All 65 users sit in the 1000–1064 uid range.

A sibling host (**`ampere`**, address withheld) sits on the same subnet
and runs the same stack — Ubuntu 22.04, OpenSSH 8.9p1, RStudio Server on 8787.
It runs its own JupyterHub instance. Local SSH credentials from PASCAL do not
work on it, so access must be requested from IT separately. Whether AMPERE has
a GPU is unconfirmed at the time of writing; what is confirmed is that PASCAL's
own hub configuration defines **no GPU profile of any kind**.

> **Note on addresses.** This repository is public, so internal IP addresses and
> fully-qualified internal hostnames are deliberately omitted throughout. Describe
> mounts by path, size and type; keep addresses in internal documentation only.

## 8) Known limitations {#8-known-limitations}

These cannot be resolved without root access, and should be raised with IT if
they become blocking:

1.  **No GPU**, and no CUDA toolkit.
2.  **No Docker daemon**; `apptainer` is the workaround.
3.  **No batch scheduler**, so no queuing or fair-share enforcement.
4.  **System GDAL 3.4.3 / PROJ 8.2.1** are old, and R's `terra` and `sf` are
    linked against them. Rebuilding the R geospatial stack in conda would
    shadow the ~1,680 packages already in the site library, so this has been
    left alone. Use the Python environment where a newer GDAL matters.
5.  **`fuse2fs` is absent**, so `apptainer` cannot mount EXT3-format SIF
    images. Normal `docker://` and SIF usage is unaffected.
6.  **The default `/opt/conda` Python is unusable** for analysis, and its
    package directory is root-owned. Per-user environments are required.
7.  **R 4.6.1 over a 4.5-built site library** (found 2026-10-28). See §4. IT's to fix;
    per-user `R_LIBS_USER` rebuilds are the stop-gap.
8.  **No AWS credentials and no S3 client.** There is no `s3fs`, `paws` or
    `boto3`, and no credentials file or `AWS_*` variables (2026-10-30). Atlas S3
    is readable over plain HTTPS. Nothing can be written to it from here.

## 9) Operating facts for dispatches {#9-operating-facts-for-dispatches}

Learned from the 2026-10 observed-pipeline dispatches
(`DISPATCH_pascal_obs_*.md`, `DISPATCH_cglabs_obs_*.md`). They affect how
dispatches to this node are written.

-   **State the profile at the top** (§3). Ask for `git log --oneline -1`,
    the branch, `cpu.max` and `memory.max` in every RESPONSE.
-   **Reads from S3 go over HTTPS; publishing goes through CGlabs.** Public
    `s3://digital-atlas/...` objects are fetched as
    `https://digital-atlas.s3.amazonaws.com/<key>`. `R/00_acquire.R` does this
    with base R since #58, so no S3 package is needed. Anonymous LIST works on
    the `domain=…`, `GLPS/`, `sos/` and similar prefixes, but not on every
    prefix.
-   **"Sync = regenerate, not transfer" holds in practice.** CGlabs
    re-ingested the ASAP FPAR record independently, and all 762 files were
    byte-identical to PASCAL's copy (md5, 2026-11-02).
-   **Download rates differ a lot by host and origin.** Plan wall time from the
    origin, not from the core count:

    | origin | PASCAL | CGlabs |
    |---|---|---|
    | JRC ASAP | ~0.48 MB/s (29.5 h for 756 dekads) | ~15× faster (1.9 h) |
    | Hugging Face (Zarr cubes) | ~2 MB/s single stream | — |

    Hugging Face returns HTTP 429 on bulk pixel reads (seen on CGlabs), so
    keep concurrency low and retry with backoff.
-   **Node sessions go idle after each RESPONSE.** Pete relays every prompt by
    hand, so a dispatch must be complete in one read and say where to stop.
-   **A shell `wait` on a `nohup`/`setsid` child never fires.** Long jobs are
    launched with `nohup … &`, writing to `logs/<name>_<STAMP>.log` plus a
    `.pid` file, and tracked with
    `while kill -0 $(cat x.pid) 2>/dev/null; do sleep 60; done` or by tailing
    the log.
-   **`project_dir` in `~/.Renviron` overrides the shell.** Use
    `ATLAS_PROJECT_DIR` (`metadata/hosts.json` notes).
