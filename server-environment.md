# 🖥️ PASCAL Server – Environment and Capabilities

*Last updated: September 2025*\
*Project*: Africa Agriculture Adaptation Atlas (AAAA)\
*Prepared by*: Pete Steward ([p.steward\@cgiar.org](mailto:p.steward@cgiar.org){.email})\
*Host*: `PASCAL.CGIARAD.ORG` (192.168.213.3)\
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
| `/cluster01` | 50 T | 23 T | NFS from 10.10.10.2 | Shared project and data storage |
| `/cluster01-bkp` | 20 T | 16 T | NFS from 10.10.10.2 | Backups, not user-readable |

All volumes are rotational. `/cluster01` holds the shared `Data`, `Projects`
and `Workspace` trees and is world-writable; this is where bulk hazard and
exposure data belongs, not in `/home`. No disk quotas are enforced, so please
be considerate — `/home` is already 71% full.

## 3) Shared use and thread etiquette {#3-shared-use-and-thread-etiquette}

PASCAL has around 69 user accounts and **no batch scheduler** (no Slurm, no
PBS). Nothing stops a single job from saturating the machine, so resource
sharing is by convention.

Before launching heavy work, check the current load with `uptime` and pick a
worker count deliberately rather than defaulting to all 80 cores:

``` r
# R
future::plan(future::multisession, workers = 16)
data.table::setDTthreads(16)
terra::gdalCache(60000)  # already set in R/0_server_setup.R
```

``` bash
# Python / OpenBLAS — one careless matrix multiply will otherwise use all 80 cores
export OMP_NUM_THREADS=16 OPENBLAS_NUM_THREADS=16 MKL_NUM_THREADS=16
```

No global thread limits are imposed at the system or profile level. This is
deliberate — when the machine is idle, using it fully is the right thing to do.
Judge per job.

## 4) R environment {#4-r-environment}

R 4.5.1 is installed system-wide with roughly 1,680 packages in
`/usr/local/lib/R/site-library`, including the full geospatial and modelling
stack used by this workflow: `terra` 1.8-93, `sf` 1.0-24, `stars`, `gdalcubes`,
`exactextractr`, `ncdf4`, `data.table` 1.18.0, `tidyverse`, `tidymodels`,
`xgboost`, `ranger`, `brms`, `lme4`, `future`/`furrr` and `reticulate`.

**RStudio Server** is available on port **8787**.

The following were added to the personal library at
`~/R/x86_64-pc-linux-gnu-library/4.5` in September 2025, as they were missing
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
conda activate /home/psteward/.conda/envs/sci
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

| Service | Address |
|---------|---------|
| SSH | port 22 |
| RStudio Server | port 8787 |
| JupyterHub | via the Hub proxy |

Home directories are shared across users on `/home`; authentication is via
SSSD against the CGIAR directory.

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
