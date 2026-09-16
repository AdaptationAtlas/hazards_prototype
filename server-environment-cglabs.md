# 🖥️ CGlabs Server – Environment and Capabilities

*Last updated: September 2026*\
*Project*: Africa Agriculture Adaptation Atlas (AAAA)\
*Prepared by*: Pete Steward ([p.steward@cgiar.org](mailto:p.steward@cgiar.org)), documented on-node by the cglabs session\
*Host*: CGlabs JupyterHub (single-user container; internal address omitted — public repo)\
*Repository*: <https://github.com/AdaptationAtlas/hazards_prototype>

This note records the hardware, storage and software available on the **CGlabs**
server, so that anyone running the `hazards_prototype` workflow knows what the
machine can do, where the data sits, and which tooling is already in place. It is
the companion to the PASCAL note (`server-environment.md`) and to the setup
performed in [R/0_server_setup.R](R/0_server_setup.R), which sets `Cglabs <- TRUE`
on this host.

> **Note on this machine vs PASCAL.** CGlabs is a **JupyterHub / Kubernetes
> single-user container**, not a bare VM. It is a smaller box than PASCAL
> (40 logical cores vs 80, Xeon Silver vs Gold) but sits on a far larger shared
> NFS store (~192 T vs 50 T). Numbers below differ from PASCAL's — do not assume
> parity.

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
-   [9) Atlas pipeline specifics](#9-atlas-pipeline-specifics)

------------------------------------------------------------------------

## 1) Hardware {#1-hardware}

| Item | Value |
|------|-------|
| CPU | 2 × Intel Xeon Silver 4210 @ 2.20 GHz |
| Cores | 20 physical (10 per socket), hyperthreading **on** → **40 logical** |
| NUMA | 2 nodes — node0 = even cores 0–38, node1 = odd cores 1–39 |
| RAM | 376 GiB total (≈ 328 GiB available at rest) |
| Swap | **None** (0 B) |
| OS | Ubuntu 24.04.3 LTS |
| Kernel | 5.4.0 (host kernel; this is a container) |
| Virtualisation | Kubernetes / JupyterHub single-user container (VT-x host) |

There is **no GPU**: no PCI display device is visible to the container, `nvidia-smi`
is not installed, and no CUDA toolkit is present. Any GPU work must run elsewhere.

Two consequences of the container model matter in practice. First, **there is no
swap** — an out-of-memory job is killed outright, it does not slow down and
recover, so worker counts must respect the 376 GiB ceiling (see §3 and §9).
Second, `nproc` and the cgroup may report the full 40 even under heavy
contention; check real load with `uptime` rather than trusting the core count.

CGlabs' strength is the same shape as PASCAL's — high core count and high memory
— but at roughly half the scale. Budget accordingly: a job that took *N* hours on
PASCAL's 80 cores will take longer here.

## 2) Storage {#2-storage}

| Mount | Size | Free | Type | Notes |
|-------|------|------|------|-------|
| `/` | 916 G | 709 G | overlay | Container root, **ephemeral** — do not store anything here |
| `/home/jovyan` | 30 T | 22 T | NFS | Per-user home (Hub PVC). The repo lives here at `~/atlas/hazards_prototype` |
| `/home/jovyan/common_data` | 192 T | 123 T | NFS | **Shared** project + data store (37 % used). All bulk hazard/exposure data belongs here |
| `/home/jovyan/file-manager` · `/examples` · `/shared-data-premium` | 192 T | 123 T | NFS | Other views onto the same shared NFS share |

Server addresses are deliberately omitted (public repo — see §7 flag). The two
NFS servers are distinct: the home PVC and the shared-data store are on different
hosts.

**Where the data actually is** (both under `common_data`, ~1.4 T combined):

| Path | Size | Setup key |
|------|------|-----------|
| `common_data/nex-gddp-cimp6_hazards` | 637 G | `nexgddp` working_dir (issue-9 / R/2 / R/3) |
| `common_data/hazards_prototype` | 759 G | `atlas_delta` / legacy working_dir |

`common_data` also holds dozens of sibling Atlas datasets (`atlas_boundaries`,
`atlas_cropSuite`, `AgERA5_*`, …). It is shared and world-writable; no quotas are
enforced, so be considerate — the container root (`/`) is ephemeral and must not
be used for outputs.

## 3) Shared use and thread etiquette {#3-shared-use-and-thread-etiquette}

CGlabs is a shared JupyterHub deployment (~25 accounts visible from inside the
container) with **no batch scheduler** (no Slurm, no PBS). Nothing stops a job
from saturating the container's 40 cores, so sharing is by convention. Because
there is **no swap**, an over-subscribed job is OOM-killed, not throttled.

Check load before launching heavy work and pick worker counts deliberately:

``` r
# R
future::plan(future::multisession, workers = 16)
data.table::setDTthreads(16)
terra::gdalCache(60000)   # already set in R/0_server_setup.R
```

``` bash
# Python / OpenBLAS — cap threads or one matrix op will grab all 40 cores
export OMP_NUM_THREADS=16 OPENBLAS_NUM_THREADS=16 MKL_NUM_THREADS=16
```

Observed steady-state load average on this box is high (~30) even with no
interactive users, because long Atlas jobs run under `nohup`. Treat `uptime`'s
number as the baseline and add your own load on top of it — do not assume the
machine is idle just because `who` shows no one logged in (JupyterHub sessions
do not appear in `who`).

## 4) R environment {#4-r-environment}

R **4.5.1** is installed, with ~746 packages across the site library
(`/usr/local/lib/R/site-library`, `/usr/lib/R/site-library`) and a personal
library at `~/R/x86_64-pc-linux-gnu-library/4.5`. The geospatial + parquet stack
this workflow needs is present:

| Package | Version | | Package | Version |
|---------|---------|-|---------|---------|
| terra | 1.8.70 | | arrow | 22.0.0 |
| sf | 1.0.21 | | duckdb | 1.5.2 |
| data.table | 1.17.8 | | exactextractr | 0.10.0 |
| future | 1.67.0 | | furrr | 0.3.1 |
| progressr | 0.17.0 | | pbapply | 1.7.4 |
| stars | 0.6.8 | | Rcpp | 1.1.1.1.1 |
| tidyverse | 2.0.0 | | | |

**Missing** (present on PASCAL, absent here — install per-user if needed):
`duckdbfs`, `gdalcubes`.

`terra` links a **modern** GDAL stack on this node — **GDAL 3.11.3 / PROJ 9.4.1 /
GEOS 3.12.2** — *newer* than the system `gdalinfo` (3.10.3, see §5). This is the
opposite of PASCAL, where `terra` was pinned to an old system GDAL 3.4.3. On
CGlabs the R side is already current; no need to reach for Python just for GDAL
version.

> **arrow + duckdb caution** — see §9. Both are installed (arrow 22.0.0, duckdb
> 1.5.2) but must not be attached in the same R session.

No RStudio Server port was found listening (`8787`/`8888` not visible from the
container); R is used via `Rscript` / the Jupyter R kernel, not RStudio.

## 5) Python environment {#5-python-environment}

Unlike PASCAL — whose default conda ships empty — the CGlabs **base** conda
environment (`/opt/conda`, **Python 3.13.9**) is analysis-ready out of the box:

| Package | Version | | Package | Version |
|---------|---------|-|---------|---------|
| numpy | 2.3.4 | | rasterio | 1.4.3 |
| pandas | 2.3.3 | | geopandas | 1.1.4 |
| xarray | 2025.10.1 | | shapely | 2.1.2 |
| pyarrow | 22.0.0 | | pyproj | 3.7.2 |
| dask | 2025.10.0 | | GDAL (osgeo) | 3.10.3 |

`python3` on `PATH` = `/opt/conda/bin/python3`. System `gdalinfo` = **GDAL 3.10.3**,
**PROJ 9.7.0**.

**Missing from base** (install with `pip`/`conda` if a script needs them):
`duckdb`, `netCDF4`, `rioxarray`. The Atlas Python ingest scripts
(`python/ingest_*.py`) run against this base env as-is.

`RETICULATE_PYTHON` is **unset** and there is no `~/.Renviron` pointing at a
Python — if you use `reticulate`, set it explicitly to `/opt/conda/bin/python3`.

## 6) Containers and other tooling {#6-containers-and-other-tooling}

| Tool | State |
|------|-------|
| Docker | **No daemon** (cannot run containers) |
| apptainer / singularity | **Not installed** (PASCAL has apptainer; CGlabs does not) |
| Node.js | v24.9.0, npm 11.6.0 |
| git | 2.43.0 |
| AWS CLI | **Not on `PATH`.** A working copy is vendored in the repo at `aws/dist/aws` (v2.27.46); invoke by full path, or add `aws/dist` to `PATH`. S3 publishing from R does not use the CLI (see §9). |

There is **no container runtime at all** on CGlabs — neither a Docker daemon nor
apptainer — so anything requiring a container image must run on another host.

## 7) Access {#7-access}

CGlabs is reached through **JupyterHub** (browser); each user gets a single-user
container with home at `/home/jovyan`. There is no direct SSH to the container and
no RStudio Server. Work is done in JupyterLab terminals / notebooks and via
`Rscript`. Long jobs run under `nohup` and survive the terminal being closed
(see §9).

> **⚠️ Flag for Pete (redaction).** Following the dispatch instruction, I have
> **omitted all internal IP addresses, the NFS server addresses, the pod
> hostname and the PVC identifiers** from this note, because
> `AdaptationAtlas/hazards_prototype` is a **public** repository. The PASCAL note
> on branch `docs/server-environment` still carries its fully-qualified internal
> hostname and two private-range NFS/host addresses (deliberately not repeated
> here); they are RFC1918 and not externally reachable, but they do expose
> internal topology.
> Recommend scrubbing them from the PASCAL note too before merging. If you
> judge this over-cautious, say so and I'll restore the addresses here.

## 8) Known limitations {#8-known-limitations}

1.  **No GPU**, no CUDA.
2.  **No swap.** OOM = instant kill, no graceful degradation — the binding
    constraint on worker counts (§3, §9).
3.  **No container runtime** — no Docker daemon and no apptainer/singularity.
4.  **No batch scheduler** — no queuing or fair-share; etiquette only.
5.  **Half of PASCAL's compute** — 40 logical cores / 376 GiB vs 80 / 397 GiB.
    Wall-clock budgets from PASCAL do not transfer; expect longer.
6.  **AWS CLI not on `PATH`** (vendored in-repo only); `duckdbfs` and `gdalcubes`
    absent from R; `duckdb`/`netCDF4`/`rioxarray` absent from base Python.
7.  **Container root `/` is ephemeral** — never write outputs there; use
    `~/atlas` or `common_data`.

## 9) Atlas pipeline specifics {#9-atlas-pipeline-specifics}

Things this repo has learned on CGlabs that a newcomer would otherwise
rediscover the slow way.

- **`arrow` and `duckdb` cannot both be attached in one R session.** With
  `arrow` 22.0.0 and `duckdb` 1.5.2 present here, attaching `arrow` and then
  calling DuckDB parquet reads crashes the R session. Use **one or the other**
  per process — the pipeline reads parquet with DuckDB *or* arrow, never both.

- **`terra::gdalCache(60000)`** (60 GB GDAL block cache) is set in
  `R/0_server_setup.R` and the observational pipeline. It trades RAM for far
  fewer re-reads on the many-layer `_int` stacks; safe here given 376 GiB, but it
  is part of the memory budget — see the OOM note below.

- **Worker counts that OOM.** R/3 §4.2 is pinned to `worker_n4.2 <- 1`
  (sequential) — 16, then 6, then 2 workers each OOM-killed on this box. With
  **no swap**, the sequential floor is the safe choice; observed peak RSS for the
  single R/3 process this session was ~1.8 GiB steady but spikes during parquet
  pushdown. Do **not** raise §4.2 workers on CGlabs.

- **`pbapply` ignores `progressr::handlers("void")`** and spams progress under
  `nohup`; `pboptions(type = "none")` is set in `0_server_setup.R` to silence it
  in detached runs.

- **`nohup` survives the session.** `nohup Rscript … &` detaches cleanly and runs
  to completion after the launching terminal/tool call returns (verified on
  multi-hour runs this session). Convention: logs go in `logs/`, one
  `…_<STAMP>.log` + `…_<STAMP>.pid` per run.

- **`0_server_setup.R` does `setwd(working_dir)`** (→ `common_data/nex-gddp-…`).
  So `source("R/…")` **after** setup fails (repo-relative path resolves against
  `common_data`). Source repo scripts by **absolute path** in one-liners, e.g.
  `Rscript -e "source('/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R'); source('/home/jovyan/atlas/hazards_prototype/R/3_freq_x_exposure.R')"`.

- **Wall-clock reference points** (measured on CGlabs):
  | Stage | Time |
  |-------|------|
  | R/3 `FORCE_OVERWRITE=1` (both timeframes) | ~18 h |
  | R/2 ensemble-only §5.2 rebuild + §5.3 (both tf) | ~9.5 h |
  | R/3 §4.1 per exposure variable (crop×usd, aligned) | ~80 min / timeframe |
  | R/2 §5.2 full FORCE (both tf) | ~26 h (per repo history) |

  Budget generously — these are ~2× the equivalent PASCAL numbers.

- **S3 publishing auth.** Publishing runs through `AtlasDataManageR::S3DirUploader`
  (paws/R), **not** the AWS CLI. It authenticates from `~/.aws/credentials` **or**
  `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` in the environment
  (`R/observational/6_publish_obs_to_s3.R` checks both). Credentials live **outside
  the repo** and must never be committed — mechanism only is recorded here, never
  values.

------------------------------------------------------------------------

*Open question for Pete:* PASCAL's note sits on the unmerged branch
`origin/docs/server-environment` and both notes are root-level files. Two machine
notes probably want a `docs/` directory with an index, and the PASCAL branch
wants merging. Raised, not actioned — your call.
