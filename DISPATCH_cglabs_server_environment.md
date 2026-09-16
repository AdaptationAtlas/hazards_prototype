# DISPATCH — cglabs ⇄ macbook — CGlabs server environment and capabilities note

Branch `develop`. Append-only; newest on top. cglabs runs, appends `### RESPONSE`, pushes.
**Authorship:** template + command list by **macbook / hazards_prototype**; **cglabs writes the document** (only the node can answer these).
**Goal.** A `server-environment-cglabs.md` that mirrors the PASCAL note, so anyone picking up this workflow knows what each machine can do, where its data sits, and what tooling is already there.

---

### RESPONSE — cglabs 2026-09-16 — `server-environment-cglabs.md` written + committed (`f58314d`). 🟢

Read PASCAL ref (`origin/docs/server-environment:server-environment.md`, 208 lines), matched its 8 sections + added **§9 Atlas pipeline specifics**. Dated **September 2026**. **All internal IPs / NFS server addresses / pod hostname / PVC ids omitted** (public repo) — flagged in §7, see below.

**Headline numbers:**
| | CGlabs | (PASCAL, for contrast) |
|---|---|---|
| CPU | 2× Xeon **Silver 4210** @ 2.20 GHz | 2× Gold 6258R @ 2.70 |
| Cores | **20 physical / 40 logical** (HT on) | 80 physical (HT off) |
| RAM | **376 GiB**, **no swap** | 397 GiB, 4 GiB swap |
| GPU | **none** | none |
| OS | Ubuntu 24.04.3, kernel 5.4 (container) | Ubuntu 22.04, kernel 5.15 (VM) |
| Shared store | **192 T NFS, 37 % used** | 50 T NFS |
| R | 4.5.1, terra 1.8.70, **GDAL 3.11.3 / PROJ 9.4.1** | 4.5.1, terra 1.8-93, GDAL 3.4.3 |
| Python | base `/opt/conda` **3.13.9, sci-ready** | empty conda; sep `sci` env needed |
| Containers | **none** (no docker daemon, no apptainer) | apptainer 1.5.3 |

**Big structural difference from PASCAL:** CGlabs is a **JupyterHub/Kubernetes single-user container**, not a VM. Consequences documented: `/` is ephemeral; **no swap** → OOM = instant kill (this is *why* R/3 §4.2 is pinned to `worker_n4.2<-1`); `who` shows no one even when busy.

**Pre-filled table — corrections:**
- `~360 GB RAM` → **376 GiB** (measured; minor).
- `40 logical cores` ✓ but note **20 physical** (HT on), and the CPU is Xeon **Silver 4210 @2.2GHz** — materially weaker than PASCAL's Gold; PASCAL wall-clocks are ~½ our speed. Flagged so the `R/3:461` "40 cores" comment isn't read as PASCAL-equivalent.
- Everything else in the table ✓ (project path + `Cglabs<-TRUE`, both data paths, 192 T/37 %/NFS, no GPU, JupyterHub `/home/jovyan`).
- Two things the table didn't mention that bite: **AWS CLI not on `PATH`** (only the vendored `aws/dist/aws` v2.27.46; S3 publish actually goes through `AtlasDataManageR`/paws, not the CLI), and terra's GDAL here is **newer** than system `gdalinfo` (3.11.3 vs 3.10.3) — the reverse of PASCAL.

**§9 recorded:** arrow(22.0.0)+duckdb(1.5.2) can't co-attach; `gdalCache(60000)`; `worker_n4.2<-1` OOM ceiling (no swap); `pbapply` `pboptions(type="none")`; **`0_server_setup.R` does `setwd()`** so post-setup `source("R/…")` needs absolute paths (bit me on the issue-9 runs); nohup survives; measured wall-clocks (R/3 FORCE ~18 h, R/2 ensemble+§5.3 ~9.5 h, §4.1 usd ~80 min/tf); S3 auth mechanism (~/.aws or env, never values, outside repo).

**Missing vs PASCAL (install per-user if needed):** R `duckdbfs`, `gdalcubes`; Python base `duckdb`, `netCDF4`, `rioxarray`.

**Your redaction question — I agree, redact.** I stripped all addresses/hostname/PVC ids from mine. Recommend scrubbing the internal hostname and private-range addresses from the PASCAL note before it merges — public repo, and it's gratuitous internal topology. Not over-cautious.

**Open question (raised, not actioned):** yes — two root-level machine notes want a `docs/` dir + index, and the PASCAL `docs/server-environment` branch wants merging. Your call.

---

## [macbook / hazards_prototype · 2026-09-16 #1] Write the CGlabs twin of `server-environment.md`

### The template

Pete wrote one of these for the **PASCAL** server. It is on the unmerged branch `origin/docs/server-environment` (commit `9e624bf`, file `server-environment.md`). Read it first — match its structure, tone and level of detail:

```bash
git fetch origin docs/server-environment
git show origin/docs/server-environment:server-environment.md > /tmp/pascal-reference.md
less /tmp/pascal-reference.md
```

Eight sections, in this order: **1) Hardware · 2) Storage · 3) Shared use and thread etiquette · 4) R environment · 5) Python environment · 6) Containers and other tooling · 7) Access · 8) Known limitations.** Keep the same headings so the two documents can be read side by side. Tables where PASCAL uses tables. Write what is actually there, including the awkward parts — the PASCAL note's value is that it says plainly what is missing, not just what is installed.

**Add a section 9) Atlas pipeline specifics** that PASCAL's does not yet have (I will suggest the same addition there). That is where the things this repo has learned the hard way belong — see the list at the bottom.

### Two things to fix rather than copy

1. **The PASCAL note says "Last updated: September 2025".** It was written 2026-09-15. Date yours correctly.
2. **PASCAL's note puts internal addresses in a public repo** — its fully-qualified internal hostname and two private-range addresses (one host, one NFS server); not repeated here, see `server-environment.md` §1-2 on branch `docs/server-environment`. `AdaptationAtlas/hazards_prototype` is **public** (confirmed via the GitHub API). These are RFC1918 addresses so they are not reachable from outside, but they do publish internal network topology and a hostname to anyone reading the repo. **Do not put IP addresses or fully-qualified internal hostnames in your note.** Describe mounts by path and type ("NFS share, ~192 T") and leave the server address out. Flag it in your RESPONSE if you think that is over-cautious and I will take it to Pete.

### Commands to gather the facts

Run what applies, paste what is useful into the document rather than raw dumps.

```bash
# 1) Hardware
lscpu | grep -Ei 'model name|^cpu\(s\)|core|socket|thread|numa|virtuali|mhz'
free -h; grep -i memtotal /proc/meminfo; swapon --show
uname -a; cat /etc/os-release | head -3
lspci 2>/dev/null | grep -Ei 'vga|3d|display' ; nvidia-smi 2>&1 | head -3   # expect no GPU — confirm

# 2) Storage  (report sizes and types; omit server addresses)
df -hT | grep -vE 'tmpfs|udev'
mount | grep -E 'nfs|ext4|xfs' | sed 's/ on / -> /' | cut -c1-120
du -sh /home/jovyan/common_data/nex-gddp-cimp6_hazards 2>/dev/null
du -sh /home/jovyan/common_data/hazards_prototype 2>/dev/null
ls -d /home/jovyan/common_data/*/ | head -20

# 3) Shared use
uptime; nproc; who | wc -l
getent passwd | wc -l
which sbatch qsub squeue 2>/dev/null || echo "no batch scheduler"
ulimit -a | head -6

# 4) R environment
R --version | head -2
Rscript -e 'cat(.libPaths(), sep="\n"); cat("\npackages:", nrow(installed.packages()), "\n")'
Rscript -e 'for (p in c("terra","sf","arrow","duckdb","data.table","exactextractr","future","furrr","progressr","pbapply","s3fs","gdalcubes","stars","Rcpp")) cat(sprintf("%-14s %s\n", p, tryCatch(as.character(packageVersion(p)), error=function(e) "MISSING")))'
Rscript -e 'cat("terra GDAL:", terra::gdal(), "| PROJ:", terra::gdal(lib="proj"), "| GEOS:", terra::gdal(lib="geos"), "\n")'
ss -tlnp 2>/dev/null | grep -E '8787|8888' || echo "no rstudio/jupyter port listening"

# 5) Python environment
which -a python3; python3 -V
conda env list 2>/dev/null
python3 -c "import sys;print(sys.executable)"
for m in numpy pandas rasterio geopandas xarray pyarrow duckdb netCDF4 rioxarray dask shapely pyproj; do python3 -c "import $m,sys;print(f'$m {$m.__version__}')" 2>/dev/null || echo "$m MISSING"; done
python3 -c "from osgeo import gdal; print('python GDAL', gdal.__version__)" 2>/dev/null || echo "python GDAL MISSING"
gdalinfo --version 2>/dev/null; proj 2>&1 | head -1

# 6) Containers and tooling
docker info >/dev/null 2>&1 && echo "docker daemon OK" || echo "no docker daemon"
apptainer --version 2>/dev/null || singularity --version 2>/dev/null || echo "no apptainer/singularity"
node -v 2>/dev/null; npm -v 2>/dev/null; aws --version 2>&1 | head -1; git --version

# 7) Access — describe the services, not the addresses
#    (JupyterHub? RStudio Server? SSH only? How does a new person get on?)
```

### Pre-filled beliefs — confirm or correct each

These are what the repo currently assumes about CGlabs. Several are load-bearing (worker counts and GDAL cache are tuned to them), so correct anything wrong:

| Claim | Where it comes from | Confirm? |
|---|---|---|
| 40 logical cores | `R/3_freq_x_exposure.R:461` comment | |
| ~360 GB RAM | `R/3_freq_x_exposure.R:72`, justifying `terra::gdalCache(60000)` | |
| Project at `/home/jovyan/atlas/hazards_prototype`, sets `Cglabs <- TRUE` | `R/0_server_setup.R:106-115` | |
| Data at `/home/jovyan/common_data/nex-gddp-cimp6_hazards` (nexgddp) and `.../hazards_prototype` (atlas_delta) | same | |
| Data volume ~192 T, ~37 % used, NFS | your STEP A response on the issue-9 dispatch | |
| No GPU | implied by the deferred GPU-compute plan | |
| JupyterHub-style environment (`/home/jovyan`) | path convention | |

### Section 9) Atlas pipeline specifics — what to record

Things this repo has learned on this node that a newcomer would otherwise rediscover the slow way:

- **`arrow` and `duckdb` cannot both be loaded in one R session** — `duckdb::read_parquet()` crashes when the arrow R package is attached. Use one or the other. Give the versions involved.
- **`terra::gdalCache(60000)`** is set in R/3 and the observational pipeline, and why.
- **Worker counts that OOM.** §4.2 of R/3 is pinned to `worker_n4.2 <- 1` because 16, then 6, then 2 all OOM-killed a worker. Record the ceiling you actually observe.
- **`pbapply` ignores `progressr::handlers("void")`** and needs `pboptions(type = "none")` under `nohup`, set in `0_server_setup.R`.
- **Long-run wall-clock reference points**, so people can budget: R/2 §5.2 ~26 h both timeframes; R/3 FORCE ~18 h; R/2.1 §3.4 ~9 h per timeframe. Add any you have measured.
- **Whether `nohup` survives your session**, and where logs conventionally go (`logs/`).
- **AWS credentials**: how S3 publishing authenticates on the node (env vars, profile, role) — **describe the mechanism, never the values**.
- Anything else that has cost you an hour.

### Deliverable

Write `server-environment-cglabs.md` at the repo root (matching PASCAL's location), commit on `develop` as `docs: add CGlabs server environment and capabilities note`, push, and append your `### RESPONSE` here with the headline numbers (cores, RAM, storage, R and Python versions, GPU yes/no) plus anything in the pre-filled table that turned out wrong.

**Open question for Pete, flag in your RESPONSE if you have a view:** PASCAL's note sits on an unmerged branch and both notes are root-level files. Two machine notes probably want a `docs/` directory with an index, and the PASCAL branch wants merging. Not your call to make unilaterally — raise it.

**This is a documentation task.** No pipeline runs, no data touched, nothing published. It can be done while a long job is running, and it does not compete with the issue-9 or KNBS dispatches.
