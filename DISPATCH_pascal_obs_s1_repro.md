# DISPATCH — pascal — reproduce the S1 Kenya smoke independently (cross-node reproducibility gate)

**Append-only; newest block on top. Prepend `### RESPONSE — pascal` to answer.**

### MACBOOK 2026-10-27 — same code, same pinned sources, a different machine: do the bytes match?

**Why.** CGlabs is running `DISPATCH_cglabs_obs_s1_ingest.md`. PASCAL has the `obs` env (GDAL 3.13.3,
zarr 3.4.1) and CGlabs a different GDAL. If both produce the same per-file md5 for the Kenya smoke, S1 is
bit-reproducible across nodes and the manifests are a usable provenance record. If they differ, we need
to know whether it is rounding in the AgERA5 area-average (benign, bounded) or something else.

**Rules.** Writes only under a new smoke root on the bulk data root. Read-only otherwise. Background with
logs in `logs/obs_s1_repro_<STAMP>.log`. Large profile or SSH; say which. No usernames, hostnames, IPs.

#### P1 — env + selftest
```bash
cd <repo> && git pull --ff-only
PY=~/.conda/envs/obs/bin/python                 # the env from DISPATCH_pascal_obs_env.md
$PY -c "import rasterio, zarr, xarray; print(rasterio.__gdal_version__, zarr.__version__, xarray.__version__)"
$PY -m pip install -q "rasterio>=1.4" requests 2>&1 | tail -1   # only if the import above fails on rasterio/requests
$PY python/obs/s1_ingest.py selftest           # must end ALL S1 SELFTEST ASSERTIONS PASSED
```

#### P2 — the smoke, exactly as CGlabs runs it
```bash
SM=<bulk>/atlas_obs_hazards_smoke_pascal; mkdir -p $SM
nohup $PY python/obs/s1_ingest.py ingest --smoke --out-root $SM --years 2015 --rh both > logs/obs_s1_repro_$STAMP.log 2>&1 &
# (no `link` needed: S2 is not run here)
$PY python/obs/s1_ingest.py validate --smoke --out-root $SM --years 2015
$PY python/obs/s1_ingest.py validate --smoke --out-root $SM --years 2015 --vars hurs
$PY python/obs/s1_ingest.py validate --smoke --out-root $SM --years 2015 --vars hurs --token chirps-chirts-agera5-rh15
```
Expect 1,825 files, 0 failures, validate PASS ×3.

#### P3 — digest per variable (what we compare across nodes)
From the manifest CSV(s) in `$SM/manifests/`, for each `var`+`token`: sort rows by date, concatenate the
`md5` column, and hash it:
```bash
for m in $SM/manifests/s1_manifest_*.csv; do
  $PY - "$m" <<'P'
import csv,hashlib,sys,collections
rows=list(csv.DictReader(open(sys.argv[1]))); g=collections.defaultdict(list)
for r in rows: g[(r["var"],r["token"])].append((r["date"],r["md5"]))
for k,v in sorted(g.items()):
    v.sort(); print(k[0],k[1],len(v),hashlib.md5("".join(m for _,m in v).encode()).hexdigest())
P
done
```
Also report per variable the domain-mean over days (min / median / max) that `validate` prints, and the
`libs` block of the manifest JSON.

#### P4 — keep the files
Do **not** delete `$SM`. If CGlabs' digests differ, the macbook will ask for a 365-file value comparison
(about 17 MB) rather than a re-run.

#### Then
Prepend `### RESPONSE — pascal` with P1-P3, commit only this file, push, verify on `origin/develop`.
