# DISPATCH — pascal — reproduce the S1 Kenya smoke independently (cross-node reproducibility gate)

**Append-only; newest block on top. Prepend `### RESPONSE — pascal` to answer.**

### RESPONSE — pascal 2026-10-27

**Session.** JupyterHub, **Small profile** this time, not Large: `cpu.max` = `800000 100000` → 8
usable CPUs, `memory.max` 32 GiB. No SSH or RStudio route. Repo at `0a82c46`. P1-P4 all done; the
smoke tree is kept.

**One deviation, stated up front.** `<bulk>/atlas_obs_hazards_smoke_pascal` could not be created:
the top-level `Workspace` directory is not writable by an ordinary account (`Permission denied`).
The smoke root is therefore one level deeper, in the account-writable area of the same bulk mount:
`<bulk>/atlas/atlas_obs_hazards_smoke_pascal`. Nothing else changed, and **all writes stayed under
that new root**.

#### P1 — env + selftest

`rasterio` was **not** in the `obs` env, so the conditional install in the dispatch ran:
`pip install "rasterio>=1.4" requests` → rasterio 1.5.2, requests 2.34.2.

```
rasterio 1.5.2  gdal 3.12.2  zarr 3.4.1  xarray 2026.9.0  requests 2.34.2
```

**Note the GDAL.** The env's `gdalinfo` is 3.13.3, but `rasterio` ships its own and reports
**3.12.2** — and rasterio is what writes the tifs, so 3.12.2 is the number that matters for a
byte-comparison. The manifest `libs` block records 3.12.2, correctly.

`selftest` → **ALL S1 SELFTEST ASSERTIONS PASSED** (17 assertions: lattice/snapping, Africa and
fine windows, `coarsen5` block-mean and NaN handling, the J→MJ range check, the mask-change check,
the −9999 exclusion, and the source-tag skip/`--force` rule).

#### P2 — the smoke

`ingest --smoke --years 2015 --rh both`, 621.3 s wall, Kenya box (33.5, −5.0, 42.5, 5.5),
window 36×42 on the 1440×400 lattice.

```
manifest: s1_manifest_20261027_082115_15689.csv (2190 rows, 0 failures)
```

**2,190 tif files on disk, 127 MB.** That is 6 variable-token series × 365 — the dispatch's "1,825
files" counts five; the sixth is the second `hurs` series that `--rh both` writes under the
`-rh15` token. 1,825 + 365 = 2,190, so this matches the dispatch's intent rather than its arithmetic.

Per-leg timing: `pr` 42 s, `tasmax` 37 s, `tasmin` 39 s, `rsds` 108 s (0.30 s/day),
`hurs` daytime-mean 317 s (0.90 s/day, five RH hours per day), `hurs` RH15-only 79 s (0.21 s/day).

**validate ×3, all PASS:**

| run | result | domain-mean over days (min / median / max) |
|---|---|---|
| all vars, default token | **PASS** | `pr` 0.02 / 1.15 / 15.76 mm d⁻¹ · `tasmax` 29.10 / 32.40 / 36.84 °C · `tasmin` 20.23 / 22.78 / 24.92 °C · `rsds` 16.34 / 20.75 / 26.60 MJ m⁻² d⁻¹ |
| `--vars hurs` | **PASS** | 35.26 / 56.11 / 71.97 % |
| `--vars hurs --token …-rh15` | **PASS** | 21.45 / 43.18 / 62.37 % |

Also reported: `lattice equals base_rast_nexgddp.tif: True`, 365/365 days present for every
variable, and `tasmax < tasmin` beyond the 0.05 quantum in **0 cells**.

The only non-PASS lines are three `link missing` warnings for `chirps_wrld/` and the two soils
rasters — expected, since the dispatch says no `link` is needed when S2 is not run.

RH15-only is drier than the daytime mean at every statistic (21.45 vs 35.26 min, 43.18 vs 56.11
median), which is the right sign for mid-afternoon RH against a 06-18 mean.

#### P3 — the digests to compare across nodes

Sorted by date, `md5` column concatenated, md5 of that string:

```
var     token                        n    digest
hurs    chirps-chirts-agera5         365  8f64756048c9487a5de4f7d1872f4ccf
hurs    chirps-chirts-agera5-rh15    365  e2a982a40e1ea2b8fcbf6795e8f527ca
pr      chirps-chirts-agera5         365  0de8c398ea2fdc4eba931d0b19fff945
rsds    chirps-chirts-agera5         365  d4698ab7c7fa5cb5d0dfd28ee8087ca5
tasmax  chirps-chirts-agera5         365  384f3747f8eb7a0fe3ba57dd99a55baf
tasmin  chirps-chirts-agera5         365  9e8e7df8388b70c50290477ed205d5fe
```

**Manifest `libs` block:**

```json
{"python": "3.12.15", "numpy": "2.5.4", "xarray": "2026.9.0",
 "zarr": "3.4.1", "rasterio": "1.5.2", "gdal": "3.12.2"}
```

Pins the run resolved, for completeness: CHIRTS cube revision `c44a0b58772e32f7cbb2b5a740d5e6586ceeb054`,
CHIRPS v3 rnl revision `e9466d3109c6d087c6cf60c19ca6ae20873d358a`, AgERA5 from the FAO GISMGR mirror
recorded as `v2.0 (FAO GISMGR mirror; archive rewritten 2025-09)`, lattice origin (−180, 50),
shape 400×1440, res 0.25.

**If CGlabs' digests differ,** my first suspicion is `pr` and the two temperature variables rather
than `rsds`/`hurs`: those three come straight from the cubes via an exact 5×5 block mean of int16
values, so they should be bit-identical regardless of GDAL. `rsds` and `hurs` go through the
AgERA5 0.1°→0.25° area-weighted average in float, which is where a library-version difference
would show up first. So a split where `pr`/`tasmax`/`tasmin` match and `rsds`/`hurs` do not would
point at benign float rounding; `pr` differing would point at something else.

#### P4 — files kept

`<bulk>/atlas/atlas_obs_hazards_smoke_pascal` is intact — 2,190 tifs, 127 MB, plus
`manifests/s1_manifest_20261027_082115_15689.{csv,json}`. Ready for a 365-file value comparison if
the digests disagree. Log: `logs/obs_s1_repro_20261027_082114.log` (gitignored).

---

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
