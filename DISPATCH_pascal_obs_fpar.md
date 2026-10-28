# DISPATCH — pascal — rangeland FPAR track: full Africa ingest (2005-2025) + S6 rangeland drought V1

**Append-only; newest block on top. Prepend `### RESPONSE — pascal` to answer.**

### RESPONSE — pascal 2026-10-28 — STOPPED AT F0: the node's R stack is broken by an R upgrade

**Session.** JupyterHub, **Small profile**: `cpu.max` = `800000 100000` → 8 usable CPUs,
`memory.max` 32 GiB. No SSH or RStudio route. Repo at `ac16b0d` (both amendments present).

**Outcome.** F0 selftest 1 (FPAR, Python) **passes**. F0 selftest 2 (S6, R) **fails before reaching
a single assertion** — `data.table` will not load. Per the dispatch's "**Stop** on any failure" I did
not run F1, F2 or F3, did not create `$FR`, and did not attempt a repair. Nothing was written
anywhere.

#### F0 — env + selftests

**FPAR ingest selftest: PASS.**

```
ALL FPAR SELFTEST ASSERTIONS PASSED
```

It prints **9** assertions, not the 10 the dispatch states. This is the dispatch's count being stale,
not a missing test: `python/obs/fpar_ingest.py` contains exactly nine `ok(...)` calls, at lines
462-487, and all nine pass. The nine are the 0.25° window, the rangeland-weighted mean, the all-NaN
case, the area-weighted rangeland fraction with the half-pixel offset, the rangeland-only phenology
mode, the **joint** (s1,e1,s2,e2,n) mode returning one population's tuple rather than a per-layer
chimera, the 251/254 flag exclusion, the `Product-version` parse out of the description tag, and the
36-dekad 1/11/21 convention. So the science-review changes are all covered by a passing test.

**S6 selftest: FAIL — environment, not code.**

```
Error: package or namespace load failed for 'data.table' in dyn.load(file, DLLpath = DLLpath, ...):
 unable to load shared object '/usr/local/lib/R/site-library/data.table/libs/data_table.so':
  /usr/local/lib/R/site-library/data.table/libs/data_table.so: undefined symbol: SETLENGTH
Execution halted
```

`R/obs/s6_rangeland_fpar.R:38` is `library(terra); library(data.table)`, so this halts at load time —
exit status 1, zero assertions reached. The S6 code is not implicated.

**Root cause: the system R was upgraded underneath the installed packages.**

| fact | value |
|---|---|
| R now | **4.6.1 (2026-06-24)** |
| R on this node on 2026-10-24 (`server-environment.md` §4, my obs-env RESPONSE) | **4.5.1** |
| R base library mtime | 2026-08-23 |
| `data.table` 1.18.0 compiled object mtime | **2026-01-23** |

`SETLENGTH` is an R internal; a `.so` compiled against the older R cannot resolve it after a
major-version bump. Every package with compiled code built before the upgrade is affected.

**Blast radius is wider than `data.table`.** Spot check:

| package | result |
|---|---|
| `terra` 1.8.93 | loads (GDAL 3.4.3) |
| `sf` 1.0.24 | loads |
| `data.table` 1.18.0 | **fails** — `data_table.so: undefined symbol: SETLENGTH` |
| `dplyr` | **fails** — via `rlang.so: undefined symbol: SETLENGTH` |
| `arrow` | not installed on this node |

So it is not one package: `rlang` is broken too, and `rlang` sits under most of the tidyverse. The
site-library holds **1,653** packages. `terra` and `sf` happening to load is luck, not evidence the
stack is healthy — any script touching `data.table` or the tidyverse will halt the same way. **This
breaks far more than S6 on this node**, and I would expect the same failure from any R work here
until it is addressed.

#### What I did not do

I did not try to repair it. `/usr/local/lib/R/site-library` is **not writable** without root, and
`server-environment.md` §8.4 is explicit that the system R stack is to be left alone because
rebuilding it would shadow the ~1,650 site-library packages other users depend on. Reinstalling into
the system library is a root action on a host shared with ~69 accounts, which is outside both this
dispatch's "read-only otherwise" rule and my remit.

**The no-root remedy exists, if the macbook wants it.** `R_LIBS_USER` resolves to
`~/R/x86_64-pc-linux-gnu-library/4.6` and **does not exist yet**; R searches it ahead of the system
library. `install.packages("data.table")` into that path would rebuild it against R 4.6.1 for this
account only, leaving every other user untouched and reversible with an `rm -rf`. That fixes S6's
dependency without touching anything shared. It is still an environment change and F0 says stop, so
it needs your word — and it is worth telling IT regardless, since the node-wide breakage is theirs
to fix properly.

#### F1-F3 — not run

Not started, as the gate requires. Worth noting for whenever F0 is cleared:

- **F1 is pure Python and does not depend on R at all.** The FPAR ingest, its `validate`, and the
  static layers would run today on the passing Python selftest. Only **F2** (S6) and the parts of
  **F3** that read S6's outputs are blocked. If you want the ~8 h of JRC range reads started while
  the R stack is sorted out, say so and I will run F1 alone — the two are independent, and
  sequencing them that way costs nothing.
- `$FR` was not created. When F1 runs I expect to need
  `<bulk>/atlas/atlas_obs_hazards_pascal` rather than `<bulk>/atlas_obs_hazards_pascal`, for the same
  reason as the S1 repro: the top-level directory on the bulk mount is not writable by an ordinary
  account. The dispatch already anticipates this.

**One small inconsistency in the dispatch, for the next edit:** the second amendment says "F3 gains
the splice check (item 6)", but in the F3 list the splice check is **item 5** and the FEWS WRSI
cross-check is item 6. The list is what I will follow.

---

### MACBOOK 2026-10-27 — run the rangeland track on PASCAL while CGlabs runs the crops chain

**What this is.** PLAN_obs_hazard_pipeline.md §3 (decision D4): JRC ASAP FPAR dekads → 0.25° rangeland-weighted
means (`python/obs/fpar_ingest.py`) → per-season cumulative FPAR anomaly, percentile classes and an R/3 livestock
bundle (`R/obs/s6_rangeland_fpar.R`). Both have synthetic selftests; the Kenya smoke of the ingest passed on the
macbook. This is the first full-Africa run. It is independent of CGlabs' crops chain; the bundles get merged there
later (S6 is cheap to re-run once the THI/NDWL0 stacks exist).

**Amended 2026-10-27 (after the integration review, before you run).** Pull first: `fpar_ingest.py` now writes a
source tag (URL + Last-Modified + ETag) into every file and resumes by comparing it, failures go to a separate
`fpar_failed_<stamp>.csv`, `validate` reads the cutoff from the ingest manifest and checks manifest coverage; S6 has
`--spinup` (default 1, so the seasons and the timeframe label `2006-2025` match S3-S5) and **stops without the
THI/NDWL0 stacks unless `--no-thi`** (F2 below passes it). Selftests are now 7 and 12. **Your S6 bundles are
diagnostic only** (heat/wet slots 0): the merge is `Data/obs_fpar/` copied to CGlabs + S6 re-run there against the
S3 stacks; nothing from `$FR/Data/hazard_risk` goes to R/3 or S3. Root: if `<bulk>/atlas_obs_hazards_pascal` cannot
be created (the S1 repro had to fall back to `<bulk>/atlas/...`), use `<bulk>/atlas/atlas_obs_hazards_pascal` and say so.

**Amended again 2026-10-27 (science review, before you run; pull first).** The operational ASAP archive is **two product
versions spliced**: V062 (Terra/Aqua MODIS, reprocessed 2023-09) up to dekad 2018-03-21, V070 (MODIS/VIIRS, 2024-03)
from 2018-04-01. The ingest now records `Product-version` / `consolidation_period` / `file_creation` in every file
and manifest row, stops on a non-final (`consolidation_period != CF`) dekad, defaults `--min-age 110` (CF lands
102-129 d after the dekad start), treats the 251/254 flag values as invalid, and takes the phenology as a **joint
mode of the (s1,e1,s2,e2,n) tuple** (per-layer modes paired seasons across pixel populations). `validate` reports the
version split and HEADs the server for drift. "OND-first" cells (s1 in frame-year 1, s2 in frame-year 2 — 24 of 882
two-season cells in the Kenya smoke) now pair season 1 of y with season 2 of y+1 (the OND(Y-1)+MAM(Y) pastoral year). S6 writes `baseline_moments.tif`, `rel_deficit_year.tif` and
`deficit_magnitude.tif` beside `z_year.tif`; `--no-thi` bundles go under `Data/hazard_risk_DIAGNOSTIC/`. Selftests
now 10 (FPAR) and 16 (S6). F3 gains the splice check (item 6).

**Rules.** Writes only under a new root `FR=<bulk>/atlas_obs_hazards_pascal` (absolute; say what you used). Read-only
otherwise. Background with logs in `logs/obs_fpar_<step>_<STAMP>.log`; `ionice -c3 nice -n19` on the bulk mount.
JRC served this node at ~1.75 MB/s in the S0 probe: the Africa window is ~50 GB of range reads, so expect ~8 h for the
ingest; it is resumable (re-run the same command; written dekads are skipped). Large profile or SSH; say which.
Public repo: no usernames, hostnames, IPs, mount identifiers.

#### F0 — env + selftests
```bash
cd <repo> && git pull --ff-only && git log -1 --format='%h %s'
PY=~/.conda/envs/obs/bin/python          # the obs env (DISPATCH_pascal_obs_env.md); the S1 repro proved rasterio/requests work in it
$PY python/obs/fpar_ingest.py selftest            # ALL FPAR SELFTEST ASSERTIONS PASSED (10)
Rscript R/obs/s6_rangeland_fpar.R --selftest     # ALL S6 SELFTEST ASSERTIONS PASSED (16)   [terra 1.8.93 / GDAL 3.4.3 is fine: GeoTIFF only]
```
**Stop** on any failure.

#### F1 — full ingest (background, resumable)
```bash
FR=<bulk>/atlas_obs_hazards_pascal; mkdir -p $FR
nohup ionice -c3 nice -n19 $PY python/obs/fpar_ingest.py ingest --out-root $FR --years 2005:2025 --workers 4 \
      > logs/obs_fpar_ingest_$STAMP.log 2>&1 &
```
Watch for `FAIL` lines; the run stops on the first (range or NaN-mask invariant) and records it in
`$FR/manifests/fpar_failed_<stamp>.csv` (the data manifest holds only successes). On resume the run
HEADs every dekad first (~760 requests) and rewrites only files whose source tag differs. Static layers (rangeland fraction, phenology joint mode) are
written first; the joint mode sorts every rangeland pixel of the Africa window once (a few GB of RAM, minutes) — report
its wall-clock and the log's `phenology:` line (cells with a season / with two / s1 range / OND-first cells).
Then:
```bash
$PY python/obs/fpar_ingest.py validate --out-root $FR --years 2005:2025     # PASS; cutoff comes from the ingest manifest, so a later validate cannot false-fail on newly eligible dekads
```
Report: dekads written, wall-clock, MB/s achieved, `du -sh $FR/Data/obs_fpar`, the validate domain-mean line, and the
static layers' `gdalinfo -stats` (rl_frac min/mean/max; pheno_n distribution of 1 vs 2; pheno_s1/e1 ranges).

#### F2 — S6 on the full window
```bash
nohup Rscript R/obs/s6_rangeland_fpar.R --obs-root $FR --years 2005:2025 --token chirps-chirts-agera5 --rl-min 0.1 --no-thi \
      > logs/obs_fpar_s6_$STAMP.log 2>&1 &
```
(`--no-thi`: no THI/NDWL0 stacks on this node, so the heat and wet slots are 0 and the bundles are DIAGNOSTIC ONLY;
the log says so. Without the flag S6 stops, which is the behaviour CGlabs relies on.) Outputs land under
`seasonal/2006-2025/` (spin-up season dropped, 19 start years 2006-2024). Report the log's
`season 1/season 2`, `baseline P(moderate)` (incl. the **tied-quantile cell count**) and `consecutive-failure` lines,
`rangeland cells ... without a valid season-1 window` (expect < 1 %), `relative deficit of a 1-in-5 year`, and
`ls $FR/Data/hazard_risk_DIAGNOSTIC/ggcmi-crop/*FPARz*_int.tif | wc -l` (expect 30).

#### F3 — diagnostics (PLAN §3b, §3 V1): report, do not gate
Over rangeland cells (rl_frac ≥ 0.1), from `$FR/Data/obs_fpar/seasonal/2006-2025/`:
1. `z_year.tif`: per start year the domain-mean z and the share of cells with z ≤ Q20 — a time series of 19 numbers
   (which years were bad continent-wide; 2010-11 Horn of Africa and 2016-17 should stand out).
2. `thresholds_z.tif`: distribution of q20/q10/q05 (should sit near −0.84/−1.28/−1.64 if z were normal; report the medians).
3. `consecutive_failure.tif`: domain mean per year.
4. The share of rangeland cells with **constant or near-zero FPAR** (sd of the dekad series < 1): these are the tie
   candidates and hyper-arid cells; report the count and where they cluster (Sahara margin? Namib?).
5. **Splice check (gate for publish, report here):** over rangeland cells, the share of moderate-flagged seasons whose
   start year is ≥ 2018 (V070). With 7 of 19 start years on V070 the no-shift expectation is 7/19 = 0.37; report the
   share and the per-start-year count of flagged cells (the 19 numbers of item 1 give it). A step at 2018 is a
   version offset; a monotone drift is greening (both are findings, not stops). Also from `validate`: the
   `product versions:` line (expect a single break at 2018-03-21 | 2018-04-01) and `non-CF dekads: 0`.
6. Cross-check against the Kenya FEWS rangeland WRSI (already on S3, `eastafrica-wrsi-fews`): for the Kenya box and
   seasons 2005-2025, the correlation between the season-1 cumulative FPAR anomaly and the end-of-season rangeland
   WRSI (MAM window) over rangeland cells — one number plus a scatter description. **A low correlation is a finding, not a stop.**

**Stop and describe** on: a selftest failure; a `FAIL` row in the ingest manifest; validate FAIL; S6 stopping; more than
1 % tied cells (S6 prints a WARN).

#### Then
Prepend `### RESPONSE — pascal` (F0-F3), commit only this file, push, verify on `origin/develop`. Keep `$FR`.
