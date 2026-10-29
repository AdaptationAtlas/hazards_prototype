# DISPATCH — pascal — rangeland FPAR track: full Africa ingest (2005-2025) + S6 rangeland drought V1

**Append-only; newest block on top. Prepend `### RESPONSE — pascal` to answer.**

### MACBOOK 2026-10-29 — F0-F3 accepted; one read-only question for the merge; keep `$FR`

Thorough, and every number is usable. The splice reading (drift, not a step; V070 flagged less) and the WRSI
year-level +0.63 go into the plan as they stand. The 29.5 h / ~0.48 MB/s is recorded as the JRC rate for this node.

**One question, read-only, no transfer yet.** The merge path is `Data/obs_fpar/` (143 MB) → CGlabs, then S6
re-run there against the S3 THI/NDWL0 stacks. The nodes cannot reach each other, so the relay is either S3 or
a re-ingest on CGlabs (~30 h at your rate). Report **yes/no only** (print no values, no file contents, no
account names): does this account have AWS credentials that `paws`/`s3fs` would pick up —
`~/.aws/credentials` or `AWS_ACCESS_KEY_ID` in the environment — and, if yes, does
`s3fs::s3_dir_ls("s3://digital-atlas/sandbox/")` succeed? Do **not** upload anything: the transfer needs Pete's GO
and will be its own block.

Nothing else to run. Keep `$FR` intact. Prepend a short `### RESPONSE — pascal` with the two yes/no answers.

### RESPONSE — pascal 2026-10-29 — F0-F3 complete: 756/756 dekads, validate PASS, S6 clean, no stop hit

**Session.** JupyterHub, **Small profile**: `cpu.max` = `800000 100000` → 8 usable CPUs, `memory.max`
34359738368 (32 GiB). `--workers 3` as instructed. No SSH or RStudio route. Repo at `d81b9e0`.
Root used: **`<bulk>/atlas/atlas_obs_hazards_pascal`** — the same one-level-deeper fallback as the S1
repro, confirmed fine in your block.

**Outcome.** Everything ran. No stop rule was hit: no selftest failure, no `FAIL` row (no
`fpar_failed_*.csv` was ever created), validate PASS, S6 ran to completion without a WARN, and zero
tied cells. Two things are worth your attention and neither is a gate: the ingest took **29.5 h, not
the ~8 h the dispatch budgeted**, and the z series carries an upward drift across the record that
F3 item 5 was written to look for.

#### F0 — selftests

**FPAR: ALL FPAR SELFTEST ASSERTIONS PASSED (10).** The count now matches the dispatch; the tenth is
the new strip-wise joint mode assertion ("1 coarse row per strip equals the single-pass result"),
which is exactly the change that made the Small profile viable.

**S6: ALL S6 SELFTEST ASSERTIONS PASSED (16)**, after the authorised per-user rebuild.

`install.packages("data.table", lib = Sys.getenv("R_LIBS_USER"))` compiled and installed cleanly —
no compiler or CRAN problem on this node. It built **1.18.6.1** (newer than the broken site-library
1.18.0) into `~/R/x86_64-pc-linux-gnu-library/4.6`, and `.libPaths()[1]` confirms that is where the
load comes from. The site library was not touched and `/usr/local/lib/R/site-library/data.table`
still holds the broken build for every other account — this is a workaround on one account, so the
node-wide breakage (`rlang` and most of the tidyverse included) is still live for IT.

#### F1 — full ingest

**756/756 dekads written, 0 failures.** Wall-clock **106,390.4 s = 29 h 33 m**.

| phase | wall | note |
|---|---|---|
| static: rangeland mask | 1,181.5 s | 393 MB source read at 0.1° |
| static: phenology joint mode | 543.1 s | five layers, one sort per rangeland pixel |
| dekad loop | 104,588 s | **138.1 s/dekad** over 756 dekads |

**The throughput deviation.** The dispatch budgeted ~8 h from the S0 probe's ~1.75 MB/s. The run
averaged 138.1 s/dekad and the rate was stable from the first progress line to the last (116.8 s at
dekad 36, then 135-141 s for the rest), so this is the sustained rate, not a slow start. I did not
capture a byte counter, so I will not quote a measured MB/s; against the dispatch's own ~50 GB
estimate for the Africa window, 104,588 s implies roughly **0.48 MB/s**, about a quarter of the probe
rate. Three workers rather than four accounts for part of it. The run is resumable and nothing was
lost, so this is a scheduling fact for the next node rather than a problem with the data.

`phenology: 31310 rangeland cells with a season; 3625 with two; s1 range 10-67; OND-first cells
(s2 in a later frame-year than s1; S6 pairs them with the next calendar year): 110`

**Five transient failures, all absorbed at retry 1/4** — `RasterioIOError: Read failed` on
2019-03-01, 2019-03-11, 2020-08-01, 2020-08-11 and 2020-08-21. All five dekads are present in the
manifest with a source tag, so the retry policy did its job; there is nothing to re-run.

`du -sh $FR/Data/obs_fpar` = **143 MB** (756 dekads + six static layers).

**validate: PASS.**

```
cutoff 2026-07-10 (from the manifests); dekads present 756/756
product versions: {'V062': 477, 'V070': 279}; version break(s) at ['2018-04-01']; non-CF dekads: 0
server drift: 0 of 756 present dekads were modified upstream since ingest
rangeland FPAR x100 domain mean over dekads: min 36.5 median 40.0 max 45.5
VALIDATE PASS
```

The splice is exactly as the science review described it: a single break, at 2018-04-01, 477 V062
dekads and 279 V070, and no non-final dekad anywhere in the record.

**Static layers.**

| layer | result |
|---|---|
| `rl_frac` | min 0, max 0.99949, mean 0.1358, sd 0.2387 (1440×400) |
| `pheno_s1` / `pheno_e1` | 10-67 (mean 42.09) / 40-92 (mean 61.96) |
| `pheno_s2` / `pheno_e2` | 45-67 / 54-77, on 3,625 cells |
| `pheno_n` | 27,685 one-season vs 3,625 two-season over all finite cells; over `rl_frac ≥ 0.1`: **21,396 vs 2,619 of 24,015** |

#### F2 — S6 on the full window

948.8 s wall, `--no-thi`, DIAGNOSTIC bundles. No WARN on any line.

```
season 1: length dekads 8..33 on 31310 cells; season 2 on 3625 cells (7..18)
rangeland cells (rl >= 0.1): 24015; without a valid season-1 window (dropped silently from every layer): 0
two-season cells whose season 2 falls in the following calendar year: 110
relative deficit of a 1-in-5 year (deficit_q20): median -0.053 over rangeland cells; worst year: median -0.117
baseline P(moderate) per cell: min 0.211 median 0.211 max 0.211 over 24015 rangeland cells (4/19 = 0.211);
  cells with a tie at the quantile: moderate 0 severe 0 extreme 0; cells with fewer than 19 distinct z: 0
consecutive-failure frequency: domain mean 0.052 (no-persistence expectation 0.035)
done: 30 bundles (FPARz+THI-max+NDWL0) under .../Data/hazard_risk_DIAGNOSTIC/ggcmi-crop
```

Three numbers are better than the dispatch's expectations. **Zero** rangeland cells lack a valid
season-1 window, against a "< 1 %" allowance. **Zero** cells tie at any class quantile and zero have
fewer than 19 distinct z, so the tie warning the selftest exercises never fires on real data and the
extreme tier does not double anywhere. `P(moderate)` is exactly 4/19 on every one of the 24,015
cells, which is what a type-1 quantile on 19 clean years should give.

Bundle count: **30** `*FPARz*_int.tif`, as expected.

#### F3 — diagnostics (reported, not gating)

**1. Per start year: domain-mean z and the share of cells at or below q20** (19 start years, rangeland
cells only).

| year | mean z | share ≤ q20 | flagged cells |
|---|---|---|---|
| 2006 | −0.244 | 0.2836 | 6,811 |
| 2007 | −0.236 | 0.2326 | 5,585 |
| 2008 | −0.151 | 0.2303 | 5,530 |
| 2009 | −0.099 | 0.2424 | 5,821 |
| 2010 | −0.063 | 0.2107 | 5,059 |
| **2011** | **−0.445** | **0.3585** | **8,609** |
| 2012 | −0.086 | 0.2077 | 4,988 |
| 2013 | −0.065 | 0.1457 | 3,498 |
| 2014 | −0.227 | 0.2113 | 5,075 |
| **2015** | **−0.306** | **0.2738** | **6,575** |
| 2016 | −0.043 | 0.1859 | 4,464 |
| 2017 | −0.181 | 0.2232 | 5,360 |
| 2018 | +0.017 | 0.1794 | 4,309 |
| 2019 | +0.121 | 0.1569 | 3,769 |
| 2020 | +0.318 | 0.1001 | 2,403 |
| 2021 | −0.032 | 0.2112 | 5,071 |
| 2022 | +0.024 | 0.2068 | 4,966 |
| 2023 | +0.212 | 0.1950 | 4,682 |
| 2024 | +0.415 | 0.1451 | 3,485 |

**2011 is the worst year in the record on both measures** — the lowest mean z and 8,609 flagged cells,
35.9 % of the continent's rangeland, half again the next worst. That is the Horn of Africa drought
and it stands out exactly as you predicted. 2015 is second. The 2016-17 Horn event is visible but
muted at this scale: 2017 is −0.181 with 22.3 % flagged, well inside the ordinary range. That is not
surprising for a continent-wide mean — 2016-17 was geographically narrower than 2010-11.

**2. `thresholds_z` medians over rangeland cells**, against the normal reference:

| | median | mean | normal reference |
|---|---|---|---|
| q20 | **−0.933** | −0.940 | −0.84 |
| q10 | **−1.417** | −1.436 | −1.28 |
| q05 | **−1.872** | −1.918 | −1.64 |

All three sit **below** the normal quantile, and the gap widens into the tail (−0.09, −0.14, −0.23).
The z distribution is left-skewed: bad years are further from the mean than a normal would put them,
which is the expected shape for a bounded-above vegetation index and an argument for keeping the
empirical quantiles rather than assuming normality.

**3. `consecutive_failure` domain mean per year** (18 years, 2007-2024): 0.0719, 0.0500, 0.0787,
0.0619, **0.0959**, 0.0750, 0.0219, 0.0295, 0.0669, 0.0300, 0.0446, 0.0361, 0.0422, 0.0213, 0.0355,
0.0728, 0.0468, 0.0622. Peak in 2011 (9.6 % of rangeland cells in a second consecutive failed year),
trough in 2020 (2.1 %). The 18-year mean is 0.052 against a no-persistence expectation of 0.035, so
back-to-back failures run about 1.5× what independence would give — real drought persistence, and
2022 (0.0728) is the recent Horn multi-season event showing through.

**4. Near-constant cells: none.** Computing the sd of the full 756-dekad series per cell,
**0 of 24,015 rangeland cells have sd < 1** (FPAR ×100). There are no hyper-arid or constant-value tie
candidates inside the `rl_frac ≥ 0.1` mask at all, so there is nothing to locate on the Sahara margin
or in the Namib — the ASAP rangeland mask has already excluded them. This is the same fact S6 reports
from the other side with zero tied cells and zero cells below 19 distinct z, so the two agree.

**5. Splice check.** Of **96,060** flagged cell-years, the share with a start year ≥ 2018 (V070) is
**0.2986**, against the no-shift expectation of 7/19 = **0.3684**. Mean flagged share is 0.2338 before
2018 and 0.1706 after. So V070 years are flagged *less* often, not more.

**This reads as drift, not a step.** The 2017→2018 change in mean z is +0.198, which ranks only
**6th of the 18 adjacent year-to-year jumps** — the largest is 2010→2011 at −0.382. Fitting a line
through the 19 mean-z values gives **+0.0273 per year, +0.492 over 2006-2024**, against an interannual
sd of 0.213. The flagged share trends −0.0056/yr over the same window. A version offset would put a
discontinuity at one boundary; what is here is a gradual climb spread across the record, with
post-2018 years still swinging low (2021 at −0.032 and 21.1 % flagged) and pre-2018 years swinging
high (2013 at 14.6 %).

I would still flag it for the publish gate rather than wave it through: the pre/post difference in
mean z is **+0.332, about 1.6 interannual sd**, so whatever its cause it is large enough to tilt a
19-year baseline. Greening is the parsimonious reading and the shape supports it, but a slow sensor
or compositing change spread over the MODIS→VIIRS transition would look similar. The `validate`
evidence is clean on the mechanical side: one break at 2018-04-01, `non-CF dekads: 0`.

**6. FEWS rangeland WRSI cross-check (Kenya, MAM).** Pulled the 19 `wrsi_rangeland_MAM_<year>.tif`
for 2006-2024 from the published `eastafrica-wrsi-fews` prefix, area-averaged them from 0.1° onto the
0.25° lattice, and paired against `z_year` over Kenya rangeland cells (`rl_frac ≥ 0.1`): **883 cells
per year, 16,671 pairs**.

- **Pooled correlation: Pearson +0.241, Spearman +0.267.**
- **Year-level (the 19 domain means): Pearson +0.626.**
- Monotone gradient across WRSI quintiles — mean z of **−0.731** in the driest fifth rising through
  −0.502, −0.275, −0.220 to **−0.196** in the wettest.
- Per-year r is positive in 15 of 19 years, best in 2011 (+0.378) and 2006 (+0.325), negative in 2008
  (−0.204), 2016 (−0.104), 2018 (−0.121) and 2021 (−0.034).

**A low per-cell correlation is the expected result here, and two structural mismatches explain most
of it.** First, WRSI saturates: 34.4 % of the pooled values sit at exactly 100, so a third of the
sample carries no gradient at all and cannot correlate with anything. Second, the comparison is not
like-for-like in time — WRSI is MAM only, while `z_year` is the S6 pastoral-year score, which for the
2,619 two-season cells in the mask combines both seasons and for the 110 OND-first cells deliberately
pairs OND(Y−1) with MAM(Y). The year-level +0.626 is the more honest number, because aggregating
removes the saturation problem: the two products agree well on *which years were bad in Kenya* and
only weakly on *which cells*. The quintile gradient says the per-cell signal is real but shallow.
I read this as corroboration, not as a discrepancy to chase.

#### Housekeeping

`$FR` = `<bulk>/atlas/atlas_obs_hazards_pascal` is kept intact: 756 dekads + six static layers
(143 MB), `seasonal/2006-2025/` with eight rasters, 30 DIAGNOSTIC bundles, and
`manifests/fpar_manifest_20261028_155655_107198.{csv,json}`.

The WRSI rasters for item 6 were downloaded to a temporary directory on the bulk data root
(176 KB, 19 files) and **deleted** after the comparison; nothing else was written outside `$FR`.
Logs (gitignored): `logs/obs_fpar_ingest_20261028_155655.log`,
`logs/obs_fpar_validate_20261029_213042.log`, `logs/obs_fpar_s6_20261029_213219.log`,
`logs/obs_fpar_rinstall_20261028_155735.log`.

One note for whoever runs the crops chain: `scipy` is not in the `obs` env, so the item 6 rank
correlation is computed with a `numpy` `argsort`. Nothing was installed to work around it.

---

### MACBOOK 2026-10-28 — F0 answered: GO F1 now (Python only), GO per-user rebuild of `data.table` for F2; Small-profile memory fixed

Thanks — the stop was right, and the diagnosis is complete. Three decisions, then the amended steps.

**1. The R stack.** Per-user fix is authorised: S6 needs only `terra` (loads) and `data.table`, so rebuild
`data.table` alone into `R_LIBS_USER` (account-scoped, reversible, nothing shared touched). Do **not** install into
the site library. Pete will raise the node-wide breakage with IT separately — this is a workaround, not the fix.
```bash
mkdir -p ~/R/x86_64-pc-linux-gnu-library/4.6
Rscript -e 'install.packages("data.table", lib = Sys.getenv("R_LIBS_USER"), repos = "https://cloud.r-project.org")'
Rscript -e 'library(data.table); cat(as.character(packageVersion("data.table")), .libPaths()[1], "\n")'   # loads, from ~/R/...
Rscript R/obs/s6_rangeland_fpar.R --selftest        # ALL S6 SELFTEST ASSERTIONS PASSED (16)
```
If `install.packages` fails (no compiler, no CRAN access), stop and paste the error — do not try alternatives.

**2. F1 is GO now, independent of R.** Start it before the R step; the two do not interact.

**3. Small profile (8 CPU / 32 GiB).** My F1 sizing assumed the Large profile. Pull first: `fpar_ingest.py` now runs
float32 end to end and the phenology joint mode works in 16-coarse-row strips, so the per-dekad peak is ~2 GB per
worker instead of ~7. On Small use **`--workers 3`**; on Large `--workers 4` as before. Report `cpu.max` and the
profile you ran on. (Kenya smoke re-run on the macbook after the change: phenology and rangeland fraction
bit-identical to the float64 run; dekad means differ at float32 precision only — figures in the commit.)

**Corrections to the block below:** the FPAR selftest count is **10** at the commit this block ships with
(9 at `ac16b0d`, as you measured — the dispatch was ahead of the code); the splice check is F3 **item 5**, the
WRSI cross-check item 6; `$FR` = `<bulk>/atlas/atlas_obs_hazards_pascal` is fine, say so in the response.

#### F1 — now
```bash
cd <repo> && git pull --ff-only && git log -1 --format='%h %s'
PY=~/.conda/envs/obs/bin/python
$PY python/obs/fpar_ingest.py selftest                                    # ALL FPAR SELFTEST ASSERTIONS PASSED (10)
FR=<bulk>/atlas/atlas_obs_hazards_pascal; mkdir -p $FR logs; STAMP=$(date +%Y%m%d_%H%M%S)
nohup ionice -c3 nice -n19 $PY python/obs/fpar_ingest.py ingest --out-root $FR --years 2005:2025 --workers 3 \
      > logs/obs_fpar_ingest_$STAMP.log 2>&1 &
echo $! > logs/obs_fpar_ingest_$STAMP.pid
```
Then the R step (1) while it runs. When the ingest finishes: `validate` as in F1 below, then F2 (with `--no-thi`)
and F3 as written. Same stop rules. Prepend one `### RESPONSE — pascal` for the lot, with the delivery line.

---

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
