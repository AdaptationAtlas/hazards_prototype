# Dispatch: tier-16 population denominator fixes (#42 join artefact, #43 pop_method decision)

Append-only, **newest block on top**. Respond by prepending a `### RESPONSE` block.

### RESPONSE — Block A STOPPED at gate A1 (cglabs, 2026-10-06 11:57 EAT)

**Nothing written, nothing published.** A0 passed; A1 failed its x1.0000 invariant; stopped there per
the dispatch. Diagnosis below is read-only (a second dry run with the default method).

**A0 — PASS.** All invariants as written:

```
exposure_gfm_seasonal.parquet  i.cols=                 mtime=2026-09-17 12:56:53
exposure_jrc_rp.parquet        i.cols=i.pop_source     mtime=2026-09-17 12:56:53
exposure_totals.parquet        i.cols=i.pop_source     mtime=2026-09-17 12:56:53
rows=290 adm1=47 national pop_total=52837534 pop_total_grid=55119798
pop_source=knbs-projection-2026 pop_method=county-level pop_year=2026
```

No other Rscript running; no `POP_*`/`APPLY` env set. `i.pop_source` holds `knbs-census-2019` on
every row of both affected tables (the right-hand copy from the pre-fix join).

**A1 — FAIL.** With the dispatch's env (`POP_METHOD=county-level`) every table moved
(`logs/relevel42_dryrun_20261006_115637.log`):

```
re-levelling Data/exposure/intersect | POP_SOURCE=knbs-projection POP_YEAR=2026 POP_METHOD=county-level | DRY RUN
  exposure_jrc_rp.parquet      dropped stale join artefact: i.pop_source (#42)
  exposure_totals.parquet      dropped stale join artefact: i.pop_source (#42)
  exposure_gfm_seasonal.parquet      2610673 ->      2675885  (x1.0250)
  exposure_jrc_rp.parquet           5668271 ->      5816936  (x1.0262)
  exposure_totals.parquet          52837534 ->     54226998  (x1.0263)
  national pop_total now 54226998 [knbs-projection-2026 / county-growth-from-2020]
DRY RUN — nothing written.
```

The two `dropped stale join artefact` lines are exactly as expected (jrc + totals, not GFM) — the #42
fix itself behaves. The x-factors do not.

**Diagnostic dry run, default method (`POP_METHOD` unset = `county-growth`), same env otherwise**
(`logs/relevel42_dryrun_DIAG_default_method_20261006_115747.log`):

```
re-levelling Data/exposure/intersect | POP_SOURCE=knbs-projection POP_YEAR=2026 POP_METHOD=county-growth | DRY RUN
  exposure_jrc_rp.parquet      dropped stale join artefact: i.pop_source (#42)
  exposure_totals.parquet      dropped stale join artefact: i.pop_source (#42)
  exposure_gfm_seasonal.parquet      2610673 ->      2610673  (x1.0000)
  exposure_jrc_rp.parquet           5668271 ->      5668271  (x1.0000)
  exposure_totals.parquet          52837534 ->     52837534  (x1.0000)
  national pop_total now 52837534 [knbs-projection-2026 / county-growth-from-2020]
DRY RUN — nothing written.
```

**What this means.** The live tier-16 tables were built with **`county-growth`**, not `county-level`.
That matches the record: Pete picked option C **with `county-growth`** on 2026-09-17
(`archive/dispatches/DISPATCH_cglabs_knbs_population.md` lines 40 and 347, RESPONSE #4, commit
4cf82b4), and `county-growth` reproduces every live value to x1.0000. The live `pop_method` column
saying `county-level` (`county-level-yearmatched` in GFM) is a **mislabel**, not the method used.
54,226,998 is the published-KNBS-level figure for 2026; 52,837,534 is census-anchored growth.

Consequences for this dispatch, for macbook to rule on — not improvised here:

1. **Block A's env is wrong.** `POP_METHOD=county-level` is Block B, not the published state. The
   value-neutral env is the default method (`POP_METHOD` unset or `county-growth`).
2. **Block A cannot be fully value-neutral even so.** The fixed 7b rewrites `pop_method` from the
   stored strings; under `county-growth` it writes `county-growth-from-2020` (GFM:
   `county-growth-from-2020-yearmatched`), i.e. it would *correct* the mislabel as a side effect.
   Numeric columns stay x1.0000. Decide whether that string change is in scope for A or wants its
   own line in the dispatch / CDH record.
3. **#43 may rest on the mislabel.** `HANDOVER_2026-10-06_ke-enso-exposure-denominator-answers.md`
   §(b) treats `pop_method = county-level` on projection rows as the live choice awaiting Pete. The
   live *numbers* are already the `county-growth` Pete chose. The 1.4 M question is still real as a
   policy question, but the premise "published with county-level" is not.
4. **How the mislabel got written** I did not chase. The code at 4cf82b4 (lines 141-144) should
   have produced `county-growth-from-2020` for a `knbs-projection-2026` reference, yet the parquet
   says `county-level`. Worth a look on macbook before the next apply.

**Awaiting:** a corrected Block A (env + expectation on the `pop_method` string), or a GO to run it
with the default method as-is. Either is a seconds-long re-run of A1→A5 from here. Tier 16 on S3
is untouched; the KE-ENSO workaround stays in place.

---

## Block B — `pop_method` re-level — **PARKED, needs Pete GO** (#43)

Do **not** run this block yet. The live tables are published with `pop_method = county-level` on
projection-sourced rows; the pipeline default is `county-growth`. The choice moves the national
denominator by ~1.4 M for 2025 (51.96 M vs 53.33 M) and carries a KNBS licence dimension
([#43](https://github.com/AdaptationAtlas/hazards_prototype/issues/43), bundled with #33). It is
Pete's call, not a data fix. When he rules, this block becomes one more 7b run with a different
`POP_METHOD` and a tier-16 republish — the same mechanics as Block A, so nothing new to build.

**Block A must not change any value.** If it does, stop: that would mean Block B has been run by
accident.

---

## Block A — strip the leaked `i.pop_source` column and republish tier 16 (#42)

**Why.** `exposure_jrc_rp.parquet` and `exposure_totals.parquet` on S3 each carry a stray
`i.pop_source` alongside the real `pop_source`; `exposure_gfm_seasonal.parquet` does not. It is a
`data.table` join artefact in `relevel()` — the non-`by_year` branch did not drop the table's own
`pop_source` before `scale_dt[dt, on = "adm1_pcode"]`, so the right-hand copy was retained under the
`i.` prefix. The `by_year` branch did drop it, which is exactly why the GFM table escaped.

**Fixed on `develop`** in `R/observational/7b_relevel_exposure_pop.R`:
1. `pop_source` added to the `drop` vector at the top of `relevel()`, so neither branch can leak it
   (it is re-set unconditionally further down in both).
2. Any pre-existing `^i\.` column is stripped on **read**, so re-levelling an already-affected table
   repairs it rather than aborting.
3. A write-time assertion refuses to write any table carrying a `^i\.` column.

Validated on macbook: the join mechanism was reproduced in isolation (artefact present before the
fix, absent after, `pop_scale_adm1` / `pop_total_grid` / `pop_source` values unchanged), and the
script parses.

**This block is value-neutral by design.** The env below reproduces the denominator the tables were
published with. Every table must come back x1.0000. **Do not change `POP_METHOD` here** — that is
Block B.

### A0 — preflight (read-only, seconds)

```bash
cd /home/jovyan/atlas/hazards_prototype && git pull --ff-only && git log -1 --oneline
pgrep -af Rscript || echo "no Rscript running"
env | grep -E '^(POP_|APPLY|IN_DIR|EXP_ROOT)' || echo "no POP_ env set (good)"
Rscript -e '
  source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")
  suppressPackageStartupMessages(library(arrow)); library(data.table)
  d <- file.path(dirname(chirts_chirps_hist_dir), "exposure", "intersect")
  for (f in c("exposure_gfm_seasonal.parquet","exposure_jrc_rp.parquet","exposure_totals.parquet")) {
    n <- names(read_parquet(file.path(d, f), as_data_frame = FALSE))
    cat(sprintf("%-30s i.cols=%-16s mtime=%s\n", f,
        paste(grep("^i\\.", n, value = TRUE), collapse = ",") , format(file.mtime(file.path(d,f)))))
  }
  t <- as.data.table(read_parquet(file.path(d, "exposure_totals.parquet")))
  cat(sprintf("rows=%d adm1=%d national pop_total=%.0f pop_total_grid=%.0f\n",
      nrow(t), uniqueN(t$adm1_pcode), sum(t$pop_total), sum(t$pop_total_grid)))
  cat(sprintf("pop_source=%s pop_method=%s pop_year=%s\n", t$pop_source[1], t$pop_method[1], t$pop_year[1]))'
```

**Expect (invariants, not figures):**
- `exposure_jrc_rp` and `exposure_totals` each report **exactly one** `i.` column, `i.pop_source`.
- `exposure_gfm_seasonal` reports **none**.
- 290 rows, 47 counties; `pop_source` is a `knbs-projection-<year>` string and `pop_method` is
  `county-level`.
- **Record `national pop_total` — every later gate compares against it.**

**STOP if** the GFM table also carries an `i.` column, or any table carries an `i.` column other
than `i.pop_source`, or `pop_method` is not `county-level`. Any of those means the live state is not
what this dispatch was written against. Describe what you see; do not improvise.

### A1 — dry run (seconds, foreground)

```bash
cd /home/jovyan/atlas/hazards_prototype
POP_SOURCE=knbs-projection POP_YEAR=2026 POP_METHOD=county-level \
POP_YEAR_MATCH=1 POP_REF_YEAR=2026 \
Rscript R/observational/7b_relevel_exposure_pop.R 2>&1 | tee logs/relevel42_dryrun_$(date +%Y%m%d_%H%M%S).log
```

**Expect:**
- Two `dropped stale join artefact: i.pop_source (#42)` lines — for `exposure_jrc_rp` and
  `exposure_totals`, and **not** for the GFM table.
- All three tables report **x1.0000**. A factor that is not 1.0000 means the denominator moved:
  **STOP**, you are accidentally in Block B territory.
- `national pop_total now <N> [knbs-projection-2026 / county-level]` where `<N>` equals the A0
  figure.
- Ends with `DRY RUN — nothing written.`

### A2 — apply (seconds, foreground)

Same command with `APPLY=1`:

```bash
cd /home/jovyan/atlas/hazards_prototype
POP_SOURCE=knbs-projection POP_YEAR=2026 POP_METHOD=county-level \
POP_YEAR_MATCH=1 POP_REF_YEAR=2026 APPLY=1 \
Rscript R/observational/7b_relevel_exposure_pop.R 2>&1 | tee logs/relevel42_apply_$(date +%Y%m%d_%H%M%S).log
```

**Expect:** the same x1.0000 lines, then `WROTE -> …`.

### A3 — local gates before publishing (read-only)

```bash
cd /home/jovyan/atlas/hazards_prototype
Rscript -e '
  source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")
  suppressPackageStartupMessages(library(arrow)); library(data.table)
  d <- file.path(dirname(chirts_chirps_hist_dir), "exposure", "intersect")
  ok <- TRUE
  for (f in c("exposure_gfm_seasonal.parquet","exposure_jrc_rp.parquet","exposure_totals.parquet")) {
    t <- as.data.table(read_parquet(file.path(d, f)))
    leaked <- grep("^i\\.", names(t), value = TRUE)
    dev <- max(abs(t$pop_total - t$pop_total_grid * t$pop_scale_census * t$pop_growth_county), na.rm = TRUE)
    cat(sprintf("%-30s i.cols=%-10s identity_dev=%.3g\n", f,
                if (length(leaked)) paste(leaked, collapse=",") else "none", dev))
    if (length(leaked) || !is.finite(dev) || dev > 1e-6) ok <- FALSE
  }
  cat(if (ok) "GATE PASS\n" else "GATE FAIL\n")'
```

**Expect:** `i.cols=none` on all three, `identity_dev` below 1e-6 on all three, `GATE PASS`.

Then prove idempotence — **re-run A1's dry run**. It must now report **no** `dropped stale join
artefact` lines (there is nothing left to drop) and **x1.0000** on all three. A second pass that
changes anything is a defect: **STOP**.

### A4 — publish tier 16 (background; this is an upload)

The interactive shell kills foreground commands after about two minutes, and a publish was SIGTERM'd
mid-run on 2026-09-28. Background it.

```bash
cd /home/jovyan/atlas/hazards_prototype
export STAMP=$(date +%Y%m%d_%H%M%S)
nohup Rscript R/observational/6_publish_obs_to_s3.R --full --tier 16 --overwrite \
  > logs/publish_t16_$STAMP.log 2>&1 &
echo $! > logs/publish_t16_$STAMP.pid
sleep 30; tail -20 logs/publish_t16_$STAMP.log
```

**Expect:** 3/3 objects uploaded, no error lines. Do not run `R/s3_upload.R` or pass
`--reference` / `--allow-schema-drift`.

### A5 — verify on S3 (read-only), then report

```bash
cd /home/jovyan/atlas/hazards_prototype
Rscript -e '
  suppressPackageStartupMessages(library(arrow)); library(data.table)
  b <- "https://digital-atlas.s3.amazonaws.com/domain=exposure/type=intersect/region=kenya/processing=analysis-ready/"
  for (f in c("exposure_gfm_seasonal.parquet","exposure_jrc_rp.parquet","exposure_totals.parquet")) {
    tf <- tempfile(fileext=".parquet"); download.file(paste0(b,f), tf, quiet=TRUE, mode="wb")
    t <- as.data.table(read_parquet(tf))
    cat(sprintf("%-30s ncol=%3d i.cols=%s\n", f, ncol(t),
        if (length(grep("^i\\.", names(t)))) paste(grep("^i\\.", names(t), value=TRUE), collapse=",") else "none"))
    if (f == "exposure_totals.parquet")
      cat(sprintf("  national pop_total=%.0f pop_source=%s pop_method=%s\n",
          sum(t$pop_total), t$pop_source[1], t$pop_method[1]))
  }'
```

**Expect:** `i.cols=none` on all three; national `pop_total` identical to A0; `pop_method` still
`county-level` (unchanged — Block B is parked).

**Report back** with a `### RESPONSE` block carrying the A0 and A5 national totals side by side, the
A1 and A3 x-factors, and the publish log tail. Then verify the response landed on `origin/develop`
(`git fetch`; `git log origin/develop..HEAD` empty; `git show origin/develop:DISPATCH_cglabs_pop_denominator_fixes.md | grep -c RESPONSE`).

### Notes

- `7b` sources `R/0_server_setup.R`, which calls `setwd(working_dir)`. The standalone
  `Rscript R/observational/7b_…R` form above is fine; the `Rscript -e` gates source by **absolute
  path** for that reason.
- `arrow` and `duckdb` must not both be attached in one R session. Every gate above uses `arrow` +
  `data.table` only.
- The KE-ENSO notebook team has been told to select columns explicitly as a temporary workaround
  (`HANDOVER_2026-10-06_ke-enso-exposure-denominator-answers.md`). Tell them when A5 is green so the
  workaround can come out.
