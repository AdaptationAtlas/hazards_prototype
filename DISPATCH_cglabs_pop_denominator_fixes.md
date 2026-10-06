# Dispatch: tier-16 population denominator fixes (#42 join artefact, #43 pop_method decision)

Append-only, **newest block on top**. Respond by prepending a `### RESPONSE` block.

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
