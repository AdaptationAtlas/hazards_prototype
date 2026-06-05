# CR-119: ensemble_season_timeseries canonical (2026-06-05 12:00 UTC) — three regressions, permanent-fix plan

**Where:** `R/2.1_create_monthly_haz_tables.R` §3.3, `R/_helpers.R` (`write_parquet_pushdown`), `R/s3_upload.R`.
**Affected canonical:** `s3://digital-atlas/domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=timeseries_mean_month/timeframe=3months/period={1995-2014,2021-2040,2041-2060,2061-2080,2081-2100}/baseline=1995-2014/variable=ensemble_season_timeseries.parquet`.
**Blast radius:** every notebook reader (`notebooks/climateRationale/notebook.qmd` `futureProjections_dataAll`; `notebooks/sandbox/obs_month_overlay.qmd` `cmip6_future_data`). Binder Error on every fetch + `TProtocolException: Invalid data` on any aggregate.

**Acute action (do first, ~5 min):** restore previous canonical via S3 versioning. HEAD on the file shows `x-amz-version-id` set — bucket is versioned. List + restore the pre-2026-06-05-12:00 versions of all 5 files (4 futures + 1 historic):

```bash
aws s3api list-object-versions --bucket digital-atlas \
  --prefix 'domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=timeseries_mean_month/timeframe=3months/' \
  --query 'Versions[?contains(Key,`ensemble_season_timeseries.parquet`)].[Key,VersionId,LastModified,Size]' \
  --output table

# Per file, copy the prior VersionId to overwrite current latest:
aws s3api copy-object --bucket digital-atlas \
  --copy-source 'digital-atlas/<KEY>?versionId=<PRIOR_VID>' \
  --key '<KEY>'
```

Notebook side already defensively reverted its SELECT to legacy columns (q17/q83/n_models dropped — ribbon falls back to mean line via `??` fallback). Once canonical is restored, notebook works immediately. CR-060 ribbon code stays in place ready for the next clean rebake.

---

## What broke

### 1. `iso3` column dropped from canonical schema

**Symptom:** every notebook fetch (`WHERE iso3 = '<XXX>'` or `WHERE iso3 IN (...)`) throws:

```
Binder Error: Referenced column "iso3" not found in FROM clause!
Candidate bindings: "read_parquet.sd", "read_parquet.min", "read_parquet.q83"
```

**Verified by:** `DESCRIBE SELECT * FROM read_parquet('<file>') LIMIT 0` against `period=2021-2040`. 25 columns present (admin0_name, admin1_name, scenario, timeframe, year, hazard, season, baseline_name, mean, max, min, sd, q5..q95, mean_anomaly..q95_anomaly, n_models, models). **iso3 not in schema.**

**Root cause:** `R/2.1_create_monthly_haz_tables.R:738` aggregation by-clause:

```r
data_anomaly_ens <- data_anomaly[, list(
  mean = mean(value, na.rm = TRUE),
  …  # q5, q17, q50, q83, q95, etc.
),
by = list(admin0_name, admin1_name, scenario, timeframe, year, hazard, season, baseline_name)
]
```

`iso3` is not in the by-clause → dropped at this point.

`write_parquet_pushdown` at line 792 lists iso3 first in `sort_by`:

```r
sort_by = c("iso3", "admin0_name", "hazard", "scenario", "season", "year", "timeframe", "admin1_name")
```

but `R/_helpers.R:80` silently filters to columns that exist:

```r
sort_cols_present <- intersect(sort_by, names(tbl))
```

The comment at line 787-789 claims "iso3 is added downstream by the publisher" but `R/s3_upload.R` has **zero** iso3-related code. Either a `data-management` helper was removed, or the comment was aspirational.

**Fix:** add iso3 to the §3.3 by-clause (and make sure upstream `data_anomaly` carries it; if not, propagate up from §3.2 or wherever the per-model anomaly table is built).

```r
by = list(iso3, admin0_name, admin1_name, scenario, timeframe, year, hazard, season, baseline_name)
```

Same fix in §3.3's sibling aggregation for `data_ag_ens` (line 769):

```r
by = list(iso3, admin0_name, admin1_name, scenario, timeframe, hazard, season, baseline_name)
```

### 2. Per-file size inflated ~14× (~20 MB → ~295 MB per period)

**Verified by:** `curl -I` on `period=2021-2040` returns `Content-Length: 294633355`. Old canonical (pre-2026-05-27) was ~20 MB per the existing comment in the notebook's `cmip6_future_data` spinner copy.

**Root cause #1 — `models` column replicated per row** at `R/2.1:744`:

```r
data_anomaly_ens[, models := models]
```

`models` is the comma-joined GCM list ("ACCESS-CM2,ACCESS-ESM1-5,…"), ~250 bytes per row. At admin1 × scenario × year × season × hazard granularity that's millions of rows. ~150-250 MB of bytes spent storing the same string. DuckDB `COPY TO PARQUET` does dictionary-encode varchars when cardinality is low — but a low-cardinality varchar dictionary-encoded *still* writes one index per row. The dictionary is small but the column chunks aren't.

**Root cause #2 — schema doubling.** Original ensemble schema was `mean, mean_anomaly, sd, sd_anomaly` (4 numeric). The 2026-05-27 republish added `min, max, min_anomaly, max_anomaly` (8). CR-060 added `q5, q17, q50, q83, q95` + `_anomaly` twins + `n_models` (19 total numeric). Each column adds its own row-group statistics, encoding metadata, and column-chunk overhead in the thrift footer.

**Fix A — move `models` out of the data:**

```r
# After write_parquet_pushdown:
arrow::write_parquet(
  # OR: re-open with arrow + rewrite metadata
  …,
  metadata = list(
    models         = unique(data_anomaly_ens$models)[1],   # the joined GCM list
    n_models_total = length(strsplit(unique(data_anomaly_ens$models)[1], ",")[[1]]),
    schema_version = "cr060_v2"
  )
)
```

Easier: drop `models` from the data.table before `write_parquet_pushdown`, write the parquet, then `arrow::write_parquet_metadata()` (or re-write the file via `arrow::open_dataset() %>% arrow::write_dataset()`). The notebook side reads it once via `arrow::ParquetFile$metadata()` and shows it in captions / "About this plot" — no per-row cost.

**Expected win:** −40 % per file size before any other change. ~295 MB → ~180 MB.

**Fix B — check column encoding.** DuckDB `COPY TO PARQUET` doesn't expose `use_dictionary` or `column_encoding` knobs as cleanly as `arrow::write_parquet()`. If the size still feels wrong after Fix A, switch §3.3's write path to `arrow::write_parquet(use_dictionary = TRUE, compression = "zstd", compression_level = 9)` and benchmark. Dictionary encoding on `scenario` / `season` / `hazard` / `admin0_name` / `admin1_name` (all low-to-medium cardinality varchars) is the lever.

### 3. Thrift corruption — aggregates throw `Invalid data`

**Symptom:**

```sql
SELECT * FROM read_parquet('<file>') LIMIT 5;   -- works
SELECT DISTINCT admin0_name FROM read_parquet('<file>');   -- Invalid Error: TProtocolException: Invalid data
SELECT admin0_name, COUNT(*) FROM read_parquet('<file>') GROUP BY 1;   -- same
```

Single-row reads hit valid row groups. Aggregates have to walk the full thrift footer, which is dangling somewhere — symptom is consistent with one writer's row-group bodies left on disk under a second writer's footer.

**Suspect cause:** commit `9d54147` (2026-06-03) — sec 3.3 parallelised with `future_lapply(file_combos, …, future.seed = TRUE, future.scheduling = worker_n2)`. If two workers write to the same `save_file2` path (e.g. two `file_combos` rows resolve to the same output filename), the second writer truncates and overwrites the footer but the first writer's row-group page bodies remain. DuckDB's footer pointers then reference offsets that decode to garbage thrift on aggregate scans.

To verify: `git diff 6fa5424 9d54147 -- R/2.1_create_monthly_haz_tables.R` and check whether `save_file2` is guaranteed-unique across `file_combos`. If `file_combos` has duplicate `(scenario, timeframe, hazard, baseline)` rows with different `model` columns but the §3.3 aggregation drops `model`, then `save_file2` collapses and workers race.

**Fix C — three options, ranked by safety:**
1. **Serialise §3.3 again.** Drop the `future_lapply` parallelism; revert to `lapply()`. Sec 3.3 was running in single-digit minutes pre-parallel — acceptable for the cost of avoiding the race entirely.
2. **Assert unique save paths before launch.** Add a `stopifnot(!any(duplicated(file_combos$save_file2)))` (or whatever the per-group save path expression is) before `future_lapply`. Fast-fails if the worker domain isn't safely partitioned.
3. **Per-worker temp paths + atomic rename.** Worker writes to `<save_file2>.<worker_pid>.tmp`; controller-side post-step concatenates per-iso3 (see Phase 2 below) and renames into place. Safest but heaviest refactor.

Recommend (1) for the immediate next rebake. Move to (2) or restructure to per-iso3 partitioning (Phase 2 below) for the durable fix.

---

## Permanent fix — three-change architecture, ranked by leverage

### Change 1 (Phase 1, do with the next rebake) — drop `models` from rows, fix iso3, add probe

Already covered above. Combined effect:
- iso3 in schema (1 column added; trivial bytes).
- `models` moved to file kv-metadata (~−40 % file size).
- `probe_parquet()` smoke test added to `R/s3_upload.R` — catches all three of today's regressions before they ship (schema check + aggregate-scan check + size sanity).

Skeleton for the smoke test:

```r
# R/utils/probe_parquet.R
probe_parquet <- function(path, required_cols = c("iso3","admin0_name","admin1_name",
                                                   "scenario","season","year","hazard",
                                                   "mean","mean_anomaly","sd_anomaly"),
                          max_size_ratio = 2.0) {
  drv <- duckdb::duckdb(dbdir = ":memory:")
  con <- DBI::dbConnect(drv); on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # 1. Schema check
  schema <- DBI::dbGetQuery(con, sprintf("DESCRIBE SELECT * FROM read_parquet('%s') LIMIT 0", path))
  missing <- setdiff(required_cols, schema$column_name)
  if (length(missing) > 0) stop(sprintf("probe_parquet: schema missing %s", paste(missing, collapse=",")))

  # 2. Aggregate-scan check — catches thrift corruption
  tryCatch(
    DBI::dbGetQuery(con, sprintf("SELECT iso3, COUNT(*) AS n FROM read_parquet('%s') GROUP BY iso3", path)),
    error = function(e) stop(sprintf("probe_parquet: aggregate scan failed (thrift corruption likely): %s", e$message))
  )

  # 3. Size canary — fail if file is >max_size_ratio× the previous version
  # (Implementation TBD — needs a per-file size baseline. For now warn-only.)
  invisible(TRUE)
}
```

Wire it into `R/s3_upload.R` before each `aws s3 cp` call. If it throws, halt the publish.

### Change 2 (Phase 2, next-next rebake) — per-iso3 hive partitioning

**Why:** Future Projections is fundamentally per-country UX. The user picks a country, the chart redraws for that country. Reading the other 53 countries' bytes is pure waste. Even after Phase 1 the file is ~180 MB; a single-country fetch should be ~5 MB. The cleanest expression of that is hive partition on iso3.

**New layout:**

```
domain=climate/.../period=2021-2040/baseline=1995-2014/
  variable=ensemble_season_timeseries/
    iso3=KEN/data.parquet      ~5 MB
    iso3=TZA/data.parquet
    …
    iso3=DZA/data.parquet
```

**Pipeline change** (§3.3, after aggregation):

```r
iso3_codes <- unique(data_anomaly_ens$iso3)
for (iso3_code in iso3_codes) {
  iso3_dir <- file.path(out_dir, sprintf("variable=ensemble_season_timeseries/iso3=%s", iso3_code))
  dir.create(iso3_dir, recursive = TRUE, showWarnings = FALSE)
  write_parquet_pushdown(
    data_anomaly_ens[iso3 == iso3_code],
    file.path(iso3_dir, "data.parquet"),
    sort_by         = c("hazard", "scenario", "season", "year", "admin1_name"),
    verify_stats_on = c("hazard", "scenario", "season")
  )
}
```

This **also** sidesteps the §3.3 parallelism race: workers naturally partition by iso3, so no two ever write the same path.

**Sizing math** (SSA worst case):
- ~600 admin1s / 54 countries ≈ 11 admin1s per country
- 11 admin1s × 4 hazards × 5 seasons × 4 SSPs × 20 years × 1 period = 35,200 rows per period × iso3
- After Phase 1 column slim (no `models`): ~5–10 MB per file at ZSTD-9
- 54 files × ~5 MB = ~270 MB total for one period across all SSA. Half today's single file — but split, so any consumer fetches only what they need.

**Notebook adaptation** (single-line URL change):

```js
// Before (today's layout):
read_parquet('.../period=2021-2040/.../variable=ensemble_season_timeseries.parquet') WHERE iso3 = 'KEN'

// After:
read_parquet('.../period=2021-2040/.../variable=ensemble_season_timeseries/iso3=KEN/data.parquet')
// — no WHERE iso3 needed, file path IS the filter; hive_partitioning=true makes iso3 a virtual column.
```

For region scopes, fetch N files in parallel:

```js
const urls = iso3List.map(c => `${base}/period=${p}/.../iso3=${c}/data.parquet`);
const sql  = `SELECT * FROM read_parquet([${urls.map(u => `'${u}'`).join(",")}])`;
```

### Change 3 (Phase 1, do alongside Change 1) — `write_parquet_pushdown` knobs

In `R/_helpers.R`:

1. **Drop `row_group_size` to ~25,000 OR raise to ~250,000.** 50,000 was a middle-ground guess. For the post-slim ensemble file (~180 MB / period, ~3 M rows) the footer overhead at 50k rg-size is ~60 row groups; at 250k it's ~12 row groups (smaller footer, fewer thrift records). For per-iso3 files (~35k rows each) 50k means single row group → no pushdown anyway, so the small-table branch at line 124 kicks in.
2. **Add a `verify_aggregate` post-write check.** After the `parquet_metadata` validation that's already there, run `SELECT COUNT(*) FROM read_parquet('<path>')` and assert it returns equal to `nrow(tbl)`. Catches today's thrift corruption at the source — `write_parquet_pushdown` refuses to return a known-bad file.

```r
# At the end of write_parquet_pushdown, after the existing verify_stats_on loop:
verify_count <- DBI::dbGetQuery(con, sprintf("SELECT COUNT(*) AS n FROM read_parquet('%s')", out_path))
if (verify_count$n != nrow(tbl)) {
  stop(sprintf("write_parquet_pushdown: %s row count post-write %d != input %d (corruption?)",
               out_path, verify_count$n, nrow(tbl)))
}
```

That single line, run inline, would have caught today's regression at the pipeline step instead of at the user's browser.

---

## Acceptance criteria for the next rebake

Before publishing to canonical:

1. **`probe_parquet()` returns TRUE** for every output of §3.3 (4 futures + 1 historic file, or 4×54 + 1×54 = 220 files under per-iso3 layout).
2. **DuckDB-WASM smoke test** — open one of the new files in the browser via the notebook sandbox; full `SELECT *` succeeds, `SELECT COUNT(*) GROUP BY iso3` succeeds (per the memory rule "standalone DuckDB success is NOT a sufficient smoke test").
3. **Size canary** — per-file size is within 2× of the previous canonical's size baseline (recorded somewhere — perhaps a small `parquet_sizes.json` checked into the pipeline repo, updated on each successful publish).
4. **Notebook side** — `notebooks/climateRationale/notebook.qmd` + `notebooks/sandbox/obs_month_overlay.qmd` re-add the q17/q83/n_models columns to the `futureProjections_dataAll` / `cmip6_future_data` SELECT and the CR-060 ribbon swap turns on automatically.

---

## Out of scope here

- CR-061 (Recent Changes ribbon) — closed obsolete 2026-06-05 (Recent Changes is observational since the 2026-05-21 commit `5c730e2` in `atlas_notebooks`).
- CR-058 — per-iso3 partitioning of *all* hazard parquets, not just the climate timeseries. Phase 2 here is the climate timeseries; if it works well, generalise.

## Cross-refs

- `atlas_notebooks` ISSUES.md CR-119 (notebook-side) — pairs with this.
- `atlas_notebooks/playbook/handovers/climateRationale/dispatches/2026-06-05_cr060-parquet-regression.md` — first-pass diagnosis (now superseded by this combined doc + the architectural sibling).
- `atlas_notebooks/playbook/handovers/climateRationale/dispatches/2026-06-05_cmip6-parquet-permanent-fix.md` — notebook-side mirror of the architectural plan.
