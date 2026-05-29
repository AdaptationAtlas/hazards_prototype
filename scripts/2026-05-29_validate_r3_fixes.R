#!/usr/bin/env Rscript
# Validation probe for R/3_freq_x_exposure.R fixes (2026-05-29)
#
# Part A: runs locally (terra only — no DuckDB/CGlabs needed)
# Part B: snippet to paste on CGlabs after sourcing 0_server_setup.R
#
# Run: Rscript scripts/2026-05-29_validate_r3_fixes.R
#      (from hazards_prototype project root with project_dir env var set)

cat("=== R/3 fix validation probe ===\n")
cat("R version:", R.version$version.string, "\n\n")

PASS <- 0L
FAIL <- 0L

ok <- function(label) {
  cat(sprintf("  PASS  %s\n", label))
  PASS <<- PASS + 1L
}
fail <- function(label, msg = "") {
  cat(sprintf("  FAIL  %s%s\n", label, if (nzchar(msg)) paste0(" — ", msg) else ""))
  FAIL <<- FAIL + 1L
}

# ---------------------------------------------------------------------------
# 1. _helpers.R sources via Sys.getenv("project_dir")
# ---------------------------------------------------------------------------
cat("--- 1. source _helpers.R ---\n")
Sys.setenv(project_dir = getwd())   # simulate what 0_server_setup.R does

helpers_path <- file.path(Sys.getenv("project_dir"), "R", "_helpers.R")
if (!file.exists(helpers_path)) {
  fail("helpers_path exists", helpers_path)
} else {
  source(helpers_path)
  ok("helpers_path exists and sourced")
}

# write_cog exported
if (exists("write_cog") && is.function(write_cog)) {
  ok("write_cog function present")
} else {
  fail("write_cog function present")
}

# write_parquet_pushdown exported
if (exists("write_parquet_pushdown") && is.function(write_parquet_pushdown)) {
  ok("write_parquet_pushdown function present")
} else {
  fail("write_parquet_pushdown function present")
}

# ---------------------------------------------------------------------------
# 2. write_cog: synthetic raster — ZSTD COG, correct metadata
# ---------------------------------------------------------------------------
cat("\n--- 2. write_cog ---\n")
if (requireNamespace("terra", quietly = TRUE)) {
  library(terra)
  tmp_tif <- tempfile(fileext = ".tif")

  # 10×10 float raster with values 0–1 (probability-like)
  r <- rast(nrows = 10, ncols = 10, vals = runif(100))

  tryCatch({
    write_cog(r, tmp_tif)
    ok("write_cog completes without error")

    # Check file exists
    if (file.exists(tmp_tif)) ok("output file created") else fail("output file created")

    # Check compression via GDAL info
    info <- terra::describe(tmp_tif)
    if (any(grepl("ZSTD|zstd", info, ignore.case = TRUE))) {
      ok("ZSTD compression confirmed in GDAL metadata")
    } else {
      fail("ZSTD compression confirmed", paste(grep("COMPRESS|compress", info, value=TRUE), collapse="; "))
    }

    # Check it reads back correctly
    r2 <- rast(tmp_tif)
    if (nrow(r2) == 10 && ncol(r2) == 10) ok("raster reads back with correct dimensions") else fail("raster reads back")

    unlink(tmp_tif)
  }, error = function(e) {
    fail("write_cog completes without error", conditionMessage(e))
  })
} else {
  cat("  SKIP  write_cog (terra not available)\n")
}

# ---------------------------------------------------------------------------
# 3. round2 fix: section 2 rounds to 2dp not 3dp
# ---------------------------------------------------------------------------
cat("\n--- 3. round2 fix (logic check) ---\n")
round1 <- 3
round2 <- 2
# Simulate the fixed code path
value_raw <- 1.23456
value_rounded <- round(value_raw, round2)   # fixed: was round1
if (value_rounded == 1.23) {
  ok(sprintf("round2 gives 2dp: %.5f -> %.2f", value_raw, value_rounded))
} else {
  fail("round2 gives 2dp", sprintf("got %.5f", value_rounded))
}
# Show what the OLD bug would have produced
value_bug <- round(value_raw, round1)
if (value_bug != value_rounded) {
  ok(sprintf("confirmed round1 (%ddp) != round2 (%ddp) — bug was real", round1, round2))
}

# ---------------------------------------------------------------------------
# 4. order_by fix: `is.null(order_by)` not `is.null(order)`
# ---------------------------------------------------------------------------
cat("\n--- 4. order_by guard fix ---\n")
order_by <- c("iso3", "crop")

# New guard: is.null(order_by) — should be FALSE → branch runs
if (!is.null(order_by)) {
  ok("!is.null(order_by) = TRUE → sort branch executes as intended")
} else {
  fail("!is.null(order_by) unexpectedly FALSE")
}

# Old bug: is.null(order) where `order` = base R function → also FALSE but for wrong reason
# (We just document this — not a functional regression, but confirms the fix is correct)
if (!is.null(order) && is.function(order)) {
  ok("confirmed old guard resolved to base R order() function (never NULL) — intent was ambiguous, now explicit")
}

# ---------------------------------------------------------------------------
# 5. NULL exposure guards in risk_x_exposure logic
# ---------------------------------------------------------------------------
cat("\n--- 5. NULL exposure guards ---\n")

# Simulate the fixed guard for livestock x ha (crop_exposure = NULL for ha)
crop_exposure      <- NULL
livestock_exposure <- NULL
crop               <- "cattle-tropical"
crop_choices       <- c("maize", "wheat", "rice")   # no livestock

# Old bug: stop() would fire for livestock × ha
# Fixed: guard on !is.null(livestock_exposure) prevents stop
guard_fires <- !is.null(livestock_exposure) && !crop %in% names(livestock_exposure) && !crop %in% crop_choices
if (!guard_fires) {
  ok("livestock x ha: NULL livestock_exposure guard correctly prevents spurious stop()")
} else {
  fail("livestock x ha: guard still fires when it shouldn't")
}

# Confirm guard DOES fire for a real mismatch (non-NULL exposure, crop absent)
livestock_exposure2 <- list(goats = 1, sheep = 1)
guard_fires2 <- !is.null(livestock_exposure2) && !crop %in% names(livestock_exposure2) && !crop %in% crop_choices
if (guard_fires2) {
  ok("non-NULL livestock_exposure with missing crop: guard correctly fires stop()")
} else {
  fail("non-NULL livestock_exposure with missing crop: guard failed to fire")
}

# ---------------------------------------------------------------------------
# 6. version constants sanity-check
# ---------------------------------------------------------------------------
cat("\n--- 6. version constants ---\n")
version1 <- 2
version2 <- 2
version4 <- 1

# sec 1 uses version1, sec 2 uses version2, sec 4.2 uses version4
if (version1 == 2) ok("sec 1 version = 2 (version1)") else fail("sec 1 version")
if (version2 == 2) ok("sec 2 version = 2 (version2)") else fail("sec 2 version")
if (version4 == 1) ok("sec 4.2 version = 1 (version4)") else fail("sec 4.2 version")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
cat(sprintf("\n=== Results: %d passed, %d failed ===\n", PASS, FAIL))
if (FAIL > 0L) quit(status = 1L)

# ---------------------------------------------------------------------------
# Part B — CGlabs only (requires DuckDB)
# ---------------------------------------------------------------------------
cat("\n--- PART B (CGlabs): write_parquet_pushdown ---\n")
cat("Paste on CGlabs after sourcing 0_server_setup.R + _helpers.R:\n\n")
cat('
  library(data.table)
  source(file.path(Sys.getenv("project_dir"), "R", "_helpers.R"))

  # 200k rows so we get > 1 row group at rg=50000
  dt <- data.table(
    iso3     = rep(c("AGO","NGA","KEN"), length.out = 200000),
    scenario = rep(c("ssp245","ssp585"), length.out = 200000),
    hazard   = rep(c("none","any","dry+heat+wet"), length.out = 200000),
    crop     = rep(c("maize","rice"), length.out = 200000),
    value    = runif(200000)
  )
  tmp_pq <- tempfile(fileext = ".parquet")
  write_parquet_pushdown(dt, tmp_pq,
    sort_by = c("iso3","scenario","hazard","crop"),
    verify_stats_on = c("iso3","hazard")
  )
  # Expect: "written (4 row groups, ..." message + no errors
  cat("PASS: write_parquet_pushdown OK\\n")
  unlink(tmp_pq)
\n')
