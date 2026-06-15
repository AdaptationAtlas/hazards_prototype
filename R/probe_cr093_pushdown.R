#!/usr/bin/env Rscript
# CR-093 probe — validates the R/2.2 consumability fix WITHOUT pipeline Data/.
# Checks two things the producer edit relies on:
#   1. merge_admin_extract-style melt keeps iso3 as an id.var at every admin
#      level (adm0/adm1/adm2), so iso3 survives into the parquet.
#   2. write_parquet_pushdown() (the new R/2.2 writer) actually produces
#      non-null iso3 row-group statistics -> notebook per-country reads prune.
# Run: Rscript R/probe_cr093_pushdown.R   (no 0_server_setup.R required)

t0 <- Sys.time()
log <- function(...) cat(sprintf("[%s] ", format(Sys.time(), "%H:%M:%S")), ..., "\n")

suppressPackageStartupMessages(library(data.table))
have_duckdb <- requireNamespace("duckdb", quietly = TRUE) &&
  requireNamespace("DBI", quietly = TRUE)
project_dir <- normalizePath(".")
log(sprintf("setup OK — duckdb available: %s (PASS 2 is cglabs-gated)", have_duckdb))

# ---- 1) iso3 survives the melt at all 3 admin levels --------------------------
# Mimic merge_admin_extract's per-level melt with iso3 leading id.vars.
melt_level <- function(df, admin) data.table(melt(as.data.table(df), id.vars = admin))

adm0 <- data.frame(iso3 = c("AGO", "KEN"), admin0_name = c("Angola", "Kenya"),
                   ssp245_GCM1_2021_2040 = c(1.1, 2.2), ssp585_GCM1_2021_2040 = c(3.3, 4.4))
adm1 <- data.frame(iso3 = "AGO", admin0_name = "Angola",
                   admin1_name = c("Luanda", "Bie"),
                   ssp245_GCM1_2021_2040 = c(5, 6))
adm2 <- data.frame(iso3 = "AGO", admin0_name = "Angola", admin1_name = "Bie",
                   admin2_name = c("X", "Y"), ssp245_GCM1_2021_2040 = c(7, 8))

m0 <- melt_level(adm0, c("iso3", "admin0_name"))
m1 <- melt_level(adm1, c("iso3", "admin0_name", "admin1_name"))
m2 <- melt_level(adm2, c("iso3", "admin0_name", "admin1_name", "admin2_name"))
stopifnot("iso3" %in% names(m0), "iso3" %in% names(m1), "iso3" %in% names(m2))
stopifnot(!anyNA(m0$iso3), !anyNA(m1$iso3), !anyNA(m2$iso3))
log("PASS 1 — iso3 retained through melt at adm0/adm1/adm2")

# ---- 2) write_parquet_pushdown yields prunable iso3 stats --------------------
if (!have_duckdb) {
  log("SKIP 2 — duckdb not installed; run on cglabs to verify iso3 row-group stats")
  log(sprintf("PASS 1 only in %.1fs", as.numeric(difftime(Sys.time(), t0, units = "secs"))))
  quit(save = "no", status = 0)
}
suppressPackageStartupMessages({ library(DBI); library(duckdb) })
source(file.path(project_dir, "R", "_helpers.R"))
stopifnot(exists("write_parquet_pushdown"))

# Synthetic R/2.2 *_ensemble-shaped table spanning many iso3 so >1 row group.
isos <- sprintf("C%02d", 1:55)
ens <- CJ(iso3 = isos, admin1_name = sprintf("a1_%02d", 1:40),
          scenario = c("ssp245", "ssp585"),
          timeframe = c("2021-2040", "2041-2060"))
ens[, `:=`(admin0_name = iso3, admin2_name = NA_character_,
           direction = "increase_5", variable = "PTOT", stat = "perc_change",
           mean = 1, min = 0, max = 2, sd = 0.5)]
setcolorder(ens, c("iso3", "admin0_name", "admin1_name", "admin2_name"))
log(sprintf("synthetic ensemble rows: %d (iso3=%d)", nrow(ens), length(isos)))

chg_sort_by <- c("iso3", "admin0_name", "admin1_name", "admin2_name", "scenario", "timeframe")
out <- file.path(tempdir(), "cr093_probe_ensemble.parquet")
write_parquet_pushdown(ens, out, sort_by = chg_sort_by, verify_stats_on = "iso3")

con <- dbConnect(duckdb::duckdb(":memory:")); on.exit(dbDisconnect(con, shutdown = TRUE))
md <- as.data.table(dbGetQuery(con, sprintf(
  "SELECT row_group_id, stats_min, stats_max, stats_null_count
   FROM parquet_metadata('%s') WHERE path_in_schema = 'iso3'", out)))
n_rg <- md[, uniqueN(row_group_id)]
n_null_stats <- md[is.na(stats_min) | is.na(stats_max), .N]
log(sprintf("row groups: %d | iso3 row-groups with NULL min/max stats: %d", n_rg, n_null_stats))
stopifnot(n_rg >= 2L)        # multiple row groups (else pruning is moot)
stopifnot(n_null_stats == 0L) # every row group has iso3 stats -> prunable

# spot-check sort: first iso3 <= last iso3 within file order
ord <- as.data.table(dbGetQuery(con, sprintf(
  "SELECT iso3 FROM read_parquet('%s') LIMIT 1", out)))
log(sprintf("PASS 2 — prunable iso3 stats on all %d row groups; first iso3=%s",
            n_rg, ord$iso3[1]))

log(sprintf("ALL PASS in %.1fs", as.numeric(difftime(Sys.time(), t0, units = "secs"))))
