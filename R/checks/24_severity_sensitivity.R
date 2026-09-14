# Issue #24 option D — threshold/severity sensitivity study.
#
# Question: does ANY published severity class of the hazard-exposure product
# track the crop-model climate-change signal, or is the insensitivity found for
# Kenya in #19 a property of the product rather than of the severe class?
#
# Method (no re-bake; published parquets only):
#   numerator   domain=hazard_exposure .../variable=vop_usd15/period=annual,
#               hazard='any', hazard_vars='NDWS+NTx35+NDWL0', crop=maize,
#               model=historic and model=ENSEMBLE (scenario ssp585, 2041-2060),
#               severity = moderate | severe | extreme
#   denominator harmonized VoP, same currency (usd15), tech='all'
#               (per #23 the tech='all' slice is the sound one in that file)
#   reference   GYGA/WOFOST maize, SSP585, cultivar 'current', cycle 'both',
#               horizon 2005 (baseline Yw) and 2050 (= 2041-2060, % change)
#
# Reported per severity, across all African admin1 units that join:
#   - level and change of exposure (pp, historic -> SSP585 2041-60)
#   - saturation: share of units pinned at 0 % or 100 %
#   - Spearman vs GYGA % change  (does exposure rank like the crop model?)
#   - Spearman vs GYGA baseline Yw (does it just rank aridity?)
#   - the same correlations computed WITHIN country then pooled, so a
#     cross-country aridity gradient cannot manufacture the correlation
#
# Phase 2 (NOT done here, needs cglabs): recompute exposure at alternative
# thresholds (NTx30/NTx32, TAVG-based crop heat class) from the 04_indices
# outputs. This script only exercises what is already published.
#
# Usage:
#   GYGA_PARQUET_KEY=<32-char key> Rscript R/checks/24_severity_sensitivity.R
# See R/checks/19_exposure_vs_gyga_kenya.R for where the key comes from.
# Downloads (~350 MB) cached in $ISSUE19_CACHE (shared with the #19 script).

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(dplyr)
})
options(arrow.unsafe_metadata = TRUE, width = 200, scipen = 999)

t_start <- Sys.time()
log_msg <- function(...) cat(sprintf("[%s] [+%5.0fs] %s\n", format(Sys.time(), "%H:%M:%S"),
                                     as.numeric(difftime(Sys.time(), t_start, units = "secs")), paste0(...)))

project_dir <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else {
  a <- grep("^--file=", commandArgs(), value = TRUE)
  if (length(a)) normalizePath(file.path(dirname(sub("^--file=", "", a[1])), "..", "..")) else getwd()
}
out_dir <- file.path(project_dir, "R", "checks", "24_outputs")
cache   <- Sys.getenv("ISSUE19_CACHE", path.expand("~/.cache/hazards_prototype_issue19"))
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(cache, recursive = TRUE, showWarnings = FALSE)

fetch <- function(url, dest) {
  if (file.exists(dest) && file.size(dest) > 0) return(dest)
  log_msg("download ", basename(dest))
  if (download.file(url, dest, mode = "wb", quiet = TRUE, method = "libcurl") != 0) stop("download failed: ", url)
  dest
}
md_table <- function(dt, digits = 2) {
  dt <- as.data.frame(dt)
  fmt <- function(x) if (is.numeric(x)) ifelse(is.na(x), "NA", formatC(x, digits = digits, format = "f", drop0trailing = TRUE)) else ifelse(is.na(x), "", as.character(x))
  cells <- as.data.frame(lapply(dt, fmt), stringsAsFactors = FALSE)
  paste0(c(paste0("| ", paste(names(dt), collapse = " | "), " |"),
           paste0("|", paste(rep("---", ncol(dt)), collapse = "|"), "|"),
           apply(cells, 1, function(r) paste0("| ", paste(r, collapse = " | "), " |"))), collapse = "\n")
}
md <- c("# Issue #24 option D — severity sensitivity of the hazard-exposure product", "",
        paste0("Generated ", format(Sys.time(), "%Y-%m-%d %H:%M %Z"), " by R/checks/24_severity_sensitivity.R"), "")
add_md <- function(title, dt, note = NULL, digits = 2) md <<- c(md, paste0("## ", title), "", if (!is.null(note)) c(note, ""), md_table(dt, digits), "")

s3 <- "https://digital-atlas.s3.amazonaws.com/"
hz <- paste0(s3, "domain=hazard_exposure/source=atlas_cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_usd15/period=annual/")
severities <- strsplit(Sys.getenv("SEVERITIES", "moderate,severe,extreme"), ",")[[1]]

# ------------------------------------------------------ 1. exposure --------
# Queried straight off S3 with DuckDB httpfs: predicate pushdown + column
# projection fetch only the relevant row groups, so the three ENSEMBLE files
# (110 / 93 / 66 MB) never have to be downloaded. ~25 s per file vs ~75 min
# for a full pull on a slow link. DuckDB runs as a CLI subprocess, so it never
# shares an R session with arrow (they crash together in this environment).
log_msg("== 1. exposure, 3 severities, Africa-wide, maize (DuckDB over HTTPS) ==")
if (!nzchar(Sys.which("duckdb"))) stop("duckdb CLI required on PATH")
# One file per query, cached per file, with retries: a single UNION over all six
# is fragile (an HTTP range read occasionally returns a bad block and DuckDB
# reports "Snappy decompression failure"), and a retry then loses all the work.
extract_one <- function(model, sev, scen, tf, attempts = 4) {
  dest <- file.path(cache, sprintf("exposure_maize_%s_%s.csv", model, sev))
  if (file.exists(dest) && file.size(dest) > 0) return(fread(dest))
  # Prefer a verified local copy if one is cached; only reach over the network otherwise.
  local_pq <- file.path(cache, sprintf("hazexp_usd15_annual_%s_%s.parquet", model, sev))
  src <- if (file.exists(local_pq)) local_pq else sprintf("%smodel=%s/severity=%s/interaction.parquet", hz, model, sev)
  sql <- sprintf(paste("INSTALL httpfs; LOAD httpfs;",
                       "COPY (SELECT admin0_name, admin1_name, '%s' AS severity, scenario, timeframe, value",
                       "FROM read_parquet('%s')",
                       "WHERE crop='maize' AND hazard='any' AND hazard_vars='NDWS+NTx35+NDWL0'",
                       "AND scenario='%s' AND timeframe='%s'",
                       "AND admin1_name IS NOT NULL AND admin2_name IS NULL) TO '%s' (HEADER);"),
                 sev, src, scen, tf, dest)
  sf <- tempfile(fileext = ".sql"); writeLines(sql, sf); on.exit(unlink(sf), add = TRUE)
  for (i in seq_len(attempts)) {
    if (system2("duckdb", stdin = sf, stdout = NULL, stderr = "") == 0 && file.exists(dest)) {
      log_msg("  ", model, "/", sev, ": ", nrow(fread(dest)), " rows", if (i > 1) paste0(" (attempt ", i, ")") else "")
      return(fread(dest))
    }
    unlink(dest)
    log_msg("  ", model, "/", sev, ": attempt ", i, " failed, retrying")
    Sys.sleep(3 * i)
  }
  stop("remote exposure query failed after ", attempts, " attempts: ", model, "/", sev)
}
log_msg("querying 6 remote parquets (slow step, ~2-3 min; cached per file)")
ex <- rbindlist(lapply(severities, function(s) rbind(extract_one("historic", s, "historic", "historic"),
                                                     extract_one("ENSEMBLE", s, "ssp585", "2041-2060"))))
ex <- ex[is.finite(value)]   # historic files carry some NaN cells (see #12)
log_msg("exposure rows: ", nrow(ex), "; admin1 units: ", uniqueN(paste(ex$admin0_name, ex$admin1_name)))

den <- as.data.table(read_parquet(file.path(cache, "vop_nominal-usd-2015.parquet")))[
  exposure == "vop" & crop == "maize" & tech == "all" & !is.na(admin1_name) & is.na(admin2_name),
  .(admin0_name, admin1_name, vop_total = value)]
ex <- merge(ex, den, by = c("admin0_name", "admin1_name"))
ex <- ex[vop_total > 1e5]                                   # drop trivial-VoP units
ex[, pct := pmin(100, 100 * value / vop_total)]
w <- dcast(ex, admin0_name + admin1_name + severity + vop_total ~ scenario, value.var = "pct")
setnames(w, c("historic", "ssp585"), c("exp_hist", "exp_2050"), skip_absent = TRUE)
w[, d_pp := exp_2050 - exp_hist]
log_msg("units with a usable denominator: ", uniqueN(paste(w$admin0_name, w$admin1_name)))

# ---------------------------------------------------------- 2. GYGA --------
log_msg("== 2. GYGA reference ==")
key <- Sys.getenv("GYGA_PARQUET_KEY")
if (!nzchar(key)) stop("GYGA_PARQUET_KEY not set")
key <- gsub("[^A-Za-z0-9+/=_-]", "", key)
gy_file <- fetch(paste0(s3, "parquet_tests/ATLAS-all_granular_aggregation_yields_fixed_all_crops_data_all_admin_levels_both_cycles.parquet"),
                 file.path(cache, "gyga_aggregated_enc.parquet"))
gy_csv <- file.path(cache, "gyga_africa_maize_adm1_ssp585.csv")
if (!file.exists(gy_csv)) {
  sql <- sprintf(paste("PRAGMA add_parquet_key('k24','%s');",
                       "COPY (SELECT admin0_name, admin1_name, horizon, yield_rfd_avg, yield_rfd_rlt_change, yield_irr_rlt_change",
                       "FROM read_parquet('%s', encryption_config={footer_key:'k24'})",
                       "WHERE crop='maize' AND ssp='SSP585' AND cultivar='current' AND cycle='both'",
                       "AND admin2_name IS NULL AND admin1_name IS NOT NULL) TO '%s' (HEADER);"), key, gy_file, gy_csv)
  sf <- tempfile(fileext = ".sql"); writeLines(sql, sf)
  if (!nzchar(Sys.which("duckdb"))) stop("duckdb CLI required")
  if (system2("duckdb", stdin = sf, stdout = NULL, stderr = "") != 0) stop("GYGA decrypt failed - wrong key?")
  unlink(sf)
}
gy <- fread(gy_csv)
gref <- merge(gy[horizon == 2005, .(admin0_name, admin1_name, yw_base = yield_rfd_avg)],
              gy[horizon == 2050, .(admin0_name, admin1_name, yw_pct = yield_rfd_rlt_change, irr_pct = yield_irr_rlt_change)],
              by = c("admin0_name", "admin1_name"))
log_msg("GYGA admin1 units: ", nrow(gref))

j <- merge(w, gref, by = c("admin0_name", "admin1_name"))
j <- j[!is.na(yw_pct) & !is.na(exp_hist) & !is.na(exp_2050)]
log_msg("joined units per severity: ", nrow(j) / length(severities), " across ", uniqueN(j$admin0_name), " countries")
fwrite(j[order(severity, admin0_name, admin1_name)], file.path(out_dir, "24_africa_exposure_by_severity_vs_gyga.csv"))

# -------------------------------------------------- 3. sensitivity ---------
log_msg("== 3. severity sensitivity ==")
sp <- function(a, b) if (sum(complete.cases(a, b)) > 5) round(cor(a, b, method = "spearman", use = "complete.obs"), 2) else NA_real_
# within-country: rank inside each country, pool, so a continental aridity gradient cannot drive it
wc <- function(dt, xcol, ycol) {
  d <- dt[, .(x = get(xcol), y = get(ycol), admin0_name)][!is.na(x) & !is.na(y)]
  d <- d[, if (.N >= 4) .(rx = rank(x) - mean(rank(x)), ry = rank(y) - mean(rank(y))) else NULL, by = admin0_name]
  if (nrow(d) < 10) return(NA_real_)
  round(cor(d$rx, d$ry), 2)
}
res <- rbindlist(lapply(severities, function(s) {
  d <- j[severity == s]
  data.table(severity = s, units = nrow(d), countries = uniqueN(d$admin0_name),
             mean_exp_hist = round(mean(d$exp_hist), 1), mean_exp_2050 = round(mean(d$exp_2050), 1),
             vop_wtd_hist = round(sum(d$exp_hist * d$vop_total) / sum(d$vop_total), 1),
             vop_wtd_2050 = round(sum(d$exp_2050 * d$vop_total) / sum(d$vop_total), 1),
             median_d_pp = round(median(d$d_pp), 2), units_moving_gt5pp = sum(abs(d$d_pp) > 5),
             pct_saturated = round(100 * mean(d$exp_hist < 0.5 | d$exp_hist > 99.5), 1),
             rho_level_vs_ywpct = sp(d$exp_2050, d$yw_pct), rho_dpp_vs_ywpct = sp(d$d_pp, d$yw_pct),
             rho_level_vs_ywbase = sp(d$exp_2050, d$yw_base),
             rho_within_country_level_vs_ywpct = wc(d, "exp_2050", "yw_pct"),
             rho_within_country_dpp_vs_ywpct = wc(d, "d_pp", "yw_pct"),
             rho_within_country_level_vs_ywbase = wc(d, "exp_2050", "yw_base"))
}))
fwrite(res, file.path(out_dir, "24_severity_sensitivity_summary.csv"))
add_md("Severity sensitivity — Africa-wide, maize, SSP585 2041-60", res,
       note = paste("`pct_saturated` = share of units pinned at 0 % or 100 % in the historic period.",
                    "`rho_*_vs_ywpct` = Spearman against the GYGA rainfed potential-yield % change;",
                    "`rho_*_vs_ywbase` = against GYGA baseline potential yield.",
                    "`within_country` ranks inside each country before pooling, so a continental aridity gradient cannot manufacture the correlation."))
print(res)

log_msg("== 4. per-country detail (severe) ==")
byc <- j[severity == "severe", .(units = .N, mean_exp = round(mean(exp_2050), 1), median_d_pp = round(median(d_pp), 2),
                                 rho_vs_ywpct = sp(exp_2050, yw_pct)), by = admin0_name][units >= 5][order(rho_vs_ywpct)]
fwrite(byc, file.path(out_dir, "24_per_country_severe.csv"))
add_md("Per-country correlation, severe class (countries with >= 5 admin1 units)", byc)
print(byc)

writeLines(md, file.path(out_dir, "24_summary.md"))
log_msg("wrote ", length(list.files(out_dir)), " files to ", out_dir)
log_msg("DONE in ", round(as.numeric(difftime(Sys.time(), t_start, units = "mins")), 1), " min")
