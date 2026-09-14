# Issue #19 reproduction — "Evaluate Climate Risks" (hazard exposure, % of VoP)
# vs "View Projected Climate Impacts" (GYGA/WOFOST projected yield) for Kenyan
# maize. Rebuilds the numbers a user sees on the live Atlas for Nandi, Bungoma,
# Narok and Meru, then joins exposure and yield change for all 47 counties.
#
# Live-site sources (AdaptationAtlas/atlas_notebooksV1):
#   whatsAtRisk/notebook.qmd     -> repo-local haz_risk_vop_int_gzip.parquet,
#                                   severity hard-coded 'severe',
#                                   % = value / sum(value where hazard != 'any')
#   projectedImpacts/notebook.qmd -> Observable 876b0cd897b4c214 -> view
#                                   `aggregated` = DuckDB-ENCRYPTED parquet on S3
#                                   (GYGA licence guard). yield_rfd_* = rainfed
#                                   water-limited POTENTIAL yield, not farm yield.
#
# Outputs (R/checks/19_outputs/):
#   19_v1_exposure_kenya_maize.csv        V1 exposure, all 47 counties x scenarios x hazards
#   19_v1_reproduction_4counties.csv      wide table, 4 counties, historic + SSP585 2041-60
#   19_gyga_reproduction_4counties.csv    GYGA SSP585, all horizons/cycles, current cultivar
#   19_kenya47_exposure_vs_gyga.csv       joined county table (SSP585 2041-60 / 2050)
#   19_correlations.csv                   Spearman/Pearson across counties
#   19_exposure_band_means.csv            GYGA change by exposure band
#   19_new_pipeline_4counties.csv         2025-07 bake (domain=hazard_exposure) same cells
#   19_harmonized_vop_all_lt_rfall.csv    tech=all < rf-all audit of harmonized VoP files
#   19_summary.md                         all tables as markdown
#
# Usage (from project root):
#   GYGA_PARQUET_KEY=<32-char key> Rscript R/checks/19_exposure_vs_gyga_kenya.R
#   Rscript R/checks/19_exposure_vs_gyga_kenya.R --skip-new-pipeline   # skip 100 MB download
#   Rscript R/checks/19_exposure_vs_gyga_kenya.R --skip-gyga           # exposure half only
#
# GYGA_PARQUET_KEY is NOT stored in this repo. It is the concatenation of the
# three `.value` cells (duckVersion, duckdb, query) in Observable notebook
# 876b0cd897b4c214 ("Africa's Future Harvest Database Setup"), which the live
# notebook uses client-side. Ask Brayden / Sebastián. GYGA data are licensed:
# do not commit raw dumps; only the county-level values already shown on the
# live site are written to 19_outputs.
#
# Requirements: data.table, arrow (>= 12), dplyr; and ONE of: `duckdb` CLI on
# PATH, or the duckdb R package (used in a child process because arrow and
# duckdb R must not be loaded together in this repo's environment).
# Downloads (~140 MB total) are cached in $ISSUE19_CACHE
# (default ~/.cache/hazards_prototype_issue19) and reused on re-run.

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(dplyr)
})
options(arrow.unsafe_metadata = TRUE, width = 200, scipen = 999)

# ---------------------------------------------------------------- setup ----
t_start <- Sys.time()
log_msg <- function(...) {
  cat(sprintf("[%s] [+%5.0fs] %s\n", format(Sys.time(), "%H:%M:%S"),
              as.numeric(difftime(Sys.time(), t_start, units = "secs")),
              paste0(...)))
}
args <- commandArgs(trailingOnly = TRUE)
skip_new  <- "--skip-new-pipeline" %in% args
skip_gyga <- "--skip-gyga" %in% args

project_dir <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else {
  # walk up from the script location if run via Rscript, else getwd()
  a <- grep("^--file=", commandArgs(), value = TRUE)
  if (length(a)) normalizePath(file.path(dirname(sub("^--file=", "", a[1])), "..", "..")) else getwd()
}
out_dir <- file.path(project_dir, "R", "checks", "19_outputs")
cache   <- Sys.getenv("ISSUE19_CACHE", path.expand("~/.cache/hazards_prototype_issue19"))
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(cache,   recursive = TRUE, showWarnings = FALSE)
log_msg("project_dir=", project_dir)
log_msg("out_dir=", out_dir, "  cache=", cache)

focus <- c("Nandi", "Bungoma", "Narok", "Meru")

fetch <- function(url, dest) {
  if (file.exists(dest) && file.size(dest) > 0) {
    log_msg("cached  ", basename(dest), " (", round(file.size(dest) / 1e6, 1), " MB)")
    return(dest)
  }
  log_msg("download ", basename(dest), " <- ", url)
  ok <- tryCatch(download.file(url, dest, mode = "wb", quiet = TRUE, method = "libcurl"), error = function(e) 1L)
  if (ok != 0 || !file.exists(dest)) stop("download failed: ", url)
  log_msg("   done  ", round(file.size(dest) / 1e6, 1), " MB")
  dest
}

md_table <- function(dt, digits = 2) {
  dt <- as.data.frame(dt)
  fmt <- function(x) if (is.numeric(x)) ifelse(is.na(x), "NA", formatC(x, digits = digits, format = "f", drop0trailing = TRUE)) else ifelse(is.na(x), "", as.character(x))
  cells <- as.data.frame(lapply(dt, fmt), stringsAsFactors = FALSE)
  paste0(c(paste0("| ", paste(names(dt), collapse = " | "), " |"),
           paste0("|", paste(rep("---", ncol(dt)), collapse = "|"), "|"),
           apply(cells, 1, function(r) paste0("| ", paste(r, collapse = " | "), " |"))),
         collapse = "\n")
}
summary_md <- c("# Issue #19 — exposure vs GYGA reproduction (Kenya maize)", "",
                paste0("Generated ", format(Sys.time(), "%Y-%m-%d %H:%M %Z"), " by R/checks/19_exposure_vs_gyga_kenya.R"), "")
add_md <- function(title, dt, note = NULL, digits = 2) {
  summary_md <<- c(summary_md, paste0("## ", title), "", if (!is.null(note)) c(note, ""), md_table(dt, digits), "")
}

# ---------------------------------------------- 1. V1 exposure (live) ------
log_msg("== 1. V1 'Evaluate Climate Risks' exposure ==")
v1_base <- "https://raw.githubusercontent.com/AdaptationAtlas/atlas_notebooksV1/main/notebooks/whatsAtRisk/"
v1_haz  <- fetch(paste0(v1_base, "haz_risk_vop_int_gzip.parquet"), file.path(cache, "v1_haz_risk_vop_int_gzip.parquet"))
v1_exp  <- fetch(paste0(v1_base, "exposure_adm_sum_sorted.parquet"), file.path(cache, "v1_exposure_adm_sum_sorted.parquet"))

v1 <- open_dataset(v1_haz) %>%
  filter(admin0_name == "Kenya", crop == "maize", !is.na(admin1_name), is.na(admin2_name)) %>%
  collect() %>% as.data.table()
log_msg("V1 Kenya maize admin1 rows: ", nrow(v1), "; counties: ", uniqueN(v1$admin1_name),
        "; severity: ", paste(unique(v1$severity), collapse = ","))
# notebook: vop_total = sum(value) over hazard != 'any'  (includes 'no hazard')
v1_tot <- v1[hazard != "any", .(vop_total = sum(value)), by = .(admin1_name, scenario, timeframe)]
v1 <- merge(v1, v1_tot, by = c("admin1_name", "scenario", "timeframe"))
v1[, pct := 100 * value / vop_total]
fwrite(v1[order(admin1_name, scenario, timeframe, hazard),
          .(admin1_name, scenario, timeframe, severity, hazard, vop = value, vop_total, pct = round(pct, 2))],
       file.path(out_dir, "19_v1_exposure_kenya_maize.csv"))

v1[, vop_total_M := round(vop_total / 1e6, 1)]
v1_wide <- dcast(v1[admin1_name %in% focus & ((scenario == "historic") | (scenario == "ssp585" & timeframe == "2041_2060"))],
                 admin1_name + scenario + timeframe + vop_total_M ~ hazard, value.var = "pct")
for (j in setdiff(names(v1_wide), c("admin1_name", "scenario", "timeframe", "vop_total_M"))) set(v1_wide, j = j, value = round(v1_wide[[j]], 1))
fwrite(v1_wide, file.path(out_dir, "19_v1_reproduction_4counties.csv"))
add_md("V1 exposure — % of maize VoP exposed, severe (live notebook file)", v1_wide, digits = 1)
print(v1_wide)

# ------------------------------------------------- 2. GYGA (live) ----------
gy <- NULL
if (skip_gyga) {
  log_msg("== 2. GYGA skipped (--skip-gyga) ==")
} else {
  log_msg("== 2. GYGA 'View Projected Climate Impacts' ==")
  key <- Sys.getenv("GYGA_PARQUET_KEY")
  if (!nzchar(key)) {
    log_msg("GYGA_PARQUET_KEY not set -> skipping GYGA half. See header for where the key lives.")
  } else {
    key <- gsub("[^A-Za-z0-9+/=_-]", "", key)   # defensive: no quotes into SQL
    gy_url <- "https://digital-atlas.s3.amazonaws.com/parquet_tests/ATLAS-all_granular_aggregation_yields_fixed_all_crops_data_all_admin_levels_both_cycles.parquet"
    gy_file <- fetch(gy_url, file.path(cache, "gyga_aggregated_enc.parquet"))
    gy_csv  <- file.path(cache, "gyga_kenya_maize_adm1.csv")   # stays in cache, NOT in repo
    sql <- sprintf(paste(
      "PRAGMA add_parquet_key('k19', '%s');",
      "COPY (SELECT admin1_name, ssp, horizon, cycle, cultivar,",
      "        yield_rfd_avg, yield_rfd_abs_change, yield_rfd_rlt_change,",
      "        yield_irr_avg, yield_irr_abs_change, yield_irr_rlt_change",
      "      FROM read_parquet('%s', encryption_config = {footer_key: 'k19'})",
      "      WHERE admin0_name = 'Kenya' AND crop = 'maize' AND admin2_name IS NULL AND admin1_name IS NOT NULL)",
      "TO '%s' (HEADER);"), key, gy_file, gy_csv)
    sql_file <- tempfile(fileext = ".sql"); writeLines(sql, sql_file)
    if (nzchar(Sys.which("duckdb"))) {
      log_msg("decrypting with duckdb CLI ", trimws(system2("duckdb", "--version", stdout = TRUE)))
      rc <- system2("duckdb", stdin = sql_file, stdout = NULL, stderr = "")
    } else if (requireNamespace("duckdb", quietly = TRUE)) {
      log_msg("decrypting with duckdb R package in a child process (arrow/duckdb must not share a session)")
      rc <- system2(file.path(R.home("bin"), "Rscript"),
                    c("-e", shQuote(sprintf("con<-DBI::dbConnect(duckdb::duckdb()); for (s in strsplit(paste(readLines('%s'),collapse=' '), ';')[[1]]) if (nzchar(trimws(s))) DBI::dbExecute(con, s); DBI::dbDisconnect(con, shutdown=TRUE)", sql_file))),
                    stdout = NULL, stderr = "")
    } else stop("need duckdb CLI on PATH or the duckdb R package")
    unlink(sql_file)
    if (rc != 0 || !file.exists(gy_csv)) stop("GYGA decrypt/query failed (rc=", rc, ") - wrong key?")
    gy <- fread(gy_csv)
    log_msg("GYGA Kenya maize admin1 rows: ", nrow(gy), "; counties: ", uniqueN(gy$admin1_name),
            "; ssp: ", paste(unique(gy$ssp), collapse = ","), "; horizons: ", paste(sort(unique(gy$horizon)), collapse = ","))

    gy4 <- gy[admin1_name %in% focus & ssp == "SSP585" & cultivar == "current"][order(admin1_name, cycle, horizon),
              .(admin1_name, horizon, cycle, yw_rfd = round(yield_rfd_avg, 2), rfd_abs_chg = round(yield_rfd_abs_change, 2),
                rfd_pct_chg = round(yield_rfd_rlt_change, 1), yw_irr = round(yield_irr_avg, 2), irr_pct_chg = round(yield_irr_rlt_change, 1))]
    fwrite(gy4, file.path(out_dir, "19_gyga_reproduction_4counties.csv"))
    add_md("GYGA — maize, SSP585, cultivar current (live notebook table `aggregated`)", gy4,
           note = "yw_rfd = rainfed water-limited POTENTIAL yield (t/ha), yw_irr = irrigated potential yield. horizon 2005 = 1995-2014 baseline, 2050 = 2041-2060.")
    print(gy4)
  }
}

# ------------------------------------- 3. join + correlations (47 cty) -----
if (!is.null(gy)) {
  log_msg("== 3. Kenya-wide join: exposure (SSP585 2041-60) vs GYGA (SSP585 2050, both, current) ==")
  e50 <- dcast(v1[scenario == "ssp585" & timeframe == "2041_2060"], admin1_name + vop_total ~ hazard, value.var = "value")
  e50[, `:=`(pct_any  = 100 * any / vop_total,
             pct_dry  = 100 * (dry + `dry+heat` + `dry+wet` + `dry+heat+wet`) / vop_total,
             pct_heat = 100 * (heat + `dry+heat` + `heat+wet` + `dry+heat+wet`) / vop_total,
             pct_wet  = 100 * (wet + `dry+wet` + `heat+wet` + `dry+heat+wet`) / vop_total)]
  eh <- v1[scenario == "historic" & hazard == "any", .(admin1_name, pct_any_hist = pct)]
  g50 <- gy[ssp == "SSP585" & horizon == 2050 & cycle == "both" & cultivar == "current",
            .(admin1_name, yw_2050 = yield_rfd_avg, yw_abs = yield_rfd_abs_change, yw_pct = yield_rfd_rlt_change, irr_pct = yield_irr_rlt_change)]
  g05 <- gy[ssp == "SSP585" & horizon == 2005 & cycle == "both" & cultivar == "current", .(admin1_name, yw_base = yield_rfd_avg)]
  j <- Reduce(function(a, b) merge(a, b, by = "admin1_name", all.x = TRUE),
              list(e50[, .(admin1_name, vop_musd = vop_total / 1e6, pct_any, pct_dry, pct_heat, pct_wet)], eh, g05, g50))
  j[, d_exposure_pp := pct_any - pct_any_hist]
  j <- j[order(pct_any), .(admin1_name, vop_musd = round(vop_musd, 1), exp_hist = round(pct_any_hist, 1), exp_2050 = round(pct_any, 1),
                           d_exp_pp = round(d_exposure_pp, 1), dry = round(pct_dry, 1), heat = round(pct_heat, 1), wet = round(pct_wet, 1),
                           yw_base = round(yw_base, 2), yw_2050 = round(yw_2050, 2), yw_abs = round(yw_abs, 2), yw_pct = round(yw_pct, 1), irr_pct = round(irr_pct, 1))]
  fwrite(j, file.path(out_dir, "19_kenya47_exposure_vs_gyga.csv"))
  add_md("Kenya counties — exposure vs GYGA (sorted by exposure)", j, digits = 2,
         note = paste0("V1 counties without a GYGA row: ", paste(j[is.na(yw_2050) & is.na(yw_base), admin1_name], collapse = ", "),
                       ". Missing yields (NA): ", paste(j[!is.na(yw_base) & is.na(yw_pct) | (is.na(yw_base) & admin1_name %in% gy$admin1_name), admin1_name], collapse = ", "), "."))

  jj <- j[!is.na(yw_pct)]
  sp <- function(a, b, m = "spearman") round(cor(a, b, method = m, use = "complete.obs"), 2)
  cors <- data.table(pair = c("exposure any% vs GYGA rainfed % change", "exposure any% vs GYGA rainfed abs change (t/ha)",
                              "exposure dry% vs GYGA rainfed % change", "d exposure (pp) vs GYGA rainfed % change",
                              "exposure any% vs GYGA BASELINE Yw", "exposure any% vs GYGA IRRIGATED % change"),
                     spearman = c(sp(jj$exp_2050, jj$yw_pct), sp(jj$exp_2050, jj$yw_abs), sp(jj$dry, jj$yw_pct),
                                  sp(jj$d_exp_pp, jj$yw_pct), sp(jj$exp_2050, jj$yw_base), sp(jj$exp_2050, jj$irr_pct)),
                     pearson  = c(sp(jj$exp_2050, jj$yw_pct, "pearson"), sp(jj$exp_2050, jj$yw_abs, "pearson"), sp(jj$dry, jj$yw_pct, "pearson"),
                                  sp(jj$d_exp_pp, jj$yw_pct, "pearson"), sp(jj$exp_2050, jj$yw_base, "pearson"), sp(jj$exp_2050, jj$irr_pct, "pearson")),
                     n = nrow(jj))
  fwrite(cors, file.path(out_dir, "19_correlations.csv"))
  add_md("Correlations across counties", cors)
  print(cors)

  bands <- jj[, .(n = .N, mean_yw_base = round(mean(yw_base), 2), mean_yw_abs = round(mean(yw_abs), 2), mean_yw_pct = round(mean(yw_pct), 1),
                  mean_irr_pct = round(mean(irr_pct), 1)),
              by = .(exposure_band = cut(exp_2050, c(-1, 10, 50, 90, 101), labels = c("<10%", "10-50%", "50-90%", ">90%")))][order(exposure_band)]
  fwrite(bands, file.path(out_dir, "19_exposure_band_means.csv"))
  add_md("GYGA change by exposure band", bands)
  print(bands)

  quad <- jj[, .N, by = .(low_exposure = exp_2050 < 10, big_loss = yw_pct < -5)][order(-low_exposure, -big_loss)]
  add_md("Quadrants: exposure < 10 % vs GYGA loss worse than −5 %", quad)
  kw <- v1[hazard == "any" & ((scenario == "historic") | (scenario == "ssp585" & timeframe == "2041_2060")),
           .(vop_weighted_exposure_pct = round(100 * sum(value) / sum(vop_total), 1), counties = .N), by = .(scenario, timeframe)]
  add_md("Kenya VoP-weighted maize exposure (any severe hazard)", kw)
  log_msg("Kenya VoP-weighted maize exposure: ", paste(kw$scenario, kw$vop_weighted_exposure_pct, collapse = " | "))
  dup <- jj[, .(counties = paste(admin1_name, collapse = ", "), n = .N), by = .(yw_base, yw_pct)][n > 1]
  add_md("GYGA bit-identical county clusters", dup)
}

# ------------------------------------ 4. 2025-07 pipeline bake (S3) -------
if (skip_new) {
  log_msg("== 4. new-pipeline comparison skipped (--skip-new-pipeline) ==")
} else {
  log_msg("== 4. 2025-07 bake: domain=hazard_exposure vop_usd15 annual severe ==")
  s3 <- "https://digital-atlas.s3.amazonaws.com/"
  hz_base <- paste0(s3, "domain=hazard_exposure/source=atlas_cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_usd15/period=annual/")
  f_hist <- fetch(paste0(hz_base, "model=historic/severity=severe/interaction.parquet"), file.path(cache, "hazexp_usd15_annual_historic_severe.parquet"))
  f_ens  <- fetch(paste0(hz_base, "model=ENSEMBLE/severity=severe/interaction.parquet"), file.path(cache, "hazexp_usd15_annual_ENSEMBLE_severe.parquet"))
  ex_base <- paste0(s3, "domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/")
  f_usd15 <- fetch(paste0(ex_base, "variable=vop_nominal-usd-2015.parquet"), file.path(cache, "vop_nominal-usd-2015.parquet"))
  f_intl  <- fetch(paste0(ex_base, "variable=vop_intld15-2021.parquet"),     file.path(cache, "vop_intld15-2021.parquet"))

  rd <- function(f) open_dataset(f) %>% filter(admin0_name == "Kenya", crop == "maize", hazard_vars == "NDWS+NTx35+NDWL0", admin1_name %in% focus, is.na(admin2_name)) %>% collect() %>% as.data.table()
  nx <- rbind(rd(f_hist), rd(f_ens), fill = TRUE)
  log_msg("new-pipeline rows (4 counties): ", nrow(nx), "; hazards: ", paste(unique(nx$hazard), collapse = ","), " (no 'none'/total row -> external denominator, see #9)")
  den <- as.data.table(read_parquet(f_usd15))[admin0_name == "Kenya" & crop == "maize" & exposure == "vop" & admin1_name %in% focus & is.na(admin2_name) & tech %in% c("all", "rf-all", "irr")]
  den_w <- dcast(den, admin1_name ~ tech, value.var = "value")
  nx <- merge(nx, den_w[, .(admin1_name, vop_all = all, vop_rfall = `rf-all`)], by = "admin1_name")
  nx[, `:=`(pct_all = 100 * value / vop_all, pct_rfall = 100 * value / vop_rfall)]
  nx[, `:=`(den_all_M = round(vop_all / 1e6, 1), den_rfall_M = round(vop_rfall / 1e6, 1))]
  nx_w <- dcast(nx[(scenario == "historic") | (scenario == "ssp585" & timeframe == "2041-2060")],
                admin1_name + scenario + timeframe + den_all_M + den_rfall_M ~ hazard, value.var = "pct_all")
  for (jn in setdiff(names(nx_w), c("admin1_name", "scenario", "timeframe", "den_all_M", "den_rfall_M"))) set(nx_w, j = jn, value = round(nx_w[[jn]], 1))
  fwrite(nx_w, file.path(out_dir, "19_new_pipeline_4counties.csv"))
  add_md("2025-07 bake — % of maize VoP exposed (ENSEMBLE mean, severe, annual, usd15; denominator = harmonized usd15 tech=all)", nx_w, digits = 1,
         note = "den_all_M < den_rfall_M for Nandi/Meru is the harmonized-file anomaly audited below; with rf-all as denominator the % are ~3x smaller for those two.")
  print(nx_w)

  audit <- function(f) {
    d <- as.data.table(read_parquet(f))[exposure == "vop" & tech %in% c("all", "rf-all", "irr")]
    w <- dcast(d, iso3 + admin0_name + admin1_name + admin2_name + crop ~ tech, value.var = "value")
    w[, lvl := fifelse(is.na(admin1_name), "adm0", fifelse(is.na(admin2_name), "adm1", "adm2"))]
    w[, bad := !is.na(all) & !is.na(`rf-all`) & all < `rf-all` * 0.999]
    w[, consistent := abs(all - (`rf-all` + fifelse(is.na(irr), 0, irr))) <= 0.01 * pmax(all, 1)]
    rbind(w[, .(file = basename(f), level = lvl, rows = .N, all_lt_rfall = sum(bad), pct_all_lt_rfall = round(100 * mean(bad), 1),
                pct_all_eq_rfall_plus_irr = round(100 * mean(consistent, na.rm = TRUE), 1)), by = lvl][, !"lvl"],
          w[, .(file = basename(f), level = "ALL", rows = .N, all_lt_rfall = sum(bad), pct_all_lt_rfall = round(100 * mean(bad), 1),
                pct_all_eq_rfall_plus_irr = round(100 * mean(consistent, na.rm = TRUE), 1))])
  }
  aud <- rbind(audit(f_usd15), audit(f_intl))
  fwrite(aud, file.path(out_dir, "19_harmonized_vop_all_lt_rfall.csv"))
  add_md("Harmonized VoP audit — tech=all vs rf-all (+irr)", aud)
  print(aud)
}

# ---------------------------------------------------------------- done -----
summary_md <- c(summary_md, "## Session", "", paste0("R ", R.version$major, ".", R.version$minor, "; arrow ", packageVersion("arrow"),
                                                     "; data.table ", packageVersion("data.table"), "; duckdb CLI: ",
                                                     if (nzchar(Sys.which("duckdb"))) trimws(system2("duckdb", "--version", stdout = TRUE)) else "none"), "")
writeLines(summary_md, file.path(out_dir, "19_summary.md"))
log_msg("wrote ", length(list.files(out_dir)), " files to ", out_dir)
log_msg("DONE in ", round(as.numeric(difftime(Sys.time(), t_start, units = "mins")), 1), " min")
