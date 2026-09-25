# Seasonal rainfall (CHIRPS v3 PTOT) baseline + trends for eastern Kenya counties.
#
# Colleague request (2026-09-25): 1991-2020 baseline and trends through 2025 for
# MAM and OND seasonal totals, counties Machakos, Makueni, Kitui, Embu,
# Tharaka-Nithi, Meru, plus Kenya national for context.
#
# Input: the published Atlas observational admin-periods parquet on S3
#   s3://digital-atlas/domain=climate/type=observational/source=chirps-chirts-era5/
#     region=africa/processing=admin-periods/variable=adm{0,1}_obs.parquet
# produced by R/observational/3_extract_obs_admin.R + 4_aggregate_obs_admin_periods.R.
# PTOT per period = sum of CHIRPS v3 monthly totals, zonal mean over the polygon
# at the native 0.05 deg grid (value_mean), zonal sd across pixels (value_sd).
#
# Usage:
#   Rscript R/misc/ke_eastern_rainfall_trends.R <out_dir>
# If <out_dir>/kenya_ptot_seasonal_all_counties.csv is absent the script pulls it
# with the duckdb CLI (needs `duckdb` on PATH + httpfs, anonymous public bucket).
#
# Method:
#   baseline  = 1991-2020 mean, sd, CV, min, max per zone x season (WMO normal
#               period; matches the review's reference period).
#   trend     = Theil-Sen slope (trend::sens.slope, 95% CI) + Mann-Kendall test
#               (trend::mk.test) over two windows: 1981-2025 (full CHIRPS record)
#               and 1991-2025 (baseline start to present). OLS slope reported for
#               comparison. Slopes expressed as mm/decade and % of baseline mean
#               per decade. No pre-whitening (seasonal totals show weak lag-1
#               autocorrelation; check ar1 column).
#   anomalies = per year: mm and % departure from 1991-2020 mean, and z-score.

suppressPackageStartupMessages({
  library(data.table); library(trend); library(ggplot2)
})

args    <- commandArgs(trailingOnly = TRUE)
out_dir <- if (length(args) >= 1) args[1] else "ke_rain_trends"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
log_step <- function(...) { cat(format(Sys.time(), "[%H:%M:%S] "), sprintf(...), "\n", sep = ""); flush.console() }

# 1) Data ----------------------------------------------------------------------
in_csv <- file.path(out_dir, "kenya_ptot_seasonal_all_counties.csv")
if (!file.exists(in_csv)) {
  log_step("Pulling from S3 via duckdb CLI -> %s", in_csv)
  root <- "s3://digital-atlas/domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=admin-periods"
  sql <- sprintf("
INSTALL httpfs; LOAD httpfs; SET s3_region='us-east-1';
COPY (
  SELECT 'admin1' AS level, admin1_name AS zone, gaul1_code AS gaul_code, year, period, value_mean AS ptot_mm, value_sd AS ptot_sd_mm
  FROM read_parquet('%s/variable=adm1_obs.parquet', hive_partitioning=false)
  WHERE admin0_name='Kenya' AND variable='PTOT' AND period IN ('MAM','OND','annual')
  UNION ALL
  SELECT 'admin0', admin0_name, gaul0_code, year, period, value_mean, value_sd
  FROM read_parquet('%s/variable=adm0_obs.parquet', hive_partitioning=false)
  WHERE admin0_name='Kenya' AND variable='PTOT' AND period IN ('MAM','OND','annual')
  ORDER BY level, zone, period, year
) TO '%s' (HEADER);", root, root, in_csv)
  st <- system2("duckdb", c("-c", shQuote(sql)))
  if (st != 0 || !file.exists(in_csv)) stop("duckdb pull failed")
}
d <- fread(in_csv)
d[, year := as.integer(year)]

# Kenya adm0 carries two rows per year: gaul0_code 137 = Kenya mainland,
# gaul0_code 135 = the disputed Ilemi Triangle sliver (small, dry). Keep 137.
# (Atlas-wide disputed-territory convention, see CR-115.)
n_dup <- d[level == "admin0", .N, by = .(period, year)][N > 1, .N]
if (n_dup > 0) { log_step("adm0: %d period-years duplicated; keeping gaul0_code 137", n_dup); d <- d[!(level == "admin0" & gaul_code != 137)] }
stopifnot(d[, .N, by = .(zone, period, year)][, all(N == 1)])

focus <- c("Kenya", "Machakos", "Makueni", "Kitui", "Embu", "Tharaka-Nithi", "Meru")
stopifnot(all(focus %in% d$zone))
d[, zone := factor(zone, levels = c(focus, sort(setdiff(unique(zone), focus))))]
d[, period := factor(period, levels = c("MAM", "OND", "annual"))]
setorder(d, zone, period, year)
log_step("Rows: %d | zones: %d | years %d-%d", nrow(d), uniqueN(d$zone), min(d$year), max(d$year))

# 2) Baseline 1991-2020 --------------------------------------------------------
base <- d[year %between% c(1991, 2020), .(
  n_years = .N, mean_mm = mean(ptot_mm), sd_mm = sd(ptot_mm), cv = sd(ptot_mm) / mean(ptot_mm),
  min_mm = min(ptot_mm), min_year = year[which.min(ptot_mm)],
  max_mm = max(ptot_mm), max_year = year[which.max(ptot_mm)],
  p20_mm = quantile(ptot_mm, .2), p80_mm = quantile(ptot_mm, .8)
), by = .(zone, period)]
stopifnot(all(base$n_years == 30))

d <- merge(d, base[, .(zone, period, base_mean_mm = mean_mm, base_sd_mm = sd_mm)], by = c("zone", "period"))
d[, anom_mm  := ptot_mm - base_mean_mm]
d[, anom_pct := 100 * anom_mm / base_mean_mm]
d[, z_score  := anom_mm / base_sd_mm]
setorder(d, zone, period, year)

# 3) Trends --------------------------------------------------------------------
windows <- list(`1981-2025` = c(1981, 2025), `1991-2025` = c(1991, 2025))
trend_one <- function(x, yr) {
  ok <- !is.na(x); x <- x[ok]; yr <- yr[ok]
  ss <- trend::sens.slope(x, conf.level = 0.95)
  mk <- trend::mk.test(x)
  ols <- summary(lm(x ~ yr))$coefficients
  r <- residuals(lm(x ~ yr)); ar1 <- cor(r[-1], r[-length(r)])
  list(n = length(x), year_start = min(yr), year_end = max(yr),
       sen_mm_yr = unname(ss$estimates), sen_lo95_mm_yr = ss$conf.int[1], sen_hi95_mm_yr = ss$conf.int[2],
       mk_tau = unname(mk$estimates["tau"]), mk_p = mk$p.value,
       ols_mm_yr = ols["yr", "Estimate"], ols_p = ols["yr", "Pr(>|t|)"], resid_ar1 = ar1)
}
tr <- rbindlist(lapply(names(windows), function(w) {
  yr <- windows[[w]]
  d[year %between% yr, c(list(window = w), trend_one(ptot_mm, year)), by = .(zone, period)]
}))
tr <- merge(tr, base[, .(zone, period, base_mean_mm = mean_mm)], by = c("zone", "period"))
tr[, `:=`(sen_mm_decade = 10 * sen_mm_yr,
          sen_lo95_mm_decade = 10 * sen_lo95_mm_yr, sen_hi95_mm_decade = 10 * sen_hi95_mm_yr,
          sen_pct_decade = 100 * 10 * sen_mm_yr / base_mean_mm,
          sen_total_change_mm = sen_mm_yr * (year_end - year_start),
          direction = fifelse(sen_mm_yr > 0, "wetter", "drier"),
          signif = fcase(mk_p < 0.01, "p<0.01", mk_p < 0.05, "p<0.05", mk_p < 0.10, "p<0.10", default = "ns"))]
setcolorder(tr, c("zone", "period", "window", "n", "year_start", "year_end", "base_mean_mm",
                  "sen_mm_yr", "sen_mm_decade", "sen_lo95_mm_decade", "sen_hi95_mm_decade",
                  "sen_pct_decade", "sen_total_change_mm", "direction", "mk_tau", "mk_p", "signif",
                  "ols_mm_yr", "ols_p", "resid_ar1"))
setorder(tr, zone, period, window)

# 4) Write ---------------------------------------------------------------------
num_cols <- function(x) x[, names(.SD) := lapply(.SD, signif, 5), .SDcols = is.numeric][]
fwrite(num_cols(copy(d[zone %in% focus])),  file.path(out_dir, "ke_eastern_ptot_series.csv"))
fwrite(num_cols(copy(base[zone %in% focus])), file.path(out_dir, "ke_eastern_ptot_baseline_1991_2020.csv"))
fwrite(num_cols(copy(tr[zone %in% focus])),   file.path(out_dir, "ke_eastern_ptot_trends.csv"))
fwrite(num_cols(copy(tr)),                    file.path(out_dir, "ke_all_counties_ptot_trends.csv"))
fwrite(num_cols(copy(base)),                  file.path(out_dir, "ke_all_counties_ptot_baseline_1991_2020.csv"))
log_step("CSV written to %s", out_dir)

# 5) Plots ---------------------------------------------------------------------
pf <- d[zone %in% focus & period != "annual"]
bf <- base[zone %in% focus & period != "annual"]
sen_lines <- tr[zone %in% focus & period != "annual" & window == "1981-2025"]
# Theil-Sen intercept = median(y - slope * x) (Conover), so the line is unbiased in level.
sen_int <- merge(pf, sen_lines[, .(zone, period, sen_mm_yr)], by = c("zone", "period"))[
  , .(intercept = median(ptot_mm - sen_mm_yr * year)), by = .(zone, period)]
sen_lines <- merge(sen_lines, sen_int, by = c("zone", "period"))
sen_lines[, `:=`(y0 = intercept + sen_mm_yr * 1981, y1 = intercept + sen_mm_yr * 2025)]
sen_lines[, lab := sprintf("Sen %+.0f mm/dec (%+.1f%%/dec), MK p=%.2f", sen_mm_decade, sen_pct_decade, mk_p)]

p1 <- ggplot(pf, aes(year, ptot_mm)) +
  geom_rect(data = bf, aes(xmin = 1991, xmax = 2020, ymin = mean_mm - sd_mm, ymax = mean_mm + sd_mm),
            inherit.aes = FALSE, fill = "grey85", alpha = .6) +
  geom_hline(data = bf, aes(yintercept = mean_mm), colour = "grey40", linetype = 2) +
  geom_line(colour = "steelblue4") + geom_point(size = 1, colour = "steelblue4") +
  geom_segment(data = sen_lines, aes(x = 1981, xend = 2025, y = y0, yend = y1), colour = "firebrick", linewidth = .8) +
  geom_text(data = sen_lines, aes(x = 1981, y = Inf, label = lab), hjust = 0, vjust = 1.4, size = 2.6, colour = "firebrick") +
  facet_grid(zone ~ period, scales = "free_y") +
  labs(title = "Seasonal rainfall totals, CHIRPS v3 (zonal mean), 1981-2025",
       subtitle = "Dashed = 1991-2020 mean; grey band = ±1 sd (1991-2020); red = Theil-Sen trend 1981-2025",
       x = NULL, y = "Season total (mm)") +
  theme_bw(base_size = 10) + theme(strip.text.y = element_text(angle = 0))
ggsave(file.path(out_dir, "ke_eastern_ptot_series_trend.png"), p1, width = 10, height = 12, dpi = 150)

p2 <- ggplot(pf, aes(year, anom_pct, fill = anom_pct > 0)) +
  geom_col(show.legend = FALSE) +
  scale_fill_manual(values = c(`TRUE` = "steelblue4", `FALSE` = "firebrick")) +
  geom_vline(xintercept = c(1990.5, 2020.5), linetype = 3, colour = "grey40") +
  facet_grid(zone ~ period) +
  labs(title = "Seasonal rainfall anomaly vs 1991-2020 mean (%)", subtitle = "Dotted lines bound the 1991-2020 baseline",
       x = NULL, y = "% departure from 1991-2020 mean") +
  theme_bw(base_size = 10) + theme(strip.text.y = element_text(angle = 0))
ggsave(file.path(out_dir, "ke_eastern_ptot_anomaly_pct.png"), p2, width = 10, height = 12, dpi = 150)

tf <- tr[zone %in% focus & period != "annual"]
p3 <- ggplot(tf, aes(x = sen_pct_decade, y = zone, colour = window)) +
  geom_vline(xintercept = 0, colour = "grey50") +
  geom_errorbarh(aes(xmin = 100 * sen_lo95_mm_decade / base_mean_mm, xmax = 100 * sen_hi95_mm_decade / base_mean_mm), height = .3, position = position_dodge(width = .6)) +
  geom_point(aes(shape = signif != "ns"), size = 2.5, position = position_dodge(width = .6)) +
  scale_shape_manual(values = c(`TRUE` = 16, `FALSE` = 1), labels = c(`TRUE` = "MK p<0.10", `FALSE` = "not significant"), name = NULL) +
  scale_y_discrete(limits = rev(focus)) +
  facet_wrap(~ period) +
  labs(title = "Theil-Sen rainfall trend, % of 1991-2020 mean per decade (95% CI)", x = "% per decade", y = NULL, colour = "Window") +
  theme_bw(base_size = 10)
ggsave(file.path(out_dir, "ke_eastern_ptot_trend_summary.png"), p3, width = 9, height = 5, dpi = 150)
log_step("Plots written. Done.")

print(tr[zone %in% focus & period != "annual", .(zone, period, window, base_mean_mm = round(base_mean_mm), sen_mm_decade = round(sen_mm_decade, 1),
                                                   sen_pct_decade = round(sen_pct_decade, 1), mk_p = round(mk_p, 3), signif)])
