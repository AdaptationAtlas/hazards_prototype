# Variability and extremes in seasonal rainfall (CHIRPS v3 PTOT), eastern Kenya.
#
# Companion to R/misc/ke_eastern_rainfall_trends.R. Same zones, seasons and
# 1991-2020 baseline. Asks whether the *spread* and the *tails* of the seasonal
# total distribution have shifted, which a trend in the mean cannot show.
#
# Usage:
#   Rscript R/misc/ke_eastern_rainfall_variability.R <out_dir>
# Expects in <out_dir>:
#   kenya_ptot_seasonal_all_counties.csv   (written by ke_eastern_rainfall_trends.R)
#   kenya_spei03_season_end.csv            (SPEI-03 at May and December, admin-monthly parquet;
#                                           SQL to regenerate is in the README of the docs folder)
#
# Method (all on seasonal totals, MAM and OND, 1981-2025, per zone):
#   Variability
#     - 15-year centred rolling sd and CV of the seasonal total.
#     - Halves comparison 1981-2002 vs 2003-2025: sd, CV, and a Brown-Forsythe
#       test (one-way ANOVA on |detrended residual - group median|) for a change
#       in spread. Residuals are from a Theil-Sen fit over 1981-2025 so a mean
#       trend cannot masquerade as a variance change.
#     - Theil-Sen slope + Mann-Kendall p of |detrended residual| vs year, i.e. a
#       monotonic trend in the typical size of departures from trend.
#   Extremes (thresholds fixed from the 1991-2020 baseline)
#     - dry season  : total < 20th percentile of 1991-2020   (expected 1 in 5)
#     - wet season  : total > 80th percentile of 1991-2020   (expected 1 in 5)
#     - failed      : total < 75 % of the 1991-2020 mean
#     - drought     : SPEI-03 at season end (May / Dec) <= -1 (moderate or worse)
#     Counts per block (1981-90, 1991-2000, 2001-10, 2011-20, 2021-25) with the
#     expected count under the baseline; logistic regression of each indicator
#     on year -> odds ratio per decade and p.
#   Whiplash
#     - Seasons ordered chronologically (MAM y, OND y, MAM y+1, ...). Swing =
#       z(t) - z(t-1). Theil-Sen / MK trend in |swing|; count of large flips
#       (|swing| > 2 sd with sign change) per block; runs of >= 3 consecutive dry
#       seasons (below p20).

suppressPackageStartupMessages({ library(data.table); library(trend); library(ggplot2) })
args    <- commandArgs(trailingOnly = TRUE)
out_dir <- if (length(args) >= 1) args[1] else "ke_rain_trends"
log_step <- function(...) { cat(format(Sys.time(), "[%H:%M:%S] "), sprintf(...), "\n", sep = ""); flush.console() }

focus <- c("Kenya", "Machakos", "Makueni", "Kitui", "Embu", "Tharaka-Nithi", "Meru")
blocks <- data.table(block = c("1981-1990", "1991-2000", "2001-2010", "2011-2020", "2021-2025"),
                     y0 = c(1981, 1991, 2001, 2011, 2021), y1 = c(1990, 2000, 2010, 2020, 2025))
block_of <- function(y) blocks$block[findInterval(y, blocks$y0)]

# 1) Data ----------------------------------------------------------------------
d <- fread(file.path(out_dir, "kenya_ptot_seasonal_all_counties.csv"))
d <- d[!(level == "admin0" & gaul_code != 137) & period %in% c("MAM", "OND")]
d[, year := as.integer(year)]
stopifnot(d[, .N, by = .(zone, period, year)][, all(N == 1)])
sp <- fread(file.path(out_dir, "kenya_spei03_season_end.csv"))[, .(zone, period, year = as.integer(year), spei03)]
d <- merge(d, sp, by = c("zone", "period", "year"), all.x = TRUE)
stopifnot(d[year <= 2025, !anyNA(spei03)])

base <- d[year %between% c(1991, 2020), .(mean_mm = mean(ptot_mm), sd_mm = sd(ptot_mm),
                                          p20 = quantile(ptot_mm, .2), p80 = quantile(ptot_mm, .8)), by = .(zone, period)]
d <- merge(d, base, by = c("zone", "period"))
d[, z := (ptot_mm - mean_mm) / sd_mm]
d[, `:=`(dry = ptot_mm < p20, wet = ptot_mm > p80, failed = ptot_mm < 0.75 * mean_mm, drought = spei03 <= -1)]
d[, block := block_of(year)]
setorder(d, zone, period, year)
d[, zone := factor(zone, levels = c(focus, sort(setdiff(unique(zone), focus))))]
log_step("Zones %d, seasons %d, years %d-%d", uniqueN(d$zone), uniqueN(d$period), min(d$year), max(d$year))

# Theil-Sen detrended residuals over 1981-2025
d[, resid := {
  s <- trend::sens.slope(ptot_mm)$estimates; b <- median(ptot_mm - s * year)
  ptot_mm - (b + s * year)
}, by = .(zone, period)]

# 2) Variability ---------------------------------------------------------------
roll <- d[, {
  w <- 15; h <- (w - 1) / 2; yrs <- year
  rbindlist(lapply(seq_along(yrs), function(i) {
    idx <- which(abs(yrs - yrs[i]) <= h)
    if (length(idx) < w) return(NULL)
    list(year = yrs[i], roll_sd_mm = sd(ptot_mm[idx]), roll_cv = sd(ptot_mm[idx]) / mean(ptot_mm[idx]),
         roll_mean_mm = mean(ptot_mm[idx]))
  }))
}, by = .(zone, period)]

half_of <- function(y) fifelse(y <= 2002, "1981-2002", "2003-2025")
vtest <- d[, {
  h <- half_of(year)
  bf <- anova(lm(abs(resid - ave(resid, h, FUN = median)) ~ h))$`Pr(>F)`[1]
  ss <- trend::sens.slope(abs(resid)); mk <- trend::mk.test(abs(resid))
  list(sd_1981_2002 = sd(ptot_mm[h == "1981-2002"]), sd_2003_2025 = sd(ptot_mm[h == "2003-2025"]),
       cv_1981_2002 = sd(ptot_mm[h == "1981-2002"]) / mean(ptot_mm[h == "1981-2002"]),
       cv_2003_2025 = sd(ptot_mm[h == "2003-2025"]) / mean(ptot_mm[h == "2003-2025"]),
       sd_ratio = sd(ptot_mm[h == "2003-2025"]) / sd(ptot_mm[h == "1981-2002"]),
       brown_forsythe_p = bf,
       absresid_sen_mm_decade = 10 * unname(ss$estimates),
       absresid_sen_pct_basesd_decade = 100 * 10 * unname(ss$estimates) / sd_mm[1],
       absresid_mk_p = mk$p.value)
}, by = .(zone, period)]
setorder(vtest, zone, period)

# 3) Extremes ------------------------------------------------------------------
ext_block <- d[, .(n = .N, dry = sum(dry), wet = sum(wet), failed = sum(failed), drought = sum(drought),
                   expected_dry_or_wet = round(.2 * .N, 1)), by = .(zone, period, block)]
setorder(ext_block, zone, period, block)
ext_wide <- dcast(ext_block, zone + period ~ block, value.var = c("dry", "wet", "failed", "drought"))

logit_trend <- function(y, yr) {
  if (sum(y) < 2 || sum(!y) < 2) return(list(or_per_decade = NA_real_, p = NA_real_))
  f <- suppressWarnings(glm(y ~ I((yr - 2003) / 10), family = binomial))
  co <- summary(f)$coefficients
  list(or_per_decade = exp(co[2, 1]), p = co[2, 4])
}
ext_trend <- rbindlist(lapply(c("dry", "wet", "failed", "drought"), function(v)
  d[, c(list(indicator = v, n_events = sum(get(v)), rate_1981_2002 = mean(get(v)[year <= 2002]),
             rate_2003_2025 = mean(get(v)[year > 2002])), logit_trend(get(v), year)), by = .(zone, period)]))
setorder(ext_trend, zone, period, indicator)

# 4) Whiplash ------------------------------------------------------------------
seq_d <- copy(d)[, t := year + fifelse(period == "MAM", 0, 0.5)][order(zone, t)]
seq_d[, swing := z - shift(z), by = zone]
seq_d[, big_flip := !is.na(swing) & abs(swing) > 2 & sign(z) != sign(shift(z)), by = zone]
whip <- seq_d[!is.na(swing), {
  ss <- trend::sens.slope(abs(swing)); mk <- trend::mk.test(abs(swing))
  list(mean_abs_swing_1981_2002 = mean(abs(swing)[year <= 2002]), mean_abs_swing_2003_2025 = mean(abs(swing)[year > 2002]),
       absswing_sen_per_decade = 20 * unname(ss$estimates),   # 2 seasons per year -> 20 steps per decade
       absswing_mk_p = mk$p.value, big_flips_1981_2002 = sum(big_flip[year <= 2002]), big_flips_2003_2025 = sum(big_flip[year > 2002]))
}, by = zone]
runs <- seq_d[, {
  r <- rle(dry); ends <- cumsum(r$lengths); starts <- ends - r$lengths + 1
  k <- which(r$values & r$lengths >= 3)
  if (!length(k)) NULL else list(run_start = paste(period[starts[k]], year[starts[k]]), run_end = paste(period[ends[k]], year[ends[k]]), n_seasons = r$lengths[k])
}, by = zone]
setorder(runs, zone, run_start)

# 5) Write ---------------------------------------------------------------------
sig5 <- function(x) x[, names(.SD) := lapply(.SD, signif, 5), .SDcols = is.numeric][]
f <- function(x) x[zone %in% focus]
fwrite(sig5(f(copy(roll))),      file.path(out_dir, "ke_eastern_ptot_rolling15_variability.csv"))
fwrite(sig5(f(copy(vtest))),     file.path(out_dir, "ke_eastern_ptot_variability_tests.csv"))
fwrite(sig5(f(copy(ext_block))), file.path(out_dir, "ke_eastern_ptot_extremes_by_block.csv"))
fwrite(sig5(f(copy(ext_trend))), file.path(out_dir, "ke_eastern_ptot_extremes_trend.csv"))
fwrite(sig5(f(copy(whip))),      file.path(out_dir, "ke_eastern_ptot_whiplash.csv"))
fwrite(f(copy(runs)),            file.path(out_dir, "ke_eastern_ptot_dry_runs.csv"))
fwrite(sig5(f(copy(d))[, .(zone, period, year, ptot_mm, z, spei03, dry, wet, failed, drought, block)]),
       file.path(out_dir, "ke_eastern_ptot_season_flags.csv"))
fwrite(sig5(copy(vtest)),     file.path(out_dir, "ke_all_counties_ptot_variability_tests.csv"))
fwrite(sig5(copy(ext_trend)), file.path(out_dir, "ke_all_counties_ptot_extremes_trend.csv"))
log_step("CSV written")

# 6) Plots ---------------------------------------------------------------------
th <- theme_bw(base_size = 10) + theme(strip.text.y = element_text(angle = 0))
p1 <- ggplot(f(roll), aes(year, roll_cv)) +
  geom_hline(data = f(base)[, .(zone, period, cv = sd_mm / mean_mm)], aes(yintercept = cv), linetype = 2, colour = "grey40") +
  geom_line(colour = "steelblue4", linewidth = .8) +
  facet_grid(zone ~ period, scales = "free_y") +
  labs(title = "Interannual variability: 15-year centred rolling CV of seasonal total",
       subtitle = "Dashed = CV over 1991-2020. Window centre year on x-axis (1988-2018).", x = NULL, y = "CV (sd / mean)") + th
ggsave(file.path(out_dir, "ke_eastern_ptot_rolling_cv.png"), p1, width = 9, height = 11, dpi = 150)

eb <- melt(f(ext_block), id.vars = c("zone", "period", "block", "n", "expected_dry_or_wet"),
           measure.vars = c("dry", "wet", "failed", "drought"), variable.name = "indicator", value.name = "count")
eb[, rate := count / n]
p2 <- ggplot(eb, aes(block, rate, fill = indicator)) +
  geom_col(position = position_dodge(width = .8)) +
  geom_hline(yintercept = .2, linetype = 3, colour = "grey30") +
  scale_fill_manual(values = c(dry = "firebrick", wet = "steelblue4", failed = "darkorange3", drought = "grey40"),
                    labels = c(dry = "< p20 (dry)", wet = "> p80 (wet)", failed = "< 75% of mean (failed)", drought = "SPEI-03 <= -1")) +
  facet_grid(zone ~ period) +
  labs(title = "Share of seasons beyond 1991-2020 thresholds, by block",
       subtitle = "Dotted = 20 %, the expected share of dry (or wet) seasons under the 1991-2020 distribution. 2021-2025 block has 5 seasons.",
       x = NULL, y = "Share of seasons", fill = NULL) + th + theme(legend.position = "bottom", axis.text.x = element_text(angle = 30, hjust = 1))
ggsave(file.path(out_dir, "ke_eastern_ptot_extremes_by_block.png"), p2, width = 9, height = 12, dpi = 150)

sq <- f(seq_d)
p3 <- ggplot(sq, aes(t, z)) +
  geom_hline(yintercept = 0, colour = "grey50") +
  geom_hline(yintercept = -1, colour = "firebrick", linetype = 2) +
  geom_hline(yintercept = 1, colour = "steelblue4", linetype = 2) +
  geom_line(colour = "grey60") +
  geom_point(aes(colour = period), size = 1.3) +
  geom_point(data = sq[big_flip == TRUE], shape = 21, size = 3, colour = "black", fill = NA) +
  scale_colour_manual(values = c(MAM = "darkgreen", OND = "purple")) +
  facet_wrap(~ zone, ncol = 1, strip.position = "right") +
  labs(title = "Season-by-season standardised anomaly (z vs 1991-2020), MAM and OND interleaved",
       subtitle = "Open circles = large flips: |change in z| > 2 with sign reversal between consecutive seasons", x = NULL, y = "z-score", colour = NULL) +
  th + theme(legend.position = "bottom")
ggsave(file.path(out_dir, "ke_eastern_ptot_whiplash.png"), p3, width = 10, height = 12, dpi = 150)
log_step("Plots written. Done.")

print(f(vtest)[, .(zone, period, sd_ratio = round(sd_ratio, 2), bf_p = round(brown_forsythe_p, 3), absresid_pct_dec = round(absresid_sen_pct_basesd_decade, 1), mk_p = round(absresid_mk_p, 3))])
print(f(ext_trend)[, .(zone, period, indicator, n_events, r1 = round(rate_1981_2002, 2), r2 = round(rate_2003_2025, 2), or_dec = round(or_per_decade, 2), p = round(p, 3))])
print(f(whip)); print(f(runs))
