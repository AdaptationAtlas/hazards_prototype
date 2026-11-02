# S6 sensitivity comparison: each yearly-score variant (seasonal/<timeframe>_<variant>/) against V1 (seasonal/<timeframe>/).
# Read-only. Prints one block per variant and writes <fpar>/seasonal/variant_compare_<timeframe>.csv.
#
# Usage: Rscript R/obs/s6_variant_compare.R --obs-root <abs> [--timeframe 2006-2025] [--variants sum,annual12,min-detrend,sum-detrend]
#
# Per variant: Jaccard of the moderate-flagged cell-years against V1, the share of V1's flagged cell-years it keeps,
# the 3 worst start years (largest flagged share), Spearman correlation of the yearly domain-mean z with V1's,
# the linear trend of the yearly domain-mean z (z per decade), and the splice share (flagged cell-years with start
# year >= 2018 over all flagged; the no-shift expectation is the share of start years >= 2018).
suppressPackageStartupMessages(library(terra))
lg <- function(...) cat(sprintf("[%s] [S6-cmp] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), paste0(...)))
stopf <- function(...) stop("[S6-cmp] STOP: ", sprintf(...), call. = FALSE)

v1_fail <- function(d) {
  zy <- rast(file.path(d, "z_year.tif")); q <- rast(file.path(d, "thresholds_z.tif"))[["q20"]]
  f <- ifel(zy <= q, 1, 0); names(f) <- names(zy); list(z = zy, fail = f)
}
summarise <- function(z, fail) {
  yrs <- as.integer(names(fail))
  list(years = yrs, mz = global(z, "mean", na.rm = TRUE)[, 1], share = global(fail, "mean", na.rm = TRUE)[, 1])
}
compare <- function(obs_root, timeframe = "2006-2025", variants = c("sum", "annual12", "min-detrend", "sum-detrend")) {
  sdir <- file.path(obs_root, "Data", "obs_fpar", "seasonal"); bdir <- file.path(sdir, timeframe)
  if (!file.exists(file.path(bdir, "z_year.tif"))) stopf("V1 outputs not found under %s (run S6 without --score first)", bdir)
  b <- v1_fail(bdir); bs <- summarise(b$z, b$fail); bv <- values(b$fail)
  out <- list()
  for (v in variants) {
    d <- file.path(sdir, paste0(timeframe, "_", v))
    if (!file.exists(file.path(d, "fail_moderate.tif"))) { lg(sprintf("%s: not found under %s, skipped", v, d)); next }
    z <- rast(file.path(d, "z_year.tif")); f <- rast(file.path(d, "fail_moderate.tif"))
    if (!identical(names(f), names(b$fail))) stopf("%s: start years %s differ from V1 %s", v, paste(range(names(f)), collapse = ".."), paste(range(names(b$fail)), collapse = ".."))
    fv <- values(f); ok <- !is.na(fv) & !is.na(bv)
    inter <- sum(fv[ok] == 1 & bv[ok] == 1); uni <- sum(fv[ok] == 1 | bv[ok] == 1)
    s <- summarise(z, f)
    late <- s$years >= 2018
    row <- data.frame(variant = v, jaccard_vs_v1 = inter / uni, kept_of_v1 = inter / sum(bv[ok] == 1),
                      spearman_meanz_vs_v1 = cor(s$mz, bs$mz, method = "spearman"),
                      trend_meanz_per_decade = 10 * unname(coef(lm(s$mz ~ s$years))[2]),
                      splice_share = sum(colSums(fv == 1, na.rm = TRUE)[late]) / sum(fv == 1, na.rm = TRUE),
                      splice_expect = mean(late),
                      worst3 = paste(s$years[order(-s$share)][1:3], collapse = ","))
    lg(sprintf("%-12s Jaccard %.3f | keeps %.3f of V1 | rho(mean z) %.3f | trend %+.3f z/decade | splice %.3f (expect %.3f) | worst %s",
               v, row$jaccard_vs_v1, row$kept_of_v1, row$spearman_meanz_vs_v1, row$trend_meanz_per_decade, row$splice_share, row$splice_expect, row$worst3))
    out[[v]] <- row
  }
  late <- bs$years >= 2018; bvl <- values(b$fail)
  base <- data.frame(variant = "min (V1)", jaccard_vs_v1 = 1, kept_of_v1 = 1, spearman_meanz_vs_v1 = 1,
                     trend_meanz_per_decade = 10 * unname(coef(lm(bs$mz ~ bs$years))[2]),
                     splice_share = sum(colSums(bvl == 1, na.rm = TRUE)[late]) / sum(bvl == 1, na.rm = TRUE), splice_expect = mean(late),
                     worst3 = paste(bs$years[order(-bs$share)][1:3], collapse = ","))
  lg(sprintf("%-12s (reference) trend %+.3f z/decade | splice %.3f (expect %.3f) | worst %s", "min (V1)", base$trend_meanz_per_decade, base$splice_share, base$splice_expect, base$worst3))
  res <- do.call(rbind, c(list(base), out)); f <- file.path(sdir, sprintf("variant_compare_%s.csv", timeframe))
  write.csv(res, f, row.names = FALSE); lg("wrote ", f); invisible(res)
}

if (sys.nframe() == 0) {
  a <- commandArgs(TRUE); getopt <- function(k, d = NULL) { i <- match(k, a); if (is.na(i)) d else a[i + 1] }
  obs_root <- getopt("--obs-root"); if (is.null(obs_root) || !startsWith(obs_root, "/")) stopf("--obs-root must be absolute")
  compare(obs_root, getopt("--timeframe", "2006-2025"), strsplit(getopt("--variants", "sum,annual12,min-detrend,sum-detrend"), ",")[[1]])
}
