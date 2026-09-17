# 2026-09-17_maize_season_year_mapping.R
# -----------------------------------------------------------------------------
# WHICH RAINFALL SEASONS DOES A KNBS MAIZE PRODUCTION *YEAR* CORRESPOND TO?
#
# The question (Pete, 2026-09-17): if KNBS reports 10,000 t of maize in 2024, is that OND 2023 +
# MAM 2024? It matters for anything that joins production statistics to climate seasons, and KNBS
# does not publish the rule. Its own National Agriculture Production Report uses BOTH attributions
# in different places: the cotton section counts a crop "grown in October-November-December 2022 and
# harvested in May-June 2023" as 2023 (harvest-year), while the food-crops narrative credits the
# 2023 result to "heavy and well-spread short rains", i.e. OND 2023 (planting-year). The maize table
# states no season composition at all.
#
# So this measures it instead of assuming.
#
# METHOD
#   outcome    YIELD (t/ha), within-county z-score across years. Yield rather than production
#              because area responds to prices and the fertiliser subsidy, not to weather; z-scored
#              within county because the question is "was this a good year HERE", not "is this a big
#              county". Production is reported alongside for completeness.
#   predictors county season rainfall (CHIRPS, Atlas admin-periods parquet), z-scored the same way:
#                MAM(Y), OND(Y), OND(Y-1), MAM(Y)+OND(Y), MAM(Y)+OND(Y-1)
#   grouping   counties are classified by their OWN 1991-2020 climatology
#              (ond_share = OND / (MAM + OND)) rather than a hand-drawn unimodal/bimodal list.
#
# HEADLINE RESULT (see the issue for the full write-up)
#   In the counties that actually grow Kenya's maize, same-year OND has NO relationship to yield
#   (top-12 counties, 65% of national production: r = -0.02) while previous-year OND has the
#   strongest single-season one (r = 0.22, p = 0.06). That is the harvest-aligned mapping:
#       production year Y  <-  OND(Y-1) + MAM(Y)
#   Short-rains-dominant eastern counties (Tana River, Meru, Kitui, Makueni) invert it: OND(Y)
#   r = 0.40, MAM r = -0.05. A single national rule is therefore a deliberate simplification.
#
# CONFIDENCE: moderate, not high. Six years, weak correlations, and 2023 carries both a fertiliser
# subsidy and a 15% area expansion. Consistent with the mapping; not proof of it.
#
# INPUTS
#   scripts/2026-09-17_knbs_maize_panel.csv        47 counties x 2019-2024, built by
#                                                  scripts/2026-09-17_parse_knbs_maize_panel.py
#   S3 admin-periods parquet (public, no credentials) via the local `duckdb` CLI
#
# RUN
#   Rscript scripts/2026-09-17_maize_season_year_mapping.R
# -----------------------------------------------------------------------------
suppressPackageStartupMessages({library(data.table)})
setDTthreads(1)   # the default thread pool deadlocked on this workload under Rscript

project_dir <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else getwd()
panel_csv <- file.path(project_dir, "scripts", "2026-09-17_knbs_maize_panel.csv")
cache_dir <- Sys.getenv("CACHE_DIR", tempdir())
dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
S3 <- paste0("https://digital-atlas.s3.amazonaws.com/domain=climate/type=observational/",
             "source=chirps-chirts-era5/region=africa/processing=admin-periods/variable=adm1_obs.parquet")

# ---- county seasonal rainfall + climatology, straight off S3 -----------------
duck <- function(sql, out) {
  if (file.exists(out)) return(invisible(out))
  if (Sys.which("duckdb") == "") stop("the duckdb CLI is needed to read the S3 parquet")
  system2("duckdb", c("-c", shQuote(sprintf("LOAD httpfs; COPY (%s) TO '%s' (HEADER, DELIMITER ',');",
                                            sql, out))), stdout = NULL)
  invisible(out)
}
f_clim <- file.path(cache_dir, "ken_seasons.csv")
f_clm  <- file.path(cache_dir, "ken_climatology.csv")
duck(sprintf("SELECT admin1_name, year, period, variable, value_mean FROM read_parquet('%s')
              WHERE iso3='KEN' AND period IN ('MAM','OND') AND variable='PTOT'
                AND year BETWEEN 2017 AND 2024", S3), f_clim)
duck(sprintf("SELECT admin1_name, period, avg(value_mean) AS clim_mm FROM read_parquet('%s')
              WHERE iso3='KEN' AND variable='PTOT' AND period IN ('MAM','OND')
                AND year BETWEEN 1991 AND 2020 GROUP BY 1,2", S3), f_clm)

maize <- fread(panel_csv)
clim  <- fread(f_clim)
clm   <- fread(f_clm)

# climate admin1 names are GAUL; the panel is keyed on IEBC COD-AB p-codes. Same 47 counties, two
# spellings, so normalise both sides. "Ilemi Triangle" is the disputed sliver — no production.
norm <- function(x) toupper(gsub("[^A-Za-z0-9]", "", x))
key  <- unique(maize[, .(adm1_pcode, adm1_name, k = norm(adm1_name))])
key  <- rbind(key, key[adm1_name == "Nairobi"][, k := "NAIROBICITY"])
attach_pcode <- function(dt) {
  dt[, k := norm(admin1_name)]
  out <- key[dt, on = "k"]
  miss <- unique(out[is.na(adm1_pcode), admin1_name])
  if (length(miss)) message("  dropped (no production reported): ", paste(miss, collapse = ", "))
  out[!is.na(adm1_pcode)]
}
clim <- attach_pcode(clim); clm <- attach_pcode(clm)

# ---- regime from each county's own climatology -------------------------------
reg <- dcast(clm, adm1_pcode + adm1_name ~ period, value.var = "clim_mm")
reg[, ond_share := OND / (MAM + OND)]
reg[, regime := fifelse(ond_share < 0.40, "long-rains dominant",
                fifelse(ond_share > 0.55, "short-rains dominant", "mixed"))]

# ---- assemble ----------------------------------------------------------------
pt  <- dcast(clim, adm1_pcode + year ~ period, value.var = "value_mean")
setnames(pt, c("MAM", "OND"), c("MAM_ptot", "OND_ptot"))
lag <- pt[, .(adm1_pcode, year = year + 1L, OND_ptot_lag = OND_ptot)]
cl  <- merge(pt, lag, by = c("adm1_pcode", "year"), all.x = TRUE)

d <- merge(maize, cl, by = c("adm1_pcode", "year"))
d <- merge(d, reg[, .(adm1_pcode, ond_share, regime)], by = "adm1_pcode")
d[, yield := production_t / area_ha]
d <- d[is.finite(yield) & area_ha > 0]

z <- function(x) if (sum(is.finite(x)) < 3 || sd(x, na.rm = TRUE) == 0) NA_real_ * x else
  (x - mean(x, na.rm = TRUE)) / sd(x, na.rm = TRUE)
d[, `:=`(zM = z(MAM_ptot), zOs = z(OND_ptot), zOl = z(OND_ptot_lag),
         zMOs = z(MAM_ptot + OND_ptot), zMOl = z(MAM_ptot + OND_ptot_lag),
         zy = z(yield), zp = z(production_t)), by = adm1_pcode]

CAND <- c(MAM = "zM", OND_same = "zOs", OND_lag = "zOl",
          MAM_plus_OND_same = "zMOs", MAM_plus_OND_lag = "zMOl")
score <- function(dd, outcome = "zy") {
  rbindlist(lapply(names(CAND), function(nm) {
    x <- dd[[CAND[[nm]]]]; y <- dd[[outcome]]
    ok <- is.finite(x) & is.finite(y)
    if (sum(ok) < 8) return(NULL)
    ct <- suppressWarnings(cor.test(x[ok], y[ok]))
    data.table(mapping = nm, n = sum(ok), r = round(unname(ct$estimate), 3),
               p = signif(ct$p.value, 2))
  }))[order(-r)]
}
hdr <- function(t) cat("\n== ", t, " ==\n", sep = "")

hdr("ALL COUNTIES - yield")           ; print(score(d))
hdr("ALL COUNTIES - production")      ; print(score(d, "zp"))
for (rg in c("long-rains dominant", "mixed", "short-rains dominant")) {
  hdr(sprintf("%s (%d counties) - yield", toupper(rg), d[regime == rg, uniqueN(adm1_pcode)]))
  cat("   ", paste(sort(unique(d[regime == rg, adm1_name])), collapse = ", "), "\n")
  print(score(d[regime == rg]))
}
top <- d[, .(tot = sum(production_t)), by = .(adm1_pcode, adm1_name)][order(-tot)][1:12]
hdr(sprintf("TOP 12 MAIZE COUNTIES (%d%% of national production) - yield",
            round(100 * sum(top$tot) / d[, sum(production_t)])))
print(top[, .(county = adm1_name, kt = round(tot / 1000),
              regime = reg$regime[match(adm1_pcode, reg$adm1_pcode)])])
print(score(d[adm1_pcode %in% top$adm1_pcode]))

hdr("NATIONAL, area-weighted (n = 6; read with caution)")
nat <- d[, .(prod = sum(production_t), area = sum(area_ha),
             MAM = weighted.mean(MAM_ptot, area_ha),
             OND_same = weighted.mean(OND_ptot, area_ha),
             OND_lag = weighted.mean(OND_ptot_lag, area_ha)), by = year][order(year)]
nat[, yield := prod / area]
print(nat[, .(year, prod_kt = round(prod / 1000), yield = round(yield, 2),
              MAM = round(MAM), OND_same = round(OND_same), OND_lag = round(OND_lag))])
cat(sprintf("   cor(yield, MAM) = %.3f | cor(yield, OND same) = %.3f | cor(yield, OND lag) = %.3f\n",
            cor(nat$yield, nat$MAM), cor(nat$yield, nat$OND_same),
            cor(nat$yield, nat$OND_lag, use = "complete.obs")))
cat("\n   The national series favours same-year OND, but it is six points and 2023 drives it -",
    "\n   a year that also carried the fertiliser subsidy and a 15% area expansion. The county",
    "\n   evidence, where the maize actually is, says the opposite. Trust the county evidence.\n")
