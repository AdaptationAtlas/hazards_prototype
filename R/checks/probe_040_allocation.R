#!/usr/bin/env Rscript
# R/checks/probe_040_allocation.R
# ================================
# Read-only probe of R/0.4.0_create_crop_vop_intld15.R's allocation table (#38 / #39), run on
# the node BEFORE the value-changing 0.4.0 re-run. Evaluates the script exactly as it runs -
# SPAM load + resample, admin zonal totals, allocation groups, FAO GPV + production, the
# coverage guard - and stops before any raster is built or any file written (the audit CSV
# write is skipped too). Prints: the groups, every guarded pair, the named pairs behind #38 /
# #39 and the per-country share of GPV that the guard blanks. ~5-10 min (the SPAM zonal).
#
# Usage (cglabs, repo root): EXPOSURE_RES=0.25 Rscript R/checks/probe_040_allocation.R [--min-cov 0.10]
t0 <- Sys.time()
.ts  <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.log <- function(fmt, ...) cat(sprintf("[%s] [probe-040] %s\n", .ts(), sprintf(fmt, ...)))
args <- commandArgs(trailingOnly = TRUE)
opt  <- function(x, d) { i <- match(x, args); if (is.na(i) || i == length(args)) d else args[i + 1] }
Sys.setenv(VOP_COVERAGE_MIN = opt("--min-cov", Sys.getenv("VOP_COVERAGE_MIN", "0.10")))
if (!nzchar(Sys.getenv("EXPOSURE_RES"))) Sys.setenv(EXPOSURE_RES = "0.25")
if (!nzchar(Sys.getenv("FORCE_OVERWRITE"))) Sys.setenv(FORCE_OVERWRITE = "0")

setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
.log("sourcing %s", setup); suppressMessages(suppressWarnings(source(setup)))
suppressPackageStartupMessages({ library(data.table); library(terra) })

script <- file.path(project_dir, "R", "0.4.0_create_crop_vop_intld15.R")
src <- readLines(script)
cut <- grep("^spam_vop_intd <- vop_allocate_rasters", src)[1]
stopifnot(!is.na(cut))
.log("evaluating %s lines 1-%d (everything up to the raster allocation); skipping the audit-CSV write", basename(script), cut - 1)
ex <- parse(text = src[1:(cut - 1)], keep.source = FALSE)
for (e in ex) {
  txt <- paste(deparse(e), collapse = " ")
  if (grepl("0_server_setup", txt, fixed = TRUE)) next
  if (grepl("fwrite(alloc", txt, fixed = TRUE)) { .log("skipped: %s", substr(txt, 1, 60)); next }
  eval(e, envir = globalenv())
}
stopifnot(exists("alloc"), exists("groups"), exists("spam_nat"))

.log("--- allocation groups with more than one layer or item")
print(groups[, .(layers = paste(unique(layer), collapse = " + "), items = paste(unique(item), collapse = " + ")), by = group][grepl("\\+", layers) | grepl("\\+", items)], nrows = 20)
.log("--- allocation table: %d pairs | guarded %d | not judged %d | continental GPV %.2f B I$, of which guarded %.2f B I$ (%.1f%%)",
     nrow(alloc), alloc[guarded == TRUE, .N], alloc[grepl("not judged", reason), .N], sum(alloc$gpv) / 1e6, alloc[guarded == TRUE, sum(gpv)] / 1e6, 100 * alloc[guarded == TRUE, sum(gpv)] / sum(alloc$gpv))
show <- function(d) d[, .(iso3, group, gpv_MI = round(gpv / 1e3, 1), fao_kt = round(fao_prod_t / 1e3, 1), spam_kt = round(spam_prod_t / 1e3, 1), coverage = signif(coverage, 3), guarded, reason)]
.log("--- every guarded pair (value will be NA)"); print(show(alloc[guarded == TRUE][order(iso3, -gpv)]), nrows = 300)
.log("--- share of each country's crop GPV blanked by the guard (countries with any)")
print(alloc[, .(gpv_B = round(sum(gpv) / 1e6, 2), guarded_B = round(sum(gpv[guarded]) / 1e6, 2), share = round(sum(gpv[guarded]) / sum(gpv), 3), n_guarded = sum(guarded), n = .N), by = iso3][n_guarded > 0][order(-share)], nrows = 60)
.log("--- coverage distribution over allocated pairs (SPAM national t / FAO production t)")
cv <- alloc[guarded == FALSE & is.finite(coverage), coverage]
cat(sprintf("   n=%d | 5%%=%.2f 25%%=%.2f median=%.2f 75%%=%.2f 95%%=%.2f | pairs with coverage < 0.5: %d, > 2: %d\n", length(cv), quantile(cv, .05), quantile(cv, .25), median(cv), quantile(cv, .75), quantile(cv, .95), sum(cv < .5), sum(cv > 2)))
.log("--- named pairs (#38 millet, #39 Sudan + Nigeria banana, #40 Seychelles)")
named <- rbind(data.table(iso3 = c("KEN", "ETH", "UGA", "TZA"), group = "pmil+smil"), data.table(iso3 = c("NGA", "UGA", "GHA", "CMR"), group = "banpl"),
               data.table(iso3 = "SDN", group = c("sorg", "grou", "sesa", "whea", "pmil+smil", "sugc")), data.table(iso3 = "SYC", group = "cnut"), data.table(iso3 = c("ETH", "KEN"), group = "acof+rcof"))
print(show(merge(named, alloc, by = c("iso3", "group"), all.x = TRUE)), nrows = 40)
.log("--- SPAM national tonnage inside the compound groups (the split the value will follow)")
print(merge(spam_nat, unique(groups[, .(layer, group)]), by = "layer")[group %in% c("pmil+smil", "acof+rcof", "banpl") & iso3 %in% c("KEN", "ETH", "UGA", "TZA", "NGA", "GHA", "CMR", "CIV", "RWA", "BDI")][order(group, iso3, layer), .(group, iso3, layer, spam_kt = round(prod_t / 1e3, 1))], nrows = 60)
.log("done in %.1f min (read-only; nothing written)", as.numeric(difftime(Sys.time(), t0, units = "mins")))
