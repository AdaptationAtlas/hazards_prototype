#!/usr/bin/env Rscript
# R/checks/fixture_publish_tier_gates.R
# =====================================
# Off-node fixture for the 2026-10-07 publish changes: the multi-variable key scheme in
# scripts/r3_publish_tiers.R and the new first-publish gate G6b (tier_vs_exposure) in
# R/checks/r3_tier_drift_vs_live.R. Synthetic parquets, no data, seconds.
#
# Why G6b exists, and so why it is worth a fixture of its own: G6 judges a tier against whatever is
# live at its key. Four of the keys this bake writes have never been published, so for those G6 is
# skipped entirely and the first version would reach the Atlas with no value gate. G6b uses the
# invariant that needs no live object - freq_any + freq_none = 1 per pixel, so a tier's admin0
# historic any+none total per (iso3, crop) IS the exposure total it was multiplied by.
#
# A gate that cannot fail is worse than no gate, because it reads as assurance. So every assertion
# below is paired: the honest product passes, and a specific corruption of it fails.
#
# Usage: Rscript R/checks/fixture_publish_tier_gates.R
suppressPackageStartupMessages({ library(data.table); library(arrow); library(dplyr) })
root <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else {
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd()
}
source(file.path(root, "R", "checks", "r3_tier_drift_vs_live.R"))
ok <- function(cond, msg) { if (!isTRUE(cond)) stop("FIXTURE FAIL: ", msg) else cat("  ok  ", msg, "\n") }
tmp <- file.path(tempdir(), "pubgates"); dir.create(tmp, showWarnings = FALSE)

SCEN <- c("historic", "ssp126", "ssp245", "ssp370", "ssp585")
HV   <- c("NDWS+NTx35", "NDWS+NTx35+NDWL0")
PAIRS <- data.table(iso3 = c("KEN", "KEN", "NGA", "NGA", "AGO"),
                    crop = c("maize", "wheat", "maize", "cowpea", "maize"),
                    E    = c(4e6, 2e5, 9e6, 7e5, 3e5))

# A product shaped like R/3 §4.2's output. `scale` lets a caller corrupt one pair. Admin1 rows are
# included because .drift_read must filter them out - a gate that accidentally summed admin0 and
# admin1 would read as a 2x product shift.
mk_product <- function(f, pairs = PAIRS, scale = NULL, extra = NULL) {
  p <- copy(pairs)
  if (!is.null(scale)) p[iso3 == scale$iso3 & crop == scale$crop, E := E * scale$by]
  if (!is.null(extra)) p <- rbind(p, extra)
  rows <- CJ(i = seq_len(nrow(p)), scenario = SCEN, hazard_vars = HV, hazard = c("any", "none"), unique = TRUE)
  rows[, `:=`(iso3 = p$iso3[i], crop = p$crop[i], E = p$E[i])]
  # any + none must total the exposure; split them unevenly so a gate that silently used only one
  # of the two would not land on the right number by luck.
  rows[, value := ifelse(hazard == "any", 0.3 * E, 0.7 * E)]
  adm0 <- rows[, .(iso3, admin0_name = iso3, admin1_name = NA_character_, admin2_name = NA_character_,
                   crop, scenario, hazard_vars, hazard, severity = "severe", value)]
  adm1 <- copy(adm0)[, `:=`(admin1_name = "a-province", value = value / 2)]
  write_parquet(rbind(adm0, adm1), f); f
}
# A 0.4.4-shaped exposure basis: several `exposure` values side by side, as the combined reference
# has, so filtering on the wrong one is a real possibility the gate has to catch.
mk_exposure <- function(f, pairs = PAIRS) {
  base <- pairs[, .(iso3, admin0_name = iso3, admin1_name = NA_character_, admin2_name = NA_character_, crop, tech = "all")]
  d <- rbind(
    cbind(base, exposure = "prod",      unit = "t",                value = pairs$E),
    cbind(base, exposure = "vop",       unit = "nominal-usd-2021", value = pairs$E * 311),
    cbind(base, exposure = "harv-area", unit = "ha",               value = pairs$E / 3))
  # The basis read sums the ADMIN2 rows (#18: the independently-zoned admin0 rows disagree with the
  # product's derived totals at borders). Two districts summing to each pair's E, and an admin0 row
  # DELIBERATELY inflated for one pair - the gate must judge on admin2 and ignore it.
  d2a <- copy(d)[, `:=`(admin1_name = "a-province", admin2_name = "dist-1", value = value * 0.6)]
  d2b <- copy(d)[, `:=`(admin1_name = "a-province", admin2_name = "dist-2", value = value * 0.4)]
  d[iso3 == "KEN" & crop == "maize", value := value * 30]   # the KEN:robusta-coffee shape, 29.9x
  write_parquet(rbind(d, d2a, d2b), f); f
}

prod_f <- mk_product(file.path(tmp, "good_prod_t.parquet"))
expo_f <- mk_exposure(file.path(tmp, "exposure_res-25.parquet"))

## ---------------------------------------------------------------- the honest product passes
g <- tier_vs_exposure(prod_f, expo_f, "prod")
print(g$gates)
ok(isTRUE(g$pass), "G6b passes a product whose any+none reproduces its exposure input")
ok(nrow(g$material) == nrow(PAIRS) && max(abs(g$material$ratio - 1)) < 1e-9,
   "every pair is material and lands on ratio 1 exactly - INCLUDING the pair whose independently-zoned admin0 row is inflated 30x, because the basis is the admin2 rows the product was built from (#18; the KEN robusta-coffee case)")

## ---------------------------------------------------------------- a shifted pair fails
bad1 <- mk_product(file.path(tmp, "shifted.parquet"), scale = list(iso3 = "NGA", crop = "maize", by = 1.5))
g1 <- tier_vs_exposure(bad1, expo_f, "prod")
ok(!g1$pass, "G6b fails a product where one material pair moved 50 % away from its exposure")
ok(g1$worst$iso3 == "NGA" && g1$worst$crop == "maize" && abs(g1$worst$ratio - 1.5) < 1e-9,
   "and names the pair, with the ratio, so the cause can be chased")

## ---------------------------------------------------------------- a 2 % move is inside the bound
bad2 <- mk_product(file.path(tmp, "small.parquet"), scale = list(iso3 = "NGA", crop = "maize", by = 1.01))
ok(isTRUE(tier_vs_exposure(bad2, expo_f, "prod", tol_pair = 0.02)$pass),
   "a 1 % move passes the default 2 % bound (rounding and the #18 admin0-vs-admin2 split live in there)")
ok(!tier_vs_exposure(bad2, expo_f, "prod", tol_pair = 0.005)$pass,
   "and the bound is real: the same product fails at 0.5 %")

## ---------------------------------------------------------------- invented value fails
inv <- mk_product(file.path(tmp, "invented.parquet"),
                  extra = data.table(iso3 = "KEN", crop = "coffee", E = 5e6))   # no exposure row for KEN coffee
gi <- tier_vs_exposure(inv, expo_f, "prod")
ok(!gi$pass && nrow(gi$invented) == 1 && gi$invented$crop == "coffee",
   "G6b fails a product that carries value for a pair the exposure has no row for (there was nothing to multiply)")

## ---------------------------------------------------------------- named residuals: allowed WITH a reason, and only those
# The Wete/Pemba case (#13 B5): one district with a hazard-coverage hole breaches one pair's bound
# while the product is right everywhere the hazard layer has a pixel. A named, reasoned pair passes
# as ALLOWED; an unnamed breach still fails; naming must not blunt the bound for anything else.
shift2 <- mk_product(file.path(tmp, "named.parquet"), scale = list(iso3 = "NGA", crop = "maize", by = 0.97))
allow_tab <- data.table(iso3 = "NGA", crop = "maize", reason = "synthetic Wete: one admin2 unit uncovered by the hazard layer")
ga <- tier_vs_exposure(shift2, expo_f, "prod", tol_pair = 0.02, allow = allow_tab)
ok(isTRUE(ga$pass) && nrow(ga$allowed) == 1 && ga$allowed$reason != "",
   "a NAMED pair breaching the bound passes as ALLOWED, carrying its reason into the report")
ok(!tier_vs_exposure(shift2, expo_f, "prod", tol_pair = 0.02)$pass,
   "the same product FAILS without the naming - the exemption is the list, not a loosened bound")
ga2 <- tier_vs_exposure(bad1, expo_f, "prod", tol_pair = 0.02,
                        allow = data.table(iso3 = "KEN", crop = "wheat", reason = "names a DIFFERENT pair"))
ok(!ga2$pass && ga2$worst$iso3 == "NGA",
   "an UNNAMED breach still fails even with a list present - the list excuses only the pairs it names")
qres <- fread(file.path(root, "metadata", "g6b_expected_residuals.csv"))
ok(all(c("iso3", "crop", "reason", "decided") %in% names(qres)) && nrow(qres[iso3 == "TZA" & crop == "coconut"]) == 1 &&
   qres[iso3 == "TZA", grepl("Wete", reason) && grepl("#50", reason)],
   "the shipped file carries TZA:coconut with the Wete evidence and the #50 pointer - a residual needs a reason, not just a row")

## ---------------------------------------------------------------- a dropped pair fails
drop <- mk_product(file.path(tmp, "dropped.parquet"), pairs = PAIRS[!(iso3 == "KEN" & crop == "maize")])
gd <- tier_vs_exposure(drop, expo_f, "prod")
ok(!gd$pass && nrow(gd$missing_product) == 1 && gd$missing_product$iso3 == "KEN",
   "G6b fails when a material exposure pair is missing from the product (a silently truncated tier)")

## ---------------------------------------------------------------- declared unmodelled crops (#13 step 2)
# The intld twin carries nine SPAM composite groups (B5 recovered their 62.42 B I$) that R/3 cannot
# model - no ecocrop entry, no crop stack, nothing to multiply - so row 3 read them as 389 missing
# pairs on a tier whose ratios were clean at 0.0059. Declared crops are reported, not gated; an
# UNDECLARED missing crop still fails, which is the truncation row 3 exists to catch.
expo_comp <- mk_exposure(file.path(tmp, "exposure_comp.parquet"),
                         rbind(PAIRS, data.table(iso3 = "KEN", crop = "rest-of-crops", E = 9e6)))
gu <- tier_vs_exposure(prod_f, expo_comp, "prod")
ok(isTRUE(gu$pass) && nrow(gu$unmodelled) == 1 && gu$unmodelled$crop == "rest-of-crops",
   "a declared composite group with exposure and no product row is REPORTED as unmodelled, not failed")
gu2 <- tier_vs_exposure(prod_f, expo_comp, "prod", no_product_crops = character(0))
ok(!gu2$pass && nrow(gu2$missing_product) == 1,
   "...and without the declaration the same pair FAILS row 3 - the list is falsifiable, not a blanket")
expo_tr <- mk_exposure(file.path(tmp, "exposure_tr.parquet"),
                       rbind(PAIRS, data.table(iso3 = "KEN", crop = "sorghum", E = 9e6)))
ok(!tier_vs_exposure(prod_f, expo_tr, "prod")$pass,
   "a REAL crop with exposure and no product row still fails - a truncated tier cannot hide behind the composites")

## ---------------------------------------------------------------- the materiality floor is unit-aware (#13 step 3)
# R/3 rounds every tier to integers per pixel, so zonal relative error falls with size: measured on
# ha, every pair above 1e4 was within 0.44 % while six pairs under 8,213 ha breached 2 % on rounding
# alone. A floor of 1e3 is noise in USD and a real area in hectares, so the floor lives in VAR_SPECS
# per tier. Below it, a breaching pair is reported immaterial, not failed; a real breach above it
# still fails.
tiny <- mk_product(file.path(tmp, "tiny.parquet"),
                   pairs = rbind(PAIRS, data.table(iso3 = "MDG", crop = "tea", E = 1018)),
                   scale = list(iso3 = "MDG", crop = "tea", by = 0.93))
expo_tiny <- mk_exposure(file.path(tmp, "expo_tiny.parquet"), rbind(PAIRS, data.table(iso3 = "MDG", crop = "tea", E = 1018)))
ok(isTRUE(tier_vs_exposure(tiny, expo_tiny, "prod", min_exposure = 1e4)$pass),
   "a 7 % breach on a 1,018-unit pair passes under the physical tiers' 1e4 floor - integer rounding, not a defect")
ok(!tier_vs_exposure(tiny, expo_tiny, "prod", min_exposure = 1e3)$pass,
   "...and fails under the old unit-blind 1e3 floor, which is the #13 step-3 stop replayed")
ok(!tier_vs_exposure(mk_product(file.path(tmp, "big_breach.parquet"), scale = list(iso3 = "NGA", crop = "maize", by = 0.93)),
                     expo_f, "prod", min_exposure = 1e4)$pass,
   "a 7 % breach on a 9-million-unit pair still fails above the floor - the floor scopes materiality, not the bound")
## ---------------------------------------------------------------- the wrong basis cannot pass quietly
e <- tryCatch({ tier_vs_exposure(prod_f, expo_f, "vop-nonsense"); NULL }, error = function(e) conditionMessage(e))
ok(!is.null(e) && grepl("matches no rows", e) && grepl("prod/t", e),
   "an exposure_var that matches nothing aborts and lists the (exposure, unit) pairs present, instead of degrading to no gate")
ok(!tier_vs_exposure(prod_f, expo_f, "vop")$pass,
   "pointing the production tier at the vop rows fails - the basis is checked by value, not assumed from the filename")

## ---------------------------------------------------------------- the key scheme
# VAR_SPECS lives inside the publish script, which sources setup; lift just that assignment out.
pub <- file.path(root, "scripts", "r3_publish_tiers.R")
for (ex in parse(pub)) {
  if (is.call(ex) && length(ex) >= 3 && as.character(ex[[1]])[1] %in% c("<-", "=") &&
      is.name(ex[[2]]) && as.character(ex[[2]]) %in% c("VAR_SPECS", "S3_KEY_BASE", "s3_base_for")) eval(ex, envir = globalenv())
}
ok(setequal(names(VAR_SPECS), c("vop_usd", "vop_intld", "ha", "prod_t", "head_n")),
   "all five tiers R/3 produces have a publish spec")
.s3 <- vapply(VAR_SPECS, function(x) x$s3_var, character(1))
ok(!anyDuplicated(.s3), "every tier gets its own variable= segment, so no two tiers can overwrite each other's key")
ok(identical(VAR_SPECS$vop_usd$s3_var, "vop_nominal-usd21"),
   "the live, notebook-read usd key keeps its irregular historical spelling")
.dirs <- vapply(VAR_SPECS, function(x) x$dir_key, character(1))
ok(!anyDuplicated(.dirs), "no two tiers read the same local directory")
ok(setequal(vapply(VAR_SPECS, function(x) x$twin_expo, character(1)),
            c("vop", "vop", "harv-area", "prod", "number")),
   "each tier declares which `exposure` rows of its 0.4.4 basis are its own")
ok(identical(unname(vapply(VAR_SPECS, function(x) x$g6b_min, numeric(1))), c(1e3, 1e3, 1e4, 1e4, 1e4)),
   "currency tiers keep the 1e3 materiality floor; the physical tiers carry 1e4 of their own unit")
ok(all(c("total-highland", "total-tropical") %in% G6B_NO_PRODUCT_CROPS) && length(G6B_NO_PRODUCT_CROPS) == 11,
   "the TLU aggregates are declared unmodelled alongside the nine crop composites - a weighted index in the headcount slot, which the ten-entry head_n vocabulary can never carry")
k <- s3_base_for(VAR_SPECS$prod_t, "annual", "ENSEMBLE")
ok(identical(k, paste0("domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/",
                       "processing=hazard-risk-exposure/variable=prod_t/period=annual/model=ENSEMBLE")),
   "the key carries variable, period and model on their own axes, so annual and ENSEMBLE need no new scheme")
ok(identical(s3_base_for(VAR_SPECS$vop_usd, "jagermeyr", "ENSEMBLEmean"),
             paste0("domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/",
                    "processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=jagermeyr/model=ENSEMBLEmean")),
   "and the live usd key is byte-identical to what has been published since 2026-09")

## ---------------------------------------------------------------- G6 declared hazard_vars change
# #13: the bake's whole purpose is to retire PTOT-L+NTxS+PTOT-G and add the per-crop optimum sets, so
# strict parity against the pre-#13 live object CANNOT pass - and silently waving it through with
# --allow-value-drift would also wave through the value populations. The declaration makes parity
# check the change we INTEND: live sets minus retired plus added must equal the local sets, and the
# row arithmetic must close exactly. Undeclared differences still fail.
mk_hv <- function(f, sets) {
  rows <- CJ(i = seq_len(nrow(PAIRS)), scenario = SCEN, hazard_vars = sets, hazard = c("any", "none"), unique = TRUE)
  rows[, `:=`(iso3 = PAIRS$iso3[i], crop = PAIRS$crop[i], value = ifelse(hazard == "any", 0.3, 0.7) * PAIRS$E[i])]
  write_parquet(rows[, .(iso3, admin0_name = iso3, admin1_name = NA_character_, admin2_name = NA_character_,
                         crop, scenario, hazard_vars, hazard, severity = "severe", value)], f); f
}
live_f  <- mk_hv(file.path(tmp, "hv_live.parquet"),  c("NDWS+NTx35+NDWL0", "PTOT-L+NTxS+PTOT-G"))
local_f <- mk_hv(file.path(tmp, "hv_local.parquet"), c("NDWS+NTx35+NDWL0", "PTOT-L+NTxM+PTOT-G", "NDWS+NTxM+NDWL0"))
pg <- function(res) res$gates[gate == "parity" & population %in% c("rows", "hazard_vars")]
r_strict <- tier_drift(local_f, live_f, min_live = 1)
ok(all(pg(r_strict)$result == "FAIL"),
   "with no declaration, strict parity FAILs on both rows and hazard_vars - every existing invocation means what it meant")
r_decl <- tier_drift(local_f, live_f, min_live = 1,
                     hv_retired = "PTOT-L+NTxS+PTOT-G", hv_added = c("PTOT-L+NTxM+PTOT-G", "NDWS+NTxM+NDWL0"))
ok(all(pg(r_decl)$result == "ok") && grepl("closes exactly", pg(r_decl)[population == "rows", detail]),
   "the declared retirement + additions make parity PASS, with the row arithmetic closing exactly")
r_wrong <- tier_drift(local_f, live_f, min_live = 1,
                      hv_retired = "PTOT-L+NTxS+PTOT-G", hv_added = "PTOT-L+NTxM+PTOT-G")
ok(any(pg(r_wrong)$result == "FAIL"),
   "an INCOMPLETE declaration still FAILs - the gate checks the stated change, not the presence of a statement")

cat("\nALL PUBLISH-GATE FIXTURE ASSERTIONS PASSED\n")
