#!/usr/bin/env Rscript
# R/checks/vop_align_live_gate.R
# ==============================
# LIVE-ARTIFACT twin of R/qaqc_vop_vs_faostat.R. That gate reads the VoP rasters on
# the node; this one reads the PUBLISHED exposure parquet on S3, so the VoP basis of
# what users actually get can be checked from any machine with no pipeline compute.
#
# Question: does the published crop + livestock VoP still distribute FAOStat Gross
# Production Value in **constant international dollars**? Distribution is
# proportion-based (shares sum to 1 per country), so the published national total
# must come back ~1x the FAOStat national GPV it was built from.
#   ratio ~1        -> basis sound, currencies aligned (I$)
#   ratio off       -> currency/units/vintage error in the LIVE product
#
# Populations are split BEFORE any bound is tested (2026-09-16 lesson: two false
# fails blocked a correct publish):
#   material     - reference clears GATE_MIN_REF; ratio-gated
#   immaterial   - reference is noise; reported, and failed only if the product
#                  invents value against it (> GATE_MAX_ABS)
#   out-of-scope - structurally absent by product design, asserted not assumed
#   unmatched    - no product/reference pair exists; cannot be gated, only reported
# signif() everywhere, never round(): a ratio of 0.4 must not print as 0.
#
# Run (any machine, ~1 min, needs duckdb CLI on PATH):
#   Rscript R/checks/vop_align_live_gate.R
# Env: GATE_MIN_REF (1e5) GATE_MAX_ABS (1e6) GATE_CACHE (tempdir) GATE_REFRESH=1
# Read-only against S3 + FAOStat. Exit 0 = PASS, 1 = FAIL.

suppressPackageStartupMessages({ library(data.table); library(countrycode) })
t0   <- Sys.time()
.ts  <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.log <- function(fmt, ...) { cat(sprintf("[%s] [gate-vop-live] %s\n", .ts(), sprintf(fmt, ...))); flush.console() }
.step <- function(label, expr) {
  s <- Sys.time(); on.exit(.log("%-38s %5.1fs", label, as.numeric(difftime(Sys.time(), s, units = "secs"))))
  force(expr)
}

MIN_REF <- as.numeric(Sys.getenv("GATE_MIN_REF", "1e5"))
MAX_ABS <- as.numeric(Sys.getenv("GATE_MAX_ABS", "1e6"))
CACHE   <- Sys.getenv("GATE_CACHE", file.path(tempdir(), "vop_align_live_gate"))
REFRESH <- nzchar(Sys.getenv("GATE_REFRESH"))
YEARS   <- 2019:2023   # matches the vop_intld15-2021 window used by 0.4.0 / 0.4.1
FAO_EL  <- "Gross Production Value (constant 2014-2016 thousand I$)"
FAO_URL <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Value_of_Production_E_Africa.zip"
LIVE    <- paste0("https://digital-atlas.s3.amazonaws.com/domain=exposure/type=combined/",
                  "source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/",
                  "variable=vop_intld15-2021.parquet")

# MapSPAM-Africa is clipped to SSA, so North Africa has a real FAOStat crop GPV and a
# structurally-zero product. GLW4 is global, so the same countries DO carry livestock.
# Different scope per commodity type -- conflating them is what makes a passing gate fail.
CROP_OUT_OF_SCOPE <- strsplit(Sys.getenv("GATE_CROP_OOS", "DZA,EGY,LBY,MAR,TUN"), ",")[[1]]
# R/qaqc_vop_vs_faostat.R remove_countries as iso3: excluded islands + former-state splits
LVST_OUT_OF_SCOPE <- strsplit(Sys.getenv("GATE_LVST_OOS", "CPV,COM,MUS,REU,SYC"), ",")[[1]]

dir.create(CACHE, recursive = TRUE, showWarnings = FALSE)
repo <- if (file.exists("metadata/SPAM2010_FAO_crops.csv")) "." else Sys.getenv("project_dir")

# ---------------------------------------------------------------- product side
# One duckdb call per file with retries: a UNION over S3 dies on Snappy range errors.
.dd <- function(sql, out) {
  for (i in 1:3) {
    ok <- system2("duckdb", c("-csv", "-c", shQuote(paste("LOAD httpfs;", sql))),
                  stdout = out, stderr = file.path(CACHE, "duckdb.err"))
    if (ok == 0 && file.exists(out) && file.size(out) > 0) return(invisible(TRUE))
    .log("duckdb attempt %d failed, retrying", i); Sys.sleep(5)
  }
  stop("duckdb failed 3x: ", paste(readLines(file.path(CACHE, "duckdb.err"), warn = FALSE), collapse = " | "))
}
crop_f <- file.path(CACHE, "live_crop_vop_adm0.csv")
lvst_f <- file.path(CACHE, "live_lvst_vop_adm0.csv")
if (REFRESH || !file.exists(crop_f)) .step("extract live crop VoP (adm0)", .dd(sprintf(
  "SELECT iso3, admin0_name, crop, unit, sum(value) AS grid_vop FROM read_parquet('%s')
   WHERE admin1_name IS NULL AND admin2_name IS NULL AND tech = 'all' GROUP BY ALL ORDER BY iso3, crop;",
  LIVE), crop_f))
if (REFRESH || !file.exists(lvst_f)) .step("extract live livestock VoP (adm0)", .dd(sprintf(
  "SELECT iso3, admin0_name, crop, unit, sum(value) AS grid_vop FROM read_parquet('%s')
   WHERE admin1_name IS NULL AND admin2_name IS NULL AND tech IS NULL GROUP BY ALL ORDER BY iso3, crop;",
  LIVE), lvst_f))
crop_p <- fread(crop_f); lvst_p <- fread(lvst_f)
.log("product: %d crop rows (%d crops) + %d livestock rows | unit=%s",
     nrow(crop_p), uniqueN(crop_p$crop), nrow(lvst_p),
     paste(unique(c(crop_p$unit, lvst_p$unit)), collapse = ","))

# -------------------------------------------------------------- reference side
zipf <- file.path(CACHE, "vop.zip"); csvf <- file.path(CACHE, "Value_of_Production_E_Africa_NOFLAG.csv")
if (REFRESH || !file.exists(csvf)) .step("download FAOStat VoP (Africa)", {
  utils::download.file(FAO_URL, zipf, quiet = TRUE); utils::unzip(zipf, exdir = CACHE)
})
fao <- .step("read FAOStat const-I$ GPV", {
  d <- fread(csvf, encoding = "UTF-8")[Element == FAO_EL]
  yc <- paste0("Y", YEARS); stopifnot(all(yc %in% names(d)))
  d <- d[, c("Area Code (M49)", "Area", "Item Code", yc), with = FALSE]
  setnames(d, c("m49", "area", "code_fao", yc))
  d[, code_fao := as.integer(code_fao)]
  d[, iso3 := countrycode(as.integer(gsub("[^0-9]", "", as.character(m49))), "un", "iso3c", warn = FALSE)]
  d
})
fao_unresolved <- fao[is.na(iso3), unique(area)]
fao <- melt(fao[!is.na(iso3)], id.vars = c("iso3", "code_fao"), measure.vars = paste0("Y", YEARS),
            variable.name = "year", value.name = "gpv_k")
fao[, gpv_k := as.numeric(gpv_k)]
# thousand I$ -> I$, median across the window (identical basis to 0.4.0 / 0.4.1)
fao_item <- fao[, .(fao_vop_i = median(gpv_k, na.rm = TRUE) * 1000), by = .(iso3, code_fao)]

# --------------------------------------------------- the shared gate machinery
gate <- function(pair, label, out_of_scope, notes = character()) {
  d <- copy(pair)
  ck <- names(d)[3]   # the commodity column: "commodity" for crops, "species" for livestock
  d[, population := fifelse(iso3 %in% out_of_scope, "out-of-scope",
                    fifelse(is.na(grid_vop) | is.na(fao_vop_i), "unmatched",
                    fifelse(is.finite(fao_vop_i) & fao_vop_i >= MIN_REF, "material", "immaterial")))]
  d[, ratio := grid_vop / fao_vop_i]
  mat <- d[population == "material"]; imm <- d[population == "immaterial"]
  oos <- d[population == "out-of-scope"]; unm <- d[population == "unmatched"]
  inv <- imm[!is.na(grid_vop) & grid_vop > MAX_ABS]
  oosv <- oos[!is.na(grid_vop) & grid_vop > MAX_ABS]
  ob  <- mat[!is.na(ratio) & (ratio < 0.5 | ratio > 2.0)]

  cat(sprintf("\n===================== %s =====================\n", label))
  cat(sprintf("POPULATIONS: material %d | immaterial %d | out-of-scope %d | unmatched %d  (of %d pairs)\n",
              nrow(mat), nrow(imm), nrow(oos), nrow(unm), nrow(d)))
  for (n in notes) cat(sprintf("  note: %s\n", n))
  cat("\n--- MATERIAL (ratio-gated) ---\n")
  cat(sprintf("  median ratio %s | range [%s, %s] | within 0.90-1.10: %d/%d | out of [0.50,2.00]: %d\n",
              signif(median(mat$ratio, na.rm = TRUE), 4), signif(min(mat$ratio, na.rm = TRUE), 4),
              signif(max(mat$ratio, na.rm = TRUE), 4), mat[abs(ratio - 1) <= 0.10, .N], nrow(mat), nrow(ob)))
  if (nrow(ob)) print(ob[order(-abs(log(ratio)))][, .(iso3, commodity = get(ck),
      grid_vop = signif(grid_vop, 4), fao_vop_i = signif(fao_vop_i, 4), ratio = signif(ratio, 4))])
  cat("  worst 8 material by |log(ratio)|:\n")
  print(mat[!is.na(ratio) & ratio > 0][order(-abs(log(ratio)))][1:min(8, .N),
      .(iso3, commodity = get(ck), grid_vop = signif(grid_vop, 4),
        fao_vop_i = signif(fao_vop_i, 4), ratio = signif(ratio, 4))])
  cat(sprintf("\n--- IMMATERIAL (ref < %s; reported, not ratio-gated) --- %d pairs, %d inventing value > %s\n",
              signif(MIN_REF, 3), nrow(imm), nrow(inv), signif(MAX_ABS, 3)))
  if (nrow(inv)) print(inv[order(-grid_vop)][, .(iso3, commodity = get(ck),
      grid_vop = signif(grid_vop, 4), fao_vop_i = signif(fao_vop_i, 4))])
  cat(sprintf("--- OUT-OF-SCOPE (by product design) --- %d pairs, %d claiming value > %s (would be a finding)\n",
              nrow(oos), nrow(oosv), signif(MAX_ABS, 3)))
  if (nrow(oosv)) print(oosv[order(-grid_vop)][1:min(6, .N), .(iso3, commodity = get(ck), grid_vop = signif(grid_vop, 4))])
  cat(sprintf("--- UNMATCHED (no pair) --- %d pairs: %s\n", nrow(unm),
              paste(sort(unique(unm$iso3)), collapse = ",")))

  med <- median(mat$ratio, na.rm = TRUE)
  ok <- c(`median-in-band` = isTRUE(med >= 0.90 && med <= 1.10), `0 material out of [0.5,2]` = nrow(ob) == 0,
          `0 invented-value` = nrow(inv) == 0, `out-of-scope silent` = nrow(oosv) == 0)
  cat(sprintf("VERDICT %s: %s  (%s)\n", label, if (all(ok)) "PASS" else "FAIL",
              paste(sprintf("%s %s", names(ok), ok), collapse = " | ")))
  list(pass = all(ok), median = med, data = d)
}

# ------------------------------------------------------------- CROP: 32 crops
# Join on FAO item CODE, not item name: names are ambiguous and two SPAM crops can
# share one FAO item (coffee 656 = acof + rcof) while one crop can span two (rape).
map <- fread(file.path(repo, "metadata/SPAM2010_FAO_crops.csv"), encoding = "UTF-8")
setnames(map, c("code_spam2010", "short_spam2010", "long_spam2010", "code_fao", "name_fao", "name_fao_val"))
map[, `:=`(code_fao = as.integer(code_fao), prod_crop = gsub(" ", "-", long_spam2010))]
in_prod <- unique(crop_p$crop)
mapc <- map[prod_crop %in% in_prod, .(prod_crop, short_spam2010, code_fao)]
notes_c <- character()
unmapped <- setdiff(in_prod, unique(mapc$prod_crop))
if (length(unmapped)) notes_c <- c(notes_c, sprintf("product crops with no FAO mapping: %s", paste(unmapped, collapse = ", ")))
shared <- mapc[, uniqueN(prod_crop), by = code_fao][V1 > 1, code_fao]
if (length(shared)) notes_c <- c(notes_c, sprintf("FAO items shared by >1 product crop, counted once: %s", paste(shared, collapse = ", ")))
# a FAO item whose SPAM backing is only partly in the product makes the reference
# structurally larger (millet 79 = pmil + smil, and smil is absent) -- named, not hidden
partial <- map[code_fao %in% unique(mapc$code_fao),
               .(missing = paste(setdiff(short_spam2010, map[prod_crop %in% in_prod, short_spam2010]), collapse = "+")),
               by = code_fao][nzchar(missing)]
if (nrow(partial)) notes_c <- c(notes_c, sprintf("FAO items only PARTLY represented (code:missing-SPAM): %s",
                                paste(sprintf("%d:%s", partial$code_fao, partial$missing), collapse = ", ")))
crop_pair <- merge(
  crop_p[, .(grid_vop = sum(grid_vop, na.rm = TRUE)), by = .(iso3, admin0_name)],
  fao_item[code_fao %in% unique(mapc$code_fao), .(fao_vop_i = sum(fao_vop_i, na.rm = TRUE)), by = iso3],
  by = "iso3", all = TRUE)
crop_pair[, commodity := "ALL-CROPS"]
res_c <- gate(crop_pair[, .(iso3, admin0_name, commodity, grid_vop, fao_vop_i)],
              "CROP national total (const I$)", CROP_OUT_OF_SCOPE, notes_c)

# -------------------------------------------------------- LIVESTOCK: 5 species
# lps2fao + the "(indigenous)" meat substitution R/qaqc_vop_vs_faostat.R applies,
# resolved to FAO item codes: meat (indigenous) + milk / eggs per species.
lps_codes <- rbindlist(list(
  data.table(species = "cattle",  code_fao = c(944L, 882L)),   # meat + raw milk
  data.table(species = "sheep",   code_fao = c(1012L, 982L)),
  data.table(species = "goats",   code_fao = c(1032L, 1020L)),
  data.table(species = "pigs",    code_fao = 1055L),
  data.table(species = "poultry", code_fao = c(1094L, 1062L))  # chicken meat + hen eggs
))
lvst_p[, species := gsub("-(highland|tropical)$", "", crop)]
lvst_pair <- merge(
  lvst_p[, .(grid_vop = sum(grid_vop, na.rm = TRUE)), by = .(iso3, admin0_name, species)],
  merge(lps_codes, fao_item, by = "code_fao")[, .(fao_vop_i = sum(fao_vop_i, na.rm = TRUE)), by = .(iso3, species)],
  by = c("iso3", "species"), all = TRUE)
res_l <- gate(lvst_pair[, .(iso3, admin0_name, species, grid_vop, fao_vop_i)],
              "LIVESTOCK per species (const I$)", LVST_OUT_OF_SCOPE,
              "buffalo / camel / other poultry excluded both sides, as on the node")
cat("\nper-species medians (material only):\n")
print(res_l$data[population == "material", .(n = .N, median_ratio = signif(median(ratio, na.rm = TRUE), 4),
      in_band = sum(abs(ratio - 1) <= 0.10)), by = species][order(species)])

if (length(fao_unresolved)) cat(sprintf("\nFAO areas countrycode could not resolve (expected: former states): %s\n",
                                        paste(fao_unresolved, collapse = ", ")))
out <- rbind(res_c$data[, .(commodity_type = "crop", iso3, commodity, grid_vop, fao_vop_i, ratio, population)],
             res_l$data[, .(commodity_type = "livestock", iso3, commodity = species, grid_vop, fao_vop_i, ratio, population)])
fwrite(out[order(commodity_type, population, iso3, commodity)], file.path(CACHE, "vop_align_live_gate.csv"))
cat(sprintf("\nOVERALL: %s  (crop %s median %s | livestock %s median %s)\n",
            if (res_c$pass && res_l$pass) "PASS" else "FAIL",
            if (res_c$pass) "PASS" else "FAIL", signif(res_c$median, 4),
            if (res_l$pass) "PASS" else "FAIL", signif(res_l$median, 4)))
.log("report -> %s | elapsed %s", file.path(CACHE, "vop_align_live_gate.csv"),
     format(round(difftime(Sys.time(), t0, units = "secs"))))
quit(status = if (res_c$pass && res_l$pass) 0 else 1)
