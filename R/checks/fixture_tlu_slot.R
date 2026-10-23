#!/usr/bin/env Rscript
# R/checks/fixture_tlu_slot.R
# ===========================
# Synthetic replay of issue #52 through the real text of R/0.4.4_process_exposure.R section 2.9, run
# BEFORE the node touches 0.4.4 (no data needed, seconds). Fails loudly on any assertion.
#   #52      the two TLU aggregates (crop total-highland / total-tropical) leave exposure = "number"
#            for exposure = "tlu", unit = "tlu"; values untouched; the ten species rows untouched
#   stop     a GLW extraction with no TLU rows aborts section 2.9 (the relabel is a declaration that
#            must find its rows)
#   units    the section 3.1 hard-stop unit list carries "tlu", so a combined table without it aborts
# Usage: Rscript R/checks/fixture_tlu_slot.R
suppressPackageStartupMessages(library(data.table))
root <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else { fa <- grep("^--file=", commandArgs(FALSE), value = TRUE); if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd() }
ok <- function(cond, msg) { if (!isTRUE(cond)) stop("FIXTURE FAIL: ", msg) else cat("  ok  ", msg, "\n") }

src <- readLines(file.path(root, "R", "0.4.4_process_exposure.R"))
a <- grep("^# 2\\.9\\) TLU slot", src); b <- grep("^# 3\\) Combine exposure totals", src)
ok(length(a) == 1 && length(b) == 1 && a < b, "0.4.4 section 2.9 located")
s29 <- src[a:(b - 1)]

## upstream-shaped: what section 2's GLW extraction returns (layer names as 0.4.1 writes them,
## underscores still in place - section 3.1 hyphenates later), species + TLU, number + two vop units
sp <- c("cattle", "sheep", "goats", "pigs", "poultry")
ge <- rbindlist(list(
  CJ(iso3 = c("KEN", "LBY"), admin2_name = c("a", "b"), crop = c(paste0(sp, "_highland"), paste0(sp, "_tropical")))[, `:=`(exposure = "number", unit = "number", tech = NA_character_, stat = "sum", value = 1000)],
  CJ(iso3 = c("KEN", "LBY"), admin2_name = c("a", "b"), crop = c("total_highland", "total_tropical"))[, `:=`(exposure = "number", unit = "number", tech = NA_character_, stat = "sum", value = 123.4)],
  CJ(iso3 = c("KEN", "LBY"), admin2_name = c("a", "b"), crop = c("cattle_tropical", "total_tropical"))[, `:=`(exposure = "vop", unit = "intld15-2021", tech = NA_character_, stat = "sum", value = 5e6)]
))
before <- copy(ge)
env <- new.env(parent = globalenv())
env$glw_extracted <- copy(ge)
env$.log044 <- function(msg) cat("[0.4.4]", msg, "\n")
for (e in parse(text = s29, keep.source = FALSE)) eval(e, envir = env)
g <- env$glw_extracted

ok(nrow(g) == nrow(before), "no row added or lost")
tl <- g[grepl("^total_", crop) & unit == "tlu"]
ok(nrow(tl) == 8 && all(tl$exposure == "tlu"), "the 8 TLU rows (2 iso3 x 2 admin2 x 2 systems) now carry exposure = tlu, unit = tlu")
ok(all(tl$value == 123.4), "TLU values untouched by the relabel")
ok(nrow(g[exposure == "number"]) == 40 && all(g[exposure == "number", unit] == "number") && !any(grepl("^total_", g[exposure == "number", crop])),
   "the 40 species head-count rows stay exposure = number, and no total-* row remains in that slot")
ok(nrow(g[exposure == "vop"]) == 8 && all(g[exposure == "vop", unit] == "intld15-2021"),
   "vop rows (including a total_tropical vop row) untouched - the relabel is scoped to exposure == number")
ok(identical(g[exposure != "tlu"][order(iso3, admin2_name, crop, exposure)][, .(iso3, admin2_name, crop, exposure, unit, value)],
             before[!(exposure == "number" & grepl("^total_", crop))][order(iso3, admin2_name, crop, exposure)][, .(iso3, admin2_name, crop, exposure, unit, value)]),
   "every non-TLU row is identical before and after")

## the stop: an extraction with no TLU rows
env2 <- new.env(parent = globalenv())
env2$glw_extracted <- before[!grepl("^total_", crop)]
env2$.log044 <- function(msg) invisible(NULL)
r <- tryCatch({ for (e in parse(text = s29, keep.source = FALSE)) eval(e, envir = env2); "ran" }, error = function(e) conditionMessage(e))
ok(grepl("no total-highland / total-tropical rows", r), "section 2.9 STOPS when the extraction holds no TLU rows (the declaration must find its rows)")

## section 3.1 unit list
u <- grep('Sys.getenv\\("EXPOSURE_UNITS",', src)
ok(length(u) == 1 && grepl('"tlu"', src[u + 1]) || grepl('tlu"', src[u + 1]), "section 3.1's EXPOSURE_UNITS default names tlu, so a combined table without the relabelled rows aborts")

cat("\nALL TLU-SLOT FIXTURE ASSERTIONS PASSED\n")
