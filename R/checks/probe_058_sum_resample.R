#!/usr/bin/env Rscript
# R/checks/probe_058_sum_resample.R
# =================================
# Issue #58 phase B. On 2026-11-11 PASCAL's R/0.4.1 printed GLW resample mass ratios almost exactly
# twice the macbook's (cattle 1.9048 vs 0.9520, all five species x2.001). The ratio is
# dst / src = resample(count, grid, "sum") / global(count). This probe separates the two halves, so we
# know whether the source mass (cellSize, i.e. PROJ) or the resample (terra/GDAL "sum") is off.
#
# Read-only, seconds, no setup sourced. Usage:
#   Rscript R/checks/probe_058_sum_resample.R
#
# Reference (macbook, terra 1.9.34 / GDAL 3.8.5 / PROJ 9.5.1, 2026-11-12):
#   density sum        20384475
#   cellSize sum       510065622        (Earth's surface, km2: 5.10e8)
#   count sum (src)    1525236453
#   resampled (dst)    1452641015       ratio 0.9524 (the 50S-50N band of base_rast_nexgddp)
#   ones: dst sum      1354539, max cell 9   (1/12 deg -> 0.25 deg is a clean 3x3 aggregation)
suppressPackageStartupMessages(library(terra))
root <- if (nzchar(Sys.getenv("ATLAS_PROJECT_DIR"))) Sys.getenv("ATLAS_PROJECT_DIR") else {
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  if (length(fa)) dirname(dirname(dirname(normalizePath(sub("^--file=", "", fa[1]))))) else getwd()
}
source(file.path(root, "R", "00_paths.R"))
say <- function(...) cat(sprintf("[%s] %s\n", format(Sys.time(), "%H:%M:%S"), paste0(...)))
g1 <- function(r, f = "sum") global(r, f, na.rm = TRUE)[, 1]

say("terra ", as.character(packageVersion("terra")), "  GDAL ", gdal(), "  PROJ ", gdal(lib = "proj"))
g <- rast(file.path(atlas_dir("GLW4_2020", absolute = TRUE), "Ct_2020_Da.tif"))
b <- rast(file.path(root, "metadata", "base_rast_nexgddp.tif"))
say("GLW grid ", paste(dim(g), collapse = "x"), " res ", paste(signif(res(g), 8), collapse = ","),
    "  base grid ", paste(dim(b), collapse = "x"), " res ", paste(res(b), collapse = ","))

cs <- cellSize(g, unit = "km")
cnt <- g * cs
src <- g1(cnt)
say(sprintf("density sum      %.0f", g1(g)))
say(sprintf("cellSize sum     %.0f   (expect ~5.10e8 km2)", g1(cs)))
say(sprintf("count sum (src)  %.0f", src))

dst <- g1(resample(cnt, b, method = "sum"))
say(sprintf("resampled (dst)  %.0f   ratio dst/src %.4f", dst, dst / src))

one <- g * 0 + 1
r1 <- resample(one, b, method = "sum")
say(sprintf("ones: src %.0f  dst %.0f  max cell %.0f  (expect max 9)", g1(one), g1(r1), g1(r1, "max")))

# Same aggregation without GDAL's warper: terra's own aggregate() over exact 3x3 blocks, then crop.
agg <- aggregate(cnt, fact = 3, fun = "sum", na.rm = TRUE)
say(sprintf("aggregate(fact=3) sum %.0f   cropped to base band %.0f", g1(agg), g1(crop(agg, b))))
say("done")
