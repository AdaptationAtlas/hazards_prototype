# =============================================================================
# mass_check.R - stop a sum-resample that does not conserve mass (issue #58)
#
#   source(file.path(project_dir, "R", "mass_check.R"))
#   dst <- terra::resample(src, grid, method = "sum")
#   mass_check(src, dst, "0.4.1 GLW resample")
#
# Why: GDAL 3.4.3's warper returns up to 7,399 in a cell that must sum at most
# 9 whenever the source carries a NoData mask (PASCAL, 2026-11-13; see
# R/checks/probe_058_sum_resample.R). terra::resample(method = "sum") goes
# through that warper, so outputs came out x2.0 (0.25 deg) and x1.6 (0.05 deg)
# with no error. 0.4.1 already printed a mass warning, but it compared the
# GLOBAL source with a grid that covers only part of the globe, so it fired on
# every host and was never read.
#
# What it compares: the destination total against the source cropped to the
# destination's extent (snap = "near"), layer by layer. On a correct GDAL
# (3.8.5, 3.11) that ratio is 0.999-1.000 at both Atlas grids; on GDAL 3.4.3
# it is 1.6-2.0.
#
# On failure it STOPS, naming the layers, the ratios and the GDAL version.
# ATLAS_MASS_CHECK=warn downgrades the stop to a warning (diagnosis only).
# ATLAS_MASS_TOL sets the tolerance (default 0.005, i.e. 0.5 %).
# =============================================================================

mass_check <- function(src, dst, label,
                       tol = as.numeric(Sys.getenv("ATLAS_MASS_TOL", "0.005")),
                       on_fail = Sys.getenv("ATLAS_MASS_CHECK", "stop")) {
  ref <- terra::crop(src, terra::ext(dst), snap = "near")
  s <- terra::global(ref, "sum", na.rm = TRUE)[, 1]
  d <- terra::global(dst, "sum", na.rm = TRUE)[, 1]
  r <- d / s
  # A layer with no mass in the window (s == 0) cannot be judged; it is
  # reported as NaN/Inf and only fails if the destination gained mass.
  bad <- (is.finite(r) & abs(r - 1) > tol) | (s == 0 & d != 0)
  ratios <- paste(sprintf("%s=%.4f", names(dst), r), collapse = ", ")
  if (!any(bad, na.rm = TRUE)) {
    message(sprintf("[%s] mass conserved within %g: %s", label, tol, ratios))
    return(invisible(r))
  }
  msg <- sprintf(paste0(
    "[%s] mass NOT conserved (tolerance %g, GDAL %s): %s. A sum-resample that ",
    "does not conserve mass writes wrong values. Check this host with ",
    "Rscript R/checks/probe_058_sum_resample.R (issue #58); GDAL 3.4.3 is known bad."),
    label, tol, terra::gdal(), ratios)
  if (identical(on_fail, "warn")) warning(msg, call. = FALSE) else stop(msg, call. = FALSE)
  invisible(r)
}
