// Single-pass EABYEP daily soil-water-balance kernel for the NDWS / NDWL0 /
// NDWL50 indices. Replaces the R per-day loop over terra rasters (~30 raster
// ops/month) with one C++ pass: per cell, march the days carrying soil-water
// availability, emitting ERATIO (-> NDWS) and LOGGING (-> NDWL) per cell-day.
//
// Math is identical to eabyep_calc() in fast_calc_NDWS/NDWL0/NDWL50.R. NA is
// propagated explicitly (R's min/max return NA on NA; std::min/NaN is UB), so a
// masked/ocean cell (NA soil) or an NA met day yields NA from that point on.
//
// Packaged (not sourceCpp-into-env) so parallel future() workers load it via
// library(wbkernel) - same rationale as trendkernel.

#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::export]]
List wb_kernel_cpp(NumericMatrix rain,   // [ncell x ndays] mm/day
                   NumericMatrix evap,   // [ncell x ndays] PET mm/day (ET0 / ETMAX)
                   NumericVector soilcp, // [ncell] soil water holding capacity
                   NumericVector soilsat,// [ncell] additional saturation capacity
                   NumericVector avail0) // [ncell] initial availability (prior-month last day)
{
  const int ncell = rain.nrow();
  const int ndays = rain.ncol();
  if (evap.nrow() != ncell || evap.ncol() != ndays)
    stop("rain and evap must have identical dimensions");
  if (soilcp.size() != ncell || soilsat.size() != ncell || avail0.size() != ncell)
    stop("soilcp/soilsat/avail0 length must equal nrow(rain)");

  NumericMatrix eratio(ncell, ndays);
  NumericMatrix logging(ncell, ndays);
  NumericVector avail_final(ncell);

  for (int c = 0; c < ncell; ++c) {
    double cp  = soilcp[c];
    double sat = soilsat[c];
    double av  = avail0[c];

    // Whole-cell NA: undefined soil -> NA everywhere for this cell.
    if (NumericMatrix::is_na(cp) || NumericMatrix::is_na(sat)) {
      for (int d = 0; d < ndays; ++d) {
        eratio(c, d)  = NA_REAL;
        logging(c, d) = NA_REAL;
      }
      avail_final[c] = NA_REAL;
      continue;
    }

    const double demand_denom = 97.0 - 3.868 * std::sqrt(cp);  // ERATIO denominator

    for (int d = 0; d < ndays; ++d) {
      double rn = rain(c, d);
      double ev = evap(c, d);

      if (NumericMatrix::is_na(av) || NumericMatrix::is_na(rn) || NumericMatrix::is_na(ev)) {
        eratio(c, d)  = NA_REAL;
        logging(c, d) = NA_REAL;
        av = NA_REAL;                 // NA carries forward (matches R)
        continue;
      }

      double av_in  = std::min(av, cp);                 // avail <- min(avail, soilcp)
      double percwt = std::min(av_in / cp * 100.0, 100.0);
      percwt        = std::max(percwt, 1.0);
      double er     = std::min(percwt / demand_denom, 1.0);
      double demand = er * ev;
      double result = av_in + rn - demand;
      double lg     = std::min(std::max(result - cp, 0.0), sat);
      double av_nx  = std::max(std::min(cp, result), 0.0);

      eratio(c, d)  = er;
      logging(c, d) = lg;
      av = av_nx;
    }
    avail_final[c] = av;
  }

  return List::create(_["eratio"]  = eratio,
                      _["logging"] = logging,
                      _["avail_final"] = avail_final);
}
