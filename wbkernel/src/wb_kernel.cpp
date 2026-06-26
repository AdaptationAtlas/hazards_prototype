// Single-pass FAO-56 / AquaCrop root-zone soil-water-balance kernel for the
// NDWS (drought) and NDWL0/NDWL50 (waterlogging) hazard indices.
//
// Peer-reviewed basis:
//   * Drought: FAO-56 root-zone water balance + water-stress coefficient Ks,
//     Allen, Pereira, Raes & Smith (1998) FAO Irrigation & Drainage Paper 56,
//     Ch.8 (TAW eq.82, RAW eq.83, Ks eq.84, depletion balance eq.85). Revised
//     for gridded/climate use: Pereira et al. (2026) Water 18:793.
//   * Waterlogging: AquaCrop aeration-stress coefficient Ks_aer, Raes et al.
//     (2009) Agron. J. 101:438-447; Steduto et al. (2009) ibid. 426-437; FAO
//     AquaCrop Reference Manual Ch.3 - anaerobiosis point a fixed depth of water
//     below saturation, Ks_aer linear 1 (at anaerobiosis pt) -> 0 (at saturation),
//     applied after a lag of `aer_lag` saturated days.
//
// State per cell = S, soil water RELATIVE TO FIELD CAPACITY (mm): S<0 = depletion
// (Dr = -S), 0<S<=ssat = water in the saturation-excess band (toward saturation).
// Daily (Kc = 1, crop-agnostic reference; no irrigation/capillary terms):
//   Dr  = max(-S, 0)
//   Ks  = 1 if Dr<=RAW else (TAW-Dr)/(TAW-RAW)          [linear; clamp 0..1]
//   ETc = Ks * et0
//   S   = clamp(S + rain - ETc, -TAW, ssat)             [cap@ssat=RO/DP; @-TAW=WP]
// Inputs taw=sscp (mm, =TAW), ssat (mm, sat-FC band), a_mm (mm, anaerobiosis
// offset = 0.05*Zr*1000), s0 (mm, initial S). NA-propagating to match R.

#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::export]]
List wb_kernel_cpp(NumericMatrix rain,    // [ncell x ndays] mm/day
                   NumericMatrix et0,     // [ncell x ndays] FAO-56 PM ET0 mm/day
                   NumericVector taw,     // [ncell] total available water (=sscp) mm
                   NumericVector ssat,    // [ncell] saturation-excess storage mm
                   NumericVector a_mm,    // [ncell] anaerobiosis offset mm (0.05*Zr*1000)
                   NumericVector s0,      // [ncell] initial S (water rel. to FC) mm
                   double p = 0.5,        // depletion fraction (RAW = p*TAW)
                   int aer_lag = 4)       // days above anaerobiosis pt before full aeration stress
{
  const int ncell = rain.nrow();
  const int ndays = rain.ncol();
  if (et0.nrow() != ncell || et0.ncol() != ndays)
    stop("rain and et0 must have identical dimensions");
  if (taw.size()!=ncell || ssat.size()!=ncell || a_mm.size()!=ncell || s0.size()!=ncell)
    stop("taw/ssat/a_mm/s0 length must equal nrow(rain)");

  NumericMatrix ks(ncell, ndays);       // drought stress coeff (->NDWS)
  NumericMatrix ksaer(ncell, ndays);    // aeration stress coeff, lag-applied (->NDWL50)
  NumericMatrix wl(ncell, ndays);       // waterlogged occurrence 0/1 (->NDWL0)
  NumericVector s_final(ncell);

  for (int c = 0; c < ncell; ++c) {
    double TAW = taw[c], SS = ssat[c], Am = a_mm[c], S = s0[c];

    if (NumericMatrix::is_na(TAW) || NumericMatrix::is_na(SS) ||
        NumericMatrix::is_na(Am) || TAW <= 0) {
      for (int d = 0; d < ndays; ++d) { ks(c,d)=NA_REAL; ksaer(c,d)=NA_REAL; wl(c,d)=NA_REAL; }
      s_final[c] = NA_REAL;
      continue;
    }
    const double RAW = p * TAW;
    const double denom = TAW - RAW;                 // = (1-p)*TAW > 0
    double theta_air = SS - Am;                     // anaerobiosis pt (mm above FC)
    if (theta_air < 0) theta_air = 0;               // shallow sat band
    const double aer_denom = SS - theta_air;        // = min(Am, SS)
    int aer_run = 0;

    for (int d = 0; d < ndays; ++d) {
      double P = rain(c,d), E0 = et0(c,d);
      if (NumericMatrix::is_na(S) || NumericMatrix::is_na(P) || NumericMatrix::is_na(E0)) {
        ks(c,d)=NA_REAL; ksaer(c,d)=NA_REAL; wl(c,d)=NA_REAL; S=NA_REAL; continue;
      }
      double Dr = (S < 0) ? -S : 0.0;
      double Ks = (Dr <= RAW) ? 1.0 : (TAW - Dr) / denom;
      if (Ks < 0) Ks = 0; else if (Ks > 1) Ks = 1;
      double ETc = Ks * E0;
      S = S + P - ETc;
      if (S > SS) S = SS;                            // runoff / deep percolation
      if (S < -TAW) S = -TAW;                        // can't dry below wilting point

      double excess = (S > 0) ? S : 0.0;
      bool waterlogged = excess > theta_air;
      aer_run = waterlogged ? (aer_run + 1) : 0;
      double Ka;                                      // aeration stress (1=none,0=full)
      if (!waterlogged || aer_denom <= 0) Ka = 1.0;
      else Ka = (SS - S) / aer_denom;                 // 1 at theta_air -> 0 at SS
      if (Ka < 0) Ka = 0; else if (Ka > 1) Ka = 1;
      if (aer_run < aer_lag) Ka = 1.0;                // lag: full stress only after lag days

      ks(c,d) = Ks;
      ksaer(c,d) = Ka;
      wl(c,d) = waterlogged ? 1.0 : 0.0;
    }
    s_final[c] = S;
  }

  return List::create(_["ks"]=ks, _["ksaer"]=ksaer, _["wl"]=wl, _["s_final"]=s_final);
}
