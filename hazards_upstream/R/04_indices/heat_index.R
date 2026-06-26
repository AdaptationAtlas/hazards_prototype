# NWS (Rothfusz) Heat Index + CHC-style WBGT conversion.
# Heat Index algorithm: NOAA/NWS, https://www.wpc.ncep.noaa.gov/html/heatindex_equation.shtml
#   - Steadman (1979) simple formula, averaged with T; if that >= 80 F, the full
#     Rothfusz (1990) regression is used, with the low-RH and high-RH adjustments.
# WBGT (shaded/indoor) from HI: WBGTmax(C) = -0.0034*HI(F)^2 + 0.96*HI(F) - 34
#   - Williams et al. (2024) Sci. Data, Eq.6 (CHC-CMIP6); transform from
#     Bernard & Iheanacho (2015) JOEH 12:323-333. This is exactly how CHC derives
#     WBGTmax (CHIRTS-ERA5, CHC-CMIP6) - daily, no hourly reconstruction.
#
# All vectorised for scalars (validation) and terra SpatRasters (pipeline);
# conditional branches done by arithmetic masks (terra-safe). Inputs: ta in degC,
# rh in % (use Tmax + RH-at-Tmax for a daily-max heat index, matching CHC).

# min(x,hi)/max(x,lo) that work on numeric and SpatRaster (pmin/pmax don't).
.hi_clamp_hi <- function(x, hi) x - (x - hi) * (x > hi)
.hi_clamp_lo <- function(x, lo) x + (lo - x) * (x < lo)

# NWS Heat Index. ta degC, rh %. Returns degC (or degF if fahrenheit=TRUE).
heat_index_nws <- function(ta, rh, fahrenheit = FALSE) {
  T <- ta * 9/5 + 32                                   # -> Fahrenheit
  R <- rh

  # Steadman simple formula
  HIs <- 0.5 * (T + 61.0 + (T - 68.0) * 1.2 + R * 0.094)

  # Full Rothfusz regression
  HIf <- -42.379 + 2.04901523*T + 10.14333127*R - 0.22475541*T*R -
          0.00683783*T*T - 0.05481717*R*R + 0.00122874*T*T*R +
          0.00085282*T*R*R - 0.00000199*T*T*R*R

  # Low-RH adjustment: RH<13 and 80<=T<=112 -> subtract. Clamp sqrt arg >=0 so the
  # term is finite everywhere; the mask zeroes it outside the valid window.
  adj_lo <- ((13 - R)/4) * sqrt(.hi_clamp_lo((17 - abs(T - 95.0))/17, 0))
  m_lo   <- (R < 13) * (T >= 80) * (T <= 112)
  HIf    <- HIf - adj_lo * m_lo

  # High-RH adjustment: RH>85 and 80<=T<=87 -> add.
  adj_hi <- ((R - 85)/10) * ((87 - T)/5)
  m_hi   <- (R > 85) * (T >= 80) * (T <= 87)
  HIf    <- HIf + adj_hi * m_hi

  # Use full regression where avg(simple, T) >= 80, else the simple value.
  use_full <- ((HIs + T)/2 >= 80)
  HI <- HIs * (1 - use_full) + HIf * use_full

  if (fahrenheit) HI else (HI - 32) * 5/9
}

# CHC shaded WBGTmax (degC) from temperature + humidity, via the Heat Index.
# Feed Tmax + RH-at-Tmax to reproduce CHC's daily WBGTmax.
wbgt_chc <- function(ta, rh) {
  hif <- heat_index_nws(ta, rh, fahrenheit = TRUE)     # HI in degF
  -0.0034 * hif^2 + 0.96 * hif - 34                    # Williams 2024 Eq.6
}
