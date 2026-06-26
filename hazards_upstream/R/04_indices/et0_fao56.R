# =============================================================================
# FAO-56 Penman-Monteith reference evapotranspiration (ET0)
# Allen, R.G., Pereira, L.S., Raes, D., Smith, M. (1998). Crop evapotranspiration
#   - Guidelines for computing crop water requirements. FAO Irrigation & Drainage
#   Paper 56. https://www.fao.org/4/x0490e/x0490e00.htm   (equation numbers below)
#
# Canonical, citable reference ET0. Replaces the non-standard CIAT `peest2`
# (Priestley-Taylor with VPD-scaled alpha + shortwave-only net radiation) as the
# Atlas PET, for: published PET variable, the NDWS/NDWL water balance, and SPEI.
#
# All functions are vectorised: arguments may be scalars (validation) or terra
# SpatRasters / numeric vectors (pipeline). Inputs from NEX-GDDP-CMIP6:
#   tmax, tmin (degC), rs = rsds (MJ m-2 day-1), u2 from sfcWind (m/s @2m),
#   rh = hurs (% , daily mean) OR rhmax/rhmin, plus latitude (deg), DOY, elev (m).
# Saturated/actual vapour pressure helpers and the radiation chain follow FAO-56
# exactly so results are reproducible against the FAO worked examples.
# =============================================================================

# Elementwise clamps that work for BOTH numeric scalars/vectors AND terra
# SpatRasters (pmin/pmax don't dispatch reliably on SpatRaster). Arithmetic only.
.clamp_hi <- function(x, hi) x - (x - hi) * (x > hi)   # == min(x, hi)
.clamp_lo <- function(x, lo) x + (lo - x) * (x < lo)   # == max(x, lo)

# Saturation vapour pressure e°(T) [kPa] - FAO-56 Eq. 11
.es_T <- function(Tc) 0.6108 * exp(17.27 * Tc / (Tc + 237.3))

# Slope of the sat. vapour pressure curve Delta [kPa/degC] at Tmean - Eq. 13
.delta_svp <- function(Tmean) {
  (4098 * (0.6108 * exp(17.27 * Tmean / (Tmean + 237.3)))) / (Tmean + 237.3)^2
}

# Atmospheric pressure P [kPa] from elevation z [m] - Eq. 7
.pressure <- function(z) 101.3 * ((293 - 0.0065 * z) / 293)^5.26

# Psychrometric constant gamma [kPa/degC] - Eq. 8
.gamma_psy <- function(P) 0.000665 * P

# Extraterrestrial radiation Ra [MJ m-2 day-1] - Eq. 21 (lat in degrees, J = DOY)
.ra_extra <- function(lat_deg, J) {
  phi <- lat_deg * pi / 180
  dr  <- 1 + 0.033 * cos(2 * pi * J / 365)              # Eq. 23 inverse rel. dist.
  dec <- 0.409 * sin(2 * pi * J / 365 - 1.39)           # Eq. 24 solar declination
  ws  <- acos(.clamp_lo(.clamp_hi(-tan(phi) * tan(dec), 1), -1))  # Eq. 25 sunset hour angle
  (24 * 60 / pi) * 0.0820 * dr *
    (ws * sin(phi) * sin(dec) + cos(phi) * cos(dec) * sin(ws))   # Eq. 21
}

# FAO-56 Penman-Monteith daily ET0 [mm/day] - Eq. 6
# ea: pass actual vapour pressure directly, else derived from rh (mean) or rhmax/rhmin.
et0_fao56 <- function(tmax, tmin, rs, u2, lat_deg, J, elev = 0,
                      rh = NULL, rhmax = NULL, rhmin = NULL, ea = NULL,
                      albedo = 0.23, G = 0) {
  Tmean <- (tmax + tmin) / 2
  D     <- .delta_svp(Tmean)                 # Delta
  P     <- .pressure(elev)
  g     <- .gamma_psy(P)                      # gamma
  esmax <- .es_T(tmax); esmin <- .es_T(tmin)
  es    <- (esmax + esmin) / 2                # Eq. 12
  if (is.null(ea)) {
    if (!is.null(rhmax) && !is.null(rhmin)) {
      ea <- (esmin * rhmax / 100 + esmax * rhmin / 100) / 2   # Eq. 17
    } else if (!is.null(rh)) {
      ea <- (rh / 100) * es                                   # Eq. 19 (mean RH)
    } else stop("provide ea, or rh, or rhmax+rhmin")
  }
  Ra  <- .ra_extra(lat_deg, J)
  Rso <- (0.75 + 2e-5 * elev) * Ra                            # Eq. 37
  Rns <- (1 - albedo) * rs                                    # Eq. 38
  # Net longwave Rnl - Eq. 39 (sigma in MJ K-4 m-2 day-1)
  sigma <- 4.903e-9
  TmaxK <- tmax + 273.16; TminK <- tmin + 273.16
  Rnl <- sigma * ((TmaxK^4 + TminK^4) / 2) *
         (0.34 - 0.14 * sqrt(ea)) *
         (1.35 * .clamp_hi(rs / Rso, 1) - 0.35)
  Rn  <- Rns - Rnl
  num <- 0.408 * D * (Rn - G) + g * (900 / (Tmean + 273)) * u2 * (es - ea)
  den <- D + g * (1 + 0.34 * u2)
  num / den
}
