# End-to-end identity probe for §3.4 Speedup #3: the FULL per-group fit
# (yue_tfpw + Theil–Sen + MK) using the Rcpp kernel must equal the existing
# trend::-based path on slope/ci_low/ci_high/p_value/tfpw_applied/lag1_ac,
# across series that DO and DON'T trigger TFPW pre-whitening. Exit 0 = identical.
suppressMessages({ library(Rcpp); library(trend); library(data.table) })
sourceCpp("R/trend_kernel.cpp")

# ---- OLD path: verbatim yue_tfpw (R/2.1 L956-972) + outer block (L998-1028) ----
yue_tfpw <- function(year, value, threshold = 0.1) {
  n <- length(value)
  if (n < 4L || !all(is.finite(value))) return(list(y = value, applied = FALSE, r = NA_real_))
  ts0 <- tryCatch(trend::sens.slope(value), error = function(e) NULL)
  if (is.null(ts0)) return(list(y = value, applied = FALSE, r = NA_real_))
  slope0     <- unname(ts0$estimates)
  intercept0 <- median(value - slope0 * year, na.rm = TRUE)
  detr       <- value - (slope0 * year + intercept0)
  d          <- detr - mean(detr, na.rm = TRUE)
  denom      <- sum(d * d, na.rm = TRUE)
  r          <- if (denom > 0) sum(d[-n] * d[-1L], na.rm = TRUE) / denom else 0.0
  if (abs(r) <= threshold) return(list(y = value, applied = FALSE, r = r, ts0 = ts0))
  wr <- c(detr[1L], detr[-1L] - r * detr[-n])
  list(y = wr + slope0 * year + intercept0, applied = TRUE, r = r, ts0 = NULL)
}
fit_old <- function(year, value) {
  pw <- yue_tfpw(year, value); yw <- pw$y
  ts <- if (!pw$applied && !is.null(pw$ts0)) pw$ts0 else tryCatch(trend::sens.slope(yw), error = function(e) NULL)
  if (is.null(ts)) list(slope=NA_real_, ci_low=NA_real_, ci_high=NA_real_, p_value=NA_real_, tfpw_applied=FALSE, lag1_ac=pw$r)
  else list(slope=unname(ts$estimates), ci_low=ts$conf.int[1], ci_high=ts$conf.int[2],
            p_value=tryCatch(trend::mk.test(yw)$p.value, error=function(e) NA_real_),
            tfpw_applied=pw$applied, lag1_ac=pw$r)
}

# ---- NEW path: Rcpp kernel for both ts0 reuse and the post-whitening fit ----
fit_new <- function(year, value) {
  n <- length(value)
  if (n < 4L || !all(is.finite(value)))
    return(list(slope=NA_real_, ci_low=NA_real_, ci_high=NA_real_, p_value=NA_real_, tfpw_applied=FALSE, lag1_ac=NA_real_))
  ts0 <- mk_sen_cpp(value)
  slope0 <- ts0$slope
  intercept0 <- median(value - slope0 * year, na.rm = TRUE)
  detr <- value - (slope0 * year + intercept0)
  r <- lag1_ac_cpp(detr)
  if (abs(r) <= 0.1)
    return(list(slope=ts0$slope, ci_low=ts0$ci_low, ci_high=ts0$ci_high, p_value=ts0$p_value, tfpw_applied=FALSE, lag1_ac=r))
  wr <- c(detr[1L], detr[-1L] - r * detr[-n])
  z <- wr + slope0 * year + intercept0
  tsz <- mk_sen_cpp(z)
  list(slope=tsz$slope, ci_low=tsz$ci_low, ci_high=tsz$ci_high, p_value=tsz$p_value, tfpw_applied=TRUE, lag1_ac=r)
}

set.seed(11)
gen <- function(kind, n) switch(kind,
  clean    = 10 + 0.5*(1:n) + rnorm(n,0,2),
  ar1hi    = as.numeric(arima.sim(list(ar=0.85), n)) + 0.3*(1:n),  # triggers TFPW
  ar1mid   = as.numeric(arima.sim(list(ar=0.5), n)) + 0.3*(1:n),
  tied     = round(10 + 0.3*(1:n) + rnorm(n,0,1)),
  flat     = rep(5,n) + rnorm(n,0,0.001),
  noise    = rnorm(n))
kinds <- c("clean","ar1hi","ar1mid","tied","flat","noise"); ns <- c(4,7,10,20,24,34,40)

cols <- c("slope","ci_low","ci_high","p_value","lag1_ac")
maxd <- setNames(rep(0,length(cols)), cols)
ncmp<-0L; nbad<-0L; n_tfpw<-0L
years <- function(n) 2021:(2020+n)
for (kind in kinds) for (n in ns) for (rep in 1:30) {
  v <- gen(kind,n); if(!all(is.finite(v))) next
  yr <- years(n)
  a <- fit_old(yr,v); b <- fit_new(yr,v); ncmp<-ncmp+1L
  if (isTRUE(b$tfpw_applied)) n_tfpw<-n_tfpw+1L
  if (!identical(as.logical(a$tfpw_applied), as.logical(b$tfpw_applied))) {
    nbad<-nbad+1L; cat(sprintf("tfpw-mismatch %s/%d: old=%s new=%s\n",kind,n,a$tfpw_applied,b$tfpw_applied)); next }
  for (cc in cols) {
    av<-a[[cc]]; bv<-b[[cc]]
    if (is.na(av)&&is.na(bv)) next
    if (is.na(av)!=is.na(bv)) { nbad<-nbad+1L; cat(sprintf("NA-mismatch %s/%d %s: old=%s new=%s\n",kind,n,cc,av,bv)); next }
    dd<-abs(av-bv); if(dd>maxd[[cc]]) maxd[[cc]]<-dd
  }
}
cat(sprintf("compared %d series (%d triggered TFPW)\n", ncmp, n_tfpw))
for (cc in cols) cat(sprintf("  %-9s max|diff|=%.3e\n", cc, maxd[[cc]]))
if (nbad>0) stop(sprintf("FAIL: %d mismatches", nbad))
if (max(maxd) < 1e-9) cat(sprintf("PASS: kernel-yue == trend-yue (max %.2e); both branches exercised\n", max(maxd))) else
  stop(sprintf("FAIL: max diff %.3e exceeds 1e-9", max(maxd)))
