# Integration probe for §3.4: full data.table by-block + .EACHI intercept, KERNEL
# path vs TREND path, on synthetic multi-group / multi-baseline data (value/year
# shared across baselines; baseline_value differs). Asserts identical trend_summary
# (slope/intercept/ci/p/tfpw/lag1_ac) for every (group × baseline). Exit 0 = identical.
suppressMessages({ library(Rcpp); library(trend); library(data.table) })

# --- helpers copied verbatim from R/2.1 §3.4 (kernel env + fit_value_kernel + yue_tfpw) ---
kernel_cpp <- "R/trend_kernel.cpp"; kernel_cache <- "R/.rcpp_cache"
.kernel_env <- new.env(parent = baseenv())
.ensure_kernel <- function() {
  if (is.null(.kernel_env$mk_sen_cpp))
    suppressMessages(Rcpp::sourceCpp(kernel_cpp, cacheDir = kernel_cache, env = .kernel_env))
  invisible(NULL)
}
fit_value_kernel <- function(year, value) {
  n <- length(value)
  if (n < 4L || !all(is.finite(value)))
    return(list(slope=NA_real_, ci_low=NA_real_, ci_high=NA_real_, p_value=NA_real_, tfpw_applied=FALSE, lag1_ac=NA_real_))
  ts0 <- .kernel_env$mk_sen_cpp(value); slope0 <- ts0$slope
  intercept0 <- median(value - slope0 * year, na.rm = TRUE)
  detr <- value - (slope0 * year + intercept0)
  r <- .kernel_env$lag1_ac_cpp(detr)
  if (abs(r) <= 0.1) return(list(slope=ts0$slope, ci_low=ts0$ci_low, ci_high=ts0$ci_high, p_value=ts0$p_value, tfpw_applied=FALSE, lag1_ac=r))
  wr <- c(detr[1L], detr[-1L] - r * detr[-n]); z <- wr + slope0 * year + intercept0
  tsz <- .kernel_env$mk_sen_cpp(z)
  list(slope=tsz$slope, ci_low=tsz$ci_low, ci_high=tsz$ci_high, p_value=tsz$p_value, tfpw_applied=TRUE, lag1_ac=r)
}
yue_tfpw <- function(year, value, threshold = 0.1) {
  n <- length(value)
  if (n < 4L || !all(is.finite(value))) return(list(y=value, applied=FALSE, r=NA_real_))
  ts0 <- tryCatch(trend::sens.slope(value), error=function(e) NULL)
  if (is.null(ts0)) return(list(y=value, applied=FALSE, r=NA_real_))
  slope0 <- unname(ts0$estimates); intercept0 <- median(value - slope0*year, na.rm=TRUE)
  detr <- value - (slope0*year + intercept0); d <- detr - mean(detr, na.rm=TRUE)
  denom <- sum(d*d, na.rm=TRUE); r <- if (denom>0) sum(d[-n]*d[-1L], na.rm=TRUE)/denom else 0.0
  if (abs(r) <= threshold) return(list(y=value, applied=FALSE, r=r, ts0=ts0))
  wr <- c(detr[1L], detr[-1L] - r*detr[-n]); list(y=wr + slope0*year + intercept0, applied=TRUE, r=r, ts0=NULL)
}
.ensure_kernel()

fit_keys <- c("admin0_name","admin1_name","scenario","timeframe","model","hazard","season")

# --- synthetic source: value/year shared across baselines; baseline_value differs ---
set.seed(99)
g <- CJ(admin0_name="Angola", admin1_name=c("R1","R2"), scenario=c("ssp245","ssp585"),
        timeframe="2030s", model=c("m1","m2","m3"), hazard=c("PTOT","TMAX"),
        season=c("DJF","annual"), year=2021:2044)
g[, gid := .GRP, by=fit_keys]
# mix: ~half groups AR1 (trigger TFPW), half clean
g[, value := { if (gid[1] %% 2 == 0) as.numeric(arima.sim(list(ar=0.8), .N)) + 0.3*(year-2021)
               else 20 + 0.4*(year-2021) + rnorm(.N,0,2) }, by=fit_keys]
mk_bl <- function(off, nm){ d<-copy(g); d[,baseline_name:=nm]; d[,baseline_value:=30+off+0.05*gid]; d[] }
bls <- list(mk_bl(0,"b1"), mk_bl(7,"b2"))

# ---------- TREND path (current code) ----------
trend_one <- function(d) {
  vf <- d[, { pw<-yue_tfpw(year,value); yw<-pw$y
    ts<-if(!pw$applied && !is.null(pw$ts0)) pw$ts0 else tryCatch(sens.slope(yw),error=function(e)NULL)
    if(is.null(ts)) list(slope=NA_real_,intercept=NA_real_,ci_low=NA_real_,ci_high=NA_real_,p_value=NA_real_,tfpw_applied=FALSE,lag1_ac=pw$r)
    else list(slope=unname(ts$estimates),intercept=median(baseline_value-unname(ts$estimates)*year),
              ci_low=ts$conf.int[1],ci_high=ts$conf.int[2],
              p_value=tryCatch(mk.test(yw)$p.value,error=function(e)NA_real_),tfpw_applied=pw$applied,lag1_ac=pw$r)
  }, by=c(fit_keys,"baseline_name")]
  vf
}
trend_res <- rbindlist(lapply(bls, trend_one))

# ---------- KERNEL path (#1 dedup + #3 kernel + .EACHI intercept) ----------
value_fit <- NULL; kl <- list()
for (k in seq_along(bls)) {
  d <- bls[[k]]
  if (is.null(value_fit)) value_fit <- d[, fit_value_kernel(year,value), by=fit_keys]
  ts <- d[value_fit, on=fit_keys,
    .(slope=i.slope, intercept=median(baseline_value - i.slope*year), ci_low=i.ci_low, ci_high=i.ci_high,
      p_value=i.p_value, tfpw_applied=i.tfpw_applied, lag1_ac=i.lag1_ac), by=.EACHI]
  ts[, baseline_name := bls[[k]]$baseline_name[1]]
  kl[[k]] <- ts
}
kernel_res <- rbindlist(kl)

# ---------- compare ----------
key <- c(fit_keys,"baseline_name")
setkeyv(trend_res, key); setkeyv(kernel_res, key)
stopifnot(nrow(trend_res) == nrow(kernel_res))
if (!identical(trend_res$tfpw_applied, kernel_res$tfpw_applied)) stop("FAIL: tfpw_applied differs")
num <- c("slope","intercept","ci_low","ci_high","p_value","lag1_ac"); mx <- 0
for (cc in num) {
  na_mm <- sum(is.na(trend_res[[cc]]) != is.na(kernel_res[[cc]]))
  if (na_mm) stop(sprintf("FAIL: NA-mismatch in %s (%d)", cc, na_mm))
  dd <- max(abs(trend_res[[cc]] - kernel_res[[cc]]), na.rm=TRUE)
  cat(sprintf("  %-10s max|diff|=%.3e\n", cc, dd)); mx <- max(mx, dd)
}
cat(sprintf("groups=%d baselines=2 rows=%d  TFPW-applied groups=%d\n",
            uniqueN(g$gid), nrow(kernel_res), sum(value_fit$tfpw_applied)))
if (mx < 1e-9) cat(sprintf("PASS: kernel+#1 == trend full §3.4 (max %.2e)\n", mx)) else
  stop(sprintf("FAIL: max diff %.3e exceeds 1e-9", mx))
