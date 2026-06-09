# Multisession-worker probe: confirms the §3.4 kernel loads INSIDE future_lapply
# workers (fresh processes) from the shared Rcpp cache and computes correctly.
# Mirrors the export pattern: empty global .kernel_env + values exported, each worker
# .ensure_kernel() loads from cache. Exit 0 = workers produced kernel results matching
# the main-process trend:: reference.
suppressMessages({ library(Rcpp); library(trend); library(future.apply) })
.t0 <- Sys.time()
cat(sprintf("[%s] start (compiles kernel + spawns 4 workers; first run ~15-40s) ...\n", format(Sys.time(), "%H:%M:%S")))

kernel_cpp <- normalizePath("R/trend_kernel.cpp")
kernel_cache <- file.path(normalizePath("R"), ".rcpp_cache")
dir.create(kernel_cache, showWarnings = FALSE, recursive = TRUE)
.kernel_env <- new.env(parent = baseenv())   # stays EMPTY in main (export-safe)
.ensure_kernel <- function() {
  if (is.null(.kernel_env$mk_sen_cpp))
    suppressMessages(Rcpp::sourceCpp(kernel_cpp, cacheDir = kernel_cache, env = .kernel_env))
  invisible(NULL)
}
fit_value_kernel <- function(year, value) {
  n <- length(value)
  if (n < 4L || !all(is.finite(value))) return(list(slope=NA_real_, p_value=NA_real_))
  ts0 <- .kernel_env$mk_sen_cpp(value); slope0 <- ts0$slope
  intercept0 <- median(value - slope0*year, na.rm=TRUE)
  detr <- value - (slope0*year + intercept0); r <- .kernel_env$lag1_ac_cpp(detr)
  if (abs(r) <= 0.1) return(list(slope=ts0$slope, p_value=ts0$p_value))
  wr <- c(detr[1L], detr[-1L]-r*detr[-n]); z <- wr + slope0*year + intercept0
  tsz <- .kernel_env$mk_sen_cpp(z); list(slope=tsz$slope, p_value=tsz$p_value)
}

# precompile into throwaway env (populates cache, leaves .kernel_env empty) — as in R/2.1
.probe_env <- new.env(parent = baseenv())
suppressMessages(Rcpp::sourceCpp(kernel_cpp, cacheDir = kernel_cache, env = .probe_env))
stopifnot(is.function(.probe_env$mk_sen_cpp))

set.seed(5)
series <- lapply(1:12, function(i) {
  n <- sample(15:34, 1)
  v <- if (i %% 2 == 0) as.numeric(arima.sim(list(ar=0.75), n)) + 0.3*(1:n) else 10 + 0.5*(1:n) + rnorm(n,0,2)
  list(year = 2021:(2020+n), value = v)
})

plan(multisession, workers = 4)
res <- future_lapply(series, function(s) {
  .ensure_kernel()                       # worker loads kernel from cache
  k <- fit_value_kernel(s$year, s$value)
  list(slope = k$slope, p = k$p_value, pid = Sys.getpid())
}, future.globals = TRUE, future.seed = TRUE)
plan(sequential)

# main-process trend:: reference for the SAME fit (using the validated kernel-yue logic
# is circular, so recompute via trend:: directly through the same TFPW path)
ref_fit <- function(year, value) {
  n <- length(value)
  ts0 <- trend::sens.slope(value); slope0 <- unname(ts0$estimates)
  intercept0 <- median(value - slope0*year, na.rm=TRUE); detr <- value - (slope0*year+intercept0)
  d <- detr - mean(detr); r <- sum(d[-n]*d[-1])/sum(d*d)
  if (abs(r) <= 0.1) { yw <- value } else { wr <- c(detr[1], detr[-1]-r*detr[-n]); yw <- wr + slope0*year + intercept0 }
  ts <- trend::sens.slope(yw); list(slope=unname(ts$estimates), p=trend::mk.test(yw)$p.value)
}

mx <- 0; npid <- length(unique(sapply(res, `[[`, "pid")))
for (i in seq_along(series)) {
  rf <- ref_fit(series[[i]]$year, series[[i]]$value)
  mx <- max(mx, abs(res[[i]]$slope - rf$slope), abs(res[[i]]$p - rf$p), na.rm=TRUE)
}
cat(sprintf("ran %d series across %d worker PIDs; max|diff| vs trend:: = %.3e\n", length(series), npid, mx))
if (mx < 1e-9)
  cat(sprintf("[%s] PASS: multisession workers load kernel from cache & match trend:: — total %.1fs\n",
              format(Sys.time(), "%H:%M:%S"), as.numeric(difftime(Sys.time(), .t0, units = "secs")))) else
  stop(sprintf("FAIL: max diff %.3e", mx))
