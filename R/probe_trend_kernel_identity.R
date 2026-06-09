# Identity probe for §3.4 Speedup #3 (Rcpp Theil–Sen + MK kernel).
# Compares mk_sen_cpp() against trend::sens.slope() + trend::mk.test() across many
# random series (clean, tied, short n, autocorrelated, flat, monotone). Exit 0 = all
# match to < 1e-9 on slope/ci_low/ci_high/p_value over every series.
suppressMessages({ library(Rcpp); library(trend) })

.t0 <- Sys.time()
cat(sprintf("[%s] compiling R/trend_kernel.cpp (cold compile ~15-40s, cached ~2s) ...\n",
            format(Sys.time(), "%H:%M:%S")))
sourceCpp("R/trend_kernel.cpp")
cat(sprintf("[%s] compiled in %.1fs — running comparisons ...\n",
            format(Sys.time(), "%H:%M:%S"), as.numeric(difftime(Sys.time(), .t0, units = "secs"))))

ref <- function(x, cl = 0.95) {
  ss <- trend::sens.slope(x, conf.level = cl)
  mk <- trend::mk.test(x)
  list(slope = unname(ss$estimates), ci_low = ss$conf.int[1], ci_high = ss$conf.int[2],
       p_value = mk$p.value, S = unname(mk$estimates["S"]), varS = unname(mk$estimates["varS"]))
}

set.seed(7)
gen <- function(kind, n) {
  switch(kind,
    clean    = 10 + 0.5 * seq_len(n) + rnorm(n, 0, 2),
    flat     = rep(5, n) + rnorm(n, 0, 0.001),
    monotone = cumsum(abs(rnorm(n, 1, 0.3))),
    tied     = round(10 + 0.3 * seq_len(n) + rnorm(n, 0, 1)),   # integer ties
    heavytie = sample(c(1, 1, 1, 2, 2, 3), n, replace = TRUE),   # many ties
    ar1      = as.numeric(arima.sim(list(ar = 0.7), n)) + 0.4 * seq_len(n),
    noise    = rnorm(n)
  )
}
kinds <- c("clean", "flat", "monotone", "tied", "heavytie", "ar1", "noise")
ns    <- c(4, 5, 7, 10, 20, 24, 34, 40)

maxdiff <- setNames(rep(0, 4), c("slope", "ci_low", "ci_high", "p_value"))
worst   <- NULL
ncmp <- 0L; nbad <- 0L
for (kind in kinds) for (n in ns) for (rep in 1:25) {
  x <- gen(kind, n)
  if (!all(is.finite(x))) next
  r <- tryCatch(ref(x), error = function(e) NULL)
  k <- mk_sen_cpp(x)
  if (is.null(r)) next
  ncmp <- ncmp + 1L
  for (cc in c("slope", "ci_low", "ci_high", "p_value")) {
    rv <- r[[cc]]; kv <- k[[cc]]
    if (is.na(rv) && is.na(kv)) next
    if (is.na(rv) != is.na(kv)) {
      nbad <- nbad + 1L
      cat(sprintf("NA-mismatch %s/%d %s: ref=%s ker=%s\n", kind, n, cc, rv, kv)); next
    }
    dd <- abs(rv - kv)
    if (dd > maxdiff[[cc]]) { maxdiff[[cc]] <- dd; if (cc == "p_value") worst <- list(kind, n, x) }
  }
}

cat(sprintf("compared %d series\n", ncmp))
for (cc in names(maxdiff)) cat(sprintf("  %-9s max|diff|=%.3e\n", cc, maxdiff[[cc]]))

if (nbad > 0) stop(sprintf("FAIL: %d NA-pattern mismatches", nbad))
if (max(maxdiff) < 1e-9)
  cat(sprintf("[%s] PASS: kernel == trend:: across all series (max %.2e) — total %.1fs\n",
              format(Sys.time(), "%H:%M:%S"), max(maxdiff), as.numeric(difftime(Sys.time(), .t0, units = "secs")))) else
  stop(sprintf("FAIL: max diff %.3e exceeds 1e-9", max(maxdiff)))
