# Reproduce §3.4 value_fit (kernel) IN-PROCESS vs INSIDE A FUTURE WORKER on real data.
# The earlier probe proved the package + closure work in the main process; the run failed
# only under multisession (6 future workers) -> 100% NA slope. This isolates that:
# mirrors the real call mechanism — make_fit_value_kernel as an auto-detected global,
# future.packages=c(...,"trendkernel"). Run on CGLabs: Rscript R/probe_sec3_4_worker_valuefit.R
suppressMessages({library(data.table); library(arrow); library(future); library(future.apply)})

D  <- "/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/hazard_timeseries_mean_month"
sf <- file.path(D, "haz_3months_adm_mean_1995-2014_anomaly-historic_seasons.parquet")  # small (125M)
fit_keys <- c("admin0_name","admin1_name","scenario","timeframe","model","hazard","season")

# verbatim copy of make_fit_value_kernel from R/2.1 (lines 990-1008)
make_fit_value_kernel <- function() function(year, value) {
  n <- length(value)
  if (n < 4L || !all(is.finite(value)))
    return(list(slope=NA_real_, ci_low=NA_real_, ci_high=NA_real_,
                p_value=NA_real_, tfpw_applied=FALSE, lag1_ac=NA_real_))
  ts0        <- trendkernel::mk_sen_cpp(value)
  slope0     <- ts0$slope
  intercept0 <- median(value - slope0 * year, na.rm = TRUE)
  detr       <- value - (slope0 * year + intercept0)
  r          <- trendkernel::lag1_ac_cpp(detr)
  if (abs(r) <= 0.1)
    return(list(slope=ts0$slope, ci_low=ts0$ci_low, ci_high=ts0$ci_high,
                p_value=ts0$p_value, tfpw_applied=FALSE, lag1_ac=r))
  wr  <- c(detr[1L], detr[-1L] - r * detr[-n])
  z   <- wr + slope0 * year + intercept0
  tsz <- trendkernel::mk_sen_cpp(z)
  list(slope=tsz$slope, ci_low=tsz$ci_low, ci_high=tsz$ci_high,
       p_value=tsz$p_value, tfpw_applied=TRUE, lag1_ac=r)
}

# the worker function — same shape as .sec34_FUN's inner value_fit computation
FUN <- function(ignored) {
  dt <- as.data.table(read_parquet(sf))
  dt <- dt[is.finite(value) & is.finite(year)]
  fk <- make_fit_value_kernel()
  vf <- dt[, fk(year, value), by = fit_keys]
  # report whether the kernel actually resolved in this process
  list(slopeNA = mean(is.na(vf$slope)),
       groups  = nrow(vf),
       can_call_kernel = tryCatch({trendkernel::mk_sen_cpp(as.numeric(1:8))$ok}, error=function(e) paste("ERR:",conditionMessage(e))),
       loaded  = "trendkernel" %in% loadedNamespaces())
}

cat("==== IN-PROCESS (main R session) ====\n")
ip <- FUN(1)
cat(sprintf("slopeNA=%.1f%%  groups=%d  can_call_kernel=%s  ns_loaded=%s\n",
            100*ip$slopeNA, ip$groups, ip$can_call_kernel, ip$loaded))

cat("\n==== FUTURE MULTISESSION WORKER (replicates the run) ====\n")
plan(multisession, workers = 2)
wk <- future_lapply(1:2, FUN, future.packages = c("data.table","arrow","trend","trendkernel"),
                    future.seed = TRUE)[[1]]
plan(sequential)
cat(sprintf("slopeNA=%.1f%%  groups=%d  can_call_kernel=%s  ns_loaded=%s\n",
            100*wk$slopeNA, wk$groups, wk$can_call_kernel, wk$loaded))

cat("\n==== VERDICT ====\n")
if (ip$slopeNA < 0.5 && wk$slopeNA > 0.99)
  cat("CONFIRMED multisession-only: kernel works in-process, NA in worker. ",
      "worker can_call_kernel=", wk$can_call_kernel, " ns_loaded=", wk$loaded, "\n")
if (wk$slopeNA < 0.5) cat("Worker is FINE here — bug not reproduced; run-vs-probe env differs.\n")
