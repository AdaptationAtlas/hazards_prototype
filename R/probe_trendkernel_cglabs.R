# Decisive isolation for the §3.4 100%-NA-slope bug (Bug C) on CGLabs.
# Splits "is the installed trendkernel package broken?" from "is the data path broken?".
# Run on CGLabs: Rscript R/probe_trendkernel_cglabs.R
suppressMessages({library(data.table); library(arrow)})

cat("==== A. is trendkernel installed & where ====\n")
ip <- tryCatch(find.package("trendkernel"), error = function(e) NA)
cat("path:", ip, "\n")
cat("version:", tryCatch(as.character(packageVersion("trendkernel")), error=function(e) "NA"), "\n")
if (!is.na(ip)) {
  so <- list.files(file.path(ip, "libs"), full.names = TRUE, recursive = TRUE)
  cat("shared objs:\n"); print(file.info(so)[, c("size","mtime")])
}

cat("\n==== B. kernel on a KNOWN vector (expect slope=1, p<0.001) ====\n")
x  <- as.numeric(1:12)            # perfect linear trend, slope 1
r1 <- trendkernel::mk_sen_cpp(x)
cat(sprintf("mk_sen_cpp(1:12): slope=%s p_value=%s ok=%s\n", r1$slope, r1$p_value, r1$ok))
r2 <- trendkernel::mk_sen_cpp(c(3,1,4,1,5,9,2,6,5,3,5,8))
cat(sprintf("mk_sen_cpp(noisy): slope=%s p_value=%s\n", r2$slope, r2$p_value))
cat(sprintf("lag1_ac_cpp(1:12)=%s\n", trendkernel::lag1_ac_cpp(x)))
pkg_ok <- isTRUE(is.finite(r1$slope)) && abs(r1$slope - 1) < 1e-9
cat(if (pkg_ok) ">>> PACKAGE OK\n" else ">>> PACKAGE BROKEN (returns NA / wrong on known input)\n")

cat("\n==== C. real data path: one fit-group through the actual closure ====\n")
D  <- "/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/hazard_timeseries_mean_month"
sf <- file.path(D, "haz_3months_adm_mean_1981-2014_anomaly-historic_seasons.parquet")
dt <- as.data.table(read_parquet(sf))
dt <- dt[is.finite(value) & is.finite(year)]
fit_keys <- c("admin0_name","admin1_name","scenario","timeframe","model","hazard","season")
g  <- dt[dt[, .I[1], by = fit_keys]$V1[1:1]]            # pick the first group's keys
key1 <- g[1, ..fit_keys]
grp <- merge(dt, key1, by = fit_keys)
setorder(grp, year)
cat(sprintf("group n=%d  year range %s..%s  value head: %s\n", nrow(grp),
            min(grp$year), max(grp$year), paste(round(head(grp$value),3), collapse=",")))

mk <- function() function(year, value) {       # verbatim copy of make_fit_value_kernel body
  n <- length(value)
  if (n < 4L || !all(is.finite(value)))
    return(list(slope=NA_real_, p_value=NA_real_, note="guard-tripped"))
  ts0 <- trendkernel::mk_sen_cpp(value)
  list(slope = ts0$slope, p_value = ts0$p_value, note="ok")
}
fk <- mk()
res <- fk(grp$year, grp$value)
cat(sprintf("closure result: slope=%s p_value=%s note=%s\n", res$slope, res$p_value, res$note))

tr <- tryCatch(trend::sens.slope(grp$value)$estimates, error=function(e) paste("ERR", conditionMessage(e)))
cat(sprintf("trend::sens.slope on same group: %s\n", unname(tr)))

cat("\n==== VERDICT ====\n")
if (!pkg_ok)            cat("PACKAGE is broken on this host -> rebuild: R CMD INSTALL trendkernel\n")
else if (is.na(res$slope)) cat("PACKAGE ok but DATA PATH yields NA -> bug in closure/data, note=", res$note, "\n")
else                    cat("Both OK here -> bug is elsewhere (merge/.EACHI/value_fit caching)\n")
