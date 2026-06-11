# Validates the §3.4 WORKER-LOCAL kernel wiring under future multisession.
# Reproduces the fix for the all-NA-slope bug: a shared/exported kernel env does not
# survive future's globals layer (silently → NA fits). Worker-local loading
# (load_kernel_env + make_fit_value_kernel, each worker its own env) does. Asserts the
# multisession result is identical to a plain-lapply reference. Exit 0 = wiring sound.
suppressMessages({ library(data.table); library(arrow); library(Rcpp); library(future.apply) })
ts <- function() format(Sys.time(), "%H:%M:%S")
cppf <- normalizePath("R/trend_kernel.cpp"); cache <- file.path(normalizePath("R"), ".rcpp_cache")

# --- the exact wiring used in R/2.1 §3.4 ---
load_kernel_env <- function() {
  ke <- new.env(parent = baseenv())
  suppressMessages(Rcpp::sourceCpp(cppf, cacheDir = cache, env = ke)); ke
}
make_fit_value_kernel <- function(ke) function(year, value) {
  n <- length(value)
  if (n < 4L || !all(is.finite(value))) return(list(slope = NA_real_, p_value = NA_real_))
  t0 <- ke$mk_sen_cpp(value); s0 <- t0$slope; i0 <- median(value - s0*year)
  d <- value - (s0*year + i0); r <- ke$lag1_ac_cpp(d)
  if (abs(r) <= 0.1) return(list(slope = t0$slope, p_value = t0$p_value))
  wr <- c(d[1L], d[-1L] - r*d[-n]); z <- wr + s0*year + i0; tz <- ke$mk_sen_cpp(z)
  list(slope = tz$slope, p_value = tz$p_value)
}

# --- synthetic: 4 source files, globally-unique group keys, mix AR1/clean ---
set.seed(1); dir <- tempfile("wire"); dir.create(dir)
fk <- c("srcid", "admin1_name", "scenario", "model", "hazard", "season")
files <- vapply(1:4, function(s) {
  g <- CJ(srcid = sprintf("s%d", s), admin1_name = sprintf("r%03d", 1:300), scenario = "ssp245",
          model = c("m1","m2"), hazard = c("PTOT","TMAX"), season = c("DJF","annual"), year = 2021:2044)
  g[, gid := .GRP, by = fk]
  g[, value := if (gid[1] %% 2 == 0) as.numeric(arima.sim(list(ar = 0.8), .N)) + 0.3*(year-2021)
               else 20 + 0.4*(year-2021) + rnorm(.N, 0, 2), by = fk]
  p <- file.path(dir, sprintf("src%d.parquet", s)); write_parquet(g, p); p
}, character(1))

cat(sprintf("[%s] compiling kernel (cache warm)...\n", ts()))
.pe <- load_kernel_env()  # warm cache + verify compile

# reference: plain lapply
rf <- make_fit_value_kernel(.pe)
ref <- rbindlist(lapply(files, function(f) data.table(read_parquet(f))[, rf(year, value), by = fk]))

# worker-local under future multisession
worker <- function(f) {
  fit <- make_fit_value_kernel(load_kernel_env())          # OWN env, built in-worker
  data.table(arrow::read_parquet(f))[, fit(year, value), by = fk]
}
plan(multisession, workers = 3)
new <- rbindlist(future_lapply(files, worker, future.seed = TRUE))
plan(sequential)

setkeyv(ref, fk); setkeyv(new, fk); m <- merge(ref, new, by = fk, suffixes = c(".r", ".n"))
sd_ <- max(abs(m$slope.r - m$slope.n), na.rm = TRUE); pd <- max(abs(m$p_value.r - m$p_value.n), na.rm = TRUE)
cat(sprintf("[%s] REF slopeNA=%.3f  NEW(multisession) slopeNA=%.3f  matched=%d/%d  max|slopeΔ|=%.2e max|pΔ|=%.2e\n",
            ts(), mean(is.na(ref$slope)), mean(is.na(new$slope)), nrow(m), nrow(ref), sd_, pd))
ok <- mean(is.na(new$slope)) < 0.02 && nrow(m) == nrow(ref) && sd_ < 1e-9 && pd < 1e-9
cat(if (ok) "PASS: worker-local kernel correct under future multisession\n" else "FAIL\n")
if (!ok) quit(status = 1)
