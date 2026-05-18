# Shared helpers for the observational pipeline scripts.
#
# Source me after bootstrap_minimal() in each pipeline script:
#   source("R/observational/_helpers.R")
#
# Exports:
#   system_resources()           - probe cores + RAM (Linux + Mac)
#   parse_cli_flag(args, name)   - read --name <value> from commandArgs
#   resolve_workers(args, ...)   - compute worker count from CLI + heuristics
#   print_resource_banner(...)   - one-line "what will I use" stamp
#   parallel_flags_usage()       - usage text for --workers / --cpu-fraction
#                                  / --mem-fraction (for usage printouts)

#' Probe system cores and memory. Linux reads /proc/meminfo; Mac reads sysctl.
#' Returns NA fields rather than erroring if detection fails.
system_resources <- function() {
  cores_logical  <- parallel::detectCores(logical = TRUE)
  cores_physical <- parallel::detectCores(logical = FALSE)
  if (is.na(cores_physical)) cores_physical <- cores_logical

  total_gb <- NA_real_
  free_gb  <- NA_real_
  if (file.exists("/proc/meminfo")) {
    mem <- readLines("/proc/meminfo", warn = FALSE)
    grab <- function(key) {
      ln <- grep(paste0("^", key, ":"), mem, value = TRUE)
      if (length(ln) == 0L) return(NA_real_)
      as.numeric(sub(".*?(\\d+).*", "\\1", ln[1])) / 1024 / 1024
    }
    total_gb <- grab("MemTotal")
    free_gb  <- grab("MemAvailable")
    if (is.na(free_gb)) free_gb <- grab("MemFree")
  } else {
    out <- tryCatch(system("sysctl -n hw.memsize", intern = TRUE),
                    error = function(e) NA, warning = function(w) NA)
    if (length(out) && !is.na(out[1])) total_gb <- as.numeric(out[1]) / 1024^3
    free_gb <- total_gb
  }

  list(
    cores_logical  = cores_logical,
    cores_physical = cores_physical,
    total_ram_gb   = total_gb,
    free_ram_gb    = free_gb
  )
}

#' Parse a `--name value` CLI flag from commandArgs. Returns NULL when absent.
parse_cli_flag <- function(args, name, type = c("integer", "double", "character")) {
  type <- match.arg(type)
  i <- match(paste0("--", name), args)
  if (is.na(i) || i == length(args)) return(NULL)
  v <- args[i + 1L]
  switch(type,
    integer   = as.integer(v),
    double    = as.numeric(v),
    character = v
  )
}

#' Resolve parallel worker count from CLI flags + per-script defaults.
#'
#' Precedence: --workers > min(cpu_fraction * cores, mem_fraction * RAM / per_worker_gb).
#' Always clamped to [min_workers, max_workers] and at least 1.
#'
#' @param args             commandArgs(trailingOnly = TRUE)
#' @param per_worker_gb    estimated peak RAM per worker (script-specific)
#' @param default_cpu_frac default fraction of logical cores (0.5)
#' @param default_mem_frac default fraction of free RAM (0.5)
#' @param min_workers      lower clamp (default 1)
#' @param max_workers      upper clamp (default Inf)
#'
#' @return integer worker count
resolve_workers <- function(args,
                            per_worker_gb,
                            default_cpu_frac = 0.5,
                            default_mem_frac = 0.5,
                            min_workers = 1L,
                            max_workers = Inf) {
  explicit <- parse_cli_flag(args, "workers", "integer")
  if (!is.null(explicit) && !is.na(explicit)) {
    return(as.integer(max(min_workers, min(max_workers, explicit))))
  }

  cpu_frac <- parse_cli_flag(args, "cpu-fraction", "double")
  mem_frac <- parse_cli_flag(args, "mem-fraction", "double")
  if (is.null(cpu_frac) || is.na(cpu_frac)) cpu_frac <- default_cpu_frac
  if (is.null(mem_frac) || is.na(mem_frac)) mem_frac <- default_mem_frac

  res <- system_resources()
  cores_target <- max(1L, floor(cpu_frac * res$cores_logical))
  mem_budget   <- mem_frac * res$free_ram_gb
  workers_mem <- if (is.finite(mem_budget) && per_worker_gb > 0) {
    max(1L, floor(mem_budget / per_worker_gb))
  } else {
    cores_target
  }
  n <- min(cores_target, workers_mem)
  n <- max(min_workers, min(max_workers, n))
  as.integer(n)
}

#' One-line resource banner so the script's parallel config is visible on
#' startup. Call once after resolve_workers().
print_resource_banner <- function(workers, per_worker_gb, label = "parallel") {
  res <- system_resources()
  cat(sprintf(
    "[%s] workers=%d  per_worker_gb~%.1f  cores=%d/%d (logical/physical)  RAM=%.1f free / %.1f total GB\n",
    label, workers, per_worker_gb,
    res$cores_logical, res$cores_physical,
    res$free_ram_gb, res$total_ram_gb
  ))
  flush.console()
}

#' Usage text for the standard parallel flags. Append to per-script usage().
parallel_flags_usage <- function() {
  paste(
    "  --workers N         explicit worker count (overrides auto-detect)",
    "  --cpu-fraction X    use X fraction of logical cores (default 0.5)",
    "  --mem-fraction X    use X fraction of free RAM     (default 0.5)",
    sep = "\n"
  )
}
