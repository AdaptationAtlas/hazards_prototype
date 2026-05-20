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

#' Probe system cores and memory, respecting cgroup limits when present.
#' Order of precedence for the memory budget:
#'   1. cgroup v2 memory.max
#'   2. cgroup v1 memory.limit_in_bytes
#'   3. /proc/meminfo MemTotal (host RAM)
#'   4. sysctl hw.memsize (Mac fallback)
#' Returns NA fields rather than erroring if detection fails.
system_resources <- function() {
  cores_logical  <- parallel::detectCores(logical = TRUE)
  cores_physical <- parallel::detectCores(logical = FALSE)
  if (is.na(cores_physical)) cores_physical <- cores_logical

  read_first_number <- function(path) {
    if (!file.exists(path)) return(NA_real_)
    v <- tryCatch(readLines(path, n = 1, warn = FALSE),
                  error = function(e) character(0))
    if (length(v) == 0L || v[1] == "max") return(NA_real_)
    suppressWarnings(as.numeric(v[1]))
  }

  cgroup_max_bytes <- read_first_number("/sys/fs/cgroup/memory.max")
  if (is.na(cgroup_max_bytes)) {
    cgroup_max_bytes <- read_first_number("/sys/fs/cgroup/memory/memory.limit_in_bytes")
  }
  cgroup_total_gb <- if (is.finite(cgroup_max_bytes)) cgroup_max_bytes / 1024^3 else NA_real_

  # cgroup v1's memory.limit_in_bytes returns a giant sentinel (~9.2 EB =
  # 2^63 - 1 rounded to page size) when no limit is set, instead of the
  # cgroup v2 string "max". Treat any cgroup_total absurdly larger than the
  # host total as "unset" - otherwise the resource banner shows billions of
  # free GB and the mem-budget heuristic picks an unrealistic worker count.

  cgroup_used_bytes <- read_first_number("/sys/fs/cgroup/memory.current")
  if (is.na(cgroup_used_bytes)) {
    cgroup_used_bytes <- read_first_number("/sys/fs/cgroup/memory/memory.usage_in_bytes")
  }
  cgroup_used_gb <- if (is.finite(cgroup_used_bytes)) cgroup_used_bytes / 1024^3 else NA_real_

  host_total_gb <- NA_real_
  host_avail_gb <- NA_real_
  if (file.exists("/proc/meminfo")) {
    mem <- readLines("/proc/meminfo", warn = FALSE)
    grab <- function(key) {
      ln <- grep(paste0("^", key, ":"), mem, value = TRUE)
      if (length(ln) == 0L) return(NA_real_)
      as.numeric(sub(".*?(\\d+).*", "\\1", ln[1])) / 1024 / 1024
    }
    host_total_gb <- grab("MemTotal")
    host_avail_gb <- grab("MemAvailable")
    if (is.na(host_avail_gb)) host_avail_gb <- grab("MemFree")
  } else {
    out <- tryCatch(system("sysctl -n hw.memsize", intern = TRUE),
                    error = function(e) NA, warning = function(w) NA)
    if (length(out) && !is.na(out[1])) host_total_gb <- as.numeric(out[1]) / 1024^3
    host_avail_gb <- host_total_gb
  }

  # Sanity-clamp the cgroup numbers. cgroup v1 unset limits show up as
  # giant sentinels (~9.2 EB rather than NA); if cgroup_total exceeds host
  # by an unrealistic factor, treat cgroup as effectively unset so the
  # downstream free / total logic falls back to host values.
  if (!is.na(cgroup_total_gb) && !is.na(host_total_gb) &&
      cgroup_total_gb > host_total_gb * 1.5) {
    cgroup_total_gb <- NA_real_
    cgroup_used_gb <- NA_real_
  }

  # Effective budget = tightest of (cgroup limit, host total).
  total_gb <- if (!is.na(cgroup_total_gb) && !is.na(host_total_gb)) {
    min(cgroup_total_gb, host_total_gb)
  } else if (!is.na(cgroup_total_gb)) {
    cgroup_total_gb
  } else {
    host_total_gb
  }

  # Free budget: prefer cgroup-aware (total - cgroup_used) when in a container;
  # else use host MemAvailable.
  free_gb <- if (!is.na(cgroup_total_gb) && !is.na(cgroup_used_gb)) {
    max(0, cgroup_total_gb - cgroup_used_gb)
  } else {
    host_avail_gb
  }

  list(
    cores_logical   = cores_logical,
    cores_physical  = cores_physical,
    total_ram_gb    = total_gb,
    free_ram_gb     = free_gb,
    host_total_gb   = host_total_gb,
    cgroup_total_gb = cgroup_total_gb,
    cgroup_used_gb  = cgroup_used_gb
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

  cpu_frac      <- parse_cli_flag(args, "cpu-fraction", "double")
  mem_frac      <- parse_cli_flag(args, "mem-fraction", "double")
  mem_budget_gb <- parse_cli_flag(args, "mem-budget", "double")
  if (is.null(cpu_frac) || is.na(cpu_frac)) cpu_frac <- default_cpu_frac
  if (is.null(mem_frac) || is.na(mem_frac)) mem_frac <- default_mem_frac

  res <- system_resources()
  cores_target <- max(1L, floor(cpu_frac * res$cores_logical))

  # Effective memory budget: explicit --mem-budget wins, else mem_frac of the
  # cgroup-aware free RAM detected by system_resources().
  if (!is.null(mem_budget_gb) && !is.na(mem_budget_gb)) {
    mem_budget <- mem_budget_gb
  } else {
    mem_budget <- mem_frac * res$free_ram_gb
  }

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
#' startup. Call once after resolve_workers(). Shows both the effective
#' (cgroup-aware) budget and the underlying host figures when they differ.
print_resource_banner <- function(workers, per_worker_gb, label = "parallel") {
  res <- system_resources()
  detail <- if (is.finite(res$cgroup_total_gb) &&
                !isTRUE(all.equal(res$cgroup_total_gb, res$host_total_gb))) {
    sprintf(" [cgroup=%.1f / host=%.1f GB]", res$cgroup_total_gb, res$host_total_gb)
  } else {
    ""
  }
  cat(sprintf(
    "[%s] workers=%d  per_worker_gb~%.1f  cores=%d/%d (logical/physical)  RAM=%.1f free / %.1f total GB%s  budget=%.1f GB\n",
    label, workers, per_worker_gb,
    res$cores_logical, res$cores_physical,
    res$free_ram_gb, res$total_ram_gb, detail,
    workers * per_worker_gb
  ))
  flush.console()
}

#' Usage text for the standard parallel flags. Append to per-script usage().
parallel_flags_usage <- function() {
  paste(
    "  --workers N         explicit worker count (overrides auto-detect)",
    "  --cpu-fraction X    use X fraction of logical cores (default 0.5)",
    "  --mem-fraction X    use X fraction of free RAM     (default 0.5)",
    "  --mem-budget G      explicit memory budget in GB (overrides --mem-fraction)",
    "  --overwrite         rebuild outputs even when already present on disk",
    sep = "\n"
  )
}

#' Returns TRUE if --overwrite appears anywhere in args (boolean flag, no value).
parse_overwrite_flag <- function(args) {
  isTRUE("--overwrite" %in% args)
}
