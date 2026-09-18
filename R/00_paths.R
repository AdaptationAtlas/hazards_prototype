# =============================================================================
# 00_paths.R - declarative path resolver for hazards_prototype (issue #29)
#
# Source it directly, from the repo root:
#     source("R/00_paths.R")
# or let R/0_server_setup.R source it for you (it does, near the top).
#
# Replaces the chain of string equalities on `project_dir` that used to live in
# R/0_server_setup.R:105-136. A new machine is now a stanza in
# metadata/hosts.json plus (optionally) a couple of environment variables -
# configuration, not a code edit.
#
# Provides:
#   * atlas_repo_root()      - self-locating path to the git checkout
#   * atlas_host_id()        - which metadata/hosts.json profile matched
#   * atlas_climdat_source() - "nexgddp" | "atlas_delta", env-overridable
#   * atlas_common_data()    - shared bulk-data root for this host
#   * atlas_working_dir()    - the root that `Data/` hangs off
#   * atlas_dir()            - one key -> one path, the only lookup you need
#   * atlas_candidates()     - per-tree variants of one key, for probe callers
#   * atlas_env_flag()/atlas_env_or() - env readers with upstream's semantics
#   * atlas_describe()       - print how everything resolved, and why
#   * atlas_paths_selftest() - every resolved path as a named vector
#
# DESIGN CONTRACT - this file is PURE.
#   It must never call setwd(), dir.create(), or touch the network. The only
#   side effect permitted is options(). That purity is what lets the resolver be
#   exercised for every host profile, from any machine, without going near a
#   live data tree - see R/checks/70_path_snapshot.R.
#
#   Directory creation and the setwd() stay in R/0_server_setup.R, where they
#   have always been.
#
# Deliberately NOT sourced by hazards_upstream/. That subtree tracks the
# producer repo and commits to standalone, dependency-free operation. The two
# agree on exactly one thing - the COMMON_DATA environment variable - and a
# test pins them together. The coupling is a named variable, not a source().
# =============================================================================

# Idempotent: sourcing twice is a no-op. Scripts source this from several
# entry points and some of them re-source it after rm(list = ls()).
if (!isTRUE(getOption("atlas.paths_loaded"))) {

  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop(
      "00_paths.R needs the 'jsonlite' package to read metadata/hosts.json.\n",
      "  install.packages(\"jsonlite\")\n",
      "It is already an unconditional dependency of R/0_server_setup.R, so this ",
      "should only bite on a bare R install."
    )
  }

  `%||%` <- function(a, b) {
    if (is.null(a) || length(a) == 0L || (length(a) == 1L && is.na(a))) b else a
  }

  # ---- repo root ------------------------------------------------------------
  # Five-step chain, first hit wins. Steps 2 and 3 between them cover every way
  # this repo is actually run: source() from RStudio or another script (ofile),
  # and `Rscript R/foo.R` (--file=, where ofile is unset).
  #
  # The ofile scan matches basename == "00_paths.R" specifically, NOT just any
  # outer ofile. A script that source()s this file leaves its own ofile higher
  # in the call stack; matching that would root us in R/checks/ or
  # R/observational/ and the sentinel assert below would then fail confusingly.
  # Same correctness note as hazards_upstream/R/00_setup.R:39-43.
  .atlas_find_repo_root <- function() {
    opt <- getOption("atlas.repo_root")
    if (!is.null(opt) && nzchar(opt)) return(opt)

    # (2) source()-set ofile for THIS file
    for (i in seq_len(sys.nframe())) {
      of <- sys.frame(i)$ofile
      if (!is.null(of) && basename(of) == "00_paths.R") {
        return(dirname(dirname(normalizePath(of, mustWork = FALSE))))
      }
    }

    # (3) Rscript --file=
    fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
    if (length(fa)) {
      d <- dirname(normalizePath(sub("^--file=", "", fa[1]), mustWork = FALSE))
      # Walk up until the sentinel is found; scripts live at varying depths
      # (R/, R/checks/, R/observational/, scripts/).
      for (. in 1:4) {
        if (file.exists(file.path(d, "metadata", "data.json"))) return(d)
        d <- dirname(d)
      }
    }

    # (4) explicit env, then the legacy variable 0_server_setup.R used to write.
    #
    # ATLAS_PROJECT_DIR is checked FIRST and that ordering is load-bearing:
    # R applies ~/.Renviron AFTER the shell environment, so on a host whose
    # .Renviron sets project_dir (PASCAL does), `project_dir=... Rscript ...`
    # is silently ignored - the file wins. ATLAS_PROJECT_DIR is not in anyone's
    # .Renviron, so it survives. Use it, Sys.setenv() inside the call, or
    # R_ENVIRON_USER.
    for (v in c("ATLAS_PROJECT_DIR", "project_dir")) {
      p <- Sys.getenv(v, unset = "")
      if (nzchar(p)) return(path.expand(p))
    }

    # (5) walk up from the working directory looking for the sentinel
    d <- normalizePath(getwd(), mustWork = FALSE)
    for (. in 1:6) {
      if (file.exists(file.path(d, "metadata", "data.json"))) return(d)
      nd <- dirname(d)
      if (identical(nd, d)) break
      d <- nd
    }

    stop(
      "00_paths.R could not locate the hazards_prototype checkout.\n",
      "  Tried: options(atlas.repo_root), the source() ofile, Rscript --file=,\n",
      "         $ATLAS_PROJECT_DIR, $project_dir, and a walk up from ", getwd(), "\n",
      "Fix: run from the repo root, or set ATLAS_PROJECT_DIR=/path/to/hazards_prototype"
    )
  }

  atlas_repo_root <- function() {
    opt <- getOption("atlas.repo_root")
    if (!is.null(opt) && nzchar(opt)) return(opt)
    root <- .atlas_find_repo_root()
    # Sentinel assert. A wrong root here silently resolves Data/ somewhere
    # unintended, which is the exact failure issue #29 was filed about, so it
    # fails loudly rather than guessing.
    if (!file.exists(file.path(root, "metadata", "data.json"))) {
      stop(
        "Resolved repo root has no metadata/data.json, so it is not a ",
        "hazards_prototype checkout:\n  ", root
      )
    }
    # options() survive rm(list = ls()), which several pipeline scripts do
    # between sections. Same reasoning as upstream's options(hazards.r_root=).
    options(atlas.repo_root = root)
    root
  }

  # ---- environment readers --------------------------------------------------
  # Truthy/falsy parsing, matching hazards_upstream/R/00_setup.R:62-66 so one
  # variable cannot mean opposite things in the two halves of the repo.
  # strict = TRUE turns an unrecognised value into an error at script start
  # rather than a silently-wrong boolean.
  .ATLAS_TRUE  <- c("1", "true", "t", "yes", "y", "on")
  .ATLAS_FALSE <- c("0", "false", "f", "no", "n", "off")

  atlas_env_flag <- function(name, default = FALSE, strict = FALSE) {
    v <- Sys.getenv(name, unset = NA_character_)
    if (is.na(v) || v == "") return(default)
    v <- tolower(trimws(v))
    if (v %in% .ATLAS_TRUE)  return(TRUE)
    if (v %in% .ATLAS_FALSE) return(FALSE)
    if (strict) {
      stop(
        sprintf(
          "%s=%s is not a recognised boolean. Use one of {%s} or {%s}.",
          name, Sys.getenv(name),
          paste(.ATLAS_TRUE, collapse = ","), paste(.ATLAS_FALSE, collapse = ",")
        )
      )
    }
    default
  }

  atlas_env_or <- function(name, default) {
    v <- Sys.getenv(name, unset = NA_character_)
    if (is.na(v) || v == "") default else v
  }

  # ---- host profile ---------------------------------------------------------
  .atlas_hosts <- function() {
    cached <- getOption("atlas.hosts")
    if (!is.null(cached)) return(cached)
    f <- file.path(atlas_repo_root(), "metadata", "hosts.json")
    if (!file.exists(f)) stop("metadata/hosts.json not found at: ", f)
    h <- jsonlite::fromJSON(f, simplifyVector = FALSE)
    options(atlas.hosts = h)
    h
  }

  # Match on the exact project_dir string, which is what the old chain did and
  # therefore what keeps CGlabs byte-for-byte. `hostname` is only consulted as a
  # tiebreak when no project_dir matched, so it can never override a known box.
  .atlas_match_host <- function() {
    forced <- Sys.getenv("ATLAS_HOST", unset = "")
    hosts <- .atlas_hosts()$hosts

    if (nzchar(forced)) {
      hit <- Filter(function(h) identical(h$id, forced), hosts)
      if (!length(hit)) {
        stop(
          "ATLAS_HOST=", forced, " does not match any id in metadata/hosts.json.\n",
          "  Known ids: ", paste(vapply(hosts, `[[`, "", "id"), collapse = ", ")
        )
      }
      return(hit[[1]])
    }

    root <- atlas_repo_root()
    for (h in hosts) {
      pd <- unlist(h$match$project_dir %||% list())
      if (length(pd) && any(root == pd)) return(h)
    }

    nodename <- as.character(Sys.info()[["nodename"]] %||% "")
    if (nzchar(nodename)) {
      for (h in hosts) {
        rx <- unlist(h$match$hostname %||% list())
        if (length(rx) && any(vapply(rx, function(p) grepl(p, nodename), logical(1)))) {
          return(h)
        }
      }
    }

    NULL
  }

  atlas_host_id <- function() {
    h <- .atlas_profile_raw()
    h$id %||% "<unregistered>"
  }

  .atlas_profile_raw <- function() {
    cached <- getOption("atlas.host_profile")
    if (!is.null(cached)) return(cached)

    h <- .atlas_match_host()

    if (is.null(h)) {
      # Unknown host. Proceed ONLY if the caller gave us a root to work from;
      # otherwise fail loud. A generic default would quietly create an empty
      # tree and a twelve-hour run would produce nothing.
      has_escape <- nzchar(Sys.getenv("ATLAS_COMMON_DATA")) ||
        nzchar(Sys.getenv("COMMON_DATA")) ||
        nzchar(Sys.getenv("ATLAS_WORKING_DIR"))
      if (!has_escape) {
        ids <- vapply(.atlas_hosts()$hosts, `[[`, "", "id")
        stop(
          "Unrecognised host - no metadata/hosts.json profile matches this checkout.\n",
          "  detected repo root : ", atlas_repo_root(), "\n",
          "  known host ids     : ", paste(ids, collapse = ", "), "\n\n",
          "Either add a stanza to metadata/hosts.json:\n",
          '    { "id": "<name>",\n',
          '      "match": { "project_dir": ["', atlas_repo_root(), '"] },\n',
          '      "common_data": "/path/to/common_data",\n',
          '      "trees": { "nexgddp":     { "working_dir": "{common_data}/hazards_prototype" },\n',
          '                 "atlas_delta": { "working_dir": "{common_data}/hazards_prototype" } } }\n\n',
          "or, for a one-off run:\n",
          "    COMMON_DATA=/path/to/common_data Rscript <script>"
        )
      }
      h <- list(id = "<unregistered>", status = "UNREGISTERED")
    }

    options(atlas.host_profile = h)
    h
  }

  atlas_profile <- function() .atlas_profile_raw()

  # ---- climate data source --------------------------------------------------
  # Was two unconditional assignments in 0_server_setup.R:61-62 where the second
  # always won. Now a real, overridable setting.
  atlas_climdat_source <- function() {
    d <- .atlas_hosts()$defaults$climdat_source %||% "nexgddp"
    s <- tolower(trimws(atlas_env_or("ATLAS_CLIMDAT_SOURCE", d)))
    if (!s %in% c("nexgddp", "atlas_delta")) {
      stop("ATLAS_CLIMDAT_SOURCE must be 'nexgddp' or 'atlas_delta', got: ", s)
    }
    s
  }

  # ---- roots ----------------------------------------------------------------
  # COMMON_DATA is the variable hazards_upstream::common_data_root() reads, so
  # exporting it once points both halves of the repo at the same bulk store.
  atlas_common_data <- function() {
    v <- Sys.getenv("ATLAS_COMMON_DATA", unset = "")
    if (!nzchar(v)) v <- Sys.getenv("COMMON_DATA", unset = "")
    if (nzchar(v)) return(path.expand(v))

    p <- .atlas_profile_raw()$common_data
    if (is.null(p) || !nzchar(p)) {
      stop(
        "No common_data root for host '", atlas_host_id(), "'.\n",
        "Set COMMON_DATA=/path, or add \"common_data\" to its metadata/hosts.json stanza."
      )
    }
    path.expand(p)
  }

  # Cross-platform: POSIX root, tilde, UNC share, or a Windows drive letter.
  # Needed because the Data/ keys stay relative while everything else is not.
  .atlas_is_absolute <- function(p) {
    grepl("^(/|~|\\\\\\\\|[A-Za-z]:)", p)
  }

  # Expand {common_data} in a hosts.json template.
  .atlas_expand <- function(tmpl) {
    if (is.null(tmpl) || !nzchar(tmpl)) return(NA_character_)
    path.expand(gsub("{common_data}", atlas_common_data(), tmpl, fixed = TRUE))
  }

  # Per-host tree entry wins over the defaults template. This is what keeps the
  # nexgddp/atlas_delta fork confined to CGlabs: every other host pins both
  # trees to the same literal, exactly as the old chain did.
  .atlas_tree <- function(source = atlas_climdat_source(), key = "working_dir") {
    prof <- .atlas_profile_raw()
    v <- prof$trees[[source]][[key]]
    if (is.null(v) || !nzchar(v)) {
      v <- .atlas_hosts()$defaults$trees[[source]][[key]]
    }
    .atlas_expand(v)
  }

  atlas_working_dir <- function() {
    v <- Sys.getenv("ATLAS_WORKING_DIR", unset = "")
    if (nzchar(v)) return(path.expand(v))
    w <- .atlas_tree(key = "working_dir")
    if (is.na(w)) {
      stop(
        "No working_dir for host '", atlas_host_id(), "' / climdat_source '",
        atlas_climdat_source(), "'. Set ATLAS_WORKING_DIR or fix metadata/hosts.json."
      )
    }
    w
  }

  # Working dirs across every climdat source, deduped, preferred source first.
  # On CGlabs this reproduces the two-element candidate vector that the
  # observational scripts hardcode; everywhere else it collapses to one entry,
  # matching their single-element switch() result.
  atlas_working_dir_candidates <- function(prefer = NULL) {
    srcs <- c("nexgddp", "atlas_delta")
    if (!is.null(prefer)) srcs <- c(prefer, setdiff(srcs, prefer))
    out <- vapply(srcs, function(s) .atlas_tree(s, "working_dir") %||% NA_character_,
                  character(1))
    out <- out[!is.na(out) & nzchar(out)]
    unique(unname(out))
  }

  # ---- the key table --------------------------------------------------------
  # Infrastructure keys resolve to absolute paths outside the Data/ tree.
  .ATLAS_TREE_KEYS <- c(indices = "indices", indices_seasonal = "indices_seasonal")
  .ATLAS_RAW_KEYS  <- c("sos_raw", "isimip_raw", "chirts_raw", "chirps_raw", "cropsuite_raw")

  # Data/ subdirectory keys. Same 34 names R/0_server_setup.R builds into
  # atlas_dirs$data_dir, kept here so atlas_dir() is a single lookup for
  # callers that do not want to source the whole of setup.
  .ATLAS_DATA_KEYS <- c(
    "hazard_timeseries", "hazard_timeseries_mean_month", "hazard_timeseries_class",
    "hazard_timeseries_risk", "hazard_risk", "hazard_timeseries_mean",
    "hazard_timeseries_sd", "hazard_timeseries_int", "hazard_risk_vop_usd",
    "hazard_risk_vop", "hazard_risk_ha", "hazard_risk_n", "hazard_risk_vop_reduced",
    "hazard_exposure", "roi", "exposure", "isimip_timeseries", "isimip_timeseries_mean",
    "isimip_timeseries_sd", "cropsuite_class", "chirts_chirps_hist",
    "GLPS", "cattle_heatstress", "adaptive_capacity", "atlas_pop", "GLW4",
    "GLW4_2020", "livestock_vop", "afr_highlands", "fao", "sos", "ggcmi",
    "hydrobasins", "solution_tables"
  )

  .atlas_env_key <- function(key) paste0("ATLAS_", toupper(key), "_DIR")

  #' Resolve one logical directory key to a path.
  #'
  #' @param key      one of: "working", "data", "common_data", "indices",
  #'                 "indices_seasonal", the five *_raw keys, or any of the 34
  #'                 Data/ subdirectory names.
  #' @param absolute for Data/ keys, return working_dir-anchored rather than the
  #'                 relative "Data/<key>" string. Defaults FALSE because every
  #'                 existing caller relies on the relative form resolving
  #'                 against the cwd that 0_server_setup.R setwd()s into.
  #' @param require  stop() if the resolved path does not exist. Use this at the
  #'                 point a script genuinely needs the data, so the error names
  #'                 the missing tree instead of surfacing as a bare
  #'                 "object not found" several frames later.
  atlas_dir <- function(key, absolute = FALSE, require = FALSE) {
    stopifnot(is.character(key), length(key) == 1L)

    # (1) per-key environment override beats everything
    ov <- Sys.getenv(.atlas_env_key(key), unset = "")
    p <- if (nzchar(ov)) {
      path.expand(ov)
    } else if (key == "working") {
      atlas_working_dir()
    } else if (key == "common_data") {
      atlas_common_data()
    } else if (key == "data") {
      if (absolute) file.path(atlas_working_dir(), "Data") else "Data"
    } else if (key %in% names(.ATLAS_TREE_KEYS)) {
      .atlas_tree(key = .ATLAS_TREE_KEYS[[key]])
    } else if (key %in% .ATLAS_RAW_KEYS) {
      prof <- .atlas_profile_raw()
      v <- prof$raw[[key]] %||% .atlas_hosts()$defaults$raw[[key]]
      .atlas_expand(v)
    } else if (key %in% .ATLAS_DATA_KEYS) {
      if (absolute) file.path(atlas_working_dir(), "Data", key) else file.path("Data", key)
    } else {
      stop(
        "Unknown path key: '", key, "'.\n",
        "  infrastructure: working, data, common_data, indices, indices_seasonal, ",
        paste(.ATLAS_RAW_KEYS, collapse = ", "), "\n",
        "  Data/ subdirs : ", paste(.ATLAS_DATA_KEYS, collapse = ", ")
      )
    }

    if (isTRUE(require)) {
      if (is.na(p) || !nzchar(p)) {
        stop("Path key '", key, "' does not resolve on host '", atlas_host_id(), "'.")
      }
      if (!dir.exists(p)) {
        stop(
          "Required directory for '", key, "' is missing on host '", atlas_host_id(), "':\n",
          "  ", p, "\n",
          "Stage it, or point at an existing copy with ", .atlas_env_key(key), "=/path"
        )
      }
    }
    p
  }

  # Per-tree variants of one key, for the candidate-probe callers.
  atlas_candidates <- function(key, prefer = NULL) {
    wds <- atlas_working_dir_candidates(prefer = prefer)
    if (key %in% .ATLAS_DATA_KEYS) return(file.path(wds, "Data", key))
    if (key == "data") return(file.path(wds, "Data"))
    if (key == "working") return(wds)
    srcs <- c("nexgddp", "atlas_delta")
    if (!is.null(prefer)) srcs <- c(prefer, setdiff(srcs, prefer))
    if (key %in% names(.ATLAS_TREE_KEYS)) {
      out <- vapply(srcs, function(s) .atlas_tree(s, .ATLAS_TREE_KEYS[[key]]) %||% NA_character_,
                    character(1))
      return(unique(unname(out[!is.na(out)])))
    }
    atlas_dir(key)
  }

  # ---- reporting ------------------------------------------------------------
  atlas_paths_selftest <- function() {
    keys <- c("working", "data", "common_data", "indices", "indices_seasonal",
              .ATLAS_RAW_KEYS, .ATLAS_DATA_KEYS)
    vals <- vapply(keys, function(k) {
      tryCatch(as.character(atlas_dir(k)), error = function(e) paste0("<error: ", conditionMessage(e), ">"))
    }, character(1))
    c(host           = atlas_host_id(),
      repo_root      = atlas_repo_root(),
      climdat_source = atlas_climdat_source(),
      stats::setNames(vals, keys))
  }

  atlas_describe <- function() {
    prof <- .atlas_profile_raw()
    st <- atlas_paths_selftest()
    ts <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

    cat(sprintf("[%s] atlas paths | host=%s climdat_source=%s\n",
                ts, st[["host"]], st[["climdat_source"]]))
    shard <- Sys.getenv("ATLAS_SHARD", unset = "")
    if (nzchar(shard)) cat(sprintf("[%s] atlas paths | shard=%s\n", ts, shard))

    if (identical(prof$status, "UNVERIFIED")) {
      cat(sprintf(
        "[%s] WARN  | host profile '%s' is marked UNVERIFIED in metadata/hosts.json.\n",
        ts, prof$id
      ))
      cat(sprintf(
        "[%s] WARN  | its paths are carried over unchecked - confirm them on the node before a long run.\n",
        ts
      ))
    }
    if (identical(prof$status, "UNREGISTERED")) {
      cat(sprintf(
        "[%s] WARN  | no host profile matched; resolving from env overrides and defaults only.\n",
        ts
      ))
    }

    # The legacy ~/.Renviron entry outranks nothing now, but a stale value is
    # still read by a handful of scripts, so say so when it disagrees.
    legacy <- Sys.getenv("project_dir", unset = "")
    if (nzchar(legacy) && !identical(path.expand(legacy), st[["repo_root"]])) {
      cat(sprintf(
        "[%s] WARN  | $project_dir (%s) disagrees with the located repo root (%s).\n",
        ts, legacy, st[["repo_root"]]
      ))
      cat(sprintf(
        "[%s] WARN  | an older 0_server_setup.R wrote that into ~/.Renviron; consider removing the line.\n",
        ts
      ))
    }

    wd <- tryCatch(atlas_working_dir(), error = function(e) NA_character_)
    for (k in names(st)) {
      if (k %in% c("host", "climdat_source")) next
      v <- st[[k]]
      # Data/ keys are returned relative on purpose - they resolve against the
      # cwd that 0_server_setup.R setwd()s into. Anchor them for the existence
      # check so this report is not misleading when run from the repo root.
      probe <- if (!startsWith(v, "<error") && !.atlas_is_absolute(v) && !is.na(wd)) {
        file.path(wd, v)
      } else {
        v
      }
      mark <- if (startsWith(v, "<error")) "ERR " else if (dir.exists(probe)) "ok  " else "MISS"
      cat(sprintf("  %-4s %-28s %s\n", mark, k, v))
    }
    invisible(st)
  }

  options(atlas.paths_loaded = TRUE)
}
