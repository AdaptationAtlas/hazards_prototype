# =============================================================================
# _bootstrap.R - shared minimal bootstrap for R/observational/1..6
#
# Replaces six near-identical copies of bootstrap_minimal(), each of which
# re-implemented the same five-host project_dir switch that R/0_server_setup.R
# used to carry. Adding a machine meant editing seven places; it is now a stanza
# in metadata/hosts.json.
#
# The six were not identical, and the differences are preserved exactly rather
# than averaged away - see the table below. Getting this wrong would silently
# change where the observational pipeline reads and writes.
#
#   script  candidates          probe                                stop?  gdal  hazfn
#   1       atlas_delta FIRST   none                                 no     yes   yes
#   2       nexgddp FIRST       Data/chirts_chirps_hist/PTOT (dir)    yes    yes   no
#   3       nexgddp FIRST       Data/chirts_chirps_hist/PTOT (dir)    yes    yes   no
#   4       nexgddp FIRST       .../admin/obs_monthly_adm0.parquet    no     no    no
#   5       nexgddp FIRST       Data/chirts_chirps_hist/PTOT (dir)    no     yes   no
#   6       nexgddp FIRST       Data/chirts_chirps_hist (dir)         no     no    no
#
# PRESERVED BUG - script 1 resolves CGlabs to the atlas_delta tree while its own
# --full branch sources 0_server_setup.R and gets nexgddp, so smoke and full
# write to DIFFERENT trees. Scripts 2-6 then probe nexgddp first and find
# whichever tree the last producer run happened to use. That is pre-existing
# behaviour, not something this refactor introduces, and it is reproduced here
# verbatim via prefer = "atlas_delta". Fixing it changes where CHIRPS lands and
# belongs in its own issue with its own gate.
#
# The probe predicate is file.exists(), not dir.exists(): script 4 probes a FILE
# while 2/3/5/6 probe directories, and file.exists() is TRUE for both.
# =============================================================================

atlas_bootstrap_minimal <- function(packages = character(),
                                    probe = NULL,
                                    prefer = NULL,
                                    require_probe = NULL,
                                    gdal_cache = 60000L,
                                    set_timeout = FALSE,
                                    create_hist_dir = FALSE,
                                    source_haz_functions = FALSE,
                                    log = NULL) {
  .say <- function(...) if (is.function(log)) log(paste0(...)) else invisible(NULL)
  .say("bootstrap_minimal: resolving project / working dirs")

  if (!requireNamespace("pacman", quietly = TRUE)) {
    install.packages("pacman", repos = "https://cloud.r-project.org")
  }
  library(pacman)
  if (length(packages)) pacman::p_load(char = packages)

  # Locate and load the resolver. These scripts run standalone via Rscript, so
  # they cannot assume 0_server_setup.R has been sourced.
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  base <- if (length(fa)) {
    dirname(normalizePath(sub("^--file=", "", fa[1]), mustWork = FALSE))
  } else {
    getwd()
  }
  cand <- c(
    file.path(base, "..", "00_paths.R"),   # Rscript R/observational/N_x.R
    file.path(base, "R", "00_paths.R"),    # sourced from the repo root
    "R/00_paths.R",
    file.path(Sys.getenv("project_dir"), "R", "00_paths.R")
  )
  hit <- cand[file.exists(cand)][1]
  if (is.na(hit)) stop("_bootstrap.R could not locate R/00_paths.R")
  source(normalizePath(hit))

  project_dir <- atlas_repo_root()

  # Candidate working dirs, preferred climdat source first. On CGlabs this is
  # the same two-element vector these scripts used to hardcode; on every other
  # host it collapses to the single pinned working_dir, matching the old
  # one-element switch() result.
  candidates <- atlas_working_dir_candidates(prefer = prefer)

  working_dir <- if (!is.null(probe)) {
    hits <- vapply(candidates, function(p) file.exists(file.path(p, probe)), logical(1))
    if (any(hits)) candidates[hits][1] else candidates[1]
  } else {
    candidates[1]
  }
  .say(sprintf("  selected working_dir: %s", working_dir))

  if (!dir.exists(working_dir)) dir.create(working_dir, recursive = TRUE)
  setwd(working_dir)

  chirts_chirps_hist_dir <- file.path("Data", "chirts_chirps_hist")
  if (isTRUE(create_hist_dir) && !dir.exists(chirts_chirps_hist_dir)) {
    dir.create(chirts_chirps_hist_dir, recursive = TRUE)
  }
  if (!is.null(require_probe) && !dir.exists(chirts_chirps_hist_dir)) {
    stop(require_probe)
  }

  if (isTRUE(source_haz_functions)) {
    source(file.path(project_dir, "R", "haz_functions.R"))
  }
  if (!is.null(gdal_cache)) terra::gdalCache(gdal_cache)
  if (isTRUE(set_timeout)) options(timeout = 600)

  list(
    project_dir            = project_dir,
    working_dir            = working_dir,
    chirts_chirps_hist_dir = chirts_chirps_hist_dir
  )
}

