#!/usr/bin/env Rscript
# =============================================================================
# 74_no_host_literals.R - stop new hardcoded host paths getting in (issue #29)
#
#   Rscript R/checks/74_no_host_literals.R
#
# Issue #29 counted 78 hardcoded absolute paths across R/ and python/. Most are
# now resolved through R/00_paths.R. This guards the result: it fails if a NEW
# host literal appears in a file that is supposed to be clean.
#
# It is a regression guard, not a cleanup tool. The allow-list below is not a
# backlog - most entries are deliberate and should stay.
#
# Comments are ignored: a path in prose is documentation, not a dependency.
# =============================================================================

PATTERNS <- c("/home/jovyan", "/home/psteward", "/cluster01", "/Users/pstewarda",
              "D:/rprojects", "C:/rprojects")

# Files permitted to carry host literals, each for a stated reason.
ALLOW <- c(
  # Dated forensic artefacts. Their value IS that they record what was run on a
  # specific box on a specific day; rewriting them destroys that and buys no
  # portability, since nobody re-runs them on another host.
  "R/diag_sec3_4_kernel_speed.R", "R/diag_sec3_4_real_diff.R",
  "R/probe_sec3_4_fix_verify.R", "R/probe_sec3_4_futures_realout.R",
  "R/probe_sec3_4_merge.R", "R/probe_sec3_4_worker_valuefit.R",
  "R/probe_trendkernel_cglabs.R",
  "R/misc/heatstress.R", "R/misc/ndws_qaqc.R",
  "R/misc/generate_faostat_processed_to_raw.R",
  # Archived, not on any live path.
  "R/archive/push_to_s3_eia_climate_prioritization.R",
  # Legacy interactive runbook. The publish layer needs its own holistic
  # revision (issue #29 puts it explicitly out of scope), so leave it be.
  "R/push_to_s3.R"
)

root <- local({
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  d <- if (length(fa)) dirname(normalizePath(sub("^--file=", "", fa[1]), mustWork = FALSE)) else getwd()
  for (. in 1:5) {
    if (file.exists(file.path(d, "metadata", "data.json"))) return(d)
    d <- dirname(d)
  }
  stop("cannot locate repo root")
})

files <- list.files(file.path(root, "R"), pattern = "\\.R$", recursive = TRUE, full.names = TRUE)
files <- c(files, list.files(file.path(root, "python"), pattern = "\\.py$",
                             recursive = TRUE, full.names = TRUE))
rel <- sub(paste0("^", root, "/"), "", files)

bad <- list()
for (i in seq_along(files)) {
  if (rel[i] %in% ALLOW) next
  if (startsWith(rel[i], "hazards_upstream/")) next  # vendored subtree, tracks upstream
  if (rel[i] == "R/checks/74_no_host_literals.R") next  # this file defines the patterns
  txt <- readLines(files[i], warn = FALSE)
  # Strip comments - a path in prose documents history, it is not a dependency.
  code <- sub("#.*$", "", txt)
  hit <- which(Reduce(`|`, lapply(PATTERNS, function(p) grepl(p, code, fixed = TRUE))))
  if (length(hit)) {
    bad[[rel[i]]] <- sprintf("  %s:%d: %s", rel[i], hit, trimws(substr(txt[hit], 1, 90)))
  }
}

if (!length(bad)) {
  cat("PASS - no new host literals outside the allow-list\n")
  quit(save = "no", status = 0L)
}
cat("FAIL - hardcoded host paths found:\n\n")
for (f in names(bad)) cat(paste(bad[[f]], collapse = "\n"), "\n")
cat("\nResolve them through R/00_paths.R (atlas_dir / atlas_common_data), or add\n")
cat("the file to ALLOW in this script WITH a reason.\n")
quit(save = "no", status = 1L)
