#!/usr/bin/env Rscript
# =============================================================================
# 73_catalogue.R - the dataset catalogue (issue #29 item 2)
#
# One place to answer: what datasets does this project have, what version, where
# do they live, are they on Atlas S3, do they have CDH metadata, how would they
# get onto another host, and what is missing.
#
#   Rscript R/checks/73_catalogue.R --status            # the table
#   Rscript R/checks/73_catalogue.R --dataset <id>      # one record in full
#   Rscript R/checks/73_catalogue.R --gaps              # only what needs attention
#   Rscript R/checks/73_catalogue.R --transfer          # how each dataset reaches a host
#   Rscript R/checks/73_catalogue.R --orphans           # cross-check against CDH + stages
#   Rscript R/checks/73_catalogue.R --render            # rewrite docs/DATA_INDEX.md
#
# Source of truth is metadata/catalogue/<id>.json, one file per dataset, so
# `git log -- metadata/catalogue/<id>.json` is that dataset's change history.
# See metadata/catalogue/_SCHEMA.md.
#
# Pure read, except --render which rewrites docs/DATA_INDEX.md.
#
# "present" is answered for THIS host only, via R/00_paths.R. It says nothing
# about the other hosts - a catalogue cannot know what it cannot see, and
# pretending otherwise is how stale inventories start.
# =============================================================================

options(stringsAsFactors = FALSE)

args <- commandArgs(TRUE)
has <- function(f) f %in% args
val <- function(f, default = NULL) {
  i <- match(f, args)
  if (is.na(i) || i == length(args)) default else args[i + 1L]
}
`%||%` <- function(a, b) if (is.null(a) || length(a) == 0L) b else a

repo_root <- local({
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  d <- if (length(fa)) dirname(normalizePath(sub("^--file=", "", fa[1]), mustWork = FALSE)) else getwd()
  for (. in 1:5) {
    if (file.exists(file.path(d, "metadata", "data.json"))) return(d)
    d <- dirname(d)
  }
  stop("cannot locate repo root from ", getwd())
})
source(file.path(repo_root, "R", "00_paths.R"))

cat_dir <- file.path(repo_root, "metadata", "catalogue")
files <- sort(list.files(cat_dir, pattern = "\\.json$", full.names = TRUE))
if (!length(files)) stop("no catalogue records under ", cat_dir)
recs <- lapply(files, function(f) jsonlite::fromJSON(f, simplifyVector = FALSE))
names(recs) <- vapply(recs, function(r) r$id, character(1))

# ---- live probe: is it on this host? ----------------------------------------
resolve_loc <- function(r) {
  loc <- r$location
  if (is.null(loc)) return(NA_character_)
  base <- if (!is.null(loc$key)) {
    tryCatch(atlas_dir(loc$key, absolute = TRUE), error = function(e) NA_character_)
  } else if (!is.null(loc$path)) {
    file.path(atlas_working_dir(), loc$path)
  } else {
    NA_character_
  }
  if (is.na(base)) return(NA_character_)
  if (!is.null(loc$subpath)) base <- file.path(base, loc$subpath)
  base
}

probe <- function(r) {
  d <- resolve_loc(r)
  if (is.na(d)) return(list(state = "n/a", n = NA_integer_, dir = NA_character_))
  if (!dir.exists(d)) return(list(state = "absent", n = NA_integer_, dir = d))
  loc <- r$location
  n <- length(list.files(d, pattern = utils::glob2rx(loc$glob %||% "*"),
                         recursive = isTRUE(loc$recursive)))
  minf <- r$completeness$min_files %||% 1L
  # "empty" and "partial" are worth distinguishing: an empty directory usually
  # means setup created the tree but the producing stage never ran here, while
  # a partial one means it ran and did not finish.
  state <- if (n >= minf) "present" else if (n == 0L) "empty" else "partial"
  list(state = state, n = n, dir = d)
}

cdh_exists <- function(id) {
  if (is.null(id)) return(NA)
  any(file.exists(file.path(repo_root, "metadata", "cdh", paste0(id, c(".yaml", ".cdh.yaml")))))
}

# ---- single record ----------------------------------------------------------
if (!is.null(val("--dataset"))) {
  id <- val("--dataset")
  r <- recs[[id]]
  if (is.null(r)) stop("unknown dataset '", id, "'. Known: ", paste(names(recs), collapse = ", "))
  p <- probe(r)
  cat(sprintf("\n%s  (%s)\n", r$title, r$id))
  cat(strrep("-", 78), "\n")
  cat(r$description, "\n\n")
  cat(sprintf("  class        %s\n", r$class))
  cat(sprintf("  version      %s   status: %s\n", r$version %||% "-", r$status %||% "-"))
  if (!is.null(r$superseded_by)) cat(sprintf("  superseded by %s\n", r$superseded_by))
  cat(sprintf("  origin       %s  %s\n", r$origin$type %||% "-", r$origin$url %||% ""))
  if (!is.null(r$origin$checksum_manifest)) {
    cat(sprintf("  checksums    %s\n", r$origin$checksum_manifest))
  }
  cat(sprintf("  transfer     %s - %s\n", r$transfer$strategy, r$transfer$rationale))
  if (!is.null(r$transfer$regenerate_with)) {
    cat(sprintf("  rebuild with %s\n", r$transfer$regenerate_with))
  }
  cat(sprintf("  size         %s\n", r$transfer$approx_size %||% "unmeasured"))
  cat(sprintf("  here         %s%s\n", p$state,
              if (!is.na(p$n)) sprintf("  (n=%d)", p$n) else ""))
  if (!is.na(p$dir)) cat(sprintf("               %s\n", p$dir))
  cat(sprintf("  produced by  %s\n", paste(unlist(r$produced_by) %||% "-", collapse = ", ")))
  cat(sprintf("  consumed by  %s\n", paste(unlist(r$consumed_by) %||% "-", collapse = ", ")))
  cat(sprintf("  atlas S3     %s\n", if (is.null(r$atlas_s3)) "not published" else
    sprintf("%s%s", r$atlas_s3$prefix, if (isTRUE(r$atlas_s3$complete)) "" else "  [PARTIAL]")))
  if (!is.null(r$atlas_s3$note)) cat(sprintf("               %s\n", r$atlas_s3$note))
  cat(sprintf("  CDH record   %s\n", if (is.null(r$cdh_record)) "none" else
    sprintf("%s%s", r$cdh_record, if (isTRUE(cdh_exists(r$cdh_record))) "" else "  [FILE MISSING]")))
  if (length(r$gaps)) {
    cat("\n  gaps\n")
    for (g in unlist(r$gaps)) cat(sprintf("    - %s\n", g))
  }
  if (length(r$changelog)) {
    cat("\n  changelog\n")
    for (c in r$changelog) cat(sprintf("    %s  %s  (%s)\n", c$date, c$change, c$ref %||% ""))
  }
  cat("\n")
  quit(save = "no", status = 0L)
}

# ---- build the joined table -------------------------------------------------
tab <- do.call(rbind, lapply(recs, function(r) {
  p <- probe(r)
  data.frame(
    id = r$id,
    class = r$class,
    version = r$version %||% "-",
    status = r$status %||% "-",
    here = p$state,
    n = if (is.na(p$n)) "" else as.character(p$n),
    s3 = if (is.null(r$atlas_s3)) "-" else if (isTRUE(r$atlas_s3$complete)) "yes" else "partial",
    cdh = if (is.null(r$cdh_record)) "-" else if (isTRUE(cdh_exists(r$cdh_record))) "yes" else "BROKEN",
    transfer = r$transfer$strategy,
    gaps = length(r$gaps %||% list())
  )
}))
tab <- tab[order(tab$class, tab$id), ]
rownames(tab) <- NULL

if (has("--orphans")) {
  cat("\nCross-checks between the catalogue and the rest of the metadata.\n\n")
  # CDH records with no catalogue entry
  cdh_files <- list.files(file.path(repo_root, "metadata", "cdh"), pattern = "\\.ya?ml$")
  cdh_ids <- sub("\\.(cdh\\.)?ya?ml$", "", cdh_files)
  claimed <- unlist(lapply(recs, function(r) r$cdh_record %||% NULL))
  miss <- setdiff(cdh_ids, claimed)
  cat("CDH records with no catalogue entry (", length(miss), "):\n", sep = "")
  for (m in miss) cat("  - ", m, "\n", sep = "")
  # catalogue entries pointing at a CDH file that is not there
  broken <- tab$id[tab$cdh == "BROKEN"]
  cat("\nCatalogue entries naming a missing CDH file (", length(broken), "):\n", sep = "")
  for (b in broken) cat("  - ", b, "\n", sep = "")
  # stage inputs no record covers
  sf <- file.path(repo_root, "metadata", "stages.json")
  if (file.exists(sf)) {
    st <- jsonlite::fromJSON(sf, simplifyVector = FALSE)
    stage_ids <- vapply(st$stages, function(s) s$id, character(1))
    referenced <- unique(unlist(lapply(recs, function(r) c(unlist(r$produced_by), unlist(r$consumed_by)))))
    unref <- setdiff(stage_ids, referenced)
    cat("\nStages no catalogue record mentions (", length(unref), "):\n", sep = "")
    for (u in unref) cat("  - ", u, "\n", sep = "")
  }
  cat("\n")
  quit(save = "no", status = 0L)
}

if (has("--transfer")) {
  cat("\nHow each dataset reaches a new host.\n\n")
  for (s in c("pull-from-origin", "regenerate", "must-transfer")) {
    sel <- Filter(function(r) identical(r$transfer$strategy, s), recs)
    cat(sprintf("%s  (%d)\n", toupper(s), length(sel)))
    for (r in sel) {
      cat(sprintf("  %-26s %s\n", r$id, r$transfer$approx_size %||% "size unmeasured"))
      cat(sprintf("  %-26s %s\n", "", r$transfer$rationale))
    }
    cat("\n")
  }
  quit(save = "no", status = 0L)
}

if (has("--gaps")) {
  cat("\nDatasets with recorded gaps.\n\n")
  any_gap <- FALSE
  for (r in recs) {
    if (!length(r$gaps %||% list())) next
    any_gap <- TRUE
    cat(sprintf("%s  (%s)\n", r$id, r$status %||% "-"))
    for (g in unlist(r$gaps)) cat(sprintf("   - %s\n", g))
    cat("\n")
  }
  if (!any_gap) cat("  none recorded\n\n")
  quit(save = "no", status = 0L)
}

if (has("--render")) {
  out <- file.path(repo_root, "docs", "DATA_INDEX.md")
  dir.create(dirname(out), showWarnings = FALSE, recursive = TRUE)
  L <- character(0)
  L <- c(L, "# Data index",
    "",
    "**Generated — do not edit by hand.** Rebuild with:",
    "",
    "```bash",
    "Rscript R/checks/73_catalogue.R --render",
    "```",
    "",
    paste0("Source of truth: `metadata/catalogue/*.json` (one file per dataset, so ",
           "`git log` on a record is that dataset's change history). Schema: ",
           "`metadata/catalogue/_SCHEMA.md`."),
    "",
    paste0("The **here** column reflects one host only — whichever machine rendered this. ",
           "It is a snapshot, not a claim about the other servers."),
    "",
    "| dataset | class | version | status | here | Atlas S3 | CDH | transfer |",
    "|---|---|---|---|---|---|---|---|")
  for (i in seq_len(nrow(tab))) {
    L <- c(L, sprintf("| [`%s`](../metadata/catalogue/%s.json) | %s | %s | %s | %s%s | %s | %s | %s |",
      tab$id[i], tab$id[i], tab$class[i], tab$version[i], tab$status[i], tab$here[i],
      if (nzchar(tab$n[i])) paste0(" (", tab$n[i], ")") else "",
      tab$s3[i], tab$cdh[i], tab$transfer[i]))
  }
  L <- c(L, "", "## Transfer strategy", "",
    paste0("How a dataset reaches a new host. The monthly indices are `regenerate`, ",
           "not `must-transfer`, because they derive from a public archive that any ",
           "host can fetch directly — which is why no host-to-host link is needed."),
    "")
  for (s in c("pull-from-origin", "regenerate", "must-transfer")) {
    sel <- Filter(function(r) identical(r$transfer$strategy, s), recs)
    L <- c(L, sprintf("### %s (%d)", s, length(sel)), "")
    for (r in sel) L <- c(L, sprintf("- **%s** — %s", r$id, r$transfer$rationale))
    L <- c(L, "")
  }
  gapped <- Filter(function(r) length(r$gaps %||% list()) > 0, recs)
  L <- c(L, "## Open gaps", "",
         "Recorded in the catalogue, surfaced here so they are not invisible.", "")
  for (r in gapped) {
    L <- c(L, sprintf("### %s", r$id), "")
    for (g in unlist(r$gaps)) L <- c(L, sprintf("- %s", g))
    L <- c(L, "")
  }
  L <- c(L, "---", "",
         sprintf("Rendered %s on host `%s`.", format(Sys.Date()), atlas_host_id()))
  writeLines(L, out)
  cat("wrote ", out, " (", nrow(tab), " datasets)\n", sep = "")
  quit(save = "no", status = 0L)
}

# ---- default: --status ------------------------------------------------------
cat(sprintf("\nhost=%s  working_dir=%s\n\n", atlas_host_id(), atlas_working_dir()))
cat(sprintf("%-26s %-13s %-17s %-10s %-17s %-8s %-7s %s\n",
            "dataset", "class", "version", "here", "transfer", "atlasS3", "cdh", "gaps"))
cat(strrep("-", 110), "\n")
for (i in seq_len(nrow(tab))) {
  cat(sprintf("%-26s %-13s %-17s %-10s %-17s %-8s %-7s %s\n",
              tab$id[i], tab$class[i], substr(tab$version[i], 1, 17),
              paste0(tab$here[i], if (nzchar(tab$n[i])) paste0(":", tab$n[i]) else ""),
              tab$transfer[i], tab$s3[i], tab$cdh[i],
              if (tab$gaps[i] > 0) sprintf("%d", tab$gaps[i]) else ""))
}
cat("\n")
cat(sprintf("%d datasets. here: %d present, %d partial, %d empty, %d absent.\n",
            nrow(tab), sum(tab$here == "present"), sum(tab$here == "partial"),
            sum(tab$here == "empty"), sum(tab$here == "absent")))
cat("  --gaps for what needs attention, --orphans for metadata cross-checks,\n")
cat("  --transfer for how each reaches a new host, --dataset <id> for one record.\n\n")
