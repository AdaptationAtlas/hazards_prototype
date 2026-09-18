#!/usr/bin/env Rscript
# =============================================================================
# 70_path_snapshot.R - equivalence harness for the issue #29 path refactor
#
# Two modes.
#
#   --resolver-only            (safe anywhere, no setup, no network, ~1 second)
#       Sources ONLY R/00_paths.R and loops ATLAS_HOST over every id in
#       metadata/hosts.json x every climdat_source, dumping every resolved path.
#       Because R/00_paths.R is pure and ATLAS_HOST forces a profile, this
#       regression-tests the CGlabs resolution FROM ANY MACHINE. Output is
#       compared against the committed golden file metadata/hosts_expected.tsv.
#
#           Rscript R/checks/70_path_snapshot.R --resolver-only --check
#           Rscript R/checks/70_path_snapshot.R --resolver-only --write-golden
#
#   --out <file>               (run on the host being verified)
#       Sources the REAL R/0_server_setup.R and dumps every path-shaped global
#       it leaves behind. Run on a worktree of the merge-base for `pre`, on the
#       branch for `post`, then diff.
#
#           Rscript R/checks/70_path_snapshot.R --out /tmp/pre.tsv
#           diff -u /tmp/pre.tsv /tmp/post.tsv
#
# IS IT SAFE TO RUN MID-FLIGHT? Yes. The only mutation 0_server_setup.R performs
# is dir.create(), which is a no-op wherever the tree already exists, plus the
# section 3 downloads, which are all skip-if-exists. It writes no pipeline
# output and starts no computation. Nothing here touches a data file.
#
# WHAT IT CAPTURES - three layers, and layer 2 is the load-bearing one:
#   1. named scalars set explicitly by setup (working_dir, indices_dir, ...)
#   2. the globals created by the assign() loop at 0_server_setup.R:270-273,
#      swept GENERATIVELY out of globalenv(). Never hand-list these: if someone
#      adds a subdir, the sweep catches it and a hand-written list would not.
#   3. unlist(atlas_dirs) flattened, plus getwd() at end of setup - the setwd()
#      is a globally observable effect and must match too.
#
# Values are dumped RAW. No normalizePath(). Byte-for-byte means the literal,
# including artefacts like "Data/boundariesintermediate" (a paste0 without a
# separator at 0_server_setup.R:301) that a normalising pass would hide.
# =============================================================================

options(stringsAsFactors = FALSE)

args <- commandArgs(TRUE)
has <- function(f) f %in% args
val <- function(f, default = NULL) {
  i <- match(f, args)
  if (is.na(i) || i == length(args)) default else args[i + 1L]
}

ts <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.t0 <- Sys.time()
.tl <- .t0
say <- function(...) {
  now <- Sys.time()
  cat(sprintf("[%s] +%6.1fs (%s%4.1fs) | %s\n",
              format(now, "%Y-%m-%d %H:%M:%S"),
              as.numeric(difftime(now, .t0, units = "secs")),
              "Δ", as.numeric(difftime(now, .tl, units = "secs")),
              paste0(..., collapse = "")))
  .tl <<- now
  flush.console()
}

# Locate the repo without depending on the resolver (this script must be able to
# run against a checkout that predates R/00_paths.R, to produce the `pre` side).
repo_root <- local({
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  d <- if (length(fa)) {
    dirname(normalizePath(sub("^--file=", "", fa[1]), mustWork = FALSE))
  } else {
    getwd()
  }
  for (. in 1:5) {
    if (file.exists(file.path(d, "metadata", "data.json"))) return(d)
    d <- dirname(d)
  }
  stop("cannot locate repo root from ", getwd())
})

write_tsv <- function(df, path) {
  con <- if (is.null(path) || path == "-") stdout() else file(path, "w")
  on.exit(if (!identical(con, stdout())) close(con), add = TRUE)
  writeLines(paste(names(df), collapse = "\t"), con)
  writeLines(do.call(paste, c(unname(as.list(df)), sep = "\t")), con)
}

# -----------------------------------------------------------------------------
# Mode A: resolver-only, every host x every climdat_source
# -----------------------------------------------------------------------------
if (has("--resolver-only")) {
  golden <- file.path(repo_root, "metadata", "hosts_expected.tsv")
  say("resolver-only sweep; repo_root=", repo_root)

  source(file.path(repo_root, "R", "00_paths.R"))
  hosts <- jsonlite::fromJSON(file.path(repo_root, "metadata", "hosts.json"),
                              simplifyVector = FALSE)
  ids <- vapply(hosts$hosts, `[[`, "", "id")
  srcs <- c("nexgddp", "atlas_delta")

  rows <- list()
  for (id in ids) {
    for (s in srcs) {
      # Each combination gets a clean R process. The resolver caches its
      # profile in options(), so resolving two hosts in one session would
      # return the first one's answer for both.
      script <- sprintf(
        'source("%s"); st <- atlas_paths_selftest();
         cat(paste(names(st), unname(st), sep = "\t", collapse = "\n"))',
        file.path(repo_root, "R", "00_paths.R")
      )
      out <- suppressWarnings(system2(
        file.path(R.home("bin"), "Rscript"),
        args = c("-e", shQuote(script)),
        env = c(sprintf("ATLAS_HOST=%s", id),
                sprintf("ATLAS_CLIMDAT_SOURCE=%s", s),
                sprintf("ATLAS_PROJECT_DIR=%s", repo_root),
                # Neutralise any ambient overrides so the golden file is a
                # property of hosts.json alone, not of whoever ran it.
                "ATLAS_COMMON_DATA=", "COMMON_DATA=", "ATLAS_WORKING_DIR="),
        stdout = TRUE, stderr = TRUE
      ))
      if (!is.null(attr(out, "status")) && attr(out, "status") != 0L) {
        stop("resolver failed for host=", id, " source=", s, ":\n",
             paste(out, collapse = "\n"))
      }
      kv <- do.call(rbind, strsplit(out[grepl("\t", out)], "\t", fixed = TRUE))
      rows[[length(rows) + 1L]] <- data.frame(
        host = id, climdat_source = s, key = kv[, 1], value = kv[, 2]
      )
    }
  }

  df <- do.call(rbind, rows)
  # repo_root is machine-specific; the golden file must be portable.
  df <- df[df$key != "repo_root", ]
  df <- df[order(df$host, df$climdat_source, df$key), ]
  rownames(df) <- NULL
  say("resolved ", nrow(df), " rows across ", length(ids), " hosts x ", length(srcs), " sources")

  if (has("--write-golden")) {
    write_tsv(df, golden)
    say("wrote golden file: ", golden)
    quit(save = "no", status = 0L)
  }

  if (has("--check")) {
    if (!file.exists(golden)) stop("golden file missing: ", golden)
    exp <- read.delim(golden, colClasses = "character")
    exp <- exp[order(exp$host, exp$climdat_source, exp$key), ]
    rownames(exp) <- NULL
    if (identical(exp, df)) {
      say("PASS - resolver output matches metadata/hosts_expected.tsv")
      quit(save = "no", status = 0L)
    }
    key <- function(d) paste(d$host, d$climdat_source, d$key, sep = "|")
    m <- merge(exp, df, by = c("host", "climdat_source", "key"),
               all = TRUE, suffixes = c(".expected", ".actual"))
    bad <- m[is.na(m$value.expected) | is.na(m$value.actual) |
               m$value.expected != m$value.actual, ]
    say("FAIL - ", nrow(bad), " row(s) differ from the golden file")
    print(bad, row.names = FALSE)
    quit(save = "no", status = 1L)
  }

  write_tsv(df, val("--out", "-"))
  quit(save = "no", status = 0L)
}

# -----------------------------------------------------------------------------
# Mode C: compare two snapshots against the real gate
# -----------------------------------------------------------------------------
# "The diff must be empty" stopped being the right gate once section 3 was
# rewritten: that legitimately drops four loop-scratch globals, and the harness
# itself now captures a new globalvec: row type. Eyeballing a diff that is
# expected to be non-empty is how a real regression gets waved through.
#
# The gate that actually matters is narrower and checkable:
#   NO path value may CHANGE. Keys may disappear only if they are declared
#   expendable; new keys are fine.
#
#   Rscript R/checks/70_path_snapshot.R --compare pre.tsv post.tsv
if (has("--compare")) {
  i <- match("--compare", args)
  f1 <- args[i + 1L]; f2 <- args[i + 2L]
  if (is.na(f1) || is.na(f2)) stop("usage: --compare <pre.tsv> <post.tsv>")
  a <- read.delim(f1, colClasses = "character")
  b <- read.delim(f2, colClasses = "character")

  # Loop scratch from the old section 3. Verified to have no downstream reader:
  # files_local has none at all; files_s3 is assigned locally where used;
  # folder_path only ever appears as a data.frame column; local_dir only as a
  # named function argument.
  EXPENDABLE <- paste0("global:", c("files_local", "files_s3", "folder_path", "local_dir"))

  m <- merge(a, b, by = "key", all = TRUE, suffixes = c(".pre", ".post"))
  changed <- m[!is.na(m$value.pre) & !is.na(m$value.post) & m$value.pre != m$value.post, ]
  dropped <- m[!is.na(m$value.pre) & is.na(m$value.post), ]
  added   <- m[is.na(m$value.pre) & !is.na(m$value.post), ]
  bad_drop <- dropped[!dropped$key %in% EXPENDABLE, ]

  say("pre=", nrow(a), " rows   post=", nrow(b), " rows")
  say("changed=", nrow(changed), "  dropped=", nrow(dropped),
      " (", nrow(bad_drop), " unexpected)  added=", nrow(added))

  if (nrow(changed)) {
    cat("\nCHANGED VALUES - this is the failure condition:\n")
    for (j in seq_len(nrow(changed))) {
      cat(sprintf("  %s\n    pre : %s\n    post: %s\n",
                  changed$key[j], changed$value.pre[j], changed$value.post[j]))
    }
  }
  if (nrow(bad_drop)) {
    cat("\nUNEXPECTEDLY DROPPED:\n")
    for (k in bad_drop$key) cat("  ", k, "\n", sep = "")
  }
  if (nrow(added)) {
    cat("\nAdded (informational, not a failure):\n")
    for (k in head(added$key, 40)) cat("  ", k, "\n", sep = "")
    if (nrow(added) > 40) cat("  ... and ", nrow(added) - 40, " more\n", sep = "")
  }

  if (nrow(changed) == 0L && nrow(bad_drop) == 0L) {
    cat("\n")
    say("PASS - no path value changed, no unexpected key disappeared")
    quit(save = "no", status = 0L)
  }
  cat("\n")
  say("FAIL")
  quit(save = "no", status = 1L)
}

# -----------------------------------------------------------------------------
# Mode B: full setup snapshot on this host
# -----------------------------------------------------------------------------
out <- val("--out")
if (is.null(out)) {
  stop("usage: 70_path_snapshot.R --out <file> | --resolver-only [--check|--write-golden]")
}

say("sourcing R/0_server_setup.R (creates directories if absent; downloads are skip-if-exists)")
setwd(repo_root)
source(file.path(repo_root, "R", "0_server_setup.R"))
say("setup complete; cwd is now ", getwd())

rows <- list()
add <- function(key, value) {
  rows[[length(rows) + 1L]] <<- data.frame(
    key = key,
    value = if (is.null(value) || length(value) == 0L) "<unset>" else as.character(value)[1],
    stringsAsFactors = FALSE
  )
}

# Layer 1 + 2 in one pass. Sweep globalenv() for every length-1 character and
# every logical flag, rather than naming them. This is the guarantee: a new
# entry in `subdirs` shows up here automatically.
# This script's own variables live in the same globalenv as setup's, so they
# would otherwise be swept up and reported as "changed paths" - `out` differs
# between any two runs by construction, which would fail the gate every time.
HARNESS_INTERNAL <- c("args", "out", "repo_root", "f1", "f2", "has", "val", "ts",
                      "say", "write_tsv", "add", "rows", "golden", "ids", "srcs",
                      "script", "kv", "exp", "bad", "m", "df", "con", "j", "k")

g <- globalenv()
for (n in sort(setdiff(ls(g, all.names = TRUE), HARNESS_INTERNAL))) {
  v <- tryCatch(get(n, envir = g), error = function(e) NULL)
  if (is.character(v) && length(v) == 1L && !is.na(v)) {
    add(paste0("global:", n), v)
  } else if (is.character(v) && length(v) > 1L && length(v) <= 32L && !anyNA(v)) {
    # Short character VECTORS are path declarations too - geo_files_local (3)
    # and glw_files (8) are read by 27 and 3 other scripts respectively. A
    # scalar-only sweep silently misses them, which would let the gate pass
    # while a load-bearing vector changed underneath it.
    add(paste0("globalvec:", n), paste(v, collapse = " | "))
  } else if (is.logical(v) && length(v) == 1L && !is.na(v)) {
    add(paste0("flag:", n), as.character(v))
  }
}

# Layer 3: the nested atlas_dirs structure, flattened.
if (exists("atlas_dirs", envir = g)) {
  ad <- unlist(get("atlas_dirs", envir = g))
  for (n in sort(names(ad))) add(paste0("atlas_dirs:", n), ad[[n]])
}
add("cwd:after_setup", getwd())

df <- do.call(rbind, rows)
df <- df[!duplicated(df$key), ]
df <- df[order(df$key), ]
df$dir_exists <- vapply(df$value, function(p) {
  if (p == "<unset>") return("na")
  if (dir.exists(p)) "yes" else if (file.exists(p)) "file" else "no"
}, character(1))
rownames(df) <- NULL

write_tsv(df, out)
say("wrote ", nrow(df), " rows to ", out)
