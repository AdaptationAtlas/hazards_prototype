#!/usr/bin/env Rscript
# =============================================================================
# 76_r_env_audit.R - which R packages does the pipeline need, and do they load
# on THIS host? (issue #58, PASCAL after its R 4.5.1 -> 4.6.1 upgrade)
#
#   Rscript R/checks/76_r_env_audit.R                    # default scope, table to stdout
#   Rscript R/checks/76_r_env_audit.R --out logs/x.tsv   # also write the table
#   Rscript R/checks/76_r_env_audit.R --scan-only        # static scan, load nothing
#   Rscript R/checks/76_r_env_audit.R --files 'R/1_*.R'  # another scope (glob, repo-relative)
#
# 1. STATIC SCAN of the scope's R files (parse data, not grep): library(),
#    require(), requireNamespace(), loadNamespace(), pacman::p_load() including
#    p_load(<vector>, character.only = TRUE) and p_load(char = <vector>) where
#    <vector> is assigned c("...") in the same file, and every pkg:: / pkg:::.
# 2. LOAD TEST: each package in its own fresh `Rscript --no-init-file` so one
#    broken .so cannot poison the next, with a timeout. Status per package:
#      OK       loads; version and the library it loaded from are reported
#      BROKEN   installed but library() fails (first error line reported)
#      MISSING  not installed in any .libPaths() entry
#
# READ-ONLY. It installs nothing, sources no pipeline script (R/0_server_setup.R
# would run pacman::p_load, which INSTALLS what is missing), and writes only the
# optional --out file.
#
# Default scope: R/0.4*.R, plus what they need sourced first (R/0_server_setup.R,
# R/haz_functions.R, R/00_paths.R, R/00_acquire.R), and hazards_upstream/R/**.
# =============================================================================

args <- commandArgs(TRUE)
has <- function(f) f %in% args
val <- function(f, default = NULL) {
  i <- match(f, args)
  if (is.na(i) || i == length(args)) default else args[i + 1L]
}

repo_root <- local({
  fa <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  d <- if (length(fa)) dirname(normalizePath(sub("^--file=", "", fa[1]), mustWork = FALSE)) else getwd()
  for (. in 1:5) {
    if (file.exists(file.path(d, "metadata", "data.json"))) return(d)
    d <- dirname(d)
  }
  stop("cannot locate repo root from ", getwd())
})

ts <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
t0 <- Sys.time()
logm <- function(...) cat(sprintf("[%s] +%6.1fs | %s\n", ts(),
                                  as.numeric(difftime(Sys.time(), t0, units = "secs")),
                                  paste0(...)))

# ---- scope ------------------------------------------------------------------
globs <- if (!is.null(val("--files"))) strsplit(val("--files"), ",")[[1]] else
  c("R/0.4*.R", "R/0_server_setup.R", "R/haz_functions.R", "R/00_paths.R", "R/00_acquire.R")
files <- unique(unlist(lapply(globs, function(g) Sys.glob(file.path(repo_root, g)))))
if (is.null(val("--files"))) {
  files <- c(files, list.files(file.path(repo_root, "hazards_upstream", "R"), pattern = "\\.[Rr]$",
                               recursive = TRUE, full.names = TRUE))
}
if (!length(files)) stop("no files in scope")
rel <- sub(paste0("^", repo_root, "/"), "", files)

# ---- 1. static scan -----------------------------------------------------------
LOADERS <- c("library", "require", "requireNamespace", "loadNamespace", "p_load")
scan_file <- function(f) {
  pd <- tryCatch(utils::getParseData(parse(f, keep.source = TRUE)), error = function(e) NULL)
  if (is.null(pd)) return(list(pkgs = character(0), parse_error = TRUE))
  pd <- pd[pd$terminal | pd$token == "expr", ]
  out <- pd$text[pd$token == "SYMBOL_PACKAGE"]                          # pkg:: and pkg:::
  # string vectors assigned in this file: name <- c("a", "b")
  src <- paste(readLines(f, warn = FALSE), collapse = "\n")
  vec <- list()
  m <- gregexpr("([A-Za-z.][A-Za-z0-9._]*)\\s*(<-|=)\\s*c\\(([^)]*)\\)", src, perl = TRUE)
  for (hit in regmatches(src, m)[[1]]) {
    nm <- sub("^([A-Za-z.][A-Za-z0-9._]*).*", "\\1", hit, perl = TRUE)
    s <- regmatches(hit, gregexpr("[\"']([A-Za-z][A-Za-z0-9.]*)[\"']", hit))[[1]]
    if (length(s)) vec[[nm]] <- unique(c(vec[[nm]], gsub("[\"']", "", s)))
  }
  calls <- which(pd$token == "SYMBOL_FUNCTION_CALL" & pd$text %in% LOADERS)
  full <- utils::getParseData(parse(f, keep.source = TRUE))
  for (i in calls) {
    # the call expression is the grandparent of the function-name token
    p1 <- full$parent[full$id == pd$id[i]]
    call_id <- full$parent[full$id == p1]
    txt <- utils::getParseText(full, call_id)
    inner <- sub("^[^(]*\\((.*)\\)\\s*$", "\\1", gsub("\n", " ", txt))
    char_only <- grepl("character\\.only\\s*=\\s*(T|TRUE)", inner) || grepl("^\\s*char\\s*=", inner)
    parts <- trimws(strsplit(inner, ",")[[1]])
    parts <- parts[!grepl("=", parts) | grepl("^char\\s*=", parts)]
    parts <- sub("^char\\s*=\\s*", "", parts)
    for (p in parts) {
      if (grepl("^[\"'][A-Za-z][A-Za-z0-9.]*[\"']$", p)) out <- c(out, gsub("[\"']", "", p))
      else if (grepl("^[A-Za-z.][A-Za-z0-9._]*$", p)) {
        if (char_only || pd$text[i] %in% c("requireNamespace", "loadNamespace")) {
          out <- c(out, vec[[p]])                                     # a variable holding names
        } else out <- c(out, p)                                       # bare symbol: the package
      }
    }
  }
  list(pkgs = unique(out[nzchar(out)]), parse_error = FALSE)
}

scans <- lapply(files, scan_file)
names(scans) <- rel
bad_parse <- rel[vapply(scans, `[[`, logical(1), "parse_error")]
use <- stack(lapply(scans, `[[`, "pkgs"))
names(use) <- c("package", "file")
use$file <- as.character(use$file)
pk <- sort(unique(use$package))
pk <- pk[!pk %in% c("base")]
logm("scope: ", length(files), " file(s); ", length(pk), " distinct package(s)",
     if (length(bad_parse)) paste0("; parse failed: ", paste(bad_parse, collapse = ", ")) else "")

used_by <- vapply(pk, function(p) {
  f <- unique(use$file[use$package == p])
  paste0(length(f), ":", paste(head(basename(f), 3), collapse = ";"), if (length(f) > 3) ";..." else "")
}, character(1))

# phase B needs the 0.4 chain; phase C needs hazards_upstream
scope <- vapply(pk, function(p) {
  f <- unique(use$file[use$package == p])
  up <- startsWith(f, "hazards_upstream/")
  if (all(up)) "upstream" else if (any(up)) "both" else "0.4-chain"
}, character(1))

if (has("--scan-only")) {
  tab <- data.frame(package = pk, scope = scope, used_by = used_by, row.names = NULL)
  old <- options(width = 250)
  print(tab, right = FALSE, row.names = FALSE)
  options(old)
  quit(save = "no", status = 0L)
}

# ---- 2. load test -------------------------------------------------------------
logm("R ", R.version$major, ".", R.version$minor, "  ", R.version$platform)
logm(".libPaths(): ", paste(.libPaths(), collapse = "  |  "))
rscript <- file.path(R.home("bin"), "Rscript")
probe <- function(p) {
  code <- sprintf(paste0(
    "r <- tryCatch({suppressPackageStartupMessages(library(%s, character.only = TRUE)); 'OK'},",
    " error = function(e) paste('ERR', gsub('[\\r\\n\\t]+', ' ', conditionMessage(e))));",
    "if (r == 'OK') cat('OK', as.character(utils::packageVersion('%s')), dirname(find.package('%s')), sep = '\\t') else cat(r)"),
    deparse(p), p, p)
  o <- suppressWarnings(system2(rscript, c("--no-init-file", "-e", shQuote(code)),
                                stdout = TRUE, stderr = FALSE, timeout = 120))
  o <- paste(o, collapse = " ")
  installed <- nzchar(system.file(package = p))
  if (startsWith(o, "OK\t")) {
    f <- strsplit(o, "\t")[[1]]
    return(c("OK", f[2], f[3], ""))
  }
  c(if (installed) "BROKEN" else "MISSING",
    if (installed) as.character(utils::packageDescription(p)$Version) else "",
    if (installed) dirname(system.file(package = p)) else "",
    substr(gsub(path.expand("~"), "~", sub("^ERR ", "", o), fixed = TRUE), 1, 160))
}
res <- t(vapply(pk, function(p) { r <- probe(p); cat("."); r }, character(4)))
cat("\n")
tab <- data.frame(package = pk, status = res[, 1], scope = scope, version = res[, 2], lib = res[, 3],
                  used_by = used_by, error = res[, 4], row.names = NULL)
tab <- tab[order(match(tab$status, c("BROKEN", "MISSING", "OK")), tab$package), ]

lib_label <- function(l) {
  n <- normalizePath(l, mustWork = FALSE)
  ifelse(l == "", "", ifelse(startsWith(n, normalizePath("~")), "user",
         ifelse(n == normalizePath(.Library), "base", "site")))
}
tab$lib <- lib_label(tab$lib)
cnt <- table(factor(tab$status, levels = c("OK", "BROKEN", "MISSING")))
logm("OK ", cnt[["OK"]], "   BROKEN ", cnt[["BROKEN"]], "   MISSING ", cnt[["MISSING"]],
     "   (of ", nrow(tab), ")")
for (sc in c("0.4-chain", "both", "upstream")) {
  x <- tab[tab$scope == sc, ]
  logm(sprintf("  %-9s OK %d  BROKEN %d  MISSING %d", sc, sum(x$status == "OK"),
               sum(x$status == "BROKEN"), sum(x$status == "MISSING")))
}
old <- options(width = 250)
print(tab, right = FALSE, row.names = FALSE)
options(old)
if (!is.null(val("--out"))) {
  utils::write.table(tab, val("--out"), sep = "\t", quote = FALSE, row.names = FALSE)
  logm("wrote ", val("--out"))
}
