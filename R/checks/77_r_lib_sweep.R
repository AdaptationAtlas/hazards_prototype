#!/usr/bin/env Rscript
# =============================================================================
# 77_r_lib_sweep.R - how much of a whole R library loads on THIS host?
# (issue #58: size the PASCAL R 4.5.1 -> 4.6.1 breakage before asking IT)
#
#   Rscript R/checks/77_r_lib_sweep.R --lib /usr/local/lib/R/site-library --workers 6 --out logs/x.tsv
#
# 76_r_env_audit.R answers "do the packages THE PIPELINE needs load". This
# answers "how much of the library is broken for EVERY user", which is the
# claim an IT ticket makes.
#
# Each package in --lib is loaded in its own fresh
# `Rscript --no-environ --no-init-file` with R_LIBS_USER pointed at a path
# that does not exist, so the per-user library cannot mask a broken site
# build. What loads is what a plain account on this host sees. Failures are
# classified from the error text:
#   direct    the package's OWN shared object fails (e.g. undefined symbol SETLENGTH)
#   via:<pkg> another package's shared object fails while loading it (the root)
#   dep       a dependency is not installed
#   other     anything else (first line reported)
# Also tallies the library by the R version each package was Built under and
# by NeedsCompilation.
#
# READ-ONLY: installs nothing, sources no pipeline script, writes only --out.
# =============================================================================

args <- commandArgs(TRUE)
val <- function(f, default = NULL) {
  i <- match(f, args)
  if (is.na(i) || i == length(args)) default else args[i + 1L]
}
lib <- val("--lib", "/usr/local/lib/R/site-library")
workers <- as.integer(val("--workers", "4"))
if (!dir.exists(lib)) stop("no such library: ", lib)

`%||%` <- function(a, b) if (is.null(a) || length(a) == 0L || is.na(a)) b else a
t0 <- Sys.time()
logm <- function(...) cat(sprintf("[%s] +%6.1fs | %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
                                  as.numeric(difftime(Sys.time(), t0, units = "secs")),
                                  paste0(...)))

ip <- installed.packages(lib.loc = lib, fields = c("Built", "NeedsCompilation"))
pk <- unname(ip[, "Package"])
built <- sub("^R ([0-9]+\\.[0-9]+).*", "\\1", ip[, "Built"])
comp <- ifelse(is.na(ip[, "NeedsCompilation"]), "no", ip[, "NeedsCompilation"])
logm("R ", R.version$major, ".", R.version$minor, "; library has ", length(pk), " package(s); ",
     workers, " worker(s)")
bt <- table(built = built, compiled = factor(comp, levels = c("yes", "no")))
for (b in rownames(bt)) logm(sprintf("  built under R %-4s  compiled %4d  pure-R %4d", b,
                                     bt[b, "yes"], bt[b, "no"]))

rscript <- file.path(R.home("bin"), "Rscript")
nouser <- file.path(tempdir(), "no-user-library")
probe <- function(p) {
  code <- sprintf("suppressPackageStartupMessages(library(%s, character.only = TRUE)); cat('LOADOK')", deparse(p))
  o <- suppressWarnings(system2(rscript, c("--no-environ", "--no-init-file", "-e", shQuote(code)),
                                stdout = TRUE, stderr = TRUE, timeout = 120,
                                env = c(paste0("R_LIBS_USER=", nouser), "R_LIBS_SITE=")))
  o <- paste(o, collapse = " ")
  if (grepl("LOADOK", o, fixed = TRUE)) return(c("OK", "", ""))
  so <- regmatches(o, regexec("unable to load shared object '[^']*/([A-Za-z0-9.]+)/libs/", o))[[1]]
  sym <- regmatches(o, regexec("undefined symbol: ([A-Za-z0-9_]+)", o))[[1]]
  cls <- if (length(so) == 2L) {
    if (so[2] == p) "direct" else paste0("via:", so[2])
  } else if (grepl("there is no package called", o, fixed = TRUE)) "dep" else "other"
  c("BROKEN", cls, substr(gsub("\\s+", " ", if (length(sym) == 2L) paste("undefined symbol:", sym[2]) else o), 1, 160))
}
res <- parallel::mclapply(pk, probe, mc.cores = workers, mc.preschedule = FALSE)
res <- do.call(rbind, res)
tab <- data.frame(package = pk, built = built, compiled = comp, status = res[, 1],
                  cause = res[, 2], detail = gsub(path.expand("~"), "~", res[, 3], fixed = TRUE),
                  row.names = NULL, stringsAsFactors = FALSE)

br <- tab[tab$status == "BROKEN", ]
logm("loaded OK ", sum(tab$status == "OK"), "   BROKEN ", nrow(br), "   (of ", nrow(tab), ")")
logm("  direct (own shared object fails): ", sum(br$cause == "direct"))
logm("  via another package's shared object: ", sum(startsWith(br$cause, "via:")))
logm("  missing dependency: ", sum(br$cause == "dep"), "   other: ", sum(br$cause == "other"))
d <- br[br$cause == "direct", ]
if (nrow(d)) {
  logm("ROOTS (direct failures) and the symbol each lacks:")
  for (i in seq_len(nrow(d))) logm(sprintf("  %-22s built R %-4s %s", d$package[i], d$built[i], d$detail[i]))
}
v <- table(sub("^via:", "", br$cause[startsWith(br$cause, "via:")]))
if (length(v)) {
  logm("knock-on failures by root:")
  for (r in names(sort(v, decreasing = TRUE))) logm(sprintf("  %-22s %d package(s)", r, v[[r]]))
}
syms <- table(sub("^undefined symbol: ", "", d$detail[grepl("^undefined symbol", d$detail)]))
if (length(syms)) logm("undefined symbols among roots: ", paste(sprintf("%s=%d", names(syms), as.integer(syms)), collapse = "  "))
o <- br[br$cause %in% c("dep", "other"), ]
for (i in seq_len(min(nrow(o), 40L))) logm(sprintf("  %-6s %-22s %s", o$cause[i], o$package[i], o$detail[i]))

if (!is.null(val("--out"))) {
  utils::write.table(tab, val("--out"), sep = "\t", quote = FALSE, row.names = FALSE)
  logm("wrote ", val("--out"))
}
