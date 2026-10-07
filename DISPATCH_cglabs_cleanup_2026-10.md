# Dispatch: S3 + node clean-up pass — retained backups, parked sets, the retired 2015 key (2026-10-07)

**Status:** Block A is read-only and runnable now. **Blocks B and C delete and are GO-gated.** Do not
start either without its GO line filled in here. Block D is macbook work.

**Append your response at the top of this file** as `## cglabs response — <summary> (<date>, <sha>)`,
newest first; commit, push, verify it landed (`git log origin/develop..HEAD` empty).

**Why.** Three threads finished and left copies behind on purpose. Each is now superseded by a
verified live product:
- the R/3 res-25 re-bake, `archive/`-bound `DISPATCH_cglabs_r3_res25_rerun.md`;
- the family keys, `DISPATCH_cglabs_family_keys.md`, Block C GO'd 2026-10-01 and deferred to this pass;
- the item-2 exposure pass, `archive/dispatches/DISPATCH_cglabs_exposure_intld_fixes.md`.

Pete released all of it into one pass (2026-10-01 and 2026-10-07).

**Safety.** `s3://digital-atlas` is versioned: noncurrent versions are kept at least 270 days, two
newest always. Every S3 delete here leaves a recoverable version. **Node deletes in Block C have no
undo.** That is why C comes last and is gated separately.

**Out of scope, do not touch:**
- `sandbox/backup/issue9_20260916_135730` and `issue9_20260925_041056`;
- any `Data/_parked_*` not named below;
- anything outside `sandbox/backup/` on S3, except the one 2015 key.

**Stop at every gate.** If anything deviates from an expectation, stop and describe it.

---

## Block A — inventory (read-only, minutes; paste everything)

```bash
cd <hazards_prototype> && git fetch origin && git pull --ff-only && git log -1 --oneline
pgrep -af Rscript || echo "no Rscript running"
Rscript -e '
  suppressPackageStartupMessages(library(s3fs)); suppressMessages(suppressWarnings(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")))
  b <- "s3://digital-atlas/sandbox/backup/"
  targets <- paste0(b, c("issue9_20260928_142229", "issue9_20260928_142507", "issue9_20260928_142537", "issue9_20260928_142557", "issue9_20260930_120740", "issue9_20261006_195255"))
  cat("== S3 backup prefixes to delete ==\n")
  for (t in targets) { f <- tryCatch(s3_dir_ls(t, recurse = TRUE, type = "file"), error = function(e) character(0)); sz <- if (length(f)) sum(s3_file_info(f)$size, na.rm = TRUE) else 0
    cat(sprintf("%-60s %4d objects %8.1f MB\n", sub(b, "", t), length(f), sz / 1e6)) }
  cat("\n== all prefixes under sandbox/backup (for the record; only the six above go) ==\n"); print(basename(s3_dir_ls(b)))
  k <- "s3://digital-atlas/domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/variable=vop_nominal-usd-2015.parquet"
  cat("\n== retired key ==\n", sub("s3://digital-atlas/", "", k), "exists:", s3_file_exists(k), if (s3_file_exists(k)) sprintf("size %d", s3_file_info(k)$size) else "", "\n")
  cat("\n== node parked dirs (working_dir:", getwd(), ") ==\n")
  for (d in file.path("Data", c("_parked_intld_fixes_20261004_173417", "_parked_usd_rebake_20261006_091428", "_parked_issue9"))) {
    if (!dir.exists(d)) { cat(sprintf("%-50s ABSENT\n", d)); next }
    f <- list.files(d, recursive = TRUE, full.names = TRUE); cat(sprintf("%-50s %6d files %8.1f GB  newest %s\n", d, length(f), sum(file.size(f)) / 1e9, format(max(file.mtime(f)), "%Y-%m-%d"))) }
  cat("\n== every Data/_parked_* present (only the three above go) ==\n"); print(list.files("Data", "^_parked_"))'
# nothing live may point into what goes
grep -rnE 'sandbox/backup/issue9_2026(0928|0930|1006)|_parked_(intld_fixes|usd_rebake|issue9)|vop_nominal-usd-2015\.parquet' R/ scripts/ metadata/ --include='*.R' --include='*.py' --include='*.json' --include='*.yaml' | grep -v '^R/archive' | head -20 || echo "no code/metadata references"
df -h <common_data mount>
```
**Expect:**
- No Rscript running.
- Six backup prefixes, each with more than 0 objects (paste counts and MB).
- The retired key exists.
- The three parked dirs are present or `ABSENT` (an absent one is fine; say which).
- **No code or metadata reference** to anything that goes, except documentation and comments.

Paste whatever `grep` prints: a `.R` or `.json` hit outside a comment is a STOP. Paste everything and
**STOP**.

---

## Block B — S3 deletes (GO-gated; background; minutes)

**GO line (Pete fills in):** `GO B: ______ (date)`

```bash
cd <hazards_prototype>; STAMP=$(date +%Y%m%d_%H%M%S); echo $STAMP > logs/cleanup_stamp.txt
cat > logs/cleanup_B_$STAMP.R <<'RS'
suppressPackageStartupMessages(library(s3fs)); suppressMessages(suppressWarnings(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")))
ts <- function() format(Sys.time(), "%F %T")
b <- "s3://digital-atlas/sandbox/backup/"
targets <- paste0(b, c("issue9_20260928_142229", "issue9_20260928_142507", "issue9_20260928_142537", "issue9_20260928_142557", "issue9_20260930_120740", "issue9_20261006_195255"))
for (t in targets) {
  f <- tryCatch(s3_dir_ls(t, recurse = TRUE, type = "file"), error = function(e) character(0))
  cat(sprintf("[%s] %s: deleting %d objects\n", ts(), sub(b, "", t), length(f))); flush.console()
  if (length(f)) s3_file_delete(f)
  left <- tryCatch(s3_dir_ls(t, recurse = TRUE, type = "file"), error = function(e) character(0))
  cat(sprintf("[%s] %s: left %d\n", ts(), sub(b, "", t), length(left))); flush.console()
}
# family-keys Block C (GO'd 2026-10-01): retire vop_nominal-usd-2015 - backup first, then delete, then prove it is gone
key <- "s3://digital-atlas/domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/variable=vop_nominal-usd-2015.parquet"
bak <- sub("^s3://digital-atlas/", paste0(b, "retired_", format(Sys.time(), "%Y%m%d_%H%M%S"), "/"), key)
stopifnot(s3_file_exists(key)); tmp <- tempfile(fileext = ".parquet"); s3_file_download(key, tmp)
s3_file_upload(tmp, bak, ACL = "public-read", overwrite = TRUE); stopifnot(s3_file_exists(bak))
cat(sprintf("[%s] backup %s size %d\n", ts(), bak, s3_file_info(bak)$size))
s3_file_delete(key); cat(sprintf("[%s] deleted %s | still exists? %s\n", ts(), key, s3_file_exists(key)))
cat(sprintf("[%s] ===== DONE\n", ts()))
RS
nohup Rscript logs/cleanup_B_$STAMP.R > logs/cleanup_B_$STAMP.log 2>&1 &
# when DONE:
cat logs/cleanup_B_$STAMP.log | grep -v '^\s*$' | tail -20
curl -sI "https://digital-atlas.s3.amazonaws.com/domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/variable=vop_nominal-usd-2015.parquet" | head -1
```
**Expect:**
- Each of the six prefixes reports `left 0`.
- The 2015 key is backed up under `sandbox/backup/retired_<stamp>/`, at the size Block A printed.
- `still exists? FALSE`, then `HTTP/1.1 404` (or 403, which S3 returns for a missing public key).
- `===== DONE`, and no R error.

Paste it and **STOP**.

---

## Block C — node parked dirs (GO-gated; NO UNDO)

**GO line (Pete fills in):** `GO C: ______ (date)`

```bash
cd <hazards_prototype>; STAMP=$(cat logs/cleanup_stamp.txt)
Rscript -e '
  suppressMessages(suppressWarnings(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")))
  for (d in file.path("Data", c("_parked_intld_fixes_20261004_173417", "_parked_usd_rebake_20261006_091428", "_parked_issue9"))) {
    if (!dir.exists(d)) { cat(d, "ABSENT\n"); next }
    stopifnot(grepl("^Data/_parked_", d)); n <- length(list.files(d, recursive = TRUE))
    unlink(d, recursive = TRUE); cat(sprintf("%-50s removed %d files | still exists? %s\n", d, n, dir.exists(d))) }
  cat("Data/_parked_* now:", paste(list.files("Data", "^_parked_"), collapse = ", "), "\n")' 2>&1 | tee logs/cleanup_C_$STAMP.log
df -h <common_data mount>
```
**Expect:**
- Each dir reports `still exists? FALSE`, or `ABSENT`.
- The remaining `Data/_parked_*` list contains only directories this dispatch did not name.
- Free space has gone up by about Block A's GB total.

Paste it and **STOP**.

---

## Block D — macbook

Close #23, which was a defect in the retired 2015 object. Record the retirement in the `R/checks`
headers. `git mv` `DISPATCH_cglabs_r3_res25_rerun.md`, `DISPATCH_cglabs_family_keys.md` and this file
to `archive/dispatches/`, with index rows. Update the handover. Nothing for the node.
