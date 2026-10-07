# Dispatch: S3 + node clean-up pass — retained backups, parked sets, the retired 2015 key (2026-10-07)

## macbook response — Block A accepted; GO B (six prefixes + 2015 key); four 0520xx backups stay; C held (2026-10-07)

Clean inventory, and thanks for flagging the 0520xx prefixes.
- **Grep hits:** accepted as not blocking. `R/checks/19` and `24` read a local `ISSUE19_CACHE` copy
  and already stop with a "retired" message. `park_stale_ensemble_stacks.R` only creates the
  `_parked_issue9` path.
- **GO B** covers exactly the six named prefixes and the 2015 key, nothing more. The four
  `issue9_20261006_0520xx` prefixes (Block D's pre-overwrite copies) **stay**, as do the May
  `202605*` prefixes and the two named out-of-scope ones.
- **Block C is HELD:** the three node parked dirs (132.6 GB) are kept for now.

Run Block B as written, paste, and STOP.

---

## cglabs response — Block A inventory complete: six prefixes 12 objects / 417.5 MB, retired key present (2.25 MB), three parked dirs 132.6 GB; grep has three `.R` hits outside comments (none read what goes) — STOP for ruling + GO B (2026-10-07, 4e91373)

**Block A run as written, read-only.** `git pull` ff to 4e91373. No Rscript running.

**S3 backup prefixes to delete (six named):**
```
issue9_20260928_142229     1 objects      8.7 MB
issue9_20260928_142507     2 objects     17.3 MB
issue9_20260928_142537     1 objects      7.1 MB
issue9_20260928_142557     2 objects      2.7 MB
issue9_20260930_120740     3 objects    191.2 MB
issue9_20261006_195255     3 objects    190.5 MB
                          12 objects    417.5 MB total
```
All six > 0 objects, as expected.

**All prefixes under `sandbox/backup/` (for the record; only the six go):**
```
20260526_121951  20260527_073937  20260529_180100  20260530_154522
issue9_20260916_135730  issue9_20260925_041056                      (named out of scope)
issue9_20260928_142229  issue9_20260928_142507  issue9_20260928_142537  issue9_20260928_142557   (go)
issue9_20260930_120740  issue9_20261006_195255                                                   (go)
issue9_20261006_052021  issue9_20261006_052040  issue9_20261006_052104  issue9_20261006_052133   (NOT in the dispatch)
```
Note for the record: the four `issue9_20261006_0520xx` prefixes are the Block D publish backups
(reference + family, both resolutions, 2026-10-06 morning). The dispatch neither lists them to go nor
names them out of scope. Not touching them; say if they belong in B.

**Retired key:** `domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/variable=vop_nominal-usd-2015.parquet`
**exists: TRUE, size 2,248,114 bytes.**

**Node parked dirs** (working_dir = `common_data/nex-gddp-cimp6_hazards`):
```
Data/_parked_intld_fixes_20261004_173417       332 files     0.7 GB   newest 2026-10-05
Data/_parked_usd_rebake_20261006_091428       1608 files     4.0 GB   newest 2026-09-30
Data/_parked_issue9                          17546 files   127.9 GB   newest 2026-09-15
                                                           132.6 GB total
```
`Data/_parked_*` present: exactly those three — nothing else would remain after C.

**Reference grep** (`R/ scripts/ metadata/`, `*.R *.py *.json *.yaml`, excluding `R/archive`) — hits:
```
R/checks/19_exposure_vs_gyga_kenya.R:279   # comment: variable=vop_nominal-usd-2015.parquet is RETIRED from S3 …
R/checks/19_exposure_vs_gyga_kenya.R:285   f_usd15 <- file.path(cache, "vop_nominal-usd-2015.parquet")
R/checks/19_exposure_vs_gyga_kenya.R:286   if (!file.exists(f_usd15)) stop("… retired from S3 and not cached at …")
R/checks/24_severity_sensitivity.R:118     # comment: … retired from S3 (no producer, unmaintained vintage, #23)
R/checks/24_severity_sensitivity.R:120     if (!file.exists(file.path(cache, "vop_nominal-usd-2015.parquet"))) stop("… retired from S3 and not in ISSUE19_CACHE …")
R/checks/24_severity_sensitivity.R:121     den <- as.data.table(read_parquet(file.path(cache, "vop_nominal-usd-2015.parquet")))[
R/park_stale_ensemble_stacks.R:9           # comment: parks into Data/_parked_issue9/<STAMP>/hazard_timeseries_int/<tf>/
R/park_stale_ensemble_stacks.R:69          park <- file.path(dirname(int_root), "_parked_issue9", STAMP, "hazard_timeseries_int", tf)
```
No hits in `scripts/` or `metadata/`; no `.py`/`.json`/`.yaml` hits. No hit for any of the six backup
prefixes, `_parked_intld_fixes`, or `_parked_usd_rebake`.

The gate says a `.R` hit outside a comment is a STOP, so stopping here as written. Assessment of the
five code-line hits:
- **19 L285–286 and 24 L120–121** reference the *local* `ISSUE19_CACHE` copy of
  `vop_nominal-usd-2015.parquet`, never the S3 key; both scripts were rewritten 2026-09-26 to treat the
  key as already retired and to `stop()` when the cache is absent. Deleting the S3 key changes nothing
  for them. (Block D's "record the retirement in the R/checks headers" is these two files.)
- **park_stale L69** *builds* the `Data/_parked_issue9/<STAMP>/…` path and `dir.create()`s it when it
  parks stale ensemble stacks; it never reads from it. Removing `Data/_parked_issue9` in C does not
  break it; a future run would simply recreate the directory.
Nothing live reads what goes. Your call whether that counts as "documentation and comments".

**Mount:** `common_data` NFS mount: size 192 T, used 70 T, available 123 T (37 % used). C would return
~132.6 GB.

**Not started:** Block B (`GO B:` blank) and Block C (`GO C:` blank). Waiting.

---


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

**GO line (Pete fills in):** `GO B: Pete Steward, 2026-10-07 (the six named prefixes + the 2015 key only)`

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

**GO line (Pete fills in):** `GO C: HELD (Pete, 2026-10-07) — node parked dirs are kept for now`

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
