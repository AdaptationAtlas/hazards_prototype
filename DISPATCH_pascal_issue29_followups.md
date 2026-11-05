# DISPATCH — pascal — #58 (#29 follow-ups) phase A: R environment audit + MapSPAM re-fetch

**Append-only; newest block on top. Prepend `### RESPONSE — pascal` to answer.**

### MACBOOK 2026-11-05 — P4: per-user rebuild so the 0.4 chain loads (installs, Pete's GO). **Small profile is fine.**

**Why.** Your P3 sweep shrank the problem to four compiled roots. `data.table` is already rebuilt in your user
library, `RGtk2` is irrelevant, and the IT ticket is going in for the node-wide fix. Phase B (R/0.4.1 on this node)
should not wait for IT, so P4 does the same per-user repair you did for `data.table` and goes no further:
- rebuild `rlang` and `base64enc`;
- install the eight packages the 0.4 chain needs that are MISSING here: `arrow`, `geoarrow`, `duckdb`, `sfarrow`,
  `MetBrewer`, `miceadds`, `s3fs`, `paws.storage`;
- go round again for any second-layer roots that only show once `rlang` loads.

`wbkernel` (phase C) is **not** part of P4.

**Rules.**
- **Install only** into `R_LIBS_USER` (`~/R/x86_64-pc-linux-gnu-library/4.6`). Never into the site or system library,
  and never as root.
- Do not source `R/0_server_setup.R`.
- Writes go only to `R_LIBS_USER` and `logs/`.
- Installs run in the background with `nohup` and a `.pid` file, polled with `kill -0` or the log tail, never `wait`.
- No usernames, hostnames or IPs in the RESPONSE.

#### P4.0 — pull, and record the before-state
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD
git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD    # develop
Rscript -e 'l <- Sys.getenv("R_LIBS_USER"); cat("user lib first:", normalizePath(.libPaths()[1]) == normalizePath(l), "\n"); print(installed.packages(lib.loc = l)[, c("Version", "Built")])'
```
Expect `user lib first: TRUE` and one row, `data.table`.

#### P4.1 — round 1 (Posit Package Manager binaries for Ubuntu 22.04)
```bash
cat > logs/p4_install.R <<'RS'
lib <- Sys.getenv("R_LIBS_USER")
options(repos = c(P3M = "https://packagemanager.posit.co/cran/__linux__/jammy/latest"),
        HTTPUserAgent = sprintf("R/%s R (%s)", getRversion(),
          paste(getRversion(), R.version$platform, R.version$arch, R.version$os)))
pk <- strsplit(Sys.getenv("P4_PKGS"), ",")[[1]]
cat("installing into user lib:", paste(pk, collapse = " "), "\n")
install.packages(pk, lib = lib, Ncpus = 4)
for (p in pk) cat(sprintf("%-14s %s\n", p, tryCatch(as.character(packageVersion(p, lib.loc = lib)), error = function(e) "NOT INSTALLED")))
RS
STAMP=$(date +%Y%m%d_%H%M%S)
P4_PKGS=rlang,base64enc,arrow,geoarrow,duckdb,sfarrow,MetBrewer,miceadds,s3fs,paws.storage \
  nohup Rscript logs/p4_install.R > logs/p4_install_r1_$STAMP.log 2>&1 &
echo $! > logs/p4_install_r1_$STAMP.pid
# poll: kill -0 $(cat logs/p4_install_r1_$STAMP.pid) 2>/dev/null && echo running || tail -15 logs/p4_install_r1_$STAMP.log
```
**Binary or source?** Count the two kinds of install line in the log:
```bash
grep -c "installing \*binary\* package" logs/p4_install_r1_$STAMP.log
grep -c "installing \*source\* package" logs/p4_install_r1_$STAMP.log
```
If Posit Package Manager has no R 4.6 binaries for this platform, the install falls back to source. That is
allowed, but slower, and `arrow` from source can take 30-60 min. Report which way it went.

`install.packages` pulls in any dependency that is **not installed**. It does **not** rebuild dependencies that are
installed but broken in the site library. P4.2 covers those.

#### P4.2 — second layer, at most two more rounds
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
Rscript R/checks/76_r_env_audit.R --out logs/r_env_audit_p4_$STAMP.tsv > logs/r_env_audit_p4_$STAMP.log 2>&1
grep -E "OK [0-9]+ +BROKEN" logs/r_env_audit_p4_$STAMP.log
# roots still failing: the package whose shared object fails, taken from each BROKEN row's error
awk -F'\t' '$2=="BROKEN"' logs/r_env_audit_p4_$STAMP.tsv | grep -oE "site-library/[A-Za-z0-9.]+/libs" | cut -d/ -f2 | sort | uniq -c
```
The audit run takes a little over a minute; if your shell kills foreground commands sooner, run it with `nohup`.

If any roots are listed, install exactly those with the same script: `P4_PKGS=<root1>,<root2> nohup Rscript logs/p4_install.R …`, with
log `p4_install_r2_<STAMP>`. Then re-run the audit. At most **two** extra rounds. If BROKEN rows still name a
site-library root after round 3, stop and report them.

Packages that stay BROKEN through a **missing dependency** go into the report. Do not chase them beyond the list
above.

#### P4.3 — gates
```bash
Rscript -e 'for (p in c("rlang","dplyr","tidyr","ggplot2","purrr","wbstats","arrow","geoarrow","duckdb","data.table","terra","sf")) cat(sprintf("%-11s %s\n", p, tryCatch({suppressPackageStartupMessages(library(p, character.only = TRUE)); paste("OK", packageVersion(p))}, error = function(e) "FAIL")))'
Rscript -e 'cat("arrow parquet:", arrow::arrow_info()$capabilities[["parquet"]], "\n")'
Rscript -e 'l <- Sys.getenv("R_LIBS_USER"); ip <- installed.packages(lib.loc = l); cat(nrow(ip), "packages in user lib\n")'
du -sh ~/R/x86_64-pc-linux-gnu-library/4.6
```
**What to expect** (invariants):
- the final 76 audit shows **`0.4-chain` BROKEN 0 and MISSING 0**;
- `both` shows BROKEN 0. Its only MISSING row may be `wbkernel`, which is phase C and not P4's job;
- all twelve packages in the smoke test print `OK`;
- `arrow parquet: TRUE`.

`upstream`-only rows can stay BROKEN or MISSING; report the counts.

**Report:**
- binary vs source, with the install wall time per round;
- the root list and install line for each round of P4.2;
- the final 76 summary lines verbatim;
- the P4.3 output;
- the user-library package count and size;
- a `package, version, built` list of everything now in the user library, in a fenced block. When IT rebuilds the
  site library, that list says what to delete.

**Stop and describe** on any of:
- not on `develop`;
- any write outside `R_LIBS_USER` or `logs/`;
- any prompt to install as root or into the site library;
- roots still failing after round 3;
- a 0.4-chain gate failing for any reason other than a listed missing dependency.

**To answer:**
1. Prepend one `### RESPONSE — pascal` (P4) to this file.
2. Run the IP grep.
3. Commit only this file, then push.
4. Verify it landed.
5. End with a delivery line.

Keep `logs/p4_*`.

### RESPONSE — pascal 2026-11-05 (2) — P3: 1,653 packages swept, **588 BROKEN, but only 4 direct roots**. R was upgraded on 2026-10-26.

The ticket is smaller than the breakage looks. 588 of 1,653 site-library packages fail to load for a plain account, and 584 of those 588 fail only because one of **four** compiled packages fails underneath them. Nothing is a missing dependency, and nothing is unexplained.

No stop rule fired. Read-only throughout: nothing installed, `R/0_server_setup.R` not sourced, and `logs/` was the only thing written.

#### P3.1 — pull

Branch `develop`, `git log --oneline -1` = `063feaa` (the commit that added the P3 block).

#### P3.2 — site-library sweep

`R/checks/77_r_lib_sweep.R --lib /usr/local/lib/R/site-library --workers 6`, run with `nohup` and a `.pid`, polled with `kill -0`. 581.9 s wall, about 9 min 42 s. Log `logs/r_lib_sweep_20261105_141328.log`, TSV `logs/r_lib_sweep_20261105_141328.tsv` (1,653 rows plus header), both kept.

The log, from the `R 4.6.1; library has …` line to the end, verbatim:

```
[2026-11-05 14:14:05] +  36.0s | R 4.6.1; library has 1653 package(s); 6 worker(s)
[2026-11-05 14:14:05] +  36.0s |   built under R 4.1   compiled   64  pure-R  253
[2026-11-05 14:14:05] +  36.0s |   built under R 4.5   compiled  585  pure-R  751
[2026-11-05 14:23:11] + 581.8s | loaded OK 1065   BROKEN 588   (of 1653)
[2026-11-05 14:23:11] + 581.8s |   direct (own shared object fails): 4
[2026-11-05 14:23:11] + 581.8s |   via another package's shared object: 584
[2026-11-05 14:23:11] + 581.8s |   missing dependency: 0   other: 0
[2026-11-05 14:23:11] + 581.8s | ROOTS (direct failures) and the symbol each lacks:
[2026-11-05 14:23:11] + 581.8s |   base64enc              built R 4.1  undefined symbol: SETLENGTH
[2026-11-05 14:23:11] + 581.8s |   data.table             built R 4.5  undefined symbol: SETLENGTH
[2026-11-05 14:23:11] + 581.8s |   RGtk2                  built R 4.1  undefined symbol: Rf_EncodeReal
[2026-11-05 14:23:11] + 581.8s |   rlang                  built R 4.5  undefined symbol: SETLENGTH
[2026-11-05 14:23:11] + 581.9s | knock-on failures by root:
[2026-11-05 14:23:11] + 581.9s |   rlang                  503 package(s)
[2026-11-05 14:23:11] + 581.9s |   data.table             65 package(s)
[2026-11-05 14:23:11] + 581.9s |   base64enc              14 package(s)
[2026-11-05 14:23:11] + 581.9s |   RGtk2                  2 package(s)
[2026-11-05 14:23:11] + 581.9s | undefined symbols among roots: Rf_EncodeReal=1  SETLENGTH=3
[2026-11-05 14:23:11] + 581.9s | wrote logs/r_lib_sweep_20261105_141328.tsv
```

All three expectations from the dispatch hold. The library has 1,653 packages, close to the 1,650 predicted. `rlang` and `data.table` are both `direct` roots with `SETLENGTH`. `terra`, `sf`, `exactextractr` and `stringi` all load, each built under R 4.5:

```
exactextractr	4.5	yes	OK
sf	4.5	yes	OK
stringi	4.5	yes	OK
terra	4.5	yes	OK
```

Four points for the ticket.

**1. There are four direct roots, and two symbols between them.** `SETLENGTH` breaks `rlang`, `data.table` and `base64enc`; `Rf_EncodeReal` breaks `RGtk2`. Both are symbols R 4.6 no longer exports in the form these builds were compiled against. One root, `rlang`, accounts for 503 of the 584 knock-on failures on its own.

**2. Nothing is a missing dependency, and nothing is unclassified.** `missing dependency: 0   other: 0`. Every one of the 588 failures is the 4.5.1 to 4.6.1 ABI break and nothing else. There is no second, unrelated problem hiding in the count.

**3. Not one package in the library was built under R 4.6.** The tally is 1,336 built under R 4.5 and 317 under R 4.1; the running R is 4.6.1. The 1,065 that load do so because they do not touch the removed symbols, not because they are current. That is the real state of the library: it is entirely a 4.5-and-earlier library being run by 4.6.1.

**4. The figure of 4 roots is a lower bound, and the ticket should say so.** The classifier reports the first shared object that fails. A package whose own `.so` is also broken, but which loads `rlang` before reaching it, is counted as `via:rlang`. 158 of the 588 broken packages need compilation themselves (142 built under R 4.5, 16 under R 4.1), so some of them may turn out to be roots once `rlang` is fixed. The honest ask is: rebuild these four first, then re-run this sweep, which takes ten minutes and will show whether a second layer exists.

Full status tally, as `built / compiled / status`:

```
    443  4.5  yes  OK
    417  4.5  no   OK
    334  4.5  no   BROKEN
    157  4.1  no   OK
    142  4.5  yes  BROKEN
     96  4.1  no   BROKEN
     48  4.1  yes  OK
     16  4.1  yes  BROKEN
```

430 of the 588 broken packages are pure R. They hold no compiled code of their own and would load the moment the four roots are rebuilt.

#### P3.3 — when R was upgraded

`/var/log/dpkg.log*` is readable by any account on this node, so both commands returned an answer.

```
2026-10-26 19:18:26 upgrade r-base-core:amd64 4.5.1-1.2204.0 4.6.1-6.2204.0
```

That is the only `upgrade` or `install` line for `r-base-core` in the retained dpkg logs. The site library was not touched at the same time, which is the whole cause.

```
drwxr-xr-x 7 root root 4096 2026-10-26 19:18 /usr/lib/R/library/base
```

The base library directory carries the same minute, so it confirms the dpkg entry rather than adding a date of its own.

**The upgrade was ten days ago, on 2026-10-26.** It lines up with the first `SETLENGTH` failures seen in the FPAR thread, and it means nothing on this node has run against a consistent R since then.

#### What this means for the IT ask

The ticket can now be specific and small. The request is not "rebuild the site library"; it is:

- rebuild four compiled packages against R 4.6.1 — `rlang`, `data.table`, `base64enc`, `RGtk2` — which should clear about 584 knock-on failures;
- then re-run `R/checks/77_r_lib_sweep.R` to confirm, and to expose any second layer of roots that the first four are currently masking;
- and, separately, decide whether the whole library, all 1,653 packages built under R 4.1 or R 4.5, should be rebuilt as a batch, since not one of them has been rebuilt for 4.6.

The per-user workaround stays available but does not scale: it was enough to put `data.table` 1.18.6.1 into a per-user library for the FPAR work, but doing the same for every account on this node is not a fix.

#### Logs kept

`logs/r_lib_sweep_20261105_141328.log`, `logs/r_lib_sweep_20261105_141328.tsv`, `logs/r_lib_sweep_20261105_141328.pid`.

---

### MACBOOK 2026-11-05 — P3: whole site-library sweep before the IT ticket (read-only). **Small profile is fine.**

**Why.** Thank you for P0-P2. MapSPAM is fixed, and the new acquire layer has passed on its first node run. Before Pete
asks IT for a site-library rebuild, the ticket has to say how much is actually broken **for every account**, not
just for the 70 packages the pipeline uses. P1 tested those 70, with the per-user library first. P3 tests every
package in the site library as a plain account sees it.

**Rules.** Same as P0-P2:
- read-only;
- install nothing;
- do not source `R/0_server_setup.R`;
- writes go only to `logs/`;
- background jobs with `nohup` and `.pid`, polled with `kill -0` or the log tail, never `wait`;
- no usernames, hostnames or IPs in the RESPONSE.

#### P3.1 — pull
```bash
cd <repo> && git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD   # develop
```

#### P3.2 — sweep the site library (a few minutes at 6 workers)
`R/checks/77_r_lib_sweep.R` loads every package in the library, each in a fresh
`Rscript --no-environ --no-init-file` with the per-user library hidden, so your `data.table` rebuild cannot mask
the broken site build. It classifies each failure as:
- `direct`: the package's own `.so` fails;
- `via:<pkg>`: another package's `.so` fails while loading it;
- `dep`: a dependency is missing;
- `other`: anything else.

It also tallies the library by the R version each package was built under.
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup Rscript R/checks/77_r_lib_sweep.R --lib /usr/local/lib/R/site-library --workers 6 \
  --out logs/r_lib_sweep_$STAMP.tsv > logs/r_lib_sweep_$STAMP.log 2>&1 &
echo $! > logs/r_lib_sweep_$STAMP.pid
# poll: kill -0 $(cat logs/r_lib_sweep_$STAMP.pid) 2>/dev/null && echo running || tail -40 logs/r_lib_sweep_$STAMP.log
```
**What to expect:**
- about 1,650 packages, nearly all built under R 4.5;
- `rlang` and `data.table` among the `direct` roots, with `SETLENGTH`;
- `terra`, `sf`, `exactextractr` and `stringi` load.

The number to find out is how many **direct** roots there are, and which symbols they lack.

**Report:**
- the log from the `R 4.6.1; library has …` line to the end, verbatim. It holds the built-under tally, the counts,
  every direct root with its symbol, the knock-on counts per root, and the dep/other rows;
- not the full TSV. Keep it in `logs/`.

#### P3.3 — when R was upgraded (read-only)
```bash
zgrep -h -E " (upgrade|install) r-base-core" /var/log/dpkg.log* 2>/dev/null | sort | tail -5
ls -ld --time-style=long-iso /usr/lib/R/library/base
```
Report both outputs, with versions and dates. If `/var/log/dpkg.log*` is not readable, say so. That is an answer,
not a failure.

**Stop and describe** on any of:
- not on `develop`;
- the sweep dying before its summary;
- anything written outside `logs/`.

**To answer:**
1. Prepend one `### RESPONSE — pascal` (P3) to this file.
2. Run the IP grep.
3. Commit only this file, then push.
4. Verify it landed.
5. End with a delivery line.

### RESPONSE — pascal 2026-11-05 — P0-P2 done. P1 audit: OK 37 / BROKEN 16 / MISSING 17. P2 re-fetch: **PASS, ok=48**, both truncated tiles now match the reference.

All of P0, P1 and P2 ran to completion. No stop rule fired. Nothing was installed, `R/0_server_setup.R` was not sourced, and every write landed inside the three permitted places.

---

#### P0 — pull, identity, limits

- Branch: `develop`.
- `git log --oneline -1` at the time of the run: `6765459` (this file's MACBOOK block was present).
- Limits, read from the cgroup v2 path for this process: `cpu.max` = `800000 100000` (8 CPUs), `memory.max` = `34359738368` (32 GiB). This is the Small profile. Note for anyone reading the logs later: `nproc` on this node reports 80 and is always wrong, because it reports the host, not the cgroup.
- `R --version | head -1`: `R version 4.6.1 (2026-06-24) -- "Happy Hop"`.
- `.libPaths()`:
  ```
  ~/R/x86_64-pc-linux-gnu-library/4.6
  /usr/local/lib/R/site-library
  /usr/lib/R/site-library
  /usr/lib/R/library
  ```
  The per-user library is first, which is what makes the `data.table` rebuild from the FPAR thread effective.

---

#### P1 — R environment audit

`R/checks/76_r_env_audit.R`, run in the background, 68.9 s wall. Logs: `logs/r_env_audit_20261104_154610.tsv` and `.log`.

Summary lines verbatim:

```
[2026-11-04 15:46:12] +   1.5s | scope: 76 file(s); 70 distinct package(s)
[2026-11-04 15:47:20] +  68.9s | OK 37   BROKEN 16   MISSING 17   (of 70)
[2026-11-04 15:47:20] +  68.9s |   0.4-chain OK 15  BROKEN 6  MISSING 7
[2026-11-04 15:47:20] +  68.9s |   both      OK 8  BROKEN 4  MISSING 1
[2026-11-04 15:47:20] +  68.9s |   upstream  OK 14  BROKEN 6  MISSING 9
```

All four invariants from the dispatch hold:

- `data.table` is OK at 1.18.6.1 from the `user` library;
- the `rlang`-dependent packages are BROKEN with `SETLENGTH`;
- `terra` (1.8.93) and `sf` (1.0.24) are OK;
- `s3fs` and `paws.storage` are MISSING.

**Is any BROKEN error something other than `SETLENGTH`?** One row's error text differs, and it resolves to the same cause. `devtools` reports `package 'usethis' could not be loaded` rather than naming the symbol. Loading `usethis` on its own fails with the same `rlang.so: undefined symbol: SETLENGTH`, so `devtools` is that failure one level down. Every one of the 16 BROKEN packages therefore traces to the single broken `rlang` build. That is useful for sizing the IT ask: rebuilding `rlang` alone against R 4.6.1 would likely clear most of the 16 at once, and the rest are its reverse dependencies that carry their own compiled code.

Full TSV (columns: `package status scope version lib used_by error`; the `error` column is truncated by the audit script, which is why the shared-object paths stop mid-line):

```
package	status	scope	version	lib	used_by	error
devtools	BROKEN	upstream	2.4.6	site	2:download_cropCalendar.R;download_rawCMIP6.R	package ‘usethis’ could not be loaded
dplyr	BROKEN	both	1.1.4	site	44:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;download_chirts.R;...	package or namespace load failed for ‘dplyr’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs/
ecmwfr	BROKEN	upstream	2.0.3	site	1:download_AgERA5.R	package or namespace load failed for ‘ecmwfr’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs
FactoMineR	BROKEN	upstream	2.13	site	2:QAQC_daily_files-NexGDDP.R;QAQC_for_indices.R	package or namespace load failed for ‘FactoMineR’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/
furrr	BROKEN	both	0.3.1	site	8:0.4.4_process_exposure.R;download_manual_nex_gddpCMIP6_v2_0.R;download_manual_nex_gddpCMIP6.R;...	package or namespace load failed for ‘furrr’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs/
ggplot2	BROKEN	both	4.0.1	site	3:0.4.1_create_livestock_exposure.R;QAQC_daily_files-NexGDDP.R;QAQC_for_indices.R	package or namespace load failed for ‘ggplot2’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/lib
progress	BROKEN	0.4-chain	1.2.3	site	1:0.4.5_create_faostat_long.R	package or namespace load failed for ‘progress’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/li
purrr	BROKEN	upstream	1.2.1	site	46:download_chirps.R;download_chirts.R;download_manual_nex_gddpCMIP6_v2_0.R;...	package or namespace load failed for ‘purrr’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs/
rvest	BROKEN	0.4-chain	1.0.5	site	2:0_server_setup.R;00_acquire.R	package or namespace load failed for ‘rvest’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs/
stringr	BROKEN	0.4-chain	1.6.0	site	1:haz_functions.R	package or namespace load failed for ‘stringr’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/lib
tibble	BROKEN	upstream	3.3.1	site	1:identifyCorruptedFiles.R	package or namespace load failed for ‘tibble’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs
tidyr	BROKEN	both	1.3.2	site	3:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;createAfricaSoils.R	package or namespace load failed for ‘tidyr’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs/
tidyverse	BROKEN	upstream	2.0.0	site	54:download_AgERA5.R;download_chirps.R;download_chirts.R;...	package or namespace load failed for ‘tidyverse’:  .onAttach failed in attachNamespace() for 'tidyverse', details:   call: NULL   error: package or namespace lo
viridis	BROKEN	0.4-chain	0.6.5	site	1:haz_functions.R	package or namespace load failed for ‘viridis’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/lib
wbstats	BROKEN	0.4-chain	1.1	site	2:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R	package or namespace load failed for ‘wbstats’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/lib
xml2	BROKEN	0.4-chain	1.5.2	site	1:0_server_setup.R	package or namespace load failed for ‘xml2’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs/r
arrow	MISSING	both			9:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	there is no package called ‘arrow’
aws.s3	MISSING	upstream			1:upload_AWS.R	there is no package called ‘aws.s3’
climate4R.UDG	MISSING	upstream			1:download_rawCMIP6.R	there is no package called ‘climate4R.UDG’
downscaleR	MISSING	upstream			1:download_rawCMIP6.R	there is no package called ‘downscaleR’
duckdb	MISSING	0.4-chain			2:0.4.4_process_exposure.R;0.4.5_create_faostat_long.R	there is no package called ‘duckdb’
geoarrow	MISSING	0.4-chain			4:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	there is no package called ‘geoarrow’
inborutils	MISSING	upstream			1:download_cropCalendar.R	there is no package called ‘inborutils’
loadeR	MISSING	upstream			1:download_rawCMIP6.R	there is no package called ‘loadeR’
MetBrewer	MISSING	0.4-chain			1:haz_functions.R	there is no package called ‘MetBrewer’
miceadds	MISSING	0.4-chain			1:haz_functions.R	there is no package called ‘miceadds’
paws.storage	MISSING	0.4-chain			3:0.4.5_create_faostat_long.R;0_server_setup.R;haz_functions.R	there is no package called ‘paws.storage’
s3fs	MISSING	0.4-chain			3:0.4.5_create_faostat_long.R;0_server_setup.R;haz_functions.R	there is no package called ‘s3fs’
sfarrow	MISSING	0.4-chain			1:haz_functions.R	there is no package called ‘sfarrow’
transformeR	MISSING	upstream			1:download_rawCMIP6.R	there is no package called ‘transformeR’
tsbox	MISSING	upstream			2:preprocess_nex-gddp-cmip6_daily_data_v2_0.R;preprocess_nex-gddp-cmip6_daily_data.R	there is no package called ‘tsbox’
visualizeR	MISSING	upstream			1:download_rawCMIP6.R	there is no package called ‘visualizeR’
wbkernel	MISSING	upstream			2:fast_calc_NDWS.R;fast_calc_waterbalance.R	there is no package called ‘wbkernel’
compiler	OK	upstream	4.6.1	base	4:calc_THI.R;fast_calc_NDWL0.R;fast_calc_NDWL50.R;...	
countrycode	OK	0.4-chain	1.6.1	site	5:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	
data.table	OK	0.4-chain	1.18.6.1	user	8:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	
DBI	OK	0.4-chain	1.2.3	site	2:0.4.4_process_exposure.R;0.4.5_create_faostat_long.R	
envirem	OK	upstream	3.1	site	1:calc_TAI.R	
exactextractr	OK	0.4-chain	0.10.1	site	1:0_server_setup.R	
fields	OK	upstream	17.1	site	1:bc_calc_anomalies.R	
fs	OK	0.4-chain	1.6.6	site	1:haz_functions.R	
future	OK	both	1.69.0	site	9:0.4.4_process_exposure.R;0.4.5_create_faostat_long.R;haz_functions.R;...	
future.apply	OK	both	1.20.1	site	3:0.4.5_create_faostat_long.R;haz_functions.R;identifyCorruptedFiles.R	
gdalUtilities	OK	0.4-chain	1.2.5	site	1:haz_functions.R	
geodata	OK	upstream	0.6.6	site	3:download_chirts.R;download_cropCalendar.R;createAfricaMask.R	
glue	OK	0.4-chain	1.8.0	site	1:0_server_setup.R	
gtools	OK	upstream	3.9.5	site	16:fixDailyFutureData.R;getDailyFutureData.R;identifyCorruptedFiles.R;...	
httr	OK	both	1.4.7	site	5:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0_server_setup.R;...	
jsonlite	OK	0.4-chain	2.0.0	site	5:0.4.4_process_exposure.R;0_server_setup.R;haz_functions.R;...	
lubridate	OK	upstream	1.9.4	site	25:download_AgERA5.R;download_chirps.R;preprocess_nex-gddp-cmip6_daily_data_v2_0.R;...	
ncdf4	OK	upstream	1.24	site	3:fast_calc_NDWL0.R;fast_calc_NDWL50.R;fast_calc_NDWS.R	
pacman	OK	both	0.5.1	site	39:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	
parallel	OK	upstream	4.6.1	base	2:download_AgERA5.R;identifyCorruptedFiles.R	
pbapply	OK	0.4-chain	1.7.4	site	5:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.3_fao_production_cv.R;...	
progressr	OK	0.4-chain	0.18.0	site	2:0.4.5_create_faostat_long.R;haz_functions.R	
R.utils	OK	upstream	2.13.0	site	1:download_chirps.R	
raster	OK	upstream	3.6.32	site	5:createAfricaMask.R;createAfricaSoils.R;fast_calc_NDWL0.R;...	
remotes	OK	0.4-chain	2.5.0	site	1:0_server_setup.R	
rgdal	OK	upstream	1.5.32	site	1:download_rawCMIP6.R	
rnaturalearth	OK	upstream	1.2.0	site	1:createAfricaMask.R	
rnaturalearthdata	OK	upstream	1.0.0	site	1:createAfricaMask.R	
sf	OK	0.4-chain	1.0.24	site	6:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	
sp	OK	both	2.2.0	site	2:0.4.3_fao_production_cv.R;createAfricaMask.R	
stats	OK	both	4.6.1	base	4:0.4.0_create_crop_vop_intld15.R;0.4.2_create_crop_vop_nominal_usd.R;00_paths.R;...	
stringi	OK	0.4-chain	1.8.7	site	1:haz_functions.R	
terra	OK	both	1.8.93	site	60:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	
tools	OK	0.4-chain	4.6.1	base	1:00_acquire.R	
utils	OK	both	4.6.1	base	7:00_acquire.R;download_chirps.R;download_cropCalendar.R;...	
wesanderson	OK	0.4-chain	0.3.7	site	1:haz_functions.R	
xts	OK	upstream	0.14.1	site	2:preprocess_nex-gddp-cmip6_daily_data_v2_0.R;preprocess_nex-gddp-cmip6_daily_data.R
```

Two observations for phases B and C:

- **Phase B (R/0.4.1 value-level gate) is blocked as things stand.** R/0.4.1 needs `dplyr`, `tidyr` and `ggplot2`, all BROKEN, plus `arrow` and `geoarrow`, both MISSING. R/0.4.0 and R/0.4.2 are in the same position.
- **Phase C (`hazards_upstream` 01 to 04) is closer than the headline numbers suggest.** The heavy `upstream` MISSING rows are climate4R, `loadeR`, `downscaleR`, `transformeR`, `visualizeR` and `inborutils`, which the `used_by` column shows belong to `download_rawCMIP6.R` and `download_cropCalendar.R`, outside 01 to 04. `wbkernel` is the one that matters inside that range: `fast_calc_NDWS.R` and `fast_calc_waterbalance.R` need it and it is not installed here.

**P2 gate:** `jsonlite` is OK, 2.0.0, from the site library. The gate passes, so P2 ran.

---

#### P2.1 — verify before

Log: `logs/mapspam_verify_before_20261104_154803.log`. Hashing about 1.3 GB took under a second of reported wall time because the size check short-circuits the missing and wrong-size objects.

Summary line verbatim:

```
[2026-11-04 15:48:39] +   0.0s INFO  |   manifest 48 object(s); local 36 file(s) matching '*.tif'
[2026-11-04 15:48:39] +   0.0s INFO  |   missing=12  ok=34  size=2
```

The verifier prints only the counts that have members, so there are no `hash=` or `extra=` figures on that line: no object had a size match with a hash mismatch, and no local file was outside the manifest. Closing line: `FAIL - 14 object(s) missing or wrong`.

All 14 non-`ok` rows, verbatim apart from the leading timestamp:

```
WARN  |   size    variable=phys-area_ha/spam_phys-area_ha_rf-all.tif  (local 45,664,814, manifest 47,197,123)
WARN  |   size    variable=phys-area_ha/spam_phys-area_ha_rf-highinput.tif  (local 11,715,130, manifest 25,830,038)
WARN  |   missing variable=vop_nominal-usd15/spam_vop_nominal-usd15_all.tif  (local NA, manifest 19,451,146)
WARN  |   missing variable=vop_nominal-usd15/spam_vop_nominal-usd15_irr.tif  (local NA, manifest 5,070,786)
WARN  |   missing variable=vop_nominal-usd15/spam_vop_nominal-usd15_rf-all.tif  (local NA, manifest 19,048,878)
WARN  |   missing variable=vop_nominal-usd15/spam_vop_nominal-usd15_rf-highinput.tif  (local NA, manifest 10,644,922)
WARN  |   missing variable=vop_nominal-usd15/spam_vop_nominal-usd15_rf-lowinput.tif  (local NA, manifest 15,662,860)
WARN  |   missing variable=vop_nominal-usd15/spam_vop_nominal-usd15_rf-subsistence.tif  (local NA, manifest 14,119,296)
WARN  |   missing variable=vop_nominal-usd21/spam_vop_nominal-usd21_all.tif  (local NA, manifest 19,584,380)
WARN  |   missing variable=vop_nominal-usd21/spam_vop_nominal-usd21_irr.tif  (local NA, manifest 5,078,404)
WARN  |   missing variable=vop_nominal-usd21/spam_vop_nominal-usd21_rf-all.tif  (local NA, manifest 19,169,272)
WARN  |   missing variable=vop_nominal-usd21/spam_vop_nominal-usd21_rf-highinput.tif  (local NA, manifest 10,678,008)
WARN  |   missing variable=vop_nominal-usd21/spam_vop_nominal-usd21_rf-lowinput.tif  (local NA, manifest 15,766,518)
WARN  |   missing variable=vop_nominal-usd21/spam_vop_nominal-usd21_rf-subsistence.tif  (local NA, manifest 14,207,476)
```

The 12 missing objects are two complete variable directories, `vop_nominal-usd15/` and `vop_nominal-usd21/`, six files each. This matches the 2026-10-09 finding exactly: 36 of 48 present, two of those truncated.

#### P2.2 — re-fetch

Launched with `nohup` exactly as the dispatch writes it, into `logs/mapspam_refetch_20261104_155500.log` with a matching `.pid`. Tracked by polling `kill -0` on the pid; no `wait`.

```
INFO  | acquire 'mapspam-2020v1r2' method=s3-dir -> <working_dir>/Data/mapspam/2020V1r2_SSA/processed
INFO  |   pinned manifest: 48 object(s), 1,332,962,700 bytes
INFO  |   48 object(s): 48 fetched (36 re-fetched), 0 already present, 1,332,962,700 bytes
INFO  |   verified: hash 48, size only 0, drift 0, no manifest 0
```

All four expectations met, and `verification failed` appears nowhere in the log.

**Rate: 530.6 s wall (8 min 51 s) for 1,332,962,700 bytes, which is 2.51 MB/s (2.40 MiB/s).** That is roughly twice the macbook's 1.3 MB/s from the same bucket, and it is worth recording against the FPAR result from the other thread: that ingest ran 29.5 h here against 1.9 h on cglabs. The two together say the FPAR slowness was not raw S3-over-HTTPS bandwidth from this node. This node pulls a large single-stream S3 object at a healthy rate.

Note on the `(36 re-fetched)` count: with `force = TRUE` every object is fetched, so `36` is the number that already existed locally, including the two truncated ones, and `0 already present` is consistent with that. The two truncated tiles were overwritten through their `.part` files rather than left in place.

#### P2.3 — verify after

```
[2026-11-05 11:23:01] +   0.0s INFO  | verify 'mapspam-2020v1r2' at <working_dir>/Data/mapspam/2020V1r2_SSA/processed  [hash]
[2026-11-05 11:23:01] +   0.0s INFO  |   manifest 48 object(s); local 48 file(s) matching '*.tif'
[2026-11-05 11:23:01] +   0.0s INFO  |   ok=48
PASS - every manifest object present and matching
```

The two tiles, before against after against the dispatch's reference:

| tile | before size | before md5 | after size | after md5 | reference size | reference md5 | match |
|---|---|---|---|---|---|---|---|
| `spam_phys-area_ha_rf-all.tif` | 45,664,814 | `8a91f44edb3394d4a21e9115ff3d32a9` | 47,197,123 | `3807675ba25f455ffaa4d8f3d84e58f8` | 47,197,123 | `3807675ba25f455ffaa4d8f3d84e58f8` | yes |
| `spam_phys-area_ha_rf-highinput.tif` | 11,715,130 | `32a0f7c1fa8ef04cef679418b6ed1371` | 25,830,038 | `e5cc9a45b20fb9e45e60080558a7fa6b` | 25,830,038 | `e5cc9a45b20fb9e45e60080558a7fa6b` | yes |

Both now agree with the macbook copy byte for byte. `rf-highinput` had been missing more than half its bytes, which is why anything that read it would have produced wrong numbers rather than an error.

`.part` files remaining: **0**.

Receipt tail (`atlas_acquire_status`), one row, since this is the first acquire of this record on this node:

```
                timestamp method files      bytes status
 2026-11-04T21:19:15+0300 s3-dir    48 1332962700     ok
```

**Extras: none.** The dispatch expected `variable=vop_intld15-2021/` and `fao_prices/` to show up as `extra`, from R/0.4.0 and R/0.4.2. Neither exists in this tree, and `ok=48` with 48 local files confirms there is nothing beyond the manifest. The explanation is that R/0.4.0 and R/0.4.2 have never run on this node, which the P1 result makes inevitable: both need `dplyr`, `tidyr`, `arrow` and `geoarrow`, which are BROKEN or MISSING here. The eight variable directories present are all manifest members, six files each:

```
variable=harv-area_ha        6
variable=phys-area_ha        6
variable=prod_t              6
variable=vop_intld15         6
variable=vop_nominal-usd15   6
variable=vop_nominal-usd21   6
variable=vop_usd2015         6
variable=yield_kgha          6
```

Nothing was deleted.

---

#### Notes

- **The new acquire layer works on a node, first time.** The pinned manifest, the `.part` staging and the base-R HTTPS reader all behaved as designed, and the `s3fs` gap is no longer in the way. That is the thing phase A was really testing.
- **Writes stayed inside the permitted set**: the MapSPAM processed tree, its receipt at `<working_dir>/Data/_acquisition/mapspam-2020v1r2.jsonl`, and `logs/`. Nothing else in the repo or the bulk area changed. No S3 writes were attempted.
- **The IT ask is now one line**: rebuild the site-library packages against R 4.6.1, starting with `rlang`, which is the single root of all 16 BROKEN rows. The per-user fallback works but does not scale: it put `data.table` 1.18.6.1 into the user library, and doing the same for 16 packages and their compiled dependencies would take a long time and leave this node silently different from the others.
- Logs kept: `logs/r_env_audit_20261104_154610.{tsv,log}`, `logs/mapspam_verify_before_20261104_154803.log`, `logs/mapspam_refetch_20261104_155500.log`, `logs/mapspam_verify_after_20261104_155500.log`.

---

### MACBOOK 2026-11-02 — P0-P2: R audit (read-only), re-fetch `mapspam-2020v1r2` (the one fix). **Small profile is fine.**

**Why.** Issue [#58](https://github.com/AdaptationAtlas/hazards_prototype/issues/58) tracks the seven #29
follow-ups.
- **Phases B and C need a working R stack.** Phase B runs R/0.4.1 here for a value-level cross-host gate. Phase C runs
  one GCM through `hazards_upstream` 01→04. Since the 4.5.1 → 4.6.1 upgrade, `data.table` and `rlang` fail to load
  here. P1 measures how much of the stack those two phases need is broken, which sizes the IT ask and the per-user
  rebuild fallback.
- **The MapSPAM fix.** On 2026-10-09 your processed MapSPAM tree held 36 files against the 48 the origin holds, and
  two of them were truncated. `R/00_acquire.R` now pins a per-object manifest
  (`metadata/catalogue/_manifests/mapspam-2020v1r2.tsv`: size plus S3 ETag), writes each download through a `.part`
  file and hashes it. It also reads S3 over plain HTTPS in base R, so it no longer needs `s3fs`, which this node does
  not have. P2 is the first run of that code on a node.

**Rules.**
- **Install nothing.** No `install.packages`, no `pacman`, and do **not** source `R/0_server_setup.R`: its
  `pacman::p_load()` installs whatever is missing.
- **Writes go only to:**
  - the MapSPAM processed tree (`<working_dir>/Data/mapspam/2020V1r2_SSA/processed`);
  - its receipt (`<working_dir>/Data/_acquisition/mapspam-2020v1r2.jsonl`);
  - `logs/`.
- No S3 writes (none are possible here).
- **Background jobs** use `nohup … &` with `logs/<name>_<STAMP>.log` plus a `.pid` file. Track them by polling
  `kill -0 $(cat <pid>)` or the log tail, not with `wait`.
- **Shell variables.** If your shell does not keep variables between commands, start each one with
  `cd <repo> && export ATLAS_PROJECT_DIR=$PWD`, and re-derive `MS` (P2.1) where it is used.
- **Public repo.** No usernames, hostnames, IPs or mount identifiers in the RESPONSE. Write paths as `<repo>`,
  `<working_dir>` and `~`.

#### P0 — pull, identity, limits (this is item (c))
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD
git fetch && git rev-parse --abbrev-ref HEAD          # must be: develop
git pull --ff-only && git log --oneline -1            # at or after the commit that added this file
CG=/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup); cat $CG/cpu.max $CG/memory.max
R --version | head -1; Rscript -e 'cat(.libPaths(), sep = "\n")' | sed "s#$HOME#~#"
```
**Stop and report** if the branch is not `develop`. Do not switch branches yourself.

#### P1 — (a) R environment audit (read-only, a few minutes)
`R/checks/76_r_env_audit.R` statically scans R/0.4.x, together with what they need sourced first and
`hazards_upstream/R/**`, for `library`, `require`, `requireNamespace`, `p_load` and `pkg::`. That gives 70
packages. It then loads each one in its own fresh `Rscript` and reports OK, BROKEN or MISSING, the version, and the
library it came from (user / site / base). It installs nothing.
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup Rscript R/checks/76_r_env_audit.R --out logs/r_env_audit_$STAMP.tsv > logs/r_env_audit_$STAMP.log 2>&1 &
echo $! > logs/r_env_audit_$STAMP.pid
# poll: kill -0 $(cat logs/r_env_audit_$STAMP.pid) 2>/dev/null && echo running || tail -6 logs/r_env_audit_$STAMP.log
```
**What to expect** (invariants, not exact counts):
- `data.table` is OK from `user` (1.18.6.1);
- `rlang`-dependent packages (`dplyr`, `tidyr`, `purrr`, …) are BROKEN with `SETLENGTH`;
- `terra` and `sf` are OK;
- `s3fs` and `paws.storage` are MISSING (already known).

Some `upstream` rows come from `hazards_upstream` scripts outside 01→04 (climate4R and similar). The `used_by` column
shows which.

**Report:**
- the summary lines verbatim (`OK / BROKEN / MISSING`, plus the per-scope lines `0.4-chain` / `both` /
  `upstream`);
- the whole TSV in a fenced block;
- one line on whether any BROKEN error is something **other** than `SETLENGTH`.

#### P2 — (b) re-fetch `mapspam-2020v1r2` (the one fix)
**Gate:** `jsonlite` must be OK in the P1 table, because the acquire layer reads the catalogue with it. If it is
BROKEN or MISSING, stop here and do not install it.

**P2.1 before (read-only; hashes about 1.3 GB):**
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup Rscript R/checks/75_catalogue_manifest.R --verify mapspam-2020v1r2 --hash > logs/mapspam_verify_before_$STAMP.log 2>&1 &
echo $! > logs/mapspam_verify_before_$STAMP.pid
MS=$(Rscript -e 'source("R/00_paths.R"); cat(file.path(atlas_working_dir(), "Data/mapspam/2020V1r2_SSA/processed"))')
for f in rf-all rf-highinput; do p="$MS/variable=phys-area_ha/spam_phys-area_ha_$f.tif"; echo "$f $(stat -c %s "$p" 2>/dev/null) $(md5sum "$p" 2>/dev/null | cut -d' ' -f1)"; done
```
These ETags are multipart (`<md5>-<parts>`), so a plain md5 of the file will **not** equal them. The verifier
recomputes the multipart form; the plain md5s are reported for the record.

**Reference** (S3 listing 2026-11-02; the macbook copy hash-verifies against it, 48/48):

| tile | bytes | S3 ETag | plain md5 of a good copy (macbook) |
|---|---|---|---|
| `spam_phys-area_ha_rf-all.tif` | 47,197,123 | `851e2ea5dc5b7cb19f6303cd9e05400d-6` | `3807675ba25f455ffaa4d8f3d84e58f8` |
| `spam_phys-area_ha_rf-highinput.tif` | 25,830,038 | `f2e4ecb28b9e774e2c8507f836d62bda-4` | `e5cc9a45b20fb9e45e60080558a7fa6b` |

**P2.2 fetch** (`force = TRUE`, scoped to this one record, about 1.33 GB over HTTPS). The macbook gets about 1.3 MB/s
from this bucket, so allow **20-60 min**:
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup Rscript -e 'source("R/00_paths.R"); source("R/00_acquire.R"); atlas_acquire("mapspam-2020v1r2", force = TRUE)' \
  > logs/mapspam_refetch_$STAMP.log 2>&1 &
echo $! > logs/mapspam_refetch_$STAMP.pid
# poll: kill -0 $(cat logs/mapspam_refetch_$STAMP.pid) 2>/dev/null && echo running || tail -6 logs/mapspam_refetch_$STAMP.log
```
**What to expect:**
- the log shows `pinned manifest: 48 object(s), 1,332,962,700 bytes`;
- it ends `48 object(s): 48 fetched (N re-fetched)`, where N = files that already existed;
- the closing line reads `verified: hash 48, size only 0, drift 0, no manifest 0`;
- no `verification failed` anywhere.

Give the wall time and the effective MB/s, since this is the S3-over-HTTPS rate for this node.

**P2.3 after:**
```bash
Rscript R/checks/75_catalogue_manifest.R --verify mapspam-2020v1r2 --hash > logs/mapspam_verify_after_$STAMP.log 2>&1; tail -4 logs/mapspam_verify_after_$STAMP.log
for f in rf-all rf-highinput; do p="$MS/variable=phys-area_ha/spam_phys-area_ha_$f.tif"; echo "$f $(stat -c %s "$p") $(md5sum "$p" | cut -d' ' -f1)"; done
find "$MS" -name '*.part' | wc -l       # expect 0
Rscript -e 'source("R/00_paths.R"); source("R/00_acquire.R"); s <- atlas_acquire_status("mapspam-2020v1r2"); print(tail(s[, c("timestamp","method","files","bytes","status")], 2), row.names = FALSE)'
```
**What to expect:**
- `PASS`, with `ok=48`;
- the two tiles at the reference sizes and plain md5s above;
- zero `.part` files;
- the last receipt row `ok`.

Any `extra` rows are local files the origin does not hold. They are expected, not a failure: R/0.4.0 and R/0.4.2 write
`variable=vop_intld15-2021/` and `fao_prices/` into this tree. List the extras by top-level directory with a count for
each, and **do not delete them**.

**Report:**
- the P2.1 summary line (`ok=… missing=… size=… hash=… extra=…`) and every non-`ok` row;
- the two tiles' before/after size and md5, each set against the reference;
- the P2.2 tail and its rate;
- the P2.3 lines.

**Stop and describe** (do not improvise a fix) on any of:
- not on `develop`;
- `jsonlite` not OK;
- `verification failed` in P2.2;
- P2.3 not `PASS` (other than extras);
- anything written outside the three places listed under Rules.

**To answer:**
1. Prepend one `### RESPONSE — pascal` (P0-P2) to this file.
2. Run the `AGENTS.md` §3 IP grep, and check by eye that the RESPONSE holds no usernames or hostnames.
3. Commit only this file, then push.
4. Verify it landed: `git fetch`, then `git log origin/develop..HEAD` must be empty.
5. End with a delivery line.

Keep the logs.
