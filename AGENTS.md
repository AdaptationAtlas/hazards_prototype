## Project instructions for coding agents

R project for a climate-hazards processing pipeline (Africa Agriculture Adaptation Atlas).

**Start here:** the newest **session** handover at the repository root, `HANDOVER_<date>.md` —
current state, what is in flight, and what to pick up next. Then `R/NEXT_FULL_REBAKE.md` if you
are touching R/2 or R/3, and `README.md` for the science pipeline.

Two handover shapes, distinguished by the filename:
- `HANDOVER_<date>.md` — **session** handover. The entry point. One current at a time.
- `HANDOVER_<date>_<topic>.md` — **topic briefing** for a specific consumer, e.g. a schema change
  a notebook needs to know about. Read when that topic is yours; not an entry point.

Sections 1 and 3 below apply to any agent on any machine. **Section 2 is CGlabs-specific**;
the equivalent for the Afrilabs/PASCAL box is `server-environment.md` on branch
`docs/server-environment`, and standing that machine up is issue #29.

**Root holds live work only.** Closed dispatch threads, old handovers and superseded plans are
in `archive/dispatches/` with an index. When a thread closes, `git mv` it there, add a row to
that README, and repoint any reference from a file that stays.

### 1) Two-node development model

Work is split across two machines and coordinated through `DISPATCH_cglabs_*.md` files (append-only, **newest block on top**):

- **macbook** — authors code, writes dispatch blocks describing what to run.
- **CGlabs** (this node; `R/0_server_setup.R` sets `Cglabs <- TRUE`) — owns the live data under `/home/jovyan/common_data/nex-gddp-cimp6_hazards`, runs and validates the pipeline, publishes to `s3://digital-atlas`, and reports back by prepending a `### RESPONSE` block to the dispatch it answers.

Operating rules that have proven load-bearing:
- **Stop at every gate a dispatch names.** If anything deviates from the dispatch's stated expectation, stop at that step and describe what you see — do **not** improvise a fix. (This caught a stale-ensemble tier, a silent grid mismatch, a NULL-exposure abort, and two gate false-fails.)
- After committing a `### RESPONSE`, **verify it landed on `origin/develop`** (`git fetch`; `git log origin/develop..HEAD` empty; `git show origin/develop:<file> | grep -c <marker>`). Push races have happened.
- Commit trailer: `Co-Authored-By: <your own model name> <noreply@anthropic.com>` — sign as whatever model you actually are, so the record stays accurate across sessions and machines.
- Publish only via the script a dispatch names (e.g. `scripts/r3_publish_tiers.R`). Never run `R/s3_upload.R` or the derive script, and never pass `--reference` / `--allow-schema-drift`, unless a dispatch explicitly says so. Keep any `_parked_*` / `sandbox/backup/` dirs until told they can go.

### 2) CGlabs node gotchas (see `server-environment-cglabs.md` §9 for the full list)

- **`R/0_server_setup.R` runs `setwd(working_dir)`.** So a repo-relative `source("R/…")` evaluated *after* sourcing setup fails (cwd is now `common_data`). In `Rscript -e '…'` one-liners, source repo scripts by **absolute path**: `source('/home/jovyan/atlas/hazards_prototype/R/…')`. Standalone `Rscript R/foo.R` is fine.
- **No swap; 376 GiB / 40 logical cores.** OOM = instant kill. R/3 §4.2 is deliberately `worker_n4.2 <- 1`.
- **Data is in `common_data`, not repo-relative.** The container `/` is ephemeral — never write outputs there. Ingest `--out` defaults are repo-relative (a known trap); pass an absolute `common_data` path.
- **Long jobs:** `nohup Rscript … & ` survives; logs go in `logs/<name>_<STAMP>.log` (+ `.pid`).
- **`arrow` and `duckdb` cannot both be attached in one R session.** AWS CLI is not on `PATH` (S3 goes through `AtlasDataManageR`/paws).

### 3) Repo hygiene

`hazards_prototype` is a **public** GitHub repo — do not commit internal IP addresses,
fully-qualified internal hostnames, mount or volume identifiers, or credentials. This includes
quoting them in order to recommend their removal, which has happened twice. Describe a mount by
path, size and type instead. Before committing anything that pastes node output:

```bash
git grep -nIE '\b(10|192\.168|172\.(1[6-9]|2[0-9]|3[01]))\.[0-9]{1,3}\.[0-9]{1,3}\b'
```

`.gitignore` covers `*.log`, `nohup.out`, `aws/`, `__pycache__/`.
