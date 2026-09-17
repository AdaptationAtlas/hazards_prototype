## Project instructions for coding agents

R project for a climate-hazards processing pipeline (Africa Agriculture Adaptation Atlas). This file orients an agent working **on the CGlabs node**. See `README.md` for the science pipeline and `server-environment-cglabs.md` for the full machine note.

### Two-node development model

Work is split across two machines and coordinated through `DISPATCH_cglabs_*.md` files (append-only, **newest block on top**):

- **macbook** — authors code, writes dispatch blocks describing what to run.
- **CGlabs** (this node; `R/0_server_setup.R` sets `Cglabs <- TRUE`) — owns the live data under `/home/jovyan/common_data/nex-gddp-cimp6_hazards`, runs and validates the pipeline, publishes to `s3://digital-atlas`, and reports back by prepending a `### RESPONSE` block to the dispatch it answers.

Operating rules that have proven load-bearing:
- **Stop at every gate a dispatch names.** If anything deviates from the dispatch's stated expectation, stop at that step and describe what you see — do **not** improvise a fix. (This caught a stale-ensemble tier, a silent grid mismatch, a NULL-exposure abort, and two gate false-fails.)
- After committing a `### RESPONSE`, **verify it landed on `origin/develop`** (`git fetch`; `git log origin/develop..HEAD` empty; `git show origin/develop:<file> | grep -c <marker>`). Push races have happened.
- Commit trailer: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Publish only via the script a dispatch names (e.g. `scripts/r3_publish_tiers.R`). Never run `R/s3_upload.R` or the derive script, and never pass `--reference` / `--allow-schema-drift`, unless a dispatch explicitly says so. Keep any `_parked_*` / `sandbox/backup/` dirs until told they can go.

### Node gotchas (see `server-environment-cglabs.md` §9 for the full list)

- **`R/0_server_setup.R` runs `setwd(working_dir)`.** So a repo-relative `source("R/…")` evaluated *after* sourcing setup fails (cwd is now `common_data`). In `Rscript -e '…'` one-liners, source repo scripts by **absolute path**: `source('/home/jovyan/atlas/hazards_prototype/R/…')`. Standalone `Rscript R/foo.R` is fine.
- **No swap; 376 GiB / 40 logical cores.** OOM = instant kill. R/3 §4.2 is deliberately `worker_n4.2 <- 1`.
- **Data is in `common_data`, not repo-relative.** The container `/` is ephemeral — never write outputs there. Ingest `--out` defaults are repo-relative (a known trap); pass an absolute `common_data` path.
- **Long jobs:** `nohup Rscript … & ` survives; logs go in `logs/<name>_<STAMP>.log` (+ `.pid`).
- **`arrow` and `duckdb` cannot both be attached in one R session.** AWS CLI is not on `PATH` (S3 goes through `AtlasDataManageR`/paws).

### Repo hygiene

`hazards_prototype` is a **public** GitHub repo — do not commit internal IPs, hostnames, or credentials. `.gitignore` covers `*.log`, `nohup.out`, `aws/`, `__pycache__/`.
