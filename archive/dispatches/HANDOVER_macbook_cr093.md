# Handover → macbook / VS Code Claude Code instance

Date: 2026-06-24. From: the cglabs Claude Code session (runs IN
`~/atlas/hazards_prototype` on CGlabs with live `Data/`, `aws`, and S3 write).
To: the Claude Code instance on Pete's local macbook (VS Code), which has the
git repo but **no access to the CGlabs server**.

## Your machine's hard constraints (read first)
You have the repo (synced via git on branch `develop`) but you **cannot**:
- read live `Data/` — it lives at `/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data`
  on CGlabs, not on the macbook;
- run `R/2.2_haz_change.R`, `R/2_calculate_haz_freq.R`, `R/validate_cr093_real.R`,
  or `R/publish_cr093_r22.R` against real data — they need that `Data/` + the
  CGlabs `aws` binary + S3 credentials;
- publish to or read the `digital-atlas` S3 bucket.

So your role is **code + docs only**: edit scripts, reason about logic, write
commits, push to `develop`. Anything that must touch live data, validate, or
publish is the **cglabs instance's** job — hand those steps back via a commit +
a note, and the cglabs session runs them.

## Branch / sync model
- Both machines push to `develop` on `github.com/AdaptationAtlas/hazards_prototype`.
- ALWAYS `git pull --rebase origin develop` before pushing — concurrent pushes
  happen (we hit a few this session). Resolve conflicts, re-push.
- Commit message footer: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

## What is DONE and live (CR-093, R/2.2) — do not redo
R/2.2_haz_change.R was revived end-to-end this session. On `develop` @ origin:
1. Migrated to the `admin_extract(boundaries_zonal, boundaries_index)` API
   (mirrors R/2.1:111-132); dedup `boundaries_index` by zone_id; gaul-code joins.
2. SEC1 fixes: `_sd` drop (anchored `_mean[.]tif$`), dual historic-prefix dedup,
   ENSEMBLE filter, per-table var parse.
3. SEC2/3/4: reconciled to the dash-delimited risk-dir grammar (`THI-max-max-G..`,
   `NTxNN-mean-G..`, `NDWS-mean-G..`), `"historic"` scenario token, `seq_len(nrow)`.
4. Non-finite payloads → NA before write (clean NULLs).
5. Validated on live Data/: `R/validate_cr093_real.R` = **10 PASS / 0 FAIL**.
6. Published to canonical keys and verified live over HTTPS (all 10 reachable,
   iso3-prunable):
   `s3://digital-atlas/domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=hazard-change/timeframe=annual/variable=<name>.parquet`
7. Catalogued: `hazard_change` record in `metadata/data.json`.

Full technical detail: `R/HANDOFF_cr093_r22.md`.

## Open work you CAN start (code-only; cglabs validates/runs)
1. **R/2 rebake NaN fix — see `ISSUE_cr093_nan_zeroprecip.md`.** This is the main
   one. Two root causes in the R/2-produced rasters:
   - (correctness) `100*d/past` in SEC1 of R/2.2 blows up where historic precip
     `past ≈ 0` (deserts) → `Inf` gets classified as a counted "+5% increase".
     The real fix is upstream in `R/2_calculate_haz_freq.R`: mask cells whose
     historic baseline `PTOT` is below a meaningful threshold **before** the %
     change is derived. The threshold is a SCIENCE decision — flag it for Pete,
     don't guess a number.
   - (cosmetic) zero-coverage admin units → `0/0` NaN; fine to leave NULL or drop.
   You can DRAFT the R/2 edit + a short rationale, commit it, and leave a note
   that cglabs must re-bake R/2 (multi-hour) + re-run R/2.2 + re-validate +
   re-publish. Do NOT mark the issue closed — only the R/2.2 band-aid is done.
2. Pure-logic refactors / comment cleanups in the R/2.x scripts that don't need
   data to verify (lint-level, dead-code, the `ntx_perc_by_model` extra
   `area`/`total_area` columns noted in the handoff).

## Open work you CANNOT do (needs cglabs — leave for that instance)
- Re-running R/2 / R/2.2, `R/validate_cr093_real.R`, `R/publish_cr093_r22.R`.
- The terra §5.2 vectorize probe on CGlabs (`R/probe_r2_5_2_vec.R`, item 2c in
  the handoff) — confirms `USE_R2_5_2_VEC` parity before a multi-hour R/2 bake.
- Any S3 publish / browser-verify.

## Hand-back protocol
When you finish a code change that needs live validation or publish, push it to
`develop` and write a one-paragraph note (here or in a commit body) listing the
exact commands cglabs should run (e.g. the R/2.2 refresh block, then
`Rscript R/validate_cr093_real.R`, then `CONFIRM=1 Rscript R/publish_cr093_r22.R`).
The cglabs session picks it up on its next `git pull`.
