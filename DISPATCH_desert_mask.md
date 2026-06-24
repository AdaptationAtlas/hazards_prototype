> ✅ **DONE — executed on cglabs 2026-06-24 (commit 17e612b). Do not re-run.**
> Re-ran R/2.2 with the 100 mm/yr baseline mask (SEC1 mask log confirmed),
> validated 10/10, republished all 10 domain=climate hazard-change keys
> (CONFIRM=1, backups taken, verified null-stat=0). Sanity: EGY New Valley % →
> NULL (2752/2880), ptot_diff full coverage, arid increase_5 area 64.9M→61.0M,
> nan=0. data.json hazard_change methods updated; ISSUE cause-1 RESOLVED.
> STEP 3 compound diagnostic (`PTOT_DELTA_MIN_MM`) intentionally SKIPPED (default
> ships baseline-100 only) — run it if Pete wants the arid-band delta numbers.
> Housekeeping: two `.preFix-*.bak` sets exist on S3 for ptot/thi (a 2-min
> foreground-timeout retry); prune older backups when convenient. Live URLs not
> browser-verified by a human yet.

# DISPATCH — activate desert PTOT mask + close ISSUE_cr093_nan_zeroprecip

For: cglabs Claude Code (live Data/ + S3) — or run by hand in the cglabs terminal.
From: macbook, 2026-06-24. **R/2.2-ONLY — no R/2 rebake.** Independent of the
deferred full rebake; closes the desert false-"increase" follow-up on its own.

## What this does
R/2.2 SEC1 now masks `past[past < PTOT_BASELINE_MIN_MM] <- NA` (default **100 mm/yr**)
before the `%` change, and computes Δmm from the UNMASKED baseline (full coverage).
So hyper-arid cells stop producing `Inf%` counted as a precip increase, while
`ptot_diff` keeps reporting absolute change everywhere. Re-run R/2.2, re-validate,
republish the `domain=climate` hazard-change keys.

## STEP 0 — sync + confirm
```bash
git pull origin develop
grep -n 'PTOT_BASELINE_MIN_MM' R/2.2_haz_change.R   # default must read "100"
```

## STEP 1 — re-run R/2.2 (mask active by default)
```bash
nohup Rscript -e 'source("R/0_server_setup.R"); source(file.path(project_dir,"R","2.2_haz_change.R"))' \
  > logs/cr093_desert_$(date +%Y%m%d_%H%M).log 2>&1 &
tail -f logs/cr093_desert_*.log
# expect the log line: "SEC1: masking baseline PTOT < 100.0 mm/yr before % change"
```

## STEP 2 — sanity check (the validator does NOT check value ranges)
Confirm the fix actually landed, before publishing:
```r
# in R, on the freshly-written ptot_change_by_model + ptot_diff parquets:
# - a known hyper-arid admin (e.g. EGY New Valley / Al-Kharga): % rows now NA
#   (no spurious +increase), Δmm rows still populated (full coverage)
# - arid-zone "increase_5" %-area should DROP vs the currently-published parquet
# - non-finite count still 0 (the !is.finite guard stays as cheap defense)
```
STOP + report if the arid % isn't NA or the increase-area didn't drop — the mask
didn't take effect.

## STEP 3 — (OPTIONAL) compound-cut diagnostic — numbers for Pete, don't ship yet
```bash
# second run with the absolute floor, to a SCRATCH location or just to compare
# the arid-band increase/decrease %-area; do NOT publish this unless Pete approves
# the compound cut after seeing the delta:
PTOT_DELTA_MIN_MM=10 Rscript -e 'source("R/0_server_setup.R"); source(file.path(project_dir,"R","2.2_haz_change.R"))'
# report: how much does the 100-200mm-band increase/decrease %-area change vs default?
```
(Default ships baseline-100 only. Compound `PTOT_DELTA_MIN_MM` stays a separate
decision — see ISSUE_cr093_nan_zeroprecip.md.)

## STEP 4 — gate
```bash
Rscript R/validate_cr093_real.R     # require GATE PASSED (10/10)
```

## STEP 5 — publish (backs up existing S3 objects first, public-read)
```bash
CONFIRM=1 Rscript R/publish_cr093_r22.R
# verify the 10 domain=climate hazard-change keys are reachable + prunable as before
```

## STEP 6 — metadata + close
- Add the masking rationale to the `hazard_change` record in `metadata/data.json`
  (the citable paragraph in ISSUE_cr093_nan_zeroprecip.md) — now appropriate, since
  the published data IS masked. Republish data.json if the catalog tracks it.
- Mark `ISSUE_cr093_nan_zeroprecip.md` cause-1 RESOLVED (baseline mask live).
  Cause-2 (zero-coverage zones) stays NA-by-design. Note the compound cut as a
  separate open option if not shipped.

## Scope note
This is the desert/correctness fix ONLY. Poultry-THI #13 + the §5.x perf/Tier-D
items remain bundled for the next full rebake (R/NEXT_FULL_REBAKE.md). Doing this
now does NOT touch R/2 or the haz_risk products.
