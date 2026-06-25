> ✅ **GATE PASS — executed on cglabs 2026-06-25 (HEAD 88fc098).**
> - TASK 1: `=== GATE SUMMARY: 9 passed, 0 failed ===`, exit 0. All 4 checks
>   (fresh compute w/ `n_gcms=1` marker; no-FORCE SKIP; `FORCE_OVERWRITE=1`
>   recompute+overwrite, mtime advanced, no abort; year-1850 no-input → loud
>   `stopifnot` rc=1).
> - TASK 2 content sanity (ACCESS-ESM1-5, NDWS-1995-01): min 18.15, max 31,
>   NA% 74.2 (ocean/no-data) → in [0,31], not all-NA → **PASS**.
> - TASK 3: all 13 `04_indices/*.R` parse **OK** on this box.
> - Newly-surfaced warnings (expected, `warn=-1` dropped): during NDWS compute,
>   `rm(list=...)` warns `object 'ETMAX'/'LOGGING'/'NDWL0' not found` — harmless
>   (cleanup rm of vars not created in the NDWS branch); cosmetic, a `mget`/
>   `Filter(exists,...)` guard would silence — macbook follow-up, non-blocking.
> - Test artifact left in place (`historical_ACCESS-ESM1-5/NDWS/{NDWS,AVAIL}-1995-01.tif`);
>   safe to delete. **Did NOT sweep 01/02/03/05/06 — macbook's next code step.**

# DISPATCH → cglabs Claude Code — Stage-0 Phase-2 GATE

**You are the cglabs session** in the two-session model: macbook = code/docs,
**cglabs = run/validate on live `Data/`**. Both push to `develop`; always
`git pull --rebase` before pushing. Do NOT edit pipeline logic here — your job is
to RUN the gate, validate, and report back. Any code fix goes back to macbook.

## Context (what macbook already did, all on `develop`)
Stage-0 workout, `hazards_upstream/` (the vendored AdaptationAtlas/hazards nexgddp
index producer):
- **Phase 1** (`de9df60`): loud-fail safety gates across 02/03/04/06 (stopifnot
  file-count gates, OR→AND corruption test, DRY_RUN on `rm -r`, overwrite=TRUE).
- **Phase 2** (`05bd138`, `c5b5f06`, `1691bed`): new shared helper
  `R/00_setup.R` (`common_data_root()`, `.log()`, env run-controls
  `SCENARIO/SSPS/YRS/GCMS/MONTHS/FORCE_OVERWRITE`, `cfg_*`, `should_skip()`).
  **All 13 `04_indices/` scripts migrated** to source it. Behaviour-preserving by
  default; `warn=-1` dropped so warnings now surface.
- Full detail: `hazards_upstream/STAGE0_PHASE2_ROLLOUT.md`.

Everything is parse-clean + unit-tested on macbook, but **not yet run on real
Data/**. That is this dispatch.

## Pre-flight
```bash
cd <repo root>
git checkout develop && git pull --rebase origin develop
git log --oneline -1   # expect 1691bed (or later) "test(stage0): ... GATE harness"
command -v Rscript      # confirm R present
echo "COMMON_DATA=${COMMON_DATA:-$HOME/common_data}"
ls -d "${COMMON_DATA:-$HOME/common_data}/nex-gddp-cmip6/pr/historical/" | head   # inputs present?
```

## TASK 1 (REQUIRED) — run the self-checking GATE
Validates the 04_indices migration at runtime, smallest scope (NDWS 1995-01, one
GCM = the historical AVAIL seed month, no prior-AVAIL dependency).
Default GCM = `ACCESS-ESM1-5` (confirmed present at
`common_data/nex-gddp-cmip6/pr/historical/ACCESS-ESM1-5`).
```bash
bash hazards_upstream/R/04_indices/gate_phase2_ndws.sh 2>&1 | tee /tmp/phase2_gate.out
echo "exit=${PIPESTATUS[0]}"
```
Override with another present GCM if needed:
```bash
GATE_GCM=MPI-ESM1-2-HR bash hazards_upstream/R/04_indices/gate_phase2_ndws.sh
```
The harness asserts (and prints PASS/FAIL per check):
1. timestamped `.log` run-config marker with `n_gcms=1`;
2. `NDWS-1995-01.tif` + `AVAIL-1995-01.tif` written;
3. re-run without `FORCE_OVERWRITE` → SKIPS; with `FORCE_OVERWRITE=1` →
   RECOMPUTES + overwrites, no "file exists" abort (overwrite=TRUE);
4. no-input year 1850 → FAILS LOUD via the Phase-1 `stopifnot` (non-zero exit).

**Exit 0 = GATE PASS. Non-zero = FAIL.**

## TASK 2 (REQUIRED) — content sanity on the gate output
The harness checks files EXIST; also confirm values are sane (NDWS = soil-water-stress
days in a month, so 0–31, not all-NA):
```bash
GCM="${GATE_GCM:-ACCESS-ESM1-5}"
F="${COMMON_DATA:-$HOME/common_data}/nex-gddp-cmip6_indices/historical_${GCM}/NDWS/NDWS-1995-01.tif"
Rscript --vanilla -e "
  suppressMessages(library(terra)); r <- rast('$F')
  v <- values(r); cat('min',min(v,na.rm=T),'max',max(v,na.rm=T),
    'NA%',round(100*mean(is.na(v)),1),'\n')
  stopifnot(min(v,na.rm=T) >= 0, max(v,na.rm=T) <= 31, !all(is.na(v)))
  cat('CONTENT SANITY PASS\n')"
```

## TASK 3 (REQUIRED) — parse-check the whole migrated stage on this box
Catches any environment-specific parse/source issue macbook can't see:
```bash
cd hazards_upstream/R/04_indices
for f in *.R; do Rscript --vanilla -e "invisible(parse('$f'))" \
  >/dev/null 2>&1 && echo "OK  $f" || echo "FAIL $f"; done
```

## Report back to macbook (paste into the handover / reply)
- GATE exit code + the `=== GATE SUMMARY: N passed, M failed ===` line.
- TASK 2 min/max/NA% line + PASS/FAIL.
- Any FAIL: the relevant lines from `/tmp/phase2_gate.out` (or the mktemp log the
  harness prints) — macbook needs the actual error text to fix.
- If a `.log` shows newly-surfaced warnings (expected, since `warn=-1` was dropped),
  note them — they may be pre-existing latent issues worth a follow-up.

## On PASS
Reply "GATE PASS" + the summary. **Do NOT sweep the remaining stages here** — that's
macbook's code work (01/02/03/05/06 migration via the recipe in the rollout doc).
Macbook will stage those next.

## On FAIL
Do NOT modify pipeline scripts. Send the failing check + error text back to macbook;
macbook fixes on `develop`, you `git pull --rebase` and re-run the gate.

## Safety notes
- The gate writes only `historical_<GCM>/NDWS/NDWS-1995-01.tif` + `AVAIL-1995-01.tif`
  — a tiny, disposable test artifact for one month. The FORCE step overwrites it.
  Safe to delete after: `rm -f .../historical_<GCM>/NDWS/{NDWS,AVAIL}-1995-01.tif`.
- Unrelated human action still open (NOT a Claude task): rotate the leaked CDS key at
  https://cds.climate.copernicus.eu (account UID 63618) — see HANDOVER_2026-06-24.md.
