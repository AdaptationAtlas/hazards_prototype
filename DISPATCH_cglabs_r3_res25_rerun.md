# Dispatch: R/3 re-run on the res-25 exposure rasters + hazard_exposure tier republish (#30 follow-up)

**Status:** code on `develop` (this commit and the two before it). Blocks A and B are runnable now.
Block C is the long one. **Block D writes to S3 and is GO-gated** - do not start it without the GO
line in this file.

**Why.** Issue #30 republished the exposure *reference* at both resolutions and regenerated the
exposure *rasters* with the resolution in every name. R/3 (`R/3_freq_x_exposure.R`) now reads only
the rasters tagged for its own grid (`res-25`), but it has not run since. The live hazard product
(`variable=vop_nominal-usd21`, three severity tiers, published 2026-09-16) was built from the 0.05°
0.4.2 crop nominal-USD raster aligned in-flight; that raster is now native 0.25°, so the usd product
will shift slightly (border-cell price assignment). `R/checks/usd_total_vs_reference.R` fails on its
usd side for exactly that reason (intld side 1.000). This dispatch re-bakes R/3 in full (usd + intld +
ha, both timeframes - p.steward 2026-09-26) and republishes the three usd tiers behind a new
value-drift gate.

**What changed in code (read before running):**
- `R3_CROP_VOP_USD` now **defaults to `2021`**. The old default (`2015`) pointed at the legacy
  `spam_vop_usd2015_all.tif`, which was retired into `_pre30_backup/` and deleted, so the old default
  is a guaranteed hard-stop. No env var needed for this run.
- `scripts/r3_publish_tiers.R` gains **G6**, a value-drift gate against the live tier
  (`R/checks/r3_tier_drift_vs_live.R`). G1-G5 never looked at a value. G6 compares admin0 historic
  `any + none` totals per (iso3, crop) in populations: continental per-crop total within 5 %,
  material pairs (live ≥ 1e6 USD) within 25 % with median within 3 %, small/border-heavy countries in
  their own table (50 %), livestock as a "must not move" control (2 %), zero flips, zero unmatched,
  row/scenario parity. It prints in `--dry-run`. `--allow-value-drift` demotes it to WARN; do not
  pass it unless a block here says so.
- `R/checks/probe_r3_res25_preflight.R` - read-only, seconds: resolves every R/3 exposure input the
  way R/3 does and says whether `.align_exposure()` will fire.

**Scoping.** R/3 has no country / tier / variable switch. Scope is by **parking**: move the outputs
to rebuild out of the way (`mv`, never `rm`), run with `FORCE_OVERWRITE` unset, and skip-if-exists
rebuilds only what is missing. Everything parked stays in `Data/_parked_r3_res25_<STAMP>/` until
told otherwise.

**Expectations are invariants, not figures.** Where a block says "count == parked count" or "ratio
essentially 1", that is the gate; a specific number in a log is an example, not a target. If
anything deviates from a stated expectation, **stop at that step and describe what you see.**

---

## Block A - read-only audit (< 5 min, paste everything)

```bash
cd <hazards_prototype> && git fetch origin && git checkout develop && git pull --ff-only && git log -1 --oneline
pgrep -af Rscript || echo "no Rscript running"
env | grep -E '^(FORCE_OVERWRITE|R3_|SKIP_R3|REBAKE_SCENARIO)' || echo "no R/3 env set (good)"
Rscript R/checks/probe_r3_res25_preflight.R
df -h <common_data mount>
```

**Expect:**
- preflight exits 0. Every one of the six inputs "resolves to exactly 1 file" and is `_res-25`
  (crop ha is the untagged native SPAM file - that is correct). Legacy `vop_usd2015_all` absent.
- `compareGeom(Data/base_rast.tif, metadata/base_rast_nexgddp.tif)` - **record the answer**. TRUE:
  R/3 multiplies directly. FALSE: `.align_exposure()` will resample every exposure raster (sum); that
  is allowed, but Block B's log must then show it and must not show `WARN exposure mass not conserved`.
- `_pre30_backup` **absent**. Its deletion was authorised in #30 but never recorded as done. If it is
  still there: **STOP**, report its size and entry count, do not delete it in this block.
- Inventory: paste the table. This is the record of what B and C park.

**STOP.** Paste. Block B may follow immediately if A is clean.

---

## Block B - probe: one tier through the real R/3 path (writes to node Data/, ~1-1.5 h)

Parks only jagermeyr · severe · usd; §4.1 rebuilds that third of the usd tifs, §4.2 builds exactly
one group. Everything else is skip-if-exists.

```bash
cd <hazards_prototype>
export STAMP=$(date +%Y%m%d_%H%M%S); echo $STAMP > logs/r3_res25_stamp.txt
Rscript -e '
  source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")
  stamp <- readLines("/home/jovyan/atlas/hazards_prototype/logs/r3_res25_stamp.txt")
  src <- file.path(atlas_dirs$data_dir$hazard_risk_vop_usd, "jagermeyr")
  dst <- file.path("Data", paste0("_parked_r3_res25_", stamp), "hazard_risk_vop_usd", "jagermeyr"); dir.create(dst, recursive = TRUE)
  f <- c(list.files(src, "_severe_.*_int_.*vop_nominal-usd-2021\\.tif$", full.names = TRUE),
         list.files(src, "^haz-freq-exp_vop_nominal-usd-2021_ENSEMBLE(mean)?_int_adm_severe\\.parquet(\\.json)?$", full.names = TRUE))
  cat("parking", length(f), "files ->", dst, "\n"); print(table(tools::file_ext(f)))
  ok <- file.rename(f, file.path(dst, basename(f))); stopifnot(all(ok))
  cat("remaining severe usd _int tifs in src:", length(list.files(src, "_severe_.*_int_.*vop_nominal-usd-2021\\.tif$")), "(expect 0)\n")'
nohup Rscript -e 'source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); source("/home/jovyan/atlas/hazards_prototype/R/3_freq_x_exposure.R")' \
  > logs/r3_res25_probe_$STAMP.log 2>&1 &
echo $! > logs/r3_res25_probe_$STAMP.pid
```

**T + 5 min - check the header, kill if wrong:**
```bash
grep -E 'Using crop vop usd file|Using crop vop intd file|Using livestock|hazard grid res|overwrite4|R3_CROP_VOP_USD|align|WARN' logs/r3_res25_probe_$STAMP.log | head -30
```
Expect `Using crop vop usd file: spam_vop_nominal-usd-2021_all_res-25.tif | R3_CROP_VOP_USD = 2021`,
every other exposure file `_res-25`, `hazard grid res 0.25x0.25 -> reading exposure rasters tagged res-25`,
`overwrite4= FALSE`, and either no `.align_exposure` lines or exactly the ones Block A predicted, with
**no** `WARN exposure mass not conserved`. Anything else: `kill $(cat logs/r3_res25_probe_$STAMP.pid)`,
un-park (move the files back), paste.

**At exit (wait for `script complete` / process gone):**
```bash
grep -E '4\.1\.1\) .*Complete|4\.2\) Extracting|FAILED|SKIPPED|WARN|Error|complete' logs/r3_res25_probe_$STAMP.log | tail -40
ls -la $(Rscript -e 'source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); cat(file.path(atlas_dirs$data_dir$hazard_risk_vop_usd,"jagermeyr"))' 2>/dev/null)/failed_* 2>/dev/null || echo "no failed_* (good)"
Rscript R/checks/usd_total_vs_reference.R --res 0.25 --severity severe
Rscript scripts/r3_publish_tiers.R --tiers severe --dry-run
```
**Expect (gates):**
- §4.1 usd: number of `_severe_..._int_..._vop_nominal-usd-2021.tif` now in the dir **== number parked**;
  `failed_risk_x_exposure_vop_nominal-usd-2021.txt` absent; the `skipped_not_in_exposure_*` list, if any,
  names only commodities the 2026-09-16 run also skipped. Elapsed in the tens of minutes, not single digits
  (single digits = instant failures swallowed).
- §4.2: `haz-freq-exp_vop_nominal-usd-2021_ENSEMBLEmean_int_adm_severe.parquet` + `.json` mtime > launch;
  sidecar `ensemble.n_members == 18`.
- `usd_total_vs_reference` **usd side PASS** (this is the number the whole item is about; intld unchanged, PASS).
- Publisher dry-run: G1-G5 ok; **G6 table printed** - paste it whole. Expect `total`, `material`,
  `livestock`, `flips`, `unmatched`, `parity` all ok; small-country table populated is fine.

**STOP.** Paste all of it. Do **not** start Block C until macbook has read the G6 table.

---

## Block C - full re-bake (usd + intld + ha, both timeframes; most of a day)

Only after Block B is clean. Parks everything else R/3 §4 writes (the probe's severe usd outputs stay;
they are final artefacts built by the same code from the same inputs).

```bash
cd <hazards_prototype>; export STAMP=$(cat logs/r3_res25_stamp.txt)
Rscript -e '
  source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")
  stamp <- readLines("/home/jovyan/atlas/hazards_prototype/logs/r3_res25_stamp.txt")
  root <- file.path("Data", paste0("_parked_r3_res25_", stamp))
  for (v in c("hazard_risk_vop_usd", "hazard_risk_vop", "hazard_risk_ha")) for (tf in timeframe_choices) {
    src <- file.path(atlas_dirs$data_dir[[v]], tf); if (!dir.exists(src)) next
    dst <- file.path(root, v, tf); dir.create(dst, recursive = TRUE, showWarnings = FALSE)
    f <- list.files(src, "\\.(tif|parquet|json|txt)$", full.names = TRUE)
    if (v == "hazard_risk_vop_usd" && tf == "jagermeyr") f <- f[!grepl("_severe_.*_int_|_int_adm_severe\\.parquet", basename(f))]   # keep the probe outputs
    ok <- file.rename(f, file.path(dst, basename(f))); stopifnot(all(ok))
    cat(sprintf("%-20s %-10s parked %5d | left behind %4d\n", v, tf, length(f), length(list.files(src))))
  }'
nohup Rscript -e 'source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); source("/home/jovyan/atlas/hazards_prototype/R/3_freq_x_exposure.R")' \
  > logs/r3_res25_full_$STAMP.log 2>&1 &
echo $! > logs/r3_res25_full_$STAMP.pid
```
"left behind" must be 0 everywhere except `hazard_risk_vop_usd/jagermeyr` (the probe's severe files).
Header check at T + 5 min exactly as Block B.

**Checkpoints (paste when you look):** `grep -E '4\.1\.1\) .*Complete|4\.2\) Extracting|FAILED|WARN|Error' logs/r3_res25_full_$STAMP.log`.
Per-variable §4.1 elapsed must be in the same order of magnitude as the last honest run (intld ~175 min
per timeframe, usd ~80-115 min); a variable completing in single-digit minutes is the silent-failure
signature and is a **STOP**. `failed_risk_x_exposure_*.txt` must not appear.

**At exit:**
```bash
Rscript R/checks/usd_total_vs_reference.R --res 0.25 --severity severe
Rscript R/checks/usd_total_vs_reference.R --res 0.25 --severity moderate
Rscript scripts/stamp_ensemble_membership.R --dry-run
# intld and ha are not published; gate them new-vs-parked with the same drift check at TIGHT bounds
# (their inputs are renames of the rasters the parked product was built from, so ratios must be essentially 1)
Rscript -e '
  source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); source("/home/jovyan/atlas/hazards_prototype/R/checks/r3_tier_drift_vs_live.R")
  stamp <- readLines("/home/jovyan/atlas/hazards_prototype/logs/r3_res25_stamp.txt"); root <- file.path("Data", paste0("_parked_r3_res25_", stamp))
  for (v in c(hazard_risk_vop = "vop_intld15-2021", hazard_risk_ha = "harv-area_ha")) for (s in c("severe", "moderate", "extreme")) {
    d <- names(v)[1]; fn <- sprintf("haz-freq-exp_%s_ENSEMBLEmean_int_adm_%s.parquet", v, s)
    res <- tier_drift(file.path(atlas_dirs$data_dir[[d]], "jagermeyr", fn), file.path(root, d, "jagermeyr", fn),
                      tol_pair = 0.02, tol_median = 0.01, tol_total = 0.01, tol_small = 0.05, tol_control = 0.02)
    cat(sprintf("\n== %s %s: %s\n", v, s, if (res$pass) "PASS" else "FAIL")); print(res$gates[result == "FAIL"])
    if (!res$pass) print_drift(res, label = fn) }'
Rscript scripts/r3_publish_tiers.R --dry-run
```
**Expect:** both gate sides PASS at both severities; 18 members everywhere; intld/ha drift **PASS at
the tight bounds** (they were not supposed to move - anything wider is a STOP and an explanation, not
a widened bound); publisher dry-run G1-G6 ok for all three tiers, G6 tables pasted.

**STOP. Await GO.**

---

## Block D - publish the three usd tiers (LIVE WRITE - GO-gated)

> GO line goes here, written by macbook after reading Block C's G6 tables.

```bash
cd <hazards_prototype>; export STAMP=$(cat logs/r3_res25_stamp.txt)
Rscript scripts/stamp_ensemble_membership.R --timeframe jagermeyr        # membership from the hazard_risk source folder; must say 18 everywhere
Rscript scripts/r3_publish_tiers.R 2>&1 | tee logs/publish_tiers_res25_$STAMP.log     # no --allow-*, no --skip-gates, no --reference
```
Then **verify from S3, not from the uploader** (s3fs may multipart, so ETag is not md5 - re-download):
```bash
Rscript -e '
  suppressPackageStartupMessages({library(arrow); library(dplyr); library(s3fs)}); source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")
  base <- "s3://digital-atlas/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=jagermeyr/model=ENSEMBLEmean/"
  for (s in c("severe","moderate","extreme")) {
    loc <- file.path(atlas_dirs$data_dir$hazard_risk_vop_usd, "jagermeyr", sprintf("haz-freq-exp_vop_nominal-usd-2021_ENSEMBLEmean_int_adm_%s.parquet", s))
    key <- paste0(base, "severity=", s, "/int=multi-hazard.parquet"); tmp <- tempfile(fileext = ".parquet"); s3_file_download(key, tmp)
    sm <- function(f) { d <- open_dataset(f); c(n = nrow(d), s = (d |> summarise(s = sum(value, na.rm = TRUE)) |> collect())$s) }
    a <- sm(loc); b <- sm(tmp)
    cat(sprintf("%-9s md5 %s | rows S3 %d local %d %s | sum(value) %s | sidecar %s\n", s,
      if (tools::md5sum(loc) == tools::md5sum(tmp)) "MATCH" else "MISMATCH", b["n"], a["n"], if (a["n"] == b["n"]) "==" else "!=",
      if (isTRUE(all.equal(a["s"], b["s"]))) "==" else "!=", if (s3_file_exists(paste0(key, ".json"))) "present" else "MISSING")) }'
for s in severe moderate extreme; do
  curl -sI -r 0-99 "https://digital-atlas.s3.amazonaws.com/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=jagermeyr/model=ENSEMBLEmean/severity=$s/int=multi-hazard.parquet" | grep -E '^HTTP|Last-Modified'
  curl -s "https://digital-atlas.s3.amazonaws.com/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=jagermeyr/model=ENSEMBLEmean/severity=$s/int=multi-hazard.parquet.json" | grep -o '"n_members":[0-9]*'; done
```
**Expect:** three `MATCH`, rows and sums `==`, sidecars present, `HTTP/1.1 206` ×3 with today's
`Last-Modified`, `"n_members":18` ×3. Then the CR-068 probes from `atlas_notebooks/scripts/`
(`probe_no_hazard_arithmetic_quick.sh AGO`, `probe_cross_parquet_vop_drift.sh AGO`): every ratio
≤ 100 %, adm0 == Σ adm1, zero NaN.

Paste all of it. Retain `Data/_parked_r3_res25_$STAMP/` and the `sandbox/backup/issue9_<STAMP>/`
objects the publisher wrote until told they can go. The CDH record edit
(`metadata/cdh/africa-hazard-exposure-nexgddp.yaml`, `updated:` + a note sentence) is macbook work.

---

## cglabs response — Blocks A + B run, STOP at G6 (cglabs 2026-09-27)

**STAMP = `20260926_185444`.** Repo at `b34fdaa`. Block C NOT started; nothing written to S3;
parked set intact at `Data/_parked_r3_res25_20260926_185444/`. `DISPATCH_cglabs_family_keys.md` untouched.

### Block A — clean
Preflight green on every substantive gate. The lone process-gate FAIL was two months-old zombie
babysitter shells (pids 2289981/2394838, R/2 jobs long finished, `until ! pgrep -f 2_calculate_haz_freq`
loops matching themselves); killed with your OK, gate then clean. `compareGeom(base_rast,
base_rast_nexgddp) = TRUE` → R/3 multiplies directly. All 6 inputs 1 file, res-25 except crop_ha
(native 0.05, geom FALSE → align predicted). Legacy usd2015 absent, `_pre30_backup` absent.

### Block B mechanics — clean
Launch 18:55 → complete 20:29 (**1h34m**). Header: `overwrite4=FALSE`, all inputs res-25,
`R3_CROP_VOP_USD=2021`, no WARN/Error/mass-not-conserved. jagermeyr vop_nominal-usd-2021 rebuild
**23.6 min** (not the single-digit silent-fail signature). §4.1 severe usd **174 tifs == 174 parked**;
no `failed_*`. §4.2 severe parquet/json mtime 20:06/20:07 > launch. No `.align_exposure` fired (ha was
skip-if-exists, outputs existed, so align never needed).

**Sidecar note (not blocking):** rebuilt severe sidecar has `ensemble.n_members = [0]`, `members = []`.
Not a regression — the parked *live* 2026-09-16 sidecar is `n_members = None`, and node-side moderate/
extreme siblings too. No node-side raw sidecar ever carries 18; that is stamped later by
`scripts/stamp_ensemble_membership.R` and verified on S3. R/3 L1295 writes `length(.ens_members)`,
empty at §4.2. The Block-B expectation "sidecar n_members == 18" cannot hold on raw node output.

`usd_total_vs_reference --res 0.25 --severity severe`: **usd PASS** (median 0.997, range [0.9598,1]),
intld PASS (1.000). But this samples only **AGO/KEN/NGA**.

### Block B GATE — G6 value drift vs LIVE: **FAIL. ABORTED. This is the STOP.**
G1–G5 ok. G6 severe FAIL — large, crop-specific, crop-side only:

| gate | result | detail |
|---|---|---|
| parity rows/vars/scen | ok | 67,952,655 ==; 4==4; scenarios match |
| spread (freq_any+freq_none=1) | **FAIL** | live 1.00 vs local **0.941** |
| unmatched | ok | 0 |
| flips | **FAIL** | 1 — ZWE wheat **112×** |
| total continental/crop (5%) | **FAIL** | 33 crops, ratio [0.659, **3.879**] |
| material pairs ±25% | **FAIL** | 629 pairs, **82 outside**, range [0.419, **112.3**] |
| material median (3%) | ok | median 1 |
| small-country ±50% | **FAIL** | 175 pairs, 15 outside |
| livestock control (2%) | **ok** | range **[1,1]** — untouched |

Worst continental crops: **wheat 3.88×, oilpalm 3.57×, plantain 1.74×**, cowpea 0.66×.
Worst pairs: ZWE wheat 112×, ZMB wheat 22.8×, oilpalm 5–21× across COD/CMR/TZA/MDG/GAB/AGO/CAF/COG.

**Read of it:**
1. Livestock control exactly 1 → multiply mechanics and livestock path fine; the **crop USD exposure** moved.
2. usd_total_vs_reference PASS = rebuilt product is internally consistent with the **new res-25 reference
   exposure**. G6 compares vs the **2026-09-16 live** product. So the res-25 crop-USD exposure raster
   differs from what live was built on — concentrated in wheat/oilpalm/plantain. Points at the **#30
   exposure rebake**, not R/3 logic.
3. The 3-country reference check missed it — worst offenders (ZWE/ZMB/COD/CMR) not in the sampled trio,
   and wheat/oilpalm minor there.
4. The **spread FAIL** (any+none = 0.941 locally) is separate and concerning — possible partition
   integrity issue, not just a value shift. Wants a macbook read.

**Not proceeding to C. Not passing `--allow-value-drift`.** Need your call on the G6 drift
(esp. wheat/oilpalm and the spread invariant) before the full re-bake.

---

## macbook response — G6 FAIL is REAL and sits in 0.4.2's price fill; Block C is HELD (2026-09-27)

Good stop. G6 did exactly what it was built for, and the read in your point 2 is right: the shift is in
the **crop nominal-USD exposure raster**, not in R/3. Verified from here against S3, read-only
(duckdb httpfs, admin0, tech = all, finite values): the old `variable=vop_nominal-usd-2021.parquet`
(2025-11-03, built from the 2025-08 0.4.2 rasters live was built from) vs the reference republished on
2026-09-25 at **both** resolutions:

| crop | old (M USD) | new res-05 | new res-25 | r05 | r25 |
|---|---:|---:|---:|---:|---:|
| wheat | 3,224 | 12,412 | 12,453 | 3.85 | 3.86 |
| oilpalm | 5,316 | 18,963 | 18,842 | 3.57 | 3.54 |
| plantain | 19,801 | 34,611 | 34,405 | 1.75 | 1.74 |
| cowpea | 8,005 | 5,278 | 5,267 | 0.66 | 0.66 |
| cassava | 48,253 | 57,594 | 56,930 | 1.19 | 1.18 |
| everything else | | | | 0.97-1.04 | 0.97-1.04 |

**r05 == r25 to two decimals → the grid is innocent.** The per-country ratios are region-shaped
constants: oilpalm **20.37×** in TZA/BDI/COD/MDG (all `East_Africa` in `regions`), **5.84×** in
AGO/GNQ/STP/CMR/GAB/CAF/COG/NAM (`Central_Africa`), cassava **2.11×** in CAF/COG/GAB/SDN/CMR, ZWE wheat
**112.6×**, ZMB wheat **22.8×**. That is the signature of `add_nearby()` (`R/haz_functions.R`, sourced from
GitHub `main` at run time): the fill chain own → 5-yr median → **mean of neighbours** → **mean of region**
→ **mean of continent** → world median, with `mean()` everywhere. One absurd producer price (Zimbabwe's
USD-converted series is the obvious candidate for wheat; something in East Africa for oil palm fruit) is
inherited by every country in its region that has no own price. ZMB inherits ZWE through the neighbour
mean. 735 material pairs: 72 outside ±25 % at res-05, 138 at res-25 (the latter adds the genuine
border-cell shifts), median 1.00 on both.

**What this means:** (a) the R/3 rebuild is internally correct — `usd_total_vs_reference` PASS says
product == reference, and the livestock control at exactly 1 says the multiply is clean. (b) The
**republished reference itself** (`crop-livestock_all_res-05`, `_res-25` and the alias, `unit =
nominal-usd-2021` rows) has carried these values live since 2026-09-25; its `intld15-2021` rows come
from 0.4.0 (no producer prices) and are unaffected, as is the live hazard product (2026-09-16). (c) The
2026-09-16 hazard product was built from 0.4.2 rasters dated 2025-08; between then and Block H the price
inputs or the fill changed (FAO CSVs, or `main`'s `haz_functions.R`) — Block B2 below finds out which.
(d) `usd_total_vs_reference`'s AGO/KEN/NGA default is blind to this by construction; it now takes
`--iso3 all`.

**Corrections to the dispatch, all on develop after `b34fdaa`:**
- Sidecar: you are right, raw R/3 output never carried 18. Root cause is worse than "stamped later":
  §4.2 derived membership from per-GCM `_int` tifs **left over in the output folder** by pre-`ensemble_only`
  runs, so any parked/clean folder yields 0 — and Block C's parking would have zeroed every tier.
  R/3 §4.2 and `scripts/stamp_ensemble_membership.R` now read the **source** folder
  (`hazard_risk/<tf>`, the R/2 §5.3 per-GCM stacks) and fall back to the output folder. Block D gains
  `stamp_ensemble_membership.R` before the publisher; the Block B `n_members == 18` expectation is
  withdrawn for raw output and applies after the stamp.
- G6 "spread": my invariant was wrong — some hazard_vars combinations carry no rows for some crops, on
  live too (your 1.00). Now informational; the pair total is the max over defined combinations.

**Block C is HELD.** Re-baking R/3 against contaminated exposure rasters would only reproduce the
contamination in the product. Order is now: B2 (below) → fix 0.4.2's fill → re-run 0.4.2 at both
resolutions → 0.4.4 at both → republish the reference (`--reference-only`, both res) → then C.
Decisions for Pete are listed after B2.

### Block B2 — price-fill probe (read-only, ~1-3 min, paste everything)

```bash
cd <hazards_prototype> && git fetch origin && git checkout develop && git pull --ff-only && git log -1 --oneline
Rscript -e 'source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); for (f in list.files(fao_dir, "\\.csv$", full.names = TRUE)) cat(sprintf("%-55s %s %6.0f MB\n", basename(f), format(file.mtime(f), "%Y-%m-%d %H:%M"), file.size(f)/1e6))'
EXPOSURE_RES=0.25 Rscript R/checks/probe_042_price_fill.R 2>&1 | tee logs/probe_042_$(date +%Y%m%d_%H%M%S).log
Rscript R/checks/usd_total_vs_reference.R --res 0.25 --severity severe --iso3 all 2>&1 | tail -25
```
**Expect:** the probe runs 0.4.2 §1 and §3 only (its log says so), writes nothing, and prints (i) the
FAO file mtimes and price observations per year — say whether 2022/2023 have prices; (ii) the fill-source
counts; (iii) the top-30 prices vs the world median — **wheat ZWE and oil palm fruit in one East African
country should be at the top, in the hundreds or thousands of times the world price**; (iv) per suspect
crop, every country's final price, its source, and the raw FAO series for the odd ones. The `--iso3 all`
gate is informational (product and reference share the rasters, so it should PASS; if it does not, say
so). **STOP.** Nothing else runs until the fill is fixed.

### For Pete — decisions

1. **The 2026-09-25 reference publish carries these nominal-USD values live** (both resolutions + alias).
   The `intld15-2021` rows and the live hazard product are unaffected. Options: leave until the corrected
   republish (days), or roll the `nominal-usd-2021` rows back — there is no clean object to roll back to
   (the pre-#30 backup has the livestock-in-nominal bug in its intld rows). Recommend: leave, fix fast,
   say so in the two new CDH records' `note` meanwhile.
2. **Fix shape for 0.4.2** (value-changing, needs GO): (a) discard own prices outside a sane band around
   the world median before any averaging (e.g. 1/5-5×; Zimbabwe-type currency artefacts); (b) `median`
   instead of `mean` in neighbour / region / continent fills; (c) log every fill source so the next
   contamination is visible in the run log; (d) a per-crop cross-basis check (nominal ÷ intld per
   country in a band) as an exposure gate — the same check that would have caught #38.
3. Then re-run 0.4.2 → 0.4.4 (both res) → `--reference-only` republish (gates: columns, distinct,
   rows identical to the 2026-09-25 objects; plus old-key vs new per-crop ratios inside ±25 % except
   documented corrections) → Block C.

---

## cglabs response — Block B2 done, root cause confirmed in 0.4.2 fill; C stays HELD (2026-09-27)

Repo at `fc398be`. B2 read-only, wrote only its log (`logs/probe_042_20260927_112448.log`). Your
diagnosis is confirmed at the source.

**FAO inputs:** Prices refreshed **2026-05-15** (`Prices_E_Africa_NOFLAG.csv`, `Prices_E_All_Data_
(Normalized).csv` 214 MB); VoP `All_Data` still **2025-08-21**. So the price CSVs changed since the
2025-08 rasters live was built from. Producer-price obs/year: `2021:199 2022:222 2023:167` — 2022 and
2023 both have prices and both sit inside the fill windows.

**Fill sources y2021 (1705 rows):** continent mean 501 · region mean 401 · neighbours mean 398 · own
2019-23 median 251 · own 2014-23 median 100 · world median 54. Only **21 %** carry an own price; **76 %**
come off the mean-fill chain — high leverage for one bad own price.

**Smoking gun (top prices vs world median):**

| iso3 | crop | source | final USD/t | world | ratio |
|---|---|---|---:|---:|---:|
| ZWE | wheat | own 2019-23 median | 33,790 | 292.8 | **115×** |
| ZMB | wheat | neighbours mean | 11,680 | 292.8 | 39.9× |
| BWA | wheat | neighbours mean | 11,520 | 292.8 | 39.3× |
| BDI/COD + 14 E.Afr | oilpalm | nbr/region mean | 4,638 | 149 | 31.1× |
| AGO/CMR/… | oilpalm | continent mean | 1,330 | 149 | 8.93× |

**Mechanism, confirmed:**
1. Unfiltered artefact in own median: raw FAO ZWE wheat `2021:421, 2022:67170` (rest NA in window) →
   median of the 2 points = 33,790. One Zimbabwe currency-conversion year survives because the window
   is 2 points.
2. `mean()` propagates it: ZMB/BWA (no own wheat price) inherit ZWE via neighbours mean → ~11.7k.
   Wheat region means: Southern 7,139 (ZWE drag) vs Central 664 / East 482 / West 313 / North 306.
3. Single-member region = that member: oilpalm East Africa has only RWA own (`2022:4820, 2023:4456` →
   4,638); region mean over East Africa = RWA alone = 4,638, spread to 17 countries. Central/Southern/
   North oilp own = NaN → continent mean 1,330 (itself inflated by RWA/ZWE) → 8.9× for the rest.

**`usd_total_vs_reference --iso3 all` (severe, res-25): FAIL, but benign.** usd median 0.997, 1226
material pairs, range [0.4442, 1.008]; only **2 pairs outside [0.5,2]** (UGA oilpalm 0.458 — res-25
border-cell shift at the materiality floor). intld PASS (0.999). Product vs new reference agree to
0.997 because both carry the contaminated rasters — blind to the fill bug by construction, as you noted.
The FAIL is 2 marginal border pairs, not the price problem.

**Block C stays HELD.** Confirms the fix belongs in 0.4.2's `add_nearby`: band-filter own prices vs
world median **before any averaging** (kills ZWE wheat 67170 and RWA oilp 4638 at source), median not
mean in neighbour/region/continent, log every fill source. The oilpalm case shows band-filtering own
prices is the primary lever (median alone won't help a single-member region); both are needed.
Awaiting Pete's GO on the fix shape (decision 2).

---

## macbook response — fix built and replay-validated; Block C0 (GO-gated) corrects the exposure before C (2026-09-27)

B2 nailed it: ZWE wheat 2022 = 67,170 surviving a two-point median, RWA oil palm fruit as the only
`East_Africa` own price, `mean()` carrying both. 76 % of country x crop prices come off the fill chain,
so the chain's robustness is the product's.

**Fix on develop (this commit):**
- **`R/price_fill.R`** (new, sourced by 0.4.2 by PATH — `haz_functions.R` comes from GitHub `main` at
  run time, so a fix there would not reach a node run): `clip_prices_to_world_band()` drops any own
  observation outside `[1/band, band]` x the world median **for that crop and year** before any
  averaging (`PRICE_BAND`, default 5), returning the dropped rows for the log; `fill_price_robust()`
  fills neighbours → region (excl. self) → continent → world with **medians** and writes a
  `price_source` per row (`PRICE_FILL_STAT=mean` reproduces the old behaviour for A/B only).
- **`R/0.4.2_create_crop_vop_nominal_usd.R` §3** uses both, prints the dropped observations and the
  ten prices furthest from world per vintage, and writes
  `mapspam_pro_dir/fao_prices/crop_price_<vintage>-t_fill-sources_<tag>.csv` as the audit trail.
  Synthetic replay of your B2 numbers: ZWE wheat 33,796 → 421 (own 2021 stands), ZMB 17,108 → 420
  (neighbours median), TZA oil palm 4,638 → 152 (continent median), RWA's own price clipped.
- **`R/checks/vop_cross_basis_gate.R`** (new): nominal ÷ intld per (iso3, crop) on one 0.4.4 table.
  Wide by design (material pairs within [1/10, 10], per-crop median within [1/5, 5]; the 2026-09-25
  object's legitimate spread is 0.2-4.0) — it exists to catch a broken side, and on the live res-25
  object it FAILS on exactly the B2 set (ZWE wheat 122x, ZMB 45x, oil palm 41-47x). The same gate
  would have caught #38 from the other side.
- CDH records for both resolutions now carry a "known defect in the nominal-usd-2021 rows, correction
  in progress" paragraph naming the affected crops (decision 1: leave live, fix fast).

### Block C0 — regenerate the nominal-USD exposure and republish the reference (GO-gated: writes rasters, tables and S3)

> GO line goes here (Pete, decision 2 = fix shape as above).

C0-1 park + re-run 0.4.2 at both resolutions (FORCE irrelevant to 0.4.2 — it always writes):
```bash
cd <hazards_prototype> && git fetch origin && git checkout develop && git pull --ff-only && git log -1 --oneline
export STAMP=$(cat logs/r3_res25_stamp.txt); P=Data/_parked_r3_res25_$STAMP/mapspam_nominal_usd; mkdir -p $P
Rscript -e 'source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); cat(normalizePath(mapspam_pro_dir), "\n")'   # confirm the dir, then:
mv <mapspam_pro_dir>/variable=vop_nominal-usd-2015 <mapspam_pro_dir>/variable=vop_nominal-usd-2020 <mapspam_pro_dir>/variable=vop_nominal-usd-2021 <mapspam_pro_dir>/fao_prices $P/
ls <mapspam_pro_dir> | grep -c 'vop_nominal-usd'      # expect 0 - the parked dirs must be OUTSIDE mapspam_pro_dir or 0.4.4 §1 will still see them
for RES in 0.25 0.05; do EXPOSURE_RES=$RES Rscript R/0.4.2_create_crop_vop_nominal_usd.R > logs/0.4.2_res${RES}_fix_$STAMP.log 2>&1 || { echo "FAILED 0.4.2 @ $RES - stop"; break; }
  grep -E 'exposure grid =|price clip:|fill sources:' logs/0.4.2_res${RES}_fix_$STAMP.log; done
EXPOSURE_RES=0.25 Rscript R/checks/probe_042_price_fill.R --top 15 2>&1 | grep -A18 'top 15 prices'
```
**Expect:** `price clip` line names the dropped observations — **ZWE whea 2022 and RWA oilp 2022/2023 must
be among them**; fill-source counts print per vintage; the probe's top-15 ratios to world are all
**inside 5x**. Anything above 5x: STOP and paste.

C0-2 targeted 0.4.4 at both resolutions (only the nominal-USD extractions are missing, so no FORCE):
```bash
Rscript -e 'source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); stamp <- readLines("/home/jovyan/atlas/hazards_prototype/logs/r3_res25_stamp.txt")
  dst <- file.path("Data", paste0("_parked_r3_res25_", stamp), "exposure_tables"); dir.create(dst, recursive = TRUE, showWarnings = FALSE)
  f <- list.files(exposure_dir, "^(exposure_adm_sum_spam20-20_glw420-20|vop_nominal-usd-2021_adm_sum_spam20_glw420)_res-(05|25)\\.parquet(\\.json)?$", full.names = TRUE)
  stopifnot(all(file.rename(f, file.path(dst, basename(f))))); cat("parked", length(f), "§3 outputs (expect 8)\n")'
for RES in 0.25 0.05; do EXPOSURE_RES=$RES Rscript R/0.4.4_process_exposure.R > logs/0.4.4_res${RES}_fix_$STAMP.log 2>&1 || { echo "FAILED 0.4.4 @ $RES - stop"; break; }
  grep -E 'section 1:|section 2:|section 3.1:|section 3.3' logs/0.4.4_res${RES}_fix_$STAMP.log; done
```
**Expect:** §1 extracts only the nominal-USD tifs (the others reload their caches; minutes, not hours);
§3.1 14 columns; `vop_intld15-2021` twin untouched (skip-if-exists — it did not change).

C0-3 gates (paste all):
```bash
Rscript R/checks/vop_cross_basis_gate.R --res 0.25;  Rscript R/checks/vop_cross_basis_gate.R --res 0.05
Rscript R/qaqc_vop_vs_faostat.R 2>&1 | tail -8                                    # intld side must be unchanged
# new nominal-USD table vs the LAST GOOD publication (variable=vop_nominal-usd-2021.parquet, 2025-11-03, built from the 2025-08 prices)
Rscript -e '
  suppressPackageStartupMessages({library(arrow); library(dplyr); library(data.table); library(s3fs)}); source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")
  old <- tempfile(fileext = ".parquet"); s3_file_download("s3://digital-atlas/domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/variable=vop_nominal-usd-2021.parquet", old)
  rd <- function(f) open_dataset(f) |> filter(is.na(admin1_name), exposure == "vop", tech == "all") |> select(iso3, crop, value) |> collect() |> as.data.table()
  o <- rd(old)[is.finite(value)]; n <- rd(file.path(exposure_dir, "vop_nominal-usd-2021_adm_sum_spam20_glw420_res-05.parquet"))[is.finite(value)]
  m <- merge(o, n, by = c("iso3", "crop"), suffixes = c("_old", "_new"))[value_old >= 1e6][, ratio := value_new / value_old]
  cat(sprintf("material pairs %d | median %.3f | outside +/-25%%: %d\n", nrow(m), median(m$ratio), sum(abs(m$ratio - 1) > 0.25)))
  print(m[, .(n = .N, r = signif(sum(value_new) / sum(value_old), 3)), by = crop][order(-abs(log(r)))][1:12])
  print(m[order(-abs(log(ratio)))][1:15, .(iso3, crop, old = signif(value_old / 1e6, 3), new = signif(value_new / 1e6, 3), ratio = signif(ratio, 3))])'
Rscript scripts/r3_publish_tiers.R --reference-only --res 0.05 --allow-unit-vintage-change --dry-run
Rscript scripts/r3_publish_tiers.R --reference-only --res 0.25 --allow-unit-vintage-change --allow-res-change --dry-run
Rscript scripts/r3_publish_tiers.R --family-only --res 0.05 --allow-unit-vintage-change --dry-run
Rscript scripts/r3_publish_tiers.R --family-only --res 0.25 --allow-unit-vintage-change --allow-res-change --dry-run
```
**Expect (invariants):** cross-basis gate PASS at both res; qaqc intld unchanged (livestock 1.00, crop
~0.99); vs the last good publication, per-crop continental ratios for wheat / oilpalm / plantain /
cassava / cowpea back **inside +/-25 %** and the material-pair median near 1 (some legitimate movement
from the newer FAO prices is expected — name the pairs still outside, do not explain them away);
reference dry-runs `14 columns identical` and **rows identical** to the 2026-09-25 objects (only values
changed); family dry-runs as in `DISPATCH_cglabs_family_keys.md` Block A. **STOP, await GO.**

C0-4 (GO) publish reference both res, then family both res, verify from S3 by re-download (md5 + rows),
paste, then macbook removes the "correction in progress" paragraph from the two CDH records and
**Block C** (full R/3 re-bake) is released.

**Addendum to C0-3 (same day).** Running the cross-basis gate here on the live res-25 object shows the
contamination set AND two pre-existing defects the old-vs-new comparison is blind to because they were
already in the 2025-11 publication and are in the live hazard product today: **Sudan** nominal-USD at
~1/5000 of intld across a dozen crops (wheat, sorghum, groundnut, sesame, cotton, sugarcane, potato, …)
and **Nigeria banana** at ~1/1000 — producer-price currency artefacts in the other direction. The clip
removes those prices too, so in C0-3 **SDN (all crops) and NGA banana are expected to rise by three to
four orders of magnitude against the last good publication**; list them, do not treat them as a FAIL.
The arbiter for them is the cross-basis gate (PASS at both resolutions). Pearl-millet KEN/ETH/RWA/UGA/SDN
at ~1/4000 is #38 (intld side) and stays until that fix; the gate reports it as one-sided/outside and
that is the one expected residual — say so when you paste.

---

## cglabs response — C0-1..C0-3 run; high-side contamination FIXED, but cross-basis gate FAILS on SDN/NGA; STOP before C0-4 (2026-09-28)

Repo at `08fdaf6` (dd3ce9f ancestor, fix present). STAMP `20260926_185444`. Node-local writes only;
nothing published. Parked set + regenerated tables intact.

**Runner correction (flag).** The C0-1/C0-2 commands run 0.4.2/0.4.4 as bare `Rscript R/0.4.x.R`, which
die immediately: `could not find function "exposure_grid"` — that function is in `0_server_setup.R:201`,
and both scripts only `source()` `haz_functions.R` from `main` (0.4.4 line 1 even says "run
0_server_setup.R before executing"). Ran them the Block-B/probe way,
`Rscript -e 'source(0_server_setup.R); source(0.4.x.R)'`. Runner-only; no science change.

**Parking wrinkle (flag).** `mapspam_pro_dir` held two extra legacy short-naming dirs the mv list did
not name — `variable=vop_nominal-usd15`, `variable=vop_nominal-usd21` (Sep 23 11:28, `_adm_sum.parquet`
only, **no .tif**). So `grep -c vop_nominal-usd` read 2, not 0. Verified inert: 0.4.4 §1 is a recursive
`.tif` glob (line 132) and these hold no tif; nothing in the C0 chain reads that naming (the
`r3_publish_tiers.R` `vop_nominal-usd21` hits are the S3 key). Left them in place. No `nominal-usd` tif
remained anywhere under `mapspam_pro_dir` after parking.

**C0-1 — clip fired, targets dropped.** `dropped 99 of 3295 own observations; band 5x; fill = median`,
identical at both res. Dropped list includes **ZWE whea 2022 = 67,167** and **RWA oilp 2022/2023 =
4,819/4,455**, plus the whole ZWE-2022 currency block and ZWE/RWA sugarcane. Probe top-15 post-fix: no
crop above 5x high-side; the two furthest are low-side GIN/SLE millet 0.16 (genuine cheap crop).
Fill sources y2021 now all median (continent 452 · neighbours 514 · region 394 · own window 246 · own
longer 99).

**C0-2 — 0.4.4 both res.** §1 66->45 tifs, 39-file extraction ~1 min each (minutes, not hours); §3.1
14 columns; units kept number/ha/t/nominal-usd-2021/intld15-2021; intld twin reloaded from cache.

**C0-3 gates:**
- qaqc intld **unchanged**: livestock median 1.00 (242/242 within 0.9-1.1), crop 0.99 (36/50; worst
  DZA edge 0.00). Good.
- vs last-good (res-05): 736 material pairs, **median 1.000**, **140 outside +/-25%**. The **high-side
  contamination is fixed** — continental ratios oilpalm 1.06 (was 3.57x), plantain 1.17 (1.74x),
  cassava 1.11 (1.19x), wheat ~1.0 (was 3.88x, not in worst-12). New movers to name: BDI/RWA
  arabica-coffee ~15x up, GIN sweet-potato 8.35x up, GIN cotton 5.65x up, COD coffee 4.4x up, ZWE/MRT/
  GIN sugarcane ~0.07-0.10 down (ZWE clipped, 1960->141), sugarcane continental 0.49, cowpea 0.66.
- **cross-basis gate FAILS at BOTH res** (res-25: 28 pairs outside [1/10,10]; res-05: 26). Per-crop
  medians all within [1/5,5]. Out-of-band set: **SDN ~13 crops** (wheat 1.6e-4, sorghum, groundnut,
  sesame, cotton, sugarcane, potato, banana…), **NGA banana** 7.3e-4, **pearl-millet KEN/ETH/RWA/UGA/
  SDN/MWI** (#38, intld side).
- 4 publisher dry-runs: structural gates PASS — 14 columns identical, distinct exposure/unit/stat
  identical, rows identical to the 2026-09-25 objects (reference 7,847,746 / 5,514,954; family
  1,508,624 / 768,818; res-25 family 0.70x baseline = res-change informational).

**The problem: the addendum's SDN/NGA model is wrong.** They were expected to rise 3-4 orders of
magnitude as the clip removed their prices, leaving pearl-millet as the one residual. Instead SDN/NGA
**barely moved** — SDN wheat 28,687->22,484 (0.78x), NGA banana 2.86M->1.90M (0.67x), most ~1.0x — and
**they are not in the clip's dropped-99 list.** The clip removes *high-side* producer-price artefacts
(ZWE/RWA), which is exactly the G6 contamination and it is gone. SDN/NGA are a *low-side* nominal-VoP/
exchange artefact (SDN's applied nominal wheat price is ~0.04 USD/t, implying the tiny value comes from
the FAO Value_of_Production/exchange path, not the Prices_E producer-price series the band clip
touches). So the cross-basis arbiter still FAILS on them.

**STOP before C0-4.** The fix does what it was scoped to (kills the G6 contamination) but the
cross-basis gate — the named arbiter — is not green because of the pre-existing SDN/NGA low-side defect
this shape does not reach. Decision for you: (a) extend the clip / add a low-side band on the
VoP-derived price (SDN/NGA), or (b) scope SDN/NGA out explicitly like pearl-millet #38 so the gate's
expected residual is defined and PASSES. Then re-run C0-1..C0-3 and issue the second GO.

---

## macbook response — SDN/NGA are an intld-side SPAM-footprint defect, not a price; gate now says which side; re-run C0-3 gates only (2026-09-28)

Both flags accepted: the bare `Rscript R/0.4.x.R` runner was wrong (setup must be sourced first — as
you did) and the two `vop_nominal-usd15/21` short-name dirs are inert. Thank you for not re-running
the unchanged fix.

**Your read is right, and the mechanism is now known.** From the live table, admin0, `tech = all`:

| | SPAM 2020 production | intld15-2021 | nominal-usd-2021 | implied USD/t |
|---|---:|---:|---:|---:|
| SDN all crops | **0.05 Mt** | 5.86 B I$ | 0.02 B USD | ordinary |
| SDN wheat | 0.0 kt | 142.6 M | 0.012 M | 499 |
| SDN groundnut | 5.9 kt | 1,912 M | 4.6 M | 780 |
| NGA banana | 1.5 kt | 2,599 M | 0.74 M | 480 |
| NGA plantain | 6,478 kt | **0** | 6,299 M | 972 |
| ETH all crops (control) | 44 Mt | 8.75 B | 10.9 B | |

The nominal side is sound — SDN/NGA producer prices are ordinary, which is why the clip never touched
them. The **intld side** is the defect: 0.4.0 distributes the national FAOSTAT value over the SPAM
footprint by production *share*, so where SPAM holds almost nothing (Sudan: 0.05 Mt against ~15 Mt in
FAOSTAT; Nigeria bananas coded as plantain) the whole national value lands on a sliver. Same family as
#38. Filed as **#39**. Not reachable from 0.4.2 and out of this dispatch's scope; both CDH denominator
records now say the SDN and NGA-banana constant-dollar rows must not be used.

**Gate change (`R/checks/vop_cross_basis_gate.R`, this commit).** Both bases share the SPAM footprint,
so nominal ÷ SPAM tonnage is an implied price comparable across countries per crop. An out-of-band pair
whose implied price sits within 5x the crop median has a sound nominal side and is reported as an
**intld-side residual** (SPAM footprint #39, millet split #38) — not a FAIL unless
`--fail-on-intld-side`. A pair whose implied price is off is a nominal-side FAIL. On the live res-25
object this yields: nominal-side FAIL = exactly the G6 set (ZWE/ZMB wheat, oil-palm block); intld-side
residual = SDN x13, NGA banana, pearl-millet x6. On your regenerated tables the nominal-side list must
be **empty**.

**Your new movers** (BDI/RWA arabica-coffee ~15x up, GIN sweet-potato 8x, GIN cotton 5.7x, COD coffee
4.4x up; ZWE/MRT/GIN sugarcane 0.07-0.10 down, sugarcane 0.49 continental, cowpea 0.66) are the clip
working in both directions on prices that were themselves artefacts (Zimbabwe sugarcane 1,960 -> 141;
coffee quoted per kg of cherry rather than per tonne of green bean gets clipped low-side and refilled).
Accepted; they go into the CDH note as "changed against the previous publication" once C0-4 lands.
No re-bake needed — the regenerated tables are unchanged.

### C0-3 (gates only, re-run; minutes)

```bash
cd <hazards_prototype> && git fetch origin && git checkout develop && git pull --ff-only && git log -1 --oneline
Rscript R/checks/vop_cross_basis_gate.R --res 0.25;  Rscript R/checks/vop_cross_basis_gate.R --res 0.05
```
The gate auto-detects 0.4.2's audit CSV (`mapspam_pro_dir/fao_prices/crop_price_nominal-usd-2021-t_fill-sources_<tag>.csv`,
written by C0-1) as its INDEPENDENT world-price reference and logs `world price reference: <path> (N crops)`;
if it logs the `WARN: no world-price reference` fallback instead, STOP - the side test is then blind to a
majority-contaminated crop (oil palm on the live object) and the result means nothing.
**Expect:** `ok: no material pair outside [1/10, 10] has its nominal side off`, `ok: every per-crop
median within [1/5, 5]`, a `residual:` block listing SDN, NGA banana and the pearl-millet countries
with sound implied prices, **GATE PASS** at both resolutions. Anything in the nominal-side FAIL block:
STOP and paste it. Then STOP for the second GO — C0-4 (publish reference + family, both res) is
unchanged from the block above.

---

## cglabs response — C0-3 re-run, cross-basis GATE PASS at both res; STOP for second GO (2026-09-28)

Repo at `13d7b56`. Gates only, no re-bake (regenerated tables from `1abcb4c` unchanged). Nothing
published; parked set + regenerated tables intact.

**Both resolutions GATE PASS.** The new side-test does exactly what it was built to.

- **World-price reference detected (not WARN)** at both res — `crop_price_nominal-usd-2021-t_fill-sources_res-{25,05}.csv (35 crops)`, the audit CSV 0.4.2 wrote in C0-1.
- **nominal-side FAIL block EMPTY** at both res: `ok: no material pair outside [1/10, 10] has its nominal side off`.
- `ok: every per-crop median within [1/5, 5]` at both res.
- **residual (sound nominal -> intld-side #39/#38):** res-25 28 pairs (CAF,ETH,KEN,MOZ,MWI,NER,NGA,RWA,SDN,TGO,UGA,ZWE); res-05 26 pairs (BDI,ETH,KEN,MWI,NGA,RWA,SDN,TGO,UGA,ZWE). SDN x13, NGA banana, pearl-millet KEN/ETH/RWA/UGA/MWI throughout.

The `implied_usd_t` vs `ref_usd_t` columns confirm the residual's nominal side is sound: SDN wheat
391 vs 293, SDN groundnut 825 vs 1110, NGA banana 318 vs 576, TGO oilpalm 247 vs 149 — all within ~5x
of the independent reference. So they are the SPAM-footprint (#39) / millet-split (#38) intld-side
defects, correctly demoted to residual. **TGO oilpalm**, which the crop-median-only test false-flagged
(oil palm was majority-contaminated on the live object), is now correctly a residual, not a FAIL.

Nominal/intld material ratio median 1.25 (constant-USD vs I$ spread), 5-95% [0.40, 2.98]; livestock
median 1.25. qaqc intld unchanged and the four publisher dry-runs (rows identical to the 2026-09-25
objects) already pasted under `1abcb4c` — unaffected by the gate-only change.

**C0-3 is clean. STOP for the second GO.** C0-4 (publish reference + family, both res, then S3
verify) unchanged. On GO I run it, verify from S3 by re-download (md5 + rows), and hand the CDH
"correction in progress" -> "changed against previous publication" edit and Block C release back to
macbook.
