# DISPATCH — cglabs — #13: the full R/2 + R/3 rebake, and the five-tier publish

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### RESPONSE 2026-10-08 (B0) — cglabs — on `09a396c`: 7/7 fixtures PASS; `probe_r2_5_2_vec.R` **PROBE PASS** on node terra (haz_sum and ensemble mean/stdev identical), so `USE_R2_5_2_VEC` stays ON; THI_max poultry_highland Extreme = 89 present; reference + both family twins carry the B5 mtimes at both resolutions; 123 T free of 192 T. B5 clean-up done. B1 launched 11:2x.

```
PASS vop_allocate | PASS fao_code_join | PASS r3_physical_tiers | PASS publish_tier_gates
PASS gate_zonal_basis | PASS crop_heat_interactions | PASS qaqc_denominator
haz_sum PASS — functionally identical (values + missingness + any_haz)
ensemble PASS — terra::mean/stdev identical to per-layer loop
PROBE PASS — all §5.2 vectorizations functionally identical
```

Logs: `logs/r13_b0_fixtures_20261008_111955.log` (+ one `.out` per fixture),
`logs/r13_b0_probe_r2_5_2_vec_20261008_111955.log`.

Pre-conditions: `metadata/haz_classes.csv:73` = `THI_max,Extreme,3,>,poultry_highland,89`.
`Data/exposure/{exposure_adm_sum_spam20-20_glw420-20,vop_intld15-2021_adm_sum_…,vop_nominal-usd-2021_adm_sum_…}_res-{05,25}.parquet`
mtimes 2026-10-07 19:57-20:02 (the B5 A3 run). `df`: 123 T free of 192 T (37 % used).

**B5 clean-up (authorised in the archived thread's close block):** deleted on the node, plain `rm`:
`fao_prices/crop_factor_intld15-2021-t_res-25.tif` (26.7 MB), `sandbox/stage0_harness_20261007_184538/`
(557 MB; symlink targets `nex-gddp-cmip6/` and `chirps_wrld/` confirmed intact afterwards),
`sandbox/b5_old_adm_sum_20261007_193653/` (7 MB). **Nothing on S3 touched**; the
`s3://digital-atlas/sandbox/backup/issue9_20261008_07*/` backups stay.

**B1 launched:** `FORCE_OVERWRITE=1 RUN_R2_RUN3=1 RUN_R2_RUN5_3=1 Rscript R/2_calculate_haz_freq.R`,
`logs/r2_rebake_20261008_111955.log`; an RSS sampler (`logs/r13_rss_20261008_111955.log`, every 60 s,
all R processes) runs beside it for the peak-memory line. Baseline for the +38 % invariant, from the
last bake (`logs/r2_ens_5_3_20260914_074411.log`): §5.2 132 combinations × 306 scen_x_model; §5.3
annual 268.3 min, jagermeyr 248.5 min (44 crops × 3 sev × 20 models). Expect ~370 / ~343 min.

---

> **RELEASED 2026-10-08. The precondition is met: B5 completed A0-A6, published nine keys and verified
> every one MD5 == ETag off S3** (`archive/dispatches/DISPATCH_cglabs_b5_fao_code_join.md`). This
> thread is now live — start at B0.

**What B5 changed under this bake, and what it means for the gates here:**

- **The constant-I$ reference is now 240.24 B I$, up from 200.28 (+20.0 %)** — the recovered composite
  value. Every intld product this bake builds inherits that. It is correct, and it is large; the
  CDH records and the note to Brayden must state it.
- **The nominal side is unchanged** (201.92 B, and the family key published row-identical). So the
  `vop_usd` tier, which is the one step here with a live object to drift against, should show
  **G6 ≈ 1**. A large move on the usd tier would mean something other than B5 moved, and is a stop.
- The physical twins the new tiers gate against are confirmed present in the res-25 table:
  `prod`/`t`, `harv-area`/`ha`, `number`/`number`.
- The crop QAQC denominator was fixed mid-thread (it medianed a composite group's items instead of
  summing them). It now reads **0.98 / 0.99** with livestock at 242/242. If it reads near 1.17 again,
  the checkout is stale.

Thread opened 2026-10-07 (macbook). Runbook: `R/NEXT_FULL_REBAKE.md`. Context and decisions:
`HANDOVER_2026-10-07.md` §2. Roughly a working day per stage — read the whole dispatch before
launching anything.

**There is no Stage-0 refresh before this bake** (settled 2026-10-07): water-balance v2 is blocked on
inputs (#45) and the #14 HSH fix runs afterwards with its thresholds. R/2 reads today's indices.

---

## What is different from the last bake

Five changes land in this run. Each is pinned by an off-node fixture; run them all at B0.

| change | effect on the run |
|---|---|
| **#41 physical tiers** — `prod_t` and `head_n` now in R/3's `to_do_list` | R/3 §4 does **5** variables, not 3 |
| **#25(a)** a third crop interaction row, `NDWS+NTxM+NDWL0` | **§5.3 and R/3 §4 are about 38 % larger than last time.** Expected, not a runaway |
| **#25(b)** crop heat family `NTxS` → `NTxM` | published `hazard_vars` renamed; no extra compute |
| **B5** FAO code join + CAF/GIN pins | already landed in the exposure reference by the B5 thread |
| **publisher** `--variables` / `--model` axes, and the new **G6b** gate | the publish step is five commands, not one |

---

## Gates — stop at every one

### B0. Fixtures and pre-conditions, before anything long

```
cd /home/jovyan/atlas/hazards_prototype
git fetch && git log --oneline -1 origin/develop && git pull
for f in vop_allocate fao_code_join r3_physical_tiers publish_tier_gates gate_zonal_basis crop_heat_interactions qaqc_denominator; do
  echo "--- $f"; project_dir=$PWD Rscript R/checks/fixture_$f.R >/dev/null 2>&1 && echo PASS || echo FAIL
done
```

All seven must PASS. Then, and this one is **not optional**:

```
Rscript R/probe_r2_5_2_vec.R
```

`USE_R2_5_2_VEC` defaults **ON** and its terra identity probe has only ever run on macbook terra
(runbook item 3). **If it fails, export `USE_R2_5_2_VEC=0` for the whole bake and say so in the
RESPONSE.** A silent vectorisation difference in §5.2 would propagate into every tier.

Confirm before launching:
- `metadata/haz_classes.csv` has `THI_max,Extreme,3,>,poultry_highland,89` (the #13 fix itself);
- the exposure reference and both family twins on disk carry the B5 mtimes, at **both** resolutions;
- disk headroom — the last report was 123 T free of 192 T, and this run adds two new tier trees.

### B1. R/2 — both timeframes

```
export FORCE_OVERWRITE=1 RUN_R2_RUN3=1 RUN_R2_RUN5_3=1
nohup Rscript R/2_calculate_haz_freq.R > logs/r2_rebake_$(date +%Y%m%d_%H%M%S).log 2>&1 &
```

**`§3` and `§5.3` are toggle-only — `FORCE_OVERWRITE` alone does NOT enable them** (`run3` at
`R/2:658`, `run5.3` at `R/2:705`). Without both toggles the crop-stack and per-crop interaction
producers are skipped, `haz_risk/` never refreshes, and R/3 §4.1 silently consumes stale stacks.
`run5.2` already follows `FORCE_OVERWRITE`, so `RUN_R2_RUN5_2` is not needed separately.

**Report from the log, as invariants:**
1. The printed `crop_interactions` table — **three** crop rows, heat `NTx35`, `NTxM`, `NTxM`. If it
   shows two rows, the checkout is stale: stop.
2. The `0.2.2.1)` ecocrop line — either "every SPAM commodity matched" or the no-match table. We
   expect `tomatoes` to appear; **anything else in that list is a finding**.
3. §5.3 combination count and wall-clock against the last bake. **Expect roughly +38 %**, because
   crops went from two interaction rows to three. Materially more than that is worth stopping on.
4. Peak RSS. No swap, 376 GiB, OOM is an instant kill.

### B2. R/3 — five variables, both timeframes

**Park, do not FORCE.** `mv` the existing `hazard_risk_{vop,vop_usd,ha}/<timeframe>` aside rather
than overwriting, so a failed run leaves the old product intact and comparable. `hazard_risk_prod`
and `hazard_risk_n` are new and start empty.

```
nohup Rscript R/3_freq_x_exposure.R > logs/r3_rebake_$(date +%Y%m%d_%H%M%S).log 2>&1 &
```

Defaults are correct: `R3_CROP_VOP_USD=2021`, `run4.1` on, `run4.2` on. **Do not set
`R3_ALLOW_41_FAILURES`** — a silent §4.1 failure is what hid #9 for months.

**A clean exit is not evidence of a clean run.** Before trusting it, check all four:
- `failed_risk_x_exposure_*.txt` — absent or empty, for every one of the five variables;
- `skipped_not_in_exposure_*.txt` — expect livestock × `prod_t`, livestock × `harv-area_ha` and
  crops × `head_n` as classified NOT_APPLICABLE skips. **That is correct behaviour, not a failure.**
  Any crop missing from a crop tier is a finding;
- per-variable §4.1 elapsed minutes — five non-trivial entries;
- output mtimes against the run start, per variable and timeframe.

### B3. R/2.2 and the validators

R/2.2 re-runs and the CR-093 desert mask carries automatically. Then:

- `Rscript R/validate_cr093_real.R` (note: it checks structure, **not value ranges** — see
  `archive/dispatches/ISSUE_cr093_nan_zeroprecip.md`, so a PASS is not a statement about the numbers)
- **the #13 check itself:** `poultry_highland` **Extreme** exposure must **DROP** against the parked
  product — 89 °C is a higher bar than 79. Report the before/after at adm0 for a few countries.
  **If it rises or is unchanged, stop: that is the bug this bake exists to fix.**

### B4. Gates, before any publish

```
Rscript R/checks/usd_total_vs_reference.R --iso3 all          # --basis admin2 is now the default
Rscript R/checks/vop_cross_basis_gate.R --res 0.25
```

- `usd_total_vs_reference` now gates on the **admin2 basis** and *measures* the admin0-vs-admin2 gap
  separately (#18's border cells, as a number). **Invariant: material ratios near 1.** The printed
  zonal-basis line is informational.
- `vop_cross_basis_gate`: the four CAF/GIN coffee rows are **pre-registered** in
  `metadata/cross_basis_expected_residuals.csv` — expected, because B5 corrected the FAO tables and
  deliberately not SPAM. **Any other new residual is a finding.**

Then stamp ensemble membership from the `hazard_risk` source folder:
`Rscript scripts/stamp_ensemble_membership.R`. **18 members, or the publisher refuses.**

### B5. Publish — five variables, in the no-regrets order

One command per step, each independently gated, **each with its own `--drift-exposure` basis**. Run
every one with `--dry-run` first and report the gate output before the live write. Background the
live ones and log to `logs/` — a publish was SIGTERM'd mid-run on 2026-09-28.

Bases (all `_res-25`, the grid the bake ran on), under `<exposure_dir>`:

| tier | `--drift-exposure` basis |
|---|---|
| `vop_usd` | `vop_nominal-usd-2021_adm_sum_spam20_glw420_res-25.parquet` |
| `vop_intld` | `vop_intld15-2021_adm_sum_spam20_glw420_res-25.parquet` |
| `ha`, `prod_t`, `head_n` | `exposure_adm_sum_spam20-20_glw420-20_res-25.parquet` |

```
# 1. the proven path first, against a live object
Rscript scripts/r3_publish_tiers.R --variables vop_usd   --drift-exposure vop_usd=<usd twin>
# 2-4. new keys: G6 cannot run (no live object), so G6b is the gate
Rscript scripts/r3_publish_tiers.R --variables vop_intld --drift-exposure vop_intld=<intld twin>
Rscript scripts/r3_publish_tiers.R --variables ha        --drift-exposure ha=<reference>
Rscript scripts/r3_publish_tiers.R --variables prod_t    --drift-exposure prod_t=<reference>
Rscript scripts/r3_publish_tiers.R --variables head_n    --drift-exposure head_n=<reference>
# 5. then the same five with --timeframe annual
```

**Stop after step 1 and report** before doing 2-5. It is the only step with a live object to drift
against, so it is the one that proves the bake end-to-end against something known.

**`--model ENSEMBLE` and per-GCM are step 6 and are NOT in this dispatch.** They widen the notebook
read contract and need Brayden's confirmation first.

Do not pass `--allow-schema-drift`, `--allow-value-drift`, `--allow-exposure-mismatch`,
`--no-exposure-gate` or `--skip-gates` unless a later block here says so.

### B6. Post-publish

- the CR-068 probes: `probe_no_hazard_arithmetic_quick.sh <ISO3>` and
  `probe_cross_parquet_vop_drift.sh <ISO3>` against canonical S3. **Both have known bugs that read as
  data defects** — see the runbook's post-bake section before interpreting either.
- the self-contained check instead: `any + none` from the product itself, crossed to
  `crop-livestock_all_res-25.parquet` (the unsuffixed key is the 0.05° alias; #9/#12 closed on
  exactly that mistake).
- diff local against S3 per key. The uploader always overwrites and never skips, so a stale key needs
  an explicit delete — and **`s3fs` deletes are PERMANENT**, so back up first and use `paws`
  `delete_object()` without a VersionId if one is genuinely needed.

---

## What NOT to do

- **Do not start before the B5 thread finishes.** The reference must be the republished one.
- **Do not set `R3_ALLOW_41_FAILURES`, `SKIP_R3_4_1` or `--skip-gates`.**
- **Do not FORCE over the existing R/3 outputs** — park them.
- **Never `s3fs::s3_file_delete()` / `s3_dir_delete()`.** Permanent, no delete marker.
- **Do not publish the model axis or `value_sd`** — out of scope here.
- **Do not improvise a fix at any gate.** Stop and describe what you see.

## Open questions this bake forces, for the macbook side — not node work

- **#37:** MapSPAM 2020 is CC-BY-SA-4.0 and share-alike propagates into `hazard_exposure`. Four new
  CDH records get authored off this bake, so the licence question has to be answered then.
- **#20:** publish the generated threshold table from `metadata/haz_classes.csv` with the release.
- **Consumer note:** the crop-specific `hazard_vars` is renamed `PTOT-L+NTxM+PTOT-G` and
  `NDWS+NTxM+NDWL0` is new; `PTOT-L+NTxS+PTOT-G` is retired. The live V1 notebook reads
  `NDWS+NTx35+NDWL0`, unchanged. Brayden needs this with the key list on `data-management#2`.

## Reporting

Prepend a `### RESPONSE <date>` block per block completed — do not wait until the end; this runs for
days. Then verify it landed on `origin/develop`: `git fetch`; `git log origin/develop..HEAD` empty;
`git show origin/develop:DISPATCH_cglabs_issue13_rebake.md | grep -c RESPONSE`.

Commit trailer: `Co-Authored-By: <your own model name> <noreply@anthropic.com>`.
