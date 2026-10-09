# DISPATCH — cglabs — #13: the full R/2 + R/3 rebake, and the five-tier publish

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### RESPONSE 2026-10-09 (B1 interim — FINDING) — cglabs — the #25(a) third crop row `NDWS+NTxM+NDWL0` is in the table but PRODUCES NOTHING: §5.2 wrote 129 combinations and none pairs a crop-specific NTx threshold with NDWS/NDWL0; §5.3 is writing `PTOT-L+NTxM+PTOT-G` stacks (1,023 so far) and zero `NDWS+NTxM+NDWL0`. Cause located (haz_classes.csv has NDWS/NDWL0 only for `generic`; the combination builder ignores `*_fixed`). Invariant 3 (+38 %) will therefore NOT hold. R/2 left running — the omission does not corrupt the rest. Second item for B2: 1,980 stale `NTxS` stacks per timeframe would be ingested by R/3 §4.1 unless parked.

**Where the run is** (`logs/r2_rebake_20261008_111955.log`, annual timeframe): §1 193.0 min, §2 64.3 min, §2.1 11.4 min,
§3 36.8 min, §4 39.4 min, §4.1 2.9 min, **§5.2 514.5 min** (full recompute under FORCE; the 2026-09-14 figure of
1.8 min was a skip-existing run, so there is no like-for-like baseline), §5.3 started 02:16 and is writing to
`Data/hazard_risk/annual/` (~55 files/min, 15 workers). Peak RSS so far 260.8 GiB (16:20, §3, 20 workers);
§5.2/§5.3 sit at 18-37 GiB. jagermeyr follows.

**Finding 1 — the new interaction row is dropped before it reaches §5.2.**

Evidence, from the files:
- §5.2 fresh output (`hazard_timeseries_int/annual`, 43,860 files, 129 distinct combinations): the only
  `NDWS+NTx*+NDWL0` names are the three generic severities `NDWS-G15+NTx35-G7+NDWL0-G2`,
  `NDWS-G20+NTx35-G14+NDWL0-G5`, `NDWS-G25+NTx35-G21+NDWL0-G8` (1,020 files = 1 combo family × 3 sev × 340
  scen-model). Crop-specific heat thresholds (`NTx20…NTx38`, 13 of them, × G7/G14/G21) appear ONLY paired with
  `PTOT-L…+PTOT-G…`. No `NDWS-G15+NTx30-G7+NDWL0-G2`-shaped file exists.
- §5.3 fresh output after 72 min: `PTOT-L+NTxM+PTOT-G_int.tif` 1,023; `NDWS+NTx35+NDWL0_int.tif` 60 (generic
  crop only); `NDWS+NTxM+NDWL0_int.tif` **0**. The k-loop writes every combo of a (crop, sev, model) together,
  so 1,023 of one and 0 of the other is structural, not ordering.
- The log's table does carry the row (`3: NTxM NDWL0 NDWS FALSE TRUE TRUE crop`), so the checkout is current.

Cause (`R/2_calculate_haz_freq.R:468-485`): `combinations_c` is built per (crop, severity) by mapping each
row's `heat_simple / dry_simple / wet_simple` through `haz_class[crop == crop_focus & description ==
severity_focus]` with `replace_exact_matches()`, then `combinations_c[!is.na(heat) & !is.na(dry) & !is.na(wet)]`.
The `*_fixed` flags are never consulted there. `metadata/haz_classes.csv` carries `NDWS` and `NDWL0` **only
under `crop = generic`** (3 rows each; 0 rows for any named crop), so for every real crop row 3 maps `dry` and
`wet` to NA and is dropped. Row 2 survives because `PTOT_G/PTOT_L` are per-crop in the CSV and `NTxM`
thresholds are generated per crop from ecocrop at run time (`:228-333`). Row 1 survives only for the generic
crop, which is why `NDWS+NTx35+NDWL0` has exactly 60 stacks. The fixture `fixture_crop_heat_interactions.R`
pins the table and the labels, not the builder, so it passed.

**Not changed.** No code edited, R/2 not stopped. What a fix needs, for the macbook: the builder has to
resolve a `*_fixed = TRUE` hazard from the `generic` rows (that is what "fixed" means), or the CSV needs
per-crop NDWS/NDWL0 rows. Once fixed, the missing combos can be produced without a full re-run: §5.2 with
`overwrite = FALSE` writes only the absent names (the runbook's pre-delete + overwrite=FALSE pattern, nothing
to delete here), then §5.3 the same way — §5.3's own cost for the missing ~1,000 stacks per timeframe is
small next to today's. That can run after this R/2 completes and before B2.

**Invariant 3 consequence:** §5.3 will come in near the 2026-09-14 figure (268 / 249 min), not +38 %, because
the third row contributes nothing. That is the deviation; it is explained.

**Finding 2 — stale retired-family stacks will be read by R/3 §4.1 unless parked.** `hazard_risk/annual` and
`hazard_risk/jagermeyr` each hold **1,980 `*_PTOT-L+NTxS+PTOT-G_int.tif`** from the last bake. FORCE never
touches them (different name), and R/3 §4.1 takes `list.files(haz_risk_dir, ".tif$")` through
`.rebake_scope`, which is identity by design (`R/3:565`, `.rebake_scope <- function(files) files`). So B2 as
written would ingest the retired `NTxS` family alongside `NTxM` and the tiers would carry both labels —
contradicting the "RETIRED, not reused" assertion the fixture makes about the published set. Proposed B2
pre-step, for the macbook to confirm: `mv` the 1,980 `*NTxS*_int.tif` in each timeframe dir into
`hazard_risk/<timeframe>/_parked_NTxS_<stamp>/` (a subdirectory is not listed by the non-recursive
`list.files`), alongside the dispatch's parking of `hazard_risk_{vop,vop_usd,ha}`. Same for any other
retired name found when the run ends (none seen so far).

Also for the record: `hazard_timeseries_int/annual` holds 33,911 pre-run files beside the 43,860 fresh ones;
§5.3 reads by exact combo name from `haz_int_file_tab`, so stale extras there are inert.

---

### RESPONSE 2026-10-08 (B1 launch) — cglabs — R/2 running since 11:22 with `FORCE_OVERWRITE=1 RUN_R2_RUN3=1 RUN_R2_RUN5_3=1`. One deviation: the dispatch's bare `Rscript R/2_calculate_haz_freq.R` dies at once (`object 'ms_codes_url' not found`) — R/2 does not source setup itself; relaunched in the AGENTS.md §2 form. Invariants 1-2 already hold: three crop rows (NTx35 / NTxM / NTxM); ecocrop "every SPAM commodity matched" (no no-match list, so no `tomatoes` either). 3-4 follow at completion.

**The deviation.** First launch, exactly as B1 is written: log ends at line 2 with
`Error: object 'ms_codes_url' not found`. `ms_codes_url` is defined in `R/0_server_setup.R:621`;
`R/2_calculate_haz_freq.R` lists setup as a prerequisite in its header (`:37`) and never sources it
(`:107` sources only `haz_functions.R`). The 2026-09-14 bake's log opens with the setup banner, so that
run was launched with setup sourced. Relaunched from the repo root as

```
FORCE_OVERWRITE=1 RUN_R2_RUN3=1 RUN_R2_RUN5_3=1 nohup Rscript -e 'source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); source("/home/jovyan/atlas/hazards_prototype/R/2_calculate_haz_freq.R")' > logs/r2_rebake_20261008_111955.log 2>&1 &
```

(setup's `Sys.setenv(project_dir = getwd())` at `:111` is what R/2's `Sys.getenv("project_dir")` needs,
so the cwd at launch matters: repo root). The aborted log is kept as
`logs/r2_rebake_20261008_111955_ABORTED_nosetup.log`. **B2's R/3 line has the same shape** — R/3 also
reads `ms_codes_url` (`R/3:294`) — and will be launched the same way unless told otherwise.

**Invariant 1 — crop_interactions: three rows, heat NTx35 / NTxM / NTxM.** Checkout is current.

```
   heat_simple wet_simple dry_simple heat_fixed wet_fixed dry_fixed   type
1:       NTx35      NDWL0       NDWS       TRUE      TRUE      TRUE   crop
2:        NTxM     PTOT_G     PTOT_L      FALSE     FALSE     FALSE   crop
3:        NTxM      NDWL0       NDWS      FALSE      TRUE      TRUE   crop
   (animal: THI_max+NDWL0+NDWS, THI_max+PTOT_G+PTOT_L — two rows, unchanged)
```

**Invariant 2 — ecocrop:** `every SPAM commodity matched an ecocrop species`. The dispatch expected
`tomatoes` in a no-match table; there is no table. Reporting as seen, not reconciled — if `tomatoes`
was expected to be unmatched, something matched it (the 44-crop list printed above it starts
`arabica coffee / Coffea arabica, banana / Musa acuminata, …`).

Invariants 3 (§5.3 count + wall-clock vs 268 / 249 min; expect ~+38 %) and 4 (peak RSS, from
`logs/r13_rss_20261008_111955.log`, 60 s samples of all R processes) are reported when R/2 finishes
— hours, both timeframes. `timeframes = annual jagermeyr` confirmed in the log.

---

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
