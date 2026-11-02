# DISPATCH — cglabs — #54 + #52: constant-I$ placeholders and the TLU slot

Append-only, newest block on top. Read `AGENTS.md` first, then `HANDOVER_2026-10-23.md` §3 items 2 and 5.

---

### RESPONSE — cglabs 2026-11-02 — **A and B complete, every gate PASS.** GATE 0 PASS; the four flagged rows are exactly LBY/SLE cattle_meat, LSO poultry_meat, SWZ sheep_meat; GATE 054-REF PASS 3/3 at both resolutions. Two findings: A2 shows the **raster** already disagrees for DJI and ESH, and the rebuilt res-25 twin is now **post-#52**, which supersedes the `G6B_NO_PRODUCT_CROPS` workaround in `DISPATCH_cglabs_obs_s3_r3.md`. No S3 writes, R/3 not run, parked copies kept.

Stamp `20261102_185857`. `$PARK = {NWD}/Data/_parked_issue54_20261102_185857` (68 MB, six parquets + `fao_vop` + three `variable=vop_intld15-*` dirs). Logs `logs/issue54_*_20261102_185857.log`.

## A

**A0.** HEAD `931e5a7` (`peetmate`, the RESPONSE (5) commit on the S3/R3 thread). No other pipeline R job was running. All **fourteen** fixtures print `ALL ... PASSED`:

```
fixture_crop_heat_interactions  fixture_fao_code_join      fixture_gate_054
fixture_gate_zonal_basis        fixture_interaction_builder fixture_intld_guard
fixture_livestock_price         fixture_price_fill          fixture_publish_tier_gates
fixture_qaqc_denominator        fixture_r3_physical_tiers   fixture_tlu_slot
fixture_upstream_run_params     fixture_vop_allocate
```

(`fixture_upstream_run_params` needs about 4 minutes — my first pass timed it out at 100 s and printed `Terminated`; re-run alone it passes. Not a failure.)

CSV `{glw2020_pro_dir}/fao_vop/glw4-2020_vop_intld15-2021-prices-used.csv`, mtime **2026-10-21 16:54:23**, columns `iso3,glw3_name,atlas_name,value,glw3_no,variable`. **No `intld_source` column** — the pre-guard vintage, so gate 0 can reproduce the live values.

**A1 / A3 gate 1.** Probe ran clean, 0.6 min, wrote 1,485 rows to
`{NWD}/checks/issue54_intld_guard_20261102_185857.csv` (190 KB, committed under `R/checks/041_outputs/`).

**A3 gate 2 — GATE 0 PASS.** 495 rows; live total **89.020 B**, probe OLD total **89.020 B**, **0 rows differ**.

**A3 gate 3 — invariants.**

| invariant | result |
|---|---|
| flagged rows are exactly LBY cattle_meat, SLE cattle_meat, LSO poultry_meat, SWZ sheep_meat (y2021) | **4 rows, exactly those** |
| every non-flagged row unchanged | **TRUE** (0 rows differ) |
| every flagged row: nominal > 0, intld_new > 0, intld_new == nominal / basis_median | **TRUE** |
| NEW / OLD continental > 1 and < 1.01 | **1.00103** (89.020 B → 89.112 B, +0.092 B) |
| per-(iso3, class) factor table lists exactly four pairs | **four changed pairs** — see the note below |

The flagged table, y2021:

| iso3 | atlas_name | glw3 | production_t | nominal_M | intld_old | intld_new_M | basis_ratio | basis_median | factor |
|---|---|---|---|---|---|---|---|---|---|
| LBY | cattle_meat | cattle | 6,330.00 | 37.95 | 1,000 | 48.34 | 38,000 | 0.785 | 48,340 |
| SLE | cattle_meat | cattle | 11,108.44 | 32.83 | 1,000 | 41.81 | 32,800 | 0.785 | 41,810 |
| LSO | poultry_meat | poultry | 585.15 | 1.05 | 2,000 | 0.61 | 525 | 1.720 | 305.4 |
| SWZ | sheep_meat | sheep | 165.53 | 0.85 | 0 | 0.77 | NA | 1.100 | Inf |

`intld_source`, y2021: `fao gpv constant-I$` 386 rows / 89.0200 B, `NA` 105 rows (no production), **`basis fallback` 4 rows / 0.0915 B**. Three flagged rows had `intld_old > 0` (ratio beyond the band), one had `intld_old <= 0`. Item medians: cattle_meat 0.7852, cattle_milk 1.4520, goat_meat 0.7426, goat_milk 1.7320, pig_meat 1.3110, poultry_eggs 1.9520, poultry_meat 1.7190, sheep_meat 1.0990, sheep_milk 1.6120.

**Note on the fourth invariant, reported rather than stopped on.** The per-(iso3, glw3 class) factor table prints **36 rows**, not four. Thirty-two of them are `old = 0, new = 0, factor = NaN` — countries with no livestock of that class, identity rows. Exactly **four** rows carry a change, and they are the four expected:

| iso3 | class | old | new | factor |
|---|---|---|---|---|
| LBY | cattle | 60,195,000 | 108,530,133.2 | 1.803 |
| SLE | cattle | 61,149,000 | 102,962,163.2 | 1.684 |
| LSO | poultry | 1,739,000 | 2,347,705.1 | 1.350 |
| SWZ | sheep | 0 | 772,261.8 | Inf |

I read this as the invariant holding — four pairs move — with the table printing unfiltered. I did not treat it as a stop, and I changed nothing. If you meant the printed table itself to have four rows, that is a one-line filter in the probe's reporting, and your call.

**Other year sets.** y2015: OLD 77.549 B → NEW 77.902 B, **6** flagged (GNQ cattle_meat and LBY sheep_meat on top of the four). y2020: OLD 86.606 B → NEW 86.695 B, **4** flagged, the same four. Only y2021 is gated.

**A2 — the TLU rasters. Finding: the raster already disagrees.**

The command as written stops on `object 'geoboundaries' not found` — `R/0_server_setup.R` defines `geo_files_local` but not `geoboundaries`, which 0.4.1 builds at its lines 88-93. I rebuilt it exactly as 0.4.1 does (`read_parquet(geo_files_local[1]) |> st_as_sf() |> vect() |> aggregate("iso3")`) and changed nothing else. Layer names are the expected `<species>_<system>`.

```
DJI tropical: total layer    59139 | weighted species   363300 | ratio 0.163
ESH tropical: total layer     4139 | weighted species    52681 | ratio 0.079
KEN tropical: total layer 16290303 | weighted species 16290303 | ratio 1.000
```

So **0.4.1 is where DJI and ESH diverge**, not 0.4.4 — the `total_*` raster layer itself is short against the weighted species sum, and the table inherits it. KEN reconstructs exactly. ESH (0.079) sits on your live-reference figure (0.081); DJI reads 0.163 here against your 0.067, and these rasters were rebuilt by B1 minutes earlier, so the DJI figure is current, not the published one. A finding for #52's second item, as you framed it. No fix attempted.

## B

**B0.** `$PARK = {NWD}/Data/_parked_issue54_20261102_185857`, 68 MB. All six parquets present (res-05 and res-25 of `exposure_adm_sum_spam20-20_glw420-20`, `vop_nominal-usd-2021_adm_sum_spam20_glw420`, `vop_intld15-2021_adm_sum_spam20_glw420`), plus `fao_vop/` and `variable=vop_intld15-{2015,2020,2021}/`.

**B1 — 0.4.1, both resolutions.** Both exit 0 (driver log empty). res-25 19:02:13 → 19:03:41 (1.5 min), res-05 19:03:41 → 19:06:11 (2.5 min).

- Gate 1: `exposure grid = base_rast_nexgddp.tif | res 0.2500 | tag res-25` and `base_raster.tif | res 0.0500 | tag res-05`. Correct.
- Gate 2: the res-25 y2021 guard line and table are **identical to the probe**, same four rows, same values:

```
[19:03:02] [0.4.1]   y2021 (#54 intld guard): 4 row(s) beyond 4x the item median ratio or with constant-I$ <= 0 -> nominal / item-median ratio
     iso3   atlas_name    nominal intld_was basis_ratio basis_median basis_n   intld_now
1:    LBY  cattle_meat 37953097.5      1000       38000        0.785      50  48336133.2
2:    SLE  cattle_meat 32832105.3      1000       32800        0.785      50  41814163.2
3:    LSO poultry_meat  1049759.1      2000         525        1.720      49    610705.1
4:    SWZ   sheep_meat   848738.5         0          NA        1.100      49    772261.8
```

- Gate 3: the y2021 `price_source` table **diffs clean against the #47 block C response** (`archive/dispatches/DISPATCH_cglabs_issue47_livestock_price.md`, lines 827-834) — byte-identical after whitespace trim. The nominal side did not move.

**B2 — gate 047 re-read.** `GATE 047 FAIL: 3/6 checks pass`, which is the predicted shape:

| check | result |
|---|---|
| nominal-usd 2021 | **ok** — 109.070 B vs 109.070 B, 0 differ |
| nominal-usd 2015 | **ok** — 95.475 B vs 95.475 B, 0 differ |
| nominal-usd 2020 | **ok** — 101.538 B vs 101.538 B, 0 differ |
| intld15 2021 | FAIL — 89.112 B vs 89.020 B, **4 differ** |
| intld15 2015 | FAIL — 77.902 B vs 77.549 B, **6 differ** |
| intld15 2020 | FAIL — 86.695 B vs 86.606 B, **4 differ** |

y2021 differing rows: SWZ sheep_meat 772,261.8 vs 0; LBY cattle_meat 48,336,133.2 vs 1,000; SLE cattle_meat 41,814,163.2 vs 1,000; LSO poultry_meat 610,705.1 vs 2,000. **Exactly the four.** The y2015 six and y2020 four match the probe's flagged lists for those year sets row for row. **No other row differs anywhere.**

**B3 — 0.4.4, both resolutions.** Both exit 0. res-25 19:08 → 19:10:23, res-05 19:11 → 19:14:41.

- Gate 1: `section 2.9 (#52): 10194 TLU rows (crop total_tropical, total_highland) relabelled exposure=number/unit=number -> exposure=tlu/unit=tlu` at res-25, **14506** at res-05. §3.1 `units present in extraction` includes **`tlu`** at both; kept = `number, ha, t, nominal-usd-2021, intld15-2021, tlu`; dropped = the 2015/2020 vintages, as designed.
- Gate 2: **`GATE 054-REF PASS (res-25): 3/3 tables pass`** and **`GATE 054-REF PASS (res-05): 3/3 tables pass`**. Both exit 0. Per table: rows match with 0 unmatched and no duplicate keys; `#52 TLU slot ok` (res-25 10,194 → 10,194, res-05 14,506 → 14,506, values identical); `not-#54 rows identical` 0 differ; `#54 pairs vs probe NEW ok` (4 pairs, 0 off by > 2 %); `intld livestock total ok` (res-25 89.0160 B → 89.1075 B, res-05 89.0194 B → 89.1110 B, both = old + the probe's 91.529 M). The nominal twin carries `#54 pairs present but unchanged: ok` — it does not move.
- Gate 3a: `R/qaqc_vop_vs_faostat.R` — livestock median 1.00, **within 0.9-1.1 = 239/242**, worst LBY/cattle 1.80. Of the 36 livestock rows outside the band, **32 are `0 / 0 = NA`** (no livestock of that class at all) and the remaining **4 are exactly the guarded pairs**: SWZ sheep Inf (FAO I$ = 0, the placeholder), LBY cattle 1.803, SLE cattle 1.684, LSO poultry 1.350. Crop side unchanged: median 0.98, 37/50, 2 FAOSTAT quantity pins (CAF and GIN item 656). So the shape is 239/242 rather than 238/242 because the SWZ row is non-finite and falls outside the denominator, not because a fifth row moved.
- Gate 3b: `vop_cross_basis_gate.R` **GATE PASS at both resolutions**. res-25: 2 out-of-band pairs with no national constant-I$ allocation (border spill, #18 — TCD:yams, BEN:bean), 1 named expected residual (ETH:tea), 409 one-sided pairs. res-05: 4 named expected residuals (ETH:tea, TGO:oilpalm, CAF:robusta-coffee, GNB:maize), 557 one-sided pairs. **Nothing new, and `LBY`, `SLE`, `LSO`, `SWZ` appear nowhere in either output.**

## One consequence you will want before block C

0.4.4 has just rewritten `{NWD}/Data/exposure/exposure_adm_sum_spam20-20_glw420-20_res-25.parquet` (mtime 19:10:10), and the new file is **post-#52**:

| crop | exposure | unit | rows |
|---|---|---|---|
| total-highland | **tlu** | **tlu** | 5,097 |
| total-tropical | **tlu** | **tlu** | 5,097 |

That is the same file the observed publisher uses as its G6b basis. **It supersedes the `G6B_NO_PRODUCT_CROPS` workaround I proposed in `DISPATCH_cglabs_obs_s3_r3.md` RESPONSE (5) (b)** — the `head_n` basis no longer contains the two TLU aggregates, so the 74 `missing_product` rows should fall out on their own. I have **not** re-measured G6b against the new basis; say the word and it is a two-minute read-only check. The pre-#52 copy is in `$PARK/exposure/` if you want the old behaviour back.

## State

Rebuilt tables sit **locally, ahead of S3**, as intended. **No S3 writes of any kind** — no publisher, no `s3_upload.R`. **R/3 not run.** Parked copies untouched. No pipeline code, gate, fixture or probe edited; no tolerance loosened; `metadata/haz_metadata.csv` and `metadata/cdh/*.yaml` untouched. Nothing running.

---

### MACBOOK 2026-10-23 — A + B. Measure (A), then rebuild 0.4.1 + 0.4.4 locally and gate it (B). No S3 writes.

**What this is about.** Two changes to the exposure reference, both coded on `develop` and
fixture-tested on the macbook, neither run on real data yet:

- **#54** (`d4d7c3f`). FAOSTAT's "Gross Production Value (constant 2014-2016 thousand I$)" holds a
  placeholder, flag E, in every year, for four livestock rows in Atlas scope: LBY cattle meat = 1,
  SLE cattle meat = 1, LSO chicken meat = 2, SWZ sheep meat = 0, against thousands of tonnes of
  production. The reference and the `vop_intld15-21` tier carry LBY and SLE cattle meat at about
  $1,000. `R/price_fill.R::livestock_intld_guard()`, called from 0.4.1 §4.2, replaces a row whose
  nominal / constant-I$ ratio is beyond 4× the item's cross-country median, or whose constant-I$ is
  ≤ 0 while nominal > 0, with nominal / item-median ratio, and names it `intld_source = "basis
  fallback"`. On the #47 probe table (y2021) that band flags exactly those four pairs.
- **#52** (`1825d3a`). 0.4.4 filed the two Tropical Livestock Unit aggregates (`total-highland`,
  `total-tropical`) under `exposure = "number"` beside the species head counts. New §2.9 relabels
  them `exposure = "tlu"`, `unit = "tlu"`, values untouched, and stops if it finds none; `tlu` is in
  the §3.1 hard-stop unit list.

Nominal USD, crops, prod, ha, phys-area and the species head counts must not move by one unit.

**New on `develop` for this block:** `R/checks/probe_054_intld_guard.R` (the measurement),
`R/checks/gate_054_reference_diff.R` (rebuilt tables vs parked copies), fixtures
`fixture_intld_guard.R`, `fixture_tlu_slot.R`, `fixture_gate_054.R`. All fourteen fixtures pass on
the macbook. The probe and the gate have **not** run on real data; A3 gate 2 is the probe's first
real-data check.

**Pete's decisions (2026-10-23):** LSO and SWZ ride along with LBY and SLE; #52 is bundled. The
threshold-free parts (#14) are a separate thread.

#### A0. Pre-flight

```bash
cd /home/jovyan/atlas/hazards_prototype && git pull --ff-only && git log -1 --format='%h %an %s'
for f in R/checks/fixture_*.R; do printf '%s: ' "$(basename $f)"; Rscript $f 2>&1 | tail -1; done
pgrep -af 'Rscript' | grep -v pgrep || echo "no R jobs running"
Rscript -e 'suppressMessages(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")); f <- file.path(glw2020_pro_dir, "fao_vop", "glw4-2020_vop_intld15-2021-prices-used.csv"); cat(f, "\n exists:", file.exists(f), " mtime:", format(file.mtime(f)), "\n cols:", paste(names(data.table::fread(f, nrows = 1)), collapse = ","), "\n")'
```

Stop if: any fixture does not print `ALL ... PASSED`; another pipeline R job is running; the CSV
is missing. Report the HEAD hash and the CSV's mtime and columns. The CSV was written by the #47
block C run (2026-10-21); its columns must **not** include `intld_source` (if they do, 0.4.1 has
already run with the guard, and gate 0 below cannot reproduce the live values — stop and say so).

#### A1. The probe (background; a few minutes)

```bash
STAMP=$(date +%Y%m%d_%H%M%S); echo $STAMP > logs/issue54_stamp.txt
OUT=/home/jovyan/common_data/nex-gddp-cimp6_hazards/checks/issue54_intld_guard_${STAMP}.csv
mkdir -p "$(dirname "$OUT")" logs
EXPOSURE_RES=0.25 nohup Rscript R/checks/probe_054_intld_guard.R --top 30 --out "$OUT" > logs/probe_054_${STAMP}.log 2>&1 &
echo $! > logs/probe_054_${STAMP}.pid
# watch: while kill -0 $(cat logs/probe_054_${STAMP}.pid) 2>/dev/null; do sleep 30; done; tail -5 logs/probe_054_${STAMP}.log
```

#### A2. #52 item 2, read-only: do the TLU rasters reconstruct for DJI and ESH?

In the live res-25 reference, `total-tropical` is 0.067× (DJI) and 0.081× (ESH) the weighted sum of
the species rows; every other country reconstructs at 1.000 (macbook, DuckDB on S3, 2026-10-23).
This tells whether the raster already disagrees (0.4.1) or only the table does (0.4.4):

```bash
Rscript -e '
suppressMessages(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")); library(terra)
r <- terra::rast(file.path(glw2020_pro_dir, "livestock_number_number_res-25.tif")); cat("layers:", names(r), "\n")
for (iso in c("DJI", "ESH", "KEN")) {
  g <- geoboundaries[geoboundaries$iso3 == iso, ]; m <- terra::mask(terra::crop(r, g), g)
  s <- terra::global(m, "sum", na.rm = TRUE)[, 1]; names(s) <- names(r)
  tlu <- 0.7 * s["cattle_tropical"] + 0.1 * s["sheep_tropical"] + 0.1 * s["goats_tropical"] + 0.2 * s["pigs_tropical"] + 0.01 * s["poultry_tropical"]
  cat(sprintf("%s tropical: total layer %.0f | weighted species %.0f | ratio %.3f\n", iso, s["total_tropical"], tlu, s["total_tropical"] / tlu))
}'
```

If a layer name differs from `<species>_<system>`, print `names(r)` and adapt the four names only.
Report the three lines. No fix here; it is a finding for #52's second item.

#### A3. Gates — stop at each

1. **The probe evaluates without error.** If it dies inside the 0.4.1 slices, stop and paste the
   error and the 20 log lines before it. Do not edit 0.4.1 or the probe to get past it.
2. **GATE 0 PASS.** OLD must reproduce the live CSV's `value` per (iso3, atlas_name), 0 rows
   differing. If it fails, the probe stops itself; paste its table. Do not re-run 0.4.1 to make
   them agree.
3. **Invariants, not figures:**
   - the `basis fallback` rows are **exactly** LBY cattle_meat, SLE cattle_meat, LSO poultry_meat,
     SWZ sheep_meat (y2021). One more or one fewer is a stop;
   - every other row is unchanged (`every non-flagged row unchanged: TRUE`);
   - every flagged row has nominal > 0, intld_new > 0, and intld_new == nominal / basis_median;
   - NEW / OLD continental is > 1 and < 1.01;
   - the per-(iso3, class) factor table lists exactly four pairs.
   If one does not hold, report which. Do not fix it.

**If A3 passes in full, continue to B. If anything stopped, report and do not start B.**

#### B0. Park what B overwrites (copy, do not move)

```bash
STAMP=$(cat logs/issue54_stamp.txt)
Rscript -e 'suppressMessages(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")); cat(normalizePath(exposure_dir), normalizePath(glw2020_pro_dir), normalizePath(working_dir), sep="\n")'
# with EXPO, GLWP, WD set from those three lines:
PARK=$WD/Data/_parked_issue54_$STAMP; mkdir -p $PARK/exposure $PARK/glw_processed
cp -a $EXPO/exposure_adm_sum_spam20-20_glw420-20_res-{05,25}.parquet \
      $EXPO/vop_nominal-usd-2021_adm_sum_spam20_glw420_res-{05,25}.parquet \
      $EXPO/vop_intld15-2021_adm_sum_spam20_glw420_res-{05,25}.parquet $PARK/exposure/
cp -a $GLWP/fao_vop $GLWP/variable=vop_intld15-* $PARK/glw_processed/
ls -la $PARK/exposure; du -sh $PARK
```

Stop if any of the six parquets is missing. `$PARK/exposure` is the `--old-dir` for B3.

#### B1. 0.4.1 at both resolutions (background, ~5 min)

```bash
nohup bash -c 'for RES in 0.25 0.05; do EXPOSURE_RES=$RES FORCE_OVERWRITE=1 Rscript -e "source(\"/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R\"); source(\"/home/jovyan/atlas/hazards_prototype/R/0.4.1_create_livestock_exposure.R\")" > logs/issue54_041_${RES}_'$STAMP'.log 2>&1 || exit 1; done' > logs/issue54_041_$STAMP.log 2>&1 &
echo $! > logs/issue54_041_$STAMP.pid
```

Gates:
1. Both runs exit 0; each log's grid line is right (`tag res-25` / `tag res-05`).
2. Each log shows, per year set, a line `(#54 intld guard): N row(s) ...` followed by the flagged
   table. **Paste the y2021 line and table from the res-25 log.** It must list the same four rows
   as the probe, with the same `intld_new`. A fifth row, or a different value, is a stop.
3. The nominal side is untouched: the y2021 `price_source` table equals the one in the #47 block C
   response (`archive/dispatches/DISPATCH_cglabs_issue47_livestock_price.md`).

#### B2. Price gate from #47, re-used as a regression check

```bash
Rscript R/checks/gate_047_prices_vs_probe.R --since "$(date -d "-1 hour" '+%Y-%m-%d %H:%M')" 2>&1 | tee logs/issue54_gate047_$STAMP.log
```

Expected: the **nominal** CSVs still equal the #47 probe's C (that gate's rows 1-3 PASS). The
**intld** CSVs will now differ on the four flagged rows, so its rows 4-6 are expected to FAIL on
exactly those rows — paste the differing rows and confirm they are the four. Any other differing
row is a stop. (The gate predates #54; it is not being loosened, it is being read.)

#### B3. 0.4.4 at both resolutions, then the #54 reference diff gate

```bash
nohup bash -c 'export FORCE_OVERWRITE=1; for RES in 0.25 0.05; do EXPOSURE_RES=$RES Rscript -e "source(\"/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R\"); source(\"/home/jovyan/atlas/hazards_prototype/R/0.4.4_process_exposure.R\")" > logs/issue54_044_${RES}_'$STAMP'.log 2>&1 || exit 1; done' > logs/issue54_044_$STAMP.log 2>&1 &
echo $! > logs/issue54_044_$STAMP.pid
# after both exit 0:
PROBE=$(ls -t /home/jovyan/common_data/nex-gddp-cimp6_hazards/checks/issue54_intld_guard_*.csv | head -1)
for RES in 0.25 0.05; do Rscript R/checks/gate_054_reference_diff.R --old-dir $PARK/exposure --res $RES --probe $PROBE 2>&1 | tee -a logs/issue54_gate054ref_$STAMP.log; done
```

Gates:
1. Both 0.4.4 runs exit 0. Each log carries `section 2.9 (#52): N TLU rows ... relabelled` with
   N > 0, and `section 3.1: units present in extraction = ...` listing `tlu`. If §2.9 stops, or
   §3.1 stops on a missing unit, stop and report.
2. **`GATE 054-REF PASS` at both resolutions (3/3 tables each):**
   - same row set once the old TLU rows are read as `tlu`;
   - the TLU rows relabelled with identical values;
   - every non-#54 row value-identical, including the whole nominal twin;
   - each of the four pairs' admin2 sum within 2 % of the probe's national NEW;
   - the intld livestock total equals old + the probe's (NEW − OLD) within 1 %.
3. Existing gates, unchanged expectations: `Rscript R/qaqc_vop_vs_faostat.R` — livestock reads
   against FAOSTAT's I$ values, so the four guarded rows are expected to be the only ones that no
   longer match (report the count; 238/242 is the shape, not the gate); `Rscript
   R/checks/vop_cross_basis_gate.R --res 0.25` and `--res 0.05` — report anything new and
   whether it involves the four countries.

If any gate deviates, stop and describe it. Leave the parked copies; they are the rollback.

#### B4. Report

For A: the HEAD hash, the fixture lines, the CSV mtime and columns, the probe's GATE 0 lines, the
totals line, the `intld_source` table, the item-median table, the flagged table, the invariant
lines, the four-pair factor table, the other-year-set lines, runtime, the CSV path, and the three
A2 lines. Commit the probe CSV under `R/checks/041_outputs/` if it is under 1 MB (public FAOSTAT
values only).

For B: `$PARK`, exit codes and runtimes, the y2021 guard line and table, the B2 differing rows,
both 0.4.4 §2.9 and §3.1 lines, both gate-054 outputs, the qaqc count and the cross-basis lines.

The rebuilt tables then sit **locally ahead of S3**. That is intended: the reference publish and
the R/3 intld tier follow in block C once Pete has seen this.

#### What NOT to do

- **No S3 writes of any kind:** no `r3_publish_tiers.R` (not even `--reference-only`), no
  `s3_upload.R`.
- **Do not run R/3.**
- **Do not delete the parked copies.**
- **Do not edit pipeline code, gates, fixtures or the probe** to get past a stop. Do not loosen a
  tolerance. If a gate false-fails, that is a finding: describe it.
- **Do not change `metadata/haz_metadata.csv`** or any `metadata/cdh/*.yaml` (they change in the
  commit that publishes).

#### Reporting

Prepend a `### RESPONSE <date>` block above this one, commit
(`Co-Authored-By: <your own model name> <noreply@anthropic.com>`), push, and verify it landed:
`git fetch`; `git log origin/develop..HEAD` empty;
`git show origin/develop:DISPATCH_cglabs_issue54_intld_placeholders.md | grep -c RESPONSE`.
