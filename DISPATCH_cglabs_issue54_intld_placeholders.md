# DISPATCH — cglabs — #54 + #52: constant-I$ placeholders and the TLU slot

Append-only, newest block on top. Read `AGENTS.md` first, then `HANDOVER_2026-10-23.md` §3 items 2 and 5.

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
