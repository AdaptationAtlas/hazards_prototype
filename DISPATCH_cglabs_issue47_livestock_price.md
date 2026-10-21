# DISPATCH — cglabs — #47: livestock nominal-USD priced at the world median

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### RESPONSE 2026-10-21 — cglabs — D. Reference + family republished. Every gate PASS. STOPPING before E.

**All four dry-runs clean, all four live runs exit 0, 18/18 keys MD5 == ETag, S3 read-back hits every
expected figure.** No gate asked for an allow-flag and none was passed. No delete of any kind. E is
**not** started and will not be until a "GO E" block lands.

HEAD `bc06ff8` (`Pete Steward`, `docs(#47): block D+E — reference republish, stop, then R/3 usd tier + publish`).

#### D1a. Dry-runs — four for four, exit 0

Every one reported the same shape, and **none requested `--allow-unit-vintage-change`,
`--allow-res-change`, `--allow-schema-drift` or `--allow-value-drift`**:

| run | columns | distinct(exposure) | distinct(unit) | distinct(stat) | rows local vs live | exit |
|---|---|---|---|---|---|---|
| `--reference-only --res 0.05` | 14 identical | identical (5) | identical (5) | identical | 8,065,336 = 8,065,336 | 0 |
| `--reference-only --res 0.25` | 14 identical | identical (5) | identical (5) | identical | 5,667,864 = 5,667,864 | 0 |
| `--family-only --res 0.05` | 14 identical | `vop` | `nominal-usd-2021` / `intld15-2021` | identical | 1,508,624 and 986,408, both equal | 0 |
| `--family-only --res 0.25` | 14 identical | `vop` | `nominal-usd-2021` / `intld15-2021` | identical | 1,060,176 and 693,192, both equal | 0 |

Rows are not merely within 25 % — they are exactly equal on every table, which is what #47 should do:
values move, shape does not. The reference's distinct sets read
`harv-area,number,phys-area,prod,vop` and `ha,intld15-2021,nominal-usd-2021,number,t` before and after.

Each dry-run also named the res-05 alias refresh it would do (`crop-livestock_all.parquet`,
`vop_nominal-usd-2021.parquet`, `vop_intld15-2021.parquet`), and res-25 correctly named none.

#### D1b. Live — four for four, exit 0

```
== --reference-only --res 0.05 exit 0      (1.2 min)
== --reference-only --res 0.25 exit 0      (0.7 min)
== --family-only --res 0.05 exit 0         (0.8 min)
== --family-only --res 0.25 exit 0         (0.5 min)
```

The same gate lines reappeared live before each upload — 14 columns identical, distinct
exposure/unit/stat identical, rows identical — so the gates were judged against the live objects at
write time, not only in the dry pass.

**Backup paths.** The publisher wrote one prefix per invocation, four in all:

```
s3://digital-atlas/sandbox/backup/issue9_20261021_174022/   (reference res-05 + alias)
s3://digital-atlas/sandbox/backup/issue9_20261021_174135/   (reference res-25)
s3://digital-atlas/sandbox/backup/issue9_20261021_174216/   (family res-05, both units + aliases)
s3://digital-atlas/sandbox/backup/issue9_20261021_174307/   (family res-25, both units)
```

Each holds the pre-#47 live object under its full `domain=exposure/…/variable=…` path. The `issue9_`
stem is the publisher's fixed backup prefix, not a mis-stamp — worth knowing when these are read back.

**18 keys written**: 9 parquets and their 9 `.json` sidecars.

#### D2.2 MD5 == ETag — 18/18

`s3fs::s3_file_info()` ETag against local `md5sum`. No ETag carried a multipart `-n` suffix, so no
object needed downloading and re-hashing.

```
crop-livestock_all_res-05.parquet                cd8f7e6d1779fbd2a1dc084de4954ed9  MATCH
crop-livestock_all_res-05.parquet.json           161f77d85e0675df7bc2c7e37eadf5f5  MATCH
crop-livestock_all.parquet                       cd8f7e6d1779fbd2a1dc084de4954ed9  MATCH
crop-livestock_all.parquet.json                  161f77d85e0675df7bc2c7e37eadf5f5  MATCH
crop-livestock_all_res-25.parquet                9f948e795136428763e9eb5281b1e1f1  MATCH
crop-livestock_all_res-25.parquet.json           82ea35fc9a9d56b995b4df329d184dc3  MATCH
vop_nominal-usd-2021_res-05.parquet              d6f14d3ef7da421c767b193a9eb4d302  MATCH
vop_nominal-usd-2021_res-05.parquet.json         89eefd0f8b55dcb99878ddc0886b3b89  MATCH
vop_nominal-usd-2021.parquet                     d6f14d3ef7da421c767b193a9eb4d302  MATCH
vop_nominal-usd-2021.parquet.json                89eefd0f8b55dcb99878ddc0886b3b89  MATCH
vop_nominal-usd-2021_res-25.parquet              502ef758713afe7717dd0ae081c68111  MATCH
vop_nominal-usd-2021_res-25.parquet.json         086ac6e37f1f0469e4a224d146c0d03c  MATCH
vop_intld15-2021_res-05.parquet                  c8971a31e4b1d16feb3f7327536cce9b  MATCH
vop_intld15-2021_res-05.parquet.json             e6c245a886f426a16f1533fb7d559e8f  MATCH
vop_intld15-2021.parquet                         c8971a31e4b1d16feb3f7327536cce9b  MATCH
vop_intld15-2021.parquet.json                    e6c245a886f426a16f1533fb7d559e8f  MATCH
vop_intld15-2021_res-25.parquet                  26fe67d4840ebd69747942a2754b0b38  MATCH
vop_intld15-2021_res-25.parquet.json             c34ca4597946adb22c7d98df8945b6ed  MATCH

MD5 == ETag: 18 / 18 keys
```

#### D2.4 Alias — confirmed by the same table

`crop-livestock_all.parquet` carries `cd8f7e6d1779fbd2a1dc084de4954ed9`, identical to
`crop-livestock_all_res-05.parquet`. The two family aliases likewise carry their res-05 hashes
(`d6f14d3e…`, `c8971a31…`). No alias picked up a res-25 hash.

#### D2.3 Read-back from S3 — every expected figure hit

Downloaded `crop-livestock_all_res-25.parquet` and `_res-05` **from S3**, not from disk, and
aggregated admin2 `vop` rows with `tech` = all or NA and finite values, split by unit and by
livestock membership:

```
== res-25 : read back from S3, 5667864 rows
               unit   lvst total_bn      n
1:     intld15-2021  FALSE  240.243  96798
2:     intld15-2021   TRUE   89.016  41108
3: nominal-usd-2021  FALSE  201.921 105633
4: nominal-usd-2021   TRUE  109.063  41108

== res-05 : read back from S3, 8065336 rows
               unit   lvst total_bn      n
1:     intld15-2021  FALSE  240.240 114828
2:     intld15-2021   TRUE   89.019  62868
3: nominal-usd-2021  FALSE  201.920 132231
4: nominal-usd-2021   TRUE  109.069  62868
```

Against the block's expectations: **nominal-usd-2021 livestock 109.063 B (res-25) and 109.069 B
(res-05)** — the C4 local figures exactly; **nominal-usd-2021 crops 201.921 B** at res-25, unchanged;
**intld15-2021 240.243 B crops + 89.016 B livestock** at res-25, unchanged.

**On "any other movement is a stop" — there is none, and I checked it rather than inferred it.**
The res-05 column reads 201.920 / 240.240 / 89.019 where the block quotes the res-25 figures
201.921 / 240.243 / 89.016. That is the res-05 twin, not movement. To prove it I ran the identical
aggregation over the **parked pre-publish** tables:

```
== PARKED (pre-publish) res-25            == PARKED (pre-publish) res-05
   intld15-2021     FALSE  240.243           intld15-2021     FALSE  240.240
   intld15-2021      TRUE   89.016           intld15-2021      TRUE   89.019
   nominal-usd-2021 FALSE  201.921           nominal-usd-2021 FALSE  201.920
   nominal-usd-2021  TRUE  104.229           nominal-usd-2021  TRUE  104.234
```

Every line matches the published table except nominal livestock, which moves 104.229 → 109.063 at
res-25 and 104.234 → 109.069 at res-05. Row counts are identical throughout (96,798 / 41,108 /
105,633 at res-25; 114,828 / 62,868 / 132,231 at res-05). One quantity moved; nothing else did.

#### State and stop

S3 now carries the #47 livestock pricing on the reference and both family twins, at both
resolutions, with the aliases refreshed. The R/3 tiers on S3 are still the pre-#47 ones — that is
expected and is what block E is for.

Parked copies at `Data/_parked_issue47_20261021_164945` are untouched and remain the rollback,
alongside the four S3 backup prefixes. The block-B probe CSV is untouched. Logs:
`logs/issue47_D_*_20261021_164945.log`.

**Stopping here. E is not started.** Waiting for a "GO E" block.

---

### MACBOOK 2026-10-21 — D + E. Publish the reference (D), STOP, then R/3 usd tier + publish (E) on GO.

**Block C accepted.** Every gate passed and the numbers reconcile three ways: the probe's C, the
rebuilt CSVs, and the rebuilt tables, all at ×1.0464. Pete GO'd the publish on 2026-10-21.
This block runs in **two parts with a hard stop between them**:
- run D, report D, then wait;
- start E only after a macbook block titled "GO E" lands above this one.

Noted from C and not acted on: the cross-basis "prune?" lists differ between res-25 and res-05
(crop pairs, pre-existing, unrelated to #47).

---

#### D. Reference + family republish (~5 min; S3 writes)

D1 runs dry-runs first. Run each command with `--dry-run`, then report its gate lines. **Then run
them live**, in the background with nohup and a log, in this order:
```bash
cd /home/jovyan/atlas/hazards_prototype && git pull --ff-only && git log -1 --format='%h %an %s'
STAMP=$(cat logs/issue47_stamp.txt)
for A in "--reference-only --res 0.05" "--reference-only --res 0.25" "--family-only --res 0.05" "--family-only --res 0.25"; do
  Rscript scripts/r3_publish_tiers.R $A --dry-run 2>&1 | tail -40
done
nohup bash -c 'for A in "--reference-only --res 0.05" "--reference-only --res 0.25" "--family-only --res 0.05" "--family-only --res 0.25"; do Rscript scripts/r3_publish_tiers.R $A; rc=$?; echo "== $A exit $rc"; [ $rc -eq 0 ] || break; done' > logs/issue47_D_publish_$STAMP.log 2>&1 &
```
**Do not pass** any of these. #47 changes values only; units, columns and resolution are unchanged:
- `--allow-unit-vintage-change`;
- `--allow-res-change`;
- `--allow-schema-drift`;
- `--allow-value-drift`.

If a gate asks for one, stop and paste it.

The publisher backs up the live keys to `sandbox/backup/` before writing. Report the backup path.

**D2. Gates and verification.**

Invariants:
1. All four live runs exit 0, and the gate lines show columns identical, `distinct(exposure/unit/stat)`
   identical and rows within 25 %.
2. **MD5 == ETag** for every key written (both suffixed references, the unsuffixed alias, and the
   family twins plus their aliases), with the method from #13 RESPONSE (k): `head_object` ETag
   against local `md5sum`.
   - If an ETag carries a multipart `-n` suffix, download the object and hash it instead.
3. **Read back from S3**, not from disk: the admin2 `vop` totals by (unit, livestock yes/no),
   with `tech` = all or NA and finite values only, for
   `crop-livestock_all_res-25.parquet` and `_res-05`. Expected:
   - **nominal-usd-2021 livestock** = the C4 local figure, 109.063 B (res-25) and 109.069 B (res-05);
   - **nominal-usd-2021 crops** = 201.921 B, unchanged;
   - **intld15-2021** = 240.243 B crops + 89.016 B livestock (res-25), unchanged.

   Any other movement is a stop.
4. The unsuffixed alias `crop-livestock_all.parquet` has the res-05 MD5.

**D3. Report, then STOP.** Prepend a RESPONSE with the dry-run gate lines, live exit codes, backup
path, the MD5 table and the read-back totals. Do not start E.

---

#### E. R/3 nominal-USD tier, then publish (starts only on "GO E")

**E1. Park the usd R/3 outputs only.** They must be moved, not copied: R/3 skips any output that
exists, and that skip is how this run is limited to usd.
```bash
Rscript -e 'suppressMessages(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"))
  src <- atlas_dirs$data_dir$hazard_risk_vop_usd; stamp <- readLines("/home/jovyan/atlas/hazards_prototype/logs/issue47_stamp.txt")
  dst <- file.path(working_dir, "Data", paste0("_parked_issue47_", stamp), "r3_vop_usd"); dir.create(dst, recursive = TRUE)
  for (tf in c("annual", "jagermeyr")) { f <- list.files(file.path(src, tf), full.names = TRUE, recursive = TRUE)
    d <- file.path(dst, tf, sub(paste0("^", file.path(src, tf), "/"), "", f)); for (x in unique(dirname(d))) dir.create(x, recursive = TRUE, showWarnings = FALSE)
    ok <- file.rename(f, d); cat(tf, ": moved", sum(ok), "of", length(f), "| left behind", length(list.files(file.path(src, tf), recursive = TRUE)), "\n") }'
```
Expect `left behind 0` for both timeframes. The other four variables' R/3 outputs stay where they
are, untouched.

**E2. Run R/3 (background, ~8-9 h for both timeframes).**
```bash
date '+%Y-%m-%d %H:%M' > logs/issue47_r3_since.txt
nohup Rscript -e 'source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); source("/home/jovyan/atlas/hazards_prototype/R/3_freq_x_exposure.R")' > logs/issue47_r3_$STAMP.log 2>&1 &
echo $! > logs/issue47_r3_$STAMP.pid
```
Environment:
- Leave `FORCE_OVERWRITE` **unset**; setting it would rebuild all five variables.
- Leave `R3_CROP_VOP_USD` at its default.
- **Do not set** `R3_ALLOW_41_FAILURES` or `SKIP_R3_4_1`.

Header check, early in the log:
- `Using crop vop usd file: spam_vop_nominal-usd-2021_all_res-25.tif | R3_CROP_VOP_USD = 2021`;
- `overwrite4= FALSE`.

If either is wrong, stop the run and report.

Silent-failure signature from memory: usd §4.1 finishing in single-digit minutes means it skipped.

**E3. Post-run gates.** Stop on any deviation.
1. Exit 0. No `failed_risk_x_exposure_*.txt`. `skipped_not_in_exposure_*.txt` holds only the
   already-classified skips.
2. Every usd output has an mtime after `issue47_r3_since.txt`. The other variables' outputs keep
   their pre-run mtimes; report the newest mtime per variable.
3. `Rscript R/checks/usd_total_vs_reference.R --iso3 all`, then the same with `--timeframe annual`.
   Material pairs must sit in [0.5, 2] with the median in [0.9, 1.1], against the **republished**
   reference.
4. `Rscript scripts/stamp_ensemble_membership.R` for both timeframes. It must report **18 members**.

**E4. Publish the usd tier**, dry-run then live, background, jagermeyr then annual:
```bash
TWIN=$(Rscript -e 'suppressMessages(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")); cat(normalizePath(file.path(exposure_dir, "vop_nominal-usd-2021_adm_sum_spam20_glw420_res-25.parquet")))' | tail -1)
Rscript scripts/r3_publish_tiers.R --variables vop_usd --drift-exposure vop_usd=$TWIN --livestock-moved --dry-run
Rscript scripts/r3_publish_tiers.R --variables vop_usd --drift-exposure vop_usd=$TWIN --livestock-moved --timeframe annual --dry-run
```
Expected in each dry-run:
- G1-G5 ok;
- the log line `G6 declared livestock exposure move`;
- the G6 livestock control reads `control ~ 1 net of the exposure input (livestock move declared)`
  and is ok;
- the crop material pairs are ok against the unchanged crop exposure;
- **G6b ok** (product against its own twin, admin2 basis, the TZA coconut named residual only).

If G6 or G6b fails, stop and paste the populations. **Do not** pass:
- `--allow-value-drift`, `--allow-schema-drift`;
- `--hv-retired` / `--hv-added` (the set is unchanged);
- `--no-exposure-gate`, `--skip-gates`;
- any `--drift-tol-*`.

If both dry-runs are clean, run both live, in the background with a log.

**E5. Verify.** MD5 == ETag for every key written, as in D2, and report the sidecars' `n_members` (18).

**E6. Report.** Prepend a RESPONSE with E1-E5. The parked copies stay until the macbook says
otherwise.

#### What NOT to do (D and E)
- **Never** use `s3fs::s3_file_delete` / `s3_dir_delete`: they delete permanently. No deletes of
  any kind.
- Never use `R/s3_upload.R` or the derive script.
- **Do not start E without "GO E".**
- Do not edit code, gates, fixtures or tolerances to pass a stop. Do not change
  `metadata/haz_metadata.csv`.

#### Reporting
Prepend `### RESPONSE <date>` per part (D, then E), commit
(`Co-Authored-By: <your own model name> <noreply@anthropic.com>`), push, and verify it landed.

---

### RESPONSE 2026-10-21 — cglabs — C. Rebuild complete. Every gate PASS. Tables are local, ahead of S3.

**C0 PASS, C1 parked, C2 PASS, GATE 047 PASS 6/6, GATE 047-REF PASS 3/3 at both resolutions,
qaqc livestock 242/242 unmoved, cross-basis PASS at both resolutions with no new findings.**
No stop. No code, gate, fixture or tolerance touched. No S3 call of any kind, no R/3, parked
copies intact, `metadata/haz_metadata.csv` untouched.

#### C0. Pre-flight — PASS

HEAD `095d9c0` (`Pete Steward`, `fix(#47): price livestock nominal USD at national prices, not the
world median`). No R job was running at start.

```
fixture_crop_heat_interactions.R: ALL CROP-HEAT INTERACTION FIXTURE ASSERTIONS PASSED
fixture_fao_code_join.R: ALL FAO CODE-JOIN FIXTURE ASSERTIONS PASSED
fixture_gate_zonal_basis.R: ALL ZONAL-BASIS FIXTURE ASSERTIONS PASSED
fixture_interaction_builder.R: ALL INTERACTION-BUILDER FIXTURE ASSERTIONS PASSED
fixture_livestock_price.R: ALL LIVESTOCK-PRICE FIXTURE ASSERTIONS PASSED
fixture_price_fill.R: ALL PRICE-FILL FIXTURE ASSERTIONS PASSED
fixture_publish_tier_gates.R: ALL PUBLISH-GATE FIXTURE ASSERTIONS PASSED
fixture_qaqc_denominator.R: ALL QAQC-DENOMINATOR FIXTURE ASSERTIONS PASSED
fixture_r3_physical_tiers.R: ALL R/3 PHYSICAL-TIER FIXTURE ASSERTIONS PASSED
fixture_vop_allocate.R: ALL QUANTITY-PIN ASSERTIONS PASSED
```

Ten for ten, matching the macbook.

#### C1. Park — done, copies not moves

```
PARK = /home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/_parked_issue47_20261021_164945
```

68 MB. All six parquets were present before the copy and are in `$PARK/exposure` with their original
10-07 mtimes preserved by `cp -a`:

```
8918339  exposure_adm_sum_spam20-20_glw420-20_res-05.parquet   Oct  7 20:02
7137701  exposure_adm_sum_spam20-20_glw420-20_res-25.parquet   Oct  7 19:57
1397331  vop_intld15-2021_adm_sum_spam20_glw420_res-05.parquet Oct  7 20:02
1120912  vop_intld15-2021_adm_sum_spam20_glw420_res-25.parquet Oct  7 19:58
1734813  vop_nominal-usd-2021_adm_sum_spam20_glw420_res-05.parquet Oct  7 20:02
1407809  vop_nominal-usd-2021_adm_sum_spam20_glw420_res-25.parquet Oct  7 19:58
```

`$PARK/glw_processed` holds `fao_vop` plus `variable=vop_nominal-usd-{2015,2020,2021}`. Note the
glob caught all three year variants, not only 2021 — that is more than the block needed but it is
the safer side of the rollback.

#### C2. 0.4.1 — both runs exit 0

| res | start | end | runtime | exit | grid line |
|---|---|---|---|---|---|
| 0.25 | 16:50:35 | 16:52:12 | 1.6 min | 0 | `base_rast_nexgddp.tif \| res 0.2500 \| tag res-25` |
| 0.05 | 16:52:24 | 16:55:28 | 3.1 min | 0 | `base_raster.tif \| res 0.0500 \| tag res-05` |

Both tags correct. Per-year-set lines, identical across the two resolutions:

```
[16:51:30] [0.4.1]   y2021: 495 (iso3, product) rows; 3 producer prices clipped vs world; singleton regions: none
[16:51:32] [0.4.1]   y2015: 495 (iso3, product) rows; 3 producer prices clipped vs world; singleton regions: none
[16:51:34] [0.4.1]   y2020: 495 (iso3, product) rows; 3 producer prices clipped vs world; singleton regions: none
```

The y2021 clip list and `price_source` table from the **res-25** log, verbatim:

```
[16:51:30] [0.4.1]   y2021: 495 (iso3, product) rows; 3 producer prices clipped vs world; singleton regions: none
     iso3 atlas_name  year price_usd price_world ratio_world
   <char>     <char> <int>     <num>       <num>       <num>
1:    NER sheep_milk  2022     183.6       962.2      0.1910
2:    BWA  goat_milk  2020      30.8       754.9      0.0408
3:    BWA  goat_milk  2021      31.8       813.2      0.0391
                       price_source     n vop_usd_bn
                             <char> <int>      <num>
1:                neighbours median   163     38.875
2:               fao producer price    57     41.322
3:     world median (item override)    50      5.929
4:                    region median    42      7.188
5:                 continent median    41      5.089
6: fao producer price longer-series    37     10.667
```

Every source is in the allowed list; no `none`, and the only `world median` is the item override.
Sums to 109.070 B. The res-05 table is byte-identical — pricing is national and grid-independent,
which is what you would want to see.

It also reconciles row for row with the probe's C: own 57 + 37 = 94, neighbours 163, region 42,
continent 41, override 50. The three clipped prices are the same three.

#### C3. GATE 047 — PASS 6/6

Full output (setup banner trimmed):

```
[2026-10-21 16:56:55] [gate-047] reference: issue47_livestock_price_20261021_125145.csv (1485 rows, year sets y2015,y2020,y2021)
[2026-10-21 16:56:55] [gate-047] ok   nominal-usd 2021: 495 rows, total new 109.070 B vs reference 109.070 B, 0 differ; mtime 2026-10-21 16:53 (after --since)
                       price_source     n value_bn
                             <char> <int>    <num>
1:                neighbours median   163   38.875
2:               fao producer price    57   41.322
3:     world median (item override)    50    5.929
4:                    region median    42    7.188
5:                 continent median    41    5.089
6: fao producer price longer-series    37   10.667
[2026-10-21 16:56:55] [gate-047] ok   nominal-usd 2015: 495 rows, total new 95.475 B vs reference 95.475 B, 0 differ; mtime 2026-10-21 16:53 (after --since)
                   price_source     n value_bn
                         <char> <int>    <num>
1:            neighbours median   150   38.163
2:                region median    66   10.867
3:           fao producer price    64   35.610
4:             continent median    60    5.646
5: world median (item override)    50    5.189
[2026-10-21 16:56:55] [gate-047] ok   nominal-usd 2020: 495 rows, total new 101.538 B vs reference 101.538 B, 0 differ; mtime 2026-10-21 16:54 (after --since)
                       price_source     n value_bn
                             <char> <int>    <num>
1:                neighbours median   170   44.016
2:     world median (item override)    50    5.214
3: fao producer price longer-series    45   17.720
4:                 continent median    45    4.413
5:               fao producer price    41   23.320
6:                    region median    39    6.855
[2026-10-21 16:56:55] [gate-047] ok   intld15 2021: 495 rows, total new 89.020 B vs reference 89.020 B, 0 differ; mtime 2026-10-21 16:54 (after --since)
[2026-10-21 16:56:55] [gate-047] ok   intld15 2015: 495 rows, total new 77.549 B vs reference 77.549 B, 0 differ; mtime 2026-10-21 16:54 (after --since)
[2026-10-21 16:56:55] [gate-047] ok   intld15 2020: 495 rows, total new 86.606 B vs reference 86.606 B, 0 differ; mtime 2026-10-21 16:54 (after --since)
[2026-10-21 16:56:55] [gate-047] GATE 047 PASS: 6/6 checks pass (9.2 s)
gate exit 0
```

y2021 nominal lands at 109.070 B as expected, and all three intld year sets are unchanged to 1e-9.
`--tol` was not touched and the probe was not re-run.

#### C4. 0.4.4 and the reference diff

Both 0.4.4 runs exit 0, both opened with the required line:

| res | start | end | runtime | exit | first line |
|---|---|---|---|---|---|
| 0.25 | 16:57:52 | 17:00:57 | 3.1 min | 0 | `script start (FORCE_OVERWRITE=1 -> overwrite=TRUE)` |
| 0.05 | 17:01:09 | 17:05:52 | 4.7 min | 0 | `script start (FORCE_OVERWRITE=1 -> overwrite=TRUE)` |

§1 did not stop on a legacy twin: `section 1: 66 tifs -> 45 after keeping res-25 + untagged`, and
the same 66 → 45 at res-05.

**GATE 047-REF, both resolutions, verbatim** (setup banners trimmed):

```
[2026-10-21 17:07:51] [gate-047-ref] probe factors: 243 (iso3, class) pairs; continental C/A = 1.0464
[2026-10-21 17:08:08] [gate-047-ref] exposure_adm_sum_spam20-20_glw420-20_res-25.parquet: old 5667864 rows (mtime 10-07 19:57), new 5667864 rows (mtime 10-21 17:00); key iso3,admin0_name,admin1_name,admin2_name,crop,exposure,unit,tech,stat
[2026-10-21 17:08:37] [gate-047-ref]   rows: ok (old 5667864 / new 5667864 / joined 5667864, unmatched-value 0, duplicate keys old FALSE new FALSE)
[2026-10-21 17:08:37] [gate-047-ref]   not-#47 rows identical: ok (5616894 rows, 0 differ)
[2026-10-21 17:08:37] [gate-047-ref]   livestock nominal pairs vs probe factor: ok (319 pairs >= 1e+06; 0 off by > 2%; worst 0.00131)
[2026-10-21 17:08:37] [gate-047-ref]   livestock nominal total: ok (old 104.229 B -> new 109.063 B, ratio 1.0464 vs probe 1.0464)
[2026-10-21 17:08:40] [gate-047-ref] vop_nominal-usd-2021_adm_sum_spam20_glw420_res-25.parquet: old 1060176 rows (mtime 10-07 19:58), new 1060176 rows (mtime 10-21 17:00); key iso3,admin0_name,admin1_name,admin2_name,crop,exposure,unit,tech,stat
[2026-10-21 17:08:45] [gate-047-ref]   rows: ok (old 1060176 / new 1060176 / joined 1060176, unmatched-value 0, duplicate keys old FALSE new FALSE)
[2026-10-21 17:08:45] [gate-047-ref]   not-#47 rows identical: ok (1009206 rows, 0 differ)
[2026-10-21 17:08:45] [gate-047-ref]   livestock nominal pairs vs probe factor: ok (319 pairs >= 1e+06; 0 off by > 2%; worst 0.00131)
[2026-10-21 17:08:45] [gate-047-ref]   livestock nominal total: ok (old 104.229 B -> new 109.063 B, ratio 1.0464 vs probe 1.0464)
[2026-10-21 17:08:47] [gate-047-ref] vop_intld15-2021_adm_sum_spam20_glw420_res-25.parquet: old 693192 rows (mtime 10-07 19:58), new 693192 rows (mtime 10-21 17:00); key iso3,admin0_name,admin1_name,admin2_name,crop,exposure,unit,tech,stat
[2026-10-21 17:08:50] [gate-047-ref]   rows: ok | all rows identical: ok (0 differ) | livestock nominal rows present: 0 (expect 0)
[2026-10-21 17:08:50] [gate-047-ref] GATE 047-REF PASS (res-25): 3/3 tables pass
ref gate res 0.25 exit 0
[2026-10-21 17:08:59] [gate-047-ref] probe factors: 243 (iso3, class) pairs; continental C/A = 1.0464
[2026-10-21 17:09:22] [gate-047-ref] exposure_adm_sum_spam20-20_glw420-20_res-05.parquet: old 8065336 rows (mtime 10-07 20:02), new 8065336 rows (mtime 10-21 17:05); key iso3,admin0_name,admin1_name,admin2_name,crop,exposure,unit,tech,stat
[2026-10-21 17:09:59] [gate-047-ref]   rows: ok (old 8065336 / new 8065336 / joined 8065336, unmatched-value 0, duplicate keys old FALSE new FALSE)
[2026-10-21 17:09:59] [gate-047-ref]   not-#47 rows identical: ok (7992806 rows, 0 differ)
[2026-10-21 17:09:59] [gate-047-ref]   livestock nominal pairs vs probe factor: ok (319 pairs >= 1e+06; 0 off by > 2%; worst 0.000884)
[2026-10-21 17:09:59] [gate-047-ref]   livestock nominal total: ok (old 104.234 B -> new 109.069 B, ratio 1.0464 vs probe 1.0464)
[2026-10-21 17:10:02] [gate-047-ref] vop_nominal-usd-2021_adm_sum_spam20_glw420_res-05.parquet: old 1508624 rows (mtime 10-07 20:02), new 1508624 rows (mtime 10-21 17:05); key iso3,admin0_name,admin1_name,admin2_name,crop,exposure,unit,tech,stat
[2026-10-21 17:10:08] [gate-047-ref]   rows: ok (old 1508624 / new 1508624 / joined 1508624, unmatched-value 0, duplicate keys old FALSE new FALSE)
[2026-10-21 17:10:08] [gate-047-ref]   not-#47 rows identical: ok (1436094 rows, 0 differ)
[2026-10-21 17:10:08] [gate-047-ref]   livestock nominal pairs vs probe factor: ok (319 pairs >= 1e+06; 0 off by > 2%; worst 0.000884)
[2026-10-21 17:10:08] [gate-047-ref]   livestock nominal total: ok (old 104.234 B -> new 109.069 B, ratio 1.0464 vs probe 1.0464)
[2026-10-21 17:10:10] [gate-047-ref] vop_intld15-2021_adm_sum_spam20_glw420_res-05.parquet: old 986408 rows (mtime 10-07 20:02), new 986408 rows (mtime 10-21 17:05); key iso3,admin0_name,admin1_name,admin2_name,crop,exposure,unit,tech,stat
[2026-10-21 17:10:14] [gate-047-ref]   rows: ok | all rows identical: ok (0 differ) | livestock nominal rows present: 0 (expect 0)
[2026-10-21 17:10:14] [gate-047-ref] GATE 047-REF PASS (res-05): 3/3 tables pass
ref gate res 0.05 exit 0
```

Reading the four lines that matter: the row sets are identical with zero unmatched values and no
duplicate keys; every non-#47 row is value-identical (5,616,894 at res-25, 7,992,806 at res-05, 0
differing) and the whole intld twin is identical with 0 livestock-nominal rows present as expected;
all 319 livestock pairs ≥ 1e6 sit on the probe factor with worst deviation **0.131 %** at res-25 and
**0.088 %** at res-05, against a 2 % bar; and the livestock nominal total moves 104.229 B → 109.063 B
at res-25 and 104.234 B → 109.069 B at res-05, ratio **1.0464** against the probe's 1.0464. The old
res-05 figure 104.234 B is the live reference's 104.229 B plus the usual res-05/res-25 difference, so
the park is the right baseline.

#### C4.3 Existing gates — both clean

**`R/qaqc_vop_vs_faostat.R`** — livestock does not move, as predicted, because it runs on the I$ basis:

```
[17:11:10] [qaqc-vop] LIVESTOCK ratios: median=1.00 | within 0.9-1.1 = 242/242 | AGO cattle grid=525.69M I$ fao=525.69M I$ ratio=1.00
  livestock: median ratio 1.00 | within 0.9-1.1: 242/242 | worst: SDN/sheep=1.00
```

242/242, median 1.00, worst 1.00. Exit 0.

**`R/checks/vop_cross_basis_gate.R`** — `GATE PASS in 0.2 min` at both resolutions, exit 0 each.
**Nothing new, and nothing new involving livestock.** Every out-of-band pair is already named:

```
res-25:
  pairs: 1396 both bases | 1039 material (intld >= 1e+06) | 357 immaterial | 411 one-sided
  material ratio nominal/intld: median 1.17 | 5-95% [0.418, 3.04] | livestock median 1.17
  info: 2 out-of-band pairs have NO national constant-I$ allocation (border spill on this grid, #18)
        - reported, not gated: TCD:yams, BEN:bean
  info: 1 out-of-band pairs are NAMED expected residuals: ETH:tea
  ok: no material pair outside [1/10, 10] has its nominal side off
  ok: every per-crop median within [1/5, 5]

res-05:
  pairs: 1181 both bases | 994 material | 187 immaterial | 559 one-sided
  material ratio nominal/intld: median 1.17 | 5-95% [0.398, 3.14] | livestock median 1.18
  info: 4 out-of-band pairs are NAMED expected residuals: ETH:tea, TGO:oilpalm, CAF:robusta-coffee, GNB:maize
  ok: no material pair outside [1/10, 10] has its nominal side off
  ok: every per-crop median within [1/5, 5]
```

Two observations, neither a stop:

- **The livestock cross-basis median moved up with the fix and stayed in band** — 1.17 at res-25,
  1.18 at res-05, against the continental 1.0464 applied on top of the old ratio. The poultry
  per-crop medians (`poultry-tropical` 1.92 over 47 pairs, `poultry-highland` 1.72 over 19) are the
  highest livestock entries and are well inside [1/5, 5]. This is the fix showing up where it should,
  on the nominal side only.
- **The "prune?" list differs between resolutions** — res-25 names TGO:oilpalm, GNB:maize,
  CAF:arabica-coffee, CAF:robusta-coffee, GIN:arabica-coffee, GIN:robusta-coffee; res-05 names only
  the three coffee pairs, because TGO:oilpalm, GNB:maize and CAF:robusta-coffee are still out of band
  there. Both are crop pairs and both lists are informational. This is pre-existing and unrelated to
  #47 — I mention it only because the two resolutions disagree, which is the sort of thing worth
  knowing before a prune.

#### State on the node

The rebuilt tables now sit **locally ahead of S3**, as the block intends: six exposure parquets at
both resolutions plus the GLW `fao_vop` CSVs and `variable=vop_nominal-usd-*` rasters, all written
2026-10-21 16:50-17:05. The live S3 reference is still the 2026-10-08 publish at 104.229 B. Nothing
was uploaded.

Rollback is `$PARK`, untouched. The block-B probe CSV is untouched. Logs for every step are in
`logs/issue47_*_20261021_164945.*` (git-ignored).

Ready for block D (reference publish) and block E (R/3 usd tier) on your word.

---

### MACBOOK 2026-10-21 — C. Fix chosen (C). Rebuild 0.4.1 + 0.4.4 locally and gate it. No S3 writes.

**Your read was right, and Pete chose C** (`R/price_fill.R` method). One correction to the read:
the LBY / SLE cattle-meat basis outliers are on the **constant-I$** side, not the price side. Their
`vop_intd15` is ~$1,000 against a nominal ~$27-48 M at a normal price. That is an intld-tier oddity,
handled separately; it does not block this.

The clip drops only three prices today (all goat or sheep milk, which take the world median
anyway), so C differs from B only in the fills (median vs mean, and which neighbours). It is still
the right code: it is the crop side's tested path, and it will catch a ZWE-style artefact after the
next FAO refresh.

**What changed on develop** (commit carrying this block):
- `R/price_fill.R`: new `livestock_nominal_price()`, which is the probe's C.
- `R/0.4.1` §4.1: calls it. §5's `-prices-used.csv` now also carries `production_t`,
  `price_usd_global`, `price_usd_final` and `price_source`.
- `R/checks/fixture_livestock_price.R` (new): runs the function **and the real text of 0.4.1 §4.1**
  on synthetic data.
- `R/checks/gate_047_prices_vs_probe.R` (new): the new CSVs must equal your committed probe output
  (C), and the intld CSVs must equal it unchanged.
- `R/checks/gate_047_reference_diff.R` (new): new exposure tables against parked copies.
  - Crop, intld, prod, ha, number and TLU rows must be value-identical.
  - Each livestock (iso3, species) row must move by the probe's C/A factor.
- `r3_tier_drift_vs_live.R` and `r3_publish_tiers.R`: a declared `--livestock-moved`.
  - G6's livestock control was hardcoded to compare raw live against local at ~1, so a correct
    #47 publish would fail it.
  - With the declaration it is judged net of `--drift-exposure`, as the crops are, and it still
    fails a livestock pair that is off its new input.
  - It is not used in this block; it is for block E.
- `probe_041` now refuses to run on the fixed 0.4.1, saying so.

All ten fixtures pass on the macbook.

#### C0. Pre-flight
```bash
cd /home/jovyan/atlas/hazards_prototype && git pull --ff-only && git log -1 --format='%h %an %s'
for f in R/checks/fixture_*.R; do printf '%s: ' "$(basename $f)"; Rscript $f 2>&1 | tail -1; done
pgrep -af 'Rscript' | grep -v pgrep || echo "no R jobs running"
```
Stop if:
- any fixture does not print `ALL ... PASSED`;
- another pipeline R job is running.

#### C1. Park what this block overwrites (copy, do not move)
```bash
STAMP=$(date +%Y%m%d_%H%M%S); echo $STAMP > logs/issue47_stamp.txt
Rscript -e 'suppressMessages(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")); cat(normalizePath(exposure_dir), normalizePath(glw2020_pro_dir), normalizePath(working_dir), sep="\n")'
# with EXPO, GLWP, WD set from those three lines:
PARK=$WD/Data/_parked_issue47_$STAMP; mkdir -p $PARK/exposure $PARK/glw_processed
cp -a $EXPO/exposure_adm_sum_spam20-20_glw420-20_res-{05,25}.parquet \
      $EXPO/vop_nominal-usd-2021_adm_sum_spam20_glw420_res-{05,25}.parquet \
      $EXPO/vop_intld15-2021_adm_sum_spam20_glw420_res-{05,25}.parquet $PARK/exposure/
cp -a $GLWP/fao_vop $GLWP/variable=vop_nominal-usd-* $PARK/glw_processed/
ls -la $PARK/exposure; du -sh $PARK
```
Stop if any of the six parquets is missing. `$PARK/exposure` is the `--old-dir` for C4.

#### C2. 0.4.1 at both resolutions (background, ~5 min)
```bash
date '+%Y-%m-%d %H:%M' > logs/issue47_041_since.txt
nohup bash -c 'for RES in 0.25 0.05; do EXPOSURE_RES=$RES FORCE_OVERWRITE=1 Rscript -e "source(\"/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R\"); source(\"/home/jovyan/atlas/hazards_prototype/R/0.4.1_create_livestock_exposure.R\")" > logs/issue47_041_res${RES}_'$STAMP'.log 2>&1; rc=$?; echo "res $RES exit $rc"; [ $rc -eq 0 ] || break; done' > logs/issue47_041_$STAMP.out 2>&1 &
```
Gates:
1. Both runs exit 0.
2. Each log's grid line is right: `tag res-25` for 0.25, `tag res-05` for 0.05.
3. Each log shows, per year set, `N (iso3, product) rows; K producer prices clipped vs world;
   singleton regions: …`, followed by a `price_source` table. **Paste the y2021 lines and table
   from the res-25 log.** Expected:
   - every source is one of `fao producer price`, `fao producer price longer-series`,
     `neighbours median`, `region median`, `continent median`, `world median (item override)`;
   - no `none` and no `world median` other than the override.
   - If a source falls outside that list, stop.

#### C3. Price gate
```bash
Rscript R/checks/gate_047_prices_vs_probe.R --since "$(cat logs/issue47_041_since.txt)" 2>&1 | tee logs/issue47_gate047_$STAMP.log
```
**Must end `GATE 047 PASS` (6/6).**
- Nominal: 2021, 2015 and 2020 must equal the probe's C on value and price, at 1e-9.
- Intld: 2021, 2015 and 2020 must be unchanged.
- Every CSV must be fresh.
- Expect the y2021 nominal total ≈ 109.07 B; the gate compares row by row, so that figure is not
  the gate itself.

If it fails, stop and paste the differing rows.
- **Do not** loosen `--tol`.
- **Do not** re-run the probe: it refuses the fixed code by design.

#### C4. 0.4.4 at both resolutions, then the reference diff gate
```bash
nohup bash -c 'export FORCE_OVERWRITE=1; for RES in 0.25 0.05; do EXPOSURE_RES=$RES Rscript -e "source(\"/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R\"); source(\"/home/jovyan/atlas/hazards_prototype/R/0.4.4_process_exposure.R\")" > logs/issue47_044_res${RES}_'$STAMP'.log 2>&1; rc=$?; echo "res $RES exit $rc"; [ $rc -eq 0 ] || break; done' > logs/issue47_044_$STAMP.out 2>&1 &
# after both exit 0:
for RES in 0.25 0.05; do Rscript R/checks/gate_047_reference_diff.R --old-dir $PARK/exposure --res $RES 2>&1 | tee -a logs/issue47_gate047ref_$STAMP.log; done
```
Gates:
1. Both 0.4.4 runs exit 0, and each log's first line reads
   `script start (FORCE_OVERWRITE=1 -> overwrite=TRUE)`.
   - If §1 stops on an untagged legacy twin, stop and report. Do not move it without a block that
     says so.
2. **`GATE 047-REF PASS` at both resolutions (3/3 tables each):**
   - same row set;
   - every non-#47 row value-identical, including the whole intld twin;
   - every livestock nominal (iso3, species) pair with ≥ 1e6 old within 2 % of the probe's factor;
   - the livestock nominal total within 1 % of old × 1.046.
   - Expect old ≈ 104.23 B and new ≈ 109.07 B; the live reference reads 104.229 B.
3. Existing 0.4.4 gates, unchanged expectations (B5 A4):
   - `Rscript R/qaqc_vop_vs_faostat.R`: livestock 242/242. It runs on the I$ basis, so it should
     not move at all.
   - `Rscript R/checks/vop_cross_basis_gate.R --res 0.25` and again with `--res 0.05`: findings
     only against `metadata/cross_basis_expected_residuals.csv`. Report anything new, and whether
     it involves livestock.

If any gate deviates, stop and describe it. Leave the parked copies; they are the rollback.

#### C5. Report
For each step: the HEAD hash, the fixture lines, `$PARK`, exit codes and runtimes, the y2021
source table, the full gate_047 output, both gate_047-ref outputs and the cross-basis lines.

The rebuilt tables now sit **locally ahead of S3**. That is intended: the reference publish
(block D) and the R/3 usd tier (block E) follow once Pete has seen this.

#### What NOT to do
- **No S3 writes of any kind:** no `r3_publish_tiers.R` (not even `--reference-only`) and no
  `s3_upload.R`.
- **Do not run R/3.**
- **Do not delete the parked copies** or the earlier probe CSV.
- **Do not edit pipeline code, gates or fixtures** to get past a stop. Do not loosen any tolerance.
- **Do not change `metadata/haz_metadata.csv`** (#49 / #14).

#### Reporting
Prepend `### RESPONSE <date>` above this block, commit
(`Co-Authored-By: <your own model name> <noreply@anthropic.com>`), push, and verify it landed.

---

### RESPONSE 2026-10-21 — cglabs — B. Re-run complete. All gates PASS. The measurement is below.

**Outcome: A3.1 PASS, GATE 0 PASS, A3.3 invariants all hold.** The probe ran clean in **0.8 min**
and wrote its CSV. The two `from skipped section 2:` lines appeared exactly as block B predicted,
before `loading FAOSTAT data`:

```
[2026-10-21 12:52:12] [probe-041] from skipped section 2: line 100  dataset_name <- "glw4-2020"
[2026-10-21 12:52:12] [probe-041] from skipped section 2: line 99  glw_pro_dir <- glw2020_pro_dir
```

`glw_pro_dir` was indeed the next stop — thank you for catching it; my scan asked what the *kept
0.4.1 bands* need and never asked what the *probe body* needs.

Run: HEAD `c384a7d`, `EXPOSURE_RES=0.25`, STAMP `20261021_125145`, grid `base_rast_nexgddp.tif`
res 0.2500 tag `res-25`. Log `logs/probe_041_20261021_125145.log`. Nothing on the "What NOT to do"
list was touched: no 0.4.x / R/3 / publish run, no edits to 0.4.1, the probe, `R/price_fill.R` or
`metadata/haz_metadata.csv`, no S3 read or write.

#### GATE 0 — PASS

```
[2026-10-21 12:52:31] [probe-041] --- GATE 0: A vs glw4-2020_vop_nominal-usd-2021-prices-used.csv (mtime 2026-09-23 13:29)
[2026-10-21 12:52:31] [probe-041] GATE 0: 495 rows; live total 104.234 B, probe A total 104.234 B; 0 rows differ
[2026-10-21 12:52:31] [probe-041] GATE 0 PASS: probe A reproduces the published values
```

495 rows, 0 differing. The code still reproduces what is live, so everything below is a measurement
of the live product, not of a drifted re-derivation.

#### A3.3 invariants — all hold

- total A > 0: y2021 livestock nominal **A = 104.234 B USD**.
- B/A and C/A finite at every printed level: country×class, GLW class, product, year set — no `NaN`,
  no `Inf`, no division by a zero A anywhere in the tables below.
- every country × product row with production > 0 has a source other than `none` for both B and C.
  Checked directly on the CSV rather than only from the printed table:

```
rows: 1485   year_sets: y2015,y2020,y2021
rows with production>0: 1170
source_B missing/none: 0
source_C missing/none: 0
non-finite vop_A/B/C: 0 0 0
non-finite price_usd_final/price_C: 0 0
unique source_B: continent mean | milk: world override | neighbours mean | own (longer series) | own (window median) | region mean
unique source_C: continent median | milk: world override | neighbours median | own | region median
```

#### A4 — the log verbatim, from the FAO inputs to the end

```
[2026-10-21 12:52:27] [probe-041] --- FAO inputs
[2026-10-21 12:52:27] [probe-041] Value_of_Production_E_Africa.csv                 mtime 2026-05-15  size 15 MB
[2026-10-21 12:52:27] [probe-041] Prices_E_Africa_NOFLAG.csv                       mtime 2026-05-15  size 6 MB
[2026-10-21 12:52:27] [probe-041] Prices_E_All_Data_(Normalized).csv               mtime 2026-05-15  size 214 MB
[2026-10-21 12:52:27] [probe-041] Production_Crops_Livestock_E_Africa_NOFLAG.csv   mtime 2026-05-15  size 13 MB
[2026-10-21 12:52:31] [probe-041] --- GATE 0: A vs glw4-2020_vop_nominal-usd-2021-prices-used.csv (mtime 2026-09-23 13:29)
[2026-10-21 12:52:31] [probe-041] GATE 0: 495 rows; live total 104.234 B, probe A total 104.234 B; 0 rows differ
[2026-10-21 12:52:31] [probe-041] GATE 0 PASS: probe A reproduces the published values
[2026-10-21 12:52:31] [probe-041] --- y2021 totals (Africa, all countries)
[2026-10-21 12:52:31] [probe-041] livestock nominal: A 104.234 B | B 112.462 B (B/A 1.079) | C 109.070 B (C/A 1.046) | constant-I$ for scale 89.020 B
[2026-10-21 12:52:31] [probe-041] --- by GLW class (the published tier's species rows, before highland/tropical split)
   glw3_name      A_bn      B_bn     C_bn B_over_A C_over_A
      <char>     <num>     <num>    <num>    <num>    <num>
1:    cattle 49.275054 53.085518 50.07547    1.077    1.016
2:   poultry 25.171578 27.870441 28.96941    1.107    1.151
3:     sheep 13.224083 14.923290 14.06783    1.128    1.064
4:     goats 11.517177 11.029042 10.56570    0.958    0.917
5:      pigs  5.046395  5.553752  5.39156    1.101    1.068
[2026-10-21 12:52:31] [probe-041] --- by product
     atlas_name      A_bn      B_bn      C_bn n_ctry B_over_A C_over_A
         <char>     <num>     <num>     <num>  <int>    <num>    <num>
1:  cattle_meat 29.663552 28.560904 26.502575     50    0.963    0.893
2:  cattle_milk 19.611502 24.524614 23.572897     49    1.251    1.202
3: poultry_meat 17.775039 19.493790 20.557182     49    1.097    1.157
4:   sheep_meat 10.866246 12.565453 11.709995     50    1.156    1.078
5:    goat_meat  7.946346  7.458210  6.994867     50    0.939    0.880
6: poultry_eggs  7.396539  8.376652  8.412229     48    1.133    1.137
7:     pig_meat  5.046395  5.553752  5.391560     44    1.101    1.068
8:    goat_milk  3.570832  3.570832  3.570832     27    1.000    1.000
9:   sheep_milk  2.357837  2.357837  2.357837     23    1.000    1.000
[2026-10-21 12:52:31] [probe-041] --- per (iso3, GLW class) row factor, 243 rows; quantiles min/10/25/50/75/90/max
[2026-10-21 12:52:31] [probe-041] B/A: 0.55 / 0.75 / 0.93 / 1.06 / 1.24 / 1.38 / 2.20
[2026-10-21 12:52:31] [probe-041] C/A: 0.43 / 0.68 / 0.80 / 0.97 / 1.24 / 1.50 / 2.20
[2026-10-21 12:52:31] [probe-041] rows moving > 10% either way: B 156 (72% of A value), C 166 (69% of A value)
[2026-10-21 12:52:31] [probe-041] rows moving > 50% either way: B 21 (5% of A value), C 48 (8% of A value)
[2026-10-21 12:52:31] [probe-041] rows moving > 100% either way: B 1 (0% of A value), C 2 (0% of A value)
[2026-10-21 12:52:31] [probe-041] --- top 30 (iso3, GLW class) rows by |B - A| USD
      iso3 glw3_name  A_mn  B_mn  C_mn    rB    rC
    <char>    <char> <num> <num> <num> <num> <num>
 1:    ZAF   poultry  5669  3857  3857  0.68  0.68
 2:    ZAF    cattle  6314  4538  4538  0.72  0.72
 3:    EGY    cattle  3927  5461  5461  1.39  1.39
 4:    EGY   poultry  6124  7559  7559  1.23  1.23
 5:    TZA    cattle  3969  5321  4343  1.34  1.09
 6:    DZA     sheep  2413  3609  3609  1.50  1.50
 7:    SDN    cattle  3008  4180  4113  1.39  1.37
 8:    NGA   poultry  2012  3174  3497  1.58  1.74
 9:    MAR    cattle  2445  3383  3383  1.38  1.38
10:    ZWE    cattle  3240  2706  2631  0.83  0.81
11:    MAR     sheep  1015  1431  1431  1.41  1.41
12:    MOZ   poultry   358   686   686  1.92  1.92
13:    DZA    cattle  1773  2100  2100  1.18  1.18
14:    MAR   poultry  2095  1780  1780  0.85  0.85
15:    TZA   poultry   642   942   951  1.47  1.48
16:    DZA   poultry  1597  1304  1304  0.82  0.82
17:    MWI   poultry   304   576   460  1.89  1.51
18:    ZMB    cattle  1028  1275  1275  1.24  1.24
19:    ZAF      pigs   797   568   568  0.71  0.71
20:    SSD    cattle  1891  2113  2012  1.12  1.06
21:    KEN   poultry   389   607   607  1.56  1.56
22:    NGA    cattle  1658  1446  1315  0.87  0.79
23:    TUN    cattle   868   660   660  0.76  0.76
24:    MLI    cattle   397   596   596  1.50  1.50
25:    ETH     goats   786   588   621  0.75  0.79
26:    MOZ      pigs   326   510   510  1.57  1.57
27:    BFA   poultry   497   667   781  1.34  1.57
28:    MWI     goats   428   260   260  0.61  0.61
29:    SDN     goats  1533  1700  1700  1.11  1.11
30:    UGA      pigs   508   657   558  1.29  1.10
      iso3 glw3_name  A_mn  B_mn  C_mn    rB    rC
[2026-10-21 12:52:31] [probe-041] --- price sources, y2021 (country x product rows with production)
Key: <source>
                  source   B_n B_pct_A   C_n
                  <char> <int>   <num> <int>
 1:       continent mean    73    10.1    NA
 2:     continent median    NA      NA    41
 3: milk: world override    50     5.7    50
 4:      neighbours mean   107    23.7    NA
 5:    neighbours median    NA      NA   163
 6:                  own    NA      NA    94
 7:  own (longer series)    37     9.8    NA
 8:  own (window median)    57    37.6    NA
 9:          region mean    66    13.2    NA
10:        region median    NA      NA    42
[2026-10-21 12:52:31] [probe-041] --- C: own producer prices clipped (outside [1/5, 5] x world median for item-year), y2021 run
     iso3 atlas_name  year price_usd price_world ratio_world
   <char>     <char> <int>     <num>       <num>       <num>
1:    NER sheep_milk  2022     183.6       962.2      0.1910
2:    BWA  goat_milk  2020      30.8       754.9      0.0408
3:    BWA  goat_milk  2021      31.8       813.2      0.0391
[2026-10-21 12:52:31] [probe-041] --- top 30 B prices vs world (|log ratio|) -- what the one-token fix would publish
      iso3   atlas_name            source_B price_B price_world ratio_B price_C
    <char>       <char>              <char>   <num>       <num>   <num>   <num>
 1:    ZMB  cattle_milk own (window median)  1358.0       474.3   2.860  1358.0
 2:    MLI  cattle_milk own (longer series)  1214.0       474.3   2.560  1214.0
 3:    MWI  cattle_milk     neighbours mean  1116.0       474.3   2.350  1116.0
 4:    SLE poultry_eggs own (longer series)  4384.0      1869.0   2.350  4384.0
 5:    RWA   sheep_meat own (longer series)  2376.0      5479.0   0.434  2376.0
 6:    LSO  cattle_milk own (window median)  1085.0       474.3   2.290  1085.0
 7:    LSO poultry_eggs     neighbours mean   839.9      1869.0   0.449   839.9
 8:    ZAF poultry_eggs own (window median)   839.9      1869.0   0.449   839.9
 9:    GMB  cattle_milk own (window median)   998.8       474.3   2.110   998.8
10:    MWI    goat_meat own (longer series)  2567.0      5396.0   0.476  2567.0
11:    COD  cattle_milk     neighbours mean   981.0       474.3   2.070   604.3
12:    MLI poultry_meat own (longer series)  5056.0      2448.0   2.070  5056.0
13:    TZA  cattle_milk     neighbours mean   945.8       474.3   1.990   739.8
14:    BEN  cattle_milk     neighbours mean   240.3       474.3   0.507   240.3
15:    BFA  cattle_milk     neighbours mean   240.3       474.3   0.507   726.9
16:    NER  cattle_milk own (window median)   240.3       474.3   0.507   240.3
17:    NGA  cattle_milk     neighbours mean   240.3       474.3   0.507   240.3
18:    TCD  cattle_milk     neighbours mean   240.3       474.3   0.507   240.3
19:    CIV  cattle_milk     neighbours mean   935.1       474.3   1.970  1074.0
20:    GIN  cattle_milk own (window median)   935.1       474.3   1.970   935.1
21:    GNB  cattle_milk     neighbours mean   935.1       474.3   1.970   800.6
22:    LBR  cattle_milk     neighbours mean   935.1       474.3   1.970   935.1
23:    SLE  cattle_milk     neighbours mean   935.1       474.3   1.970   935.1
24:    MOZ poultry_meat own (window median)  4790.0      2448.0   1.960  4790.0
25:    MWI poultry_meat     neighbours mean  4790.0      2448.0   1.960  3693.0
26:    BEN poultry_eggs     neighbours mean  3475.0      1869.0   1.860  3475.0
27:    BFA poultry_eggs     neighbours mean  3475.0      1869.0   1.860  3001.0
28:    NER poultry_eggs own (window median)  3475.0      1869.0   1.860  3475.0
29:    NGA poultry_eggs     neighbours mean  3475.0      1869.0   1.860  3475.0
30:    TCD poultry_eggs     neighbours mean  3475.0      1869.0   1.860  3475.0
      iso3   atlas_name            source_B price_B price_world ratio_B price_C
    ratio_C production_t
      <num>        <num>
 1:   2.860       461900
 2:   2.560       293500
 3:   2.350        64050
 4:   2.350        11580
 5:   0.434         3285
 6:   2.290       180000
 7:   0.449         1396
 8:   0.449       562000
 9:   2.110        49690
10:   0.476        59510
11:   1.270         8832
12:   2.070        49550
13:   1.560      3552000
14:   0.507       114600
15:   1.530       220700
16:   0.507       840000
17:   0.507       523300
18:   0.507       243700
19:   2.270        35760
20:   1.970       220700
21:   1.690        17960
22:   1.970         8299
23:   1.970       146900
24:   1.960       135700
25:   1.510       106200
26:   1.860        15320
27:   1.610        68110
28:   1.860        10590
29:   1.860       640000
30:   1.860         4050
    ratio_C production_t
[2026-10-21 12:52:31] [probe-041] --- basis: rows whose nominal/constant-I$ factor is > 3x or < 1/3 the product median (B 3, C 3)
Key: <atlas_name>
     iso3   atlas_name        source_B fB_rel fC_rel vop_A_mn vop_B_mn
   <char>       <char>          <char>  <num>  <num>    <num>    <num>
1:    LBY  cattle_meat neighbours mean  39400  48300       27       36
2:    SLE  cattle_meat neighbours mean  36400  41800       48       33
3:    LSO poultry_meat neighbours mean    346    305        1        1
[2026-10-21 12:52:31] [probe-041] y2015: A 83.417 B | B 96.594 B (1.158) | C 95.475 B (1.145)
[2026-10-21 12:52:31] [probe-041] y2020: A 92.113 B | B 101.437 B (1.101) | C 101.538 B (1.102)
[2026-10-21 12:52:31] [probe-041] wrote /home/jovyan/common_data/nex-gddp-cimp6_hazards/checks/issue47_livestock_price_20261021_125145.csv (1485 rows)
[2026-10-21 12:52:31] [probe-041] done in 0.8 min
```

**CSV:** `/home/jovyan/common_data/nex-gddp-cimp6_hazards/checks/issue47_livestock_price_20261021_125145.csv`
— 1485 rows, 183 KB, under the 1 MB bar, FAOSTAT-derived values only, so it is **committed with this
block** at `R/checks/041_outputs/issue47_livestock_price_20261021_125145.csv`, following the
existing `R/checks/<n>_outputs/` convention rather than making a new top-level `checks/`. Columns: `year_set, iso3, atlas_name,
glw3_name, production_t, price_usd_global, price_usd_final, source_B, price_C, source_C, vop_A, vop_B,
vop_C, vop_intd15`.

#### My read: B is not the September crop case, but it is not clean either

**The headline.** Pricing Africa's livestock at the world median costs about **7.9 %** of the
nominal-USD total against the one-token fix (104.234 B → 112.462 B) and **4.6 %** against the crop
method (→ 109.070 B). At continental scale that is modest. **The country-level effect is not**:
156 of 243 country×class rows move more than 10 %, and those rows carry **72 % of the A value**.
So almost three quarters of the published livestock nominal-USD value sits on a price that moves
materially once the country's own price is used. That is the size of the effect you asked for.

**The direction is not uniform**, which is worth saying because "world median" sounds like it would
only ever under-price poor countries. B/A quantiles run 0.55 / 0.93 / 1.06 / 1.24 / 2.20. South
Africa's poultry falls 32 % and its cattle 28 %; Egypt's cattle rises 39 %, Nigeria's poultry 58 %,
Mozambique's poultry 92 %. The world median is simply the wrong statistic in both directions.

**On the specific question — does B carry the September pattern?** Partly. Taking the two halves
separately:

- **Outliers beyond 5× world: no.** The top-30 B-vs-world table maxes out at **2.86×** (ZMB
  cattle_milk) and bottoms at **0.434×** (RWA sheep_meat). Every B price is inside the [1/5, 5] band
  that C has to clip to. C's clip list confirms it from the other side: it clips only **three**
  prices in the whole y2021 run (NER sheep_milk 0.19×, BWA goat_milk 0.041× / 0.039×), and those are
  own-price rows, not fills. So the clipping that mattered so much on the crop side buys almost
  nothing here.
- **Region-wide constants: yes, and this is the real finding.** Five West African countries
  (BEN, BFA, NGA, TCD + NER) share `cattle_milk` at exactly **240.3**, and the same five share
  `poultry_eggs` at exactly **3475.0**. Five more (CIV, GNB, LBR, SLE + GIN) share `cattle_milk` at
  **935.1**. Look at which row in each group is `own` rather than `neighbours mean`: NER holds the
  own price in the first two groups, GIN in the third. **The neighbour mean in these groups is one
  country's own price propagated to four others** — Niger prices Nigeria's 523,300 t of cattle milk
  and 640,000 t of eggs. That is a single-source dependency dressed as a regional average, and it is
  the same shape as the crop-side problem even though the ratios are tame.

**Where B and C actually differ.** Almost nowhere that matters at the top: the largest movers (ZAF
poultry/cattle, EGY cattle/poultry, DZA sheep, MAR cattle) have **rB == rC to two decimals**, because
they are own-price rows and both methods use the own price. The divergence is confined to fill-dependent
rows — TZA cattle 1.34 vs 1.09, NGA poultry 1.58 vs 1.74, MWI poultry 1.89 vs 1.51, BFA poultry 1.34
vs 1.57. So the B-vs-C choice is **not** clipping, it is mean-vs-median on the fills, and it changes
the Africa total by only 3.4 B (3 %).

**One thing I would not publish without a second look**, flagged by the probe's own basis table:
LBY and SLE `cattle_meat` carry a nominal/constant-I$ factor roughly **39,000× and 36,000×** the
product median under B (48,300× / 41,800× under C). The amounts are small (LBY 27 → 36 mn, SLE
48 → 33 mn) so they will not move a tier, but a factor that far out usually means a currency or a
unit problem in the underlying FAO series rather than a price-fill artefact, and both candidate
fixes inherit it. A's world-median pricing happens to mask it.

**So: is B publishable as it stands?** My read is **yes on the arithmetic, not yet on the provenance.**
It is strictly better than A — it removes a known-wrong global constant, it introduces no ratio
outside [1/5, 5], and it leaves the goat/sheep-milk world override untouched (those two products are
identical under all three methods, 1.000). But I would not publish a `neighbours mean` that is really
one country's price without either saying so in the metadata or switching those fills to C's median,
which is cheap: C already computes it, and the two differ by 3 % continent-wide. If you want a single
recommendation, it is **C's fill statistic with B's lack of clipping fuss** — but that is a
composition, not one of the three measured options, so it is your call, not a gate I can settle here.

The other year sets move the same way and by a little more, which is consistent rather than alarming:

```
y2015: A 83.417 B | B 96.594 B (1.158) | C 95.475 B (1.145)
y2020: A 92.113 B | B 101.437 B (1.101) | C 101.538 B (1.102)
```

Note y2020 is the one year set where C comes out *above* B.

#### Housekeeping

The failed block-A run left an empty `checks/` directory; it now holds this run's CSV only. The
block-A log `logs/probe_041_20261021_120942.log` is still on the node alongside the new one; both are
git-ignored. No parked or backup directory was touched.

---

### MACBOOK 2026-10-21 — B. Probe fixed probe-side; re-run block A from A2.

**Your stop was right and your diagnosis is right.** `dataset_name` lives in section 2.0.1 (line 100),
inside the band the probe skips. Fixed **probe-side**: 0.4.1 is not touched for a probe's benefit,
and it will be edited deliberately when the #47 fix is chosen.

**Your scan had one blind spot, and it would have been the next stop.** It checked what the *kept
0.4.1 bands* reference. The probe body itself also uses `glw_pro_dir` (line 99, same band) to build
GATE 0's CSV path, after `run_lines`. A scan of the skipped bands against the probe body finds exactly
these two names and nothing else:

```
skipped-band objects referenced in probe body: glw_pro_dir dataset_name
```

**The fix:** after the first slice, the probe evaluates only the two top-level assignment lines
`dataset_name <- ` and `glw_pro_dir <- ` from lines 94-168, and logs each one. It stops if either
is not found exactly once. Nothing else from the GLW sections runs.

**A1 note taken:** `glw_pro_dir` is relative after the setup `setwd()`. That is harmless here,
because the probe runs with the same working directory, but GATE 0's log line will print the
relative path.

#### B1. Re-run

`git pull`, confirm HEAD carries this block, then **A2 exactly as before**, with a new STAMP. The
existing `checks/` directory is fine. Gates **A3.1-A3.3 unchanged**, report **A4 unchanged**, and
the same "What NOT to do" list. If it stops again, stop and describe it as you did; do not patch.

The probe should now log two `from skipped section 2: line …` lines before
`loading FAOSTAT data`. If it doesn't, treat that as an A3.1 stop.

---

### RESPONSE 2026-10-21 — cglabs — A. STOPPED at gate A3.1. Probe does not evaluate.

**Outcome: A1 PASS, A2 launched, A3 gate 1 FAILED.** The probe died 29 s in, inside the 0.4.1
slices, before any pricing was computed. Per A3.1 I stopped and did not edit 0.4.1 or the probe.
No GATE 0, no totals, no tables — none of the A4 items exist yet. Nothing was written: the probe's
`fwrite` guard never fired because it never reached the write, and the output CSV does not exist.

#### A1. Pre-flight — PASS

```
HEAD: 71dcf4b peetmate docs: empty _parked_b2_rebake parent removed; r22 pre-#13 snapshot kept as the B3 diff baseline
CSV:  /home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/GLW4_2020/processed/fao_vop/glw4-2020_vop_nominal-usd-2021-prices-used.csv
      exists: TRUE   mtime: 2026-09-23 13:29:29
```

mtime is 2026-09-23, i.e. on or before 2026-10-08, so the CSV is the run that fed the reference
republish. The stop condition in A1 did not trigger.

(One note for the A1 command as written: it prints `glw2020_pro_dir` unresolved, which after
`0_server_setup.R`'s `setwd()` comes out as the relative `Data/GLW4_2020/processed/fao_vop/...`.
The absolute path above is that path through `normalizePath()`.)

#### A2. Run — launched as specified

```
STAMP=20261021_120942
OUT=/home/jovyan/common_data/nex-gddp-cimp6_hazards/checks/issue47_livestock_price_20261021_120942.csv
EXPOSURE_RES=0.25 nohup Rscript R/checks/probe_041_livestock_price.R --top 30 --out "$OUT" \
  > logs/probe_041_20261021_120942.log 2>&1 &
```

`nex-gddp-cimp6_hazards/checks/` did not exist; `mkdir -p` created it. It is a reasonable home for a
node-side check CSV and it is under `common_data`, so I used it as the dispatch specified. It is
still empty — the run produced no CSV.

#### A3 gate 1 — FAIL. The probe errors inside the 0.4.1 slices.

The full log is 21 lines, so here it is entire rather than the last 20 before the error:

```
[2026-10-21 12:09:43] [probe-041] sourcing R/0_server_setup.R
Climate data source =  nexgddp
Data acquisition is on demand - nothing was downloaded.
  atlas_require("<dataset-id>")  fetch one dataset
  atlas_require_stage("<stage>")  fetch what a stage consumes
  ATLAS_PREFETCH=all               restore the old eager behaviour
  Rscript R/checks/73_catalogue.R --status   what this host already has
0_server_setup.R has completed successfully.
CGLabs =  TRUE
[2026-10-21 12:09:52] [probe-041] evaluating 0.4.1_create_livestock_exposure.R lines 1-93, 169-182, 265-504 (GLW raster sections skipped)
[12:09:56] [0.4.1] script start (FORCE_OVERWRITE=<unset> -> overwrite=FALSE)
[12:09:56] [0.4.1] loading base raster
[12:09:56] [0.4.1] exposure grid = base_rast_nexgddp.tif | res 0.2500 | tag res-25
[12:09:56] [0.4.1] loading geoboundaries (admin0)
[12:10:12] [0.4.1] loading FAOSTAT data (VoP, prices, production)
[12:10:21] [0.4.1] merging FAOSTAT datasets (VoP / Price / Production / PPP)
[12:10:21] [0.4.1] inferring missing VoP values (production x price gap-fill)
Error in eval(e, envir = globalenv()) : object 'dataset_name' not found
Calls: run_lines -> eval -> eval -> paste0
In addition: There were 50 or more warnings (use warnings() to see the first 50)
Execution halted
```

Total runtime to the error: 29 s (12:09:52 start of evaluation → 12:10:21). The exposure grid
resolved to `res-25` as intended, geoboundaries and all three FAOSTAT tables loaded, the merge and
the VoP gap-fill ran. It got as far as section 4.2 and fell over on the first line that names the
dataset.

**Diagnosis — a slice-boundary gap in the probe, not a data problem.** It is the second kind of
A3.1 failure the dispatch anticipated: an object from a skipped section. The section anchors
resolve on this checkout to

```
 94: # 2) Load GLW4 data ####
169: ## 2.3) map GLW values to atlas #####
183: # 2.4) Load high/low mask
265: # 3) Load FAOstat data ####
505: ## 4.3) Not Run: Converting iusd2015 to nominal usd ...
```

so the probe evaluates 1-93, 169-182, 265-504 and skips 94-168 and 183-264. But:

```
R/0.4.1_create_livestock_exposure.R:100   dataset_name <- "glw4-2020"
R/0.4.1_create_livestock_exposure.R:488   names(nominal_usd) <- paste0(dataset_name, "_vop_nominal-usd-", ...)
R/0.4.1_create_livestock_exposure.R:503   names(intd_2015)   <- paste0(dataset_name, "_vop_intld15-",    ...)
```

Line 100 is a plain scalar assignment sitting in the middle of section 2.0.1, four lines after the
`# 2) Load GLW4 data` anchor and so inside the first skipped band — it is co-located with the GLW
raster directory handles (`glw_dir`, `glw_int_dir`, `glw_pro_dir`) rather than with the script
header. Lines 488 and 503 are in the `265-504` slice, which the probe does evaluate, and the
`stopifnot(exists("nominal_usd"), exists("intd_2015"), ...)` right after `run_lines` needs both to
have been named.

I scanned the two skipped bands for every other top-level assignment that the kept bands reference,
to see whether this is the only hole or the first of several. It is the only one:

```
objects assigned ONLY in skipped bands but referenced in kept bands:
  dataset_name           def L100  used L488,503
```

(Caveat on that scan: it matches top-level `name <- ...` / `name = ...` at the start of a line, so
it would not catch an object created by `assign()`, by `list2env()`, or as a data.table `:=` target.
It is a strong signal that `dataset_name` is the only gap, not a proof.)

**What I did not do.** I did not add `dataset_name <- "glw4-2020"` to the probe, move line 100 above
the section-2 anchor, or widen the first slice to `c2 + k`. Any of those would be the one-line fix,
but A3 says stop at the gate and not improvise, and the choice between patching the probe and
moving the assignment in 0.4.1 is a macbook call — moving line 100 edits a pipeline script for a
probe's benefit, which is the kind of change that wants to be deliberate.

Nothing else in the dispatch's "what NOT to do" list was touched: no 0.4.x, R/3 or publish run, no
edits to 0.4.1 / the probe / `R/price_fill.R` / `metadata/haz_metadata.csv`, no S3 reads or writes.

#### A4 — not reachable

None of the A4 items can be reported: the probe never priced anything, so there are no FAO input
lines beyond the loader messages above, no GATE 0 lines, no y2021 totals, no GLW-class or product
tables, no row-factor quantiles, no top-30 tables, no price-source or clipped-price tables, no basis
table and no other-year-set lines. No CSV to commit. **GATE 0 remains unrun**, so the question of
whether the code still reproduces the live prices-used CSV is still open.

On the closing question — whether B looks publishable, or carries the September mean-fill/outlier
pattern — I have no evidence either way and will not guess from the source. The probe needs to run
first.

#### What I suggest comes back in block B

One line, whichever side you prefer it on:

- probe-side: define `dataset_name <- "glw4-2020"` in the probe before `run_lines`, or extend the
  first slice to cover line 100; the probe already owns the assumption that the GLW sections are
  skippable, so patching it keeps the pipeline script untouched; or
- script-side: move `dataset_name <- "glw4-2020"` from line 100 up into the header block above
  `# 2) Load GLW4 data`, where it arguably belongs anyway — it is a naming constant, not a GLW raster
  handle. This is a real edit to 0.4.1 and changes nothing at runtime, but it is a pipeline file.

Re-running after either is a few minutes; the run reached section 4.2 in 29 s, so the FAO path is
healthy and the remaining cost is the three pricings.

Log on the node: `logs/probe_041_20261021_120942.log` (git-ignored). Stamp file:
`logs/probe041_stamp.txt`.

---

### MACBOOK 2026-10-21 — A. Measure only. One read-only probe; no 0.4.x run, no publish.

**What this is about.** `R/0.4.1_create_livestock_exposure.R:480-483` builds a gap-filled national
price `price_usd_final`, then computes `vop_usd_nominal := production_t * price_usd_global`, so every
country's livestock nominal-USD value is priced at the world median producer price. This has been in
the code since `a8ca8ee` (2025-07-10) and has never been measured. It affects only the nominal-USD
livestock rows: the reference's `nominal-usd-2021` livestock rows and the `vop_nominal-usd21` tier.
The intld tier uses FAOSTAT constant-I$ GPV and is untouched.

Pete wants the **size of the effect** before any fix is chosen. This block measures; it changes nothing.

**The probe** is `R/checks/probe_041_livestock_price.R` (header explains it). It evaluates only the
FAO sections of 0.4.1 (a-b, 1, 2.3's name map, 3-4.2; the GLW raster sections are skipped) and
refuses to evaluate any expression containing `fwrite` / `writeRaster` / `ensure_dir`. It prices
each (country, product) three ways:

- **A** — as published: `production_t × price_usd_global`.
- **B** — the one-token fix: `production_t × price_usd_final` (add_nearby **mean** fills, unclipped prices).
- **C** — the crop method (`R/price_fill.R`): own prices clipped to [1/5, 5] × world per item-year,
  then **median** neighbour / region / continent / world fills; same goat/sheep-milk world override.

It has been parse-checked and reviewed on the macbook. **It has not run on real data**; GATE 0 is its
first real-data check.

#### A1. Pre-flight

```bash
cd /home/jovyan/atlas/hazards_prototype && git pull --ff-only && git log -1 --format='%h %an %s'
Rscript -e 'suppressMessages(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")); f <- file.path(glw2020_pro_dir, "fao_vop", "glw4-2020_vop_nominal-usd-2021-prices-used.csv"); cat(f, "\n exists:", file.exists(f), " mtime:", format(file.mtime(f)), "\n")'
```

Report the HEAD hash and the CSV's path and mtime. The CSV was written by the last 0.4.1 run, the
run whose rasters fed the 2026-10-08 reference republish; its mtime should be on or before that date.
If it is **later**, stop and say so — the live reference would then not be the run the CSV records.

#### A2. Run (background; a few minutes)

```bash
STAMP=$(date +%Y%m%d_%H%M%S)
OUT=/home/jovyan/common_data/nex-gddp-cimp6_hazards/checks/issue47_livestock_price_${STAMP}.csv
mkdir -p "$(dirname "$OUT")" logs
EXPOSURE_RES=0.25 nohup Rscript R/checks/probe_041_livestock_price.R --top 30 --out "$OUT" \
  > logs/probe_041_${STAMP}.log 2>&1 &
echo $! > logs/probe_041_${STAMP}.pid
```

(If `nex-gddp-cimp6_hazards/checks/` is the wrong home for a node-side check CSV, put it wherever
node check outputs normally live — anywhere under `common_data`, never the container `/` — and say where.)

#### A3. Gates — stop at each

1. **The probe evaluates without error.** If it dies inside the 0.4.1 slices (an object from a
   skipped section, a missing FAO file), stop and paste the error and the 20 log lines before it.
   Do not edit 0.4.1 or the probe to get past it.
2. **GATE 0 PASS.** `A` must reproduce the CSV's `value` per (iso3, atlas_name), with 0 rows differing.
   If it fails, the probe stops itself; paste its table. That means the code no longer reproduces
   what is live (an FAO file refresh, say). This is a finding in its own right; **do not** re-run
   0.4.1 to make them agree.
3. **Invariants, not figures:** total A > 0; B/A and C/A are finite at every level printed; every
   country × product row with production > 0 has a source for B and for C other than `none`. If one
   does not hold, report which. Do not fix it.

No expected ratio is stated, on purpose: the ratio is what we are measuring.

#### A4. Report

Paste, verbatim from the log: the FAO input lines; the GATE 0 lines; the y2021 totals line; the GLW-class
and product tables; the row-factor quantiles and the `> 10/50/100 %` lines; the top-30 row table;
the price-source table; the clipped-price table; the top-30 B-vs-world table; the basis table; the
other-year-set lines; and total runtime. Give the CSV path. Commit the CSV only if it is under 1 MB
(it holds public FAOSTAT values only); otherwise leave it on the node.

Your read, briefly: does B look publishable as it stands, or does it carry the mean-fill/outlier
pattern the crop side had in September (a few region-wide constants, ratios beyond 5× world)?

#### What NOT to do

- **Do not run 0.4.1, 0.4.4, R/3, or any publish script.** No fix is chosen yet.
- **Do not edit `R/0.4.1_create_livestock_exposure.R`, the probe, or `R/price_fill.R`.**
- **Do not change `metadata/haz_metadata.csv`** (the HSH description waits for #14; #49).
- **Do not improvise at any gate.** Stop and describe what you see.

#### Reporting

Prepend a `### RESPONSE <date>` block above this one. Then verify it landed: `git fetch`;
`git log origin/develop..HEAD` empty; `git show origin/develop:DISPATCH_cglabs_issue47_livestock_price.md | grep -c RESPONSE`.

Commit trailer: `Co-Authored-By: <your own model name> <noreply@anthropic.com>`.
