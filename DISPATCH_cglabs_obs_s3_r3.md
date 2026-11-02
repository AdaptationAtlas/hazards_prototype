# DISPATCH — cglabs — observed pipeline S2 → S3-S5 → R/3 §4 → publisher dry-run, smoke first, then full

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### RESPONSE — cglabs 2026-11-02 (5) — (b) the basis IS pre-#52 (`number`), and the env var clears it 74 → 0; (c) **all 16 VoP rows are soil-hole rows**: one bundle-NA cell per admin2 unit, four units in total, arithmetic closes exactly. No defect found. Read-only, nothing written outside `logs/`.

Both answers are measurements on the J6 tree, stamp `20261102_062845`. Probe scripts kept at `logs/{b_total,b_verify,b_split,c1b_all8,c2a_vars,c2b,c3_pot}.R`.

#### (b) `total-*` — the basis is pre-#52. Answer: `number`.

`{NWD}/Data/exposure/exposure_adm_sum_spam20-20_glw420-20_res-25.parquet`, mtime **2026-10-21 17:00**, i.e. two days
before #52 landed (2026-10-23):

| crop | exposure | unit | rows |
|---|---|---|---|
| `total-highland` | `number` | `number` | 5,097 |
| `total-tropical` | `number` | `number` | 5,097 |

`exposure == "tlu"` matches **0 rows** in the file; the only values present are `vop`, `harv-area`, `phys-area`,
`prod`, `number`. So it is the pre-#52 file, exactly as you predicted, and the `head_n` basis still contains the two
TLU aggregates.

**No post-#52 basis exists on the node** — nothing under `{NWD}/Data/exposure/` is newer than 2026-10-21. So the
remedy is the env var, not a different file.

**Verified**, `tier_vs_exposure` called directly, `baseline = scenarios = "observed"`, all three `head_n` tiers:

| `no_product_crops` | material | breach | **missing_product** | unmodelled | G6b gate 3 |
|---|---|---|---|---|---|
| default nine | 357 | 111 / 111 / 113 | **74** | 0 | `74 pair(s)` **FAIL** |
| nine + `total-tropical,total-highland` | 357 | 111 / 111 / 113 | **0** | 104 | `0 pair(s)` **PASS** |

Identical on severe, moderate and extreme. The 74 rows are **only** `total-*` — `total-tropical` 51 countries
(317.0 M head), `total-highland` 23 countries (58.3 M head), and **no other crop** is in that bucket. Gate 3 goes
clean; the 104 rows move into `unmodelled`, which is reported and not gated.

**Correction to my (4) block: the count is 74, not 72** (51 + 23, not 50 + 22). My earlier figure came off the cause
CSV rather than the gate, and undercounted by two.

So: run `head_n` with
`G6B_NO_PRODUCT_CROPS="vegetables,tropical-fruit,rest-of-crops,other-roots,temperate-fruit,other-cereals,other-pulses,other-oil-crops,other-fibre-crops,total-tropical,total-highland"`
until the exposure twin is re-baked post-#52. Gate 1 (`material pairs within tol`) still fails on its own account —
that is the soil hole, item (d), not this.

#### (c) the 16 VoP rows — **soil-hole, all of them.** Your border-cell lead was right.

**1. On the gate's own admin2 basis, the shortfall is not spread — it sits in one unit per pair.**

All eight pairs, E and T per admin2 unit (vop_usd, moderate; T is any+none, max over `hazard_vars`):

| pair | E | T | ratio | units with E>0 | units short |
|---|---|---|---|---|---|
| SLE banana | 3,274,343 | 504,419 | 0.1541 | 5 | **1** |
| SLE potato | 421,187 | 0 | 0.0000 | 1 | **1** |
| SLE yams | 1,241,358 | 891,092 | 0.7178 | 5 | **1** |
| SLE cotton | 51,928 | 38,023 | 0.7322 | 5 | **1** |
| SOM plantain | 8,184 | 25 | 0.0031 | 1 | **1** |
| SOM cowpea | 25,612 | 13,821 | 0.5396 | 2 | **1** |
| UGA lentil | 3,963 | 3,767 | 0.9505 | 6 | **1** |
| MOZ robusta-coffee | 47,272 | 45,609 | 0.9648 | 4 | **1** |

Every other unit reproduces its exposure at **1.0000 exactly**. The eight pairs reduce to **four admin2 units**:

| iso3 | admin2 | gaul2 | pairs | shortfall |
|---|---|---|---|---|
| SLE | **Kambia** | 105522 | 4 | 3,555,282 |
| SOM | **Badhaadhe** | 105570 | 2 | 19,950 |
| MOZ | **Lago** | 104396 | 1 | 1,663 |
| UGA | **Namayingo** | 106299 | 1 | 196 |

All four are edge units: Kambia on the Great Scarcies estuary at the Guinea border, Badhaadhe on the coast at the
Kenya border, Lago on the Lake Niassa shore, Namayingo on the Lake Victoria shore. That is why my admin0 raster mask
assigned their cells elsewhere and reported BNA = 0.

**2. It is not VoP-specific — it is unit-wide. `min_exposure` is why only VoP surfaced.**

Same units, same crops, all three physical variables:

| unit | crop | ha | prod_t | vop_usd |
|---|---|---|---|---|
| Kambia | banana | 0.1271 | 0.0659 | 0.0716 |
| Kambia | potato | 0.0000 | 0.0000 | 0.0000 |
| Kambia | yams | 0.5868 | 0.5569 | 0.5564 |
| Kambia | cotton | 0.1711 | 0.1938 | 0.1927 |
| Badhaadhe | plantain | 0.0000 | 0.0000 | 0.0031 |
| Badhaadhe | cowpea | 0.0000 | 0.0000 | 0.0002 |
| Lago | robusta-coffee | 0.8658 | 0.8824 | 0.8777 |
| Namayingo | lentil | 0.0000 | 0.0000 | 0.0798 |

`ha` and `prod_t` are short by the same proportion. They never reached G6b's `material` bucket because in these units
their exposure is below `min_exposure = 1e3` (hectares and tonnes), while the same cells in dollars clear it. **The
"VoP-specific" shape was an artefact of the materiality threshold, not of the VoP route.**

**3. On the raster, every pair's loss is one single cell, and that cell is soil-NA.**

Per unit: admin2 rasterised with `touches = TRUE` on the res-25 grid; VoP res-25 exposure; the obs bundle
`<crop>_chirps-chirts-agera5_moderate_NDWS+NTx35+NDWL0_int.tif`; `annual12/NDWS_mean.tif` and `NTx35_mean.tif`.

| unit | crop | cells | VoP cells | **VoP on bundle-NA** | value there | = admin2 shortfall? | NTx ok & NDWS NA | NTx NA |
|---|---|---|---|---|---|---|---|---|
| Kambia | banana | 14 | 6 | **1** | 2,769,924 | **yes** | 1 | 0 |
| Kambia | potato | 14 | 2 | **1** | 421,187 | **yes** | 1 | 0 |
| Kambia | yams | 14 | 6 | **1** | 350,266 | **yes** | 1 | 0 |
| Kambia | cotton | 14 | 4 | **1** | 13,905 | **yes** | 1 | 0 |
| Badhaadhe | plantain | 26 | 4 | **1** | 8,159 | **yes** | 1 | 0 |
| Badhaadhe | cowpea | 26 | 4 | **1** | 11,791 | **yes** | 1 | 0 |
| Lago | robusta coffee | 19 | 3 | **1** | 1,663 | **yes** | 1 | 0 |
| Namayingo | lentil | 6 | 2 | **1** | 196 | **yes** | 1 | 0 |

Every row closes exactly: the VoP carried on the single bundle-NA cell **equals** E − T from the admin2 table, to the
dollar. On that cell `NTx35_mean` is present and `NDWS_mean` is NA — the climate is there, the water balance is not.
`bNA_ntx_NA = 0` everywhere, so none of this is the outside-obs-mask class.

Your second sub-question, VoP cells with no `prod_t`: 0 for Kambia and Lago, 2 each for Badhaadhe and Namayingo, and
none of those is the lost cell. Footprint differences are not the cause.

**4. SLE potato and the `ggcmi-pot` short-window lead — ruled out.**

Window length on every Kambia cell, from `pot_rf_ggcmi_crop_calendar_phase3_v1.01.nc4`: **5 months** on thirteen of
the fourteen, 9 on the other. **No cell under 3 months.** The lost cell has a 5-month window, `NDWS_mean` NA in both
`ggcmi-pot` and `annual12`, and `NTx35_mean` present. The 312 `< 3 months` cells that window reports are elsewhere.

#### What this means

The 16 VoP rows are **not a defect and not a separate class** — they are soil-hole rows that my admin0 method
misclassified, exactly as you suspected. The honest total for item (d) is therefore **1,338 + 16 = 1,354 soil-hole
pairs**, 49 outside-obs-mask, and 74 `total-*` no-product rows cleared by the env var. There is nothing here that a
soil fill would not also fix.

Nothing running. Nothing written outside `logs/`. K3 still held.

---

### RESPONSE — cglabs 2026-11-02 (6) — **L0-L3 complete, the publisher dry-run is CLEAN: all 15 tiers pass G1, G2, G3, G4 and G6b.** Soil fill took obs land cells without soil 2,494 → 49; every soil-hole breach is gone (max |ratio-1| 0.0078 against tol 0.02); MUS/CPV/SYC are ALLOWED; `total-*` gone on the post-#52 basis with no env var; KEN maize and cassava 0.976 / 0.948 → **1.0000**; the eight VoP pairs → **1.0000**. Nothing published. K3 ready.

Stamp `20261102_192848`. HEAD at launch `017e5c4`. Env grep empty before R/3 and the publisher.

#### L0 — selftests, both PASS

`soil_fill.py selftest` **9/9**, `fixture_publish_tier_gates.R` **48/48**, both as the block states.

**Node finding, environment only, no code touched.** Both python selftests fail out of the box on this node because the JupyterHub image exports geo paths that override the `obs` env:

```
PROJ_DATA=/opt/conda/share/proj        GDAL_DATA=/opt/conda/share/gdal
GDAL_DRIVER_PATH=/opt/conda/lib/gdalplugins
```

Two failures follow. First `rasterio.errors.CRSError: ... /opt/conda/share/proj/proj.db contains DATABASE.LAYOUT.VERSION.MINOR = 6 whereas a number >= 7 is expected` — the env ships its own newer `proj.db`. Then, with PROJ fixed, `rasterio.errors.DriverCapabilityError: Writer does not exist for driver: LIBERTIFF` — the leaked plugin path makes GDAL open the TIFF with the read-only LIBERTIFF driver, and `soil_fill.py:58` copies `s.profile`, so the driver travels to the write. Running as

```bash
env -u GDAL_DRIVER_PATH PROJ_DATA=<env>/share/proj GDAL_DATA=<env>/share/gdal $PY python/obs/soil_fill.py ...
```

both selftests and the fill pass. Worth pinning in `server-environment-cglabs.md`, and `soil_fill.py` would be sturdier naming `driver="GTiff"` on write rather than inheriting the reader's — your call, I changed nothing.

#### L1 — the fill. PASS, 8.7 s.

The real `pr` path on this tree is **`$OR/nex-gddp-cmip6/pr/historical/chirps-chirts-agera5/pr_2015-06-15.tif`** — variable before scenario, not the order in the block.

| layer | res | max px | valid | filled | still NA | filled range | source range |
|---|---|---|---|---|---|---|---|
| `sscp_world.tif` | 0.05000 deg | 10 | 3,712,662 | 638,524 | 10,044,814 | 7.392 .. 107.25 | 4.97 .. 111 |
| `ssat_world.tif` | 0.05000 deg | 10 | 3,712,662 | 638,524 | 10,044,814 | 10.127 .. 183.05 | 6.78 .. 200 |

Both filled ranges sit strictly inside their source ranges. `sroot_world.tif` is not in the source and was skipped; `africa_scp`, `africa_ssat`, `maize_soil_depth_{1,5}km` copied unchanged.

**`--check-pr`, the gate:** obs land cells (pr finite) 48,576; soil NA on **2,494 → 49** after the fill, identically for both layers. That is **1.96 % of "before"**, well inside the ~10 % stop.

**The shared soils are untouched:** all six md5 identical before and after
(`logs/obs_soilfill_src_md5_{before,after}_20261102_192848.txt`). The script replaced the `$OR/atlas_hazards/soils` **symlink** with a directory of filled copies and logged that it was doing so; `{common_data}/atlas_hazards/soils` never opened for write.

#### L2 — rerun

**S2** (`logs/obs_s3_full_s2_{ndws,ndwl0}_fill_<stamp>.log`), NDWS 49.8 min, NDWL0 82.7 min, both rc 0:

| index | index files | AVAIL files | rewritten in this run | WARN | error/halt | `daily inputs missing` |
|---|---|---|---|---|---|---|
| NDWS | **252** | 252 | 504 / 504 | 0 | 0 | 0 |
| NDWL0 | **252** | 252 | 504 / 504 | 0 | 0 | 0 |

(The directory holds 504 tifs per index: 252 `<IDX>-YYYY-MM.tif` plus 252 `AVAIL-YYYY-MM.tif` carry-over state. The J6 count of 252 is the index files; stating it so the next reader does not take 504 for a doubling.) NTx and THI were **not** rerun.

**S3-S5**, `--overwrite`, both commands as in J6: 44 min and 18 min, 0 WARN, 0 error, `done: 129 bundles` + `done: 30 bundles` = **159** `*_int.tif`. The `annual12` stacks were rewritten 20:55-20:56, so K3's precondition holds on fresh files.

**Two numbers moved, and the fill is why.** `maize calendar NA on land cells: 2 → 110`, and `window annual12 ... =12: 165,026 → 165,134` (+108). Land is defined as cells with an NDWS value, so filling the soil holes *added* about 108 land cells; some of them have no GGCMI maize calendar and fall to the January anchor. Every GGCMI window now reports `NA on land: 110` where J6 read 2. Expected, not a fault, but the J6 figure will not reproduce.

**R/3** (`logs/obs_s3_full_r3_fill_<stamp>.log`, 21:40 → 21:51, 11 min, COMPLETE): §4.1 **`input files = 159`**, 0 errors, and the same expected skips as J6 — 60 livestock files for `ha` and for `prod_t`, 99 crop files for `head_n`.

#### L3 — the publisher dry-run, exposure gate on. **Every gate passes on every tier.**

`logs/obs_s3_pub_dry_full_fill_<stamp>.log`, 0.7 min, `[DRY RUN - nothing written]`.

| gate | result |
|---|---|
| **G1** size, per-source floor | **15 / 15 ok** (0.6-2.4 MB against the new 0.2 MB observed floor; 0 FAIL) |
| **G2** every `hazard_vars` has `none` rows, n(none) == n(any) | **15 / 15 ok** |
| **G3** scenarios | **15 / 15 ok**, `scenarios = observed` |
| **G4** severity column | **15 / 15 ok** (5 severe, 5 moderate, 5 extreme) |
| **G6b** exposure identity | **15 / 15 ok** |

G6b per variable (the three tiers of each agree except where noted):

| variable | material | **breach** | missing_product | allowed (named) | max \|ratio-1\| | immaterial |
|---|---|---|---|---|---|---|
| prod_t | 601 | **0** | **0** | 3 | 0.0072 | reported |
| ha | 539 | **0** | **0** | 2 | 0.0068 | reported |
| head_n | 335 | **0** | **0** | 0 | 0.0025 (extreme 0.0030) | reported |
| vop_usd | 1337 | **0** | **0** | 23 | 0.0078 | 394 |
| vop_intld | 1332 | **0** | **0** | 21 | 0.0078 | 1046 for ha |

Against J6: prod_t 604/240, ha 541/212-213, head_n 335/103-104 **+72 missing**, vop_usd 1360/391, vop_intld 1351/394. **Every breach is gone**; the worst pair in the whole publish is 0.78 % off, against a 2 % tolerance.

- **soil-hole breaches: gone.** The 1,354 pairs (1,338 plus the 16 VoP rows my RESPONSE (5) reclassified) no longer breach.
- **MUS / CPV / SYC: ALLOWED.** `G6b named residuals loaded: TZA:coconut, MUS:*, CPV:*, SYC:*`, and the breaching island pairs print under `NAMED residuals, breaching but allowed with a reason` with the bbox text, e.g. MUS sugarcane E 2,984,843 → T 0, CPV sugarcane E 21,938 → T 0.
- **`total-*`: gone, and no env var was needed.** Gate 3 reads `0 pair(s)` on all 15 tiers. issue54 B rebuilt the res-25 twin post-#52, so `total-tropical` / `total-highland` now carry `exposure = tlu` and never enter the `head_n` basis. The `G6B_NO_PRODUCT_CROPS` workaround from RESPONSE (5) (b) is **not needed and should not be used**.
- **"other" rows: none.** No breach of any kind survives, so there is nothing unexplained to stop on.

**KEN maize and cassava**, the pair that opened this thread at 0.976 and 0.948:

| variable | crop | E | T | ratio |
|---|---|---|---|---|
| prod_t | maize | 3,547,721 | 3,547,723 | **1.0000** |
| prod_t | cassava | 801,382 | 801,389 | **1.0000** |
| ha | maize | 2,189,423 | 2,189,424 | **1.0000** |
| ha | cassava | 65,812 | 65,811 | **1.0000** |
| vop_usd | maize | 1,145,567,268 | 1,145,567,275 | **1.0000** |
| vop_usd | cassava | 385,025,016 | 385,025,016 | **1.0000** |

**And the eight VoP pairs from RESPONSE (5) all close**, which is an independent confirmation that they were soil-hole rows:

| pair | ratio before | ratio now |
|---|---|---|
| SLE banana | 0.1541 | **1.0000** |
| SLE potato | 0.0000 | **1.0000** |
| SLE yams | 0.7178 | **1.0000** |
| SLE cotton | 0.7322 | **1.0000** |
| SOM plantain | 0.0031 | **1.0000** |
| SOM cowpea | 0.5396 | **1.0000** |
| UGA lentil | 0.9505 | **1.0000** |
| MOZ robusta-coffee | 0.9648 | **1.0000** |

#### State

**Nothing published** — dry run only, no S3 write of any kind. Nothing running. The filled soils live under `$OR/atlas_hazards/soils`; the shared copy is byte-identical to before. **K3 is now unblocked** on its own terms: L3 is clean and the `annual12` NDWL0 stack it reads was rewritten by this run at 20:56. Starting it next unless you say otherwise.

---

### MACBOOK 2026-11-02 (4) — Pete's decisions: fill the soil gap for the observed tree (#56), islands documented (#57); rerun from S2 water balance; order after the VoP probe and issue54

**Decisions (Pete, 2026-11-02).** (1) The 1,338 soil-hole pairs: **fill the observed tree's soil layers**. New
`python/obs/soil_fill.py`: GDAL FillNodata, inverse-distance from the nearest valid soil, ≤ 0.5°. NEX soils are
read-only and untouched; the investigation of better sources is #56. (2) MUS, CPV, SYC: **documented as outside V1
coverage**. `metadata/g6b_expected_residuals.csv` now has whole-country rows (`crop = "*"`, `source =
chirps-chirts-agera5`), and the gate (`tier_vs_exposure`) accepts them; the bbox extension is #57. Publish-gate
fixture 48/48; soil-fill selftest 9/9.

**Order on this node:** (1) the read-only VoP probe and `total-*` basis check (block (3) below); (2)
`DISPATCH_cglabs_issue54_intld_placeholders.md` A + B, which rebuilds the exposure basis the dry-run reads (post-#52
`tlu` slot); (3) this block.

#### L0 — selftests
```bash
cd $REPO && git pull --ff-only && git log -1 --format='%h %s'
$PY python/obs/soil_fill.py selftest                      # ALL SOIL-FILL SELFTEST ASSERTIONS PASSED (9)
Rscript R/checks/fixture_publish_tier_gates.R | tail -1   # ALL PUBLISH-GATE FIXTURE ASSERTIONS PASSED (48)
```

#### L1 — fill (replaces the `$OR/atlas_hazards/soils` symlink with filled copies; the shared soils stay read-only)
```bash
$PY python/obs/soil_fill.py fill --src-dir {common_data}/atlas_hazards/soils --out-root $OR \
    --check-pr $OR/nex-gddp-cmip6/historical/chirps-chirts-agera5/pr/pr_2015-06-15.tif > logs/obs_soilfill_$STAMP.log 2>&1
```
(Use the real `pr` daily path on this tree if it differs.) Report each layer's line: native resolution, filled
cells, still-NA, filled range against the source range. Also report the `--check-pr` before/after counts of obs
land cells without soil. **Expect "after" to be a small fraction of "before"; if more than ~10 % of "before"
remains, stop:** that means 0.5° is too short or the holes are not edge cells. The shared `{common_data}` soils' md5
must be unchanged (state it).

#### L2 — rerun what reads the soils, nothing else
```bash
cd $REPO/hazards_upstream/R/04_indices
export COMMON_DATA=$OR SCENARIO=historical YRS=2005:2025 DAYS_CHECK=strict AVAIL_SEED_MONTHS=2005-01 FORCE_OVERWRITE=1
GCMS=chirps-chirts-agera5 nohup Rscript fast_calc_NDWS.R  > $REPO/logs/obs_s3_full_s2_ndws_fill_$STAMP.log 2>&1 &  echo $! > ...pid
GCMS=chirps-chirts-agera5 nohup Rscript fast_calc_NDWL0.R > $REPO/logs/obs_s3_full_s2_ndwl0_fill_$STAMP.log 2>&1 & echo $! > ...pid
unset COMMON_DATA SCENARIO YRS DAYS_CHECK AVAIL_SEED_MONTHS FORCE_OVERWRITE
```
**Not** NTx or THI (they don't read soils). Same invariants as J6 S2 (252 files each, 0 WARN). Then S3-S5, both
commands as in J6 with `--overwrite` (the crop bundles and the livestock `NDWL0` wet slot change), then R/3 with
`FORCE_OVERWRITE=1 ATLAS_WORKING_DIR=$OR` as in J6, then the publisher dry-run with the exposure gate as in J6.

#### L3 — report
- §4.1 `input files = 159`; R/3 COMPLETE.
- G6b per tier `material / breach / missing_product / allowed`, and the breach list classified as in J6:
  soil-hole / outside-obs-mask / other.
- **Expect:** soil-hole breaches mostly gone; MUS/CPV/SYC shown as ALLOWED; `total-*` gone if issue54 B ran.
  The 16 VoP rows are whatever the probe found. **Stop on any "other" row** not explained by the probe.
- KEN maize / cassava ratios, which were 0.976 / 0.948 before the fill.

K3 (rangeland S6, `DISPATCH_cglabs_obs_fpar.md`) runs **after** L3 is clean: S6 reads the NDWL0 annual12 stack
this rerun rewrites.

### MACBOOK 2026-11-02 (3) — G1 fixed (per-source floor); total-* is the basis vintage; the 16 VoP rows need one read-only investigation; soil/island decision pending Pete; K3 still held

Excellent run: full Africa to R/3 with no pipeline fault, and a classification that made each failure tractable.

**(a) G1 — fixed** in `scripts/r3_publish_tiers.R`: the size floor is now per source (`SOURCE_SPECS$g1_min_bytes`:
NEX 10 MB unchanged, observed 0.2 MB). Publish-gate fixture 44/44 (new assertion).

**(b) the 72 `total-*` head_n rows — basis vintage, not the product.** NEX carries no `total-*` bundle either; these
are TLU aggregates. Since #52 (0.4.4 §2.9, 2026-10-23) the twin files them under `exposure == "tlu"`, so a post-#52
head_n basis does not contain them (`R/checks/r3_tier_drift_vs_live.R` ~l.68). Check:
`exposure_adm_sum_spam20-20_glw420-20_res-25.parquet` — do the `total-*` rows carry `exposure == "number"` (pre-#52
file) or `"tlu"`? If `number`, either use the post-#52 basis or run with
`G6B_NO_PRODUCT_CROPS="<the default nine>,total-tropical,total-highland"`, as the gate's comment prescribes. Report which.

**(c) the 16 VoP rows — investigate, read-only.** These are not on any bundle-NA cell by your admin0 method, but the
gate's basis is admin2 with `touches = TRUE`. For SLE potato and SLE banana (the two worst) and SOM plantain:
1. On the gate's own basis: the admin2 units carrying the pair's exposure, and per unit E vs T.
2. In those units' cells, the VoP res-25 exposure raster vs the prod_t res-25 raster: are there VoP cells with no prod_t
   (VoP and prod footprints can differ) and is the obs bundle / `pr` NA on them?
3. For SLE potato: the `ggcmi-pot` window per cell (months), and whether the bundle is NA on short-window cells.
One table per pair. Write nothing. This decides whether (c) is a coverage edge (named residual) or a defect.

**(d) soil-hole (1,338 pairs) and outside-obs-mask islands (MUS, CPV, SYC; 49 pairs): Pete's call**, options in the
plan. No rerun until decided.

**K3: still held** — agreed, R/3 may rerun after (d). Nothing to launch except (c)'s read-only checks.

### RESPONSE — cglabs 2026-11-02 (4) — J6: S1→S2→S3-S5→R/3 complete on full Africa (159 bundles, §4.1 = 159). **STOP: (a) G1 fails all 15 tiers on its hard-coded 10 MB floor, so the publisher never reaches G6b; (b) G6b, run with the gate's own function, has 16 "other" VoP pairs (NEX = 1.000 there) and 72 livestock `total-*` pairs with no product.** K3 held.

Stamp `20261102_062845`. `ATLAS_WORKING_DIR=$OR` set explicitly on R/3 and the publisher. Env grep empty at each launch.

**Link:** `s1_ingest.py link --out-root $OR ... --nex-working-dir {NWD}`, rc 0. `Data/{mapspam,GLW4_2020,boundaries}`
symlinked into `$OR`; `chirps_wrld` and soils were already linked. `$OR` had no stale indices tree.

**S2** (`logs/obs_s3_full_s2_{ndws,ndwl0,ntx,thi,thi_rh15}_<stamp>.log` + `.pid`): COMMON_DATA=$OR YRS=2005:2025
DAYS_CHECK=strict AVAIL_SEED_MONTHS=2005-01.
- I launched all 5 at once, as in J2, not as two waves. Each script runs its own months in order.
- Wall-clock: NDWS 53 min, NDWL0 85 min.
- 0 WARN, 0 `daily inputs missing`, 0 error/halt. NTx and both THI end `----Finish----`; NDWS and NDWL0 end on `compute: 2025-12`.
- Counts: NDWS 252, NDWL0 252, NTx 31 dirs × 252, THI 252 `THI_max-` + 252 `THI_mean-` per token.

**S3-S5** (`logs/obs_s3_full_s35{,_rh15}_<stamp>.log`):
- `seasons kept=19 (2006..2024) timeframe=2006-2025`.
- `done: 129 bundles` (45 min) + `done: 30 bundles` (15 min) → **159** `*_int.tif`. 0 WARN, 0 error.
- Windows: all 16 GGCMI windows report `NA on land: 2` (2 Africa land cells with no GGCMI calendar; Kenya smoke had 0).
  annual12 and the custom windows report 0.
- `Data/obs_seasonal/chirps-chirts-agera5{,-rh15}_2006-2025/annual12/{NDWL0_mean,NDWS_mean,NTx35_mean,THI_max_max}.tif`
  exist (the K3 precondition).

**R/3** (`logs/obs_s3_full_r3_<stamp>.log`, 09:00 → 09:13, COMPLETE):
- §4.1 `input files = 159`.
- Same layout as the smoke: vop / vop_usd / n have 6 parquets (both tokens); ha / prod have 3.
- Expected skips: 60 livestock files for ha / prod_t, 99 crop files for head_n.

**Publisher dry-run, exposure gate on** (`logs/obs_s3_pub_dry_full_<stamp>.log`):
- `--drift-exposure` bases, all in `{NWD}/Data/exposure`:
  - `prod_t`, `ha`, `head_n` = `exposure_adm_sum_spam20-20_glw420-20_res-25.parquet`
  - `vop_usd` = `vop_nominal-usd-2021_adm_sum_spam20_glw420_res-25.parquet`
  - `vop_intld` = `vop_intld15-2021_adm_sum_spam20_glw420_res-25.parquet`
- **G1 FAIL on every one of the 15 tiers**, `is only 0.6–2.4 MB` (prod_t 0.9–1.2, ha 0.8–1.1, head_n 0.6–0.7,
  vop_usd 1.7–2.3, vop_intld 1.7–2.4).
- `scripts/r3_publish_tiers.R:390` is `if (file.size(local_f) < 10e6) { ... G1 FAIL ...; next }`: hard-coded, no
  option, and not source-aware. The NEX tiers carry 18 models × 5 scenarios, the observed tiers 1 × 1, so ~1-2 MB is the
  correct size. Because of the `next`, **G2-G6b never ran for any tier.** It needs a per-source G1 floor (e.g. in
  the `SOURCES` table at l.179). Not improvised.

**G6b anyway**, so the list you asked for exists. `R/checks/r3_tier_drift_vs_live.R::tier_vs_exposure`, sourced and
called exactly as the publisher would (l.425):
- `tol_pair 0.02`, per-variable `g6b_min`, `baseline = scenarios = "observed"`, the bases above, main token.
- Per tier, as `material / breach / missing_product`:

  | variable | material | breach | missing_product |
  |---|---|---|---|
  | prod_t | 604 | 240 | 0 |
  | ha | 541 | 212-213 | 0 |
  | head_n | 335 | 103-104 | **72** |
  | vop_usd | 1360 | 391 | 0 |
  | vop_intld | 1351 | 394 | 0 |

  `invented` 0 everywhere. All ratios ≤ 0.980: the product is short, never over.
- **Classification.** One row per (var, iso3, crop); full CSV on the node at `logs/obs_s3_g6b_cause_<stamp>.csv`.
- **Method:** the cells that drop out of `any + none` are the pair's bundle-NA cells. Of the pair's exposure on those cells
  inside the country, I measured the share where NDWS is NA while `pr` is present (the soil hole) and where `pr` itself
  is NA (outside the obs climate mask).
  - My first method, "hole exposure / (E − T)", was too noisy to classify with: my admin0 raster mask against the
    gate's admin2 exposure basis gave shares from −35 to 8.
  - This basis-free form is what the rows below use.

| class | pairs | detail |
|---|---|---|
| **soil-hole** (soil share ≥ 0.8; in fact = 1.00 for every row) | **1,338** | prod_t 238, ha 211, head_n 104, vop_usd 360, vop_intld 365. 43 countries; most UGA 85, TZA 84, MOZ 76, BDI 67, COD 62, GNB 59, SLE 58, MDG 58, RWA 53, KEN 51. Median ratio 0.956, p10 0.844. Worst: GNB tobacco / robusta-coffee / cocoa and ESH maize at ≈ 0 (whole crop on soil-NA cells). |
| **outside-obs-mask** (pr NA ≥ 0.8) | 49 | island states outside the S1 bbox/mask: MUS 23, CPV 18, SYC 8, e.g. MUS sugarcane 2.98 Mt → 0. Not soil; the obs product simply has no climate there. |
| **other: no-product rows** | 72 | `head_n` exposure crops `total-tropical` (50) / `total-highland` (22), 51 countries, 375 M head. The obs product has no `total-*` livestock bundle. I did not check whether NEX carries them or the gate exempts them for NEX. |
| **other: VoP shortfalls not on any NA cell** | **16** | see below |

The 16 VoP rows; BNA = exposure on bundle-NA cells = 0 for all of them, so they are not coverage holes:

| var | iso3 | crop | E | obs T | obs ratio | NEX ENSEMBLE historic T (admin2, best set) | NEX ratio |
|---|---|---|---|---|---|---|---|
| vop_usd | SLE | banana | 3,274,343 | 504,419 | 0.154 | 3,274,343 | 1.000 |
| vop_usd | SLE | potato | 421,187 | 0 | 0.000 | 421,187 | 1.000 |
| vop_usd | SLE | yams | 1,241,358 | 891,092 | 0.718 | 1,241,076 | 1.000 |
| vop_usd | SLE | cotton | 51,928 | 38,023 | 0.732 | 51,928 | 1.000 |
| vop_usd | SOM | plantain | 8,184 | 25 | 0.003 | 8,184 | 1.000 |
| vop_usd | SOM | cowpea | 25,612 | 13,821 | 0.540 | 25,612 | 1.000 |
| vop_usd | UGA | lentil | 3,963 | 3,767 | 0.951 | 3,963 | 1.000 |
| vop_usd | MOZ | robusta-coffee | 47,272 | 45,609 | 0.965 | 47,272 | 1.000 |
| vop_intld | (same 8 pairs) | | | | 0.000–0.965 | | |

- NEX reproduces these exactly, while obs loses up to 100%. Yet my method finds no exposure on obs bundle-NA cells
  inside these countries.
- So either the loss sits on border/coast cells that my admin0 raster assigns elsewhere (the gate's admin2 basis uses
  `touches = TRUE`), or something VoP- or crop-specific differs.
- One lead, not verified: SLE potato is a `ggcmi-pot` window crop, and that window has 312 cells with < 3 months
  (likely NA bundles).
- **These 16 rows are the stop.**

**K3 not started.** Its precondition holds (annual12 stacks exist), but S6 writes 30 bundles into
`$OR/Data/hazard_risk/ggcmi-crop/`, the folder R/3 reads. If the G1/G6b fixes require an R/3 rerun, §4.1 would then
read 189, not 159. Say GO if a rerun is not expected, or once R/3 is final.

Nothing running. The smoke and full trees are kept; `$SM/nex-gddp-cmip6_indices_i2_20261027` is still kept.

### RESPONSE — cglabs 2026-11-02 (3) — J5: smoke publisher dry-run, G1 FAIL on size as expected; G6b not run (`--no-exposure-gate` as written)

Stamp `20261102_062845`. Command as written, plus `ATLAS_WORKING_DIR=$SM` (`logs/obs_s3_pub_dry_<stamp>.log`):
```
G6 basis for prod_t: none supplied — G6 falls back to the live product, and G6b cannot run
--- prod_t / SEVERE ---
G1 FAIL: haz-freq-exp_prod_t_chirps-chirts-agera5_int_adm_severe.parquet is only 0.4 MB
complete in 0.2 min [DRY RUN - nothing written]
```
- G1 fails on size, as expected.
- G1's `next` skips G2-G4, so the `G3 scenarios = observed` line is never printed. The parquet's `scenario` column is
  `observed` (read directly in J4).
- The block expected a G6b FAIL for KEN maize/cassava here, but the J5 command carries `--no-exposure-gate`, so G6b
  did not run. That pair is covered in J4 (0.976 / 0.948) and in the J6 G6b list below (both soil-hole).

### MACBOOK 2026-11-02 (2) — G6b hole is the shared soil mask, identical in the live NEX product; GO J5-J6 with the G6b list reported, not stopped on

**You were right to stop, and the cause is pre-existing.** I checked the live NEX `hazard_exposure` (`prod_t`,
jagermeyr, moderate, historic 1995-2014; NaN-safe sums over KEN admin2). Within that product, `any + none` for the
soil-based sets against the soil-free PTOT set:

| crop | NDWS+NTx35+NDWL0 | PTOT-L+NTxM+PTOT-G | ratio |
|---|---|---|---|
| maize | 10,384,119 | 10,643,430 | **0.9756** |
| cassava | 2,280,324 | 2,404,173 | **0.9485** |

These are your observed ratios to four digits. The 18 Kenya cells where `sscp_world.tif`/`ssat_world.tif` are NA drop
out of every NDWS/NDWL0 product, NEX included. NEX passes G6b only because the gate takes the best hazard set per
pair (PTOT reproduces the exposure). The observed run has no soil-free set, so the hole shows. **This is not an
observed-pipeline defect, and V1 keeps NEX's soil layers on purpose**, so that NEX and observed stay comparable. A soil fill
changes NEX values too and belongs with water-balance v2 (#45). I'll raise it as its own issue.

**Working dir:** agreed and thanks for the careful clean-up. J4's command needs `ATLAS_WORKING_DIR=$SM`. J6 sets
**`ATLAS_WORKING_DIR=$OR` explicitly** (don't rely on hosts.json).

**GO J5 and J6.** G6b is **report, not stop**, for this run only, because J5/J6 publish nothing:
- J5 (smoke dry-run): expect G6b FAIL for KEN maize 0.976 and cassava 0.948 with exactly this cause; anything else
  failing is a stop.
- J6 (full Africa dry-run): report the **full G6b failure list** (iso3, crop, unit, ratio), plus for each pair the
  share of its exposure on cells where `sscp` or `ssat` is NA (your Kenya method). Classify each row **soil-hole**
  (that share ≥ 80 % of the gap) or **other**. Any **other** row is a stop. Do not pass `--allow-exposure-mismatch`.
  I'll turn the soil-hole rows into named residuals before any publish decision.
- Every other stop rule is unchanged. Then K3.

### RESPONSE — cglabs 2026-11-02 (2) — J4 ran (after a working-dir fix) and §4.1 `input files = 159`, parquets + sidecar OK; **STOP at G6b: KEN maize 0.976 and cassava 0.948 (tol 0.02), cattle-tropical 0.982 passes**. Cause: a soil-mask coverage hole (18 Kenya cells with NA `sscp`/`ssat`, so NDWS/NDWL0 NA, so bundle NA); J5/J6 not started

Pull `b7cae04`. Stamp `20261102_055250`. Env grep empty at launch.

**J4 as written failed at once, on the wrong root.**
- The first launch, verbatim (`ATLAS_CLIMDAT_SOURCE=obs R3_MODEL_ALLOW=… R3_TIMEFRAMES=ggcmi-crop EXPOSURE_RES=0.25`), died with:
  `Error: IOError: Cannot list directory 'Data/boundaries/atlas_gaul24_a0_africa.parquet'`.
- `metadata/hosts.json` resolves the obs `working_dir` to **`{common_data}/atlas_obs_hazards` (the full root)**, not
  `$SM`. Setup therefore ran in `$OR` and created ~30 empty `Data/<subdir>` folders there, including real (non-link)
  `Data/mapspam`, `Data/GLW4_2020` and `Data/boundaries`.
- Those three would have made J6's `s1_ingest.py link` skip its symlinks (`already present`). I removed only the
  dirs created by that launch, and only empty ones (0 files): `find $OR/Data -newer <pid> -type d -empty -delete`.
  `$OR/Data` holds only `obs_fpar` again.
- Relaunched with the resolver's documented override `ATLAS_WORKING_DIR=$SM` added. Everything else is as written.
  **J6 needs the same `ATLAS_WORKING_DIR=$OR`** (or rely on hosts.json there), and the J4 text should carry
  `ATLAS_WORKING_DIR=$SM`.

**J4 run** (`logs/obs_s3_r3_smoke_<stamp>.log`, 05:52 → 06:04, `3_freq_x_exposure.R COMPLETE`):
- §4.1 `input files = 159` ✓.
- 5 variables × 159. The skips are the expected crop/livestock split: `harv-area_ha` / `prod_t`: 60 livestock files
  skipped. `head_n`: 99 crop files skipped.
- Parquets under `$SM/Data/hazard_risk_{vop,vop_usd,ha,prod,n}/ggcmi-crop/`:

  | folder | parquets | tokens |
  |---|---|---|
  | vop | 6 | both |
  | vop_usd | 6 | both |
  | n | 6 | both |
  | ha | 3 | `chirps-chirts-agera5` only (crops carry no `-rh15` bundle) |
  | prod | 3 | `chirps-chirts-agera5` only |

  File names are `haz-freq-exp_<var>_<token>_int_adm_<sev>.parquet`. Columns include `scenario = observed`,
  `timeframe = 2014-2016`, `model = <token>`, `hazard_vars ∈ {NDWS+NTx35+NDWL0, NDWS+THI-max+NDWL0}`.
- Sidecar `.parquet.json`: `ensemble.n_members = [0]`; `ensemble.provenance.source_tokens` = both tokens, with
  inputs/period/seasons/severity/caveats text.

**G6b (Kenya)** on the gate's own basis (`r3_tier_drift_vs_live.R` `tier_vs_exposure`: admin2 rows summed per iso3,
`tech == all`). Exposure: SPAM `spam_prod_t_all_res-25_adm_sum.parquet` for crops; GLW4
`livestock_number_number_res-25_adm_sum.parquet` × `head_n` for cattle (no prod_t for livestock).

| crop | severity | Σ7 classes − any | any + none | exposure | ratio |
|---|---|---|---|---|---|
| maize | mod / sev / ext | +8 / −3 / −2 | 3,461,2xx | 3,547,721 | **0.9756** |
| cassava | mod / sev / ext | +4 / −1 / −3 | 760,1xx | 801,382 | **0.9485** |
| cattle-tropical (`head_n`), both tokens | all | ±9 | 13,108,2xx | 13,342,814 | 0.9824 |

- The 7 classes sum to `any` exactly up to integer rounding in every case ✓. `any + none` is constant across severities ✓.
- The `any + none` = exposure identity holds for cattle within 2%. It **fails for maize and cassava**.

**Cause: a hazard-coverage hole, not the multiply.**
- On the 0.25° grid, maize exposure on Kenya cells where the maize bundle is NA is 113,266 t (3.05%, 18 cells).
  Cassava: 51,290 t (5.91%, 15 cells).
- At those 18 cells all climate inputs are present (pr, tasmax, tasmin, rsds, hurs, NTx35: 0 NA). **`sscp_world.tif`
  and `ssat_world.tif` are NA at all 18**, so NDWS and NDWL0 are NA and the bundle is NA.
- Largest cells: 35.625 E 0.625 N (53.7 kt maize, inland: Kerio valley / Lake Baringo margin), 33.875 E 0.125 N (Lake
  Victoria shore), 40.875 E −2.375 N and 39.875 E −3.6/−3.9 N (coast).
- The soil layers are the shared NEX inputs (linked from `{common_data}/atlas_hazards/soils`), so the NEX product
  probably carries the same hole. I did not check NEX G6b for KEN maize/cassava.
- Options for macbook:
  - a named G6b residual, the `metadata/g6b_expected_residuals.csv` mechanism (TZA coconut precedent);
  - a soil-mask fill, e.g. nearest-neighbour `sscp`/`ssat` onto land cells that have exposure;
  - or accept the hole as documented coverage.
- Not decided here.

**J5, J6 not started** (stop rule: G6b failing on the smoke for a test crop). K3 waits for J6.

### MACBOOK 2026-11-02 — 159 is correct (162 was my miscount); GO J4-J6; K3 after J6

**159 is right; 162 was my miscount.** I counted `generic-crop` as a 34th crop. The obs pipeline deliberately has no
`generic-crop` bundle (the selftest asserts it), and adding one would be **wrong**: in R/3 §4 a `generic-crop`
bundle is multiplied by *every* crop's exposure and written under each crop's own name
(`gsub("generic-crop", crop_choice, save_name)`, `R/3_freq_x_exposure.R` ~l.230-247). Here every crop already
has its own windowed bundle, so a generic one would write maize-calendar hazards over the per-crop outputs. So:
**33 crops × 3 + 10 livestock × 3 × 2 tokens = 159**. Wherever the J4/J5 text says 162 (§4.1 `input files`, bundle
count), read **159**. The "maize calendar NA on N land cells" line is the same check as your
`window ggcmi-mai: … NA on land: 0`; that is accepted as written. A "generic-crop" all-crops total for the observed
product, if wanted, is a post-hoc sum of the per-crop exposures, not a bundle. I've logged that as a follow-up, not V1.

§3b share 0.844 at 1 (< 0.9) and 0 land cells without a season: clean. **FPAR K0-K2:** 762/762 byte-identical to
PASCAL across two nodes, and 1.9 h here against 29.5 h there (JRC serves this node ~15× faster). Noted.

**GO J4, J5, then J6** as written (with 159), then **K3** (`DISPATCH_cglabs_obs_fpar.md`) once J6's annual12
THI/NDWL0 stacks exist. Same stop rules. One `### RESPONSE` for J4-J5, J6 its own, K3 in the FPAR dispatch.

**Background completion:** a `wait` on a `setsid`/`nohup` child never returns in the shell that launched it, so the
"done" notice never fires. Use a pid-file loop (`while kill -0 $(cat x.pid) 2>/dev/null; do sleep 60; done`) or
check the log tail, so a finished step is not missed for two days again.

### RESPONSE — cglabs 2026-11-02 — selftest 24/24; J3 ran clean on both commands but **STOPPED at the bundle-count gate: 159, not 162** (no `generic-crop` in the obs window CSV; the selftest asserts exactly that); J4-J6 not started

Pull `b645c96`. Stamp `20261031_074742`. The env grep was empty before each launch.

**Selftest:** `logs/obs_s3_selftest2_<stamp>.log`, 24 `ok`, `ALL S3-S5 SELFTEST ASSERTIONS PASSED`. The last `ok` is
`shipped metadata/obs_crop_windows.csv parses with its comment line: 43 crops, maize = ggcmi, chickpea custom`.

**J3, both commands as written** (`logs/obs_s3_s35{,_rh15}_<stamp>.log`): no error, no WARN.
- Command 1: `done: 129 bundles` in 7.2 min.
- Command 2 (`--thi-token …-rh15 --sets livestock`): `done: 30 bundles` in 2.5 min.
- GGCMI dir: 20 `*_rf_*.nc4`.

**Gate: `ls $SM/Data/hazard_risk/ggcmi-crop/*_int.tif | wc -l` = 159, expected 162. Stop.**
- 159 = 33 crops × 3 + 10 livestock × 3 × 2 tokens. Every crop/token group has exactly 3 severities (53 groups × 3).
- The shipped `metadata/obs_crop_windows.csv` has **33 crop rows + 10 livestock = 43**. The NEX `jagermeyr` bundles
  (`{NWD}/Data/hazard_risk/jagermeyr/*_ACCESS-CM2_severe_*_int.tif`) have 34 + 10. The one difference is `generic-crop`.
- The obs selftest asserts `"S5: per-crop bundles exist for every crop x severity (no generic-crop)"`, so 159 looks
  like the design and 162 like the dispatch's arithmetic. **But R/3 handles `generic-crop` specially**
  (`R/3_freq_x_exposure.R:176,189`: the `generic` exposure is the sum of all crop layers).
- Decide: either (a) 159 is right, so the J4 §4.1 invariant becomes `input files = 159`, or (b) obs needs a
  `generic-crop` bundle for R/3, so add a window row and rerun J3 command 1.
- Not decided here.

**Other J3 items (read-only, for when it resumes):**
- **Per-window lines** (identical in both logs): `NA on land: 0` in every window.

  | window | months | cells < 3 months | cells = 12 |
  |---|---|---|---|
  | annual12 | 12..12 | 0 | 165,024 |
  | ggcmi-mai | 2..10 | 156 | 0 |
  | ggcmi-bar | 1..12 | 1,780 | 2,304 |
  | ggcmi-pot | 1..12 | 312 | 140 |
  | ggcmi-rap | 1..12 | 8 | 1,164 |
  | ggcmi-swh | 1..12 | 16 | 508 |
  | ggcmi-cot | 4..12 | 0 | 672 |
  | ggcmi-mil | 1..9 | 72 | 0 |
  | ggcmi-sor | 1..11 | 56 | 0 |
  | ggcmi-sun | 2..9 | 88 | 0 |
  | ggcmi-nut | 1..11 | 4 | 0 |

  `bea`, `cas`, `pea`, `ri1`, `sgb`, `soy` and both custom windows (`mai_maturity-4`, `mai_plant-8`) have 0 and 0.
- **`maize calendar NA on N land cells`:** no such line is printed. The script has no such log statement; the
  equivalent is `window ggcmi-mai: … NA on land: 0`.
- **Bundle check:** `maize_chirps-chirts-agera5_severe_NDWS+NTx35+NDWL0_int.tif` has 9 layers named
  `observed_chirps-chirts-agera5_2014-2016_<class>_NDWS+NTx35+NDWL0_maize_severe`, with classes any, none, dry, heat, wet,
  dry+heat, dry+wet, heat+wet, dry+heat+wet.
- **§3b diagnostic:** Kenya-box land cells, 10 livestock severe bundles per token (13,440 cell-bundles). The severe dry
  marginal is exactly 1 in **0.844** of them and exactly 0 in 0.083, for both tokens. That is below the 0.9 stop but
  high: an arid box plus the 12-month NDWS mean.

**J4, J5, J6: not started.** The J2 tree and the renamed `nex-gddp-cmip6_indices_i2_20261027` are kept.

FPAR K0-K2 are reported in `DISPATCH_cglabs_obs_fpar.md`: the ingest is done and is identical to PASCAL's, 762/762.

### MACBOOK 2026-10-30 (2) — J3 fix landed (comment-safe window loader + shipped-CSV selftest); pull and resume J3-J6

Right diagnosis, and the right place to stop. `R/obs/s3_s5_obs_hazards.R` now reads `metadata/obs_crop_windows.csv`
through `load_windows()`: `#` lines stripped before `fread`, then shape checks (five columns, window ∈
ggcmi|annual12|custom, custom codes `<mai_plant|mai_maturity>:<months>`, ggcmi rows carry a code, no `_` in crop
names). The selftest now parses the **shipped** file (24 assertions, was 23) — the fixture-only CSV is exactly the
"declaration is not an output" gap the J3 stop exposed. Also guarded `spinup = 0` (`x[-integer(0)]` is empty in R;
same bug S6 had). No change to any output of S2; the J2 tree stays.

Your J2 handling (stale I2 smoke tree moved aside, not deleted; one consistent 2013-2016 pass) was correct — keep the
renamed dir until J6 is clean. The v1.0 comparison is recorded: ERA5 Tmin 4-7 °C above v1.0 on the Eritrea/Sudan/Djibouti
coasts, and v1.0 itself ~40-41 °C at Massawa/Djibouti, so the ≤ 43 °C repaired values sit inside v1.0's own range.

```bash
cd $REPO && git pull --ff-only && git log -1 --format='%h %s'
Rscript R/obs/s3_s5_obs_hazards.R --selftest     # ALL S3-S5 SELFTEST ASSERTIONS PASSED (24; the last line names the shipped CSV, 43 crops)
```
Then **J3 as written** (both commands), J4, J5, then J6. Same stop rules. One `### RESPONSE` for J3-J5; J6 its own.

### RESPONSE — cglabs 2026-10-30 — J0 PASS, J1 done, J2 PASS (all invariants); **J3 STOPPED: `R/obs/s3_s5_obs_hazards.R` cannot parse `metadata/obs_crop_windows.csv`** (its `#` comment line becomes the header under `fread`); J4-J6 not started

Pull `ffe6697`. Stamp `20261030_185306`. `env | grep -E 'FORCE_OVERWRITE|DAYS_CHECK|AVAIL_SEED|R3_|ATLAS_CLIMDAT'` was empty before
each launch, and each step's variables were set on its own command line. R 4.5.1, data.table 1.17.8.

**J0 — PASS** (reported in the S1 dispatch RESPONSE 2026-10-29): `fixture_publish_tier_gates.R` PASS (43 ok),
`fixture_upstream_run_params.R` PASS (53 ok), `s3_s5_obs_hazards.R --selftest` PASS (23 ok; the block says 20).

**J1 — done** (details in the S1 dispatch RESPONSE 2026-10-29 (2)):
- Smoke S1 2013-2016: validate PASS, 1461/1461 per var, 0 Tmin/Tmax repair flags in the Kenya box.
- `link`: `Data/mapspam`, `Data/GLW4_2020`, `Data/boundaries` resolve under `$SM`.
- res-25 present:
  - SPAM `2020V1r2_SSA/processed/variable=`: `{prod_t, harv-area_ha, phys-area_ha}` 12 each;
    `vop_nominal-usd-{2015,2020,2021}` 18 each; `vop_intld15-2021` 9.
  - GLW4 `processed/variable=`: `vop_{intld15,nominal-usd}-{2015,2020,2021}` 3 each; `number_number` 2.

**Before J2: stale 2015 S2 outputs moved aside, not deleted.** `$SM/nex-gddp-cmip6_indices/` still held the I2
smoke (2026-10-27, 2015 only: NDWS/AVAIL, NTx20-50, PTOT, THI ×2 tokens).
- Those files were built from AgERA5 before the half-cell COG shift.
- That NDWS was seeded at 2015-01.
- With FORCE_OVERWRITE empty, every S2 script skips existing months. J2 would have kept stale 2015 NDWS/THI and
  carried AVAIL-2015-12 into 2016.

I renamed the tree to `$SM/nex-gddp-cmip6_indices_i2_20261027/` (smoke root only, reversible) so J2 builds
2013-2016 in one consistent pass. PTOT is not read by S3.

**J2 — PASS**, run exactly as written: 5 scripts in parallel, about 25 min wall-clock (NDWL0 slowest).
- **Logs:** `logs/obs_s3_s2_{ndws,ndwl0,ntx,thi,thi_rh15}_<stamp>.log`. 0 `WARN`, 0 `daily inputs missing`, 0 error/halt.
  - NTx and both THI end `----Finish----`.
  - NDWS and NDWL0 end on their `compute: 2016-12` line; they print no finish marker.
  - The processes were `setsid`-detached, so rc is inferred: no error text, and outputs are complete.
- **Counts:**
  - NDWS 48, NDWL0 48 (plus AVAIL).
  - NTx: 31 dirs × 48.
  - THI: 48 `THI_max-` + 48 `THI_mean-` per token (plus the `daily` entry, as in I2).
- **NDWS Kenya-box (33.5-42.5 E, 5 S-5.5 N) land mean by month:**
  ```
        Jan  Feb  Mar  Apr  May  Jun  Jul  Aug  Sep  Oct  Nov  Dec
  2013 29.3 26.0 21.9  9.1 24.6 26.9 28.4 27.5 27.6 28.2 21.0 25.2
  2014 28.9 23.2 23.4 23.1 24.5 26.4 28.6 27.4 26.7 24.8 20.7 25.9
  2015 30.3 26.7 28.5 12.6 20.3 25.2 29.3 29.2 28.8 25.2 13.9 22.5
  2016 26.3 27.1 28.5 15.7 20.1 27.9 28.7 28.9 29.0 29.5 24.6 28.9
  ```
  - Climatology Apr 15.1 and Nov 20.1 against 25.6-28.8 elsewhere: the April and November dips (long and short rains) are there.
  - Months with every land cell = 0: **0**. Months with every land cell = days-in-month: **0**.
- **THI, (ii) RH15 − (i) daily-mean RH, Kenya-box land mean:** `THI_max` 82.11 vs 84.44, so **−2.33**, with
  (ii) < (i) in 48/48 months. `THI_mean` −2.30.

**J3 — STOPPED at launch, both commands** (`logs/obs_s3_s35{,_rh15}_<stamp>.log`). Nothing was written; `$SM/Data/`
has only the three links.
```
[S3-5] obs_root={cd}/atlas_obs_hazards_smoke token=chirps-chirts-agera5 ... seasons kept=2 (2014..2015) timeframe=2014-2016 period=ggcmi-crop crops=43 sets=crop,livestock
Error in window == "ggcmi" : comparison (==) is possible only for atomic and list types
Calls: run -> [ -> [.data.table -> eval -> eval -> fifelse
In fread(windows_csv) : Detected 1 column names but the data has 5 columns (i.e. invalid file). Added 4 extra default column names at the end.
```
- **Cause:** line 2 of `metadata/obs_crop_windows.csv` is a `# window: ggcmi (...) | annual12 (...) | custom (...)` comment.
  - `fread` (1.17.8) takes it as the header line: `names(fread(csv))` is that one comment string, followed by `V2..V5`.
  - So the `crop`/`window`/`ggcmi_code`/`proxy` columns don't exist, and `window == "ggcmi"` fails.
- **Why the selftest missed it:** the selftest passes because it `fwrite`s its own fixture `windows.csv` with no comment line, so the real file is never parsed in a fixture.
- **Fix:** not improvised. Either drop the comment line from the CSV (move it to a `note` row or the header comment of
  the R script), or read with comments stripped (e.g. `fread(cmd = paste("grep -v '^#'", shQuote(windows_csv)))` or
  `read.csv(..., comment.char = "#")`). A selftest assertion that parses the shipped `metadata/obs_crop_windows.csv`
  would catch a recurrence.
- The GGCMI dir check passed: `ls $NWD/Data/ggcmi/*_rf_*.nc4 | wc -l` = 20.

**J4-J6 not started.** After the fix, J3 resumes as written; J2 outputs stay in place.

**Optional (S1 block 2026-10-30): CHIRTS v1.0 Tmin vs CHIRTS-ERA5 native, flagged African days ≤ 2016.** Read-only.
Box = flagged-pixel extent ± 0.1°.

The days you suggested (Massawa 2015-08-11/12, Port Sudan 2012-08-01/02) are not African. Per the localisation log:
- 2015-08-11/12: Saudi coast, 23.6-24.1 N 38.0-38.5 E.
- 2012-08-01: Kuwait. 2012-08-02: Kuwait + Saudi.

I used African flagged days instead:
- 2006-05-27 Massawa (15.03-15.22 N, 40.23-40.48 E): v1.0 Tmin max **41.2** (mean 40.1) | ERA5 native max **45.5**
- 2007-06-27 Port Sudan (21.02-21.62 N, 36.88-37.23 E): v1.0 max **39.1** (mean 35.7) | ERA5 **46.5**
- 2016-07-23 Djibouti (10.68-10.93 N, 43.73-43.98 E): v1.0 max **40.6** (mean 38.9) | ERA5 **44.6**
- (for reference, 2005-09-04 Yanbu: v1.0 35.4 | ERA5 45.1)

ERA5 is 4-7 °C above v1.0 on African coasts (10 °C at Yanbu). v1.0 itself reads ~40 °C at Massawa and Djibouti, so after
repair the ≤ 43 °C values are in v1.0's range there.

Nothing is running.

### MACBOOK 2026-10-27 — runs AFTER `DISPATCH_cglabs_obs_s1_ingest.md` I0-I3 are green. Smoke on the Kenya box, then the full Africa chain.

**What landed since the S1 dispatch (`git pull`):** `R/obs/s3_s5_obs_hazards.R` (per-crop windows → per-season
codes → R/3 bundles; selftest 20/20), `metadata/obs_crop_windows.csv`, and the R/3 + publisher per-source
plumbing (`69e0c55`): `ATLAS_CLIMDAT_SOURCE=obs`, `R3_MODEL_ALLOW`, `--source`. All 14 fixtures pass.

**Rules.** Writes only under the two obs roots. Never under the NEX trees. Background everything with logs
in `logs/obs_s3_<step>_<STAMP>.log`. `env | grep -E 'FORCE_OVERWRITE|DAYS_CHECK|AVAIL_SEED|R3_|ATLAS_CLIMDAT'`
must be empty before each launch (set what each step needs on its own command line). Public repo: no IPs,
hostnames, mount identifiers or usernames.

`SM={common_data}/atlas_obs_hazards_smoke`, `OR={common_data}/atlas_obs_hazards`, `PY` = the obs env python,
`REPO` = this checkout, `NWD={common_data}/nex-gddp-cimp6_hazards` (the NEX working dir).

#### J0 — fixtures on the node
```bash
cd $REPO && git pull --ff-only && git log -1 --format='%h %s'
for f in R/checks/fixture_publish_tier_gates.R R/checks/fixture_upstream_run_params.R; do Rscript $f > logs/obs_s3_fx_$(basename $f .R)_$STAMP.log 2>&1 && echo PASS $f || echo FAIL $f; done
Rscript R/obs/s3_s5_obs_hazards.R --selftest > logs/obs_s3_selftest_$STAMP.log 2>&1 && echo PASS selftest
```
**Stop** on any FAIL.

#### J1 — smoke S1: extend the Kenya box to 2013-2016 (S3 needs ≥ 3 data years; 2015 is already there and is skipped)
```bash
nohup $PY python/obs/s1_ingest.py ingest --smoke --out-root $SM --years 2013:2016 --rh both > logs/obs_s3_s1smoke_$STAMP.log 2>&1 &
$PY python/obs/s1_ingest.py validate --smoke --out-root $SM --years 2013:2016            # PASS, 1461 days per var
$PY python/obs/s1_ingest.py link --out-root $SM --common-data {common_data} --nex-working-dir $NWD
```
Report: `Data/mapspam`, `Data/GLW4_2020`, `Data/boundaries` now resolve under `$SM`; which `glw4-2020_vop_*_res-25.tif`
and `spam_*_res-25.tif` files exist there (R/3 reads the `res-25` tag because the base raster is 0.25°).

#### J2 — smoke S2 (unchanged upstream scripts), strict, seeded at the first year
```bash
cd $REPO/hazards_upstream/R/04_indices
export COMMON_DATA=$SM SCENARIO=historical YRS=2013:2016 DAYS_CHECK=strict AVAIL_SEED_MONTHS=2013-01
GCMS=chirps-chirts-agera5      nohup Rscript fast_calc_NDWS.R  > $REPO/logs/obs_s3_s2_ndws_$STAMP.log 2>&1 &
GCMS=chirps-chirts-agera5      nohup Rscript fast_calc_NDWL0.R > $REPO/logs/obs_s3_s2_ndwl0_$STAMP.log 2>&1 &
GCMS=chirps-chirts-agera5      nohup Rscript calc_NTx.R        > $REPO/logs/obs_s3_s2_ntx_$STAMP.log 2>&1 &
GCMS=chirps-chirts-agera5      nohup Rscript calc_THI.R        > $REPO/logs/obs_s3_s2_thi_$STAMP.log 2>&1 &     # variant (i): hurs = mean of 5 local times
GCMS=chirps-chirts-agera5-rh15 nohup Rscript calc_THI.R        > $REPO/logs/obs_s3_s2_thi_rh15_$STAMP.log 2>&1 &  # variant (ii)
unset COMMON_DATA SCENARIO YRS DAYS_CHECK AVAIL_SEED_MONTHS
```
**Invariants:** every script rc 0; 48 monthly files per index (NTx: 31 dirs × 48; THI: 48 `THI_max-` + 48 `THI_mean-`);
no `WARN` / `daily inputs missing`; Kenya-box NDWS monthly means vary seasonally with PTOT (April and November dips) and no month is 0 or 31 in every land cell (the 2-28 band in the S1 dispatch was too tight for an arid box; an un-converted radiation unit would make every day a stress day); the two THI tokens differ
(RH15 < daily-mean RH, so THI(ii) < THI(i) on average: report the box-mean difference).

#### J3 — smoke S3-S5
```bash
cd $REPO
nohup Rscript R/obs/s3_s5_obs_hazards.R --obs-root $SM --token chirps-chirts-agera5 --years 2013:2016 --spinup 1 \
      --ggcmi-dir $NWD/Data/ggcmi > logs/obs_s3_s35_$STAMP.log 2>&1 &
nohup Rscript R/obs/s3_s5_obs_hazards.R --obs-root $SM --token chirps-chirts-agera5 --thi-token chirps-chirts-agera5-rh15 \
      --years 2013:2016 --spinup 1 --sets livestock --ggcmi-dir $NWD/Data/ggcmi > logs/obs_s3_s35_rh15_$STAMP.log 2>&1 &
```
(Confirm the GGCMI dir: `ls $NWD/Data/ggcmi/*_rf_*.nc4 | wc -l` should be 20.) Report the per-window
`months …; cells <3 months; =12; NA on land` lines, the `maize calendar NA on N land cells` line, and:
- `ls $SM/Data/hazard_risk/ggcmi-crop/*_int.tif | wc -l` — expect 34 crops × 3 + 10 livestock × 3 × 2 tokens = **162**;
- one bundle opened with terra: 9 layers, names of the form `observed_<token>_2014-2016_<class>_<set>_<crop>_<sev>`;
- the §3b check: over Kenya-box land cells, the share of `annual12` cells (livestock bundles) whose severe `dry`
  marginal (dry + dry+heat + dry+wet + dry+heat+wet) is exactly 0 or exactly 1 — report the number, it is a
  diagnostic, not a gate.

#### J4 — smoke R/3 §4 (the observed bundles through the unchanged exposure multiply + zonal sum)
```bash
cd $REPO
ATLAS_CLIMDAT_SOURCE=obs R3_MODEL_ALLOW='chirps-chirts-agera5' R3_TIMEFRAMES=ggcmi-crop EXPOSURE_RES=0.25 \
  nohup Rscript -e 'source("/abs/path/to/repo/R/0_server_setup.R"); source("/abs/path/to/repo/R/3_freq_x_exposure.R")' \
  > logs/obs_s3_r3_smoke_$STAMP.log 2>&1 &
```
(`R3_MODEL_ALLOW='chirps-chirts-agera5'` also matches `…-rh15`; both tokens flow.) Setup will create empty
`Data/<subdir>` folders under `$SM` — that is expected. **Invariants:**
- §4.1 logs `input files = 162` (not 0: zero means the allow-list did not take);
- parquets under `$SM/Data/hazard_risk_{vop,vop_usd,ha,prod,n}/ggcmi-crop/`, model tokens `chirps-chirts-agera5`
  and `…-rh15` in the file names, `scenario = observed`, `timeframe = 2014-2016`;
- the sidecar JSON has `ensemble.n_members = 0` and an `ensemble.provenance` block listing both tokens;
- for Kenya (admin0 row): per crop and `hazard_vars`, the seven classes sum to `any` and `any + none` equals
  the exposure denominator's `prod_t` for that crop within rounding (the G6b identity) — compute it for maize,
  cassava, cattle-tropical.

#### J5 — publisher dry-run on the smoke (gates only; nothing uploaded)
```bash
ATLAS_CLIMDAT_SOURCE=obs Rscript scripts/r3_publish_tiers.R --dry-run --source chirps-chirts-agera5 --timeframe ggcmi-crop \
   --model chirps-chirts-agera5 --variables prod_t --tiers severe --no-exposure-gate > logs/obs_s3_pub_dry_$STAMP.log 2>&1
```
Expect G1 to **FAIL on size** (a Kenya-box parquet is far below 10 MB) — report that it fails for that reason
and that G3 reads `scenarios = observed`. That is the smoke's job; the full run is gated for real.

#### J6 — the full Africa chain (after J0-J5 are clean)
Same commands with `$OR`, `--years 2005:2025`, `AVAIL_SEED_MONTHS=2005-01`, no `--smoke`, `--spinup 1`,
two S2 waves (NDWS must run months in order; NDWL0 / NTx / THI × 2 tokens in parallel), then S3-S5, then R/3.
Then the publisher **dry-run with the exposure gate on**:
```bash
ATLAS_CLIMDAT_SOURCE=obs Rscript scripts/r3_publish_tiers.R --dry-run --source chirps-chirts-agera5 --timeframe ggcmi-crop \
   --model chirps-chirts-agera5 --variables prod_t,ha,head_n,vop_usd,vop_intld --tiers severe,moderate,extreme \
   --drift-exposure prod_t=<0.4.4 reference res-25 table>,... > logs/obs_s3_pub_dry_full_$STAMP.log 2>&1
```
Report G1-G4 and G6b per tier. **Do not publish.** Publishing is a separate GO.

**Stop and describe** on: any FAIL in J0; S2 rc ≠ 0 or any missing-day line; a bundle count ≠ 162 on the smoke; R/3 §4.1
`input files = 0`; the G6b identity failing on the smoke for any of the three test crops; a §3b share that is
≥ 0.9 at 1 (report it even if everything else passes).

#### Then
Prepend `### RESPONSE` (J0-J5, and J6 when done), commit only this file, push, verify on `origin/develop`.
