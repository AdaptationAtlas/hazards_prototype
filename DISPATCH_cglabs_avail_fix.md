🧰 **MACBOOK (2026-07-07 #5, commit `230b66e`): GATE-2 plumbing PRE-BUILT (during your 12h run) — ready when final QAQC greens. p.steward steer: full-refresh both historic+future = most robust (avoids a currency seam at the historic/future boundary; VoP is scenario-invariant so the rebuild's I$ is correct for all scenarios). Two new files, UNTESTED — validate post-R/3.**
>
> - **`R/s3_upload.R`:** added the intld `.parquet` uploader (mirrors USD block) — the vop_intld15 `interaction.parquet` (THE live product) now ships from here (was `.tif`-only).
> - **`R/derive_historic_model_parquet.R` (NEW):** extracts `scenario=="historic"` rows from each `*_ENSEMBLE_int_adm_*` parquet → `model="historic"` → `*_historic_int_adm_*` so it routes to `model=historic/` (s3_upload keys model off the filename token; no scenario→model remap). Supersedes the 2025-06-25 historic product with new const-I$ values.
>
> **Post-R/3 publish sequence (after final QAQC livestock~1 AND crop~1):**
> ```bash
> git pull   # head 230b66e
> Rscript R/derive_historic_model_parquet.R      # build model=historic parquets (verify row counts + spot value)
> # publish (parquet + tif, intld + usd, both timeframes):
> Rscript R/s3_upload.R annual    TRUE TRUE FALSE FALSE TRUE TRUE 10
> Rscript R/s3_upload.R jagermeyr TRUE TRUE FALSE FALSE TRUE TRUE 10
> # CR-068 probes on AGO
> ```
> **⚠️ One structural check for you post-R/3:** the combined ENSEMBLE parquet holds ALL scenarios (incl historic). Publishing it as `model=ENSEMBLE` + the derived `model=historic` means historic exists in BOTH partitions. Confirm the notebook reads historic from `model=historic` (harmless dup) and doesn't sum across `model=ENSEMBLE` historic rows (double-count). If it double-counts, we strip historic from the ENSEMBLE file before upload — flag it. **Still HOLDING; nothing published.**

---

> 👍 **MACBOOK (2026-07-07 #4): crop gate green (0.99) — proceed, no halt. Outliers accepted: N.Africa 0.00 = outside SSA extent (legit), islands Inf = FAO denom 0 (cosmetic), core-SSA incl NAM 0.52 = normal SPAM-vs-FAO per-country variance (low-crop country, tiny absolute) — within tolerance for a median-0.99 product. Don't chase NAM. Let the 12h finish; report final QAQC. I'll pre-build GATE-2 plumbing (derive model=historic + intld parquet uploader) during the run so publish is ready. Nothing published.**

---

🟢🟢 **CGLABS (2026-07-08): GATE 0 FULLY GREEN. Final chain (0.4.4→R/3→QAQC) clean. Both currencies FAOStat-I$-aligned (livestock 1.00, crop 0.99). NDWS de-saturated + multi-scenario intact + const-I$ validated. Ready for GATE 2 publish plumbing (macbook). Nothing published.**
>
> **Final QAQC (post crop-align): LIVESTOCK median 1.00 (242/242) | CROP median 0.99 (36/50)** — both I$-aligned to FAOStat. R/3 clean, both axes.
> **Exposure state verified:**
> - Combined ENSEMBLE parquets fresh (this chain), all 5 scenarios present (historic + 4 ssp) — multi-scenario stacks intact (guard held).
> - AGO cattle-highland NDWS historic `dry` = 11.27M = validated **const-I$** (AGO cattle grid 525.69M = FAOStat 525.69M exactly). The ~6.9× vs stale-live is the legit full-refresh (live used wrong nominal/predecessor basis).
> - NaN = crop-mask, NOT a regression: livestock combo (NDWS+THI, 10 crops) 5% NaN; crop combos (NDWS+NTx35, 23 crops) 41% ≈ PTOT-crop 38% (consistent — crops absent in many admin2 cells → NaN exposure). CR-068 NaN→0 is a product-wide question, orthogonal to Track-1.
> - Upstream de-sat holds: NDWS 29→21 days, `_int` 153-layer multi-scenario stacks.
>
> **✅ Everything data-side is now correct + validated.** The whole VOP saga resolved: density→count (head-counts), currency mislabel (livestock const-I$), crop-align (0.4.0 const-I$). QAQC anchors both to FAOStat.
>
> **NEXT → GATE 2 (macbook build, per your earlier plan):**
> 1. **Derive `model=historic`**: extract `scenario=="historic"` rows from the combined ENSEMBLE parquet → filename token `historic` → routes to `model=historic/interaction.parquet` (supersedes 2025-06-25). Keep `value_sd` + `none` (full-refresh schema).
> 2. **intld parquet uploader**: add `.parquet` branch to `s3_upload.R` INTLD block (mirror USD block L119-141) — currently tif-only.
> Ship both, I run the publish → CR-068 on AGO. Latent USD-side (livestock nominal vs crop usd2015) stays GATE-2 cleanup. **HOLDING; nothing published.**
>
> ---
>
> 🟢 **CGLABS (2026-07-07 #3): 0.4.0 resample fix WORKS — crop gate PASSED (median 1.21→0.99). Both currencies now FAOStat-I$-aligned. Gate met → running final 0.4.4 + R/3 + QAQC (~12h). Outliers all benign/out-of-scope. Nothing published.**
>
> **Crop QAQC after `d9a4192` (SPAM→base resample method=sum):**
> - **LIVESTOCK: median 1.00 | 242/242** ✓ (unchanged)
> - **CROP: median 0.99 | 36/50 within 0.9–1.1** (was 1.21 | 10/50) ✓ — crop VOP now genuine FAOStat const-I$, method-aligned with livestock.
> - `spam_vop_intld15-2021_all.tif` produced by 0.4.0. R/3 repointed to it (`ac0acab`).
>
> **17 crop outliers — all explainable, none core-SSA errors:**
> - `DZA/EGY/LBY/MAR/TUN = 0.00` → North Africa, outside the SSA MapSPAM extent (product is `region=ssa`) — grid legitimately 0.
> - `COM/CPV/MUS = Inf` → tiny islands, FAOStat GPV zero/gap (fao denom 0).
> - `NAM 0.52, ETH 0.80, GIN/BWA/SLE/SEN 0.84–0.90, RWA/SOM/BDI 1.13–1.21` → core SSA, within normal SPAM-allocation-vs-FAO-national per-country variance. NAM (0.52) the lowest — flag if you want it chased, else within tolerance for a median-0.99 product.
>
> **Gate criterion (crop median ~1) MET → proceeding per your sequence:** `FORCE 0.4.4 → FORCE R/3 → final QAQC` now running (~12h). Will report final QAQC (expect livestock ~1 AND crop ~1) + confirm cattle-highland exposure at correct const-I$ scale. If you object to any outlier (e.g. NAM), R/3 output is local/reversible — flag and I halt before publish. **After final QAQC green → GATE 2 publish plumbing (derive model=historic + intld parquet uploader) → publish → CR-068.**
>
> ---
>
> ✅ **MACBOOK (2026-07-07 #3, commit `d9a4192`): 0.4.0 resample fix applied — great catch, the gate did its job. Re-run 0.4.0 + crop QAQC.**

Your diagnosis exact. Fix (mirrors 0.4.1 L144): each SPAM prod_t stack now `resample(dat, base_rast, method="sum")` (mass-conserving, + a conservation check) inside the `spam_dat` load, before the admin zonal + `spam_prop` division. Coffee + irr/rf splits then run on the aligned 0.25° grid.

**Re-run (same hard gate):**
```bash
git pull   # head d9a4192
FORCE_OVERWRITE=1 Rscript R/0.4.0_create_crop_vop_intld15.R
Rscript R/qaqc_vop_vs_faostat.R        # crop ratio should now -> ~1 (was 1.21)
# --- GATE: only if crop median ~1 (0.9-1.1) ---
FORCE_OVERWRITE=1 Rscript R/0.4.4_process_exposure.R
FORCE_OVERWRITE=1 Rscript R/3_freq_x_exposure.R
Rscript R/qaqc_vop_vs_faostat.R        # final: livestock ~1 AND crop ~1
```
Watch for the next possible grid snag: `final_vop_i_rast` is built from `admin_rast` (already base grid) so that's fine; the coffee/irr/rf `spam_dat$all`/`$irr` refs are now base-grid too. If anything else mismatches, report — I'll fix before you spend the R/3 cycle. Livestock stays GREEN (1.00). **HOLDING; nothing published.**

---

> 🛑 **CGLABS (2026-07-07 #2): EARLY GATE caught it — `0.4.0` errors `[zonal] extents do not match`. SPAM prod_t (0.05°) is never resampled to base_rast (0.25°) before the admin zonal. 1-line-ish fix (mirror 0.4.1's mass-conserving resample). Gate held — did NOT spend the 12h R/3. NOTHING PUBLISHED.**
>
> Ran `FORCE_OVERWRITE=1 0.4.0` alone (per your hard gate). It loaded geoboundaries + SPAM (6 prod_t files, techs all/irr/rf-*), then halted:
> ```
> Error: [zonal] extents do not match
> ```
> **Grids (measured):**
> - SPAM prod_t : res **0.05°**, ext -25.3/57.8,-47/40 (SSA), dim 1739×1663
> - base_rast   : res **0.25°**, ext -180/180,-50/50 (global), dim 400×1440
> **Root cause:** `0.4.0` L44-45 rasterizes admin0 onto **base_rast** (`admin_rast <- rasterize(geoboundaries, base_rast, field="iso3")`) but the SPAM prod_t is used at its **native 0.05°** in the national-total zonal → grids differ → `[zonal] extents do not match`.
> **Fix (mirror 0.4.1, which works):** 0.4.1 does `glw <- terra::resample(glw, base_rast, method="sum")` (L144, mass-conserving) BEFORE any zonal. 0.4.0 needs the same for SPAM: **`resample(spam_prod_t, base_rast, method="sum")` before the admin0 national-total zonal** (method="sum" to conserve production mass, like 0.4.1). Then `spam_prop = pixel_prod / national_prod` on the aligned grid. Reinstatement just missed the SPAM→base resample step.
>
> **cglabs HOLDING for the 0.4.0 resample fix** — then I re-run 0.4.0 + crop QAQC (expect crop ratio →~1), and only if green proceed to 0.4.4 + the 12h R/3 + final QAQC. Livestock stays GREEN (1.00). Nothing published.
>
> ---
>
> ✅🔧 **MACBOOK (2026-07-07, commit `ac0acab`): livestock GREEN confirmed 🎉. Crop-align DONE — reinstated `0.4.0` (const-I$ ÷ production share) + repointed R/3 + fixed QAQC (countrycode). ⚠️ 0.4.0 is UNTESTED on real data — run it ALONE + crop QAQC as an EARLY GATE before the 12h R/3.**

Livestock 1.00 / 242/242 = the currency fix nailed it — great confirmation. Now crop:

**Reinstated `R/0.4.0_create_crop_vop_intld15.R`** (from `92cb0b0`) — distributes FAOStat GPV **const-I$** by SPAM production share (crop analogue of 0.4.1), writes `variable=vop_intld15-2021/spam_vop_intld15-2021_all.tif` (+ irr/rf). **Modernized** (the 2025 original wouldn't run/align): `base_rast_path` not atlas_delta (so crop grid = livestock grid = R/3 stack); **FAO GPV = median(2019:2023) ×1000** to match 0.4.1 + the QAQC (original used mean 2020-22 → wouldn't validate to 1); + logging/FORCE gate; dropped a stray undefined-var line, wrong boundary field, interactive `plot()`s.

**R/3 repointed:** crop_vop_file → `grep("vop_intld15-2021_all")` (0.4.0 output), hard-stops if absent. Supersedes the S3-legacy `spam_vop_intld15_all` (the 1.21). QAQC now checks the 0.4.0 file (+countrycode lib fix).

**⚠️ HARD GATE — 0.4.0 is untested (I can't run real data locally). Do NOT go straight to the 12h R/3. Sequence:**
```bash
git pull   # head ac0acab
FORCE_OVERWRITE=1 Rscript R/0.4.0_create_crop_vop_intld15.R    # crop const-I$ VoP only (fast-ish)
Rscript R/qaqc_vop_vs_faostat.R                                # crop ratio should now -> ~1 (was 1.21)
# --- GATE: only if crop median ratio ~1 (0.9-1.1) proceed ---
FORCE_OVERWRITE=1 Rscript R/0.4.4_process_exposure.R
FORCE_OVERWRITE=1 Rscript R/3_freq_x_exposure.R
Rscript R/qaqc_vop_vs_faostat.R                                # final: livestock ~1 AND crop ~1
```
If 0.4.0 errors or crop QAQC still off, report the error/ratios — I'll fix before you spend the R/3 cycle. Expected quirks to watch: crop layer-name↔FAO-code matching (spam2fao), the `all/irr` tech names in `variable=prod_t`, and admin_rast level table shape — all preserved from the original but flag anything.

**After both GREEN:** GATE 2 publish plumbing (derive `model=historic` + intld parquet uploader) + future-VOP decision → publish → CR-068. Latent USD-side (livestock nominal vs crop usd2015) still GATE-2. **HOLDING; nothing published.**

---

> ✅➡️ **CGLABS (2026-07-07): re-bake done, QAQC run. LIVESTOCK now validates against FAOStat I$ EXACTLY (ratio 1.00, 242/242) — currency fix confirmed correct; the 6.9× vs stale-live is a legit full-refresh. CROP is OFF (median 1.21, 10/50) → needs the 0.4.0 reinstatement. Livestock GREEN, crop-align is the remaining gate.**
>
> **Chain (FORCE 0.4.1→0.4.4→R/3, ~12h) clean. QAQC VERDICT (gridded country VoP ÷ FAOStat national GPV, const-I$, target ~1):**
> - **LIVESTOCK: median 1.00 | within 0.9–1.1 = 242/242 | AGO cattle grid 525.69M I$ = FAO 525.69M I$ = 1.00** ✓ Currency fix (`05c0b0e`, intld→real `vop_intd15`) is correct — livestock VOP raster dropped 2.12e10(nominal)→1.58e10(const-I$) and now matches FAOStat exactly.
> - **CROP: median 1.21 | within 0.9–1.1 = 10/50 | worst DZA/ALL-CROPS=0.00 | file=`spam_vop_intld15_all.tif`** ✗ The S3-legacy crop VOP is not FAOStat-I$-aligned.
> - csv: `Data/exposure/qaqc_vop_vs_faostat.csv` (330 rows). Minor: 2 FAO codes (230, 736 — old Sudan/Ethiopia splits) unmatched by `countrycode`, cosmetic.
>
> **On the 7×/6.9×:** RESOLVED as a real refresh, not a bug — livestock grid = FAOStat I$ to ratio 1.00, so the new AGO cattle-highland exposure (11.27M) is the CORRECT const-I$ value; the 2025-06-25 live (1.63M) used the wrong nominal/predecessor basis. Per the full-refresh decision, the new livestock values are shippable.
>
> **NEXT (needs macbook):** reinstate `0.4.0_create_crop_vop_intld15.R` (commit `92cb0b0`) → produce `spam_vop_intld15-2021_all` (FAOStat GPV × SPAM share, same method as livestock) → point R/3 at it → re-bake crop VOP + R/3 + re-QAQC (crop ratio should →~1). You offered "on your word" — **please push the 0.4.0 reinstatement + R/3 repoint; I'll run it.** (Then GATE 0 fully green → GATE 2 publish plumbing.)
>
> **Trivial flag:** `R/qaqc_vop_vs_faostat.R` calls `countrycode()` bare — add `library(countrycode)` (I ran it with the pkg pre-attached; installed on cglabs). **HOLDING; nothing published.**
>
> ---
>
> ✅ **MACBOOK (2026-07-06, commit `05c0b0e`): 7× TRACED + FIXED — it's a CURRENCY MISLABEL (livestock "intld15" was nominal-USD; crops are const-I$-2015). p.steward decision: fix + align crop to I$; nothing nominal in the product; QAQC vs FAOStat. Livestock fixed; crop-align plan below. Re-bake + run new QAQC.**

Your isolation is exactly right (glw_prop cancels head-scale → density fix can't move exposure → the 7× is VoP $/head basis). Traced it to the currency basis. Answers to (a)/(b):

**(a)/(b) — CONFIRMED: it's the mislabel, NOT a real VoP refresh. We would be publishing wrong-currency livestock.**
- Livestock hazard_exposure VoP (`glw4-2020_vop_intld15-2021.tif`) = 0.4.1 `vop_usd_nominal` = production × **global nominal price** (2019-2023) = **nominal USD ~2021**, mislabeled `intld15` (loop L537 used `vop_usd_nominal` for EVERY entry; the real `vop_intd15` = FAOStat constant-2014-2016 I$ GPV ×1000 was computed at 4.2 then **discarded**).
- Crop hazard_exposure VoP (`spam_vop_intld15_all.tif`, from S3/legacy) = constant **I$ 2015** (real).
- So the product mixed nominal-USD-2021 livestock with const-I$-2015 crops → the ~7× uniform multiplier (nominal-2021 cattle $ ≫ const-I$-2015). Also: current 0.4.1 didn't exist at the 2025-06-25 live bake (earliest commit 2025-07-14) — live livestock came from a predecessor on a different (smaller) basis; hence live 1.63M vs local 11.69M.

**FIX APPLIED (`05c0b0e`, 0.4.1):** distribution loop now picks the value column by output label — `intld15 → vop_intd15` (real const-I$), `nominal-usd → vop_usd_nominal`. **Both currencies produced correctly**; the intld15 product is now genuine international dollars, aligned with the crop const-I$ side. (p.steward: "nothing nominal [in the I$ product], I$ properly calculated" — done for livestock.)

**NEW QAQC (`R/qaqc_vop_vs_faostat.R`, p.steward's ask):** checks gridded **country** VoP totals vs **FAOStat national GPV (constant I$)** — the values being distributed — for BOTH livestock and crop rasters R/3 uses. Since distribution is proportion-based (sums to 1/country), the gridded country total should ≈ FAOStat GPV. **Ratio ~1 = basis+mass sound; far from 1 = currency/units/mass error → do not publish.** Run it after the re-bake.

**CROP ALIGN — plan (p.steward: "align crop too"):** crop hazard_exposure I$ VoP (`spam_vop_intld15_all`) currently comes from **S3/legacy with NO in-repo producer**. The proper producer is the **deleted `R/0.4.0_create_crop_vop_intld15.R`** (commit `92cb0b0`): it distributes FAOStat GPV **const-I$** by SPAM production share — exactly the livestock method — outputting `spam_vop_intld15-2021_all.tif`. **Recommended alignment:** reinstate 0.4.0 + point R/3 at `spam_vop_intld15-2021_all` so crop + livestock are BOTH proportion-distributed FAOStat const-I$ (same method, same 2021 window), dropping the S3-legacy crop dependency. **Sequencing:** run the QAQC first — if it shows the current S3 crop `spam_vop_intld15_all` is already ~1× FAOStat I$, crop is effectively aligned (I$) and reinstating 0.4.0 is a maintainability step; if it's off, reinstate 0.4.0 before publish. I can reinstate + validate 0.4.0 on your word.

**RE-BAKE (livestock fix) + validate:**
```bash
git pull   # head 05c0b0e
FORCE_OVERWRITE=1 Rscript R/0.4.1_create_livestock_exposure.R    # intld now real I$
FORCE_OVERWRITE=1 Rscript R/0.4.4_process_exposure.R
FORCE_OVERWRITE=1 Rscript R/3_freq_x_exposure.R                  # freq × corrected I$ VoP
Rscript R/qaqc_vop_vs_faostat.R                                  # livestock + crop vs FAOStat I$
```
Expect: livestock QAQC ratio → ~1; AGO cattle-highland exposure drops from 11.69M toward the const-I$ scale; crop QAQC confirms the crop basis. Report the QAQC VERDICT block.

**Also latent (usd side, GATE-2):** livestock `nominal-usd` vs crop `usd2015` (constant) is a parallel USD-basis mismatch — flag when we tidy the usd product. Full currency/source map saved (macbook memory + scratchpad findings). **HOLDING; nothing published.**

---

> 🛑 **CGLABS (2026-07-06): FORCE R/3 rebuilt everything from the corrected VOP — the 7× SURVIVED (still 7.16×). Density fix corrected head-COUNTS but NOT exposure. The 7× is a VoP price/magnitude-per-head difference, upstream of density. GATE 0 exposure NOT green. Needs macbook VoP-basis trace. NOTHING PUBLISHED.**
>
> **FORCE R/3 done clean** (~12h, `FORCE_OVERWRITE=1`, guard held). Parquets rebuilt 07-05 08:27. But:
> - **AGO adm0 cattle-highland NDWS `dry` (both vintages have it): LOCAL 11.69M vs LIVE 1.63M = 7.16×** — unchanged from pre-fix. Density→count fix did NOT move it.
> - **Why the density fix can't fix it:** `glw_prop = glw/glw_rast` cancels head-scale (your audit) → VOP raster global sum unchanged (2.12e10 pre+post). Correcting heads fixes `livestock_number` (0.06M→5.21M ✓) but leaves VOP magnitude identical → exposure identical.
> - **Isolated the 7× to VoP-per-head, not heads/density/split/freq:** head-counts now ~5M in BOTH vintages (live GLW4-2015, local GLW4-2020 density→count); freq is de-saturated (would LOWER local, not raise); AGO cattle split is plausible (highland 52.96M vs tropical 490M ≈ 10% highland, not skewed). Since dry-exposure = dry-freq × VoP and freq↓ + heads≈equal, the **7× must be in VoP $ per head** (price basis / national-VoP magnitude).
>
> **LEADING SUSPECT — the intld/nominal mislabel (your latent bug #1), re-examined:** you deprioritized it as "unchanged from live, not the 7×". But if the **2025-06-25 live `model=historic` was baked from a real-intld VoP** while the current pipeline builds "intld15" from `vop_usd_nominal` (production × **global** nominal price, 0.4.1 L523/L464), that basis switch **would** produce a uniform ~7× across subtypes — exactly the near-constant multiplier observed (any 7.16×, dry 7.16×). Worth confirming whether the live vintage used `vop_intd15` (real) vs the current `vop_usd_nominal`-as-intld.
>
> **QUESTIONS for macbook:**
> - (a) Trace AGO cattle **national VoP $** used now vs at the 2025-06-25 live bake (FAOStat element/year + price basis). Is the 7× a real VoP refresh (→ ship it, per full-refresh decision) or the intld-vs-nominal mislabel / a units bug (→ fix 0.4.1 L523 to use `vop_intd15`, re-bake)?
> - (b) If it IS the mislabel: is the live product real-intld and the current nominal-USD-mislabeled-as-intld (i.e. we'd be publishing wrong-currency values under `variable=vop_intld15`)?
>
> **HOLDING; nothing published.** Pipeline mechanics all correct (de-sat ✓, stacks ✓, §4.2.1 ✓, head-counts ✓); the open item is purely the VoP $ basis behind the 7×.
>
> ---
>
> 🟢🔧 **CGLABS (2026-07-05): GATE 0 GREEN at the source (0.4.1 output AGO cattle 0.06M→5.21M ✓, crop maize 1.2× ✓) — BUT the recipe's "R/3 plain" did NOT propagate the fix (overwrite-gated). Re-ran R/3 with FORCE to push corrected VOP through vop tifs + parquets.**
>
> **Chain ran (0.4.1→0.4.4→R/3→VAL, all rc=0). Validator post-fix checks GREEN:**
> - `GLW4-2020 AGO cattle × cellSize(km²)` = **5.09M (1.0× FAOStat)** ✓
> - **0.4.1 OUTPUT `livestock_number` AGO cattle = 5.21M (1.0×)** ✓ (was 0.06M — density→count fix took)
> - MapSPAM AGO maize prod_t = 2.894M t (1.2× FAOStat) ✓ (crop unaffected, as predicted)
> - (raw-tif VERDICT still 0.06M — expected; raw density unchanged, fix converts on-read.)
>
> **⚠️ BUT R/3 didn't rebuild — overwrite=FALSE skipped stale outputs:**
> - Corrected livestock VOP raster rebuilt today (`glw4-2020_vop_intld15-2021.tif`, 07-05 05:46). Note its **global sum is unchanged (2.12e10)** — expected, `glw_prop` cancels head-scale (your audit); the fix changes the **spatial/area-weighted split**, not magnitude.
> - **R/3 vop tifs + combined parquets are still 07-02** (`cattle-highland_…_int_vop` mtime 2026-07-02; combined ENSEMBLE parquet 07-02; AGO adm0 cattle-highland historic `any/dry` still **11.65M vs live 1.63M = the 7×**). R/3 "plain" is overwrite-gated → §4.1/§4.2/§4.2.1 all skipped the existing outputs → corrected VOP never reached them.
>
> **ACTION (cglabs, operational — propagating an already-approved fix):** re-running **`FORCE_OVERWRITE=1 Rscript R/3_freq_x_exposure.R`** so §4.1 recomputes vop tifs (freq × corrected VOP), §4.2/§4.2.1 rebuild parquets. FORCE (not targeted pre-delete) chosen to avoid partial-scope risk on a publish-critical rebuild; guard `5a566a5` keeps the multi-scenario stacks intact. Then verify cattle-highland AGO exposure drops to ~live scale (7× resolved) + re-validate. Flagging in case you'd prefer a targeted pre-delete instead — but proceeding with FORCE now to keep momentum.
>
> **HOLDING publish; nothing published.** GATE 2 builds + future-VOP decision still pending, after the FORCE R/3 confirms the 7× resolves.
>
> ---
>
> ✅ **MACBOOK (2026-07-04, commit `fdcf792`): density→count fix APPLIED in 0.4.1 + terra-probe-validated. Also confirmed MapSPAM CROP is structurally safe (p.steward asked) + added a crop sanity check to the validator. Re-run 0.4.1 → 0.4.4 → R/3 → re-validate. GATE 0 should go green.**

Your root-cause is spot on — thanks for the decisive ×cellSize test.

**FIX (`fdcf792`):** added `.glw_density_to_count(r) = r * terra::cellSize(r, unit="km")` in `0.4.1`, applied at **all three** raw `rast(glw_files)` reads (main L108, mask block, livestock_no block) at NATIVE res, before any resample. 2015 fallback (`if(FALSE)`) left as-is (already counts). Validated: terra probe (density × cellSize = count, layer-wise; cellSize unit="km" returns km² ≈85/cell at equator ✓) + your real-data ratio 1.05.

**MapSPAM crop — SAFE, not hit by this (audited per p.steward's ask):**
- Crop VoP is a **direct** `production × price` multiply (`0.4.2` L335: t/pixel × USD/t = USD/pixel) — **no `cellSize`/area division anywhere** (grep clean), unlike livestock's proportion-distribution.
- MapSPAM `prod_t` tifs are downloaded **pre-harmonized from S3** (`processing=atlas-harmonized`) and their **source did NOT switch** — only changes since the live bake are auto-format, "2020 added to year-sets", and file-naming (no `prod_t` unit change). So crop exposure should ≈ live.
- The GLW density trap was **livestock-specific** (the GLW4→GLW4-2020 batch was left as density). Still, I added an **empirical crop check** to the validator to be sure.

**RE-RUN (with FORCE so the fix lands):**
```bash
git pull   # head fdcf792
FORCE_OVERWRITE=1 Rscript R/0.4.1_create_livestock_exposure.R    # rebuild livestock VoP + livestock_number + masks (density->count)
FORCE_OVERWRITE=1 Rscript R/0.4.4_process_exposure.R             # re-extract exposure parquets
# then rebuild hazard x exposure:
Rscript R/3_freq_x_exposure.R                                    # plain (guard 5a566a5 protects the stacks)
Rscript R/validate_glw_vop_vintage.R                             # re-validate
```
The extended validator now reports: (a) raw ×cellSize demo (→ AGO cattle ~5M), (b) **0.4.1 OUTPUT** `livestock_number_number.tif` AGO cattle (post-fix should be ~5M, was ~0.06M), and (c) **MapSPAM AGO maize prod_t vs FAOStat (~2.5M t)** — ~1× = crop per-pixel-tonnes correct. **Report all three.**

**Still standing (fix in a follow-up, NOT this blocker):** the two latent bugs — (1) `intld15` product built from `vop_usd_nominal` (nominal-USD via global price), real `vop_intd15` discarded → mislabeled; (2) `0.4.4` L345 unit-map lacks `intld15-2021` → drops those rows. Flag when we get to GATE 2.

Once GATE 0 is green (livestock ~5M, crop ~1× FAOStat, 7× resolved), the future-VOP decision + GATE 2 plumbing resume. **HOLDING; nothing published.**

---

> ✅ **CGLABS (2026-07-03 #5): ROOT CAUSE PINNED — GLW4-2020 `_Da.tif` are per-km² DENSITY, not per-pixel counts. Data is CORRECT + current (legit ~5% 2015→2020 growth) — just a UNITS mismatch. NO re-acquire, NO revert. Fix = density→count conversion (× cell km²) in 0.4.1. Answers your provenance Q.**
>
> **Decisive test (settles re-acquire vs revert vs units):**
> - `Ct_2020_Da` raw global sum = **2.038e7** (the "20M" that looked broken).
> - `Ct_2020_Da × cellSize(km²)` global sum = **1.525e9**; 2015 `_Da` per-pixel count = **1.456e9** → **ratio 1.05**. Multiplying by pixel area recovers the correct ~1.5B global cattle AND matches the 2015 count to within 5% (the real 2015→2020 herd growth).
> - Same extent (-180/180,-90/90), same dim 2160×4320, same res 0.0833°, **same NA fraction 0.776** as the 2015 `_Da`. → **not corrupt, not partial, not wrong-product** — it is the right GLW4-2020 data, expressed as **animals per km²** instead of animals per pixel.
> - So the 2015 `_Da` was pre-multiplied to per-pixel counts; the 2020 `_Da` was left as density. `0.4.1` globs the dir (L98) + asserts "animals/pixel" (L124) → uses density as if counts → 73× undercount → distorts `glw_prop` highland split → the 7×. **Your one-root-cause audit is correct; the input isn't broken, it's mislabeled units.**
>
> **Provenance (your Q):** `Data/GLW4_2020/{Bf,Ch,Ct,Gt,Pg,Sh}_2020_Da.tif`, all dated **2025-08-25 12:0x** (one batch). Processed parquet json: `source.input_raster=GLW4`, `source_year.glw=2020`, `date_created 2026-05-25`. **No download URL/log on disk.** But provenance is now moot — the data is correct; only the unit handling is wrong.
>
> **RECOMMENDED FIX (input/preprocess, tiny):** in `0.4.1` where GLW4-2020 is read (L91-108), multiply each raster by `terra::cellSize(r, unit="km")` to convert density→per-pixel count (matching the 2015 `_Da` convention) before the `glw_prop` step. Then `validate_glw_vop_vintage.R` will show AGO cattle ≈5M and the 7× resolves. (The 2015 `_Da` needs no change.) Re-run **0.4.1 → 0.4.4 → R/3** → re-validate → GATE 2. **The two latent bugs (intld mislabel, 0.4.4 `intld15-2021` unit-map gap) still stand — fix alongside.**
>
> **cglabs HOLDING for macbook to apply the density→count fix (or authorize me to — it's a unit conversion at the input, arguably non-value-affecting-but-value-restoring; your call).** Nothing published.
>
> ---
>
> 🔬 **MACBOOK (2026-07-03 #5): AUDITED 0.4.1/0.4.2/0.4.4 end-to-end. The 80× and the 7× are almost certainly ONE root cause — the broken GLW4-2020 raster — NOT two bugs. VoP magnitude is fine; the raster's wrong SPATIAL pattern corrupts the highland split. Fix = correct/replace the GLW4-2020 input, not the code. Two latent data-quality bugs also found (not the blocker).**

**Why 80× and 7× are the same bug (the anomaly resolved):** 0.4.1 distributes VoP as `glw_vop = glw_prop × national_VoP` (L554), with `glw_prop = glw / glw_rast` (L243) = per-pixel heads ÷ national-total heads. **That ratio cancels the head scale** — so GLW4-2020 heads being 80× low does NOT change national VoP magnitude. The "$1.5 vs $876/head" is a non-quantity (VoP isn't head×price; it's national FAO VoP spread by livestock *share*). What the broken raster DOES change is the **spatial distribution**: per-pixel max 55× lower + many cells ~0 → distorted `glw_prop` → national cattle VoP splits differently into **highland vs tropical** (`split_livestock` L557) and aligns differently with per-pixel hazard freq in R/3's zonal sum. National VoP preserved; the cattle-**highland** subset inflates ~7×. So one broken input → 80× counts AND 7× highland exposure.

**Root cause = the GLW4-2020 files, not the pipeline.** Your numbers prove it: global cattle 20M (should ~1.5B), per-pixel max 3,654 vs the 2015 `_Da`'s 200,940, SAME res 0.08333°. Same grid + ~73× smaller values = a **wrong/corrupt/partial GLW4-2020 product**, not a per-km²/unit issue. 0.4.1 just globs the dir (L98) and trusts "animals/pixel" (L124). I diffed sections 4–5 (VoP formula) live-vs-now: **byte-identical** — no formula regression.

**Two latent data-quality bugs found (fix separately; NOT the publish blocker):**
1. **`intld15` product is mislabeled nominal-USD.** The distribution loop unconditionally `setnames(final_vop,"vop_usd_nominal","value")` (L523) for EVERY entry — so `vop_intld15-*` is built from `vop_usd_nominal` (= production × **global** nominal price, L464); the real `vop_intd15` (L474) is computed then discarded. Unchanged from live (not the 7×), but the "intld15" livestock exposure is NOT international dollars.
2. **0.4.4 drops `intld15-2021`.** Unit-harmonization map (L345) lists `intld15-2020` + `intld15` but NOT `intld15-2021`, while 0.4.1 now writes `intld15-2021` → those livestock rows filtered out at L346. Refactor naming-contract break.

Crop side (0.4.2) = nominal-USD from MapSPAM, unaffected.

**FIX PATH:** re-acquire/validate correct GLW4-2020 rasters (global cattle ~1.5B, per-pixel max ~200k like the 2015 `_Da`) **OR revert this bake to GLW4-2015** (flip 0.4.1 L91-108 back to the L110-121 block). Then re-run **0.4.1 → 0.4.4 → R/3** and re-run `validate_glw_vop_vintage.R` (GLW4-2020 AGO cattle should ≈5M). Separately fix the intld mislabel (use `vop_intd15` for the intld product) + the 0.4.4 `intld15-2021` unit-map gap. GATE 2 plumbing + future-VOP decision stay deferred. **HOLDING; nothing published.**

**Q for cglabs:** where did the GLW4-2020 tifs in `Data/GLW4_2020/` come from (download source/date)? A wrong/partial download is the likeliest explanation for global cattle=20M. Confirm before we decide re-acquire vs revert-to-2015.

---

> 🛑 **CGLABS (2026-07-03 #4): GATE 0 = RED. `validate_glw_vop_vintage.R` VERDICT: GLW4-2020 rasters are units/product-wrong (AGO cattle 80× TOO LOW), and there's a reconciliation anomaly (heads 80× lower but exposure 7× higher). VOP base compromised — DO NOT publish. Needs macbook to fix 0.4.1 VOP + untangle the exposure↔VOP chain.**
>
> **VERDICT block (verbatim numbers):**
> - FAOStat anchor (AGO cattle 2020): ~5.00M head
> - **GLW4 2015 `_Da` (LIVE base): AGO cattle = 4.99M (1.0× FAOStat) ✓** | global 1.46e9 (~1.5B ✓) | per-pixel max 200,940 | res 0.08333°
> - **GLW4-2020 (REBUILD base): AGO cattle = 0.06M (61,739; 0.01× the live) ✗** | global 2.04e7 (20M — impossibly low vs ~1.5B) | per-pixel max 3,654 (~55× lower) | res 0.08333°
> - GLW4-2020 files = `{Bf,Ch,Ct,Gt,Pg,Sh}_2020_Da.tif` (named `_Da`, but values ~73× too small vs the 2015 `_Da`).
> So GLW4-2020 is **NOT** the "5-8× too high" case macbook predicted — it's **80× too LOW**. Either wrong product/subset or a per-pixel→something unit change despite the `_Da` name. **Not a benign 2015→2020 refresh** (Angola cattle grew ~10%, not shrank 99%).
>
> **⚠️ RECONCILIATION ANOMALY (macbook please untangle):** the rebuild livestock VOP raster `Data/GLW4_2020/processed/…/glw4-2020_vop_intld15-2021.tif` (10 layers) global = **2.12e10 intld** — built on the broken 20M-head base. Yet the rebuilt AGO cattle-highland **exposure was 7× HIGHER** than live (52.6M vs ~7.4M), while heads are 80× LOWER. Implied price/head: live 5M head→7.4M VOP ≈ 1.5/head; rebuild 0.06M head→52.6M VOP ≈ 876/head — a ~580× price divergence. The heads↔VOP↔exposure chain is internally inconsistent across vintages; I can't resolve it without the 0.4.1 VOP-construction logic (price table, species aggregation, units). **The 7× and the 80× may be two separate bugs.**
>
> **RECOMMENDATION:** GATE 0 red → **fix `0.4.1_create_livestock_exposure.R` (GLW4-2020 selection/units, [0.4.1:98 glob dropped `_Da` selector], [0.4.1:124 per-pixel assertion]) + re-bake livestock VOP**, then re-run R/3 exposure, THEN re-check this validator (GLW4-2020 AGO cattle should ≈ 5M) before any publish. GATE 2 plumbing stays deferred. Crop (non-livestock) exposure may be unaffected — confirm separately if you want a livestock-only fix scope.
>
> **cglabs HOLDING; nothing published.** Pipeline logic (de-sat, stacks, §4.2.1) all correct — the block is purely the GLW4-2020 VOP input being wrong.
>
> ---
>
> 🚦 **MACBOOK + p.steward (2026-07-03 #3): the 7× caveat is now GATE 0 — must root-cause the VOP base BEFORE building publish plumbing. Found the likely cause + a decisive check to run. GATE 2 builds are deferred until GATE 0 is green.**

**GATE 0 — WHY the 7× (found): the GLW4 → GLW4-2020 livestock switch, which post-dates the live bake.** cattle-highland is livestock → VOP = GLW heads × price. `0.4.1_create_livestock_exposure.R` switched GLW4→GLW4-2020 in commits `35375cf` (2025-07-24) + `69d7b84` (2025-09-05) — AFTER the live `model=historic` bake (2025-06-25). Live used original **GLW4** (`5_Ct_2015_Da.tif`, dasymetric per-pixel); rebuild uses **GLW4-2020**. The switch also **dropped the explicit `_Da.tif` selector** (now globs all `.tif`, [0.4.1:98]) while the code still asserts "animals per pixel" ([0.4.1:124]).
**⚠️ 7× is TOO BIG to be a real 2015→2020 change** — Angola cattle grew ~10%, not 700%. So this is most likely a **unit/selection artifact** (GLW4-2020 tifs a different unit/product than the old `_Da`), NOT a benign refresh. Your internal-consistency checks prove VOP is *applied* right; they don't prove the *base number* is right.

**RUN THIS (decisive, read-only, ~1-2 min):**
```bash
git pull   # head e48176f
Rscript R/validate_glw_vop_vintage.R
```
It sums **AGO admin0 cattle HEADS** from GLW4-2020 (and old GLW4 `_Da` if still present) and compares to FAOStat (~5M, Angola 2020), + prints native res / per-pixel max / global sum per vintage.
- GLW4-2020 ≈ FAOStat (~5M) → head-count REAL, 7× is a legit VOP refresh → proceed to the publish-scope decision.
- GLW4-2020 ≈ 5-8× FAOStat → **UNITS BUG** in the GLW4-2020 rasters/selection → fix `0.4.1` + re-bake VOP first; the rebuild is wrong and NOTHING publishes.
Report the printed VERDICT block (both totals, ratio, res/max).

**GATE 2 builds — DEFERRED until GATE 0 green** (no point wiring publish for a wrong base). When green I'll ship both: (1) derive `model=historic` = extract `scenario=="historic"` rows from the combined ENSEMBLE parquet → filename token `historic` → routes to `model=historic/interaction.parquet`; **keep `value_sd` + `none`** (p.steward: full-refresh schema is intended); (2) add the intld `.parquet` uploader block to `s3_upload.R` (mirror the USD block L119-141). Your GATE-2 findings are correct + logged.

**Future-VOP decision (historic-only vs also-republish-future vs coordinated rebake) is ON HOLD pending GATE 0** — if it's a units bug, none of the options apply. **HOLDING; nothing published.**

---

> 🧪 **CGLABS (2026-07-03 #2): GATE 1 + GATE 2 results below. GATE 1 = 7× is VOP-base, freq NOT up (green, one caveat). GATE 2 = TWO publish-mechanism gaps found → macbook must build a step before publish. Still HOLDING.**
>
> **GATE 1 — 7× is VOP-base, de-sat did not raise freq (GREEN, 1 caveat):**
> - **Total VOP scenario-invariant:** local `any+none` for AGO adm0/cattle-highland/NDWS+THI-max+NDWL0/extreme = **52.6M for historic AND every ssp period** (52,609,621 ± <10k). Since `freq_any + freq_none = 1`, `any+none` **is** the total VOP → confirms VOP is applied consistently, scenario-independent. This 52.6M = current GLW4-2020 / intld15-2021 base.
> - **Freq (any%, VOP-weighted mean freq) NOT up:** historic **22.1%**; ssp245 by period 21.6% / 22.3% / 26.0% / 25.8% (rises with warming). Historic ≤ later future — consistent with a de-saturated historic baseline. Upstream NDWS also confirmed 29→21 days. **No evidence freq rose.**
> - **7× decomposition:** live `any`=1,632,199; if live any%≈22% then live total VOP≈**7.4M** → local 52.6M / 7.4M ≈ **7.1×** = the exposure ratio. So the 7× = VOP-base, not a freq→exposure regression.
> - **⚠️ CAVEAT (needs macbook eyes):** the live `interaction.parquet` has **no `.json` sidecar (404)** → I could not read the live VOP vintage to confirm directly. A **7× VOP jump is large** — plausibly GLW3→GLW4-2020 / deflator / units, but confirm the current `vop_intld15-2021` (GLW4-2020) base is intended/correct and not a units bug, since live vintage is unverifiable from S3.
>
> **GATE 2 — publish mechanism has TWO gaps (macbook build needed):**
> 1. **No `model=historic` file is produced.** `ls Data/hazard_risk_vop/{annual,jagermeyr}` shows only `…_{ENSEMBLE,ENSEMBLEmean,ENSEMBLEsd}_int_adm_{sev}.parquet` — **no `historic`-token parquet**. `s3_upload.R` keys `model=` off the filename token, so nothing routes to `model=historic/`. The current pipeline stacks historic as a scenario ROW inside the ENSEMBLE file. → **A derive step is required:** extract `scenario=="historic"` rows from the combined ENSEMBLE parquet → write `haz-freq-exp_<var>_historic_int_adm_<sev>.parquet` (filename token `historic`) so it publishes to `model=historic/interaction.parquet` and supersedes the 2025-06-25 file. (Match live schema: live `model=historic` had 8 subtypes, no `value_sd`; new baseline adds `none` — confirm whether to keep `value_sd` + `none` in the historic file.)
> 2. **No intld-parquet publisher.** `s3_upload.R`: the **USD** block (L119) has a `.parquet$` uploader, but the **INTLD** block (L147) is **`.tif`-only** — there is no uploader for the `variable=vop_intld15 … interaction.parquet` (the primary live product). So even with a `historic`-token parquet, `s3_upload.R` won't ship the intld parquet. → macbook: add an intld parquet uploader block (mirror the USD one at L119-141) OR point me at the script that actually published the live intld parquet.
>
> **NEXT:** macbook (a) confirm VOP base is intended (caveat above), (b) provide/authorize the derive-`model=historic` step, (c) add/identify the intld-parquet publisher. Then cglabs derives + publishes + CR-068 on AGO. **HOLDING; nothing published.**
>
> ---
>
> 📋 **MACBOOK + p.steward DECISION (2026-07-03): scope = FULL REFRESHED BASELINE. Publish the evolved product as the new `model=historic` hazard_exposure, superseding the 2025-06-25 vintage wholesale (new VOP base + `none` subtype + current GAUL/crop are all accepted, in-scope). BUT two data-side gates must pass on cglabs first — DO NOT publish until both green.**

Great recovery work — pipeline is correct + internally consistent. Answers to your 3 questions:

**(b) Schema/scope — RESOLVED (p.steward): ship it all.** The `none` subtype (CR-068), GAUL 162, current crop list, and the new VOP vintage are the intended new baseline. This publish supersedes 2025-06-25 entirely; not NDWS-values-only. So the schema drift is not a blocker — it's the point.

**(a) The 7× — must be PROVEN VOP-base before publish (GATE 1).** Reasoning agrees with you: near-uniform multiplier across subtypes (any 7.14×, dry 7.13×) + de-saturated upstream freq = a VOP-base scaling, not a freq→exposure regression. But confirm with data, not inference. exposure = haz_freq(0-1) × VOP, so decompose:
  1. **VOP vintage compare:** read the `.json` attr `source.input_raster` on the LIVE `model=historic` `interaction.parquet` vs your local rebuilt vop tif — confirm the VOP raster/vintage actually differs (GLW4-2020 / MapSPAM-2020 / intld15 deflator vs whatever 2025-06 used).
  2. **Freq is NOT up:** pull the `_int` ensemble haz_freq (0-1) for AGO adm0 / cattle-highland / NDWS+THI-max+NDWL0 / extreme / historic and confirm it is **≤ the live freq** (de-sat lowered it). If freq itself rose → REAL regression → HALT and dispatch.
  3. **Total-VOP sanity:** local `any + none` (11.65M + 40.96M = 52.6M) should equal the standalone VOP zonal sum for cattle-highland AGO. If live total VOP ≈ 7.4M and local ≈ 52.6M with any% ≈ 22% in BOTH → clean VOP refresh, benign. Report the two totals + the two freqs.

**(c) Publish mapping — CONFIRMED from code + one thing YOU must inventory (GATE 2).** [`R/s3_upload.R`] keys the S3 `model=` partition off the **filename token** (`parse_filename` → `x$gcm` for tifs, `x[4]` for parquets), NOT the scenario column — there is **no** scenario→model remap. Consequence:
  - Live serves historic from a **separate `model=historic` product**; your combined merge output is `model=ENSEMBLE` with scenario∈{historic,ssp*} inside it. If you publish only the ENSEMBLE file, its historic rows land at `model=ENSEMBLE/…`, **not** `model=historic/…` — the notebook's historic view would still read the stale 2025-06-25 `model=historic`. So a "full refreshed historic baseline" REQUIRES a file whose **filename token = `historic`** → publishes to `model=historic/interaction.parquet`.
  - **INVENTORY NEEDED:** `ls Data/hazard_risk_vop/{annual,jagermeyr}/*.parquet` and `…/hazard_risk_vop_usd/…` — is there a `*_historic_int_adm_*.parquet` (model token `historic`), de-saturated? Under current naming, historic per-GCM = `historic_<gcm>` (model=`<gcm>`) and historic ensemble = `historic_ENSEMBLEmean` (model=`ENSEMBLEmean`) — so a literal `model=historic` file may NOT be produced anymore, whereas 2025-06-25 had one. **Report the exact filenames** so we know whether the model=historic product exists or must be derived (e.g. extract scenario=historic rows → write as `…_historic_int_adm_…`). This is the crux of matching the live read structure.
  - **intld parquet publisher:** `s3_upload.R`'s vop_intld block uploads `.tif` only — **no parquet branch**. The live product is `variable=vop_intld15 … interaction.parquet`, so the intld hazard_exposure PARQUET ships via a different path ([[feedback_r21_publish_path]]). Identify/confirm which script+section actually publishes the vop_intld interaction parquet before we finalize the invocation.

**NEXT (cglabs):** run GATE 1 (report 2 totals + 2 freqs + the two VOP `.json` sources) and GATE 2 (parquet filename inventory + intld-parquet publisher). Post results — I'll confirm the exact publish invocation + any derive-model=historic step, then it's publish → CR-068 on AGO. **Still HOLDING; nothing published.**

---

> 🛑 **CGLABS (2026-07-03): R/2+R/3 recovery COMPLETE + §4.2.1 merge fix works — but HALTED before publish. Rebuilt exposure is ~7× the live values + schema changed. Needs macbook/p.steward reconciliation before any production write. NOTHING PUBLISHED.**
>
> **Pipeline recovery: DONE.** §4.2.1 co-sort fix (`f502270`) held — all 12 combined ENSEMBLE `_int_adm` parquets built (annual+jagermeyr × vop/vop_usd × 3 sev), `value`+`value_sd`, model=ENSEMBLE, all 5 scenarios, **future NDWS restored** (historic 459450 + 4×1.84M ssp). NDWS `_int` back to full 153-layer stacks. Upstream de-sat confirmed (NDWS 29→21 days; `_int` ensemble freq ~0.18).
>
> **⛔ PUBLISH BLOCKER — exposure values don't reconcile with live.** Live publish structure: historic is a **separate `model=historic`** product (`domain=hazard_exposure/…/variable=vop_intld15/period=annual/model=historic/severity=extreme/interaction.parquet`, baked 2025-06-25); ssp lives under `model=ENSEMBLE`. Apples-to-apples (AGO admin0, cattle-highland, NDWS+THI-max+NDWL0, extreme, historic):
> | subtype | LIVE | LOCAL (rebuilt) |
> |---|---|---|
> | any | 1,632,199 | 11,651,659 |
> | dry | 1,632,199 | 11,649,802 |
> | none | absent | 40,957,962 |
>
> Issues to reconcile **before publish**:
> 1. **~7× higher stress exposure** (local vs live) — de-sat should LOWER historic stress, not raise it. Upstream freq IS de-saturated, so this is likely a **VOP-base difference** (live 2025-06-25 vintage vs local `vop_intld15-2021` / MapSPAM-2020 / GLW4-2020) — but UNVERIFIED. If it's a real freq→exposure error, publishing corrupts the Atlas.
> 2. **Schema drift** since 2025-06-25: local adds the `none` subtype (CR-068(a) `hazard='none'`), GAUL admin2 shifts (160 vs 162), crop-count diff. Publishing ships all accumulated pipeline changes, not just the NDWS fix — is that in-scope for Track-1?
> 3. **Publish mapping**: my combined parquet is `model=ENSEMBLE` with scenario∈{historic,ssp*}; live splits scenario=historic → `model=historic`. How does `s3_upload.R` remap (scenario=historic rows → `model=historic` path)? I haven't traced/validated it — publishing on an unverified mapping risks overwriting/duplicating live keys.
>
> **QUESTIONS for macbook/p.steward:**
> - (a) What VOP vintage did the live 2025-06-25 `model=historic` product use? Confirm the 7× is VOP-base (expected) vs a freq→exposure regression.
> - (b) Is shipping the evolved schema (`none` subtype, GAUL, crop list) intended for this Track-1 publish, or should it be NDWS-values-only?
> - (c) Confirm the `s3_upload.R` scenario→model publish mapping + the exact invocation for the historic re-publish.
>
> **cglabs HOLDING — no publish.** Local rebuild is complete + internally consistent; only the production-publish correspondence is unresolved.
>
> ---
>
> ✅ **MACBOOK (2026-07-02, commit `f502270`): §4.2.1 merge FIXED + probe-validated. Neither A nor B verbatim — a localized co-sort at the merge (B's mechanism, no §4.2 re-extraction). Answers to your key questions below. Re-run §4.2.1 only.**

**True unique row key (within one ENSEMBLE parquet):** model/severity/exposure_var/exposure_unit are constant per file; the varying identity is
`(iso3, gaul0_code, gaul1_code, gaul2_code, admin0_name, admin1_name, admin2_name, scenario, timeframe, hazard, hazard_vars, crop)`. The **gaul codes are load-bearing** — rows exist at 3 admin levels stacked (adm2 + adm1/adm0 aggregates from L1316-1332), so gaul2 is NA on adm1 rows and gaul1+gaul2 NA on adm0 rows.

**Are the 57 936 dups (0.16%) a bug? No — an artifact of the identity you tested.** Your test key had `gaul2_code` but **dropped `gaul1_code`**. Two things then collide: (a) adm1 rows share the same duplicated `admin1_name` across different `gaul1_code` (real distinct polygons — the aggregation at L1316 groups by gaul1_code so they ARE distinct rows), and (b) the adm0/adm1 aggregate rows all carry `gaul2_code = NA`. Add `gaul1_code` back and they separate. A residual sliver may be genuine **CR-115 disputed-territory dups** (Ilemi/Abyei etc.) — those are the on-hold Atlas convention issue [[project_cr115_disputed_territory_convention]], NOT this merge's problem. **Don't chase them now.**

**Fix chosen — localized co-sort in §4.2.1 (not A, not B-as-stated):**
- **Not A (keyed join):** would row-**multiply** on the 57 936 dups. Rejected.
- **Not B-in-§4.2 (change `order_by` → full identity):** correct, but re-sorting every per-model parquet means re-running the whole §4.2 extraction — wasteful, and your per-model parquets are **already correct** (you verified full multi-scenario NDWS restored).
- **What I did:** in §4.2.1, read BOTH mean & sd fully, `setorderv` both by the **full shared identity** (all gaul codes + every hazard/scenario dim + crop), THEN the positional `value_sd <- en_sd$value`. `data.table` radix sort is **stable** and both files come off the identical extraction path (same boundaries/melt/rbind order), so even genuine ties keep the same relative order in both → 1:1 alignment, **no join, no row-multiplication**. Also rewrote the alignment guard to compare a composite key string (the old per-column `!=` with `na.rm=TRUE` silently **dropped** the NA-gaul aggregate rows and could pass a real misalignment).

**Validated (macbook):** synthetic probe mimicking the real schema — adm0/adm1/adm2 rows with NA gaul, duplicate-admin1-name-across-gaul1, and a CR-115-style TRUE dup present in both files, each shuffled **independently** → after the co-sort, `value_sd` lands on the correct identity for **all 577 rows, 0 mismatches**. Logic is sound; needs your confirm on the real 36.97M-row files.

**Re-run (merge only — everything upstream is correct + kept):**
```bash
git pull   # head f502270
# §4.2.1 is guarded by !file.exists(save_file)||overwrite4. The ENSEMBLE (combined)
# parquets never got written (merge halted), so a plain re-run rebuilds only them:
Rscript R/3_freq_x_exposure.R    # §4.1/§4.2 skip (exist); §4.2.1 builds the ENSEMBLE files
```
Verify: combined `ENSEMBLE` parquet exists per variable, has `value` + `value_sd`, `model=ENSEMBLE`, ssp rows present (future restored), and spot-check a few (admin, hazard, scenario) rows so `value_sd` = the ENSEMBLEsd `value` for that identity. Then publish → CR-068 on AGO. Guard `5a566a5` + `nrow()` `87f6d51` standing.

---

> 🛑 **CGLABS (2026-07-02): recovery WORKED (full multi-scenario stacks restored, future NDWS back) — but R/3 §4.2.1 combined-ENSEMBLE merge halts at L1402 "row order mismatch". MACBOOK: value-affecting merge fix needed.**
>
> **Recovery result (good):** guard held, no truncation. §5.3 rebuilt NDWS `_int` = **153 layers** (17 scen-periods × 9 subtypes, all 5 scenarios; PTOT sibling 136). R/3 §4.1 rebuilt vop tifs; §4.2 wrote per-model parquets — **ENSEMBLEmean/sd now full multi-scenario**: NDWS `hazard_vars` = historic 459450 + ssp126/245/370/585 @ 1 837 800 each (**future NDWS restored**, was 0 before). Historic `_int` de-saturated (~0.18 ensemble any-layer). So the per-model products are correct.
>
> **Still blocked — R/3 §4.2.1 (L1402):** the merge that folds ENSEMBLEmean+ENSEMBLEsd into the combined `ENSEMBLE` parquet (value + value_sd) is **positional** (`en_mean$value_sd <- en_sd$value`) and guards with `if (sum(en_mean$gaul2_code != en_sd$gaul2_code)+sum(en_mean$hazard != en_sd$hazard) > 0) stop("row order mismatch")`. It halts: §4.2 writes mean & sd with `sort_by = order_by = c("iso3","admin0_name","admin1_name","admin2_name","crop")` (L1165) — **omits `hazard`, `scenario`, `timeframe`, `hazard_vars`** — so within a sort group the two files order those rows differently → positional misalignment. (This is the row-order fragility flagged 2026-06-30; the `nrow()` fix `87f6d51` cleared L1398 and exposed L1402.)
>
> **Why not a trivial cglabs fix (→ macbook):** it's value-affecting (value_sd must attach to the right row) AND the full identity `(iso3,admin0,admin1,admin2,gaul2_code,scenario,timeframe,hazard,hazard_vars,crop,severity,exposure_var)` still has **57 936 dups / 36.97M (0.16%)** — so a plain keyed join would row-multiply. Need your schema knowledge: **what is the TRUE unique row key** (is there an ID, or are the 57 936 dups a separate bug — e.g. admin0/1 aggregate rows with gaul2=NA colliding)?
> **Recommended fix (your call):** (A) make §4.2.1 merge by a **keyed join on the true unique key** (after resolving the dups); or (B) co-sort BOTH mean & sd by the **full identity** in §4.2 (`sort_by` = all id cols) so the positional assign is valid. B is smaller if the dups are benign-and-identical; A is safer if dups are real. Please decide + push.
>
> **cglabs HOLDING:** per-model parquets are correct + complete; only the combined-ENSEMBLE merge + publish are blocked. No publish. Guard `5a566a5` + `nrow()` `87f6d51` both good/standing.
>
> ---
>
> ✅ **MACBOOK (2026-07-01, commit `5a566a5`): strategy VALIDATED against the code. Your recovery approach is correct — recipe confirmed below. Root-cause guard applied at both stack stages. cglabs may proceed.**

**Diagnosis confirmed by reading the code.** §5.3's output `save_file` is keyed by **model** (`<crop>_<model>_<sev>_<combo>_int.tif`), and `files <- haz_int_file_tab[hazard==combo & model==model_choice]` gathers **every scenario/timeframe of that model**. Historic per-GCM stacks are named `historic_<gcm>_1995-2014_<combo>` → split token 2 (`model`) = `<gcm>`, so a GCM `_int` legitimately stacks `historic_<gcm>` + `ssp*_<gcm>` layers into one file. `.rebake_scope` keeping only `historic_*` basenames dropped the ssp inputs → GCM `_int` built historic-only (9 vs 136). Your blast analysis is exactly right.

**Q1 — recovery mechanism: CONFIRMED, no new knob.** Pre-delete the NDWS artifacts + re-run §5.2/§5.3/R/3 with **NO `REBAKE_SCENARIO`**, `overwrite=FALSE`. The pre-delete + overwrite=FALSE already scopes to NDWS (only the deleted NDWS outputs rebuild; every non-NDWS + non-deleted output is skipped), and the unscoped inputs give §5.3 all scenarios per model → full 136-layer stacks. A separate hazard-scope knob would be **redundant** — pre-delete IS the hazard scope. Don't add one.
- **Do NOT pre-delete `haz_time_int` NDWS per-GCM stacks.** Those are single-scenario files (9 subtypes each); the historic ones you rebuilt this session are de-saturated ✓ and the future ones are intact (2026-05-28) — they are the correct §5.3 INPUTS. Deleting them just forces §5.2 to rebuild identical files.
- **Pre-delete only the aggregated/derived NDWS outputs:** hazard_risk NDWS `_int` per-crop tifs (all models — the truncated §5.3 outputs) + vop/vop_usd NDWS `_int` tifs (R/3 §4.1) + NDWS `_int_adm` parquets incl. the partial `vop_intld15` annual ENSEMBLE mean+sd (R/3 §4.2).
- **§5.2 is a near no-op** on recovery (per-GCM stacks all exist → overwrite=FALSE skips). Safe to include as a belt; the required rebuild stages are **§5.3 + R/3**.

**Q2 — audit + guard: DONE, guard applied (`5a566a5`).** Swept all 7 `.rebake_scope` sites. Only **two** aggregate multi-scenario stacks and therefore truncate under scoping:
- **R/2 §5.3 (L1653)** — per-model `_int` aggregation (the bug).
- **R/3 §4.1 (L475)** — reads those `_int` stacks; token `historic` would keep only `model==historic` files (2nd truncation vector you dodged by running R/3 plain).
- **Safe (per-scenario outputs, scoping is correct):** R/2 L711 (§1 class), L792/L807 (§2 freq), L1140 (§4 ensemble), L1352 (§5.2 class-input → per-scenario stacks). §5.2-main output (L1471) is also single-scenario (`<scen_mod_time>_<combo>.tif`) → safe.
- **Guard applied:** §5.3 now reads the **full unfiltered** `haz_time_int` set (warns if `REBAKE_SCENARIO` set); R/3 `.rebake_scope` is now **identity + warn** (R/3 has zero per-scenario products — all `_int` inputs are stacks, all outputs baseline). Non-value-affecting (fires only when REBAKE is set; recovery uses none). This makes the truncation **structurally impossible** to repeat — §5.3/R/3 physically cannot emit a scoped stack. **`git pull` → head `5a566a5` before the recovery run.**

**Q3 — overwrite semantics: YES, one pass, full stack, no toggle.** With the NDWS `_int` outputs pre-deleted, §5.3 sees `file.exists==FALSE` → builds; with inputs unfiltered, `files[hazard==combo & model==model_choice]` gathers **all** scenario/timeframe stacks for that model → the full multi-scenario `_int` in a single `terra::rast(files)` pass. No extra flag. (Historic-model `_int`, if any, is 9-layer by design = 1 timeframe × 9 subtypes — that's correct, not truncation; the truncation was only the GCM/ENSEMBLE `_int` files losing their ssp layers.) `overwrite5.3`/`overwrite` stay FALSE.

**Recovery recipe (validated):**
```bash
git pull   # head 5a566a5
# 1. pre-delete NDWS aggregated/derived outputs (NOT haz_time_int per-GCM stacks):
#    - hazard_risk/**/<crop>_*_*_<NDWS-combo>_int.tif  (all models)
#    - vop/**, vop_usd/**  NDWS _int tifs
#    - <...>_int_adm.parquet  (vop + vop_usd, incl partial vop_intld15 annual ENSEMBLE mean+sd)
# 2. rebuild stacks + exposure UNSCOPED (no REBAKE_SCENARIO), overwrite=FALSE:
RUN_R2_RUN5_2=1 RUN_R2_RUN5_3=1 Rscript R/2_calculate_haz_freq.R    # §5.2 no-op if stacks intact; §5.3 rebuilds full _int
Rscript R/3_freq_x_exposure.R                                        # plain: §4.1 vop tifs + §4.2/§4.2.1 parquets
```
Verify before publish: NDWS GCM `_int` nlyr back to ~136 (matches PTOT sibling); historic layers de-saturated; future layers = status-quo (unchanged vs live); ENSEMBLE `_int_adm` parquet has ssp rows again. Then publish → CR-068 on AGO. **Proper future NDWS de-saturation stays Track-2.**

---

> ▶▶ **MACBOOK ACTION REQUESTED (p.steward decision 2026-07-01): validate the corrected stack-stage strategy before cglabs re-runs. Include future in the rebuild (status-quo values).**
>
> p.steward chose: **(1) hand the strategy to macbook first** (don't let cglabs improvise the re-run); **(2) include future NDWS** in the rebuild (values = current-live/unchanged; proper future de-sat stays Track-2).
>
> **Please decide + push the corrected approach for the stack-stages (§5.2/§5.3/R/3):**
> - **Q1 — recovery mechanism.** The truncated NDWS `_int` future layers are already gone (overwritten to 9-layer historic), so a layer-surgical "replace historic only" is NOT possible — a **full multi-scenario NDWS rebuild** is required regardless. Confirm the cglabs recipe: pre-delete the NDWS artifacts (`_int` in hazard_risk + haz_time_int, NDWS vop tifs in vop/vop_usd, and the partially-rebuilt annual `vop_intld15` parquets) → re-run **§5.2/§5.3** and **R/3** for NDWS **WITHOUT `REBAKE_SCENARIO`** (scope = pre-delete + `overwrite=FALSE`; §5.2 restacks all scenarios from the intact freq). Is that the approach you want, or do you prefer a **hazard-scope** knob (rebuild NDWS combos across all scenarios) added to §5.2/§5.3?
> - **Q2 — audit.** Your earlier holistic pass covered R/2's `scenario=="historic"` branches, but the failure here is the **`REBAKE_SCENARIO` input-filter interacting with multi-scenario STACK outputs** at §5.2/§5.3/R/3. Please audit those three for any other place a scenario-scoped run truncates a stack (so a re-run doesn't silently drop future again). Consider making `REBAKE_SCENARIO` **error/warn** when the stage writes multi-scenario stacks.
> - **Q3 — overwrite semantics.** With `overwrite=FALSE`, will §5.2 actually rebuild a pre-deleted NDWS `_int` as a **full 136-layer** stack (all scenarios) in one pass, or does it need a specific toggle? Confirm so the recovery run is right first time.
>
> **cglabs is HOLDING** — no re-run, no publish — until macbook pushes the validated recipe. Corrupted-local inventory to fix on recovery: NDWS `_int` (hazard_risk+haz_time_int) = historic-only 9-layer; NDWS vop tifs = historic-only; a few annual `vop_intld15` `_int_adm` parquets (ENSEMBLE mean+sd, partial — R/3 crashed at §4.2.1) = historic-only-NDWS. Everything else (per-scenario freq/class/timeseries; non-NDWS stacks) is intact.
>
> ---
>
> 🛑 **CGLABS (2026-07-01): STRATEGY BUG FOUND — historic-scoping TRUNCATED the multi-scenario NDWS `_int` stacks. NOT published (live Atlas safe). Recovery feasible. Needs a decision.**
>
> **What broke:** the `_int` / vop / admin-parquet layers are **multi-scenario STACKS**, not per-scenario files. A PTOT `_int` tif = **136 layers** (9 interaction-subtypes × {historic + ssp126/245/370/585 × periods}). But `R/2 §5.3` run under `REBAKE_SCENARIO=historic` rebuilt the NDWS `_int` tifs as **historic-only (9 layers)** — dropping 127/136 layers (all future). Cascade: R/3 §4.1 built historic-only NDWS vop → §4.2 parquet has NDWS `hazard_vars` for **historic only, 0 ssp**. Confirmed: `Data/hazard_risk/annual/...NDWS+THI-max+NDWL0_int.tif` nlyr=9 (historic), mtime today; sibling PTOT `_int` nlyr=136 (all scenarios).
> **Why:** historic-scoping is correct for the **per-scenario** stages (timeseries / class / freq — files are `historic_*` vs `ssp*`), but WRONG for `_int`/vop/parquet where scenarios are **stacked into one file**. Scoping to historic there = truncation.
>
> **Blast radius:** LOCAL only — **nothing published**, live Atlas untouched. Corrupted local artifacts: NDWS `_int` (hazard_risk + haz_time_int), NDWS vop tifs, and the 2 rebuilt annual `vop_intld15` parquets — all now historic-only for NDWS. Earlier stages are FINE: historic NDWS freq de-saturated ✓, future NDWS freq (960 ssp files) intact ✓. De-saturated historic NDWS is correct; only the future layers were dropped.
>
> **Recovery (feasible, future freq intact):** rebuild the **full multi-scenario** NDWS `_int` → vop → parquet by re-running **§5.2/§5.3/R/3 for NDWS WITHOUT scenario-scoping** (scope by pre-deleting the NDWS artifacts + `overwrite=FALSE`, NO `REBAKE_SCENARIO`). §5.2 restacks all scenarios from the intact freq: historic = de-saturated (the goal), future = rebuilt from **existing (unchanged) future NDWS freq**.
> **⚠️ Implication needing your call:** future NDWS freq was **never fixed** (Track-1 = historic only; future = Track-2, deferred). So rebuilt future NDWS = **same values as currently live** (no regression, status-quo), but the recovery does **re-process future NDWS**. Proper future de-saturation stays Track-2.
>
> **Fixes applied this session that are still good + flagged for ratification:** R/1 ClusterRegistry (`44ec9e7`), R/3 §4.2.1 `nrow()` (`87f6d51`). R/2 (L1554 etc.) fixes from macbook are correct — the historic NDWS *freq* is properly de-saturated; the bug is purely the scoping strategy at the stack stages.
>
> **HOLDING for decision on the recovery approach (below).**
>
> ---
>
> 🔧 **CGLABS (2026-06-30 #2): R/3 §4.2.1 ENSEMBLE-merge tibble bug — FIX APPLIED on cglabs (commit below), FLAGGED for macbook ratification.** [New workflow per p.steward 2026-06-30: cglabs applies trivial/unambiguous/non-value-affecting blockers directly + flags here; anything value-affecting still goes to macbook first.]
> **Diff applied:** `R/3 L1398` `if (en_mean[, .N] != en_sd[, .N])` → `if (nrow(en_mean) != nrow(en_sd))`. Reason below. Please ratify.
>
> Ran R/3 plain (pre-deleted 9240 NDWS `_int` vop tifs + 72 `_int_adm` parquets). §4.1 intersect ✓; §4.2 wrote `vop_intld15-2021` annual ENSEMBLEmean+sd `_int_adm` parquets (de-saturated NDWS, 29 619 210 rows each). Then §4.2.1 (merge mean+sd → combined ENSEMBLE) halted:
> ```
> Error in if (en_mean[, .N] != en_sd[, .N]) { : argument is of length zero
> ```
> **Root cause — `R/3 L1398`:** `arrow::read_parquet()` returns a **tibble** (`tbl_df,tbl,data.frame`), but the row-count guard uses **data.table** syntax `en_mean[, .N]`. On a tibble `[, .N]` → length-0 → `integer(0) != integer(0)` → `logical(0)` → `if()` errors. (Tested directly: class `tbl_df`, `nrow`=29.6M, `x[,.N]` length 0.) It's the **only** data.table-ism in the block — the later `$` ops (L1404 `en_mean$value_sd <- en_sd$value`, L1406 `en_mean$model`) work fine on a tibble.
> **FIX (1 line, your call to apply):** L1398 → `if (nrow(en_mean) != nrow(en_sd)) {`. (Equivalently `setDT(en_mean); setDT(en_sd)` after the L1393/1394 reads — but `nrow()` is minimal and the rest of the block is tibble-safe.)
> `do_ensemble_sd4.2=TRUE` (default) → §4.2.1 **always** runs, and **publish needs the merged ENSEMBLE parquet** (`model=ENSEMBLE`, value+value_sd — that's the Atlas ensemble layer), so this blocks the publish. Existing ENSEMBLE parquets date 2025-08-15 → worked then; arrow/auto-format regressed it since. (Note: this is in R/3, which the earlier holistic audit covered R/2 only.)
>
> **cglabs state:** §4.1 done; §4.2 partial (only `vop_intld15` annual ENSEMBLE mean+sd parquets written, de-saturated ✓). Pre-deleted NDWS vop tifs being rebuilt. 0 future risk (R/3 baseline-only). Holding for the L1398 fix, then re-run R/3 → publish → CR-068.
>
> ---
>
> ✅ **CGLABS (2026-06-30): R/2 COMPLETE + verified. Moving to R/3 — with a recipe correction (REBAKE_SCENARIO no-op for R/3).**
>
> **R/2 (L1554 fix) — DONE clean, both axes.** §1 classifies NDWS (149s), §2.1+§5.2-main pass, §5.2 ensemble now builds historic combos, §5.3 completes (no n=0 halt). Verified:
> - int ENSEMBLEmean-NDWS: 18 (annual) / 18 (jagermeyr) — was 0
> - hazard_risk NDWS `_int`: 660 / 660 (33 ENSEMBLEmean each)
> - `_int` ENSEMBLEmean NDWS+THI-max+NDWL0 mean = **0.18**, range [0,1] (de-saturated, proper freq)
> - 0 triple-historic, 0 `_1981_2014`, 0 future touched (max future mtime still 2026-05-28)
>
> **⚠️ R/3 RECIPE CORRECTION (FYI — proceeding):** the dispatch step said `REBAKE_SCENARIO=historic Rscript R/3`. That's a **no-op for R/3**: its only `.rebake_scope` call (L475) filters `hazard_risk/*.tif`, but those are **bare-GCM, 0 "historic" tokens** (6121 files, 0 historic / 0 ssp) → the filter would drop *every* input and R/3 would process nothing. R/3's products are **all baseline** (vop / vop_usd / parquets: ssp=0 everywhere), so scenario scoping is moot anyway. NDWS reaches R/3 **only via `_int` compounds** (no solo).
> **Corrected R/3 run (executing):** pre-delete the stale NDWS `_int` vop tifs (3960 vop + 5280 vop_usd) **and** the `_int_adm` parquets (18 vop + 18 vop_usd — they aggregate NDWS per model/sev), then `Rscript R/3_freq_x_exposure.R` **plain** (FORCE unset, NO REBAKE_SCENARIO, overwrite=FALSE → §4.1 rebuilds only the deleted NDWS vop tifs, §4.2 rebuilds the 36 parquets from the refreshed tif set). No future guard needed (R/3 is baseline-only). **Pausing before the S3 publish for a final value-check.**
>
> ---
>
> ✅ **MACBOOK (2026-06-29 #3, commit `dcb5c1d`): FIX A applied (L1554) + full historic-path audit done (your META request). Re-run R/2.**
>
> **L1554 fixed:** dropped the `scenario_choice != "historic"` gate → `if (do_ensemble5.2)`. §5.2 now builds `historic_ENSEMBLEmean_historic_<combo>.tif` (+ `_ENSEMBLEsd`) from your 324 per-GCM stacks; §5.3's model loop (incl. ENSEMBLEmean) finds them. overwrite5.2=FALSE + pre-deleted historic combos → builds only the missing historic ensembles; future already has them.
>
> **HOLISTIC PASS over every R/2 historic conditional (so we don't hit a 5th):** swept all 10 sites — `scenario=="historic"` / `!= "historic"` / `grepl("histor")` at L115/872/894/1214/1242/1321/1459/1554/1659/1713. On the **hazard_exposure path (§1→§2→§5.2→§5.3)** only L1554 was broken; everything else is consistent:
> - **L872** (§4 risk ensemble `model != "historic"`): benign — drops only rows whose MODEL token is literally `historic`, not historic-scenario GCM rows (those are `historic_<gcm>_…`, model=`<gcm>`). Historic GCM data survives.
> - **L894 / L1242** (§4 risk + mean per-hazard ensemble): NO historic gate → historic IS built. Good.
> - **L1459** (§5.2 **main** per-GCM): no historic gate → your 324 historic stacks built. Good.
> - **L1659 + L1713** (§5.3): `timeframe_options` excludes historic, but §5.3 has a dedicated `model_choice=="historic"` count-branch — consistent by design; satisfied once L1554 builds the ensemble.
> - **L1321** (`files_hist <- grep("historic")`): DEAD — inside `if (FALSE)` (disabled "change in mean" block). Ignore.
> - **L115 / L1352-59**: naming/scenario-table construction, fine.
> So after `dcb5c1d` the **hazard_exposure path has no remaining historic asymmetry** — confident there's no 5th bug on THIS path.
>
> **⚠️ One off-path historic asymmetry FLAGGED (do NOT fix now):** **L1214** (§4.1 mean-ensemble) excludes historic the same way L1554 did → it builds **future-only** per-hazard mean-ensembles. BUT (a) it's a **different product** — `haz_mean_dir` → `timeseries_mean_month` (climate domain), NOT the hazard_exposure that R/3 builds from §5.3 `_int`; (b) it does **not crash** (lists all, excludes historic, skips existing future under overwrite=FALSE). So it does **not** block Track-1. Fixing it changes a climate-domain product (value-affecting, out of Track-1 scope) → leave it for the Track-2 holistic R/2 historic-rot cleanup you proposed. Logged.
>
> **Re-run R/2:**
> ```bash
> git pull   # head dcb5c1d
> Rscript R/probe_r2_5_2_vec.R
> RUN_R2_RUN3=1 RUN_R2_RUN5_3=1 RUN_R2_RUN5_2=1 REBAKE_SCENARIO=historic Rscript R/2_calculate_haz_freq.R
> ```
> Verify: §5.2 writes `historic_ENSEMBLEmean_1995-2014_<combo>` (+ sd), §5.3 completes all crops (no n=0 halt), 0 `_1981_2014`/future touched. Then R/3 → publish → CR-068.
>
> ---
>
> ⛔ **CGLABS (2026-06-29 #3): threshold swap WORKED — §1 classifies NDWS, §2.1+§5.2-main pass. Now blocked at §5.3: §5.2 never ensembles HISTORIC interaction combos (L1554 gate). MACBOOK fix needed (4th historic-path bug).**
>
> Re-ran R/2 (head c751f07). Progress this time:
> - §1 Classify **149s** (vs 12s) — now classifies NDWS for annual/jagermeyr ✓
> - §2 freq + §2.1 ensemble ✓ — historic NDWS freq complete (18 GCM + ENSEMBLEmean)
> - §5.2 **main** ✓ — wrote all **324 per-GCM** historic NDWS interaction stacks (18 combos × 18 GCM) in `haz_time_int_dir`, full threshold codes (`NDWS-G15+THI-max-G71+NDWL0-G2`, …)
> - §5.2 **ensemble** ✗ — **0** historic ENSEMBLEmean/sd combo stacks
> - §5.3 then halted at crop 39/sheep-tropical: `5.3) there should be 1 interaction stacks | m=7/ENSEMBLEmean, but n=0`
>
> **Root cause — `R/2 L1554`:**
> ```r
> if (scenario_choice != "historic" & do_ensemble5.2) {   # ← §5.2 interaction-ensemble SKIPS historic
>   ...                                                    #   writes <scen>_ENSEMBLEmean_<time>_<combo>.tif + _ENSEMBLEsd
> }
> ```
> So historic per-combo ENSEMBLEmean/sd interaction stacks are never built — but **§5.3 (L1716 "one interaction stack for historic timeframe") iterates m=1..20 incl. m=7/ENSEMBLEmean and demands each exists** → n=0 → halt. (It's the only `scenario_choice != "historic"` gate in the file.) Live Atlas baseline shows historic ENSEMBLE exposure, so the historic per-combo ensemble IS needed downstream — §4's ensemble (L901/L1256) is per-*hazard* (NDWS, THI) into `haz_mean_dir`, NOT the per-*combo* interaction §5.3 reads from `haz_time_int_dir`. Different product; no overlap.
>
> **RECOMMENDED FIX (A):** drop the historic exclusion at L1554 → `if (do_ensemble5.2) {`. Then §5.2 writes `historic_ENSEMBLEmean_1995-2014_<combo>.tif` (+ _ENSEMBLEsd) from the 324 per-GCM stacks, and §5.3 finds them. (overwrite5.2=FALSE + the deleted historic combos = it builds only the missing historic ensembles; future already has them.)
> **ALT (B):** if historic ensemble was deliberately excluded, then §5.3 must construct/skip ENSEMBLEmean for historic instead — but that contradicts the live baseline having historic ensemble exposure. A is correct.
>
> **⚠️ META (please read):** this is the **4th** sequential bug surfaced on the historic NDWS re-bake (ClusterRegistry → triple-historic naming → annual threshold swap → now historic ensemble gate). R/2's historic path has clearly rotted since the 2025-08-15 bake (all pre-existing historic class/int/ENSEMBLE files date to then). Rather than one-at-a-time, recommend a **holistic pass over R/2's `scenario=="historic"` branches** (§5.2 ensemble, §5.3 historic stack-count logic, any other historic special-cases) so the next re-run doesn't hit a 5th. I'll keep driving from cglabs either way.
>
> **cglabs state:** §1/§2/§2.1/§5.2-main outputs are GOOD and kept (324 per-GCM historic NDWS interaction stacks complete). Cleaned the 28 partial/`.ovr.tmp` hazard_risk NDWS files from the crashed §5.3. 0 future touched (max future mtime still 2026-05-28). Holding for the L1554 fix.
>
> ---
>
> ✅ **MACBOOK (2026-06-29, commit `4fe8e5a`): FIX A applied — threshold branch swapped. Re-run R/2.**
> Picked **FIX A** (swap branches), and it's not a judgment call — the branch was inconsistent with its own three siblings. The condition `annual_season_subset==TRUE && grepl("sos", timeframe)` means "sos-seasonal → restrict to short crops" everywhere else:
> - L991: sos → `crops[crops %in% short_crops]`
> - L1404: sos → `combinations_ss`
> - L1657: sos → `combinations_crops_ss` / `combinations_ca_ss`
> All three pair **sos → the `_ss` short variant**. Only L714-718 was flipped (sos→full `Thresholds_U`, annual→short `Thresholds_U_ss`). Swapped it so sos→`Thresholds_U_ss`, annual/jagermeyr→`Thresholds_U` (full, incl. NDWS/NDWL0/THI_max/PTOT_L). Matches the 2025-08-15 working state.
> **Your open Q (code branch vs remote `haz_class`):** it's the **code branch**, not `haz_class`. `Thresholds_U` already contains NDWS (you verified `NDWS %in% Thresholds_U = TRUE`), so `haz_class` is fine — the table was just being selected backwards for annual/jagermeyr. No `haz_class` change needed. (Didn't use FIX B — the `_ss` construction at L527 is correct for its intended sos use; only the selection was wrong.)
> Blame was masked by the 2026-05-15 auto-format commit, but the 3-sibling consistency is conclusive.
> **Re-run R/2:**
> ```bash
> git pull   # head 4fe8e5a
> # scoped NDWS still pre-deleted from last attempt — no re-delete needed unless you wrote any
> Rscript R/probe_r2_5_2_vec.R
> RUN_R2_RUN3=1 RUN_R2_RUN5_3=1 RUN_R2_RUN5_2=1 REBAKE_SCENARIO=historic Rscript R/2_calculate_haz_freq.R
> ```
> Verify: §1 now classifies historic NDWS for annual+jagermeyr (NDWS class files written), §5.2 combo `NDWS+THI-max+NDWL0` finds all layers (no NA halt), names clean 4-token, 0 `_1981_2014`/future touched. Then R/3 → publish → CR-068.
>
> ---
>
> ⛔ **CGLABS (2026-06-29): naming patch WORKS — but R/2 now blocked on a 2nd, deeper bug: NDWS is dropped from the annual/jagermeyr threshold table → §5.2 interactions crash. MACBOOK fix needed.**
>
> Re-ran R/2 (patched d44af0b): §1→§2→**§2.1 ensemble now PASSES** (naming fix confirmed; all 62230 freq files 4-token, 0 triple-historic). Then §5.2 Interactions crashed at combo 1/132:
> ```
> Error in if (all(i)) return(x) : missing value where TRUE/FALSE needed   # furrr swallowing a worker NA
> ```
> **Root cause (NOT naming):** §1 classified **0** historic NDWS this run → no NDWS freq → §5.2 combo `NDWS+THI-max+NDWL0` reads a missing NDWS layer → NA → `all(i)` NA → halt.
> Why 0: for **annual & jagermeyr** timeframes §1 uses `Thresholds_U_ss` (L717), built with a
> `crop %in% short_crops` filter (L527). **All 33 NDWS rows in `haz_class` are long-crop (`is_short=FALSE`)**,
> so NDWS — *and* NDWL0 / THI_max / PTOT_L — are dropped. `Thresholds_U_ss$code2` is only `NTx*_mean + PTOT_sum`.
> Verified: `NDWS %in% Thresholds_U_ss = FALSE`, `%in% Thresholds_U = TRUE`.
> Existing NDWS/NDWL0/THI class files all date **2025-08-15** (the last good bake) — so this regressed since; current code can't reproduce them.
>
> **The branch select at L714-718 looks SWAPPED:**
> ```r
> if (annual_season_subset == TRUE && grepl("sos", timeframe)) {
>   thresholds <- copy(Thresholds_U)      # sos-seasonal gets the FULL table…
> } else {
>   thresholds <- copy(Thresholds_U_ss)   # …annual/jagermeyr get the SHORT subset (no NDWS) — backwards
> }
> ```
> **RECOMMENDED FIX (A):** swap the branches — annual/jagermeyr (non-sos) → `Thresholds_U`; sos-seasonal → `Thresholds_U_ss`. Confirmed `Thresholds_U` covers NDWS + NDWL0 + THI_max + PTOT + NTx (all `interaction_haz`). Matches the 2025-08-15 working state + the `annual_season_subset` comment ("restrict to short crops *for seasonal* analysis").
> **ALT FIX (B):** keep the branch, but build `Thresholds_U_ss` to retain interaction hazards regardless of crop:
> `haz_class[(crop %in% short_crops | index_name %in% interaction_haz) & description != "No significant stress", …]`.
> **Open Q for macbook:** confirm whether the regression is this code branch vs the remote `haz_class` (NDWS flipped to long-crop-only) since 2025-08-15. Pick A or B and push.
>
> **cglabs state:** no junk written (0 files today, 0 triple-historic), 0 future touched (max future mtime still 2026-05-28). The 2268 scoped NDWS files stay pre-deleted; hazard_risk NDWS `_int` (1320, intermediate only — not the S3 product, and were saturated) are missing pending a successful rebuild. Holding for the fix.
>
> ---
>
> ✅ **MACBOOK (2026-06-29, commit `d44af0b`): R/2 §1 NAMING BUG FIXED + R/1 ClusterRegistry RATIFIED. Re-run R/2.**
>
> **R/1 `ClusterRegistry` (`44ec9e7`) — RATIFIED.** Diff is correct: `future:::ClusterRegistry("stop")` was removed in future ≥1.40 and `plan(sequential)` already stops workers, so wrapping in `tryCatch(..., error=function(e) NULL)` is a safe no-op on new future / harmless on old. R/1 COMPLETE 18/18 both axes (annual [20.91,21.66], jagermeyr [20.38,21.49], 0 sat, 0 future) — keep it.
>
> **R/2 §1 naming bug — PATCH APPLIED (your spec, exactly):**
> 1. **Both sites** (L755 §1 class-save + L1150 §4 ensemble): `"historic_historic_historic_"` → `"historic_"`.
> 2. **L755 site:** replaced the 4 hardcoded future-period gsubs with one general rule `gsub("_([0-9]{4})_([0-9]{4})_", "_\\1-\\2_", file_name)` — hyphenates historic 1995_2014 **and** all future windows. (Site 2 already had the general rule at L1145, so only its scenario-triple needed fixing.)
> 3. **`_1981_2014` excluded** at BOTH `haz_timeseries_dir` globs — §1 L711 **and** §4 L1133 (`files <- files[!grepl("_1981_2014", files)]`) — so only the `_1995_2014` baseline rebuilds; Track-2 left alone.
> Verified transform: `historical_ACCESS-CM2_1995_2014_NDWS-mean.tif` → `historic_ACCESS-CM2_1995-2014_NDWS-mean.tif` (clean 4-token `scenario_model_timeframe_hazard`).
>
> **Re-run R/2 (Step 4b stage 2):**
> ```bash
> git pull   # head d44af0b
> # re-pre-delete any scoped NDWS leftovers in haz_timeseries_{class,risk,int} + NDWS _int in hazard_risk
> Rscript R/probe_r2_5_2_vec.R       # terra-probe first
> RUN_R2_RUN3=1 RUN_R2_RUN5_3=1 RUN_R2_RUN5_2=1 REBAKE_SCENARIO=historic Rscript R/2_calculate_haz_freq.R
> ```
> Verify: class names are clean `historic_<model>_1995-2014_<haz>` (4-token, no `historic_historic_`), §2.1 ensemble rbindlist no longer crashes, 0 `_1981_2014` touched, 0 future mtimes. Then R/3 → publish → CR-068.
>
> ---
>
> ✅ **MACBOOK (2026-06-28, commit `e1ba945`): R/1 SCOPE FIXED — drop `_1981_2014`+`SPI`, process all 18 GCMs. Re-run R/1.**
> Both your scope findings addressed in `R/1_make_timeseries.R` L225/L230:
> 1. **`_1981_2014` excluded** (+ stray `SPI`) from the folder scan — it's the incomplete Track-2 trends window; live baseline = `_1995_2014`. Crash gone (R/1 no longer reads the missing MPI `_1981_2014` NTx40). Re-enable when Track-2 rebuilds the full 1981_2014.
> 2. **5-GCM dev limit DISABLED** (now commented). You read L231 right — it was inverted (`!grepl` DROPPED the 5, kept 13). A publishable NDWS re-bake needs ALL 18 (bridge propagated 18/18; you pre-deleted all 36 historic NDWS timeseries). Left commented for quick dev runs.
> Net: R/1 now does 18 GCMs × `_1995_2014` historic + future. overwrite=FALSE → it rebuilds ONLY the 36 absent (pre-deleted) historic NDWS timeseries; all else (incl. future, non-NDWS) is skipped. If any `_1995_2014`/future folder in `indices_dir` (ID) is incomplete, the period-aware gate now PRINTS the offending folder×hazard rows before stopping — paste them if it halts.
> **Re-run:**
> ```bash
> git pull   # head e1ba945
> Rscript R/1_make_timeseries.R     # finishes the 36 historic NDWS timeseries (13/36 already done)
> ```
> Confirm `indices_dir2/{annual,jagermeyr}` historic NDWS de-saturated for all 18 GCMs, then Step 4b (R/2 → R/3 → publish → CR-068).
>
> ---
>
> ⛔ **CGLABS (2026-06-28): R/1 now passes the gate + NDWS rebuild WORKS, but R/1 processes the incomplete `_1981_2014` window → halts. Two more R/1 scope fixes needed.**
> Progress with the period-aware gate (8ebf877): gate passes ("8640 rows"), R/1
> started rebuilding, **ACCESS-CM2 `_1995_2014` historic NDWS rebuilt = mean 21.36
> (de-saturated ✓)**, 13/36 NDWS done, **0 future touched**. Then halted:
> ```
> Error: [rast] file does not exist:
>   .../indices_seasonal/annual/historical_MPI-ESM1-2-LR_1981_2014_NTx40_mean.tif
> ```
> Causes (both R/1 scope, macbook):
> 1. **R/1 processes the `_1981_2014` window**, which is **incomplete** in
>    indices_dir2: 308 `_mean.tif` vs `_1995_2014`'s 672; `_1981_2014` NTx40 exists
>    for only 8 GCMs (MPI-ESM1-2-LR absent) → R/1 reads a missing one → crash. Per
>    your own "`_1981_2014` = leave alone (Track-2)", **add `_1981_2014` to the L225
>    folder exclusion** so R/1 only does `_1995_2014` (my NDWS target) + future.
> 2. **L231 looks INVERTED:** `gcms <- c(MRI-ESM2-0,ACCESS-ESM1-5,MPI-ESM1-2-HR,
>    EC-Earth3,INM-CM5-0); folders <- folders[!grepl(gcms, folders)]` — comment says
>    "limit to 5 atlas gcms" but it REMOVES those 5 (processes the other 13). Confirm
>    intent (the failing MPI-ESM1-2-LR is one of the 13 it keeps).
> Also: I moved an **empty stray `SPI/` dir** out of `indices_dir` (it tripped the
> gate as a 0-tif folder) → `atlas_nex-gddp_hazards/cmip6/SPI.stray-empty-bak`.
> Add `SPI` to L225 exclusion (or handle file_n==0) so it's permanent.
> **State:** 13/36 `_1995_2014` historic NDWS rebuilt (de-saturated); rest pending a
> clean R/1 run; future untouched; tree clean. Re-dispatch with the `_1981_2014`
> exclusion (+ SPI) and I finish R/1 → R/2 → R/3 → publish → CR-068.
>
> ✅ **MACBOOK (2026-06-28, commit `8ebf877`): R/1 GATE FIXED — period-aware completeness check. Re-run R/1.**
> Made `R/1_make_timeseries.R:377` derive valid tif counts per-folder from the trailing `_YYYY_YYYY` window instead of the hardcoded `{20,240}`: annual = n_years, monthly = n_years*12. So `_1995_2014` → {20,240}, `_1981_2014` → {34,408} (your 408 now passes), future windows → {20,240}, no-period folders fall back to {20,240}. Unit-checked all four cases. It also now PRINTS the offending folder×hazard rows before `stop()` (was an opaque message). `_1981_2014` is validated as complete (408 = 34yr×12mo), so it passes untouched — no data change, just the gate.
> **Re-run from where you stopped:**
> ```bash
> git pull   # head 8ebf877
> # the 36 indices_dir2 historic NDWS timeseries you pre-deleted are still absent — R/1 rebuilds them now
> Rscript R/1_make_timeseries.R
> ```
> R/1 overwrite=FALSE in-code → it rebuilds ONLY the absent (pre-deleted `_1995_2014` historic NDWS) timeseries; `_1981_2014` + everything else already-present is skipped. Confirm `indices_dir2/{annual,jagermeyr}` historic NDWS are de-saturated, then continue Step 4b (R/2 → R/3 → publish → CR-068). No future touched (no `historic` token).
>
> ---
>
> ⛔⛔ **CGLABS (2026-06-27): STOP — the NDWS fix is in a DIFFERENT index store than R/1 reads. Pre-delete recipe would rebuild from STALE data. No deletes/runs done.**
> Path check (per the recipe's "fixed NDWS index tifs are in indices_dir") — they are NOT:
> - **FX = `nex-gddp-cmip6_indices/historical_<gcm>/NDWS/`** (the hazards_upstream
>   04 producer = WHERE I re-baked): `NDWS-1995-07` mean = **22.35** (fixed ✓).
> - **ID = `indices_dir` = `atlas_nex-gddp_hazards/cmip6/indices/historical_<gcm>_<period>/NDWS/`**
>   (what **R/1 actually reads**): `historical_ACCESS-CM2_1995_2014/NDWS/NDWS-1995-07`
>   = **29.89** (STILL SATURATED).
> Two separate stores, different layouts (FX `historical_<gcm>`; ID
> `historical_<gcm>_{1981_2014,1995_2014}`). The Track-1 fast_calc re-bake fixed the
> **upstream** store (FX); the **prototype** R/1→R/2→R/3 chain consumes ID, which is
> untouched/saturated. So `find ID -name '*historic*' -delete` + R/1 would rebuild
> the timeseries from the **stale 29.89** → fix never propagates, and ID historic
> destroyed for nothing. **Halted before any delete.**
>
> **DECISION NEEDED (macbook) — how should the fix reach R/1's store (ID)?**
> 1. **Re-run the NDWS fix targeting ID** (point fast_calc/04 at `atlas_nex-gddp_hazards/cmip6/indices`, layout `historical_<gcm>_<period>`), OR
> 2. **Sync** the fixed FX historic NDWS → ID (`historical_<gcm>/NDWS` → `historical_<gcm>_1995_2014/NDWS`; also `_1981_2014`?), OR
> 3. **Re-point R/1's `indices_dir` → FX** (if FX is meant to supersede ID).
> Also clarify: ID has BOTH `_1981_2014` and `_1995_2014` period dirs — which does
> the live chain use? And is FX (hazards_upstream) intended to replace ID, or is ID
> the canonical prototype store? Confirm, then I delete ID-historic → R/1→R/2→R/3.
> (Nothing written/deleted; tree clean.)
>
> ✅ **MACBOOK (2026-06-27, commit `fb17ce9`): RECIPE FIXED — pre-delete + overwrite=FALSE (NOT overwrite=TRUE). Re-dispatch below.**
> Your dry-run was exactly right. I considered overwrite=TRUE-under-REBAKE but it fails UNSAFE (one missed input wrap → re-ships future to prod), so the recipe is **pre-delete the stale HISTORIC outputs + run overwrite=FALSE**: `file.exists` rebuilds exactly the deleted (historic) files and skips everything else (future). `REBAKE_SCENARIO=historic` stays as an input-filter belt (also wrapped the §2 L799 class list I'd missed).
> **Simplest + robust: delete ALL historic outputs, rebuild all historic** (don't try to isolate just NDWS — in `_int` compounds NDWS is renamed "dry", so NDWS-specific deletion is fragile; rebuilding all historic is consistent + future stays untouched since it has no `historic` token):
> ```bash
> git pull   # head fb17ce9
> # R/1: rebuild historic timeseries from fixed NDWS indices
> find <indices_dir2>/{annual,jagermeyr} -name '*historic*NDWS*' -delete    # (or all *historic* if simpler)
> Rscript R/1_make_timeseries.R
> # R/2: delete all historic outputs, rebuild (overwrite=FALSE protects future)
> for d in hazard_timeseries_class hazard_timeseries_risk hazard_timeseries_int hazard_risk; do
>   find <Data>/$d -name '*historic*' -delete; done
> REBAKE_SCENARIO=historic RUN_R2_RUN3=1 RUN_R2_RUN5_3=1 RUN_R2_RUN5_2=1 Rscript R/2_calculate_haz_freq.R
> # R/3: delete historic vop/_int outputs, rebuild
> for d in hazard_risk_vop hazard_risk_vop_usd; do find <Data>/$d -name '*historic*' -delete; done
> REBAKE_SCENARIO=historic Rscript R/3_freq_x_exposure.R
> ```
> Dry-run check still applies: snapshot future mtimes at T0, confirm 0 future files written after each stage. Then CR-068 probes (AGO) → publish. (overwrite vars are back to nzchar(FORCE); REBAKE_SCENARIO no longer forces overwrite.)
>
> ⛔ **CGLABS DRY-RUN (2026-06-27): R/2 step won't PROPAGATE the fix — `REBAKE_SCENARIO` scopes inputs but `overwrite=FALSE` SKIPS all existing historic outputs. Needs pre-delete (like R/1). STOPPED before R/1→R/2→R/3.**
> Dry-run: `REBAKE_SCENARIO=historic_ACCESS-CM2 RUN_R2_RUN3=1 RUN_R2_RUN5_3=1
> RUN_R2_RUN5_2=1` (FORCE unset), 1-GCM scope via the token.
> - ✅ **Zero future touched** — snapshot at T0 then checked all 4 R/2 out-dirs
>   (`hazard_timeseries_{class,risk,int}`, `hazard_risk`): **0 ssp* files written.**
> - ❌ **But 0 HISTORIC written too.** §1 Classify "DONE in 14.6s", §3 wrote nothing.
>   Cause: every stage gates on `!file.exists(x) | overwriteN` and `overwriteN =
>   .force_overwrite_r2 = FALSE` (lines 757/818/1025…). The historic NDWS-derived
>   class/freq/stack/`_int` files **already exist → skipped → never rebuilt from the
>   R/1-refreshed NDWS timeseries.** So the fix does NOT propagate.
> - Minor: §3's model loop (`models <- unique(...)`) is NOT `.rebake_scope`-filtered
>   — it iterated all 20 models (harmless: all skipped, just slow).
>
> **FIX NEEDED (macbook) before re-dispatch — mirror R/1's pre-delete for R/2:**
> the `REBAKE_SCENARIO` token-filter alone can't help while `overwrite=FALSE`. Either
> (a) **pre-delete the stale HISTORIC NDWS-dependent outputs** so `overwrite=FALSE`
> rebuilds exactly them — i.e. historic NDWS class files in `hazard_timeseries_class`
> (token `historic` + `NDWS`), the historic NDWS freq in `hazard_timeseries_risk`,
> AND the historic `_int` compounds that INCLUDE NDWS (the §5.3/`hazard_risk` stacks
> + `hazard_timeseries_int`; note NDWS reaches R/3 only via `_int` per R/3:1175/1188)
> — leaving non-NDWS + future intact; OR (b) add an **overwrite-scoped-to-REBAKE_SCENARIO**
> mode (force-rewrite only files matching the token). (a) is the clean mirror of the
> R/1 recipe but needs the exact NDWS-dependent file set enumerated. Also scope §3's
> model loop, or rely on the pre-delete + overwrite-false to no-op the rest.
> Nothing written this run (all skipped); future untouched; tree clean (env-only).
>
> 📤 **MACBOOK (2026-06-27): PUBLISH = targeted downstream R/2→R/3 rebake, HISTORIC timeframe (Pete's call).**
> The live Atlas serves `hazard_exposure` parquets, so flow the fixed NDWS through the consumer pipeline per **`R/NEXT_FULL_REBAKE.md`** — its #1 gate (hazards#19 historic NDWS saturation) is **now CLEARED** (historic NDWS de-saturated, 18/18 validated). Scope:
> - **HISTORIC timeframe only.** Future NDWS is still inflated (deferred to Track 2), so re-baking future downstream now would just re-ship inflated future drought — skip it.
> - **⚠️ START AT R/1, not R/2.** The chain is indices → **R/1_make_timeseries** (reads `indices_dir`, writes `indices_dir2`) → R/2 (reads `indices_dir2`). The fixed NDWS index tifs are in `indices_dir`; **R/2 reads the R/1-built timeseries in `indices_dir2`** — so R/1 must REBUILD the NDWS (historic) timeseries first, else R/2 consumes the stale saturated one and the fix never propagates. Run `R/1_make_timeseries.R` (FORCE/overwrite the NDWS historic timeseries) → confirm `indices_dir2/<historic>/` NDWS tifs are the de-saturated ones. (R/1.2/1.3 are isimip/cropsuite — not NDWS, skip.)
> - **Scoping = `REBAKE_SCENARIO=historic` env-filter (added `7c0cd3b`), NOT pre-delete / NOT FORCE_OVERWRITE.** R/2+R/3 now filter processing inputs to files matching the token (no-op when unset). FORCE_OVERWRITE=1 would rebuild ALL incl. still-inflated future (no native timeframe filter) → don't use it. Run each stage with `REBAKE_SCENARIO=historic`.
>   - Concrete dirs: indices_dir=`atlas_nex-gddp_hazards/cmip6/indices`; indices_dir2=`.../indices_seasonal`; hazard_risk=`Data/hazard_risk/<tf>`; R/3 OUT (=live product)=`Data/hazard_risk_vop[_usd]/<tf>` parquets.
> - **⚠️ DRY-RUN VALIDATE the filter first** (tiny scope, e.g. one GCM): run R/2 with `REBAKE_SCENARIO=historic` + logging, confirm it reads/writes ONLY `historic` files (zero future touched). The filter is applied at the input lists I could identify; a dry-run confirms no chokepoint was missed before the real bake.
> - **R/1** (no env-filter; it's `overwrite=FALSE`-gated): **delete the historic NDWS files in `indices_dir2/{annual,jagermeyr}`** (token `historic` + `NDWS`), then `Rscript R/1_make_timeseries.R` — overwrite=FALSE rebuilds only the deleted (historic NDWS) timeseries from the fixed index tifs, leaving everything else intact. (R/1's L174 list.files is dead `if(FALSE)` QAQC; its live reader is overwrite-gated, so pre-delete is the clean scoping here.)
> - **R/2**: `REBAKE_SCENARIO=historic RUN_R2_RUN3=1 RUN_R2_RUN5_3=1 RUN_R2_RUN5_2=1 Rscript R/2_calculate_haz_freq.R` (FORCE unset). Toggles enable §3/§5.3 (NOT enabled by FORCE). NDWS reaches R/3 only via `_int` compounds (R/3:1175,1188) — the filter keeps historic `_int` incl. NDWS combos. Run `Rscript R/probe_r2_5_2_vec.R` first.
> - **R/3**: `REBAKE_SCENARIO=historic Rscript R/3_freq_x_exposure.R` (FORCE unset). §4.1+§4.2, vop+vop_usd, both axes.
> - **R/3.1 — SKIP (confirmed no-op)**: computes value_adj (L212-224) but never writes (loop + legacy section write nothing). No persisted product. (Flag if season-weighting was meant to be live.)
> - **Preconditions**: **#10 delta-method exposure = DEFERRED INDEFINITELY (Pete) — do NOT run; keep existing MapSPAM-2020/GLW4-2020 exposure.** CR-115 `aggregate_disputedRegions` UNWIRED (zero calls) → dup adm0 rows persist by design, no action. Pattern-B #12 denominator external → not closed by this. Riding items (#13 poultry, CR-093) carry along, fine.
> - **Post-bake CR-068 probes** (AGO): `probe_no_hazard_arithmetic_quick.sh` + `probe_cross_parquet_vop_drift.sh` — expect ratios ≤100%, `hazard='none'` rows present, **Luanda NaN now → 0** (hazards#19 was the residual). 
> - **Publish** the hazard_exposure/hazard_risk family + log to Brayden (data-management#2). This closes CR-068 (b)+(c).
> Confirm exposure vintage + CR-115 state, then execute per the checklist.
>
> ✅✅ **CGLABS — HISTORIC NDWS RE-BAKE COMPLETE + VALIDATED (2026-06-27). Publish path needs confirming before Step 4.**
> - **wbkernel installed + equivalence-checked:** kernel ACCESS-CM2 1995 = **21.859**
>   == R-loop 21.86 (kernel ≡ R-loop + classify fix). (Note: the dispatch's
>   `MONTHS=1:12` is the wrong format — cfg_months wants a comma list; `1:12` →
>   NA → bad date. Omit MONTHS, or pass `1,2,…,12`.)
> - **Parallel kernel bake DONE:** all **18/18 GCM, exit 0** (`xargs -P 12`,
>   `SCENARIO=historical YRS=1995:2014 FORCE`), ~2.6 h wall (12 cores, ~6.6 GB/proc).
> - **Validation — SATURATION CLEARED:** every historic GCM mean NDWS now
>   **24.5-24.8** (was ~29.2), **0/18 still saturated (>27)**. Tight + consistent
>   (KACE 24.12 … ACCESS-ESM1-5 24.76). (Continental mean ~24.6 mixes deserts ~31
>   + wet regions; the per-pixel rainforest saturation is gone.)
> - NDWL0/NDWL50 untouched (already normal). Future NDWS untouched (Track 2, per Pete).
>
> **⛔ Step 4 PUBLISH — held for a path decision (outward-facing, production).**
> `07_bucket_uploads/upload_AWS.R` is a generic folder→S3 helper; the live Atlas
> `hazard_exposure` historic panel is a DOWNSTREAM product (NDWS → R/2 classify→
> freq → R/3 exposure→parquet), not the raw index tifs. So "publish historic NDWS"
> is ambiguous — it's either (a) a downstream **R/2→R/3 re-bake** of NDWS-dependent
> products + publish their parquets, or (b) a direct NDWS-indices S3 publish if the
> Atlas reads those. I won't push to production blind. **Confirm which path** (and
> whether the downstream re-bake is in Track-1 scope) → I'll execute it.
> Local fixed NDWS index tifs are ready under `nex-gddp-cmip6_indices/historical_*/NDWS/`.
>
> ⚡ **MACBOOK (2026-06-27, commit `0dc374a`): NDWS speedup ready — Rcpp eabyep kernel + GCM-level parallelism. Validate then relaunch.**
> The slow ~20s/mo is the R eabyep loop (~30 terra ops/mo). `fast_calc_NDWS.R` now
> calls `wbkernel::eabyep_kernel_cpp` (single C++ pass, EXACT replica of legacy
> eabyep, validated). Combined with process-level parallelism across GCMs, the
> 24h drops to well under an hour.
> ```bash
> git pull   # head 0dc374a
> R CMD INSTALL wbkernel          # kernel changed - REINSTALL (each per-GCM process library()-loads it)
> # 1) EQUIVALENCE CHECK before the 18-GCM bake: kernel result must match the
> #    R-loop re-bake you already did (ACCESS-CM2 1995 continental mean ~21.86):
> SCENARIO=historical YRS=1995:1995 MONTHS=1:12 FORCE_OVERWRITE=1 GCMS=ACCESS-CM2 \
>   Rscript 04_indices/fast_calc_NDWS.R
> #    -> re-check the 1995 continental mean; expect ~21.86 (kernel == R loop + classify fix).
> # 2) If it matches, launch the PARALLEL + kernel bake (process-level, NOT furrr):
> printf '%s\n' $GCMS_ALL | xargs -P 12 -I{} sh -c \
>   'SCENARIO=historical YRS=1995:2014 FORCE_OVERWRITE=1 GCMS={} Rscript 04_indices/fast_calc_NDWS.R > /tmp/ndws_{}.log 2>&1'
> ```
> `-P 12` (tune to free cores/RAM; ~6.6 GB/proc). NDWL0/NDWL50 unchanged (R loop, not re-baked).
> **STOP the current single-core serial run first** (it's the slow R-loop, ~24h; the kernel+parallel
> redo is <1h — no reason to wait it out), then run the equivalence-check + parallel bake above.
> FORCE_OVERWRITE re-does everything cleanly, so partial progress from the killed run is fine.
>
> 🟢 **MACBOOK (2026-06-27): future NDWS DEFERRED to the Track-2 rebake — do NOT run the legacy future re-bake.** (Pete) The multi-week legacy future re-bake would be thrown away by Track 2 (FAO-56/AquaCrop recomputes future NDWS correctly), so skip it. Track 1 = **historic only**: finish the 18-GCM historic bake → full-GCM validation (all ~22, none ~29) → **publish historic NDWS**. Live future NDWS stays mildly inflated until Track 2 lands. NDWL0/NDWL50: leave (normal).
>
> ✅ **CGLABS — FIX VALIDATED + historical re-bake LAUNCHED (2026-06-27, HEAD bfa4372).**
> - **Fix confirmed:** `fast_calc_NDWS.R:210` now `NDWS <- sum(ERATIO < 0.5)`;
>   cfg_yrs default `:226` now 1995:2014 (seed-aligned). 
> - **Validated:** re-baked ACCESS-CM2 historic 1995 (FORCE) → continental mean
>   NDWS **29.29 → 21.86**, i.e. now ≈ future (~22) = saturation CLEARED. (The
>   ~6-8/mo figure is wet-cells-only; the continental mean ~22 correctly mixes
>   deserts ~31 + wet regions, and matches the future spread.)
> - **NDWL0 / NDWL50 are NOT saturated** — checked ACCESS-CM2 1995/2005/2014:
>   NDWL0 ~1.8/mo, NDWL50 ~0.03/mo (both normal). They use correct boolean sums.
>   **Leave them — no re-bake needed** (the dispatch's original "trio saturated"
>   was wrong; only NDWS was, via the classify bug).
> - **Historical NDWS re-bake LAUNCHED:** all 18 GCM, `SCENARIO=historical
>   YRS=1995:2014 FORCE`, background (~20s/mo → ~24h for 18 GCM). Status file
>   `/tmp/ndws_hist_DONE.txt` on completion.
>
> **STILL PENDING (next session / when the bake lands):**
> 1. **Publish historic NDWS** — only after the 18-GCM re-bake completes + a
>    full-GCM validation pass (re-run the per-GCM mean check; expect all ~22, none ~29).
>    Then push through the production publish path → clears CR-068 (b)+(c) / Luanda NaN.
> 2. **Future NDWS re-bake** — confirmed: the classify inflation hit future too, so
>    future is over-counted (its "~22" is buggy-but-less-inflated). It DOES need
>    re-baking. But future = 18 GCM × 4 SSP × 80 yr (~69k month-computes, ~weeks at
>    20s/mo) — a **scheduled multi-week job**, not interactive. Flag for scheduling +
>    confirm SSP scope before launching.
> Holding publish until the historical bake + full validation are done.
>
> ✅ **MACBOOK — FIX APPLIED (2026-06-27, commit `bafe8c8`). Re-bake NDWS hist + future, then publish.**
> Brilliant catch. `fast_calc_NDWS.R:204-205` classify-sum → replaced with the
> correct boolean count `NDWS <- sum(ERATIO < 0.5)` (matches NDWL0/NDWL50's
> existing `sum(LOGGING>...)`). Proven locally: classify-sum 2.65 vs boolean 1.00.
>
> **Re-bake scope (corrected):**
> - **NDWS: re-bake historical AND future** (the inflation hit both; future was over-counted too, just less). All GCMs.
> - **NDWL0/NDWL50: NOT this bug** (they use correct boolean sums). First **check if they're even saturated** (the per-year read on an NDWL0 dir) — if normal, leave them; if saturated, that's a separate issue to diagnose, not this fix.
> ```bash
> git pull   # head bafe8c8
> SCENARIO=historical FORCE_OVERWRITE=1 Rscript 04_indices/fast_calc_NDWS.R
> SCENARIO=future     FORCE_OVERWRITE=1 Rscript 04_indices/fast_calc_NDWS.R
> # run chronologically from the seed; YRS via env if the :221 default still bites.
> ```
> **Validate:** NDWS should drop from ~29/mo to a normal ~6-8/mo on wet land cells
> (your trajectory got ~7/mo correct). Then publish NDWS (clears CR-068 b+c / Luanda NaN).
> Also fix the `:221` cfg_yrs default (1981:1994) vs `:137` seed (1995-01) contradiction
> so a plain historical run starts at the seed (or always pass YRS).
>
> 🎯🎯 **CGLABS — ROOT CAUSE FOUND (2026-06-27): NDWS `classify` bug, NOT seed/inputs/PET/spin-up.** No re-bake/publish (needs a macbook code fix first).
>
> **(a) per-year trend:** historic NDWS flat ~29 every year (1995=29.29 … 2013=29.28)
> → **NOT spin-up** (no decline). **(b) trajectory:** the repo probe picked the
> global-wettest cell = (144.88,13.38) **Pacific Ocean**, soilcp=NA → NDWS=NA
> (inconclusive), but ET historic 5.58 ≈ future 5.68 → **peest2 cleared**. Re-ran
> on a valid **wet African land cell (9.62,-2.38, soilcp 49.2 mm)**: the
> verbatim-replicated kernel gives **HISTORIC NDWS = 87 days/yr empty-seed, 79
> FC-seed (≈7/mo, NORMAL, and empty≈FC → seed cleared)**; future (drier here) 213/211.
> So algorithm + inputs + seed + PET are ALL fine — yet the **output files are ~29/mo
> (~348/yr)**. The gap is in the script, not the science.
>
> **THE BUG — `fast_calc_NDWS.R:204-205`:**
> ```r
> cvls <- matrix(data = c(-Inf, 0.5, 1), ncol = 3)          # ONE rule: [-Inf,0.5) -> 1
> NDWS <- terra::classify(x = ERATIO, rcl = cvls, right = F) |> sum()
> ```
> `classify` maps eratio<0.5 → 1 but **leaves eratio≥0.5 as its FRACTIONAL value**
> (proven: `classify(c(0.2,0.49,0.5,0.7,0.95))` → `1,1,0.5,0.7,0.95`). So
> `sum()` = count(stressed days) **+ Σ(fraction of every non-stressed day)**, not a
> day-count. A wet month (~7 true stress days + ~24 days × ~0.7) sums to ~27-29 →
> **saturated, worst in wet/low-stress regions (incl rainforest)** — exactly the symptom.
> Historic 29 vs future 22 = future is drier (fewer non-stress days to over-add).
>
> **FIX (macbook code):** zero the non-stressed days, e.g.
> `NDWS <- sum(ERATIO < 0.5)` (or a 2-row rcl mapping ≥0.5 → 0). Then re-bake.
> **⚠️ This inflation hits FUTURE too** — future NDWS is also over-counted (just less,
> being drier), so "future is fine / don't re-bake it" is WRONG: the fix changes
> future as well → re-bake historic AND future NDWS. NDWL0/NDWL50 use a correct
> boolean `sum(LOGGING > sst*0.5)` (no classify bug) — verify their saturation
> separately (may be a different/no issue).
>
> 🧭 **MACBOOK (2026-06-26, commit `3071948`): inputs clean → prime suspect now is the EMPTY-soil seed (spin-up).** Two clues: (1) historic is COOLER (lower PET) so should be LESS stressed, yet has MORE — so it's state/PET, not forcing; (2) the legacy seed `AVAIL=0` = bone-dry soil; in a ~2.4 mm/day climate the soil may never recharge → persistent saturation, while future (measured decades after its 2021 empty seed) has equilibrated. Two checks:
> ```bash
> git pull   # head 3071948
> # (a) CHEAP — is it spin-up? historic mean NDWS per year 1995->2014: declining = spin-up, flat ~29 = persistent
> Rscript -e 'suppressMessages(library(terra)); root<-Sys.getenv("COMMON_DATA")
>   d<-file.path(root,"nex-gddp-cmip6_indices/historical_ACCESS-CM2/NDWS")
>   for(y in seq(1995,2014,3)){f<-list.files(d,sprintf("^NDWS-%d-.*\\.tif$",y),full.names=TRUE)
>     if(length(f))cat(y, round(mean(sapply(f,function(x)mean(values(rast(x)),na.rm=TRUE))),2),"\n")}'
> # (b) DEFINITIVE — single wet-pixel trajectory, empty-seed vs FC-seed, historic vs future:
> COMMON_DATA=$COMMON_DATA GCM=ACCESS-CM2 HYR=1995 FYR=2050 FSSP=ssp245 \
>   Rscript 04_indices/probe_ndws_trajectory.R
> ```
> Tell: if NDWS(empty-seed) ≫ NDWS(FC-seed) and they converge under FC-seed → **empty-soil spin-up; fix = seed at field capacity** (what the v2 kernel already does). If ET(historic) ≫ ET(future) despite cooler temps → peest2 is the culprit. Report both. Still no re-bake/publish.
>
> 🔬🔬 **CGLABS PROBE RESULT (2026-06-26): INPUTS ARE CLEAN — broken-input hypothesis DISPROVEN.** No re-bake/publish.
> `GCM=ACCESS-CM2 HMONTH=1995-07 FMONTH=ssp245:2050-07 probe_ndws_inputs.R`,
> historic-vs-future daily means (all physically sane, near-identical):
> ```
>           HISTORIC 1995-07     FUTURE 2050-07
>   pr      2.39 mm/day (sum 74) 2.68 (sum 83)   <- NOT ~0; units fine
>   rsds    22.88 MJ/m2/day      23.36           <- in 5-30, fine
>   tasmax  23.64 C              26.47
>   tasmin  12.67 C              15.38
>   hurs    69.6 %               68.8 %
> ```
> The expected tell (historic pr≈0 / rsds off) is **ABSENT**. Historic is only ~11%
> drier than future (2.39 vs 2.68 mm/day) — far too small to drive 29 vs 22
> stress-days (~32% more). **So the saturation is NOT input-magnitude-driven.**
> Combined with the earlier finding (re-bake with the deterministic seed STILL gave
> ~29), the driver is neither the AVAIL seed NOR the input forcing magnitudes.
> **Next suspects (macbook, before any re-bake):**
> 1. **The historic NDWS already on disk vs a fresh compute from these clean inputs** —
>    is the ~29 an artifact of the OLD files, while a clean compute from these inputs
>    would give ~22? (My YRS=1995:2014 re-bake gave 29.29 — but worth confirming the
>    water-balance output spatially: is NDWS=31 even in high-rain pixels?)
> 2. **AVAIL/water-balance accumulation** (eabyep_calc): does the soil state diverge
>    historic vs future given near-equal forcing? Check ERATIO/AVAIL for a wet pixel.
> 3. **peest2 PET on historic** vs future for the SAME pixel (probe checked inputs to
>    PET, not PET output) — compare ETMAX magnitudes historic vs future.
> A spatial/ERATIO probe (one wet pixel, historic vs future ETMAX + AVAIL + ERATIO
> trajectory) would isolate it. Holding per "do NOT re-bake/publish until the driver
> is found".
>
> 🔬 **MACBOOK (2026-06-26, commit `fa3ed5a`): agreed — AVAIL seed is NOT the cause. Re-diagnose at the input level first.**
> Your diagnosis is right: future seeds AVAIL=0 identically yet is normal, so the
> historic ~29 saturation is the historic INPUT FORCING, not seeding. NDWS saturated
> on every pixel (incl. rainforest) = impossible from climate → systematic input
> corruption (historic rain ~0, or historic rsds/ET wrong). Keep `a4ba707` (it's a
> valid out-of-order-resilience fix) but it doesn't address THIS bug.
> **Run the input probe** (compares historic vs future pr/rsds/tasmax/tasmin/hurs
> magnitudes for one saturated GCM):
> ```bash
> git pull   # head fa3ed5a
> COMMON_DATA=$COMMON_DATA GCM=ACCESS-CM2 HMONTH=1995-07 FMONTH=ssp245:2050-07 \
>   Rscript 04_indices/probe_ndws_inputs.R
> ```
> Report the historic-vs-future magnitudes. Expected tell: historic **pr near 0**
> (missing *86400 units) or **rsds** off → over-depletion → saturation. That pins
> the real fix (likely re-preprocess the broken historic input, then re-bake NDWS).
> Also reconcile `fast_calc_NDWS.R:221` (cfg_yrs default 1981:1994) vs `:137` seed
> 1995-01 — they contradict; the historic default should be the 1995-2014 baseline.
> **Do NOT re-bake/publish until the input driver is found.**
>
> ⛔ **STOPPED at Step 3 — the AVAIL-seed fix does NOT clear historic saturation. NOT published.** (cglabs 2026-06-26, HEAD 7e2572c)
>
> **Step 1 scope (confirmed):** ALL 18 `historical_*` saturated. NDWS is in
> **days/month (0-31)**, so the one-liner's `m>0.9` flags everything (incl. future)
> — read the raw value: historic ≈ **29.2 days** (29/31 ≈ 0.94 = saturated) vs
> future ≈ **22 days** (22/31 ≈ 0.71 = normal). So: historic saturated, future fine.
>
> **Config blocker hit (the dispatch's Step-2 command FAILS as written):**
> `fast_calc_NDWS.R:221` forces `cfg_yrs(scenario, historical = 1981:1994)`, but the
> seed (`:137`) is hardcoded `1995-01`. With no `YRS`, the loop starts 1981-01,
> isn't the seed → reads prior 1980-12 → `stopifnot` "prior-month AVAIL missing"
> → all 3 abort in ~20s. I aligned to the documented seed with `YRS=1995:2014`
> (env override, no code edit) and re-ran; that got past the seed.
>
> **Step 3 validation — FIX DID NOT TAKE:** re-baked `historical_ACCESS-CM2`
> NDWS with `NDWS_AVAIL_FIX` default-on, `YRS=1995:2014`, FORCE, chronological
> from the 1995-01 (AVAIL=0) seed:
> - 1995 mean = **29.29** (was ~29.2) — unchanged
> - 1996 mean = **28.95** (full spin-up year later) — still saturated, NO drop to ~22.
> Per Step 3 ("if still ~0.95, STOP"), I **stopped the re-bake and did NOT publish**.
>
> **Why the seed fix can't be the cause (diagnosis pointer for macbook/Track 2):**
> future seeds AVAIL=0 the same way (`:137` includes `2021-01`) yet is ~22 (normal).
> Same seed logic, same water-balance/peest2 — so the historic ~29 saturation is
> driven by the **historic input forcing / PET**, NOT the AVAIL seeding. The
> deterministic-seed change (a4ba707) addresses the wrong root cause for this bug.
> Recommend re-diagnosing the historic pr/tasmax/tasmin/rsds (or peest2 on historic)
> before any further re-bake. (Also: reconcile the `:221` 1981:1994 default vs the
> `:137` 1995-01 seed — they contradict.)
>
> **State:** `historical_ACCESS-CM2` 1995-01..1996-04 NDWS/NDWL were FORCE-overwritten
> with new-but-still-saturated values (~29, no functional change vs old); other 17
> GCMs untouched. Live Atlas NOT modified (no publish). No code edited (env-only).

# Dispatch (TRACK 1): fix the saturated HISTORIC NDWS/NDWL, ship to the current Atlas

**Bug (hazards#19, scoped from downstream):** the **historic 1995-2014** NDWS rasters are **saturated — mean/max ≈ 0.95 every pixel/month/year** (nearly always water-stressed); the matching **future** NDWS is **normal (~0.70)**. Cause: on a resume/re-run the historic series re-seeded every month from the lexically-last (dry) `AVAIL`, pinning soil to max depletion. Deterministic prior-month seed (now default, commit `a4ba707`) fixes it. **peest2 PET unchanged; no sfcWind; the FAO-56/AquaCrop overhaul is Track 2.**

**Scope is already known — no probe needed:**
- Affected: **HISTORIC (1995-2014)**, indices **NDWS + NDWL0 + NDWL50** (share the AVAIL chain), **all GCMs**.
- **NOT affected: future (ssp*) — DO NOT re-bake it.** Other 10 indices untouched.

## Pull
```bash
cd <hazards_prototype>/hazards_upstream/R
git checkout develop && git pull        # head a4ba707+; DO NOT create branches
export COMMON_DATA=<your real data root>
```

## Step 1 — confirm scope (cheap, reads published rasters)
Verify the historic saturation across GCMs (expect mean NDWS ≈ 0.95 historic vs ~0.70 future):
```bash
Rscript -e 'suppressMessages(library(terra)); root<-Sys.getenv("COMMON_DATA")
for (g in list.dirs(file.path(root,"nex-gddp-cmip6_indices"),recursive=FALSE)) {
  f<-list.files(file.path(g,"NDWS"),"^NDWS-.*\\.tif$",full.names=TRUE); if(!length(f)) next
  m<-mean(sapply(head(f,12), function(x) mean(values(rast(x)),na.rm=TRUE)))
  cat(basename(g), sprintf("mean NDWS(first12mo)=%.2f%s\n", m, if(m>0.9)"  <- SATURATED" else ""))}'
```
GCMs flagged SATURATED on their `historical_*` series → re-bake; any not saturated → leave.

## Step 2 — re-bake the HISTORIC trio (saturated GCMs)
FORCE re-bake NDWS+NDWL0+NDWL50, **SCENARIO=historical only**, chronological from the 1995-01 seed (deterministic seed reads the prior month — the script loop is already chronological):
```bash
SCENARIO=historical FORCE_OVERWRITE=1 Rscript 04_indices/fast_calc_NDWS.R
SCENARIO=historical FORCE_OVERWRITE=1 Rscript 04_indices/fast_calc_NDWL0.R
SCENARIO=historical FORCE_OVERWRITE=1 Rscript 04_indices/fast_calc_NDWL50.R
# default GCM set; restrict to the saturated GCMs via GCMS=... if Step 1 showed any clean.
# DO NOT run SCENARIO=future (future is fine).
```

## Step 3 — validate the fix cleared it
Re-run Step 1's check: historic mean NDWS should now drop from ~0.95 to a normal spread (~0.6-0.75), comparable to future. If still ~0.95, STOP and report (fix didn't take).

## Step 4 — publish to the current Atlas
Push the re-baked historic NDWS/NDWL0/NDWL50 through the existing NDWS publish path (note the stale upload layer — push_to_s3 legacy path/ACL caveats; use the current production publish step). Confirm the live Atlas `hazard_exposure` historic panel is no longer saturated (clears CR-068 (b)+(c), Luanda NaN).

## Report back (edit this file + commit)
- Step 1 scope: which `historical_*` series were saturated
- Step 3 validation: historic mean NDWS before/after
- Step 4: published + live Atlas confirmed

Track 2 (FAO-56/AquaCrop water balance, FAO-56 PM PET, HSH/WBGT, sfcWind, SPEI, EDDI) = separate later full rebake.

---

> ⛔ **CGLABS Step 4b/R/1 BLOCKED (2026-06-27): the stage-0 completeness gate rejects the `_1981_2014` window. R/1 aborts before any rebuild — macbook gate fix needed.**
> `R/1_make_timeseries.R:377-380`: `incomplete <- folders_x_hazards[!file_n %in%
> c(20,240),]; if(nrow)>0 stop("Check file completeness before continuing")`. It
> scans EVERY `indices_dir/<scenario_gcm_period>/<hazard>` (working_dir=indices_dir,
> L61/223) and requires 20 or 240 tifs. But the **`historical_<gcm>_1981_2014`**
> folders hold **408** tifs (34 yr × 12 mo) for EVERY hazard → not in {20,240} →
> abort, immediately, before rebuilding anything. (240 = the 1995-2014 monthly
> window; 20 = annual; 408 = the longer 1981-2014 trends window the gate omits.)
> - **Pre-existing, NOT my doing:** the `_1981_2014` 408-count folders predate this;
>   the new stage-0 hardening gate just started rejecting them. (My bridge wrote the
>   `_1995_2014/NDWS` at 240 ✓; `_1981_2014` left untouched per your instruction.)
> - **State:** Step 4a OK (ID `_1995_2014` NDWS de-saturated ~21.9, 18/18). I
>   pre-deleted the 36 `indices_dir2` historic NDWS timeseries (per recipe) but R/1
>   aborted before rebuilding them → they're currently ABSENT (will be rebuilt the
>   moment R/1 can run; nothing consumes them mid-rebake). No future touched.
>
> **FIX NEEDED (macbook):** the L377 gate must allow the `_1981_2014` window — either
> add 408 to the allowed set, derive the expected count per-period from the dir name
> (`_YYYY_YYYY` → n_years×12 (+annual 20)), or exclude `_1981_2014` from R/1's
> `folders` scan (you said it's Track-2/trends, leave alone — then it shouldn't gate
> R/1 either). Then I re-run R/1 → R/2 → R/3 → publish → CR-068 probes.
>
> ✅ **CGLABS Step 4a DONE (2026-06-27): FX→ID bridge propagated the fix.** Pre-deleted
> 18 stale `historical_*_1995_2014/NDWS` (kept `_1981_2014`), ran the bridge
> (`SCENARIO=historical BRIDGE_INDICES=NDWS BRIDGE_OVERWRITE=1`, exit 0, all 18
> `copied 240/240`). ID now **de-saturated: 18/18 GCM ~21.9** (was 29.89), 0
> saturated. R/1's store is fixed. → Step 4b (R/1→R/2→R/3→publish→probes) underway.

## DECISION (macbook, 2026-06-27) — re your "how does the fix reach R/1's store (ID)?"

**Answer = your option 2 (sync FX→ID), done via the existing producer bridge** — NOT a re-point, NOT a re-bake-to-ID.

Found the bridge: `hazards_upstream/R/02_preprocess_data/saveNexGDDPindicesFollowing-atlas_hazards-structure.R`. It `file.copy`s FX `nex-gddp-cmip6_indices/<ssp>_<gcm>/<index>` → ID `atlas_nex-gddp_hazards/cmip6/indices/<ssp>_<gcm>_<prd>/<index>`. It was never re-run after the FX fix, so ID is stale. I fixed 3 bugs in it (commit on develop):
1. historic window was hardcoded `1981_2014` → now from `cfg_yrs()` (default **1995:2014**) so it writes `historical_<gcm>_1995_2014` (the live baseline + your fixed FX window).
2. `file.copy` defaulted `overwrite=FALSE` → it would SKIP the stale saturated ID files. Added `BRIDGE_OVERWRITE=1`.
3. Added `BRIDGE_INDICES=NDWS` to scope the index loop (no need to re-copy 30+ indices).

**Period question answered:** live `hazard_exposure` baseline = **`_1995_2014`**. `_1981_2014` = the longer trends/climate-domain window — **leave it alone** for Track 1 (Track-2 territory). R/1 reads ALL ID subdirs (`list.dirs`), so the `_1995_2014` panel feeds the published baseline.

**FX intended to replace ID? No** — FX is the working index store, ID is the consumer-facing reorg. The bridge is the permanent handoff; we just had to re-run it.

### Step 4a — PROPAGATE FX→ID (NDWS historic only)
```bash
cd <hazards_prototype>/hazards_upstream/R
git checkout develop && git pull       # picks up the bridge fix
export COMMON_DATA=<your real data root>
# pre-delete stale ID historic NDWS (clean copy; bridge overwrite also set as belt+braces)
find "$COMMON_DATA/atlas_nex-gddp_hazards/cmip6/indices" -maxdepth 1 -type d \
  -name 'historical_*_1995_2014' -exec rm -rf {}/NDWS \;
# run the bridge, NDWS + historic only
SCENARIO=historical BRIDGE_INDICES=NDWS BRIDGE_OVERWRITE=1 \
  Rscript 02_preprocess_data/saveNexGDDPindicesFollowing-atlas_hazards-structure.R
```
Then confirm ID is de-saturated (the store R/1 actually reads):
```bash
Rscript -e 'suppressMessages(library(terra)); root<-Sys.getenv("COMMON_DATA")
base<-file.path(root,"atlas_nex-gddp_hazards/cmip6/indices")
for (d in list.dirs(base,recursive=FALSE)) {
  if(!grepl("historical_.*_1995_2014$",basename(d))) next
  f<-list.files(file.path(d,"NDWS"),"^NDWS-.*\\.tif$",full.names=TRUE); if(!length(f)) next
  m<-mean(sapply(head(f,12),function(x) mean(values(rast(x)),na.rm=TRUE)))
  cat(basename(d), sprintf("ID mean NDWS=%.2f%s\n", m, if(m>0.9)"  <- STILL SATURATED" else ""))}'
```
Expect de-saturated (~22.35), NOT 29.89. If still saturated → STOP, the FX→ID copy didn't take.

### Step 4b — run the consumer chain (historic-scoped), then publish
Follow RUN SEQUENCE in memory `reference_consumer_chain_ndws_to_atlas` steps 1-6 (R/1 → R/2 → R/3 → s3_upload → CR-068 probes). Do not skip:
- R/1: pre-delete historic NDWS in `indices_dir2/{annual,jagermeyr}` (R/1 overwrite=FALSE in-code), then `Rscript R/1_make_timeseries.R`.
- R/2: FORCE UNSET; pre-delete historic in haz_time_class/risk/int + NDWS-bearing `_int` compounds in hazard_risk, then `RUN_R2_RUN3=1 RUN_R2_RUN5_3=1 RUN_R2_RUN5_2=1 REBAKE_SCENARIO=historic Rscript R/2_calculate_haz_freq.R`.
- R/3: pre-delete stale NDWS `_int` TIFs+parquets in hazard_risk_vop[_usd], then `REBAKE_SCENARIO=historic Rscript R/3_freq_x_exposure.R` (FORCE UNSET).
- Skip R/3.1 (no-op, issue #17).
- Publish: `Rscript R/s3_upload.R annual TRUE TRUE FALSE FALSE TRUE TRUE 10` + jagermeyr.
- CR-068 probes on AGO (project_cr068_post_bake_probes).

### Report back (edit this file + commit)
- Step 4a: ID mean NDWS before/after (expect 29.89 → ~22.35)
- Step 4b: each stage's historic-only counts (confirm 0 future written) + live Atlas confirmed de-saturated

---
> ## CGLABS REPORT (2026-06-28)
>
> **R/1 (Step 4b/stage 1) — COMPLETE.** Both timeframe axes rebuilt + de-saturated:
> - annual:    18/18  NDWS mean range [20.91, 21.66]  sat>27 = 0
> - jagermeyr: 18/18  NDWS mean range [20.38, 21.49]  sat>27 = 0
> - 36/36 historic `_1995_2014` NDWS timeseries rebuilt from fixed ID store. 0 future (`ssp*`) timeseries touched.
>
> **⚠️ CODE FIX (committed `44ec9e7`, FLAGGED for ratification):** R/1 halted twice at the
> annual→jagermeyr param-set boundary with `object 'ClusterRegistry' not found` (Execution halted),
> after annual built but before jagermeyr. Root cause: `future:::ClusterRegistry("stop")` at
> R/1 L217/576/746 — that internal was **removed in future >= 1.40** (box has future 1.67.0,
> `exists("ClusterRegistry", asNamespace("future"))` = FALSE). `plan(sequential)` (called on the
> line above each) already shuts multisession workers, so the call is dead weight. Wrapped each in
> `tryCatch(..., error=function(e) NULL)` → no-op on new future, harmless on old. 3 lines.
> Re-run after fix: clean exit, jagermeyr 18/18. Please ratify the diff.
>
> **NEXT (cglabs):** Step 4b stage 2 — R/2 (pre-delete historic in haz_time_class/risk/int +
> NDWS-bearing `_int` in hazard_risk → `probe_r2_5_2_vec.R` → `RUN_R2_RUN3=1 RUN_R2_RUN5_3=1
> RUN_R2_RUN5_2=1 REBAKE_SCENARIO=historic`) → R/3 → publish → CR-068. Future-mtime guard each stage.

---
> ## CGLABS REPORT (2026-06-29) — R/2 BLOCKED on a §1 naming bug
>
> **R/2 first attempt FAILED.** Ran probe (PASS), pre-deleted 2268 scoped NDWS files
> (class 108 / risk 120 / int 720 / hazard_risk NDWS `_int` 1320 — all asserted ssp-free;
> hazard_risk confirmed historic-baseline-only: 0 ssp tokens across all 12240 files).
> §1 Classify + §2 Frequency ran, then §2.1 ensemble crashed:
> `Error in rbindlist(...): Item 1265 has 7 columns, inconsistent with item 1 which has 4`.
>
> **Root cause — pre-existing R/2 §1 naming bug (commit `832344b`, latent until a historic re-bake):**
> The class save-name block converts period→hyphen **only for future windows** and mangles the
> historic scenario token:
> ```r
> # R/2 L755 (and identical L1150):
> file_name <- gsub("historical_", "historic_historic_historic_", file_name)   # ← scenario TRIPLED; should be "historic_"
> file_name <- gsub("2021_2040_", "2021-2040_", file_name)                     # future periods hyphenated…
> file_name <- gsub("2041_2060_", "2041-2060_", file_name)                     # …but NO 1995_2014_ / 1981_2014_ rule
> ...
> ```
> So historic outputs become `historic_historic_historic_<model>_1995_2014_<haz>` →
> 7 `_`-tokens vs the parser's expected 4 (`scenario_model_timeframe_hazard`, timeframe as a single
> hyphen token) → rbindlist blows up. No reverse-collapse exists anywhere. Future bakes are fine
> (their periods *are* hyphenated), which is why this never surfaced. The mangled name also ≠ the
> existing clean name, so the `overwrite=FALSE` gate didn't catch it → §1 rebuilt **all** historic
> hazards incl. NTx/PTOT **and `_1981_2014`** (Track-2), writing 6240 malformed files.
>
> **Cleanup done:** removed all 6240 malformed `historic_historic_historic_*` files (class 3120 +
> risk 3120; int stage hadn't run), 0 remaining. Pre-existing clean files untouched. No future touched.
>
> **PROPOSED FIX (NOT yet applied — awaiting ratification):**
> 1. L755 + L1150: `"historic_historic_historic_"` → `"historic_"`.
> 2. Replace the four hardcoded future-period gsubs with one general rule:
>    `file_name <- gsub("_([0-9]{4})_([0-9]{4})_", "_\\1-\\2_", file_name)` (handles all windows).
> 3. Scope: exclude `_1981_2014` from the historic re-bake (Track-2 — leave alone), so only the
>    `_1995_2014` baseline is rebuilt.
>
> **DECISION (p.steward, 2026-06-29):** hand the diff to **macbook** (do NOT edit code on cglabs);
> exclude `_1981_2014`. cglabs is holding — will re-run R/2 after pull.
>
> ### ▶ MACBOOK PATCH (R/2_calculate_haz_freq.R) — apply, commit, push to `develop`
> **(a) Fix scenario token — 2 sites:**
> ```
> L755:  - gsub("historical_", "historic_historic_historic_", file_name)
>        + gsub("historical_", "historic_", file_name)
> L1150: - gsub("historical_", "historic_historic_historic_", files_new)
>        + gsub("historical_", "historic_", files_new)
> ```
> **(b) Generalise period→hyphen (covers historic, replaces the 4 hardcoded future gsubs).**
> At both sites, after the scenario line, replace the `2021_2040_`…`2081_2100_` block with:
> ```r
> file_name <- gsub("_([0-9]{4})_([0-9]{4})_", "_\\1-\\2_", file_name)   # L755 site (and files_new at L1150)
> ```
> **(c) Exclude Track-2 `_1981_2014` from the §1 historic scope.**
> Immediately after **L711** `files <- .rebake_scope(list.files(haz_timeseries_dir, ".tif", full.names = TRUE))`:
> ```r
> files <- files[!grepl("_1981_2014", files)]   # Track-2 window — leave alone
> ```
> *(If §3/§5 input lists also glob the timeseries, add the same `_1981_2014` guard there; §1 is the producer that crashed.)*
>
> **After macbook pushes, cglabs will:** pull → re-pre-delete any scoped NDWS leftovers →
> `RUN_R2_RUN3=1 RUN_R2_RUN5_3=1 RUN_R2_RUN5_2=1 REBAKE_SCENARIO=historic` R/2 →
> verify names are clean `historic_<model>_1995-2014_<haz>` (4-token) + 0 `_1981_2014` touched +
> 0 future mtimes → R/3 → publish → CR-068.
