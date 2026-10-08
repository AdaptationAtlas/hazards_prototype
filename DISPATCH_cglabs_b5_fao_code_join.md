# DISPATCH — cglabs — B5: FAO code join, 0.4.0 → 0.4.4 → reference + family republish

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### RESPONSE 2026-10-08 (A5 + A6) — cglabs — PUBLISHED: reference + family, both resolutions, 9 parquet keys + 9 sidecars, every one MD5 == ETag and row-identical to local; read back off S3: continental intld **240.24 B I$** on all six intld-bearing keys, nominal 201.92 B unchanged, CAF/GIN robusta 17.79 / 60.64 M at 0.05°. Thread complete A0-A6. #13 rebake dispatch NOT started.

**A5 — dry-runs then live, `scripts/r3_publish_tiers.R` only, flags exactly as the dispatch lists
(`--allow-unit-vintage-change`; no `--allow-res-change`, no `--allow-schema-drift`, no `--allow-value-drift`).**
Code `499b5df` (dry) / `53b04cd` (live) — identical publisher. Logs: `logs/b5_a5_dry_20261007_193653.log`,
`logs/b5_a5_live_20261007_193653.log`, driver scripts beside them. All four runs backgrounded.

Dry-run gates, every key: `ok: 14 columns identical`, `distinct(exposure)` / `distinct(unit)` /
`distinct(stat)` identical to the live object; rows local vs live: reference res-05 **8,065,336 vs
8,000,059**, res-25 **5,667,864 vs 5,621,991**, intld family res-05 **986,408 vs 921,131** (the recovered
composite items add rows), nominal family res-05 **1,508,624 vs 1,508,624 (identical)** — the nominal side
is untouched by B5, as designed. Exit 0 ×4, `[DRY RUN - nothing written]`.

Live, 07:35:06-07:36:53, exit 0 ×4. Every live object was backed up first (`backup written`), then
uploaded with `SIZE MATCH`:
`s3://digital-atlas/sandbox/backup/issue9_20261008_073506/` (reference res-05 + unsuffixed alias),
`…_073534/` (reference res-25), `…_073555/` (family res-05: nominal + intld, each with alias),
`…_073630/` (family res-25: nominal + intld). Keep until told they can go.

**A6 — independent read-back, in place, DuckDB httpfs over the public endpoint (python-duckdb 1.5.5; no
CLI on the node). Nothing downloaded.** `logs/b5_a6_readback_20261007_193653.log`, `…_totals_….log`.

| key (`…/processing=atlas-harmonized/variable=`) | rows S3 = local | bytes S3 = local | ETag vs local MD5 | intld, B I$ (S3) | nominal, B (S3) |
|---|---:|---:|---|---:|---:|
| `crop-livestock_all_res-05.parquet` | 8,065,336 | 8,918,339 | **MD5 MATCH** | **240.24** | 201.92 |
| `crop-livestock_all.parquet` (alias) | 8,065,336 | 8,918,339 | MD5 MATCH | 240.24 | 201.92 |
| `crop-livestock_all_res-25.parquet` | 5,667,864 | 7,137,701 | MD5 MATCH | **240.24** | 201.92 |
| `vop_intld15-2021_res-05.parquet` | 986,408 | 1,397,331 | MD5 MATCH | **240.24** | — |
| `vop_intld15-2021.parquet` (alias) | 986,408 | 1,397,331 | MD5 MATCH | 240.24 | — |
| `vop_intld15-2021_res-25.parquet` | 693,192 | 1,120,912 | MD5 MATCH | **240.24** | — |
| `vop_nominal-usd-2021_res-05.parquet` | 1,508,624 | 1,734,813 | MD5 MATCH | — | 201.92 |
| `vop_nominal-usd-2021.parquet` (alias) | 1,508,624 | 1,734,813 | MD5 MATCH | — | 201.92 |
| `vop_nominal-usd-2021_res-25.parquet` | 1,060,176 | 1,407,809 | MD5 MATCH | — | 201.92 |

Totals are admin0 rows, `tech = all`, `exposure = vop`, finite values (the tables carry NaN for
no-production cells, so a bare `sum()` returns NaN — the first read-back pass did exactly that and was
re-run with `isfinite(value)`; 0 non-finite rows were dropped at admin0). Nine `.parquet.json` sidecars
uploaded alongside, `SIZE MATCH` each. `Last-Modified` 07:35:23-07:36:51 today on every key.

Pinned pairs off S3: CAF robusta **17.79 M**, GIN robusta **60.64 M** at 0.05° (both aliases and the family
key agree) — the national `value_alloc`; at 0.25° 23.85 / 61.37 M (border spill, as A3). Arabica 0 in both.

**Continental constant-I$ reference, as now live: 240.24 B I$ (was 200.28; +20.0 %).** Nominal
201.92 B, unchanged.

**No stale key.** Every key the publisher writes was overwritten; the unsuffixed aliases are refreshed
from res-05 by the script itself. Nothing was deleted; `s3fs` delete never called.

**Thread state:** A0-A6 complete. Left on disk for the macbook's call: `sandbox/b5_old_adm_sum_20261007_193653/`
(old caches, 12 files), `sandbox/stage0_harness_20261007_184538/` (closed thread), the inert
`fao_prices/crop_factor_intld15-2021-t_res-25.tif` duplicate (macbook OK'd deletion; not done).
`DISPATCH_cglabs_issue13_rebake.md` remains HELD and has not been opened for running.

---

### RESPONSE 2026-10-08 (A4.1 re-run) — cglabs — on `499b5df`: crop QAQC median 0.98 (37/50) at 0.25°, 0.99 (42/50) at 0.05°; pins applied (CAF 1.000, GIN 0.989); livestock 242/242 both. New fixture passes on node. GO A5: dry-runs started.

`project_dir=$PWD Rscript R/checks/fixture_qaqc_denominator.R` → `ALL QAQC-DENOMINATOR FIXTURE ASSERTIONS PASSED`.

`logs/b5_a41rerun_20261007_193653.log`:

```
EXPOSURE_RES=0.25: LIVESTOCK ratios: median=1.00 | within 0.9-1.1 = 242/242
                   crop denominator: 2 FAOSTAT quantity pin(s) applied, as 0.4.0 does
                   CROP national-total ratios: median=0.98 | within 0.9-1.1 = 37/50 | file=spam_vop_intld15-2021_all_res-25.tif
EXPOSURE_RES=0.05: LIVESTOCK ratios: median=1.00 | within 0.9-1.1 = 242/242
                   crop denominator: 2 FAOSTAT quantity pin(s) applied, as 0.4.0 does
                   CROP national-total ratios: median=0.99 | within 0.9-1.1 = 42/50 | file=spam_vop_intld15-2021_all_res-05.tif
```

Outside 0.9-1.1 at 0.05° (8 + 5 NA): DZA/EGY/LBY/MAR/TUN = 0 (outside the SPAM release), SDN 0.001
(22 guarded pairs), **DJI 0.884 and GAB 0.890** (one guarded pair each: DJI sugc, GAB oilp — the guard
blanks 12 % / 11 % of their crop GPV), and COM/CPV/ESH/MUS/SYC with no FAO denominator (removed/tiny).
CAF 1.000, GIN 0.989 — the pins now reach the denominator. Nothing else outside the band.

**A5 started**: the four `--dry-run` publishes in dispatch order, sequential, background
(`logs/b5_a5_dry_20261007_193653.sh` → `.log`). Live writes follow only after each dry-run's gates are
read, and are backgrounded. Flags exactly as the dispatch lists: `--allow-unit-vintage-change` only.

---

### MACBOOK 2026-10-07 (f) — A4.1 was a GATE defect, not a product defect. Fixed (a); re-run A4.1 then GO A5.

**Right call to stop, and the diagnosis is correct.** You separated the two questions the gate
conflates and answered both: the product reproduces its input (gridded / allocated = 0.996, and A3
read 240.24 B back off both tables against 0.4.0's own allocation check), while the gate's reference
sits ~20 % below the reference the product was built from. That is a gate failing a correct run — the
failure mode AGENTS.md puts first — and finding it by measuring the denominator against 0.4.0's GPV
rather than arguing about the ratio is exactly the right move.

**Option (a). Fixed on `develop`, with one correction to the proposed shape.**

Your fix was *sum items per year, then median across years*. 0.4.0 does the **reverse order**:
`R/0.4.0:164` medians each item's year window, then `vop_allocate.R:113` sums the items into the
group. `median(sum)` and `sum(median)` are not the same number, and a gate's reference has to be
built the *same way* as the input it judges, so `fao_gpv_i()` now does **median across the window per
item, then sum the items** — `collapse = "item_median_then_sum"`. In practice yours would have landed
very close (you measured 1.000 against 0.4.0's GPV), but "very close" is how a reference drifts.

The default stays `collapse = "median"`, so the 1:1 livestock path is untouched — it must stay at
242/242.

**Pins: also fixed, and there was a trap in it.** You are right that the gate must apply them. The
first cut passed `fao_prod = NULL` to `vop_apply_quantity_pins()`, which cannot then derive
`prod_pinned / prod_FAO`, leaves the ratio `NA`, and **silently leaves the denominator unpinned** —
a no-op that would have looked like a fix. The gate now reads the FAO production file the same way
0.4.0 does to get the ratio, and **stops** if a pin resolves no ratio. So CAF should move off 0.657.

New fixture `R/checks/fixture_qaqc_denominator.R` pins all of it: single-item groups identical either
way (why livestock never showed this), composite groups collapsing to one item under the old rule,
the per-item-median-then-sum arithmetic, the pin scaling, and the silent-no-op trap.

**A4.2 and A4.3: accepted, nothing asked.** The cross-basis gate passing with *no new un-named
residual* on either grid is the result that matters — the stop condition I gave you did not fire.
CAF robusta at 11.1 rather than 35 is explained by the nominal side being 0.4.2's price × SPAM
tonnes, which is the half B5 deliberately did not correct. Leave the "now inside the band" rows in
the file: they are out of band at 0.25°, and a residual that is registered and quiet is cheaper than
one that has to be re-litigated next bake.

**What to do now:**

1. `git pull` (expect this commit or later), then re-run **A4.1 only**, both resolutions.
   **Expected: crop median near 1 and most countries in band** — your corrected-denominator estimate
   was 0.992 with 41/49, and the pins should now also lift CAF and GIN. Livestock must stay 242/242.
2. The 8 you expect to remain outside are understood and not a stop: DZA/EGY/LBY/MAR/TUN (outside the
   SPAM release), SDN (22 guarded pairs), CPV/MUS/COM/SYC (tiny or removed).
3. **If it reads near 1, GO A5** and carry on to A6 as the dispatch stands. If it does not, stop again
   and report — do not adjust the gate yourself.
4. ~~The res-25 twin check~~ — **answered in your addendum below: all three physical twins are
   present** (`prod`/`t`, `harv-area`/`ha`, `number`/`number`, 42 crops across 55 countries). That
   clears the #13 bake's G6b basis for `prod_t`, `ha` and `head_n`. Nothing further needed.
### RESPONSE 2026-10-07 (A4 addendum) — cglabs — macbook (e) check: the res-25 combined table carries all three physical twins (prod/t, harv-area/ha, number/number) plus both vop units. No stop on this. The A4.1 stop below stands.

```
res-25  mtime 2026-10-07 19:57:59 
    exposure             unit    rows n_crops n_iso3
      <char>           <char>   <int>   <int>  <int>
1: harv-area               ha 1284444      42     55
2:    number           number   61164      12     55
3: phys-area               ha 1284444      42     55
4:      prod                t 1284444      42     55
5:       vop     intld15-2021  693192      52     55
6:       vop nominal-usd-2021 1060176      43     55
res-05  mtime 2026-10-07 20:02:02 
    exposure             unit    rows n_crops n_iso3
      <char>           <char>   <int>   <int>  <int>
1: harv-area               ha 1827756      42     55
2:    number           number   87036      12     55
3: phys-area               ha 1827756      42     55
4:      prod                t 1827756      42     55
5:       vop     intld15-2021  986408      52     55
6:       vop nominal-usd-2021 1508624      43     55
```

---

### RESPONSE 2026-10-07 (A4) — cglabs — STOPPED at A4.1: crop QAQC median 1.17 (0.25°) / 1.18 (0.05°), 9/50 and 13/50 within 0.9-1.1 — NOT near 1. Cause diagnosed, not fixed: the gate's own FAO denominator takes a MEDIAN across a composite group's items where it should SUM them; recomputed sum-then-median gives median 0.992, 41/49 in band, and equals 0.4.0's GPV to 1.000. A4.2 cross-basis PASS on both grids (only pre-registered residuals out of band; none new). A4.3 probe consistent. A5 NOT started.

All three gates ran, in order, read-only (`logs/b5_a4_20261007_193653.sh` → `.log`, 20:02-20:05). The
first deviates, so per the dispatch nothing was published and nothing was changed; the other two are
reported because they were already in the script and their output is useful.

**A4.1 — `qaqc_vop_vs_faostat.R`: DEVIATES.**

```
EXPOSURE_RES=0.25: LIVESTOCK ratios: median=1.00 | within 0.9-1.1 = 242/242
                   CROP national-total ratios: median=1.17 | within 0.9-1.1 = 9/50  | file=spam_vop_intld15-2021_all_res-25.tif
EXPOSURE_RES=0.05: LIVESTOCK ratios: median=1.00 | within 0.9-1.1 = 242/242
                   CROP national-total ratios: median=1.18 | within 0.9-1.1 = 13/50 | file=spam_vop_intld15-2021_all_res-05.tif
```

(The last run on the old rasters, 2026-10-05, read `median=1.03 | 28/50`.) Not the ~1.08 the dispatch
named for "gate on the old name basis" — the script does key on `code_fao` (`qaqc_vop_vs_faostat.R:128-132`,
`prepare_fao_data(by = "code")`), and that part reached the node run. The deviation is a different defect
in the same gate, exposed by the fix:

- **The product is right against its input.** Per country, gridded national total ÷ 0.4.0's allocated
  `value_alloc`: median **0.996** (border cells only). The raster carries what 0.4.0 allocated.
- **The gate's denominator is short.** Per country, qaqc `fao_vop_i` ÷ 0.4.0's GPV for the same country
  (sum of `gpv` over the allocation CSV): median **0.80**; ZAF 0.56, ZWE 0.57, MLI 0.65, NGA 0.82. The gate's
  reference is ~20 % below the reference the product was built from, so a correct product reads as 1.2.
- **Why.** `fao_gpv_i()` (`qaqc_vop_vs_faostat.R:49-58`) melts the per-item FAO table to
  (iso3, atlas_name, year) rows and then takes `median(gpv_i_k) by (iso3, atlas_name)`. For a single-item
  group that is the intended median across the five window years. For a composite group it is a **median
  across N items × 5 years** — one typical item's value stands in for the whole group. ZAF 2021: `temf`
  11 items sum 2.70 B, median item 0.032 B; `trof` 12 items sum 1.54 B, median 0.058 B; `vege` 16 items sum
  1.07 B, median 0.022 B. 0.4.0 sums the items (`vop_item_groups` + GPV by group). Before B5 the name join
  matched only 1-5 items per composite, so the collapse cost little (1.03); with 57 renamed codes recovered
  the composites hold 11-26 items each and the median-of-items is far below the sum.
- **Check, off the same FAO file, same codes, same years** (`Value_of_Production_E_Africa.csv`, constant
  2014-16 I$, codes from `SPAM2010_FAO_crops.csv`, 2019-2023): recomputing the denominator exactly as the
  script does reproduces its `fao_vop_i` to 3 decimals for every country; recomputing as
  **sum over items per year, then median across years** gives:

| | as the script computes it | sum items, then median years |
|---|---:|---:|
| crop median ratio (grid ÷ FAO) | 1.178 | **0.992** |
| within 0.9-1.1 | 13 / 49 | **41 / 49** |
| denominator ÷ 0.4.0's GPV, median | 0.80 | **1.000** |

  The 8 outside the band under the corrected denominator are all explained: DZA/EGY/LBY/MAR/TUN and SDN
  = 0 (outside the SPAM release / 22 Sudan pairs guarded, by design); **CAF 0.657 and GIN 0.918 because the
  gate's denominator does not apply the quantity pins** (`fao_quantity_pins.csv` is read by 0.4.0, not by
  the QAQC; CAF denominator 1.76 B vs pinned 1.16 B); CPV/MUS/COM/SYC are in `remove_countries`/tiny.
  A pinned basis will always read low in this gate unless the gate applies the same pins.

**No change was made to `R/qaqc_vop_vs_faostat.R`.** The one-line shape of the fix is clear
(aggregate `sum(gpv_i_k) by (iso3, atlas_name, year)` before the `median by (iso3, atlas_name)`, and apply
`vop_apply_quantity_pins()` to the denominator as 0.4.0 does), but a gate's reference is the macbook's to
set, and the dispatch says stop. This is the G6 lesson in the other direction: the gate judged the product
against a reference built differently from the input.

**A4.2 — `vop_cross_basis_gate.R`: GATE PASS at both resolutions.** Expected residuals file has 7 rows
(ETH tea, TGO oilpalm, GNB maize, CAF×2, GIN×2 coffee).

| | 0.25° | 0.05° |
|---|---|---|
| pairs both bases / material / one-sided | 1,396 / 1,039 / 411 | 1,181 / 994 / 559 |
| material ratio nominal/intld, median [5-95 %] | 1.22 [0.418, 3.04] | 1.22 [0.398, 3.14] |
| out-of-band, NAMED (expected) | ETH:tea 0.0785 | ETH:tea 0.0785, TGO:oilpalm 11.6, **CAF:robusta-coffee 11.1**, GNB:maize 10.2 |
| out-of-band, no national allocation (border spill, #18; reported not gated) | TCD:yams 184, BEN:bean 16.4 | — |
| named residuals now inside the band (script suggests prune) | TGO:oilpalm, GNB:maize, CAF:arabica, CAF:robusta, GIN:arabica, GIN:robusta | CAF:arabica, GIN:arabica, GIN:robusta |
| **new, un-named residuals** | **none** (`ok: no material pair outside [1/10, 10] has its nominal side off`) | **none** |
| per-crop medians | all within [1/5, 5]; worst coconut 2.60, oilpalm 2.47, arabica 0.485, cotton 0.497 | all within [1/5, 5]; worst coconut 2.61, oilpalm 2.30, cotton 0.480, robusta 0.499 |

CAF robusta reads 11.1 at 0.05° (197.6 M nominal vs 17.79 M intld — the 35× by construction the macbook
pre-registered; it lands at 11 rather than 35 because the gate's `nominal` is 0.4.2's price × SPAM tonnes
with the GIN/CAF price treatment, not FAO's). At 0.25° the same pair sits inside the band because the
border spill (23.85 M intld, A3) lifts the intld side. GIN robusta is inside the band on both grids (ratio
~5): the GIN pin ratio is 8.4× and the registered price pin pulls the other way. No pair outside the
composite groups moved out of band — the macbook's stop condition for this gate did not fire. The macbook
may want to prune the three "now inside" rows at 0.05°, or leave them since they are out of band at 0.25°.

**A4.3 — `probe_040_allocation.R` at 0.25°: consistent** with the A1/A2 runs line for line (1,210 pairs,
inside 249.28 B, guarded 30 pairs 9.04 B = 3.6 %, guarded countries BEN(2) DJI(1) GAB(1) KEN(1) SDN(22)
ZWE(3), NGA banana pooled pair coverage 1.02 unguarded, BEN guard share 1.4 %).

**Before/after per crop** is in the A2 block above (from the kept 2026-10-05 caches): continental
200.28 → 240.24 B I$ (+39.96, +20.0 %); vege +13.06, trof +7.38, rest +8.41 (was absent), orts +5.53
(absent), temf +2.51, other-cereals +1.92, other-pulses +1.48, other-oil +0.65, ofib +0.08 (absent);
robusta −1.05 (pins); 32 single-item crops unchanged to 3 decimals. Same at 0.25°.

**A5 / A6: not started.** No publish, no S3 traffic, no deletes. The 0.4.0 / 0.4.4 outputs from A1-A3
stay on disk as the candidate; the old caches stay in `sandbox/b5_old_adm_sum_20261007_193653/`.
Waiting for the macbook's call on A4.1: (a) fix the QAQC denominator (sum-then-median + pins) and I re-run
A4.1 and proceed to A5 if it reads near 1; or (b) accept the 1.17 as understood and GO A5 as the dispatch
stands. Either way, say which.

---

### MACBOOK 2026-10-07 (e) — A3 accepted. One cheap check to add at A4, for the #13 bake's sake.

**A3 is the gate it was meant to be.** Both new tables read back 240.24 B I$ against 0.4.0's own
allocation check — product judged against the input it was built from, on the same grid — and at
0.05° the two pinned pairs land on the allocated national values exactly. Nothing is asked of you for
A3 itself.

**The 0.25° CAF figure is expected, and worth stating so A4 is not misread.** 23.85 M against 17.79 M
allocated is +34 %, and it is the #18 coarse-cell behaviour, not the pin failing: a 0.25° cell on the
CAF side of a border carries a neighbour's coffee. It is *redistribution, not creation* — the
continental total is 240.24 B on both grids, so whatever CAF gains, its neighbours lose. Every
(country, crop) pair is subject to it; the pinned pairs are only conspicuous because we know what
their national value should be. Report it, do not stop on it.

**The check to add, because the #13 bake depends on it.** You confirmed the combined **0.05°** table
carries `exposure` × `unit` including **`prod`/`t`**. The bake's first-publish gate (G6b) for the new
`prod_t` tier takes its basis from the **res-25** combined table, filtered to `exposure == "prod"`,
and for `ha` and `head_n` from the same table filtered to `harv-area` and `number`. Those untagged
native rasters should survive 0.4.4's filter on both grids ("keep this grid's files plus untagged
native ones"), but it is worth one line of confirmation now rather than finding out at publish:

```
Rscript -e 'suppressMessages(library(arrow)); suppressMessages(library(dplyr))
  f <- file.path(Sys.getenv("exposure_dir"), "exposure_adm_sum_spam20-20_glw420-20_res-25.parquet")
  print(open_dataset(f) |> distinct(exposure, unit) |> collect())'
```

**Expected:** rows for `prod`/`t`, `harv-area`/`ha`, `number`/`number` and both `vop` units. If any of
the three physical ones is missing from the **res-25** table, say so and stop before A5 — it would
mean the #13 bake has no gate basis for that tier, and the publisher refuses to publish a tier whose
basis matches no rows.

**Otherwise carry on through A4-A6 as written.**

---

### RESPONSE 2026-10-07 (A3) — cglabs — 0.4.4 at 0.25° and 0.05° COMPLETE (exit 0 both, FORCE_OVERWRITE=1 verified in-log); intld read back from BOTH new tables = 240.24 B I$ = 0.4.0's allocation check; CAF/GIN coffee at 0.05° read exactly the pinned national values. No untagged-twin stop. A4 gates running.

Run: `logs/b5_044_20261007_193653.sh` → `logs/b5_044_20261007_193653.log`. The script exports
`FORCE_OVERWRITE=1` and the log's first line is `env check: FORCE_OVERWRITE=1`; each 0.4.4 start line
reads `script start (FORCE_OVERWRITE=1 -> overwrite=TRUE)`. 0.25°: 19:55:41 → 19:58:14. 0.05°:
19:58:14 → 20:02:26. §1 kept `66 tifs -> 45 after keeping <tag> + untagged` on both grids; the
untagged-legacy-twin check did not fire, so nothing was moved aside.

**Product vs input, same grid** (the gate; admin0 rows, `tech = all`, read with `arrow`):

| table (new, written today) | continental intld, B I$ | 0.4.0 allocation check | CAF robusta, M I$ | GIN robusta, M I$ |
|---|---:|---:|---:|---:|
| `vop_intld15-2021_adm_sum_spam20_glw420_res-05.parquet` (20:02) | **240.24** | 240.24 | **17.79** | **60.64** |
| `vop_intld15-2021_adm_sum_spam20_glw420_res-25.parquet` (19:58) | **240.24** | 240.24 | 23.85 | 61.37 |

At 0.05° the two pinned pairs read back **exactly** the national `value_alloc` (17,787 / 60,638 k I$);
arabica is 0 in both countries, so the whole pin lands on robusta. At 0.25° CAF reads 23.85 M against
17.79 M allocated — the coarse-cell border spill the dispatch says the cross-basis gate reports rather
than fails (a 0.25° cell on the CAF side of a border holding a neighbour's coffee); GIN 61.37 vs 60.64,
same mechanism, smaller. Both grids conserve the continental total.

Also rewritten: `exposure_adm_sum_spam20-20_glw420-20_res-{05,25}.parquet` (8.07 M / 5.67 M rows),
`vop_nominal-usd-2021_adm_sum_…`, `hpop_adm_sum_…`, and the per-tif `*_adm_sum.parquet` caches under
`variable=vop_intld15-2021/` (the 2026-10-05 ones are preserved in `sandbox/b5_old_adm_sum_20261007_193653/`).
The combined 0.05° table carries exposure × unit = harv-area/ha, phys-area/ha, **prod/t**, number,
vop/intld15-2021, vop/nominal-usd-2021 — the `prod_t` twin the #13 bake's tier needs is present.

**A4 launched** 20:02 (`logs/b5_a4_20261007_193653.sh` → `.log`): qaqc at 0.25° and 0.05°, cross-basis
gate at 0.25° and 0.05°, probe at 0.25°, in that order; each step's output is reviewed before the next is
trusted.

---

### MACBOOK 2026-10-07 (d) — A2 accepted. Factor-raster naming was my bug; fixed. Carry on through A4-A6.

**A2 is clean and nothing is asked of you for it.** Grid-independence holds on every national line,
the guarded set is identical on both grids, and the per-crop table is exactly the predicted shape:
the move is confined to the composite groups, no single-item crop moves by anything, and the only
decrease is robusta coffee by the pinned 1.05 B. Your point that `rest`/`orts`/`ofib` were **absent**
rather than zero — so the old-vs-new per-crop sum does not show their +14.0 B — is the right way to
read it, and 25.94 + 14.02 = 39.96 closes the arithmetic.

**The factor rasters: my bug, now fixed on `develop`.** You were right to flag it. `alloc_grid` is
pinned to 0.05° whatever `EXPOSURE_RES` says (`R/0.4.0:57`), because pricing has to happen on the fine
grid — a 0.25° border cell belongs to one country but carries both countries' production. I then
named the output with the *run's* `EXPOSURE_RES` tag, so the `_res-25.tif` was 0.05° data under a
0.25° name. A filename is a claim like any other, and that one was false.

Fixed: the tag is now derived from the raster's own resolution, so both runs write the same
`crop_factor_intld15-2021-t_res-05.tif` and the name cannot drift from the content. A fixture
assertion pins it. The A2 gate text is corrected to say one file, not two.

**No action for you.** Nothing reads `fao_prices/` — 0.4.4 §1 lists only `variable=*` paths, as you
noted — so the stale `_res-25.tif` on disk is inert. Delete it at your convenience
(`fao_prices/crop_factor_intld15-2021-t_res-05.tif` is the one to keep; they are byte-identical, so
nothing is lost either way), or leave it and the next 0.4.0 run will simply stop producing it.

**Carry on through A4-A6 as written.** The two things I flagged for A4 still stand: report per-crop
before/after from the caches you kept, and expect more cross-basis movement than usual across the
composite groups now that intld has risen 20 % while the nominal side is unchanged by design — stop
only if a pair *outside* the composite groups moves.

---

### RESPONSE 2026-10-07 (A2) — cglabs — 0.4.0 at 0.25° COMPLETE: audit, pins, allocation and continental total IDENTICAL to 0.05° (240.24 B I$); guarded set identical on both grids (30 pairs); both factor rasters present and byte-identical. Per-crop before/after at 0.05°: single-item crops move 0.000, composites carry all of +39.96 B. A3 (0.4.4 × 2) launched.

Run: `EXPOSURE_RES=0.25 FORCE_OVERWRITE=1`, 19:45:39 → 19:53:52 (8 min), `logs/b5_040_res25_20261007_193653.log`.

**Grid-independence, as the dispatch requires.** Every national line is the same as at 0.05°:
`B5 join audit … recovered 55.12 B I$ (18.1%) across 57 renamed item codes`; pins APPLIED 2/2 matched;
`allocation table: 1210 pairs … inside 249.28 B I$, guarded 30 pairs 9.04 B I$ = 3.6%`;
`price factor check: … 7.28e-12 (peak 4.69e+04) over 42 layers`;
`allocation check: 1067 pairs conserved to 1e-6, 143 guarded pairs empty; continental total 240.24 B I$`.
Read back off `spam_vop_intld15-2021_all_res-25.tif`: 240.24 B I$ (42 layers).

**The guarded set is identical on the two grids** — 30 pairs, same members, including BEN cowpea
(SPAM 5,775 t vs FAO 134,940 t on both). The dispatch's "one legitimate difference" (BEN cowpea 5.8 kt at
0.05° vs 25.1 kt at 0.25°) no longer arises: 0.4.0 now takes the SPAM national totals on the native grid
(script header, "identical on the two grids by …"), so the coverage guard is grid-independent. Not a
deviation — the invariant is tighter than the dispatch assumed.

**Factor rasters.** `fao_prices/crop_factor_intld15-2021-t_res-05.tif` and `…_res-25.tif` both exist
(42 layers), and are **byte-identical (md5 `a2955948…`)**, both on the 0.05° native grid. The code comment
says "written on THIS run's allocation grid", and that grid is the native one in both runs, so the
`res-25` file is a duplicate by construction. Harmless for A3 — 0.4.4 §1 lists only `variable=*` paths,
so `fao_prices/` is never extracted — but the macbook may want the comment, or the second write, revised.

**Per-crop before/after, 0.05°, continental sums of the written `all` raster vs the 2026-10-05 cache
(`sandbox/b5_old_adm_sum_20261007_193653/…_all_res-05_adm_sum.parquet`, admin0 rows), B I$:**

| crop | old | new | Δ |
|---|---:|---:|---:|
| vegetables | 11.035 | 24.099 | **+13.063** |
| tropical fruit | 6.584 | 13.959 | **+7.375** |
| temperate fruit | 2.648 | 5.153 | **+2.505** |
| other cereals | 0.523 | 2.446 | +1.923 |
| other pulses | 0.311 | 1.791 | +1.480 |
| other oil crops | 0.298 | 0.945 | +0.647 |
| rest of crops | absent | 8.413 | **+8.413** |
| other roots | absent | 5.532 | **+5.532** |
| other fibre crops | absent | 0.078 | +0.078 |
| robusta coffee | 2.383 | 1.330 | **−1.053** (the two pins) |
| arabica coffee | 1.707 | 1.707 | 0.000 |
| maize / rice / wheat / cassava / yams / sorghum / … (all 31 other crops) | — | — | **0.000 each** |
| **total** | **200.28** | **240.24** | **+39.96** |

Exactly the macbook's shape: the move is in vege, trof, temf, orts, rest (plus the smaller composites);
no single-item group moves; the only decrease is robusta coffee, by the pinned 1.05 B. Three groups that
matched nothing by name (rest, orts, ofib) now exist in the raster for the first time — they were not
"zero", they were absent — which is the +14.0 B the old-vs-new per-crop sum does not show (25.94 + 14.02 =
39.96).

**A3 launched** 19:55: `logs/b5_044_20261007_193653.sh` runs 0.4.4 at 0.25° then 0.05° with
`FORCE_OVERWRITE=1` exported and echoed into the log (`env check: FORCE_OVERWRITE=1`), log
`logs/b5_044_20261007_193653.log`. A4 follows when both finish.

---

### MACBOOK 2026-10-07 (c) — your `FORCE_OVERWRITE` call was right and my A1 was wrong. Item 1 is not material: proceed to A3.

**1. The deviation — you were correct, and the dispatch was wrong.** A1 as I wrote it set
`EXPOSURE_RES` only. 0.4.0 gates its three VoP rasters on `FORCE_OVERWRITE` (`:285`, `:305-306`), so
with the 2026-10-05 files on disk the run would have written a *new* audit, allocation CSV and factor
raster beside *unchanged* rasters, and A3 would then have re-baked the reference from pre-B5 inputs
while every log line said the fix had landed. That is exactly the silent-stale shape, and catching it
one minute in before any write is the right call. **Thank you for stopping rather than proceeding.**

A1 and A2 above are corrected to carry `FORCE_OVERWRITE=1`, so the record matches what was run. No
re-run: the forced run is the one we want.

**2. Item 1 — not material. Proceed.** The dispatch's ~53.6 B / 59 codes was a macbook estimate with
no FAOSTAT files to hand; yours is the measurement, and the measurement wins. 55.12 B over 57 codes
is +2.8 % and two codes fewer, in the direction and the groups predicted.

Your two-denominators point is right, and the two ratios reconcile: 18.1 % is against the
composite-group code-matched GPV (303.9 B), and the dispatch's ~8 % was against all-crop FAO GPV
(~670 B) — 55.12 / 670 = **8.2 %**. Both correct, different bases. The per-group table is the
confirmation that matters: the five groups predicted to move most are the top five in that order, and
`rest` / `orts` / `ofib` matched **nothing** by name, which is the defect in its purest form.

**3. Every gate I asked for holds.**
- pins 2/2 matched, at the macbook's exact figures;
- guarded share **3.5 % → 3.6 %** — the invariant. Three newly guarded pairs, all Sudan
  (`SDN opul/rest/temf`, 0.29 B), which is the predicted mechanism: recovered composite value in the
  one country SPAM barely covers. Nothing left the guarded set;
- CAF/GIN coverage 30.2 and 7.27, unguarded, `value_alloc` as computed on the macbook;
- price-factor identity 7.28e-12 over 42 layers;
- your decomposition checks: 55.12 − 12.4 − 1.73 − 1.05 = **39.94**, against the 39.96 observed.

**4. One number that needs saying out loud, and is not a gate.** The continental constant-I$ total
moves **200.28 → 240.24 B I$, +20.0 %**. That is the correct consequence of the fix — value that was
being dropped is now allocated — but it is a large, consumer-visible move in the published reference
and every intld product built on it. It is not a reason to stop; it is a reason the CDH records and
the note to Brayden have to state it rather than let it be discovered. Macbook will carry that.

**Proceed to A3 when A2 finishes**, then A4-A6 as written. Two things to carry forward:
- at A4, report the per-crop before/after from the `b5_old_adm_sum_20261007_193653/` caches you kept —
  good call keeping them, that is a better basis than memory;
- at A4's cross-basis gate, the four CAF/GIN coffee rows are pre-registered and expected. Given the
  +20 % move on the intld side with the nominal side unchanged, **expect more cross-basis movement
  than usual across the composite groups**. Report what you see; stop only if a pair outside the
  composite groups moves, since those are the ones this fix should not have touched.

---

### RESPONSE 2026-10-07 (A1) — cglabs — 0.4.0 at 0.05° COMPLETE: pins APPLIED 2/2 matched; B5 audit recovered 55.12 B I$ over 57 renamed codes (dispatch said ~53.6 B / 59 — reported as measured, not reconciled); guarded share 3.5 % → 3.6 %; price-factor identity holds; continental total 200.28 → 240.24 B I$. A2 (0.25°) running.

Node at `6a578e5`. Run: `EXPOSURE_RES=0.05 FORCE_OVERWRITE=1`, 19:37:03 → 19:45:03 (8 min),
`logs/b5_040_res05_20261007_193653.log`.

**One deviation from the A1 command, stated up front.** The dispatch's A1 line sets `EXPOSURE_RES`
only. 0.4.0 gates its three VoP rasters on `FORCE_OVERWRITE` (`R/0.4.0_create_crop_vop_intld15.R:285`,
`:305-306`): with the flag unset and the 2026-10-05 files on disk, it would have written the audit, the
allocation CSV and the factor raster but **skipped `spam_vop_intld15-2021_{all,irr,rf-all}_res-05.tif`**,
leaving 0.4.4 to read the pre-B5 rasters — the silent-stale shape A3 warns about. I started it as written,
saw `FORCE_OVERWRITE=<unset> -> overwrite=FALSE` in the first log line, killed it one minute in (before
any write; log kept as `logs/b5_040_res05_20261007_193546_ABORTED_noforce.log`) and relaunched with
`FORCE_OVERWRITE=1`. All numbers below are from the forced run. If the unforced behaviour was intended,
say so and I will re-run — but then A3 would be re-baking the reference from unchanged rasters.

**1. B5 join audit** (`vop_name_join_audit()`, national, pre-pin):

```
B5 join audit: GPV matched by item CODE 303.90 B I$ vs by NAME 248.78 B I$ — recovered 55.12 B I$ (18.1%) across 57 renamed item codes
```

Against the dispatch's never-measured figure: **55.12 B vs ~53.6 B (+1.5 B, +2.8 %); 57 renamed codes vs
59.** The 18.1 % is relative to the code-matched GPV of the composite groups' items (303.9 B), not to
"all-crop FAO GPV", so it is not the same ratio as the dispatch's ~8 %. Not reconciled; macbook to judge
whether +2.8 % / −2 codes is material. It did not look material enough to stop before 0.4.4, so A2 is
running; A3 has **not** started and waits for the word.

**2. Per-group value recovered** (thousand I$ in the log; B I$ here):

| group | by code | by name | items | renamed | recovered |
|---|---:|---:|---:|---:|---:|
| vege | 40.00 | 19.87 | 26 | 11 | **20.13** |
| trof | 25.30 | 14.53 | 15 | 6 | **10.77** |
| rest | 10.62 | 0.00 | 21 | 21 | **10.62** |
| orts | 5.58 | 0.00 | 2 | 2 | **5.58** |
| temf | 12.14 | 8.55 | 16 | 4 | **3.59** |
| ocer | 2.50 | 0.57 | 7 | 1 | 1.93 |
| opul | 2.11 | 0.35 | 6 | 3 | 1.76 |
| ooil | 6.04 | 5.38 | 8 | 4 | 0.65 |
| ofib | 0.08 | 0.00 | 5 | 5 | 0.08 |

The five groups the macbook expected to move most are the top five, in that order. `rest`, `orts` and
`ofib` had matched **nothing** by name (every item renamed). No single-item group appears: the renamed
list is exactly 57 codes, all in the composite groups (full list with the current FAOSTAT spelling and
the `SPAM2010_FAO_crops.csv` mapping name is in the log, lines 36-218; e.g. 108 "Cereals n.e.c." ←
"Cereals, nes", 463 "Other vegetables, fresh n.e.c." ← "Vegetables fresh nes", 217 "Cashew nuts, in
shell", 711 "Anise, badian, coriander, cumin, caraway, fennel and juniper berries, raw").

**3. Applied-pin table** — both rows matched, as required:

```
FAOSTAT quantity pins APPLIED to 2 (iso3, item) pair(s):
   iso3 item_code prod_before prod_after   ratio gpv_before gpv_after scale_gpv matched
1:  CAF       656      298000       8512 0.02857     622600     17790      TRUE    TRUE
2:  GIN       656      243700      29020 0.11910     509300     60640      TRUE    TRUE
```

Exactly the macbook's table (17,787 / 60,638 k I$ unrounded in the allocation CSV).

**4. Allocation table / guarded share** — invariant holds:

```
allocation table: 1210 (iso3, group) pairs with GPV | outside the SPAM release: 113 pairs in 5 countries (DZA,EGY,LBY,MAR,TUN), 53.56 B I$, NA by design | inside: 249.28 B I$, of which guarded 30 pairs 9.04 B I$ = 3.6% (VOP_COVERAGE_MIN=0.10) | 0 allocated without a FAO production row
```

| | 2026-10-05 res-05 (name join) | now (code join + pins) |
|---|---:|---:|
| (iso3, group) pairs with GPV | 1,019 | 1,210 |
| outside SPAM release | 104 pairs, 41.14 B | 113 pairs, 53.56 B |
| inside | 207.63 B | 249.28 B |
| guarded | 27 pairs, 7.36 B, **3.5 %** | 30 pairs, 9.04 B, **3.6 %** |

Newly guarded: **SDN opul, SDN rest, SDN temf** (0.29 B together) — recovered composite value in the one
country SPAM barely covers (28 of the 30 guarded pairs are Sudan). Nothing left the guarded set. CAF and
GIN coffee: `coverage` 30.2 and 7.27, `guarded = FALSE`, `value_alloc` 17,787 / 60,638 k I$ — the
macbook's 30 / 7.3.

**5. Price factor check** (new, #41): present, passed:

```
price factor check: production x factor reproduces VoP to 7.28e-12 (peak 4.69e+04) over 42 layers
```

**6. Allocation check / continental total:**

```
allocation check: 1067 pairs conserved to 1e-6, 143 guarded pairs empty; continental total 240.24 B I$
```

Before (2026-10-05, res-05): 200.28 B I$. **After: 240.24 B I$ (+39.96 B, +20.0 %).** Read back off the
written `spam_vop_intld15-2021_all_res-05.tif` (42 layers, 0.05°): 240.24 B — matches. The +40.0 B is the
55.12 B recovered, less the 12.4 B of it that falls in the five outside-SPAM countries, less the 1.73 B
removed by the guard and 1.05 B by the two coffee pins.

**Written at res-05** (all 19:37-19:45 today): `crop_vop_intld15-2021_allocation_res-05.csv`,
`crop_factor_intld15-2021-t_res-05.tif` (42 layers), `spam_vop_intld15-2021_{all,irr,rf-all}_res-05.tif`.
The `*_adm_sum.parquet` caches beside them are still the 2026-10-05 ones (0.4.4 owns them; A3 with
`FORCE_OVERWRITE=1` regenerates). Copies of those old caches are in
`nex-gddp-cimp6_hazards/sandbox/b5_old_adm_sum_20261007_193653/` so A4 can report per-crop before/after
from the old zonal sums rather than from memory.

**Next:** A2 (0.25°) is running with the same flags, `logs/b5_040_res25_20261007_193653.log`. A3 waits
for A2 and for the macbook's call on item 1.

---

### MACBOOK 2026-10-07 (b) — A0.5 answered: pins APPLIED at the pre-break FAO median. Resume at A1.

Good measurement, and the SPAM finding changed the decision — thank you for raising it rather than
proceeding. Pete's calls:

**1. Pin value: the pre-break FAOSTAT median, not ICO.** `metadata/fao_quantity_pins.csv` now carries
`status = applied`, `decided = Pete Steward 2026-10-07`:

| iso3 | prod_t | from | ratio | GPV k I$ |
|---|---:|---:|---:|---:|
| CAF | **8,512** | 297,962 | 0.02857 | 622,643 -> 17,787 |
| GIN | **29,018** | 243,703 | 0.11907 | 509,260 -> 60,638 |

The reasoning is in the CSV: the pin's job is to remove a reporting break, not to re-estimate
production against a source the rest of the chain does not use. The ICO gap is a separate and larger
claim. Both land exactly on `prod_t x 2,089.7`, so they are internally consistent by construction -
0.4.0's applied-pin log should show that.

**2. SPAM is deliberately NOT corrected.** The conservative choice: this run changes the FAO tables
only. So the constant-I$ basis is corrected while **nominal, `ha` and the new `prod_t` tier keep
SPAM's 257,009 t / 210,973 t**. The two bases will disagree by about 35x (CAF) and 8.4x (GIN) for
those pairs BY CONSTRUCTION.

**That disagreement is pre-registered**, so A4's cross-basis gate must not treat it as new:
`metadata/cross_basis_expected_residuals.csv` gains four rows - CAF and GIN x `arabica-coffee` and
`robusta-coffee` - with the full reason. If the gate reports those four and nothing else new, that is
the expected outcome. **Any OTHER new residual is still a finding: stop and report it.**

In-cell intld value for those pairs drops to ~69 I$/t (CAF) and ~287 I$/t (GIN) against 2,089.7
elsewhere, because the corrected national value is spread over SPAM's uncorrected tonnage. Expected,
and the reason the pairs are registered.

**Resume at A1.** `git pull` first - the pins, the residual rows and an updated caveat in
`docs/methods/nominal_price_method.md` are all on `origin/develop`. Nothing else changed, so A1-A6
run exactly as written.

Two expectations for A1 that now have numbers attached:
- the applied-pin table must show the two rows above with `matched = TRUE`; 0.4.0 **stops** if a pin
  matches no FAOSTAT row;
- the guarded share must not jump. Coverage for these pairs becomes spam/fao = 30 and 7.3, far ABOVE
  `VOP_COVERAGE_MIN`, so neither is newly guarded - the #39 guard fires only below the floor.

---

### RESPONSE 2026-10-07 — cglabs — A0 PASS (3/3 fixtures); A0.5 measured: CAF break 2017→2018 (10 kt→90 kt→300 kt), GIN break 2014→2015 (42 kt→211 kt); implied I$/t = 2,089.7 in EVERY year for BOTH countries — reporting change, not production. STOPPED at A0.5 for Pete's pin. Pins left `proposed`; A1 not started.

Node at `c1dd906` (`git pull` was already up to date). Nothing under `common_data` was written by this block.

**A0 — fixtures on this node: all three PASS.**

```
project_dir=$PWD Rscript R/checks/fixture_vop_allocate.R          -> ALL QUANTITY-PIN ASSERTIONS PASSED   (B5 + price-factor + pin assertions all ok)
project_dir=$PWD Rscript R/checks/fixture_fao_code_join.R         -> ALL FAO CODE-JOIN FIXTURE ASSERTIONS PASSED
project_dir=$PWD Rscript R/checks/fixture_crop_heat_interactions.R -> ALL CROP-HEAT INTERACTION FIXTURE ASSERTIONS PASSED
```

Notable lines: `production x factor reproduces the VoP raster cell by cell (max deviation 8.88e-16)`;
`B5 audit: 1804 matched by code, 1754 by name, 50 recovered over 1 renamed code`. No environment difference.

**A0.5 — CAF / GIN coffee (FAO item 656 "Coffee, green"), read straight off the bulk files.**

Files: `Data/fao/Production_Crops_Livestock_E_Africa_NOFLAG.csv` (what 0.4.0 resolves as `prod_file`;
the non-NOFLAG production file is not staged) and `Data/fao/Value_of_Production_E_Africa.csv`, element
`Gross Production Value (constant 2014-2016 thousand I$)` (exact string; the dispatch's `%like% "constant 2014-2016"`
also matches the SLC and US$ constant elements for GIN, so the table below filters on the I$ element only).

| year | CAF prod (t) | CAF GPV (k I$) | CAF I$/t | GIN prod (t) | GIN GPV (k I$) | GIN I$/t |
|---|---:|---:|---:|---:|---:|---:|
| 2010 | 5,270 | 11,013 | 2,089.8 | 29,018 | 60,638 | 2,089.7 |
| 2011 | 6,218 | 12,993 | 2,089.7 | 30,320 | 63,359 | 2,089.7 |
| 2012 | 6,923 | 14,466 | 2,089.7 | 17,907 | 37,420 | 2,089.7 |
| 2013 | 7,975 | 16,664 | 2,089.6 | 18,440 | 38,534 | 2,089.7 |
| 2014 | 10,799 | 22,566 | 2,089.6 | 41,500 | 86,721 | 2,089.7 |
| 2015 | 9,050 | 18,912 | 2,089.7 | **210,866** | **440,641** | 2,089.7 |
| 2016 | 10,120 | 21,147 | 2,089.6 | 218,635 | 456,876 | 2,089.7 |
| 2017 | 9,990 | 20,875 | 2,089.7 | 216,691 | 452,813 | 2,089.7 |
| 2018 | **89,979** | **188,028** | 2,089.7 | 235,043 | 491,163 | 2,089.7 |
| 2019 | 202,753 | 423,687 | 2,089.7 | 243,703 | 509,260 | 2,089.7 |
| 2020 | 289,283 | 604,508 | 2,089.7 | 242,682 | 507,126 | 2,089.7 |
| 2021 | 297,962 | 622,643 | 2,089.7 | 261,992 | 547,478 | 2,089.7 |
| 2022 | 306,901 | 641,322 | 2,089.7 | 261,645 | 546,753 | 2,089.7 |
| 2023 | 316,108 | 660,563 | 2,089.7 | 200,000 | 417,935 | 2,089.7 |

- **Where the break is.** CAF: between 2017 (9,990 t) and 2018 (89,979 t), then a further step to ~200-300 kt
  from 2019; a 30x jump over two years. GIN: between 2014 (41,500 t) and 2015 (210,866 t); a 5x jump in one year,
  flat at 200-260 kt since.
- **Pre-break level.** CAF 2010-2017: median 8,512 t, mean 8,293 t, range 5,270-10,799 t (ICO: 2-6 kt).
  GIN 2010-2014: median 29,018 t, mean 27,437 t, range 17,907-41,500 t (ICO: ~9 kt). Note that even the
  pre-break FAO series sits above the ICO figure for both countries.
- **Implied unit value.** 2,089.7 I$/t in every year, both countries, both sides of the break, to four
  significant figures. This is the signature named in the dispatch: FAO's constant-2014-16 I$ GPV is a
  single international price x FAO's own production, so the GPV series is the production series scaled.
  The tonnage jump is a reporting change, not production. (Nominal side for GIN moves normally:
  current-US$ GPV 25.8 M -> 131.4 M over the same 2014->2015 step, i.e. it carries the same tonnage break.)
- **What 0.4.0 uses today** (allocation CSVs of 2026-10-05, both grids): `fao_prod_t` = median(2019:2023) =
  CAF **297,962 t**, GIN **243,703 t**; GPV = CAF **622,643 k I$**, GIN **509,260 k I$**. Unguarded
  (coverage 0.863 / 0.866), identical at res-05 and res-25.

**Two things Pete should know before setting `prod_t`:**

1. **SPAM 2020 carries the same break.** The allocation table's `spam_prod_t` for `acof+rcof` is CAF
   **257,009 t** and GIN **210,973 t** at both resolutions - SPAM is calibrated to FAOSTAT national totals,
   so the inflated tonnage is already inside the physical rasters. The quantity pin corrects the **money**
   (with `scale_gpv = TRUE`, GPV_after = GPV x prod_t / fao_prod_t), but the `ha` tier, the forthcoming
   `prod_t` tier (#41) and the nominal `vop_usd` tier still carry ~257 kt / ~211 kt of coffee for these two
   countries. That is a separate decision; this pin does not reach it.
2. **Mechanics at plausible pin values** (for sizing, not a recommendation): CAF `prod_t = 4000` gives
   ratio 0.0134 and GPV 622,643 -> ~8,360 k I$; GIN `prod_t = 9000` gives ratio 0.0369 and GPV 509,260 ->
   ~18,800 k I$. Coverage becomes spam/fao = 64 and 23 - far above `VOP_COVERAGE_MIN`, so neither pair is
   newly guarded (the #39 guard fires only on coverage *below* the floor). The pinned GPV is then spread over
   SPAM's unchanged ~257 kt / ~211 kt, so the per-tonne intld value in those cells drops to ~33 I$/t and
   ~89 I$/t against 2,089.7 I$/t elsewhere. If Pete wants pre-break FAO rather than ICO, the medians above
   (8,512 t / 29,018 t) are the numbers.

**STOPPED here, per the dispatch.** `metadata/fao_quantity_pins.csv` is unchanged: both rows still
`status = proposed`, `prod_t` blank, `decided = PENDING`. A1 (0.4.0 at both resolutions) has **not** been
started, so the pins can be applied before the first 0.4.0 pass. Pete: fill `prod_t`, flip `status` to
`applied`, set `decided`, push; the next node session pulls, commits nothing else, and runs A1. If the
decision is instead "run with pins proposed", say so and A1 runs as today's behaviour plus the B5 fix.

Everything else in flight on the node is the Stage-0 harness thread (`DISPATCH_cglabs_stage0_wb_harness.md`),
which writes only to `nex-gddp-cimp6_hazards/sandbox/` and does not touch 0.4.x inputs or outputs.

---


Thread opened 2026-10-07 (macbook). Decision behind it: Pete chose **"fix before #13"** on
2026-10-07 (`HANDOVER_2026-10-07.md` §2 B5). Code landed the same day in `fd929e5` and `e5f6340`.

This is the Block-C/D shape of `archive/dispatches/DISPATCH_cglabs_exposure_intld_fixes.md`. It must
complete **before** the #13 bake, because it moves the constant-dollar reference the bake multiplies
by, and #13 is the first publish of the intld tiers.

---

## What changed and why

Everything in the constant-I$ chain joined FAOSTAT on the item **name**, taken from
`metadata/SPAM2010_FAO_crops.csv`'s `name_fao_val`. FAOSTAT renames items between releases
("Vegetables fresh nes" → "Other vegetables, fresh n.e.c."), and a renamed item simply stopped
matching — its value left the table in silence, because a missing item is indistinguishable from an
item the country does not grow. 59 of the 127 FAO codes in the composite groups
(`ocer / ofib / ooil / opul / orts / rest / temf / trof / vege`) had moved: **about 53.6 B I$, ~8 %
of all-crop FAO GPV.**

Everything now keys on `code_fao`, which is stable across renames. The nominal side is unaffected —
composite groups are not priced.

**`qaqc_vop_vs_faostat.R`'s crop denominator moved to the code too, and that half matters most
here.** Had only the producer moved, the QAQC's FAO denominator would have stayed short by the same
8 % and this correct re-bake would have read as a crop over-allocation. Expect the crop QAQC ratio
to stay near 1; if it lands near 1.08, the gate is on the old basis and something did not reach the
node run.

Off-node validation, all passing: `R/checks/fixture_vop_allocate.R` (B5 assertions plus the price
factor) and `R/checks/fixture_fao_code_join.R`. Run both on the node first — see A0.

---

## Gates — stop at each one

### A0. The fixtures, on this node

```
cd /home/jovyan/atlas/hazards_prototype
git fetch && git log --oneline -1 origin/develop     # expect e5f6340 or later
git pull
project_dir=$PWD Rscript R/checks/fixture_vop_allocate.R
project_dir=$PWD Rscript R/checks/fixture_fao_code_join.R
project_dir=$PWD Rscript R/checks/fixture_crop_heat_interactions.R
```

**Expected:** both end with `ALL ... FIXTURE ASSERTIONS PASSED`. Seconds, no data. If either fails
here but passed on macbook, stop — that is an environment difference worth knowing before a long
run.

### A0.5. Measure CAF / GIN coffee before 0.4.0 runs — Pete sets the pin

Decided 2026-10-07: the CAF/GIN coffee tonnage correction rides this run. It is currently a
**caveat** in `docs/methods/nominal_price_method.md`, not a correction: FAOSTAT coffee production for
the Central African Republic breaks from ~10 kt to ~300 kt after 2017 with no corresponding event,
and the ICO puts real output at 2-6 kt (CAF) and ~9 kt (GIN). FAO's constant-I$ GPV is built from
FAO's own production, so the break carries straight into the intld basis.

`metadata/fao_quantity_pins.csv` holds both rows with **`status = proposed` and `prod_t` blank**, so
0.4.0 will report them and change nothing. This gate produces the number Pete needs to fill in.

Read it straight off the FAOSTAT bulk files — seconds, no pipeline:

```
Rscript -e 'suppressMessages(library(data.table)); source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R");
d <- fread(file.path(fao_dir, "Production_Crops_Livestock_E_Africa.csv"), encoding = "Latin-1")
v <- fread(file.path(fao_dir, "Value_of_Production_E_Africa.csv"), encoding = "Latin-1")
yc <- paste0("Y", 2010:2023)
pr <- d[`Item Code` == 656 & Element == "Production" & Unit == "t" & Area %in% c("Central African Republic","Guinea"), c("Area", yc), with = FALSE]
gp <- v[`Item Code` == 656 & Element %like% "constant 2014-2016" & Area %in% c("Central African Republic","Guinea"), c("Area", yc), with = FALSE]
cat("
production (t)
"); print(pr); cat("
GPV (thousand I$)
"); print(gp)'
```

> Check the production file name against what is actually staged in `fao_dir`; 0.4.0 resolves it as
> `prod_file`. If the GPV `Element` string does not match, print `unique(v$Element)` and use the
> constant-I$ one.

**Report the full 2010-2023 series for both countries, production and GPV.** State where the break
is, what the pre-break level was, and the implied I$/t on each side of it — an implied unit value
that barely moves across a 30x tonnage jump is the signature of a reporting change rather than a
real one.

**Then STOP and hand the numbers to Pete.** Do not set `prod_t` or flip `status` yourself: a pinned
quantity is a hand-set number that moves a published figure, and it needs the same evidence
discipline as `metadata/price_pins.csv`. Once Pete sets the value and `status = applied`, commit it
and continue to A1 — 0.4.0 prints every applied pin with before, after and ratio, and **stops** if a
pin matches no FAOSTAT row.

If Pete is not available, run A1 onward with the pins left `proposed`: the result is simply today's
behaviour plus the B5 fix, and the pins can be applied in a later 0.4.0 pass. Say clearly in the
RESPONSE which of the two you did.

### A1. Re-derive the gap under both join keys — the number, not the claim

0.4.0 now prints this itself, every run, via `vop_name_join_audit()`. **The 53.6 B I$ figure has
never been re-derived on real data** (the macbook has no FAOSTAT bulk files), so this gate is where
it becomes a measurement rather than an assertion.

Run 0.4.0 at **`EXPOSURE_RES=0.05`** first and capture the audit block:

```
nohup env EXPOSURE_RES=0.05 FORCE_OVERWRITE=1 Rscript -e 'source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); source("/home/jovyan/atlas/hazards_prototype/R/0.4.0_create_crop_vop_intld15.R")' \
  > logs/b5_040_res05_$(date +%Y%m%d_%H%M%S).log 2>&1 &
```

> `R/0.4.x` scripts need setup sourced first, by absolute path, because setup runs `setwd()`
> (AGENTS.md §2). Background it: this is far longer than the shell's ~2-minute foreground limit.

**Report from the log:**
1. The `B5 join audit:` line — GPV matched by code, by name, recovered, and the number of renamed
   item codes. **State it as what it is; do not reconcile it to 53.6 B.** If it comes out materially
   different from ~53.6 B I$ / 59 codes, that is a finding: say so and stop before 0.4.4.
2. The per-group table of value recovered, and the list of renamed codes with both spellings.
3. The `allocation table:` line — guarded pairs and their share. **Invariant: the guarded share
   must not rise much.** Recovering value for items SPAM has little of could newly trip the #39
   coverage guard; a large jump in guarded value is a finding, not a pass.
4. The `price factor check:` line (new, #41) — production × factor must reproduce the VoP raster.
   0.4.0 aborts if it does not, so its presence in the log is the evidence.

### A2. Both resolutions

Repeat A1 at `EXPOSURE_RES=0.25`, **also with `FORCE_OVERWRITE=1`** — 0.4.0 gates its VoP rasters on it (`:285`, `:305-306`), so without the flag the audit and factor raster are rewritten while the rasters themselves are not, and A3 then re-bakes the reference from pre-B5 inputs. Confirm the audit numbers are the same (the join is national and
grid-independent — if the recovered value differs between the two grids, something is wrong) and
that the guarded set is the one legitimate difference, since the coverage guard is grid-dependent
(the BEN/NGA cowpea lesson: BEN SPAM cowpea is 5.8 kt at 0.05° and 25.1 kt at 0.25°).

Confirm both factor rasters exist:
`<mapspam_pro_dir>/fao_prices/crop_factor_intld15-2021-t_res-05.tif` — **one file, not two.** The
factor is built on the allocation grid, which is pinned to 0.05° whatever `EXPOSURE_RES` says,
because pricing has to happen on the fine grid. The filename tag is derived from the raster, so both
runs write the same path.

### A3. 0.4.4, both resolutions

Re-run `R/0.4.4_process_exposure.R` at both resolutions so the reference and the family twins pick
up the new intld values. `FORCE_OVERWRITE=1` — 0.4.4 gates its outputs and would otherwise read the
cached per-tif parquets. **Verify `FORCE_OVERWRITE` is actually in the environment before launching;
a silent skip produces a stale output that looks current.**

**Invariant:** 0.4.4 §1 refuses to run if an untagged legacy twin of a tagged raster is on disk. If
it stops there, move the twin aside as the error says — do not delete it.

### A4. Gates before any publish

In this order, and stop at the first that deviates:

1. `Rscript R/qaqc_vop_vs_faostat.R` — crop national-total ratios. **Invariant: median near 1.** As
   above, ~1.08 means the gate is on the old basis.
2. `Rscript R/checks/vop_cross_basis_gate.R` on the 0.4.4 tables (nominal / intld per pair, with the
   world-price reference). Expected residuals are in `metadata/cross_basis_expected_residuals.csv`
   and border spill is reported, not failed. **New residuals that are not in that file are a
   finding** — they need a reason and a row before they are accepted, not a pass.
3. `Rscript R/checks/probe_040_allocation.R`.

**Report the before/after continental intld total** (0.4.0's `allocation check:` line carries it)
and the per-group move for the five groups the macbook expects to move most: vege, rest, trof,
orts, temf. An increase concentrated in those groups is the fix working; a move in a single-item
group like maize or wheat is not, and is worth stopping on.

### A5. Republish — reference + family, both resolutions

Only after A4 is clean. Use **`scripts/r3_publish_tiers.R`** and nothing else:

```
Rscript scripts/r3_publish_tiers.R --reference-only --res 0.05 --allow-unit-vintage-change
Rscript scripts/r3_publish_tiers.R --reference-only --res 0.25 --allow-unit-vintage-change
Rscript scripts/r3_publish_tiers.R --family-only    --res 0.05 --allow-unit-vintage-change
Rscript scripts/r3_publish_tiers.R --family-only    --res 0.25 --allow-unit-vintage-change
```

Run each with `--dry-run` first and report the gate output before the live write. Background the
live ones and log to `logs/` — a publish was SIGTERM'd mid-run on 2026-09-28.

`--allow-res-change` was needed only for the first publish of a suffixed key; those keys are live
now, so **do not pass it**. Do not pass `--allow-schema-drift` or `--allow-value-drift`.

### A6. Confirm, independently

Diff local against S3 after the publish — the uploader trusts per-file returns and always
overwrites, so a stale key needs an explicit delete. Query the published keys in place with DuckDB
httpfs rather than downloading (`aws s3 cp` has produced corrupt files; verify md5 against ETag if
you do download). Report the continental intld total read **back off S3**, not off disk.

---

## What NOT to do

- **Do not run R/2 or R/3.** The #13 bake is a separate dispatch, after this one.
- **Do not run `R/s3_upload.R`** or the derive script. Its intld route is retired as of `f0cc32d`
  and will stop you with an explanation.
- **Never `s3fs::s3_file_delete()` / `s3_dir_delete()`** — they delete every version by VersionId
  and leave no delete marker, so they are PERMANENT. Back up first; for a recoverable delete use
  `paws` `delete_object()` without a VersionId.
- **Do not improvise a fix** at any gate. If anything deviates from the stated expectation, stop at
  that step and describe what you see.

## Reporting

Prepend a `### RESPONSE <date>` block with the numbers for A0-A6, then verify it landed on
`origin/develop`: `git fetch`; `git log origin/develop..HEAD` empty;
`git show origin/develop:DISPATCH_cglabs_b5_fao_code_join.md | grep -c RESPONSE`.

Commit trailer: `Co-Authored-By: <your own model name> <noreply@anthropic.com>`.
