# DISPATCH — cglabs — B5: FAO code join, 0.4.0 → 0.4.4 → reference + family republish

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

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
nohup env EXPOSURE_RES=0.05 Rscript -e 'source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); source("/home/jovyan/atlas/hazards_prototype/R/0.4.0_create_crop_vop_intld15.R")' \
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

Repeat A1 at `EXPOSURE_RES=0.25`. Confirm the audit numbers are the same (the join is national and
grid-independent — if the recovered value differs between the two grids, something is wrong) and
that the guarded set is the one legitimate difference, since the coverage guard is grid-dependent
(the BEN/NGA cowpea lesson: BEN SPAM cowpea is 5.8 kt at 0.05° and 25.1 kt at 0.25°).

Confirm both factor rasters exist:
`<mapspam_pro_dir>/fao_prices/crop_factor_intld15-2021-t_res-05.tif` and `..._res-25.tif`.

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
