# DISPATCH — cglabs — B5: FAO code join, 0.4.0 → 0.4.4 → reference + family republish

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

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
