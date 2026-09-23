# Dispatch: issue #30 — intld15 vintage, re-run 0.4.4 and re-gate

**Status:** code fix `99fdf73` has been on `develop` since 2026-09-18 and has never been
run. This dispatch runs it and re-gates. **It stops before publishing.** Nothing here
writes to S3.

**What is already settled**, so you do not re-derive it:

- The 6,759x ratio is **not** a stale-artifact problem. The 1,728 old interaction tifs are
  per-GCM and both §4.1 and §4.2 filter with `ensemble_only`, so they never enter a group.
  Every tif the product is built from is fresh.
- Root cause is `R/0.4.4_process_exposure.R` §3.1: an allow-list keyed on unit *values*
  plus a rename to vintage-less *names*. Producers moved to `-2021`, nothing matched, and
  a legacy on-disk vintage survived under the label `intld15`. Fixed in `99fdf73`:
  explicit `EXPOSURE_UNITS`, no rename, hard stop on a missing unit.
- The `1.198` livestock ratio is a **different defect** and is not addressed here. It was
  measured on the *published* `crop-livestock_all.parquet`, which predates the 2026-07
  livestock const-I$ fix (`05c0b0e`). Re-running 0.4.4 does not touch it; only a republish
  does. Do not expect this dispatch to move that number.

**New in this dispatch:** `R/0.4.4_process_exposure.R` gains a **§3.3**, the constant
international dollar twin of §3.2. It writes
`<exposure_dir>/vop_intld15-2021_adm_sum_spam20_glw420.parquet`. This gives a producer to
the S3 object `variable=vop_intld15-2021.parquet`, which has been live since 2025-11-03
with nothing in this repo maintaining it. Like §3.2 it is rebuilt from the **raw
extractions** and filters the source unit, so it never touches §3.1's combined table —
that separation is exactly what kept nominal USD correct while the combined table drifted.

---

## Before you start: two landmines

**1. §3.1 is skip-if-exists and will silently not rewrite.**

```r
if(!file.exists(file)|overwrite_glw|overwrite_spam){
```

`overwrite_glw` / `overwrite_spam` are both `atlas_env_flag("FORCE_OVERWRITE", strict=TRUE)`.
The combined parquet already exists on the node, so with `FORCE_OVERWRITE` unset §3.1 is
skipped entirely and you get a green run with the old file still in place — the exact
failure mode this issue is about.

Setting `FORCE_OVERWRITE=1` is **not** the fix: that also forces sections 1 and 2 to
re-extract every MapSPAM and GLW raster, which is hours. Instead **move the combined
parquet aside** (Block C). Sections 1 and 2 then reload their per-file parquets from disk
rather than re-extracting, and §3.1 rebuilds because its output is absent.

**2. FAO bulk CSVs are no longer fetched by setup.** `7ffd85f` moved them out of
`0_server_setup.R` into on-demand `R/00_acquire.R`. Setup now only *defines* `vop_file`.
If the CSVs are not on disk, 0.4.0 / 0.4.1 / `qaqc_vop_vs_faostat.R` fail on a missing
file where they used to self-heal. Block A checks this.

---

## Block A — read-only audit (~1 minute)

Decides whether 0.4.4 will run at all. The new §3.1 **aborts by design** if the expected
vintages are not on disk, so establish that first rather than discovering it mid-run.

```bash
cd <hazards_prototype>
git fetch origin && git checkout develop && git pull --ff-only
git log --oneline -1                      # expect the §3.3 commit at or near HEAD
git merge-base --is-ancestor 99fdf73 HEAD && echo "99fdf73 present"

Rscript -e '
  source("R/0_server_setup.R")
  vop <- list.files(atlas_dirs$data_dir$exposure, recursive = TRUE, pattern = "vop.*\\.tif$", full.names = FALSE)
  cat("--- VoP rasters on disk ---\n"); print(sort(basename(vop)))
  cat("--- FAO bulk present? ---\n"); print(file.exists(vop_file))
'
```

**Report back:** the raster list and the `vop_file` boolean.

**What you are looking for.** Filenames carrying `intld15-2021` and `nominal-usd-2021`.

- Both present → proceed to Block C.
- Either missing, but an **older** vintage is present → **stop and report the exact
  names.** Do not set `EXPOSURE_UNITS` to the older vintage to get past the abort. The
  abort is the feature; the answer is a 0.4.0 / 0.4.1 re-bake, and that is Pete's call.
- `vop_file` is `FALSE` → run Block B first.

## Block B — FAO bulk prereq, only if Block A says `FALSE`

```bash
Rscript -e 'source("R/0_server_setup.R"); atlas_acquire("faostat-bulk")'
```

Note `R/checks/vop_align_live_gate.R` deliberately does *not* use this — it downloads the
FAO zip itself so it runs on any machine. That divergence is intended, not an oversight.

## Block C — re-run 0.4.4 (writes; expect tens of minutes, not hours)

Sections 1 and 2 reload from their per-file parquets because `FORCE_OVERWRITE` stays
unset. Only §3.1 and §3.3 do real work.

```bash
cd <hazards_prototype>
Rscript -e 'source("R/0_server_setup.R"); cat(exposure_dir, "\n")'   # note this path

# Move the combined parquet + its sidecar aside so §3.1 rebuilds. Keep the backup.
STAMP=$(date +%Y%m%d-%H%M%S)
cd <exposure_dir>
mkdir -p _pre30_backup
mv exposure_adm_sum_spam20-20_glw420-20.parquet      "_pre30_backup/exposure_adm_sum_spam20-20_glw420-20.parquet.$STAMP"
mv exposure_adm_sum_spam20-20_glw420-20.parquet.json "_pre30_backup/exposure_adm_sum_spam20-20_glw420-20.parquet.json.$STAMP" 2>/dev/null || true

cd <hazards_prototype>
nohup Rscript R/0.4.4_process_exposure.R > logs/0.4.4_issue30_$STAMP.log 2>&1 &
```

**Watch for these lines and report them verbatim** — they are the whole point of the run:

```
section 3.1: units present in extraction = ...
section 3.1: units kept (EXPOSURE_UNITS) = number, ha, t, nominal-usd-2021, intld15-2021
section 3.1: units DROPPED = ...
section 3.3: unit kept = intld15-2021 (present in extraction: ...)
section 3.3: N rows, M crops
```

If §3.1 aborts with *"expected unit(s) absent from the extraction"*, that is the guard
working. **Stop and report.** Do not set `EXPOSURE_UNITS_LENIENT=1` to push past it — that
flag exists for a deliberate partial run, not for getting a red run to go green.

Then confirm both artifacts carry the vintage, with no rename:

```bash
Rscript -e '
  suppressPackageStartupMessages({library(arrow); library(data.table)})
  source("R/0_server_setup.R")
  for (f in c("exposure_adm_sum_spam20-20_glw420-20.parquet",
              "vop_intld15-2021_adm_sum_spam20_glw420.parquet")) {
    p <- file.path(exposure_dir, f)
    d <- as.data.table(read_parquet(p, col_select = c("exposure","unit")))
    cat("\n==", f, "==\n"); print(d[, .N, by = .(exposure, unit)][order(exposure, unit)])
  }
'
```

Expect `intld15-2021` and `nominal-usd-2021`, and **no** bare `intld15` or `usd`.

## Block D — re-gate (read-only, ~2 minutes + ~15 seconds)

```bash
Rscript R/checks/usd_total_vs_reference.R                 # local product vs local reference
Rscript R/checks/vop_align_live_gate.R                    # PUBLISHED artifact vs FAOStat
```

**Expected, and the difference matters:**

- `usd_total_vs_reference.R` must log `reference unit matched = intld15-2021` with **no**
  "matched on `intld15`" warning, and the intld side should come back in band. This is the
  gate that read 6,759x. Both sides are local files; it never touches S3.
- `vop_align_live_gate.R` reads S3 and therefore **will not change**: crop ~1.007,
  livestock ~1.198, unit still `intld15`. That is correct and expected, because nothing has
  been republished. If it *does* change, something republished without authorisation —
  stop and report.

**Report back:** the full output of both, and specifically which unit string each named.

---

## STOP HERE

Publishing is a separate, authorised step and is **not** part of this dispatch. For the
record, when Pete authorises it, it is:

```bash
Rscript scripts/r3_publish_tiers.R --reference --allow-unit-vintage-change
```

`--allow-unit-vintage-change` is needed because the `intld15 -> intld15-2021` move fails
the publisher's `distinct(unit)` gate by design. It still refuses to lose or invent a unit:
cardinality must match, because a vanishing unit is how this issue hid for months. Do not
reach for `--skip-gates`, which would wave through the schema and row checks too.

Two things that are **not** fixed by publishing the above and need their own decision:

1. No uploader in this repo writes the `type=combined` **per-unit** keys —
   `scripts/r3_publish_tiers.R` ships `crop-livestock_all` only. So the new §3.3 output
   refreshes the local artifact while the S3 object `variable=vop_intld15-2021.parquet`
   stays stale. `variable=vop_nominal-usd-2015.parquet` (read live by
   `R/checks/19_exposure_vs_gyga_kenya.R:279` and `R/checks/24_severity_sensitivity.R:118`)
   has no producer at all.
2. The published `crop-livestock_all` carries `unit_full` (`intld15-2021`,
   `nominal-usd-2021`) alongside the vintage-less `unit`. **0.4.4 emits no `unit_full`** —
   see `scripts/r3_publish_tiers.R:29`. Something between the producer and the canonical
   object is adding it. Until that is identified, a re-published file may *lose* a column
   consumers are using. Worth establishing before the publish, not after.

---

## cglabs response — Blocks A→D complete, STOPPED before publish (2026-09-20)

Ran on `develop` @ `0ddfa53` (`99fdf73` present). **Nothing written to S3.**

### Block A — audit
- `vop_file = TRUE` → FAO bulk on disk, Block B skipped.
- The audit one-liner returns `character(0)` **only because it lists `atlas_dirs$data_dir$exposure`** (= `Data/exposure`, holds knbs parquets only). The real VoP source tifs live in `mapspam_pro_dir` / `glw2020_pro_dir`. Both target vintages present there: `intld15-2021` + `nominal-usd-2021`, crop and livestock. Suggest fixing the Block A command to list those dirs.
- **Caveat found:** MapSPAM `intld15-2021` crop is baked for only **3/6 technologies** (`all,irr,rf-all`; missing `rf-highinput/lowinput/subsistence`); `nominal-usd-2021` has all 6. Does not block — the gate reads `tech=="all"`, present — but see row counts below. Looks like a partial 0.4.0/0.4.1 intld bake.

### Block C — re-run 0.4.4
Moved stale combined parquet + sidecar (mtime 2026-07-07) to `Data/exposure/_pre30_backup/` (stamp `20260920-162700`). Run completed clean in **~3 min** — sections 1/2 reloaded per-file parquets from the `variable=…/` subdirs (no re-extraction). §3.1/§3.3 verbatim:

```
section 3.1: units present in extraction = ha, intld15, intld15-2015, intld15-2020, intld15-2021, nominal-usd-2015, nominal-usd-2020, nominal-usd-2021, nominal-usd15, nominal-usd21, number, t, usd2015
section 3.1: units kept (EXPOSURE_UNITS) = number, ha, t, nominal-usd-2021, intld15-2021
section 3.1: units DROPPED = intld15, intld15-2015, intld15-2020, nominal-usd-2015, nominal-usd-2020, nominal-usd15, nominal-usd21, usd2015
section 3.3: unit kept = intld15-2021 (present in extraction: ...)
section 3.3: 541130 rows, 42 crops
```

Legacy bare `intld15` dropped; no abort, no rename. Post-run unit table:

```
== exposure_adm_sum_spam20-20_glw420-20.parquet ==
   vop  intld15-2021      541130
   vop  nominal-usd-2021 1061840   (+ ha/number/t rows)
== vop_intld15-2021_adm_sum_spam20_glw420.parquet ==
   vop  intld15-2021      541130
```

No bare `intld15`, no bare `usd`. The vop split 541130 vs 1061840 (~½) is the 3/6-tech gap showing in row counts.

### Block D — gates
- **`usd_total_vs_reference.R` → PASS.** `reference unit matched = intld15-2021 (accepted intld15-2021,intld15,intld15-2020 | present intld15-2021,nominal-usd-2021)` — matched preferred vintage, **no** "matched on `intld15`" warning. intld: 99 material pairs, **median ratio 1.000, range [0.9598, 1]**. The 6,759× gate is fixed.
- **`vop_align_live_gate.R` → UNCHANGED, as predicted.** unit=`intld15` (NOTE: vintage-less pre-fix survivor). CROP **PASS median 1.007**; LIVESTOCK **FAIL median 1.198**; OVERALL FAIL. Live object untouched (Last-Modified 2026-01-21) → **nothing republished**. (Node had no `duckdb` CLI — conda-forge `duckdb` pkg is a metapackage with no binary; ran via `python-duckdb` + a `-csv -c` shim. Recommend installing a real duckdb CLI on the node for future gate runs.)

### Answer to the open question — what adds `unit_full`
**0.4.4 itself used to.** Commit `bb5c5f7` §3.1:
```r
exposure_adm_sum_tab[,unit_full:=unit]                      # keep vintage-ful copy
exposure_adm_sum_tab[unit==units[k],unit:=names(units)[k]]  # then flatten unit to vintage-less
```
`unit_full` = preserved-vintage column; `unit` got flattened — the flattening is exactly what hid #30. Fix `99fdf73` deleted **both** lines. So old 0.4.4 = `unit` vintage-less + `unit_full` vintage-ful; fixed 0.4.4 = `unit` vintage-ful, **no `unit_full`**. A republish is semantically clean but **drops `unit_full`**. In-repo consumer = `R/misc/rebake_parquets_for_pushdown.R` (sorts/verifies on `unit_full`, would break); `R/3_freq_x_exposure.R:386` is comment only. External consumers unknown. **Decide before publish:** emit `unit_full` as a copy of `unit` for back-compat, or drop deliberately.

### Awaiting Pete
No publish performed. Blockers for the authorised publish: (1) intld15-2021 crop 3/6-tech partial bake; (2) `unit_full` drop-on-republish. Both are your call.

---

## macbook response — develop-vs-main audit on 0.4.4 (2026-09-22)

Read-only, local. Two of the response's conclusions are wrong, and both change a decision.

### The published reference is produced from `main`, not `develop`

`unit_full` does not come from a line that `99fdf73` deleted. It was never on this branch:

- `bb5c5f7` is on **origin/main only** — `git merge-base --is-ancestor bb5c5f7 develop` returns NO.
- `git log -S"unit_full" -- R/0.4.4_process_exposure.R` on develop is **empty**, and `99fdf73^`
  contains no `unit_full`. `99fdf73` removed the flatten loop and the 2020 allow-list; there was
  no `unit_full:=unit` here to remove.
- **No script anywhere on develop assigns `unit_full`.** `R/misc/rebake_parquets_for_pushdown.R`
  only sorts and verifies on it, and its own comment names 0.4.4 as the producer.

main's 0.4.4 both preserves `unit_full` and flattens `unit`, which is exactly the
`unit=intld15` / `unit_full=intld15-2021` pairing on the live object. So the published
denominator comes off the branch that is 21 commits behind on a separate line.

### How far behind that is

Merge base is `31d988e` (2025-08-21). Since then 0.4.4 has **one** commit on main (`bb5c5f7`,
2025-09-05) against **ten** on develop. Feature-by-feature, `git show <ref>:R/0.4.4_process_exposure.R`:

| feature | main | develop |
|---|---|---|
| `method="sum"` mass-conserving resample (#9) | absent | present |
| `compareGeom` guard | absent | present |
| `write_parquet_pushdown` | absent (plain `arrow::write_parquet`) | present |
| `FORCE_OVERWRITE` / `atlas_env_flag` gating | absent | present |
| `EXPOSURE_UNITS` (#30 fix) | absent | present |
| timestamped logging | absent | present |
| furrr parallelism | absent | present |
| `unit_full` | **present** | absent |

**Do not read this as "the live values are wrong".** The live gate reconciles live crop against
FAOSTAT at 1.007, so the crop side of the published denominator is sound on its own terms. What
this establishes is that the fix branch and the publishing branch are different code, and that a
republish from develop is a schema change plus a year of behaviour change in one step.

(The livestock 1.198 is resolved by measurement in the double-check section below - it is
the pre-`05c0b0e` 0.4.1 mislabel, live.)

### The 3/6-tech gap is by design, not a partial bake

`R/0.4.0_create_crop_vop_intld15.R:187-200` splits VoP into irrigated / rainfed only, writing
`_irr` and `_rf-all`. `R/0.4.2_create_crop_vop_nominal_usd.R:13-14` disaggregates across all four
SPAM technologies. The 541,130 vs 1,061,840 row split is the two producers doing different
things by design. **Nothing to re-bake.** Whether intld *should* carry the full tech breakdown is
a feature question on 0.4.0, and it does not block a publish.

### Revised decisions for Pete

1. **Branch divergence, before any republish.** Publishing from develop replaces a main-produced
   object with develop-produced output: `unit` becomes vintage-ful, `unit_full` disappears, and
   the writer changes. `R/misc/rebake_parquets_for_pushdown.R:149-150` sorts and verifies on
   `unit_full`; on a missing column it **warns and skips**, it does not break (`reorder_table`
   L310-320, `verify_stats` L272-278). **Superseded** - see the double-check section below for the
   proportionate fix; a branch reconciliation is not needed for this.
2. **Tech coverage for intld** — leave at irr/rf-all, or extend 0.4.0. Not blocking.

### Correction owed to cglabs

Block A's audit command listed `atlas_dirs$data_dir$exposure` and so returned `character(0)`.
It should list `mapspam_pro_dir` / `glw2020_pro_dir`. My error; the workaround was correct.

---

## macbook double-check of the above (2026-09-22, later)

Re-verified every recommendation adversarially. Two were wrong, one was overstated, and the
probes closed the livestock question.

### Livestock 1.198 — CLOSED by measurement

Live `crop-livestock_all`, admin0, `tech IS NULL`, AGO/KEN/ETH/NGA, every `*-tropical`
species-system: the `intld15` value **equals the `usd` value to the dollar**, and equals the
sibling `variable=vop_nominal-usd-2021.parquet` too (20/20 pairs, ratio 1.0000). The published
"constant international dollar" livestock rows are nominal USD relabelled. That is the
pre-`05c0b0e` 0.4.1 bug (`vop_usd_nominal` written under every label) baked into the live
denominator. 1.198 = nominal-2021 ÷ FAO constant-2015-I$, species-structured because price
inflation differs by species. Not a new defect; republishing from the post-fix chain fixes it.

### main never had the #30 bug

main's 0.4.4 allow-list (`bb5c5f7`) is already `usd="nominal-usd-2021", intld15="intld15-2021"`
- correct vintages, flattened, with `unit_full` preserved. develop's pre-fix map stayed at the
2020 vintages and would have **dropped every 2021 row**, so develop could not have produced the
live object under any reading. Provenance from main is now airtight, and #30 is a
develop-only regression created when develop diverged at `31d988e` and never received
`bb5c5f7`. develop's `99fdf73` supersedes the map part (explicit list, no flatten, hard stop);
the only thing develop still lacks relative to main's 0.4.4 is the `unit_full` column.

### Corrections to my earlier section

- **"would break"** was wrong. `rebake_parquets_for_pushdown.R` warns and skips a missing sort or
  verify column. Fixed in place above.
- **"settle develop-vs-main on 0.4.4 first"** was overstated. The 0.4.4 gap is one commit whose
  substance develop already supersedes. Proportionate fix: emit `unit_full := unit` in §3.1/3.2/3.3
  on develop - zero information loss now that `unit` carries the vintage, and it restores the
  column consumers read.
- For the record, **neither branch** has an in-repo publisher for the `type=combined` parquets.
  main's `push_to_s3.R` uploads the VoP **rasters** to the mapspam/glw processed prefixes, not
  these tables. `rebake_parquets_for_pushdown.R` says "renamed at publish time" - a step outside
  this repo.

### What WILL fail at publish, measured against the live object

`scripts/r3_publish_tiers.R --reference` gates require identical column sets and rows within
25 %. Both fail on the node's new output:

1. **Columns.** Live carries `unit_full` **and** five hive columns (`domain, processing, region,
   source, type`) that 0.4.4 has never written on either branch - the extraction appends only
   `exposure, unit, tech`. `unit_full := unit` closes one; the five need a decision (add them in
   §3.1, or add them in the publisher).
2. **Rows.** Live `vop/intld15` = 768,818, node = 541,130 (0.704). Same ratio on usd
   (1,508,624 → 1,061,840). Crop count is identical (42 = 32 crop + 10 livestock both sides), so it
   is not coverage. **NA-dropping is ruled out arithmetically:** live is 53 % NULL / 23 % zero /
   23 % real; dropping NULLs would give 0.47, dropping NULLs+zeros 0.23, neither 0.70. Neither
   branch's 0.4.4 drops NAs, and `admin_extract_wrap` is functionally identical across branches.
   Lead: develop rewrote the inner `admin_extract` kernel (`R/haz_functions.R`, 143 deletions).

### Ask for cglabs (read-only, on the new §3.1 output)

```r
library(arrow); library(data.table); source("R/0_server_setup.R")
d <- as.data.table(read_parquet(file.path(exposure_dir, "exposure_adm_sum_spam20-20_glw420-20.parquet")))[exposure == "vop"]
d[, .(n = .N, null_rows = sum(is.na(value)), zero_rows = sum(value == 0, na.rm = TRUE),
      admin_units = uniqueN(fcoalesce(gaul2_code, gaul1_code, gaul0_code)), countries = uniqueN(iso3),
      adm0 = sum(is.na(admin1_name)), adm1 = sum(!is.na(admin1_name) & is.na(admin2_name)), adm2 = sum(!is.na(admin2_name))), by = unit]
d[, .N, by = .(unit, crop, tech)][order(unit, crop, tech)]
```

Live comparators: intld15 → 7,245 admin units, 55 countries, adm0/adm1/adm2 rows
5,830 / 75,896 / 687,092. Report the same for the new file so the 0.70 is explained before
anyone touches the publisher.

### Separate lead, not #30 - do not chase here

Live nominal-USD pearl-millet is ~6 kUSD for KEN and ~52 kUSD for ETH against 27 M and 310 M I$
respectively; the published hazard product carries the same tiny values (KEN any+none ≈ 5.9 k), so
product and denominator agree and the exposure *fraction* is fine, but the *absolute* nominal value
is wrong for that crop. Smells like a 0.4.2 price-fill miss on pearl-millet. Candidate for its own
issue.


---

## Block E — schema fix + row profile (2026-09-22, writes locally, nothing to S3)

> **Premise corrected 2026-09-23 (see the macbook response after cglabs's Block E report):** the
> five hive columns are NOT stored in the live file and the column gate was never going to pass
> with them. `595090a` is reverted in `c44a49e`; `unit_full` stays. The `live_cols` list below is
> therefore 5 columns too long - the stored live schema is 14.

Pete decided: `unit_full := unit` in §3.1/3.2/3.3, and the five hive columns in §3.1. Both
are on `develop` now. This block re-runs 0.4.4 with them and profiles the output against the
live object so the 0.704 row ratio gets explained. **Still no publish.**

**Landmine, again:** §3.1 and §3.3 are skip-if-exists and their outputs now exist from Block C.
Move them aside first, exactly as in Block C. Do not use `FORCE_OVERWRITE`.

```bash
cd <hazards_prototype>
git fetch origin && git checkout develop && git pull --ff-only
git log --oneline -3          # expect "feat(0.4.4): unit_full + hive columns" at or near HEAD

STAMP=$(date +%Y%m%d-%H%M%S)
cd <exposure_dir>
mkdir -p _pre30_backup
for f in exposure_adm_sum_spam20-20_glw420-20.parquet vop_intld15-2021_adm_sum_spam20_glw420.parquet vop_nominal-usd-2021_adm_sum_spam20_glw420.parquet; do
  [ -e "$f" ]      && mv "$f"      "_pre30_backup/$f.$STAMP"
  [ -e "$f.json" ] && mv "$f.json" "_pre30_backup/$f.json.$STAMP"
done
cd <hazards_prototype>
nohup Rscript R/0.4.4_process_exposure.R > logs/0.4.4_issue30_E_$STAMP.log 2>&1 &
```

Expect a new line `section 3.1: 19 columns -> iso3, ..., type`. Then, once the run exits 0:

```bash
Rscript -e '
  suppressPackageStartupMessages({library(arrow); library(data.table)})
  source("R/0_server_setup.R")
  live_cols <- c("iso3","admin0_name","admin1_name","admin2_name","gaul0_code","gaul1_code","gaul2_code",
                 "crop","value","stat","exposure","unit","tech","unit_full","domain","processing","region","source","type")
  p <- file.path(exposure_dir, "exposure_adm_sum_spam20-20_glw420-20.parquet")
  cols <- names(arrow::open_dataset(p)$schema)
  cat("local-only:", setdiff(cols, live_cols), "\nlive-only :", setdiff(live_cols, cols), "\n")   # both must be empty
  d <- as.data.table(read_parquet(p))[exposure == "vop"]
  print(d[, .N, by = .(domain, type, source, region, processing)])                                 # one row, live values
  print(d[, .(n = .N, null_rows = sum(is.na(value)), zero_rows = sum(value == 0, na.rm = TRUE),
              admin_units = uniqueN(fcoalesce(gaul2_code, gaul1_code, gaul0_code)), countries = uniqueN(iso3),
              adm0 = sum(is.na(admin1_name)), adm1 = sum(!is.na(admin1_name) & is.na(admin2_name)),
              adm2 = sum(!is.na(admin2_name))), by = unit])
  print(d[unit == "intld15-2021", .N, by = .(crop, tech)][order(crop, tech)])
'
```

**Live comparators for the intld15 unit** (from the published `crop-livestock_all`, 2026-09-22):
768,818 rows · 409,254 NULL · 180,266 zero · **7,245 admin units** · 55 countries ·
adm0/adm1/adm2 rows **5,830 / 75,896 / 687,092** · 32 crops × 3 techs + 10 livestock (`tech` NULL).

**Report back:** both `setdiff`s (must be empty), the hive-value row, the per-unit profile, and
the per-crop × tech counts. The question the profile answers: where do the ~228k missing intld
rows go - fewer admin units, fewer adm2 rows, or fewer NULL rows per crop? Do not guess; the
numbers say.

Then the dry run of the publisher, which is read-only and will show what the gates now say:

```bash
Rscript scripts/r3_publish_tiers.R --reference-only --allow-unit-vintage-change --dry-run
```

**Expect** the column gate to pass and the row gate to still FAIL on 0.704 until the profile
explains it. Paste the gate output verbatim. **STOP.** No publish.

---

## cglabs response — Block E complete, STOPPED (2026-09-22)

Ran 0.4.4 @ `4fbfe3b` (`595090a` present), FORCE_OVERWRITE unset. Moved all 3 parquets +
sidecars to `_pre30_backup/` (stamp `20260922-114812`). New schema line verbatim:

```
section 3.1: 19 columns -> iso3, admin0_name, admin1_name, admin2_name, gaul0_code, gaul1_code, gaul2_code, crop, value, stat, exposure, unit, tech, unit_full, domain, type, source, region, processing
```

### Profiling one-liner
```
local-only:            (empty)
live-only :            (empty)
--- hive row ---
  domain=exposure  type=combined  source=glw4-2020_spam2020AA  region=ssa  processing=atlas-harmonized   N=1602970
--- per-unit profile ---
  unit              n        null_rows  zero_rows  admin_units  countries  adm0    adm1    adm2
  intld15-2021      541130   255347     140274     5093         55         6678    74200   460252
  nominal-usd-2021  1061840  259100     527998     5093         55         13104   145600  903136
```
Per-crop × tech (intld15-2021): uniform **5,105 rows/combo**, 32 crops × {all,irr,rf-all} +
10 livestock (tech NULL) = 106 combos = 541,130. No per-crop sparsity.

### The 0.704, explained (measured, not guessed)
It is **admin-unit count, entirely in adm2.** Node 5,093 admin units vs live 7,245 →
5093/7245 = **0.703**. adm2 deficit = live 687,092 − node 460,252 = **226,840** ≈ the ~228k
missing rows. Same 55 countries, same 42 crops, same 3-tech structure. NULL/zero do **not**
explain it (node has *fewer* NULLs: 255k vs live 409k). The node's admin boundaries carry
fewer adm2 polygons than whatever produced the live object.

### Publisher dry-run (verbatim)
```
FAIL columns: local-only=[domain,processing,region,source,type] live-only=[]
ok: distinct(exposure) identical = harv-area,number,phys-area,prod,vop
unit vintage change ALLOWED (--allow-unit-vintage-change), 5 -> 5 units:
  live  = [ha,intld15,number,t,usd]
  local = [ha,intld15-2021,nominal-usd-2021,number,t]
  gone  = [intld15,usd]  new = [intld15-2021,nominal-usd-2021]
ok: distinct(stat) identical = sum
FAIL rows: local 5584870 vs live 7847746 (>25% apart)
ABORT reference: gate failure, nothing uploaded
```

### Two corrections to Block E's premise

**1. The column gate does NOT pass — `595090a`'s hive columns are the wrong fix.** The
profiling `setdiff` is empty only because the block's hardcoded `live_cols` list *includes*
the 5 hive columns. The real live **stored** schema is **14 columns** (verified with
`read_parquet(..., hive_partitioning=false)`): `iso3, admin0_name, admin1_name, admin2_name,
gaul0_code, gaul1_code, gaul2_code, crop, value, stat, exposure, unit, tech, unit_full`. The
5 hive columns (`domain, type, source, region, processing`) are **not stored in the file** —
DuckDB synthesizes them from the Hive-partitioned S3 key path
(`domain=…/type=…/source=…/region=…/processing=…/`). The publisher downloads to a flat temp,
reads the 14 stored columns, and correctly reports the node's 5 stored hive columns as
`local-only` → column gate FAIL. **Recommendation:** keep `unit_full := unit` (correct — live
stores it), but **revert the 5 hive columns** in §3.1 — they belong in the publish-time S3
key, and storing them in the file duplicates/conflicts with the path partitioning.

**2. Both gates FAIL, not just rows.** Columns fail on the 5 hive cols above; rows fail at
**0.712 total** (5584870 / 7847746). The `0.704` in the block is the vop-only ratio
(541130 / 768818) — the publisher gates on total rows. The row FAIL is the real adm2
boundary-count gap and is **not fixable from this repo's code**: the node needs the finer
admin boundary set (7,245 admin units) that produced the live object. Until the node extracts
against that boundary vintage, the reference publish row gate will keep failing regardless of
the schema fix.

### STOP — nothing published
New parquets local only (common_data); 3 prior outputs in `_pre30_backup/` stamp
`20260922-114812`. Nothing to S3.

### duckdb note (unchanged from Block D)
Node still has no real `duckdb` CLI; ran the schema checks via `python-duckdb` + a `-csv -c`
shim. Recommend installing a proper duckdb CLI on the node.

---

## macbook response to Block E (2026-09-23)

Verified both corrections read-only against S3 and git before accepting them. One holds
entirely, one holds in its measurement but not in its conclusion.

### 1. Hive columns - cglabs is right, `595090a` reverted (`c44a49e`)

`parquet_schema()` over the live object: **14 stored columns**, `unit_full` included, no
`domain/type/source/region/processing`. `read_parquet(..., hive_partitioning=false)` → 14;
`hive_partitioning=true` → 19. DuckDB synthesises the five from the `key=value/` path segments.
My earlier DESCRIBE ran with partitioning on and I took the 19 as stored. Wrong; reverted.
`unit_full := unit` stays in §3.1/3.2/3.3 - the live file does store that one.

### 2. The 0.704 - the measurement holds, the "not fixable from this repo" does not

Per crop × tech combo, verified on the live object (maize / intld15 / all):

| level | GAUL24 vector | live rows (codes) | node rows |
|---|---|---|---|
| adm0 | 55 | 55 (55) | **63** |
| adm1 | 719 (715 codes) | 716 (712) | 700 |
| adm2 | 6,670 (6,666 codes) | 6,482 (6,478) | 4,342 |
| total | 7,444 | 7,253 | 5,105 |

So the loss is adm1 **and** adm2, and adm0 *gains* 8 rows. Cause of the loss, established
on both branches:

- **Same vector.** `metadata/data.json` names the same GAUL24 analysis-ready parquets on main and
  develop (the diff is JSON formatting only).
- **Same extraction.** `admin_extract` in `R/haz_functions.R` is byte-identical across branches
  (34 lines; the 143-line diff in that file is other functions).
- **Different zonal grid.** `0_server_setup.R` §0.6: on `atlas_delta` (main's run)
  `base_rast_path = metadata/base_raster.tif`, **0.05°**; on `nexgddp` (develop's run)
  `base_rast_path = Data/base_rast.tif`, **0.25°**. 0.4.4 §0 rasterises the three GAUL24 layers
  onto that grid (`touches=TRUE`, `overwrite_boundary_zones<-T`, so no stale cache) and §1 then
  resamples SPAM 0.05° → 0.25° with `method="sum"`. At 0.25° roughly a third of adm2 polygons and
  16 adm1 polygons never own a cell, so they never get a row. Live already shows the same effect
  at 0.05° on a smaller scale (3 adm1 / 188 adm2 short of the vector).

That is a code-path choice inside this repo, not a missing boundary product. 0.4.4 has no reason to
use the **hazard** grid: it extracts exposure rasters that ship at 0.05°. **Fix direction, for
Pete to decide:** 0.4.4 §0 takes a 0.05° zonal base independent of `climdat_source` -
`metadata/base_raster.tif`, or the published pre-rasterised `gaul24_a{level}_res-05.tif`
(`data.json` → `raster_boundaries`, "rasterized to meet the resolution requirements of the
Adaptation Atlas (0.05 degrees) ... useful for zonal extractions") which skips the rasterise step
entirely. §1's `compareGeom` resample becomes extent-only. R/3 stays at 0.25° - that is the
hazard × exposure product and is a separate grid decision. **Cost:** §1 and §2 must re-extract on
the new grid, which is the hours-long path (per-file parquets aside or `FORCE_OVERWRITE=1`), and
the row count should land near the live 7,253/combo and pass the 25 % gate.

### 3. The +8 adm0 rows - unexplained, needs the node file

The vector has exactly 55 adm0 polygons and `admin_extract` is identical, so 63 rows with
`admin1_name` NA per combo did not come from either. Two candidates I cannot test from here: a
**stale local boundary parquet** under `boundaries_dir` (setup's download is skip-if-exists; an
older GAUL24 cut with disputed slivers as extra adm0 rows - the CR-115 pattern), or something in
the adm1/adm2 layers producing rows with NA `admin1_name`. Ask below.

### Decisions for Pete

1. **Zonal grid for 0.4.4:** move to 0.05° (`base_raster.tif` or the `res-05` rasters)? This is the
   actual fix for the row gate and for 2,140 adm2 units that currently would publish with no
   exposure rows. Recommend yes.
2. Nothing else changes: `unit_full` in, hive columns out, publish still held.

### Ask for cglabs (read-only, ~1 min)

```r
suppressPackageStartupMessages({library(arrow); library(data.table)}); source("R/0_server_setup.R")
d <- as.data.table(read_parquet(file.path(exposure_dir, "exposure_adm_sum_spam20-20_glw420-20.parquet")))
d <- d[exposure == "vop" & unit == "intld15-2021" & crop == "maize" & tech == "all"]
print(d[is.na(admin1_name), .(iso3, admin0_name, gaul0_code, gaul1_code, gaul2_code, admin2_name)][order(iso3)])   # expect 63 rows; which 8 are extra?
for (f in geo_files_local) cat(basename(f), "mtime", format(file.mtime(f)), "rows", nrow(arrow::read_parquet(f)), "
")
cat("zonal base:", base_rast_path, "res", paste(terra::res(terra::rast(base_rast_path)), collapse = "x"), "
")
```

Live comparators: GAUL24 on S3 a0/a1/a2 = 55 / 719 / 6,670 rows; live per-combo adm0 = exactly 55,
one per iso3. **Report the 63-row table and the three mtimes verbatim. STOP - no publish.**

