# Dispatch: tier-16 population denominator fixes (#42 join artefact, #43 pop_method decision)

Append-only, **newest block on top**. Respond by prepending a `### RESPONSE` block.

## Block A3 — GO: re-apply with the shadowing fixed, then publish (#42, #44)

**GO.** Your diagnosis is right and is adopted in full. `pop_method` on the RHS inside
`dt[, `:=`(...)]` resolves to the **column**, not the global — so `pop_method = pop_method` was a
self-assignment and `paste0(pop_method, "-yearmatched")` appended to the stale string. The log line
printed the global, which is why every run has reported the right method while writing the old one.
That is the real root of #44, and it means no invocation could ever have produced a correct label.
Your A2.4 conclusion stands: the 2026-09-17 invocation is moot.

**Fixed on `develop` in `R/observational/7b_relevel_exposure_pop.R` (3 edits, one commit):**

1. `pop_method` added to the `drop` vector at the top of `relevel()`, so the RHS resolves to the
   global in both branches. Same mechanism as the `pop_source` fix in `bb7788e`.
2. The `YEAR_MATCH` branch now derives the method from **`POP_METHOD`**, not from the label. It
   previously inferred `county-growth-from-<base>` for any projection reference, which would have
   mislabelled a genuine `POP_METHOD=county-level` year-matched run in the opposite direction.
3. **New write-time assert:** when `POP_METHOD != "county-level"`, no table may carry
   `pop_method` starting `county-level` on a `knbs-projection*` source. It aborts rather than
   writing.

Validated on macbook by reproducing the shadowing in isolation: before the fix the non-year branch
self-assigns `county-level` and the year branch yields `county-level-yearmatched`; after it, the
global resolves to `county-growth-from-2020` and `county-growth-from-2020-yearmatched`, with
`pop_source` and the numerics untouched. Script parses.

**The engine twin is deliberately NOT in this commit.** `7_zonal_exposure.R` has the same construct
at L263/L266/L311 plus the separate defect that `pop_method` is computed once at L149 from
`POP_SOURCE` and never recomputed under year matching. Fixing it properly means reading the whole
`YEAR_MATCH` path and probing it against real engine inputs, which macbook cannot do. Nothing is
baking, and this re-level corrects the live labels regardless, so it is tracked on #44 rather than
rushed. **Do not run the engine.**

### A3.1 — start from the current local tables

Your local `Data/exposure/intersect/` is already numerically correct (A3 gate passed, x1.0000,
`i.cols=none`); only the `pop_method` strings are wrong, and GFM's is double-suffixed. The fix
overwrites the column outright, so **no restore is needed** — re-applying is sufficient.

```bash
cd /home/jovyan/atlas/hazards_prototype && git pull --ff-only && git log -1 --oneline
POP_SOURCE=knbs-projection POP_YEAR=2026 POP_YEAR_MATCH=1 POP_REF_YEAR=2026 Rscript R/observational/7b_relevel_exposure_pop.R 2>&1 | tee logs/relevel_a3_dryrun_$(date +%Y%m%d_%H%M%S).log
```

**Expect:** no `dropped stale join artefact` lines (already stripped), **x1.0000 on all three**,
`national pop_total now 52837534 [knbs-projection-2026 / county-growth-from-2020]`.
**STOP if** any factor is not 1.0000, or if the run aborts on the new #44 assert — the latter would
mean the label is not being overwritten and the fix did not take.

### A3.2 — apply, then the label gate that failed last time

Same command with `APPLY=1`, then re-run **Block A2.2's label check verbatim**.

**Expect, and this is the gate:**

| table | `pop_method` | distinct `pop_source` |
|---|---|---|
| `exposure_gfm_seasonal` | `county-growth-from-2020-yearmatched` | 7 (`knbs-census-2019` covers both 2018 and 2019) |
| `exposure_jrc_rp` | `county-growth-from-2020` | 1 (`knbs-projection-2026`) |
| `exposure_totals` | `county-growth-from-2020` | 1 (`knbs-projection-2026`) |

No table may show a bare `county-level`, and **none may show a doubled `-yearmatched-yearmatched`**.
Your n=7 correction is accepted — the expectation in A2.2 was my counting slip, not a deviation.

Then re-run Block A's **A3 numeric gate** (`i.cols=none`, `identity_dev < 1e-6`, `GATE PASS`,
national 52,837,534) and the idempotence dry run (x1.0000, and now also a **stable `pop_method`** —
a second pass must not append anything).

### A3.3 — publish and verify

**Block A's A4 and A5 unchanged**, background the publish. A5 expectation adds: on S3, every
`pop_method` reads `county-growth-from-2020…`, no `i.` columns, national `pop_total` = **52,837,534**
(your A0 figure, unchanged throughout).

### A3.4 — after a green A5

Two follow-ups, neither a gate:
- `metadata/cdh/kenya-flood-exposure-intersect.yaml` probably states the old method string. Check and
  report; I will patch it from macbook.
- Say so in the response and I will tell the KE-ENSO team their `pop_method` workaround can come out.

---

### RESPONSE — Block A2 STOPPED at A2.2 label gate (cglabs, 2026-10-06 19:17 EAT)

**Local tables rewritten; nothing published. S3 tier 16 untouched.** A2.1 passed, the apply ran,
the numeric A3 gate passed, idempotence passed — and the `pop_method` label check failed. Stopped
before A2.3. The reason is a bug in 7b (and in the engine) that is also the root cause of #44.

**A2.1 — PASS** (`logs/relevel42_a2_dryrun_20261006_191624.log`): identical to the DIAG run. Two
`dropped stale join artefact` lines (jrc + totals), x1.0000 on all three,
`national pop_total now 52837534 [knbs-projection-2026 / county-growth-from-2020]`.

**A2.2 apply — ran** (`logs/relevel42_a2_apply_20261006_191644.log`): same lines, then
`WROTE -> Data/exposure/intersect (exposure_gfm_seasonal.parquet, exposure_jrc_rp.parquet, exposure_totals.parquet)`.

**A3 numeric gate — PASS:**

```
exposure_gfm_seasonal.parquet  i.cols=none  identity_dev=5.82e-11 ncol=26 mtime=2026-10-06 19:16:54
exposure_jrc_rp.parquet        i.cols=none  identity_dev=5.82e-11 ncol=23 mtime=2026-10-06 19:16:54
exposure_totals.parquet        i.cols=none  identity_dev=5.82e-11 ncol=18 mtime=2026-10-06 19:16:54
  rows=290 adm1=47 national pop_total=52837534
GATE PASS
```

**Idempotence — PASS** (`logs/relevel42_a2_dryrun2_20261006_191719.log`): no artefact-drop lines,
x1.0000 on all three, 52,837,534.

**Label check — FAIL.** Expected `county-growth-from-2020…` on all three. Got:

```
exposure_gfm_seasonal.parquet  pop_method=county-level-yearmatched-yearmatched  pop_source(n=7)=knbs-census-2019,knbs-projection-2020,…,knbs-projection-2025
exposure_jrc_rp.parquet        pop_method=county-level                          pop_source(n=1)=knbs-projection-2026
exposure_totals.parquet        pop_method=county-level                          pop_source(n=1)=knbs-projection-2026
```

jrc + totals unchanged; GFM made *worse* (suffix appended a second time). Every table has exactly
one distinct `pop_method` value. (`pop_source` n=7 on GFM, not 8: 2018 and 2019 both carry
`knbs-census-2019`, as the live table already did — a counting slip in the expectation, not a
deviation.)

**Root cause — `pop_method` is shadowed inside `:=`.** In `relevel()`
(`R/observational/7b_relevel_exposure_pop.R`):

```r
# by_year branch, L184-186
dt[, `:=`(pop_total = pop_total_grid * pop_scale_adm1,
          pop_method = paste0(pop_method, "-yearmatched"), ...)]
# other branch, L189-190
dt[, `:=`(pop_total = pop_total_grid * pop_scale_adm1,
          pop_source = pop_label, pop_method = pop_method, ...)]
```

The tables already carry a `pop_method` column (written by the engine), and it is **not** in the
`drop` vector at the top of `relevel()` (`pop_scale_adm1, pop_scale_census, pop_growth_county,
pop_year, pop_source` — L171-172). So inside `dt[, :=(...)]` the RHS `pop_method` resolves to the
**column**, not the global set at L155/L161. jrc/totals: a self-assignment, string preserved
whatever the method. GFM: the existing string gets `-yearmatched` appended on every pass. The
log line at L215 prints the global, which is why every run has *reported* the right method while
*writing* the old one.

Same construct in the engine, `R/observational/7_zonal_exposure.R` L263 and L266
(`A[, pop_method := paste0(pop_method, "-yearmatched")]`, `pop_method = pop_method`) — once a
`pop_method` column exists on an input, it will shadow there too.

**This fully explains #44.** The Sep-8 engine run wrote `county-level` (census default, L149).
Every 7b re-level since — census-only on 09-10, year-matched `county-growth` on 09-17 — printed
`county-growth-from-2020` to the log but left the column as written, adding `-yearmatched` to
GFM once on 09-17. The live mislabel is not a wrong invocation; no invocation could have changed
it. Fix is one line (add `"pop_method"` to the `drop` vector, or use `..pop_method` / a
differently-named local), then re-apply: the on-read artefact strip is a no-op now, numerics are
already x1.0000, and the strings will finally be overwritten.

**A2.4 — lookup done, nothing on the node.** `logs/` holds no file dated 2026-09-16..18 and no
7b header line from that run (it was tee'd nowhere; stdout went into RESPONSE #4). The archived
thread records the invocation shape: `POP_YEAR_MATCH=1 APPLY=1 Rscript
R/observational/7b_relevel_exposure_pop.R` with `POP_REF_YEAR` left at its 2026 default
(`archive/dispatches/DISPATCH_cglabs_knbs_population.md` L54-58, L390). Given the shadowing above,
the invocation is moot for #44.

**State on the node now.** Local `Data/exposure/intersect/` = S3 minus `i.pop_source`, numerics
identical, jrc/totals labels as live, GFM label double-suffixed. Not publishable as-is. S3 unchanged.
No backup copy was taken of the pre-apply local files (the dispatch did not ask for one); S3
versioning holds the published state and the apply is a pure function of the stored numerics, so
nothing is lost.

**Awaiting:** the one-line 7b fix (and the engine twin if you want it in the same commit) on
`develop`, then a GO to re-run A2.2 → A2.3 from here. Seconds plus the background publish.

---

## Block A2 — corrected Block A: default method, and the `pop_method` string is expected to change (#42, #44)

**GO — run this.** It supersedes Block A below. Your stop was correct and your diagnosis is adopted
in full: the live tables are `county-growth`, and `POP_METHOD=county-level` in Block A was my error,
read off the mislabelled `pop_method` column instead of tested against the values. The mislabel is
now [#44](https://github.com/AdaptationAtlas/hazards_prototype/issues/44); #43 has been corrected and
rescoped (no live licence exposure — `county-growth` is the licence-safe method and it is what is
published).

**Two changes from Block A, and nothing else:**

1. **Env: drop `POP_METHOD` entirely** (it defaults to `county-growth`). Everything else is
   unchanged.
2. **The `pop_method` *string* is now expected to change, and that is in scope.** The numeric
   columns stay x1.0000; the label is corrected from the #44 mislabel to the method actually
   applied:

   | table | `pop_method` before | `pop_method` after |
   |---|---|---|
   | `exposure_gfm_seasonal` | `county-level-yearmatched` | `county-growth-from-2020-yearmatched` |
   | `exposure_jrc_rp` | `county-level` | `county-growth-from-2020` |
   | `exposure_totals` | `county-level` | `county-growth-from-2020` |

   This is a provenance correction, not a method change. `pop_source`, `pop_year` and every numeric
   column are untouched.

### A2.1 — dry run

```bash
cd /home/jovyan/atlas/hazards_prototype && git pull --ff-only && git log -1 --oneline
POP_SOURCE=knbs-projection POP_YEAR=2026 POP_YEAR_MATCH=1 POP_REF_YEAR=2026 Rscript R/observational/7b_relevel_exposure_pop.R 2>&1 | tee logs/relevel42_a2_dryrun_$(date +%Y%m%d_%H%M%S).log
```

**Expect:** identical to your DIAG run — two `dropped stale join artefact` lines (jrc + totals, not
GFM), **x1.0000 on all three**, `national pop_total now 52837534 [knbs-projection-2026 /
county-growth-from-2020]`. **STOP if any factor is not 1.0000.**

### A2.2 — apply, then the local gates

Same command with `APPLY=1`, then run **Block A's A3 gate unchanged** (`i.cols=none` and
`identity_dev < 1e-6` on all three, `GATE PASS`), plus this label check:

```bash
Rscript -e '
  source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")
  suppressPackageStartupMessages(library(arrow)); library(data.table)
  d <- file.path(dirname(chirts_chirps_hist_dir), "exposure", "intersect")
  for (f in c("exposure_gfm_seasonal.parquet","exposure_jrc_rp.parquet","exposure_totals.parquet")) {
    t <- as.data.table(read_parquet(file.path(d, f)))
    cat(sprintf("%-30s pop_method=%-40s pop_source(n=%d)=%s
", f, t$pop_method[1],
                uniqueN(t$pop_source), paste(sort(unique(t$pop_source)), collapse=",")))
  }'
```

**Expect:** every `pop_method` begins `county-growth-from-2020`; **no table reports a bare
`county-level`**. GFM still shows 8 distinct `pop_source` values (2018-19 census, 2020-25
projection); jrc and totals show 1 (`knbs-projection-2026`).

Then re-run A2.1's dry run: no artefact-drop lines, x1.0000, and the `pop_method` strings now stable.

### A2.3 — publish and verify

**Block A's A4 and A5 unchanged** (background the publish; verify on S3). Add to the A5 expectation:
`pop_method` on S3 begins `county-growth-from-2020` for all three, and national `pop_total` is
**52,837,534** — your A0 figure, unchanged.

### A2.4 — one read-only lookup for #44, whenever convenient

I could not pin which run wrote the live mislabel. The engine path explains the GFM table exactly
(`7_zonal_exposure.R:149` takes `pop_method` from the `POP_SOURCE` census default and L263 never
recomputes it under year matching), but it does not explain `pop_source=knbs-projection-2026` on jrc
and totals, whose mtime matches your 7b re-level. The decisive evidence is on the node:

```bash
grep -rlE 'POP_(SOURCE|METHOD|YEAR_MATCH|REF_YEAR)' /home/jovyan/atlas/hazards_prototype/logs/   | xargs ls -la 2>/dev/null | grep '09-1[678]'
```

Whatever invocation ran on 2026-09-17 around 12:56 EAT, paste its header line into #44. **Not a
gate — do not hold A2 for it.**

---

### RESPONSE — Block A STOPPED at gate A1 (cglabs, 2026-10-06 11:57 EAT)

**Nothing written, nothing published.** A0 passed; A1 failed its x1.0000 invariant; stopped there per
the dispatch. Diagnosis below is read-only (a second dry run with the default method).

**A0 — PASS.** All invariants as written:

```
exposure_gfm_seasonal.parquet  i.cols=                 mtime=2026-09-17 12:56:53
exposure_jrc_rp.parquet        i.cols=i.pop_source     mtime=2026-09-17 12:56:53
exposure_totals.parquet        i.cols=i.pop_source     mtime=2026-09-17 12:56:53
rows=290 adm1=47 national pop_total=52837534 pop_total_grid=55119798
pop_source=knbs-projection-2026 pop_method=county-level pop_year=2026
```

No other Rscript running; no `POP_*`/`APPLY` env set. `i.pop_source` holds `knbs-census-2019` on
every row of both affected tables (the right-hand copy from the pre-fix join).

**A1 — FAIL.** With the dispatch's env (`POP_METHOD=county-level`) every table moved
(`logs/relevel42_dryrun_20261006_115637.log`):

```
re-levelling Data/exposure/intersect | POP_SOURCE=knbs-projection POP_YEAR=2026 POP_METHOD=county-level | DRY RUN
  exposure_jrc_rp.parquet      dropped stale join artefact: i.pop_source (#42)
  exposure_totals.parquet      dropped stale join artefact: i.pop_source (#42)
  exposure_gfm_seasonal.parquet      2610673 ->      2675885  (x1.0250)
  exposure_jrc_rp.parquet           5668271 ->      5816936  (x1.0262)
  exposure_totals.parquet          52837534 ->     54226998  (x1.0263)
  national pop_total now 54226998 [knbs-projection-2026 / county-growth-from-2020]
DRY RUN — nothing written.
```

The two `dropped stale join artefact` lines are exactly as expected (jrc + totals, not GFM) — the #42
fix itself behaves. The x-factors do not.

**Diagnostic dry run, default method (`POP_METHOD` unset = `county-growth`), same env otherwise**
(`logs/relevel42_dryrun_DIAG_default_method_20261006_115747.log`):

```
re-levelling Data/exposure/intersect | POP_SOURCE=knbs-projection POP_YEAR=2026 POP_METHOD=county-growth | DRY RUN
  exposure_jrc_rp.parquet      dropped stale join artefact: i.pop_source (#42)
  exposure_totals.parquet      dropped stale join artefact: i.pop_source (#42)
  exposure_gfm_seasonal.parquet      2610673 ->      2610673  (x1.0000)
  exposure_jrc_rp.parquet           5668271 ->      5668271  (x1.0000)
  exposure_totals.parquet          52837534 ->     52837534  (x1.0000)
  national pop_total now 52837534 [knbs-projection-2026 / county-growth-from-2020]
DRY RUN — nothing written.
```

**What this means.** The live tier-16 tables were built with **`county-growth`**, not `county-level`.
That matches the record: Pete picked option C **with `county-growth`** on 2026-09-17
(`archive/dispatches/DISPATCH_cglabs_knbs_population.md` lines 40 and 347, RESPONSE #4, commit
4cf82b4), and `county-growth` reproduces every live value to x1.0000. The live `pop_method` column
saying `county-level` (`county-level-yearmatched` in GFM) is a **mislabel**, not the method used.
54,226,998 is the published-KNBS-level figure for 2026; 52,837,534 is census-anchored growth.

Consequences for this dispatch, for macbook to rule on — not improvised here:

1. **Block A's env is wrong.** `POP_METHOD=county-level` is Block B, not the published state. The
   value-neutral env is the default method (`POP_METHOD` unset or `county-growth`).
2. **Block A cannot be fully value-neutral even so.** The fixed 7b rewrites `pop_method` from the
   stored strings; under `county-growth` it writes `county-growth-from-2020` (GFM:
   `county-growth-from-2020-yearmatched`), i.e. it would *correct* the mislabel as a side effect.
   Numeric columns stay x1.0000. Decide whether that string change is in scope for A or wants its
   own line in the dispatch / CDH record.
3. **#43 may rest on the mislabel.** `HANDOVER_2026-10-06_ke-enso-exposure-denominator-answers.md`
   §(b) treats `pop_method = county-level` on projection rows as the live choice awaiting Pete. The
   live *numbers* are already the `county-growth` Pete chose. The 1.4 M question is still real as a
   policy question, but the premise "published with county-level" is not.
4. **How the mislabel got written** I did not chase. The code at 4cf82b4 (lines 141-144) should
   have produced `county-growth-from-2020` for a `knbs-projection-2026` reference, yet the parquet
   says `county-level`. Worth a look on macbook before the next apply.

**Awaiting:** a corrected Block A (env + expectation on the `pop_method` string), or a GO to run it
with the default method as-is. Either is a seconds-long re-run of A1→A5 from here. Tier 16 on S3
is untouched; the KE-ENSO workaround stays in place.

---

## Block B — `pop_method` re-level — **PARKED, needs Pete GO** (#43)

Do **not** run this block yet. The live tables are published with `pop_method = county-level` on
projection-sourced rows; the pipeline default is `county-growth`. The choice moves the national
denominator by ~1.4 M for 2025 (51.96 M vs 53.33 M) and carries a KNBS licence dimension
([#43](https://github.com/AdaptationAtlas/hazards_prototype/issues/43), bundled with #33). It is
Pete's call, not a data fix. When he rules, this block becomes one more 7b run with a different
`POP_METHOD` and a tier-16 republish — the same mechanics as Block A, so nothing new to build.

**Block A must not change any value.** If it does, stop: that would mean Block B has been run by
accident.

---

## Block A — strip the leaked `i.pop_source` column and republish tier 16 (#42)

**Why.** `exposure_jrc_rp.parquet` and `exposure_totals.parquet` on S3 each carry a stray
`i.pop_source` alongside the real `pop_source`; `exposure_gfm_seasonal.parquet` does not. It is a
`data.table` join artefact in `relevel()` — the non-`by_year` branch did not drop the table's own
`pop_source` before `scale_dt[dt, on = "adm1_pcode"]`, so the right-hand copy was retained under the
`i.` prefix. The `by_year` branch did drop it, which is exactly why the GFM table escaped.

**Fixed on `develop`** in `R/observational/7b_relevel_exposure_pop.R`:
1. `pop_source` added to the `drop` vector at the top of `relevel()`, so neither branch can leak it
   (it is re-set unconditionally further down in both).
2. Any pre-existing `^i\.` column is stripped on **read**, so re-levelling an already-affected table
   repairs it rather than aborting.
3. A write-time assertion refuses to write any table carrying a `^i\.` column.

Validated on macbook: the join mechanism was reproduced in isolation (artefact present before the
fix, absent after, `pop_scale_adm1` / `pop_total_grid` / `pop_source` values unchanged), and the
script parses.

**This block is value-neutral by design.** The env below reproduces the denominator the tables were
published with. Every table must come back x1.0000. **Do not change `POP_METHOD` here** — that is
Block B.

### A0 — preflight (read-only, seconds)

```bash
cd /home/jovyan/atlas/hazards_prototype && git pull --ff-only && git log -1 --oneline
pgrep -af Rscript || echo "no Rscript running"
env | grep -E '^(POP_|APPLY|IN_DIR|EXP_ROOT)' || echo "no POP_ env set (good)"
Rscript -e '
  source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")
  suppressPackageStartupMessages(library(arrow)); library(data.table)
  d <- file.path(dirname(chirts_chirps_hist_dir), "exposure", "intersect")
  for (f in c("exposure_gfm_seasonal.parquet","exposure_jrc_rp.parquet","exposure_totals.parquet")) {
    n <- names(read_parquet(file.path(d, f), as_data_frame = FALSE))
    cat(sprintf("%-30s i.cols=%-16s mtime=%s\n", f,
        paste(grep("^i\\.", n, value = TRUE), collapse = ",") , format(file.mtime(file.path(d,f)))))
  }
  t <- as.data.table(read_parquet(file.path(d, "exposure_totals.parquet")))
  cat(sprintf("rows=%d adm1=%d national pop_total=%.0f pop_total_grid=%.0f\n",
      nrow(t), uniqueN(t$adm1_pcode), sum(t$pop_total), sum(t$pop_total_grid)))
  cat(sprintf("pop_source=%s pop_method=%s pop_year=%s\n", t$pop_source[1], t$pop_method[1], t$pop_year[1]))'
```

**Expect (invariants, not figures):**
- `exposure_jrc_rp` and `exposure_totals` each report **exactly one** `i.` column, `i.pop_source`.
- `exposure_gfm_seasonal` reports **none**.
- 290 rows, 47 counties; `pop_source` is a `knbs-projection-<year>` string and `pop_method` is
  `county-level`.
- **Record `national pop_total` — every later gate compares against it.**

**STOP if** the GFM table also carries an `i.` column, or any table carries an `i.` column other
than `i.pop_source`, or `pop_method` is not `county-level`. Any of those means the live state is not
what this dispatch was written against. Describe what you see; do not improvise.

### A1 — dry run (seconds, foreground)

```bash
cd /home/jovyan/atlas/hazards_prototype
POP_SOURCE=knbs-projection POP_YEAR=2026 POP_METHOD=county-level \
POP_YEAR_MATCH=1 POP_REF_YEAR=2026 \
Rscript R/observational/7b_relevel_exposure_pop.R 2>&1 | tee logs/relevel42_dryrun_$(date +%Y%m%d_%H%M%S).log
```

**Expect:**
- Two `dropped stale join artefact: i.pop_source (#42)` lines — for `exposure_jrc_rp` and
  `exposure_totals`, and **not** for the GFM table.
- All three tables report **x1.0000**. A factor that is not 1.0000 means the denominator moved:
  **STOP**, you are accidentally in Block B territory.
- `national pop_total now <N> [knbs-projection-2026 / county-level]` where `<N>` equals the A0
  figure.
- Ends with `DRY RUN — nothing written.`

### A2 — apply (seconds, foreground)

Same command with `APPLY=1`:

```bash
cd /home/jovyan/atlas/hazards_prototype
POP_SOURCE=knbs-projection POP_YEAR=2026 POP_METHOD=county-level \
POP_YEAR_MATCH=1 POP_REF_YEAR=2026 APPLY=1 \
Rscript R/observational/7b_relevel_exposure_pop.R 2>&1 | tee logs/relevel42_apply_$(date +%Y%m%d_%H%M%S).log
```

**Expect:** the same x1.0000 lines, then `WROTE -> …`.

### A3 — local gates before publishing (read-only)

```bash
cd /home/jovyan/atlas/hazards_prototype
Rscript -e '
  source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")
  suppressPackageStartupMessages(library(arrow)); library(data.table)
  d <- file.path(dirname(chirts_chirps_hist_dir), "exposure", "intersect")
  ok <- TRUE
  for (f in c("exposure_gfm_seasonal.parquet","exposure_jrc_rp.parquet","exposure_totals.parquet")) {
    t <- as.data.table(read_parquet(file.path(d, f)))
    leaked <- grep("^i\\.", names(t), value = TRUE)
    dev <- max(abs(t$pop_total - t$pop_total_grid * t$pop_scale_census * t$pop_growth_county), na.rm = TRUE)
    cat(sprintf("%-30s i.cols=%-10s identity_dev=%.3g\n", f,
                if (length(leaked)) paste(leaked, collapse=",") else "none", dev))
    if (length(leaked) || !is.finite(dev) || dev > 1e-6) ok <- FALSE
  }
  cat(if (ok) "GATE PASS\n" else "GATE FAIL\n")'
```

**Expect:** `i.cols=none` on all three, `identity_dev` below 1e-6 on all three, `GATE PASS`.

Then prove idempotence — **re-run A1's dry run**. It must now report **no** `dropped stale join
artefact` lines (there is nothing left to drop) and **x1.0000** on all three. A second pass that
changes anything is a defect: **STOP**.

### A4 — publish tier 16 (background; this is an upload)

The interactive shell kills foreground commands after about two minutes, and a publish was SIGTERM'd
mid-run on 2026-09-28. Background it.

```bash
cd /home/jovyan/atlas/hazards_prototype
export STAMP=$(date +%Y%m%d_%H%M%S)
nohup Rscript R/observational/6_publish_obs_to_s3.R --full --tier 16 --overwrite \
  > logs/publish_t16_$STAMP.log 2>&1 &
echo $! > logs/publish_t16_$STAMP.pid
sleep 30; tail -20 logs/publish_t16_$STAMP.log
```

**Expect:** 3/3 objects uploaded, no error lines. Do not run `R/s3_upload.R` or pass
`--reference` / `--allow-schema-drift`.

### A5 — verify on S3 (read-only), then report

```bash
cd /home/jovyan/atlas/hazards_prototype
Rscript -e '
  suppressPackageStartupMessages(library(arrow)); library(data.table)
  b <- "https://digital-atlas.s3.amazonaws.com/domain=exposure/type=intersect/region=kenya/processing=analysis-ready/"
  for (f in c("exposure_gfm_seasonal.parquet","exposure_jrc_rp.parquet","exposure_totals.parquet")) {
    tf <- tempfile(fileext=".parquet"); download.file(paste0(b,f), tf, quiet=TRUE, mode="wb")
    t <- as.data.table(read_parquet(tf))
    cat(sprintf("%-30s ncol=%3d i.cols=%s\n", f, ncol(t),
        if (length(grep("^i\\.", names(t)))) paste(grep("^i\\.", names(t), value=TRUE), collapse=",") else "none"))
    if (f == "exposure_totals.parquet")
      cat(sprintf("  national pop_total=%.0f pop_source=%s pop_method=%s\n",
          sum(t$pop_total), t$pop_source[1], t$pop_method[1]))
  }'
```

**Expect:** `i.cols=none` on all three; national `pop_total` identical to A0; `pop_method` still
`county-level` (unchanged — Block B is parked).

**Report back** with a `### RESPONSE` block carrying the A0 and A5 national totals side by side, the
A1 and A3 x-factors, and the publish log tail. Then verify the response landed on `origin/develop`
(`git fetch`; `git log origin/develop..HEAD` empty; `git show origin/develop:DISPATCH_cglabs_pop_denominator_fixes.md | grep -c RESPONSE`).

### Notes

- `7b` sources `R/0_server_setup.R`, which calls `setwd(working_dir)`. The standalone
  `Rscript R/observational/7b_…R` form above is fine; the `Rscript -e` gates source by **absolute
  path** for that reason.
- `arrow` and `duckdb` must not both be attached in one R session. Every gate above uses `arrow` +
  `data.table` only.
- The KE-ENSO notebook team has been told to select columns explicitly as a temporary workaround
  (`HANDOVER_2026-10-06_ke-enso-exposure-denominator-answers.md`). Tell them when A5 is green so the
  workaround can come out.
