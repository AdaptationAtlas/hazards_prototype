# DISPATCH — cglabs — #47: livestock nominal-USD priced at the world median

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

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
