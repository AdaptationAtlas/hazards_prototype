# DISPATCH — cglabs — #47: livestock nominal-USD priced at the world median

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

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
