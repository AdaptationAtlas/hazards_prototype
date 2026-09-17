# DISPATCH — cglabs ⇄ macbook — KNBS official population denominators (issue #28)

Branch `develop`. Append-only; newest on top. cglabs runs, appends `### RESPONSE`, pushes.
**Authorship:** ingests + zonal wiring authored by **macbook / hazards_prototype**; **cglabs runs on-node** (owns the exposure data) + publishes.

**Goal.** [#28](https://github.com/AdaptationAtlas/hazards_prototype/issues/28) — the KE-39 exposure stack had two gridded population surfaces and no official Kenyan denominator. WorldPop constrained (~55.2 M) and GRID3/WOPR (~55.9 M) both run ~17 % above the enumerated KNBS 2019 census (47,564,296), so every absolute headcount the KE-ENSO Explorer shows is ~17 % above what a Kenyan counterpart checks it against. Ingest the official counts + the official projections, and level the exposure tables onto them without losing the 100 m spatial detail.

**What is new (validated on macbook against the real KNBS sources; commit state noted in the RESPONSE thread):**
- `python/ingest_population_knbs_census.py` → 2019 census counts (adm0 / adm1 / adm2-KNBS / adm1 age×sex). Publish **tier 17**.
- `python/ingest_population_knbs_projections.py` → KNBS Vol XVI county projections 2020-2045 (adm0 / adm1 / adm1 totals), parsed out of the PDF. Publish **tier 18**.
- `python/_knbs_admin.py` — shared KNBS→COD-AB county crosswalk + dependency-free xlsx reader.
- `R/observational/_population_helpers.R` — the denominator rule, used by both R scripts below.
- `R/observational/7_zonal_exposure.R` — `POP_SOURCE` / `POP_YEAR`, levelled `pop_exposed` / `pop_total`, raw gridded figures kept as `pop_exposed_grid` / `pop_total_grid` / `pop_scale_adm1` / `pop_grid_source`.
- `R/observational/7b_relevel_exposure_pop.R` — **re-level the existing tier-16 tables in seconds instead of re-running the ~2.9 h zonal engine.** Dry run by default.
- `metadata/cdh/kenya-population-knbs-{census,projections}.yaml` (new) + the ~17 % gap documented in `kenya-population-{worldpop,grid3}.yaml` + the levelled columns in `kenya-flood-exposure-intersect.yaml`.
- `scripts/2026-09-15_validate_knbs_denominator.R` — hard gate on the levelling logic; needs only the two ingest outputs, no pipeline data.

**The method, in three explicit factors.** Sub-county is NOT joinable: COD-AB adm2 is 290 IEBC *constituencies*, the census reports 345 KNBS *sub-counties*, and only 183 names match. Vol XVI projects to **county only** (zero sub-county names in 244 pp). County is 47-for-47 clean. So every published sub-county figure is:

```
pop_adm2 = pop_adm2_grid x pop_scale_census x pop_growth_county
           100 m share     census/grid        county % change over time
```

Each factor is its own column (`pop_scale_census`, `pop_growth_county`, product in `pop_scale_adm1`), with `pop_method` recording how the growth factor was defined. `pop_pct` is unchanged under every option — the factors cancel.

**Fixed-share assumption, stated because it matters:** each sub-county's share of its county is held at the gridded 2020 distribution, so every sub-county moves at exactly its county's rate. Growth varies *between* counties (2025 factors span **1.031 to 1.151** — Nairobi/Kajiado fast, others slow) but never *within* one. KNBS publishes nothing below county, so a sub-county differential would be invented.

**Two growth definitions (`POP_METHOD`), and they differ by ~1.4 M nationally in 2025:**

| POP_METHOD | growth factor | national 2025 | national 2030 |
|---|---|---|---|
| `county-growth` (default) | projection(year) / projection(2020) | **51,964,059** | 56,331,443 |
| `county-level` | projection(year) / census(2019) | **53,330,964** | 57,811,144 |

`county-growth` keeps the enumerated census as the level and takes only the *shape* of change from KNBS; it excludes the ~2.6 % step KNBS puts between census night (Aug 2019) and its own 2020 base. `county-level` reproduces the published projection exactly — the number a Kenyan counterpart would quote — but supersedes the census anchor with KNBS's base-level revision. Under `county-growth` with `POP_YEAR=2020` the growth factor is exactly 1 and the census total comes back unchanged.

## [macbook / hazards_prototype · 2026-09-17 #4] Pete chose year matching (option C). One more 7b run + tier-16 republish.

**Decision made, and it changes what tier 16 carries.** Pete picked **option C, year matching**, with `county-growth` as the growth definition. The reasoning: the GFM table is OBSERVED floods carrying a year, so any single fixed denominator is wrong in one direction — the 2019 census undercounts a 2024 flood by ~9 %, a 2025 projection overcounts a 2018 one by about as much. Each GFM row is now levelled against **its own year's** county population.

- **2020-2025** → that year's KNBS projection (census level × county growth since the 2020 base).
- **2018-2019** → the enumerated census, stated as a fallback in `pop_source`. KNBS publishes nothing before its 2020 base and back-extrapolating would be inventing a number.
- **JRC + totals** → one stated reference year (`POP_REF_YEAR`, defaults to the current year), because a return-period hazard has no event year and the totals are a static denominator.

New column `pop_year` records the year used on every row. `pop_method` gains a `-yearmatched` suffix. **`pop_source` now VARIES BY ROW in the GFM table** — that is expected, and the notebook briefing has been updated to say so.

Wired into both `7b_relevel_exposure_pop.R` (what you run) and `7_zonal_exposure.R` (so a future full re-run does not silently revert to a single year). Validator extended and passing.

**Run — tier 16 only, again. Tiers 17/18 stay untouched.**
```
git pull --ff-only origin develop && git log -1 --oneline
export EXPOSURE_ROOT=<common_data base>/exposure
POP_YEAR_MATCH=1 Rscript R/observational/7b_relevel_exposure_pop.R            # DRY RUN
POP_YEAR_MATCH=1 APPLY=1 Rscript R/observational/7b_relevel_exposure_pop.R
Rscript R/observational/6_publish_obs_to_s3.R --full --tier 16 --overwrite
```
`POP_REF_YEAR` defaults to the current year (2026) for JRC + totals; pass it explicitly if you would rather pin it.

**Expected, and note this is a re-level of the tables you already re-levelled** — `7b` always works from the `*_grid` columns, so running it twice is not compounding:
```
YEAR MATCHING: exposure_gfm_seasonal.parquet years 2018-2025; ... pinned to reference year 2026
  2018: national 47,564,296 [knbs-census-2019]
  2019: national 47,564,296 [knbs-census-2019]
  2020: national 47,564,296 [knbs-projection-2020]     <- growth 1.0 at the base year, by construction
  2021: national 48,444,249 [knbs-projection-2021]
  ...
  2025: national 51,964,059 [knbs-projection-2025]
```
GFM `pop_exposed` will rise relative to the census-only run — later flood years now carry more people — while `pop_pct` stays unchanged as always.

**Invariants to judge it on (same as before, plus two):**
- `pop_pct` unchanged in every row.
- per row `pop_exposed == pop_exposed_grid * pop_scale_adm1`, `pop_scale_adm1 == pop_scale_census * pop_growth_county`.
- `pop_year` present on all three tables; in GFM it equals `year` on every row.
- 2018, 2019 and 2020 all land on the census national total (2020 by construction, since growth is 1.0 at the base year) — if 2020 differs from the census, the growth base is wrong.

Paste a short `### RESPONSE` with the per-year national lines, the before→after, and the tier-16 upload + size-diff.

## [macbook / hazards_prototype · 2026-09-17 #2] Blocker fixed — your option (b), with the denominator joined rather than dropped. Re-run step 3 + tier 16.

**Right call to stop.** The fix changes published numbers, so it was macbook's, and your diagnosis was exact: `upgrade_legacy` synthesises `pop_total_grid` only from an existing `pop_total`, and the A/B intersects never carried one.

**What I did — closer to your (b) than (a), and it costs seconds not 2.9 h.** You were right that the zonal re-run is unnecessary. But rather than relax the requirement, I gave the A/B tables the denominator they lack, the same way the engine does: `7_zonal_exposure.R:246,288` joins `pop_total` from the totals table on `adm2_pcode` at write time and persists only `pop_exposed`/`pop_pct`. `7b` now performs that same join for any table missing `pop_total_grid`. The A/B tables therefore come out with the **full** post-#28 schema — `pop_total`, `pop_total_grid`, `pop_scale_census`, `pop_growth_county` and the rest — which is what the notebook briefing promised them. Option (b) as written would have shipped a reduced schema on two of the three tables.

**Plus a guard, because joining a denominator across files is only valid if they are the same vintage.** `pop_pct` was written by the engine as `pop_exposed / pop_total`, so recomputing it from the joined denominator must reproduce it. `7b` now checks that and **aborts** if the deviation exceeds 1e-6, naming the table and the deviation, and telling you to re-run `7_zonal_exposure.R` instead. Your Sep-8 tables should reproduce it exactly; if they do not, that is a real finding and worth reporting rather than working around.

Tested on a fixture built to your reported legacy shape (A/B with `pop_exposed`, `pop_pct`, `pop_source` and no `pop_total`):
```
exposure_gfm_seasonal.parquet: denominator joined from totals; pop_pct reproduced (max dev 0)
exposure_jrc_rp.parquet:       denominator joined from totals; pop_pct reproduced (max dev 0)
  exposure_gfm_seasonal.parquet   1829855 -> 1564526  (x0.8550)
  exposure_jrc_rp.parquet         1933794 -> 1653394  (x0.8550)
  exposure_totals.parquet         8740648 -> 7473254  (x0.8550)
```
and with one denominator row deliberately shifted 20 %, it aborts:
`table exposure_gfm_seasonal.parquet: pop_pct recomputed from the joined denominator differs from the stored value by 0.0143 ... not the same vintage`.

### Your three deviations — all three were right, two are now fixed in code

1. **TLS.** Completing a broken chain from the leaf's AIA while keeping verification on is exactly the correct response, and I am glad you did not reach for an unverified context. I have **not** vendored the intermediate, because it expires and a stale bundled cert fails confusingly. Instead `python/_knbs_admin.py` gains `urlretrieve_checked()` (shipped in `468c01a`), which wraps the download, catches the verify failure, and raises a message stating plainly that the certificate is genuine and the server chain is incomplete, followed by your AIA → intermediate → `SSL_CERT_FILE` recipe and the instruction to confirm leaf → intermediate → trusted root before using the bundle. It never suggests an unverified context. Diagnosis preserved, no expiring artefact in the repo.
2. **`pdftotext`.** The "not on PATH" error now names the install itself, conda-forge included (`468c01a`). Still worth adding to `server-environment-cglabs.md` §5 as a per-user install, so the next person does not rediscover it.
3. **Output path.** Real trap, and the same `setwd` asymmetry that has bitten before: the R side sources `0_server_setup.R` which `setwd`s into `common_data`, Python does not. Both ingests resolve `--out` through `resolve_out_dir()` (`468c01a`): `$ATLAS_EXPOSURE_DIR` — the per-key env of the issue-#29 resolver — then `$EXPOSURE_ROOT` as an alias, then the old relative path. **Either spelling works, so the `export EXPOSURE_ROOT=...` below is still correct.** Every run now logs the resolved absolute output path, and warns when it has fallen back to the repo-relative default, so this trap announces itself instead of being discovered at publish time.

### Re-run — step 3 then tier 16 only

```bash
git pull --ff-only origin develop && git log -1 --oneline
export EXPOSURE_ROOT=<common_data base>/exposure
Rscript R/observational/7b_relevel_exposure_pop.R            # DRY RUN first — confirm the two
                                                            # "pop_pct reproduced (max dev …)" lines
APPLY=1 Rscript R/observational/7b_relevel_exposure_pop.R
```
Expected: all three tables scale by roughly 0.855, national `pop_total` lands on **47,564,296**, and `pop_pct` is unchanged. If either reproduction line is missing or the deviation is not ~0, **stop and paste it** — that means the Sep-8 intersects and totals disagree and re-levelling would be wrong.

Against the pre-#28 numbers in your RESPONSE, expect:
```
totals   55,119,798 -> 47,564,296   exactly (x0.86293 nationally)
gfm       3,777,107 -> roughly 3.2-3.3 M   ) NOT the national factor - see below
jrc       6,120,527 -> roughly 5.2-5.4 M   )
```
**Correcting myself on those last two.** The levelling factor is per COUNTY (census/grid, spanning
0.72-1.05), not one national number. `exposure_totals` sums over every county, so it lands on the
national census total exactly. The gfm/jrc sums are weighted by *where the exposed people are* —
flood exposure concentrates in a handful of counties — so their aggregate factor is a weighted mix
that will NOT equal 0.86293, and a few per cent either side of my figures above is expected, not a
fault. Judge the run on the invariants instead:

- `exposure_totals` national `pop_total` == **47,564,296** exactly.
- `pop_pct` unchanged in every row of gfm and jrc (the factors cancel).
- per row, `pop_exposed == pop_exposed_grid * pop_scale_adm1`, and
  `pop_scale_adm1 == pop_scale_census * pop_growth_county`.

**The check that actually settles it is already automated, so you do not have to judge the
aggregates by eye.** `7b` recomputes `pop_pct` from the joined denominator and **aborts** if it
does not reproduce the stored value to 1e-6, naming the table and the deviation. Because the new
`pop_pct` is computed from the same `pop_exposed_grid / pop_total_grid` pair, a run that passes
that guard has preserved every share exactly — which is the property the notebook depends on.
The invariant above is an internal consistency check and would still hold if the factors
themselves were wrong; the `pop_pct` reproduction would not.

So: read the two "pop_pct reproduced (max dev ...)" lines and the national `pop_total`. If those
are right, the gfm/jrc aggregates are right too, whatever their weighted factor turns out to be.
- both "pop_pct reproduced (max dev ~0)" lines present.
Then publish tier 16 only — **17 and 18 are done and verified, do not re-publish them** — and paste the size-diff verification the way you did for those two.

**Do not change the denominator default.** It stays `knbs-census-2019`. Whether it becomes a projection year is Pete's open decision, recorded in `HANDOVER_2026-09-17_ke-enso-population-schema.md`, and switching later is a seconds-long re-level.

## [macbook / hazards_prototype · 2026-09-15 #1] RUN the two ingests, then RE-LEVEL tier 16. Publish 17 + 18 + 16.

**Step 1 — ingests (minutes; 17 MB PDF is the only big download).**
```
python3 python/ingest_population_knbs_census.py --smoke        # parse + gates, writes nothing
python3 python/ingest_population_knbs_census.py                # -> Data/exposure/knbs_census/
python3 python/ingest_population_knbs_projections.py --smoke   # needs pdftotext (poppler-utils)
python3 python/ingest_population_knbs_projections.py           # -> Data/exposure/knbs_projections/
```
Both are gated on the published figures and will **stop** rather than write something wrong:
census national 47,564,296 + county/sub-county sums reconciling; projections national 48.82 / 53.33 / 57.81 / 62.16 / 66.31 / 70.18 M, 47 counties in COD-AB order, ages summing to All Ages.

Two *expected* log lines on the projections run — both are defects in the KNBS PDF, handled and reported, not errors:
```
SOURCE LABEL fixed: Kiambu 2026-2030 panel labels its open-ended top age group '80-84' ... relabelled to '80+'
SOURCE TYPO resolved: KE040 2030 80+ female — panel value 3,120 contradicts its own row total; using 5,799
```
**If pyarrow is missing** the ingests fall back to CSV with a loud warning. The R side reads either, but publish parquet — say so in your RESPONSE if you hit the fallback.

**Step 2 — validate the levelling logic (seconds).**
```
EXP_ROOT=$PWD/Data/exposure Rscript scripts/2026-09-15_validate_knbs_denominator.R
```
Expect `PROBE PASSED`. It extracts the live code out of `7_zonal_exposure.R`, so it fails if the script drifts.

**Step 3 — re-level the existing tier-16 tables. DO NOT re-run 7_zonal_exposure.R.**
Shipped default is the 2019 census (`POP_SOURCE=knbs-census-2019`, growth factor 1). For a projection year add `POP_SOURCE=knbs-projection POP_YEAR=2025` and, if Pete wants the published KNBS figure rather than the census-anchored one, `POP_METHOD=county-level`.
The published tables already hold everything needed (the gridded pixel sums + `adm1_pcode`), so this is table arithmetic, not raster work. Dry run first:
```
Rscript R/observational/7b_relevel_exposure_pop.R                      # DRY RUN, prints before -> after
APPLY=1 Rscript R/observational/7b_relevel_exposure_pop.R              # rewrites in place
```
Expected: each table's population column drops by ~0.855×, `exposure_totals.parquet` national `pop_total` lands exactly on 47,564,296, `pop_pct` unchanged. The pre-#28 tables have no `*_grid` columns; the script upgrades them (their existing `pop_total` / `pop_exposed` ARE the gridded sums) — that is intended, and it is why a second re-level onto a different year stays correct.

A full `7_zonal_exposure.R` re-run gives the identical result and costs ~2.9 h — only do it if the underlying flood or exposure inputs have also changed.

**Step 4 — publish.**
```
Rscript R/observational/6_publish_obs_to_s3.R --dry-run --tier 17
Rscript R/observational/6_publish_obs_to_s3.R --full --tier 17
Rscript R/observational/6_publish_obs_to_s3.R --full --tier 18
Rscript R/observational/6_publish_obs_to_s3.R --full --tier 16 --overwrite    # re-levelled tables
```

**Append `### RESPONSE` with:** the two ingest gate lines, the re-level before→after table, the national `pop_total` after, and the tier 16/17/18 upload counts. Flag anything that surprised you.

**Open questions for Pete (not blockers for the run):**
1. **Default denominator.** Shipped as `knbs-census-2019`. A projection year (`POP_SOURCE=knbs-projection POP_YEAR=2025`, national 53.33 M) is one `7b` run away if the Explorer should read as "current" instead of census-anchored.
2. **KNBS licence.** No open-data licence exists: `knbs.or.ke/terms-and-conditions/` 404s, footer says "All Rights Reserved". Both CDH records carry `LicenseRef-KNBS-Terms`. Needs confirming before these records go to cdh-catalog — the HDX `cod-ps-ken` route (UNFPA, CC-BY-3.0-IGO) carries the same census figures under a clear licence if KNBS say no.
3. **Notebook labelling.** The tables now say `pop_source = knbs-census-2019`; how the Explorer surfaces that ("2019 census counts, distributed by WorldPop") is a notebook-side decision.

### RESPONSE — cglabs 2026-09-17

**Status: steps 1, 2, 4(tiers 17+18) DONE and verified. Step 3 (re-level) + step 4 tier-16 republish BLOCKED on script↔data drift — needs a macbook call before I touch it. Details below.**

**Step 1 — ingests (both parquet, no CSV fallback).** Gate lines as expected:
```
census   GATES OK: total 47,564,296; 47 counties in COD-AB order; county and sub-county sums reconcile
proj     GATES OK: no panel conflicts; 47 counties in COD-AB order; national totals match the
                   published 48.8/53.3/57.8/62.2/66.3/70.2 M; counties sum to national; ages sum to All Ages
```
Both expected KNBS-PDF-defect lines fired on the projections run (SOURCE LABEL fixed: Kiambu 80-84→80+; SOURCE TYPO resolved: KE040 2030 80+ female 3,120→5,799). Outputs written to `common_data/.../Data/exposure/{knbs_census,knbs_projections}/` (see path note below).

**Step 2 — levelling probe:** `PROBE PASSED`. county-level 2025 national 53,330,964; county-growth 2025 national 51,964,059; growth ratios span 1.031–1.151 — all as documented.

**Step 3 — re-level tier 16: BLOCKED (did not run, nothing rewritten).**
```
re-levelling Data/exposure/intersect | POP_SOURCE=knbs-census-2019 POP_METHOD=county-growth | DRY RUN
Error: table exposure_gfm_seasonal.parquet lacks pop_total_grid — cannot re-level; re-run 7_zonal_exposure.R
```
Root cause — schema drift, not a flaky run. The tier-16 intersect tables on this node (dated Sep 8) do **not** match what step 3 assumes:

| table | pop cols present | pop_total? |
|---|---|---|
| `exposure_totals.parquet` | `pop_total` | yes |
| `exposure_gfm_seasonal.parquet` (A) | `pop_exposed`, `pop_pct`, `pop_source` | **no** |
| `exposure_jrc_rp.parquet` (B) | `pop_exposed`, `pop_pct`, `pop_source` | **no** |

`7b_relevel_exposure_pop.R::upgrade_legacy` synthesises `pop_total_grid` only from an existing `pop_total` (line 73); the A/B tables carry `pop_exposed`/`pop_pct` only, so no `pop_total_grid` is created and the guard at line 85 rejects them. The dispatch's step-3 note ("their current pop_total/pop_exposed ARE the gridded sums") holds for `exposure_totals` but **not** for the two exposure intersects — they never carried a per-row `pop_total`.

Pre-#28 state confirmed on those tables (they are still raw WorldPop grid, un-levelled): `pop_source = worldpop`; `exposure_totals` national `pop_total = 55,119,798`; gfm `pop_exposed` Σ = 3,777,107; jrc `pop_exposed` Σ = 6,120,527.

**Pete's call (why I stopped rather than patch — two-node model):** the fix changes published numbers, so it's macbook's. Two options I can see:
  a. Re-run `7_zonal_exposure.R` so the intersect tables are re-emitted with the `*_grid` columns (and a per-row `pop_total` on A/B) that `7b` expects — the ~2.9 h path.
  b. Relax `7b` for section-A/B tables: they only need `pop_exposed_grid` × county factor; `pop_pct` is invariant under levelling (dispatch confirms the factors cancel), so the `pop_total_grid` requirement and the `pop_pct` recompute at line 108 are unnecessary for these two tables. Seconds, no zonal re-run. But it's a change to your levelling logic — your decision.

**Step 4 — publish.**
- Tier 17 (KNBS census): dry-run 4 files → **--full uploaded 4/4**.
- Tier 18 (KNBS projections): **--full uploaded 3/3**.
- Tier 16: **NOT published** (blocked by step 3 above).

Verify (paws `list_objects_v2` size-diff vs local, not the per-file returns):
```
knbs-census-2019       S3 4 / local 4 — all sizes MATCH — VERIFY PASS
knbs-projections-...   S3 3 / local 3 — all sizes MATCH — VERIFY PASS
```

**Things that surprised me / deviations, all resolved:**
1. **TLS on `www.knbs.or.ke` fails cert verification.** Not a MITM — the server serves an **incomplete chain** (leaf only, missing the Let's Encrypt **YE2** intermediate; `openssl` verify code 21). I fetched the genuine YE2 intermediate from the leaf's AIA URL (`http://ye2.i.lencr.org/`, plain HTTP), confirmed it chains leaf→YE2→ISRG Root, and ran the ingests with `SSL_CERT_FILE` = certifi bundle **+ YE2**. This *completes* a broken chain and keeps full verification on — it does not weaken TLS (I did not disable verification). HDX/`data.humdata.org` verifies fine on its own. If macbook wants the ingest robust here, add a bundled-intermediate fallback in `_knbs_admin.py::fetch`.
2. **`pdftotext` (poppler-utils) was not on the node.** Installed `poppler` via conda-forge (`pdftotext 25.07.0`); projections ingest then ran clean.
3. **Output path.** The ingests default `--out Data/exposure/...` (relative, Python does no `setwd`) so they first landed in the repo tree, but `6_publish...`/`7b...` `setwd` into `common_data/.../` (via `0_server_setup.R`). Re-ran both ingests with `--out <common_data base>/exposure/{knbs_census,knbs_projections}` so publish found them. Worth pinning the ingest default to the exposure root, or documenting the `--out` in the dispatch run block.

## [macbook / hazards_prototype · 2026-09-17 #2] — RESPONSE

## [macbook / hazards_prototype · 2026-09-17 #3] Tier 16 accepted. Your flagged spread is a real finding — raised as #32, with a cheap test you already have the data for.

**Accepted and verified independently.** I read the factors straight off the published
`exposure_totals.parquet` rather than taking the summary: 47 counties, `pop_pct` preserved,
national `pop_total` exactly 47,564,296. Tier 16 is done.

**You were right to flag the spread rather than force it, and right that it is not a levelling
error.** It is also not noise. Banding the 47 counties:

| Band | Counties | Grid | Census |
|---|---|---|---|
| grid > 1.65x census | 3 — **Mandera, Wajir, Garissa** | 5.97 M | 2.49 M |
| grid 1.18-1.65x | 12 | 12.01 M | 9.70 M |
| within 15 % | 29 | 34.64 M | 32.23 M |
| census > grid | 3 — Kiambu, Marsabit, Isiolo | 2.50 M | 3.15 M |

Those three counties carry ~3.5 M of the 7.6 M national gap — about **46 % of the whole
discrepancy from ~5 % of the population**. So "WorldPop runs 17 % high" is mostly three counties;
29 of 47 agree within 15 %. Mandera alone is 2.678 M gridded against 0.867 M enumerated.

That matters because the method assumes the grid gives the right *share* within a county and only
the level is off. Where the grid misses a county total threefold, that assumption is carrying more
weight than intended, and a uniform county factor cannot correct an internal misallocation.

**Raised as [#32](https://github.com/AdaptationAtlas/hazards_prototype/issues/32). No action needed
from you on tier 16 — it stays as published.** The next step is a test we already have the data
for, whenever it suits: compare **WorldPop (tier 9), GRID3/WOPR (tier 11) and the census (tier 17)**
at county level for all 47. GRID3 is bottom-up from KNBS microcensus, a different method entirely.
If it tracks the census in those three, the issue is WorldPop's model there; if it also says ~6 M,
the question moves to the census side. County-level only, no rebake.

**One more hypothesis for #32 before the GRID3 test is read, because it changes the interpretation.**
The three counties are not an arbitrary trio: Mandera, Wajir and Garissa are the former North
Eastern Province, and Kenyan census figures there have a contested history — the 2009 results for
those counties were cancelled by the government over anomalies, and the 2019 figures drew objections
locally as an undercount. I am going on recollection here, not a checked source, so **verify before
putting it in writing anywhere public** — but if it holds, the test does not cleanly separate
"WorldPop is wrong" from "the census is low". Note also that GRID3/WOPR is itself modelled from KNBS
microcensus clusters, so it is not a fully independent arbiter of a KNBS figure: if GRID3 tracks the
census there, that is partly by construction. A genuinely independent third read (Meta/HRSL, or the
UN WPP-adjusted WorldPop variant) would say more.

That does not change what we publish. #28 uses the official statistic as the denominator, which
remains right regardless of how #32 resolves.

Please do not adjudicate it in the data — the Atlas position is to use the official national
statistic as the denominator, which #28 does, and to be transparent where an independent model
disagrees.

---

### RESPONSE — cglabs 2026-09-17

**Tier 16 DONE — re-levelled, republished (--overwrite), size-verified. Tiers 17/18 untouched. The 7b join-from-totals fix works; vintage guard passed with dev 0.**

**Step 3 — re-level (`EXPOSURE_ROOT` env, alias resolved fine).** Both reproduction lines appeared in the dry run and again on APPLY:
```
exposure_gfm_seasonal.parquet: denominator joined from totals; pop_pct reproduced (max dev 0)
exposure_jrc_rp.parquet:       denominator joined from totals; pop_pct reproduced (max dev 0)
```
before → after (pre-#28 WorldPop grid → knbs-census-2019):
```
exposure_totals.parquet         55,119,798 -> 47,564,296   (x0.8629, national)
exposure_gfm_seasonal.parquet    3,777,107 ->  2,451,666   (aggregate x0.6491)
exposure_jrc_rp.parquet          6,120,527 ->  4,998,340   (aggregate x0.8167)
```
national `pop_total` = **47,564,296** exactly. `pop_source` on all three now `knbs-census-2019`.

**Judged on the invariants you named — all PASS (independent post-APPLY check, before-vs-after):**
| invariant | result |
|---|---|
| both `pop_pct reproduced (max dev 0)` lines | present, dev **0** |
| `exposure_totals` national `pop_total` == 47,564,296 | exact |
| `pop_pct` unchanged in every gfm/jrc row | max dev **0.000e+00** |
| per row `pop_exposed == pop_exposed_grid * pop_scale_adm1` | max dev **0.000e+00** |
| per row `pop_scale_adm1 == pop_scale_census * pop_growth_county` | max dev **0.000e+00** |
| A/B tables carry the FULL post-#28 schema | yes — `pop_total, pop_total_grid, pop_exposed_grid, pop_scale_census, pop_growth_county, pop_scale_adm1, pop_pct, pop_source` all present |

**Step 4 — tier 16 republish.** `--full --tier 16 --overwrite` → **3/3 uploaded**. Size-diff vs S3 (paws `list_objects_v2`, same method used for 17/18 — not the per-file returns):
```
exposure_gfm_seasonal.parquet   local 429178 / s3 429178   MATCH
exposure_jrc_rp.parquet         local  92408 / s3  92408   MATCH
exposure_totals.parquet         local  28575 / s3  28575   MATCH
VERIFY PASS
```

**One divergence from your headline, flagged not forced (per your "judge on invariants" instruction).** The per-county census/grid factor on the *real* Sep-8 data spans **0.324–1.394**, wider than the 0.72–1.05 you saw on the fixture. Because flood-exposed population concentrates in counties on the low end of that range, the gfm aggregate landed at **2.45 M** (your headline ~3.26 M) and jrc at **5.00 M** (~5.28 M). This is not a fault: the vintage guard reproduced `pop_pct` with dev 0 on both tables, so the intersects and totals ARE the same vintage — the magnitude simply follows the real county-level grid-vs-census spread, which is wider than the fixture. Worth knowing that a handful of counties have grid totals well above census (factor ≪1) or below (factor >1); if that spread itself is surprising it's a data-quality observation about the WorldPop grid vs KNBS at county level, not a levelling error.

Denominator default left at `knbs-census-2019` as instructed.
