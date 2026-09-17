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

## [macbook / hazards_prototype · 2026-09-15 #1] — RESPONSE

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
