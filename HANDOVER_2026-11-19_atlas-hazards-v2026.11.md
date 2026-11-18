# Atlas hazards `v2026.11` — catalogue map, CDH records, and what is left

**Date:** 2026-11-19 · **For:** the next session picking this up (Claude Desktop or Code) and Pete ·
**Shape:** topic briefing. Covers the NEX-GDDP-CMIP6 hazard chain only; the observed, Kenya-ENSO and
rangeland products are separate threads.

Links below are relative to the repo root; on GitHub prefix
`https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/`.

## 1. Where it stands

**Done:**
- **The v2026.11 data set is complete on S3** (`s3://digital-atlas`). All 107 objects are pinned by
  versionId in [metadata/cdh/pins/atlas-hazards-2026.11_s3-versions.csv](metadata/cdh/pins/atlas-hazards-2026.11_s3-versions.csv).
  Two publishes went into it after the 11-03 freeze proposal:
  - the rebuilt hazard-change tables, 10 keys, on 2026-11-16;
  - the #54 constant-$ placeholder fix: denominator 18 keys on 2026-11-17, intld tier 12 keys on
    2026-11-18.
- **The catalogue map** is [HANDOVER_2026-11-13_catalog-map.md](HANDOVER_2026-11-13_catalog-map.md),
  addressed to Brayden. In short: **no current NEX hazard product is catalogued anywhere.**
  - STAC holds only the 2024 `hazards/…` layers (their links are broken) and the 5-model May-2025
    yearly COGs.
  - Zenodo holds code releases only.
  - CDH holds nothing for hazards.
- **Ten CDH records on schema v0.5.0, all `version: v2026.11`**, in [metadata/cdh/](metadata/cdh/).
  All pass strict validation and prettier.
  - Five hazard × exposure records: `africa-hazard-exposure-{nexgddp,intld,harvested-area,production,livestock-numbers}`.
  - `africa-exposure-combined`, which merged the old res-05 and res-25 drafts.
  - Four new records: `africa-hazard-season-{timeseries,trends,variability}-nexgddp` and
    `africa-hazard-change-nexgddp`. The three season records each carry a `*-files.csv` inventory.
- **Changelogs**, one per record, in [metadata/cdh/changelogs/](metadata/cdh/changelogs/). Each record
  links its changelog with `rel: version-history`.
  - History runs from the first public S3 upload (Pete's decision).
  - Brayden agreed on 2026-11-17 that they live in this repo.
- **Publisher fixes** (`d098557`) in `scripts/r3_publish_tiers.R`:
  - `--tlu-slot` declares the #52 TLU addition;
  - a gate ABORT now exits 1 (it used to exit 0).

  Both are covered by `R/checks/fixture_publish_tier_gates.R`.
- **Prompt codes:** every macbook dispatch now has a code like `M-YYYYMMDD-<topic>-<n>`, and the node
  ends every reply with `Answers: <code> (status)` (rule in [AGENTS.md](AGENTS.md) §1). The node
  confirmed on 2026-11-18.

**Not done — the next actions:**

| # | What | Owner | Next action |
|---|---|---|---|
| 1 | **Open the 10 cdh-catalog PRs** | Pete's go | Drafts and the exact loop are in [metadata/cdh/submissions/v2026.11/](metadata/cdh/submissions/v2026.11/README.md). Merge order: `africa-exposure-combined` and `…-season-timeseries-…` first. Claude Code's auto-mode permission check **blocked** opening them as outward communication, so Pete must say go explicitly or run the loop himself. Record the PR numbers in [metadata/cdh/README.md](metadata/cdh/README.md) afterwards |
| 2 | **Disputed-country rows in the seasonal files** (briefing Q7) | Brayden decides | EGY/KEN/SDN/SSD country rows are collapsed into one wrong row per file (Kenya MAM rainfall 9,346 mm); trends and variability rows are empty; first-level units are fine. Measured on [#11](https://github.com/AdaptationAtlas/hazards_prototype/issues/11). The fix is the agreed claimant convention (count each disputed area for every claimant, area-weighted), held since June for Brayden's grid-index test. Fix before ingest, or ingest with the caveat already in the records |
| 3 | Legacy STAC collections, the 5-model STAC collection, side STAC trees, dataset DOIs (briefing Q2-4, Q6) | Brayden | Wait for answers. Nothing to do on our side until then |
| 4 | Bucket clean-up: 26 `.preFix*.bak` objects in the hazard-change prefix, 19 in the timeseries prefix, ~75 GB legacy `atlas_cmip6` trees | #59, later | Agreed as a future task. **`s3fs` deletes are permanent** on this bucket; use `paws` `delete_object` without a VersionId |
| 5 | Pending value changes, each a later version (`v2026.12+`) | — | HSH-max recalculation (#14, about +7 °C in all 19 season files); head_n TLU tier (#52); soil-mask hole (#56); trend significance (withheld) |

## 2. How to work on it

- **Validate a record** exactly as catalog CI does:
  ```bash
  git clone --depth 1 --branch v0.5.0 https://github.com/CGIAR-Climate-Data-Hub/cdh-metadata-standard.git /tmp/cdh-std
  (cd /tmp/cdh-std && npm ci)
  node /tmp/cdh-std/scripts/validate-yaml.js --profile /tmp/cdh-std/spec/schemas/profiles/cdh.schema.json metadata/cdh/<id>.yaml
  npx prettier@3 --check metadata/cdh/<id>.yaml
  ```
  The other records in `metadata/cdh/` are still v0.3.0. Validate those with the v0.3.0 tooling.
- **When a dataset is republished:**
  1. Repin its rows in the pins CSV from the node's RESPONSE table (key, size, ETag, LastModified,
     versionId).
  2. Add a dated row to its changelog.
  3. Fix any "pending/coming" wording in the record's `note`.
  4. Re-validate.
- **Read the live parquets in place:** DuckDB over `https://digital-atlas.s3.amazonaws.com/<key>`
  with `hive_partitioning=false`. Otherwise the `timeframe=…` in the path overwrites the
  `timeframe` column.
- **Node work goes through dispatch files** (`DISPATCH_cglabs_*.md`, newest block on top, coded). Read
  AGENTS.md §1-2 first. Closed threads from this work are in `archive/dispatches/`:
  - `DISPATCH_cglabs_issue54_intld_placeholders.md`
  - `DISPATCH_cglabs_r22_hazard_change_publish.md`
  - the superseded `HANDOVER_2026-11-03_cdh-atlas-hazards-2026.11.md`
- **Writing for Brayden:** plain sentences, decisions first, CDH terms, no issue-number shorthand
  without a gloss. He found the 11-03 briefing too jargon-heavy.

## 3. Loose ends noticed, not part of this thread

- These files in the working tree belong to other sessions and are left untouched:
  - `metadata/cdh/enso-driver-indices.yaml` (modified);
  - `HANDOVER_2026-10-07_gcf-theme2-extremes-inventory.md` and
    `HANDOVER_2026-11-07_sfp-gaia-s3-conventions.md` (untracked);
  - `metadata/checks/68_categorisation_stage1.json` (untracked).
- The SFP briefing (`HANDOVER_2026-11-07_sfp-gaia-s3-conventions.md`) cites the 11-03 briefing at its
  old root path; it now lives in `archive/dispatches/`.
- `docs/briefings/atlas-hazards-cdh-2026.11.html` is the HTML render of the 11-03 briefing and still
  names the old res-05/res-25 record ids.
- `archive/dispatches/HANDOVER_2026-06-24.md:9` contains a Copernicus account ID in this public repo
  (identifying, not a credential). It has not been scrubbed. Removing it from git history needs Pete's
  call.
