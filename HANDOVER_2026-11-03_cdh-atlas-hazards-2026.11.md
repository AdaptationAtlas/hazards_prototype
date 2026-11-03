# Atlas hazard data for the Climate Data Hub — what is live, what changed, and a version to ingest

**Date:** 2026-11-03 · **For:** Brayden (CDH ingest of the Atlas hazards, this month) · **Shape:** topic briefing.
Every number below is from the live bucket or the repo as of 2026-11-03.
The S3 listing and versionIds were taken on that date, read-only.

## 1. Proposal: freeze "Atlas hazards 2026.11" and ingest that

We freeze the hazard products **as they are on S3 today** as one version, and the CDH records describe that version:

- **Version label `2026.11`.** This uses the core field `version`, which the v0.3.0 core schema already has. Its examples
  are `v1.0` and `2.0-release-2`, so use `v1.0` if you prefer a semantic label; it's your call. Later
  republishes become `2026.12`, …, linked by `previous_version`.
- **Pinned bytes.** `s3://digital-atlas` is versioned. **`metadata/cdh/pins/atlas-hazards-2026.11_s3-versions.csv`**
  records key, size, ETag and **S3 versionId** for all 107 objects in this version. Any object can be fetched exactly
  as frozen, even after a later republish: `GET https://digital-atlas.s3.amazonaws.com/<key>?versionId=<id>`.
  Noncurrent versions are kept for at least 270 days.
- **Nothing in this version is mid-change.** Pending work (section 4) publishes as a later version, never into this one.

## 2. What is in the version

| family | S3 prefix | objects | size | last publish | CDH record (`metadata/cdh/`) |
|---|---|---|---|---|---|
| **Hazard × exposure**: hazard frequency × exposure by admin unit, crop/livestock, hazard class, scenario, timeframe | `domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable={vop_nominal-usd21, vop_intld15-21, prod_t, harv-area_ha, head_n}/period={jagermeyr,annual}/model=ENSEMBLEmean/severity={moderate,severe,extreme}/int=multi-hazard.parquet` | 60 | 1.54 GB | 2026-10-19 (`vop_nominal-usd21`: 2026-10-23) | `africa-hazard-exposure-{nexgddp, intld, production, harvested-area, livestock-numbers}.yaml` (strict; `updated` 2026-10-20, so the 10-23 livestock-price note must be added) |
| **Exposure denominator**: crop + livestock value of production, area, production, head, by admin unit | `domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/variable=…` (res-05, res-25, unsuffixed alias = res-05, family twins) | 18 | 0.03 GB | 2026-10-21 | `africa-exposure-combined-res{05,25}.yaml` (strict) |
| **Hazard time series, trends, variability** (the Atlas climate-hazard tab), 18-GCM ensemble, by season | `domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=timeseries_mean_month/timeframe=3months/period=…/baseline={1981-2014,1995-2014}/variable=ensemble_season_{timeseries,trends,variability}.parquet` | 19 | 1.26 GB | 2026-09-20 | **none in v0.3.0.** `ensemble_season_trends.cdh.yaml` predates the standard and is excluded from validation. **A record is needed.** |
| **Hazard change tables** (annual hazard frequency, NTx/THI % area, PTOT change, by model + ensemble) | `domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=hazard-change/timeframe=annual/variable=*.parquet` | 10 | 0.03 GB | 2026-06-24 | **none. A record is needed.** |

**Related, already recorded, not hazard-derived:** the observed climate layers (CHIRPS/CHIRTS monthly and seasonal
PTOT, SPEI; `domain=climate/type=observational/...`) are covered by `africa-precipitation-monthly-seasonal` (merged in
cdh-catalog) and `africa-spei-drought`. They can travel with this version or separately.

**Monthly NEX-GDDP hazard indices** (`domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=global/processing=analysis-ready/model=…`,
16,000 COGs, 86 GB, 2025-05-05) hold **5 GCMs only** and predate the 18-GCM rework. Their record is
`draft/nexgddp-indices-monthly.yaml`, which is still a draft. I suggest leaving them out of 2026.11.

### Caveats a catalogue user needs (each is in the record notes or needs adding)

- **Currency.** `vop_nominal-usd21` is nominal 2021 USD. `vop_intld15-21` is constant 2015 international dollars.
  The two must not be compared or mixed.
- **Hazard × exposure has NaN rows.** About 28 % of admin2 units are NaN on some rows (no exposure or no hazard
  coverage). Sum with NaN-safe aggregation. Values are rounded to integers per pixel before the zonal sums.
- **Soil-layer gap (issue #56, found 2026-11-02).** The NDWS/NDWL0-based hazard sets lose exposure on coastal and
  lakeshore cells where the shared soil rasters are NA. For example, Kenya maize is 0.9756 and cassava 0.9485 of
  exposure, while the soil-free PTOT set is complete. This is a property of this version; the fix comes later.
- **Livestock heat thresholds (THI) are near-saturated** at the moderate tier in the humid tropics. This is known
  (#24/#25 thread) and does not affect values, but it does affect interpretation.
- **Baseline:** NEX-GDDP historical 1995-2014; the 1981-2014 baseline exists for the time series only.

## 3. What changed (June to November 2026, all live)

Only changes that reached S3 are listed, newest first. Each has a closed dispatch thread under `archive/dispatches/`
(index in its `README.md`) with the gates it passed.

| date | product | change | values changed? | ref |
|---|---|---|---|---|
| 2026-10-23 | HE `vop_nominal-usd21` (12 keys) | livestock nominal USD priced at national FAOSTAT prices, not the world median (+4.6 % continental) | yes (livestock) | #47 |
| 2026-10-21 | denominator (18 keys) | livestock nominal-2021 repriced, same change as above | yes | #47 |
| 2026-10-16…19 | HE all 5 variables × 2 periods × 3 severities (60 keys) | full R/2 + R/3 rebake: crop-specific heat family `NTxM` (#25), poultry THI threshold; **first publish** of `vop_intld15-21`, `prod_t`, `harv-area_ha`, `head_n` and of `period=annual`; 60/60 MD5 verified 10-20 | yes; 4 new tiers | #13, #25, #41 |
| 2026-10-08 | denominator | FAOSTAT joins keyed on item code: constant-I$ 200.3 → 240.2 B (+20 %); nominal unchanged | yes (intld) | B5 |
| 2026-10-07 | `vop_nominal-usd-2015` family key | retired (backup kept) | retirement | #23 |
| 2026-10-06 | HE `vop_nominal-usd21`; denominator | revised nominal price method (implied GPV/production, coverage guard, 0.05° allocation) | yes | #38–#40 |
| 2026-09-30 | HE `vop_nominal-usd21` | rebake on native 0.25° exposure | yes | #30 follow-ups |
| 2026-09-28 | denominator | corrected reference; family keys first live | yes | #23, #30 |
| 2026-09-25 | denominator | livestock currency vintage fixed (livestock vs FAOSTAT 1.198 → 1.000); `unit` carries the vintage | yes | #30 |
| 2026-09-20 | time series / trends / variability (19 keys) | rebake with the full 18-GCM ensemble (was mixed 18/13/5); both baselines | yes | #26 |
| 2026-09-16 | HE `vop_nominal-usd21` | explicit `hazard = none` on every combination; NDWS de-saturation | yes | #9, #12 |
| 2026-06-24 | hazard-change tables | 100 mm/yr desert mask (NaN at zero precipitation) | yes | CR-093 |

## 4. Not in this version (coded or validated, publishes later)

- **#54 / #52.** Constant-I$ placeholder fix (4 livestock rows) and a TLU relabel in the denominator.
  Node-validated 2026-11-02, every gate passed; awaiting a publish GO.
- **Observed hazard product (CHIRPS v3 / CHIRTS-ERA5 / AgERA5, 2006-2025).** The same hazard × exposure structure
  under `source=chirps-chirts-agera5`. The full-Africa dry run passed every gate on 2026-11-02; awaiting a publish GO.
  It would be a new record, not a change to the NEX ones.
- **Rangeland forage drought (ASAP FPAR, #31).** Built and cross-node verified; its own product, not yet published.
- **HSH heat index (#14), water-balance v2 (#45), trend significance (CR-117):** not started or blocked.

## 5. Legacy objects still on S3 (do not catalogue)

These are superseded and kept for history. They should not be ingested, or should be marked `deprecated` if
anything already points at them.

| prefix | objects | dates | superseded by |
|---|---|---|---|
| `domain=hazard_exposure/source=atlas_cmip6/region=ssa/processing=hazard-risk-exposure/variable={vop_intld15,vop_usd15,vop_nominal-usd21}/…` | 6,685 | 2025-06 … 2025-11 | the `source=nex-gddp-cmip6` hazard × exposure above |
| `domain=climate/type=hazard-indices/source=atlas_cmip6/region=ssa/…/model=historic` | 860 | 2025-12 | the NEX time series above |
| `*.preFix-2026062*.bak` copies under `processing=hazard-change` (16) and dated backups under `processing=timeseries_mean_month` (19) | 35 | 2026-06 … 2026-09 | the current keys beside them |

## 6. What I need from you

1. **Version label:** `2026.11` or `v1.0`?
2. **Records:**
   - The five hazard × exposure and two denominator records exist and validate; I will refresh their `note`/`updated`
     for the 10-23 change and add `version` plus the pin-file reference.
   - **The time series and hazard-change families have no v0.3.0 record.** I can draft both this week.
   - Should the monthly NEX indices draft stay out of 2026.11?
3. **Submission:** none of these records is in cdh-catalog yet (`metadata/cdh/README.md`). One PR per record, as
   before, or one batch for the hazards version?
4. **Legacy trees:** leave the `source=atlas_cmip6` objects on S3 and uncatalogued, or schedule a clean-up?
   Deleting is a separate decision; on this bucket an `s3fs` delete is permanent.

Sources: S3 listing and HEAD requests (2026-11-03); `metadata/cdh/`; `archive/dispatches/` (closed threads);
GitHub issues on this repo.
