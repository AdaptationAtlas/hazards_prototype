# Atlas hazard data: what is already catalogued, what is new, and change notes

**Date:** 2026-11-13 · **For:** Brayden (STAC, Zenodo and CDH catalogues) · **Shape:** topic briefing.
This follows the 2026-11-03 briefing (`HANDOVER_2026-11-03_cdh-atlas-hazards-2026.11.md`) and your answers to it.
Scope is the NEX-GDDP-CMIP6 hazard chain only. The observed-climate, Kenya ENSO and rangeland datasets are all new
and are not covered here.

Everything below was checked on 2026-11-12/13 from the public bucket listing, the public STAC, the Zenodo API and
the cdh-catalog repo. Nothing was written anywhere.

## 1. The short answer

None of the hazard datasets we publish today is catalogued anywhere.

- **STAC** holds two things: the 2024 hazard layers under the old `hazards/…` paths, and the May-2025 yearly
  hazard-index rasters (5 climate models). Neither is what the Atlas reads today.
- **Zenodo** holds code releases only (the `hazards` repo, the website). There is no dataset DOI for any hazard product.
- **CDH** has no hazard record, merged or in a pull request. The drafts are in this repo under `metadata/cdh/`.

So almost every current dataset is **new** to the catalogues. A few datasets have been on the bucket since
January 2026 and are read directly by the Atlas notebooks. These have changed several times, so they get a change
note (section 4). Pete has agreed that change notes start from the **first public upload**, not from a catalogue
entry, because so few catalogue entries exist.

## 2. What I need from you

1. **Where should the change-note file be hosted?** The v0.5.0 standard (section 4.7) already says how to
   attach one: a link in `additional_links[]` with `rel: version-history`. None of these datasets has an earlier
   Hub record, so per the same section each first record is `version: v2026.11` with no `previous_version`, and the
   history before it goes in the linked file. What I am doing now, unless you prefer otherwise:
   - one Markdown change-note file per record in our repo, `metadata/cdh/changelogs/<record-id>.md`;
   - linked from the record with `rel: version-history`;
   - a one-line pointer in `note`.

   Would you rather the file sit next to the record in cdh-catalog, or next to the data on S3?
2. **Old STAC hazard collections.** These are the 2024 layers: annual and seasonal hazard time series,
   hazard × risk, hazard × risk × value of production, classified hazards and hazard means. Every asset link in them
   has a double slash (`amazonaws.com//hazards/…`) and returns "access denied", so they are broken as published.
   Should they be marked deprecated and pointed at the new records, fixed, or removed? Pete says this is your call.
3. **STAC collection `nex-gddp-cmip6-hazards`** (16,000 yearly rasters, 5 models, May 2025). Your new zarr
   store replaces it. Should it be marked deprecated when the zarr record goes live? Its metadata faults are listed in
   appendix B, in case it stays up for a while.
4. **Side STAC trees.** `stac/AtlasV3/` has two hazard × exposure items, and both point at the superseded
   `source=atlas_cmip6` tables. Its exposure item points at a table we have overwritten five times since. Can these
   items be retired?
5. **One change to the 2026.11 version.** The 03 November briefing said nothing mid-change would go into
   2026.11. Pete has now agreed to **republish the hazard-change tables before the freeze**. The live ones date from
   24 June and miss four months of fixes. A rebuilt set has existed since 15 October. Once it is published, the
   ten pinned rows in the pin file change. Is that fine with you?
6. **Dataset DOIs.** Do you want a Zenodo dataset deposit (DOI) for the 2026.11 version, or is the CDH record
   enough? Also, the Zenodo text for the hazards NEX-GDDP code release (record 18164335) still describes the
   delta method. That can be fixed at the next release.

7. **Country rows for Egypt, Kenya, Sudan and South Sudan are broken in the seasonal files.** These are the four
   countries that claim a disputed area: Ilemi, Abyei, Hala'ib, Bir Tawil.
   - **Time series:** each of these countries has one country-level row per file instead of one per year, with an
     inflated value. Kenya March-May rainfall reads 9,346 mm against about 400 mm for its neighbours. It looks like
     every year summed into one row.
   - **Trends and variability:** these rows are empty.
   - First-level units are fine.

   This is the same disputed-territory question you settled in June: count each disputed area for every claimant,
   area-weighted. That fix was held for your grid-index test. The records carry a caveat for now. Do we fix the
   country rows before the freeze, or freeze with the caveat?

## 3. The map

"Updated" means the data was public before and has changed since. "New" means it has never been catalogued and
is either recent or was never announced.

| Dataset (plain name) | S3 location | Files | Catalogued? | Status | Change note? |
|---|---|---|---|---|---|
| Yearly hazard-index rasters, 5 models, 2021-2100 | `domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=global/…/processing=analysis-ready/` | 16,000 | STAC `nex-gddp-cmip6-hazards` | Catalogued and unchanged since May 2025. Out of date (5 models, not 18). Your zarr replaces it | No |
| Hazard × exposure tables: value of production, nominal US$ 2021, crop-calendar season | `domain=hazard_exposure/source=nex-gddp-cmip6/…/variable=vop_nominal-usd21/period=jagermeyr/` | 3 (+3 sidecars) | No | **Updated.** Public since January 2026 and read by the Atlas | **Yes**, appendix A1 |
| The same, for the annual period | `…/variable=vop_nominal-usd21/period=annual/` | 3 (+3) | No | New on 19 Oct; repriced 23 Oct | Short, A1 |
| Hazard × exposure: value of production in constant international $; harvested area; production; livestock head | `…/variable={vop_intld15-21, harv-area_ha, prod_t, head_n}/period={jagermeyr,annual}/` | 24 (+24) | No | New (first published 19 Oct) | No, initial release |
| Crop and livestock exposure totals by admin unit (0.05° and 0.25°) | `domain=exposure/type=combined/source=glw4-2020_spam2020AA/…/variable=crop-livestock_all{,_res-05,_res-25}.parquet` | 3 (+3) | Only the side tree `stac/AtlasV3` | **Updated.** Public since January 2026 | **Yes**, A2 |
| Crop and livestock value-of-production totals, one currency per file | `…/variable={vop_nominal-usd-2021, vop_intld15-2021}{,_res-05,_res-25}.parquet` | 6 (+6) | No | Earlier versions public since late 2025 | **Yes**, A2 |
| Seasonal hazard time series, 18-model ensemble | `domain=climate/…/region=africa/processing=timeseries_mean_month/…/variable=ensemble_season_timeseries.parquet` | 10 | No | **Updated.** Public since at least May 2026 | **Yes**, A3 |
| Seasonal hazard trends | `…/variable=ensemble_season_trends.parquet` | 4 | No | New in June; rebuilt in September | Short, A3 |
| Seasonal hazard year-to-year variability | `…/variable=ensemble_season_variability.parquet` | 5 | No | New in June; rebuilt in September | Short, A3 |
| Hazard change tables (frequency, heat % area, rainfall change) | `domain=climate/…/region=africa/processing=hazard-change/` | 10 | No | New in June. **Republish pending** (item 5) | Yes, once republished, A4 |

**Not to be catalogued (superseded, still on the bucket):**
- the two `source=atlas_cmip6` trees: about 7,500 objects, 75 GB;
- the 2024 `hazards/…` and `risk_prototype/…` paths;
- 35 backup copies (`*.preFix*.bak`) sitting next to the live tables.

Clean-up is the agreed later task.

## 4. What happens next on our side

1. Publish the rebuilt hazard-change tables. Before that, we check one thing in them: a shift in country-level
   livestock-heat figures in 19 countries, for example Sudan 100 % → 11.5 % in one model-scenario. It looks like the
   border-cell fix, but it needs a look first. We then update the pin file.
2. Move our records to v0.5.0, merging the 0.05° and 0.25° exposure records into one record with two structures,
   as you asked.
3. Write the two missing records: the seasonal time series, trends and variability; and the hazard-change tables.
4. Add the change notes in whatever form you choose under item 1.
5. One pull request per record.

Two further changes are ready but not yet published:
- a small constant-$ correction for four livestock rows, +0.1 % on that total;
- a relabel of livestock totals to tropical livestock units.

They go into 2026.11 if Pete releases them before the freeze, otherwise into the next version.

---

## Appendix A. Change notes (draft, newest first)

Dates are the dates the change went live on S3.

### A1. Hazard × exposure tables, value of production in nominal US$ 2021

| Date | Change | Values changed? |
|---|---|---|
| 2026-10-23 | Livestock now priced at each country's own FAOSTAT price instead of the world median. Livestock total +4.6 %; country-species values move 0.43× to 2.2×. Crop values unchanged. | Yes, livestock |
| 2026-10-16 to 10-19 | Full rebuild. The crop heat measure now uses crop-specific temperature thresholds, and the "low rain + crop heat + high rain" combination is replaced by its crop-specific version. A new combination is added: dry soil + crop heat + waterlogging. The poultry extreme heat threshold is raised (THI 79 → 89). The annual period is published for the first time. Continental totals move by less than 0.5 %. | Yes, small |
| 2026-10-06 | New crop price method: value of production ÷ production from FAOSTAT, with a guard where coverage is thin. Some crops move a lot at continental scale: cowpea ×0.33, plantain ×0.41, coffee ×0.57, cassava ×0.78, coconut ×0.75. Livestock unchanged. | Yes |
| 2026-09-30 | Rebuilt on exposure prepared directly at 0.25° instead of resampled. Most crops stay within a quarter; sugarcane roughly halves. Sidecars now state the ensemble size (18). | Yes |
| 2026-09-16 | Every hazard combination now has an explicit "no hazard" row, so hazard rows plus no-hazard rows add up to total exposure. Historical dry-soil frequency corrected (it was saturated near 100 %). Moderate and extreme severities republished alongside severe. | Yes; rows added |
| 2026-05-30 | Republished from a fresh build. What changed is not written down; most likely the fix for missing values in the sums (size changed by 17 kB). | Yes, small |
| 2026-05-26 | Interim replacement of the January table with a build from August 2025 that passes the exposure mass-conservation check. | Yes |
| 2026-01-21 | First public version, severe tier only. Moderate and extreme were added by June 2026; the exact date is not recorded. | — |

### A2. Crop and livestock exposure totals and value-of-production totals

| Date | Change | Values changed? |
|---|---|---|
| 2026-10-21 | Livestock nominal US$ repriced at national FAOSTAT prices (104.2 → 109.1 billion, +4.6 %). | Yes, livestock nominal |
| 2026-10-08 | FAOSTAT matched on item code, not item name (57 renamed items were missing). Constant-$ total 200.3 → 240.2 billion (+20 %). Nominal unchanged. | Yes, constant $ |
| 2026-10-07 | The 2015 nominal-US$ file was retired. | File removed |
| 2026-10-06 | New nominal price method (as in A1), millet split, Seychelles fix. | Yes |
| 2026-09-28 | Crop nominal values regenerated with outlier prices clipped (Zimbabwe wheat, Rwanda oil palm). Separate 0.05° and 0.25° files first published. | Yes; new files |
| 2026-09-25 | Livestock currency year fixed: livestock had been 1.198× FAOSTAT and is now 1.000×. The `unit` column now names the currency and year. First 0.25° version. | Yes; unit labels changed |
| 2026-01-21/22 | First version of the combined table now in use. The value-of-production files had earlier versions from October and November 2025. | — |

### A3. Seasonal hazard time series, trends and variability

| Date | Change | Values changed? |
|---|---|---|
| 2026-09-20 | All three rebuilt on the full 18-model ensemble; before, some hazards used 5 or 13 models. Historical series fixed. Anomalies had been computed against 1981-2014 but labelled 1995-2014; now correct. Five files with a 1981-2014 baseline added. | Yes; new files |
| 2026-06-15 | Variability files first published. | New |
| 2026-06-12 | Time series cleaned up: country code restored; spread columns kept (mean, sd, 17th/83rd percentile, model count); min, max and other percentiles dropped. Trends first published (slope and change per decade; significance not published). | Yes; columns changed |
| 2026-06-05 | A faulty version was published (country code missing, file 14× larger) and replaced on 06-12. | Yes, then reverted |
| 2026-05-27 | Rebuilt for faster browser queries; min and max columns added. | Columns added |

### A4. Hazard change tables

| Date | Change | Values changed? |
|---|---|---|
| pending (2026.11) | Rebuilt on the 18-model ensemble and the October fixes. Duplicate rows removed (the old files had 1,100-9,800 duplicates per file). Hazard frequency differs on 2.7 % of rows; heat % area differs at country level only. | Yes |
| 2026-06-24 | First published. Rainfall % change masked where baseline rainfall is under 100 mm/yr. | New |

## Appendix B. STAC and Zenodo faults found

STAC:
- The root `catalog.json` lists the Climate catalogue twice.
- Old hazard collections:
  - every asset link has `//hazards/…` and returns 403 (with one slash it returns 200);
  - the annual time-series collection has 4,951 item links but only 100 unique items;
  - the licence is "proprietary" on most, CC-BY-4.0 on one.
- `nex-gddp-cmip6-hazards`:
  - each item's end date is a year too late (a one-year file spans two years);
  - the extent starts in 1981 but there are no historical items;
  - there are no created/updated dates;
  - 5 models, while the matching code release says 18;
  - licence CC-BY-SA-4.0, while the code is GPL-3.0.
- No public code builds the hazard collections. `data-management/stac/` builds only the root and theme catalogues.
- `stac/dev_stac/` is an empty skeleton with a different layout from the public tree.
- `stac/stacV2/climate/` is a partial copy of the NEX collection (2 models).

Zenodo:
- Records 18164183 (delta method) and 18164335 (NEX-GDDP) are two versions of one concept, both v0.1.0 code.
- 18164335's description is the delta-method text.
- The website release (17899975) has a typo in its title: "Agricultultural".
- `hazards_prototype` has no release and no DOI.

## Sources

- Live S3 listing (anonymous, 2026-11-12). Version history is not readable anonymously, so the January dates come
  from earlier authenticated listings recorded in the publish logs.
- Pin file `metadata/cdh/pins/atlas-hazards-2026.11_s3-versions.csv`: all 107 objects are still live and unchanged.
- Closed dispatch threads under `archive/dispatches/` (index in its README), and `logs/E_publish_to_s3_2026052*.log`.
- Public STAC root `https://digital-atlas.s3.amazonaws.com/stac/public_stac/catalog.json`; Zenodo API; cdh-catalog
  (17 merged records, none for hazards); cdh-metadata-standard v0.5.0 schema.
