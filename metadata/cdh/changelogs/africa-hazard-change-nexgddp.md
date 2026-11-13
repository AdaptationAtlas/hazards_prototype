# Version history: hazard change tables, NEX-GDDP-CMIP6

Record `africa-hazard-change-nexgddp`.
Files: `s3://digital-atlas/domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=hazard-change/timeframe=annual/variable=*.parquet`
(10 files: hazard frequency, heat % area for NTx and THI, rainfall change and difference, each by model and as an
ensemble).

## v2026.11 — first Climate Data Hub record

**Pending republish.** This section describes the files once the rebuilt set is published, before the 2026.11 freeze.
Until then the live files are the 2026-06-24 build.

| Date live | Change | Values changed? |
|---|---|---|
| pending | Rebuilt on the 18-model ensemble and the October 2026 hazard fixes. Duplicate rows are removed; the old files had between 1,100 and 9,800 duplicates each. No key combinations are gained or lost. Hazard frequency differs on 2.7 % of rows. THI heat % area differs at country level only (0.1 % of rows), in 19 countries. NTx heat % area is unchanged. | Yes |
| 2026-06-24 | First published. Non-finite values set to missing. Rainfall % change is masked where baseline rainfall is under 100 mm/yr, to avoid huge percentages in deserts. | New |
