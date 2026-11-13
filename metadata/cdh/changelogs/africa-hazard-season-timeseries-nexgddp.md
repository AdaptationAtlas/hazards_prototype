# Version history: seasonal hazard time series, NEX-GDDP-CMIP6 ensemble

Record `africa-hazard-season-timeseries-nexgddp`.
Files: `s3://digital-atlas/domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=timeseries_mean_month/timeframe=3months/period=…/baseline={1981-2014,1995-2014}/variable=ensemble_season_timeseries.parquet` (10 files).

## v2026.11 — first Climate Data Hub record

The data as published on 2026-09-20. Earlier public versions, newest first:

| Date live | Change | Values changed? |
|---|---|---|
| 2026-09-20 | Rebuilt on the full 18-model ensemble; before, some hazards used 5 or 13 models. The historical series is fixed. Anomalies had been computed against 1981-2014 while labelled 1995-2014; they are now correct. Five files with `baseline=1981-2014` are added. | Yes; new files |
| 2026-06-12 | Cleaned up. The country code (`iso3`) is restored. The spread columns kept are mean, standard deviation, 17th and 83rd percentiles and model count. Min, max and the other percentiles are dropped. | Yes; columns changed |
| 2026-06-05 | A faulty version was published: country code missing, files about 14× larger. It was replaced on 2026-06-12. | Yes, then replaced |
| 2026-05-27 | Rebuilt for faster browser queries (smaller row groups); min and max columns added. | Columns added |
| before 2026-05-27 | Earlier public version. Its first publication date is not recorded. | — |

### Known, coming in a later version

The heat-stress index for people and livestock (`HSH-max`) will be recalculated. The fix is coded; it raises HSH by
about 7 °C.
