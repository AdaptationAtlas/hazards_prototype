# Version history for the Atlas hazard records

One file per CDH record. Each record links its file through `additional_links[]` with
`rel: version-history` (CDH metadata standard v0.5.0, section 4.7).

None of these datasets had a Climate Data Hub record before `v2026.11`, so the first record has no
`previous_version`. The changes made before that version are listed here instead. They are dated by
when the change went live in `s3://digital-atlas`. Later versions will add a section at the top.

Every object in `v2026.11` is pinned by S3 version id in
`../pins/atlas-hazards-2026.11_s3-versions.csv`. The detailed record of each change, including the
checks it passed, is in the closed dispatch threads under `archive/dispatches/` (see its README).

| record | file |
|---|---|
| `africa-hazard-exposure-nexgddp` (value of production, nominal US$ 2021) | [africa-hazard-exposure-nexgddp.md](africa-hazard-exposure-nexgddp.md) |
| `africa-hazard-exposure-intld` (value of production, constant international $) | [africa-hazard-exposure-intld.md](africa-hazard-exposure-intld.md) |
| `africa-hazard-exposure-harvested-area` | [africa-hazard-exposure-harvested-area.md](africa-hazard-exposure-harvested-area.md) |
| `africa-hazard-exposure-production` | [africa-hazard-exposure-production.md](africa-hazard-exposure-production.md) |
| `africa-hazard-exposure-livestock-numbers` | [africa-hazard-exposure-livestock-numbers.md](africa-hazard-exposure-livestock-numbers.md) |
| `africa-exposure-combined` (crop and livestock exposure totals, 0.05° and 0.25°) | [africa-exposure-combined.md](africa-exposure-combined.md) |
| `africa-hazard-season-timeseries-nexgddp` | [africa-hazard-season-timeseries-nexgddp.md](africa-hazard-season-timeseries-nexgddp.md) |
| `africa-hazard-season-trends-nexgddp` | [africa-hazard-season-trends-nexgddp.md](africa-hazard-season-trends-nexgddp.md) |
| `africa-hazard-season-variability-nexgddp` | [africa-hazard-season-variability-nexgddp.md](africa-hazard-season-variability-nexgddp.md) |
| `africa-hazard-change-nexgddp` | [africa-hazard-change-nexgddp.md](africa-hazard-change-nexgddp.md) |
