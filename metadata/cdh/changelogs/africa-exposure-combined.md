# Version history: crop and livestock exposure by administrative unit

Record `africa-exposure-combined`. This replaces the two draft records `africa-exposure-combined-res05` and
`-res25`, which are now one record with a structure per resolution.
Files: `s3://digital-atlas/domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/variable={crop-livestock_all, vop_nominal-usd-2021, vop_intld15-2021}{_res-05,_res-25,}.parquet`
(and a `.json` sidecar beside each). The unsuffixed names are aliases of `_res-05`.

## v2026.11 — first Climate Data Hub record

The data as published on 2026-10-21. Earlier public versions of these files, newest first:

| Date live | Change | Values changed? |
|---|---|---|
| 2026-10-21 | Livestock nominal US$ priced at each country's own FAOSTAT producer price instead of the world median: 104.2 → 109.1 billion US$ (+4.6 %). | Yes, livestock nominal |
| 2026-10-08 | FAOSTAT matched on item code instead of item name, which recovers 57 renamed items. Constant-$ total 200.3 → 240.2 billion (+20 %). Nominal unchanged. | Yes, constant $ |
| 2026-10-07 | The 2015 nominal-US$ file (`vop_nominal-usd-2015`) is retired. | File removed |
| 2026-10-06 | New nominal price method (value of production divided by production, with a coverage guard). Millet split into its FAOSTAT items; a Seychelles fix. Row count +1.9 %. | Yes |
| 2026-09-28 | Crop nominal values regenerated with outlier prices clipped; this removes currency artefacts such as Zimbabwe wheat and Rwanda oil palm. The `_res-05` and `_res-25` value-of-production files are published for the first time. | Yes; new files |
| 2026-09-25 | Livestock currency year fixed: livestock values had been 1.198× FAOSTAT and are now 1.000×. The `unit` column now names the currency and year (`intld15-2021`, `nominal-usd-2021`). First 0.25° version (`_res-25`). | Yes; unit labels changed |
| 2026-01-21/22 | First version of the combined table now in use. Earlier versions of the value-of-production files exist from October and November 2025. | — |

### Known, coming in a later version

- A correction for four livestock rows that used placeholder constant-$ prices: Libya and Sierra Leone cattle,
  Lesotho poultry, Eswatini sheep. Livestock constant-$ total +0.1 %.
- The livestock "total" rows will be relabelled as tropical livestock units (`exposure = tlu`).
