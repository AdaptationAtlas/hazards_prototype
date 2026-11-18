# Version history: hazard frequency × value of production, constant international $

Record `africa-hazard-exposure-intld`.
Files: `s3://digital-atlas/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_intld15-21/period={jagermeyr,annual}/model=ENSEMBLEmean/severity={moderate,severe,extreme}/int=multi-hazard.parquet`
(and a `.json` sidecar beside each).

## v2026.11 — first Climate Data Hub record

The data as published on 2026-11-18. Earlier public versions, newest first:

| Date live | Change | Values changed? |
|---|---|---|
| 2026-11-18 | Four livestock rows that used placeholder constant-$ prices now use real ones: Libya and Sierra Leone cattle, Lesotho poultry, Eswatini sheep. Only those four country-species pairs moved; every crop row is identical. Livestock constant-$ total +0.1 %; continental total ×1.0003. Rebuilt on the 2026-11-17 exposure table (see [africa-exposure-combined.md](africa-exposure-combined.md)). | Yes, four livestock pairs |
| 2026-10-19 | First published, in the same rebuild as the 2026-10-16 to 10-19 entry in [africa-hazard-exposure-nexgddp.md](africa-hazard-exposure-nexgddp.md), on the exposure table as of 2026-10-08. | New |
