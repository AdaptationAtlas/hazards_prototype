# Version history: hazard frequency × value of production, constant international $

Record `africa-hazard-exposure-intld`.
Files: `s3://digital-atlas/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_intld15-21/period={jagermeyr,annual}/model=ENSEMBLEmean/severity={moderate,severe,extreme}/int=multi-hazard.parquet`
(and a `.json` sidecar beside each).

## v2026.11 — first Climate Data Hub record

The data as published on 2026-10-19. That was the first public release of these files, built in the same rebuild as
the 2026-10-16 to 10-19 entry in [africa-hazard-exposure-nexgddp.md](africa-hazard-exposure-nexgddp.md). No change
since.

The value basis is the crop and livestock exposure table as of 2026-10-08. Its constant-$ history is in
[africa-exposure-combined.md](africa-exposure-combined.md).

### Known, coming in a later version

A correction for four livestock rows that used placeholder constant-$ prices: Libya and Sierra Leone cattle, Lesotho
poultry, Eswatini sheep. It raises the livestock constant-$ total by 0.1 %. It has been validated but not published.
