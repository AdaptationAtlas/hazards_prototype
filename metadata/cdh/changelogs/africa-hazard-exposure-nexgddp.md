# Version history: hazard frequency × value of production, nominal US$ 2021

Record `africa-hazard-exposure-nexgddp`.
Files: `s3://digital-atlas/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_nominal-usd21/period={jagermeyr,annual}/model=ENSEMBLEmean/severity={moderate,severe,extreme}/int=multi-hazard.parquet`
(and a `.json` sidecar beside each).

## v2026.11 — first Climate Data Hub record

The data as published on 2026-10-23. Earlier public versions of these files, newest first:

| Date live | Change | Values changed? |
|---|---|---|
| 2026-10-23 | Livestock now priced at each country's own FAOSTAT producer price instead of the world median. The livestock total rises 4.6 %; individual country-species values move between 0.43× and 2.2×. Crop values are unchanged. | Yes, livestock only |
| 2026-10-16 to 2026-10-19 | Full rebuild of the hazard frequencies. The crop heat measure now uses crop-specific temperature thresholds. The combination "low rainfall + crop heat + high rainfall" uses those thresholds, replacing the earlier version. A new combination is added: dry soil + crop heat + waterlogging. The extreme heat threshold for highland poultry is raised from THI 79 to 89. The `period=annual` files are published for the first time. Continental totals move by less than 0.5 %. | Yes, small; new combinations |
| 2026-10-06 | New crop price method: value of production divided by production from FAOSTAT, with a guard where FAOSTAT coverage is thin. Some crops move a lot at continental scale: cowpea ×0.33, plantain ×0.41, coffee ×0.57, coconut ×0.75, cassava ×0.78. Livestock unchanged. | Yes |
| 2026-09-30 | Rebuilt on exposure prepared directly on the 0.25° hazard grid instead of resampled from 0.05°. Most crops change by less than a quarter; sugarcane roughly halves. The sidecars now state the ensemble size (18 models). | Yes |
| 2026-09-16 | Every hazard combination now has an explicit "no hazard" row, so hazard rows plus the no-hazard row add up to total exposure. The historical dry-soil frequency is corrected; it had been saturated near 100 %. Exposure is resampled with a mass-conserving sum. All three severities are republished. | Yes; rows added |
| 2026-05-30 | Republished from a fresh build. The change is not documented; it is most likely the fix for missing values in the zonal sums. The file size changed by 17 kB. | Yes, small |
| 2026-05-26 | The January file is replaced with a build from August 2025 that passes the exposure mass-conservation check. | Yes |
| by June 2026 | `severity=moderate` and `severity=extreme` first published. The exact date is not recorded. | New files |
| 2026-01-21 | First public version (`severity=severe`, `period=jagermeyr`). | — |
