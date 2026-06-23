# THI livestock heat-stress thresholds — provenance

Source documentation for the `THI_max` / `THI_mean` livestock rows in
[`haz_classes.csv`](haz_classes.csv) (species split into `_tropical` / `_highland`,
levels Moderate / Severe / Extreme). Verified 2026-06-23 against the open-access
primary source.

## Two separate citations

The Atlas THI hazard combines a **formula** from one paper with **thresholds**
from another:

1. **THI formula** — Rahimi, J. et al. (2020). Cited in
   [`haz_metadata.csv`](haz_metadata.csv) (THI_max / THI_mean `method.description`):
   `THI = (1.8·Tdb + 32) − [(0.55 − 0.0055·RH) × (1.8·Tdb − 26.8)]`, with
   `Tdb` = dry-bulb temperature (max for `THI_max`, mean for `THI_mean`, °C) and
   `RH` = relative humidity (%). Output in °F.

2. **Severity thresholds** — **Thornton, P., Nelson, G., Mayberry, D. & Herrero, M.
   (2021). "Increases in extreme heat stress in domesticated livestock species
   during the twenty-first century." *Global Change Biology* 27(22): 5762–5772.**
   doi:[10.1111/gcb.15825](https://doi.org/10.1111/gcb.15825).
   Open access: [PMC9292043](https://pmc.ncbi.nlm.nih.gov/articles/PMC9292043/).

## Zone mapping

Atlas `_highland` = Thornton **"temperate"** zone; Atlas `_tropical` = Thornton
**"tropical"** zone. (The highland/tropical split is applied spatially via the
`afr-highlands.asc` mask — a binary, ~10-arc-min Africa highlands layer pulled from
`s3://digital-atlas/afr_highlands/`; empirically a ~1000 m elevation floor over
selected highland massifs, exact derivation not documented in-repo.)

## Which table feeds which level

- **Extreme** ← Thornton **Table 2** (zone-specific THI at which ~10% of animals in
  the zone currently sit).
- **Moderate / Severe** ← Thornton **Table 1** (per-species literature compilation;
  e.g. `cattle_highland` 72/79 = Cattle–dairy row; `cattle_tropical` 82 = Cattle–beef row;
  `poultry_highland` 71/76 = layers; `poultry_tropical` 74/79 = broilers).

## Verified Extreme thresholds (Thornton Table 2 vs `haz_classes.csv`)

| species | temperate (→ `_highland`) | tropical (→ `_tropical`) | csv match |
|---|---|---|---|
| cattle | 89 | 94 | ✓ |
| goats | 89 | 94 | ✓ |
| sheep | 86 | 93 | ✓ |
| pigs | 89 | 92 | ✓ |
| poultry | 89 | 92 | ✓ **after the 2026-06-23 fix** |

## Corrections

- **2026-06-23 — `poultry_highland` Extreme 79 → 89** (`THI_max` and `THI_mean`).
  The value 79 was a transcription error (it matches no Thornton value and sat only
  +3 above the Severe threshold, vs ~+10 for every other species). Thornton Table 2
  temperate poultry = **89**, confirmed against the PMC full text. Corrected.

## To review (not yet changed)

- `goats_highland` Moderate = **72** in the csv, but Thornton Table 1 goats "moderate"
  reads **70** in the open-access extract. May be a second small slip or sourced from a
  different Table 1 goat row — verify against the full table before changing.
- `haz_metadata.csv` THI_mean `method.description` still says Tdb is "the maximum
  temperature" (copied from THI_max) — cosmetic, but should read "mean temperature".

## Consumption note

`R/2.2_haz_change.R` currently processes only `cattle_highland` / `cattle_tropical`
for THI; the goats / sheep / pigs / poultry thresholds exist in the csv but are not
yet used downstream.
