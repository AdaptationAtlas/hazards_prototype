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

- **Extreme ← Thornton Table 2** (zone-calibrated). The paper's methods are explicit
  that the extreme thresholds were **not** taken from Table 1's breed values: they
  calibrated, per species and zone, to the THI at which ~10% of that population
  currently sits (e.g. "several studies agree on THI 89 as the onset of extreme heat
  stress in temperate cattle … nearly 10% of temperate cattle are in places with ≥1
  day/yr THI>89"; tropical cattle calibrated to 94 ≈ 7%). All five species verified
  (see table below).

- **Moderate / Severe ← a curated selection across Thornton Table 1 + other sources,
  NOT a single clean per-species row.** Table 1 is itself a multi-source compilation:
  *general* THI bands (Thom 1959 = 70/75/80; Fuquay 1981 = 72/78/90) **plus**
  per-species / per-breed rows (cattle-dairy 72/79/89, cattle-beef 72/82/94,
  goats 70/79/89, sheep 72/78/90, pigs 75/79/84, poultry-broilers 74/79/84,
  poultry-layers 71/76/82, poultry-general 73/81/85). The Atlas Moderate/Severe
  values draw on a mix of these — they do **not** map 1:1 to each species' own row.
  Observed pattern: `_highland` Moderate ≈ 72 (the Fuquay/dairy general value),
  `_tropical` Moderate ≈ 74 (a uniform tropical value), with Severe taking
  species/breed-specific values (e.g. `cattle_tropical` Severe 82 = beef row).
  **Do not treat an individual Moderate/Severe value as an error just because it
  differs from that species' own Table 1 row** — confirm against the full Table 1
  and the Atlas selection rationale first.

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

## Not errors (clarified)

- `goats_highland` Moderate = **72** (vs the goats-specific Table 1 row = 70) is **not
  a slip** — 72 is the general Fuquay (1981) / cattle-dairy Moderate value, and the
  Atlas uses ~72 as the standard `_highland` Moderate across species. Likewise the
  uniform `_tropical` Moderate = 74. These come from the curated Moderate/Severe scheme
  above, not from each species' own Table 1 row. Left unchanged.

## To review (not yet changed)

- Only the **Extreme** column is verifiable 1:1 (against Table 2) and is now fully
  consistent post the poultry fix. The exact per-cell source of each Moderate/Severe
  value is a curated mix and is not individually reconstructed here — if definitive
  per-value provenance is needed, reconcile against the full Table 1 + whoever set the
  Atlas thresholds.
- `haz_metadata.csv` THI_mean `method.description` still says Tdb is "the maximum
  temperature" (copied from THI_max) — cosmetic, but should read "mean temperature".

## Consumption note

`R/2.2_haz_change.R` currently processes only `cattle_highland` / `cattle_tropical`
for THI; the goats / sheep / pigs / poultry thresholds exist in the csv but are not
yet used downstream.
