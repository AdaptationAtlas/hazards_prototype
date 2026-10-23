# HSH human heat-stress thresholds — provenance

Source documentation for the `HSH_max` / `HSH_mean` rows in [`haz_classes.csv`](haz_classes.csv)
(generic, levels Moderate > 27 °C, Severe > 32 °C, Extreme > 41 °C). Written 2026-10-23 while
scoping issue #14. Decision that day (Pete): **keep the values, document them, and record the
open issues here.**

## Source

The rows entered the repo in `2c85ef0` (2024-01-18, "metadata files uploaded") with no citation,
and no document or script in this repo or `hazards_upstream` states one. The attribution below is
therefore **inferred from the values**, not read from a record:

| class | `haz_classes.csv` (°C) | in °F | NWS Heat Index category (older chart) |
|---|---|---|---|
| Moderate | > 27 | 80.6 | Caution, 80–90 °F |
| Severe | > 32 | 89.6 | Extreme Caution, 90–105 °F |
| Extreme | > 41 | 105.8 | Danger, 105–130 °F |

All three match the US National Weather Service Heat Index risk bands, converted to °C and rounded
to whole degrees, in the **older** boundary set (80 / 90 / 105 / 130 °F) that many NWS offices still
publish (for example weather.gov/dmx/dssheat). The **current** NWS chart moves two boundaries:
Danger starts at **103 °F (39.4 °C)** and Extreme Danger at **125 °F (51.7 °C)**
(weather.gov/media/unr/heatindex.pdf). Caution and Extreme Caution are unchanged.

The Atlas keeps three classes, so the NWS fourth band (Extreme Danger) has no row.

## What the classes are applied to

- `HSH_max` = monthly **maximum** of the daily heat index; `HSH_mean` = monthly **mean** of it.
  R/1 then takes the per-season stat named in `haz_metadata.csv` (`max` / `mean`).
- So "Moderate" on `HSH_max` means *the hottest day of the month reached NWS Caution*; on
  `HSH_mean` it means *the typical day did*. Same numbers, different severity in practice.
- The NWS bands describe an instantaneous heat index for a person in shade and light wind. Applying
  them to daily-maximum values is the standard use; applying them to a monthly mean is an Atlas
  convention with no NWS backing.

## Issues (open, recorded 2026-10-23)

1. **The live data is not an NWS heat index, so the classes do not fit it today.** The published
   HSH (`hazards_upstream/R/04_indices/calc_HSH.R` as run in 2025) is the Rothfusz regression on
   **daily-mean** temperature, with no Steadman fallback and no low/high-RH adjustments. That is what
   `haz_metadata.csv` describes and it is correct for the live files. The fix on #14
   (`f81e6d7` / `d8e2bc0`) computes the full NWS algorithm on Tmax with RH at Tmax. Measured on one
   GCM-year (2026-10-07): HSH_max moves **+7.2 °C** median, HSH_mean **+6.1 °C**, 99.6–99.9 % of
   pixels upward. After #14 the classes fit the index they were taken from; before it they do not.
2. **Exceedance will jump when #14 ships, and that is expected.** Live monthly HSH_max medians are
   21–28 °C; corrected ones 27–35 °C. Roughly half of African pixel-months will sit at or above
   Moderate. That is the NWS Caution threshold applied to the month's hottest day in Africa. Do not
   raise the thresholds to make Moderate rare again — that would fit the classes to the old, wrong
   index.
3. **Older vs current chart.** The Extreme row is 105 °F (41 °C); the current NWS chart says 103 °F
   (39 °C after rounding). Kept at 41 on 2026-10-23: a chart revision, not a science change, and
   no published product reads these classes (issue 5).
4. **`HSH_mean` reuses the `HSH_max` bands.** No source for that; see "What the classes are applied
   to". Left as is.
5. **Nothing published consumes these rows today.** HSH is in no interaction set (`crop_interactions`
   uses NTx35/NTxM, `animal_interactions` uses THI_max), so the hazard-exposure keys carry no HSH.
   The one published NEX-GDDP HSH product is `HSH-max` inside the R/2.1 `timeseries_mean_month`
   parquets, which carry values and anomalies, not classes. R/2 §1 does classify HSH from the node's
   seasonal stacks, but those outputs stay local. The rows exist for documentation
   (`published_thresholds.md`) and for any future HSH tier.
6. **The node's seasonal HSH/THI stacks predate R/1's drop.** `R/1_make_timeseries.R:297-298` skips
   HSH and THI on the gddp tree, yet `SEASONAL/<period>/` holds `HSH_*_max_max` / `_mean_mean`
   stacks (verified 2026-10-22). The current R/1 cannot regenerate them. After #14 those HSH stacks
   are stale by method. Harmless until someone re-runs R/2 and trusts them.
7. **Values beyond the chart.** The Rothfusz regression is fitted for roughly 80–110 °F and
   RH ≤ ~85 %; the corrected index extrapolates past that in the hottest, most humid pixel-months
   (highest African value in the 2026-10-07 probe: 76 °C). The classes top out at 41 °C, so this
   affects magnitudes in `timeseries_mean_month`, not classification.
8. **RH at Tmax is approximated.** `rhx_from_daily()` assumes vapour pressure is conserved through
   the day (FAO-56). That degrades in arid, irrigated, coastal and monsoon zones. Documented in
   `heat_index.R`; it is an input caveat for every HSH value, not a threshold issue.

## Cross-references

- Index definition and method text: `haz_metadata.csv` (`HSH_max`, `HSH_mean`), to be updated in the
  commit that ships #14, not before.
- Generated table: `published_thresholds.md` / `.csv` via `scripts/generate_threshold_table.R`.
- Sibling provenance: `THI_thresholds_provenance.md`.
- Issue thread: AdaptationAtlas/hazards_prototype#14.
