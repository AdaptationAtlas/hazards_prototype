# HANDOVER — season-to-year convention for KNBS crop production

**For:** the KE-ENSO notebook session (antigravity), `atlas_nb-KE-enso`.
**From:** hazards_prototype / macbook, 2026-09-17.
**Decision:** Pete, 2026-09-17 — **one national rule**, adopted.
**Code + evidence:** `scripts/2026-09-17_maize_season_year_mapping.R`, panel and parser beside it.
**Issue:** [#34](https://github.com/AdaptationAtlas/hazards_prototype/issues/34).

## The rule

```
KNBS production year Y   ←   OND(Y-1) + MAM(Y)
```

So **2024 maize = short rains OND 2023 + long rains MAM 2024**. Harvest-aligned: it groups the
seasons whose crops are *harvested* during year Y.

Apply it uniformly. It is right for the counties that carry the tonnage and knowingly wrong for four
eastern counties — see **Where the rule is wrong** below.

## Why — and why it wasn't just assumed

KNBS does not publish the rule, and its own National Agriculture Production Report uses **both**
attributions in different places:

- **Harvest-year**, explicitly (cotton): *"the data used is from the crop grown in
  October-November-December 2022 and harvested in May-June 2023"* — counted as 2023.
- **Planting-year**, in the food-crops narrative: the 2023 result credited to *"heavy and
  well-spread short rains"*, i.e. OND **2023**.

The maize table states no season composition at all. So it was measured: 47 counties × 2019-2024
(282 county-years, parsed from Annex 1 of two editions, national totals reconciling exactly to Table
3.2), against county season rainfall from the published `admin-periods` parquet. Outcome is yield
(t/ha) as a within-county z-score — yield because area follows prices and the fertiliser subsidy,
z-scored because the question is "was this a good year *here*".

| Group | Best mapping | r | Same-year OND |
|---|---|---|---|
| Long-rains dominant (12 counties) | MAM(Y) + OND(Y-1) | 0.327 | **-0.007** |
| **Top-12 maize counties — 65% of national production** | **OND(Y-1)** | **0.221** (p=0.06) | **-0.020** |
| Mixed (31) | MAM(Y) + OND(Y) | 0.292 | 0.227 |
| Short-rains dominant (4) | OND(Y) | 0.398 (p=0.05) | — (MAM -0.052) |

Where the maize actually is, same-year OND has **no** relationship to yield; previous-year OND has
the strongest single-season one. That matches the agronomy — the unimodal crop is planted MAM(Y) and
harvested Oct-Dec(Y), so OND(Y) rain falls *during and after* harvest, while OND(Y-1) supplies
pre-season moisture and the small short-rains crop harvested early in Y.

## Where the rule is wrong, and what to say about it

**Tana River, Meru, Kitui, Makueni** are short-rains dominant: for them OND(**Y**) is what matters
(r = 0.40) and MAM is nil (r = -0.05). The single national rule misattributes their season. That is
an accepted simplification, not an oversight.

If the notebook ever shows a **per-county** production-vs-climate view for those four, either say
the mapping does not hold there or use OND(Y) for them. For national or maize-basket framing, the
rule is fine as-is.

## How confident to sound

**Moderate. Do not present this as established fact.** Suggested phrasing for any methods note:

> Production years are aligned to the seasons harvested within them: OND of the previous year plus
> MAM of the reporting year. KNBS does not publish its own convention; this alignment was selected
> by testing county maize yields against candidate season mappings (2019-2024).

What would make that overclaiming: calling it "the KNBS convention", or implying KNBS confirmed it.
They have not been asked.

The honest weaknesses, if anyone challenges it:

- six years only, correlations r ≈ 0.2-0.4;
- 2023 carries a fertiliser subsidy *and* a 15% area expansion alongside strong rains;
- the **national** six-point series actually favours same-year OND (r = 0.66), contradicting the
  county evidence — it is driven by 2023 alone, which is why the county evidence was preferred;
- rainfall is county-mean, not cropland-masked;
- 2024 figures are provisional.

## What this does and does not affect

- **Does not touch any published product.** No parquet, COG or S3 object changes. This is a
  convention for *joining* production statistics to climate seasons.
- **Does** apply to any notebook panel that puts a KNBS production year next to a season — ENSO
  composites, "good year / bad year" framing, yield-vs-rainfall charts.
- Unrelated to the population work in `HANDOVER_2026-09-17_ke-enso-population-schema.md`, which is a
  separate convention with its own metadata.

## If you want it firmer

Two cheap upgrades, in priority order: swap county-mean PTOT for **cropland-masked WRSI** (already
published, tier 8), and extend the panel to 10-15 years from older Economic Survey editions. If the
ranking survives n≥10 with cropland weighting, this graduates from "adopted convention" to
"measured". Ask via Pete — the pipeline side can run it.
