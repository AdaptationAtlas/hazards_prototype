# Method: nominal US$ prices for crop value of production (`vop_nominal-usd-2021`)

**Status:** decided 2026-10-01/02 (P. Steward), implemented in `R/0.4.2_create_crop_vop_nominal_usd.R` §3 and
`R/price_fill.R`, first publish pending (`DISPATCH_cglabs_exposure_intld_fixes.md` Block D). This is the
single methods text; the dataset records and the notebooks cite it (consumers listed at the end). Evidence
behind every step: `HANDOVER_2026-10-01_exposure-intld-fixes.md` (items 3 and "Evidence"), probes
`R/checks/probe_price_method_deepdive.R`, `R/checks/probe_price_stale_slc.R`, `R/checks/probe_042_price_fill.R`.

## What the product is

Crop value of production in **nominal 2021 US dollars** per pixel and per administrative unit:
MapSPAM 2020 (Adaptation Atlas SSA release) production in tonnes × a national price in USD per tonne for
the 2019-2023 window, per country × crop. The companion basis, constant 2014-16 international dollars
(`vop_intld15-2021`, `R/0.4.0`), distributes FAOSTAT's gross production value by production share and
needs no price; the two bases must not be summed or compared.

## The price chain, per (country, crop)

1. **Own implied price** = FAOSTAT gross production value in *current thousand US$* × 1000 ÷ FAOSTAT
   production (t), per year; median over 2019-2023.
   Why: where FAO publishes a producer price the implied price equals it exactly (973 country-item-years,
   ratio 1.000); where it does not, FAO's imputation (PP methodology §1.6: related-commodity prices, the
   producer price index, ARIMAX) still reaches the GPV. Coverage of own values rises from 30 % of
   (country, crop) pairs and 41 % of production (producer-price file) to 68 % / 83 %.
   Sources: FAOSTAT QV (Value of Production) and QCL (Production) bulk files, 2026-05-14 vintage;
   FAO QV methodology note (GPV "compiled by multiplying gross production in physical terms by output
   prices at farm gate", converted "using official exchange rates as prevailing in the respective years").
2. **Stale-local-price test (rejects step 1 for a pair).** FAO's GPV in *current standard local
   currency* per tonne, last ÷ first year of the window, divided by the country's GDP-deflator ratio
   (FAO Deflators bulk, "GDP Deflator", SLC 2015 prices). Below 0.5 the pair's implied prices are
   rejected for every year. Why: where FAO has no producer price it carries a frozen local price
   forward (Sudan millet 2,742 SDG/t = the 2013 price, in 2019, 2020 and 2021, while the deflator went
   ×37) and converts it at the current official rate; the USD figure then decays with the currency and is
   not a measurement. 27 of 564 pairs on the 2026-05-14 files (Sudan 8, Angola 7, Ghana 4, Sierra Leone
   4, Egypt, Ethiopia, Kenya, Lesotho 1 each). Not an exchange-rate-premium effect: an overvalued official
   rate inflates USD values, it cannot deflate them.
3. **World-band clip.** An own implied price outside [1/5, 5] × the *World* GPV ÷ *World* production unit
   value for the same item-year is dropped (842 of 7,994 observations; Zimbabwe 2022 at 200× world on
   seven crops is the canonical case). Producer prices are clipped against the world producer-price
   median for the same crop-year. Never crosswise: the two world references differ per item (plantain
   0.2×, yams 0.25×, tea leaves 5×).
3b. **Window clip.** The window median of a surviving own value is judged once more against the
   window's world reference (implied or producer, matching its source), same 5× band. The per-year clip
   keeps a year whose world price spiked, so a window median could still sit beyond 5× (GNB sorghum
   6.7×, ERI sesame 5.5×, RWA sugarcane 6.2×). 14 window values at y2021; the cleared rows go to the fill
   chain. Added 2026-10-04 after the node's Block B review.
4. **Own producer price** (FAOSTAT "Producer Price (USD/tonne)", window median, clipped) as first fallback.
5. **Longer series** (window start − 5 … window end) of steps 1 and 4.
6. **Spatial fills**, each a **median** over other countries' own prices: neighbours → region → continent
   → world. Medians since 2026-09-27, when a mean-based fill spread one Zimbabwe artefact to Zambia and
   Botswana and one Rwanda oil-palm price to 17 East African countries.
7. **Basis guard (within item), applied to own prices BEFORE step 6's fills** (since 2026-10-05: run after
   the fill, a rejected own price still fed its neighbours' medians, e.g. GIN plantain at 30.6 USD/t into
   GNB, KEN auction coffee into UGA and TZA). ratio = price × FAO production ÷ (FAO constant-I$ GPV × 1000), per
   country; item median over own-priced countries (≥ 5). A country beyond [1/4, 4] × the median carries
   a different price *basis* (auction green coffee vs cherry, tea leaf vs made tea, seed cotton vs lint,
   export parity vs farm gate) and takes the item-median factor × its own constant-I$ value. Because the
   constant-I$ GPV is production × one international price per item, that fallback is one consistent
   USD/t per item. 18 rows at y2021 on the 2026-05-14 files (after the stale test and the window clip):
   high-side KEN coffee (4,146 → 925), BDI tobacco (8,990 → 1,094), ERI lentil; low-side Guinea, Niger,
   Tunisia, Zambia soybean, Nigeria oil palm fruit (46 → 190) and sesame. Independent evidence puts 7 of the 8 material
   fallbacks inside the supportable farm-gate range (handover "Evidence" section).
8. **Evidence pins** (`metadata/price_pins.csv`, cited per row), applied last, only where the chain is
   shown wrong by independent evidence. Four today: Angola banana 300 USD/t (the item-median fallback,
   432, is a retail-level number; MINFIN retail 454-689, export unit value 500-585, World Bank 2021:
   farm gate ≈ FOB less transport); coffee, per tonne of green-bean equivalent, Ethiopia 2,900 (FAO's
   782 is a red-cherry price), Uganda 1,700 (UCDA farm-gate FAQ), Guinea 1,200 (official minimum; FAO
   535). Sources per row in the CSV; evidence tables in the handover ("Evidence — coffee").
9. Coffee and millet prices (one FAO item each) are applied to arabica + robusta and pearl + small millet;
   each SPAM crop is then multiplied by its own tonnage.
10. **Grid.** Price × production is computed on SPAM's native 0.05° grid, with each cell given the price of
   the country holding its centre (touches only for cells no centre claims: offshore-centre coast, small
   islands). The 0.25° product is the sum-resample of that raster. Multiplying on 0.25° would price both
   sides of a border at one country's price (Benin's 0.25° cells hold 4× its own cowpea: Nigerian
   production). Added 2026-10-05.

Every row carries `price_source` ∈ {fao gpv implied, fao producer price, … longer-series, neighbours /
region / continent / world median, basis fallback, evidence pin} and the audit CSV
(`fao_prices/crop_price_nominal-usd-2021-t_fill-sources_<res>.csv`) carries every candidate, the basis
ratio and the stale flag.

## Measured effect (2026-05-14 FAO files, y2021, before the node run)

| | previous method (producer prices, median fills) | this method |
|---|---|---|
| own (incl. basis fallback / pin) share of FAO production | 56 % | 80 % |
| rows on the spatial fill chain | 1,360 of 1,705 | 1,167 of 1,705 |
| all-crops continental nominal total | 1.00 | **0.83** |
| per-crop continental ratio, lowest | | cowpea 0.34, coffee 0.39, plantain 0.46, coconut 0.54 |
| per-crop, highest | | sugar beet 1.18, sugarcane 1.15 |

The large per-crop moves are FAO's own country valuations in the big producers (NGA cowpea 101 USD/t,
cassava 74; CMR/GHA/CIV plantain ~290) replacing fills that had carried a few high producer prices
across a region. Per-country factors are identical at both resolutions; only coastal cells (touches = TRUE)
differ.

## Caveats to state wherever the product is described

- The "own" nominal price is **FAO's valuation**, including FAO's imputations, converted at official
  exchange rates. Where a parallel market existed (Nigeria 2020-23, Angola 2019-21, Sudan throughout) the
  official rate overstates the USD value of local prices.
- Prices are **national**: one USD/t per country × crop, applied to every pixel.
- **Coffee tonnage for the Central African Republic and Guinea** follows FAOSTAT, which breaks from
  ~10 kt to ~300 kt (CAF) after 2017; ICO puts real output at 2-6 kt (CAF) and ~9 kt (GIN). Value for
  those two countries is overstated on both bases regardless of price.
- Nominal US$ and constant international dollars are different bases; cross-basis ratios of 0.3-3 are
  ordinary (price level × deflator), not errors.
- Known related material not used: `metadata/fao_deflators_farmgate.csv` (farm-gate shares of FOB /
  auction prices for coffee, sugarcane, cocoa, tea) is an alternative, item-specific treatment of the
  basis problem; the general guard above was preferred so that it covers every item.

## Consumers to update when this method is first published (Block E of the dispatch)

- `metadata/cdh/africa-exposure-combined-res25.yaml`, `-res05.yaml`: the nominal-usd-2021 description,
  "changed against the previous publication", drop the #38/#39/#40 paragraphs.
- `metadata/cdh/africa-hazard-exposure-nexgddp.yaml`: the exposure basis paragraph (the usd tiers move
  with the next R/3 re-bake, #13).
- `README.md` (science pipeline, VoP section) — point here.
- `atlas_nb-KE-enso/data/economicReturns/text/methods.en.md` ("Producer Prices" section still describes
  the pre-2026 producer-price mean method and links `fao_producer_prices.R`) and the ROI notebook's
  data note — ours, edit after Block D publishes; until then the live object is the old method.
- `atlas_notebooks` (relay only): notify, do not edit.
