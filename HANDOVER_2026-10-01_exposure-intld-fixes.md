# Topic briefing — the intld side of the exposure chain: #38, #39, #40 and the price method

For the session that picks up items 2 and 3 of `HANDOVER_2026-10-01.md`. Not an entry point; read the
session handover first. Everything here was established 2026-09-26 → 10-01 while closing the #30
follow-ups (`DISPATCH_cglabs_r3_res25_rerun.md`).

## What is wrong, where, and how we know

The published exposure reference (`crop-livestock_all_res-05` / `_res-25`) carries two value-of-production
bases. The **nominal-usd-2021** rows (0.4.2: SPAM tonnage × FAOSTAT producer price) were corrected on
2026-09-28 and gate clean. The **intld15-2021** rows (0.4.0: national FAOSTAT gross production value
distributed over SPAM pixels by production *share*) are wrong wherever that distribution has nothing
sensible to distribute onto:

| # | defect | evidence | mechanism |
|---|---|---|---|
| 38 | pearl-millet inflated, small-millet absent (KEN 27 M I$ vs 6 kUSD nominal; ETH 310 M) | cross-basis ratio ~1/4000 | `R/0.4.0_create_crop_vop_intld15.R:146` merges GPV on `name_fao_val`; only `pmil` maps to FAOSTAT "Millet", `smil` carries a placeholder string. Coffee has the share-split (L172-180); millet does not. |
| 39 | Sudan all crops, Nigeria banana: intld ~400-5,000× the nominal value | SPAM 2020 SSA holds 0.05 Mt for all of Sudan (FAOSTAT ~15 Mt); NGA banana 1.5 kt vs plantain 6.5 Mt; implied nominal prices ordinary | national GPV lands on a sliver of SPAM cells; every admin unit holding one inherits the whole country's value. NGA plantain has **no** intld row (FAOSTAT "Plantains and cooking bananas" absent from the GPV file). |
| 40 | Seychelles: production present, both vop bases NaN on res-25 (live hazard product lost SYC coconut 1.34 M → 0) | `rasterize(touches = FALSE)` at 0.25°: no cell centre inside SYC | country value rasters in 0.4.0 **and** 0.4.2 (`terra::rasterize(geoboundaries, base_rast, field=)`); production is sum-resampled from 0.05° so it exists where the price does not. COM/CPV/MUS/STP fine. |

The gate that sees all three is `R/checks/vop_cross_basis_gate.R` (nominal ÷ intld per iso3 × crop at admin0,
`tech = all`; implied nominal price = nominal ÷ SPAM tonnage vs the world price per crop from 0.4.2's fill-source
CSV, `mapspam_pro_dir/fao_prices/crop_price_nominal-usd-2021-t_fill-sources_<tag>.csv`; a pair with a sound
nominal side and an out-of-band ratio is an **intld-side residual**). Today it PASSES both resolutions with the
residual = SDN ×13, NGA banana, pearl-millet ×6 (+ MOZ tea, NER maize at the band edge). The target state is
`--fail-on-intld-side` green.

No national-total gate (`vop_align_live_gate.R`, `qaqc_vop_vs_faostat.R`) can see any of this: the totals are
right, their placement is not.

## Item 2 — one 0.4.x correction pass (value-changing, needs Pete's GO)

Code, in `R/0.4.0_create_crop_vop_intld15.R` unless said otherwise:

1. **Compound FAO items split by SPAM production share**, generalising the coffee block (L172-180) to every
   `metadata/SpamCodes.csv` row with `compound = yes` whose FAO item covers several SPAM crops — millet (`pmil`
   + `smil` ← "Millet"), and check banana/plantain (FAOSTAT "Bananas" + "Plantains and cooking bananas" vs SPAM
   `bana` + `plnt`: today "Bananas" GPV sits on `bana` alone and plantain has no GPV row — decide whether to
   pool and split or to add the plantain GPV item). Fix the mapping table (`metadata/SPAM2010_FAO_crops.csv:91`
   placeholder) or the join, not both.
2. **Coverage guard.** Per (country, crop) compare SPAM national tonnage with FAOSTAT production; where SPAM
   covers less than a stated fraction (start at 10 %), do not distribute the GPV onto the footprint — leave the
   pair NA with a logged reason, or fall back to the nominal chain's tonnage-anchored value × an explicit
   PPP/deflator factor. Log every guarded pair. Sudan: confirm with the SPAM team whether it is in scope of the
   SSA release at all; if not, SDN intld rows should be absent, not inflated.
3. **`touches = TRUE`** on every `rasterize()` of a country table in 0.4.0 **and** 0.4.2 (price rasters). Border
   cells then carry every touching country's value; harmless because production already sits in one country's
   cells. Alternative, more exact: assign the nearest cell for polygons without a centre.
4. Then on the node: 0.4.0 and 0.4.2 at **both** resolutions (`EXPOSURE_RES=0.25|0.05`, run via
   `Rscript -e 'source(setup); source(script)'`; both always write), park (`mv`, never `rm`) the affected
   §1/§2 per-tif caches and §3 outputs **outside** `mapspam_pro_dir`, 0.4.4 at both resolutions (no FORCE),
   then gates: `vop_cross_basis_gate.R --fail-on-intld-side` at both res; `qaqc_vop_vs_faostat.R` (national
   totals must still reconcile — the fix moves value within countries, not between); old-vs-new per pair for
   the intld rows (pearl-millet, SDN, NGA banana, SYC are the expected movers; everything else within a few
   per cent); publisher dry-runs (`--reference-only`, `--family-only`, both res, rows identical except the
   plantain/small-millet rows that gain values). GO → publish reference + family. The usd hazard tiers do
   **not** change except SYC coconut (rides the next full R/3 bake with #13); the intld hazard tiers can then
   be considered for publication (`R/NEXT_FULL_REBAKE.md` item 7).
5. Update the three CDH records (drop the #38/#39/#40 paragraphs, add "changed against the previous
   publication"), close #38/#39/#40.

Budget: code half a day; node run under an hour per resolution for 0.4.x + 0.4.4; dispatch round trips.

## Item 3 — nominal price source: evidence (deep dive 2026-10-01, `R/checks/probe_price_method_deepdive.R`)

Question: price SPAM tonnage with FAOSTAT **producer prices** (USD/t, today) or with the **implied price** =
FAOSTAT gross production value (current thousand US$) ÷ FAOSTAT production, per country × item × year?
Measured on the FAOSTAT bulk files of 2026-05-14 (the ones the node uses), 31 SPAM items, 54 African
countries, window 2019-2023, world-band clip 5×.

**What the implied price is.** Where a producer price exists, GPV current US$ ÷ production **equals it
exactly** (973 country-item-years, ratio 1.000, IQR [1.000, 1.000]). FAO's own note: "Value of gross
production has been compiled by multiplying gross production in physical terms by output prices at farm
gate", converted "using official exchange rates as prevailing in the respective years" (QV methodology).
So the implied price is not an independent measurement; its only extra content is **FAO's imputation of
prices it does not publish** (PP methodology §1.6: related-commodity prices, the country's producer price
index, ARIMAX on the series), which flows into GPV but not into the price file.

| | producer price | implied (GPV ÷ production) |
|---|---|---|
| (iso3, item) pairs with an own value, of 868 with production | 262 (30 %), 41 % of production | 591 (68 %), 83 % of production |
| … surviving the 5× world-band clip | 254, 41 % of production | 523, **81 %** of production |
| pairs left to the neighbour/region/continent fill chain | 614 | **344** |
| own values clipped away as artefacts | 47 of 991 (4.7 %) | 417 of 2,922 (14.3 %) |
| within-pair CV 2019-23 (pairs with ≥ 3 years) | 0.160 (n = 213) | 0.108 (n = 589) |
| nominal ÷ constant-I$ GPV per pair after clip: median, 5-95 %, beyond [1/5, 5] | 1.49, [0.57, 3.49], 1.2 % | 1.16, [0.29, 3.68], 2.7 % |

Countries with **no producer price at all** in the window but implied prices: AGO, CMR, COG, ETH, GNB,
GNQ, MWI, SDN, SYC, TZA, ERI, CAF, SLE, BWA. The pairs the implied method adds are the heavy ones:
NGA yams (60 Mt), NGA cassava (58 Mt), ETH maize (11.6 Mt), NGA oil palm fruit, AGO/TZA/MWI/CMR cassava,
ETH wheat and sorghum, GHA/CMR plantain. Today every one of those is a regional or continental mean.

**What it does not fix, and what it adds.** (a) The ZWE 2022 artefact is in both series identically
(67,170 USD/t; FAO used the official rate on a hyperinflating currency) - the clip stays essential.
(b) Current-US$ GPV carries **low-side** exchange-rate artefacts the price file does not: Sudan (wheat
56 → 1.6 USD/t over 2019-23, 78 % of its implied values clipped), GNB, SYC, ERI, MDG, ZWE, COG, GIN, CPV,
AGO - hence 14.3 % clipped vs 4.7 %. Two-sided clip, which `R/price_fill.R` already is. (c) The clip
reference must match the method: the world producer-price median and the World GPV ÷ production unit
value differ by item (plantain 0.20×, yams 0.25×, cowpea 0.27×, tea leaves 5.2× - leaf vs made-tea
units), so implied values are clipped against World GPV ÷ production per item-year, producer prices
against the world producer-price median, never crosswise. (d) The low 5 % tail of nominal ÷ intld (0.29)
is largely real: in low-income countries nominal USD sits below constant international dollars (price
level ratio), e.g. NGA cassava 74 USD/t, NGA oil palm 45 USD/t survive a 5× clip legitimately - the
cross-basis band cannot tighten much either way. (e) NGA Bananas has production (4.9-7.4 Mt) but no
current-US$ GPV in any year (constant I$ exists), so #39's banana pair stays a fill under both methods.

**Recommendation (unchanged by the evidence, sharpened):** implied price as the **primary own value**
(GPV current US$ ÷ production, 2019-23 median, clipped 5× against World GPV ÷ production per item-year),
**producer price as first fallback** (clipped against the world producer-price median), then the existing
median neighbour → region → continent → world chain, with `price_source` recording which level was used
(`fao gpv implied` / `fao producer price` / fills). Expected effect: fills fall from 71 % to 40 % of pairs
and from ~59 % to ~19 % of production; the heavy pairs above move from regional means to FAO's own
country figures. Value-changing for the nominal product → its own GO, in the item-2 pass; the publish
gates (G6 against the exposure twin, cross-basis with the independent world reference) are already in
place. Caveat to state in the CDH note: the "own" nominal price is FAO's valuation, including FAO's
imputations, converted at official exchange rates.

**Decision 2026-10-01 (Pete): implied primary, producer-price fallback, 5× clip - CONFIRMED.**

**What it does not fix: the price BASIS problem (Pete's question).** Where a producer price exists the
implied price equals it, so whatever basis FAO recorded is inherited - auction or export-parity prices for
some countries, farm-gate for others, and different product forms (coffee cherry vs green bean, tea leaf vs
made tea, seed cotton vs lint). Measured with the constant-I$ GPV as yardstick (it uses one international
price per item, so a wide cross-country spread of nominal ÷ intld inside one item means inconsistent basis,
not economics):

| item | countries | nominal ÷ intld, min → max | spread |
|---|---:|---|---:|
| Coffee, green | 16 | NGA 0.06 (133 USD/t) … BDI 0.13 (268, cherry) … ETH 0.37 … CIV 0.53 … **KEN 1.98 (4,269, auction green)** | 33× |
| Cocoa beans | 9 | NGA 0.09 … GHA 0.37 … CIV 1.23 … CMR 2.26 | 25× |
| Tea leaves / tobacco / seed cotton / coconuts / sugar cane | | 0.01-0.08 → 1.3-7.8 | 50-490× |
| Maize (control) | 25 | AGO 0.39 → RWA 1.83 | 4.7× |

Maize's spread is the genuine PPP range; coffee's is product form and market level. Neither method changes
this. **Proposed guard for the 0.4.x pass (item 2 scope, Pete to confirm):** a within-item cross-country
consistency check - for each (country, item), nominal ÷ intld against the item's cross-country median; a
country beyond a band (start at 4×) is a basis mismatch and falls back from its own price to the fill chain
(or, better for the known product-form items, to the item median factor × the country's constant-I$ value,
which puts it on a consistent basis), with `price_source = "basis fallback"` and a logged list. Cash crops
to expect on that list: coffee (BDI, NGA, KEN), cocoa (NGA, CMR), tea, tobacco, seed cotton, coconuts, sugar
cane. This is the per-pair cross-basis gate turned into a correction at source.

Open questions for Pete: (a) accept implied-primary / producer-fallback; (b) coverage-guard threshold
and NA-vs-fallback for #39; (c) whether Sudan is in the SPAM SSA release's scope at all.

## Reuse

`R/price_fill.R`, `R/checks/vop_cross_basis_gate.R`, `R/checks/probe_042_price_fill.R`,
`R/checks/r3_tier_drift_vs_live.R` (`tier_drift(exposure_new=)`), `scripts/r3_publish_tiers.R`
(`--reference-only`, `--family-only`, `--drift-exposure`, `--drift-allow-flips`), fixtures in the macbook
scratchpad (`drift_fixture.R`, `price_fill_fixture.R`). Node gotchas: AGENTS.md §2.
