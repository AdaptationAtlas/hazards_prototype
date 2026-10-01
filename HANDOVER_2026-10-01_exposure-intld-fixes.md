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

## Item 3 — method discussion: where should nominal prices come from?

Today 0.4.2 prices SPAM tonnage with FAOSTAT **producer prices (USD/t)**, and 76 % of country × crop prices
are gap-fills (continent 452, neighbours 514, region 394, own 345 of 1,705 on 2026-09-28). `R/price_fill.R`
clips own prices to [1/5, 5]× the world median per crop-year and fills with medians, which removed the
Zimbabwe/Rwanda artefacts, but the product's quality is still dominated by the fill.

Proposal to discuss before the item-2 bake: derive the nominal price as **FAOSTAT nominal gross production
value ÷ FAOSTAT production** per country × crop × year (an implied farm-gate price), falling back to the
producer-price chain only where GPV is missing. The nominal GPV file (`Value_of_Production_E_Africa.csv`,
"Gross Production Value (current thousand US$)") is already loaded in 0.4.2 §1.6 and never used; its coverage is
far better than the price file's, and it mirrors how the intld chain is anchored, so the two bases would differ
only by the deflator/PPP factor rather than by data source. Keep the band clip as a guard on the implied
price. Value-changing for the nominal product → its own GO, and the cross-basis gate's band could then tighten.

Open questions for Pete: (a) accept implied prices as the primary source; (b) coverage-guard threshold and
NA-vs-fallback for #39; (c) whether Sudan is in the SPAM SSA release's scope at all.

## Reuse

`R/price_fill.R`, `R/checks/vop_cross_basis_gate.R`, `R/checks/probe_042_price_fill.R`,
`R/checks/r3_tier_drift_vs_live.R` (`tier_drift(exposure_new=)`), `scripts/r3_publish_tiers.R`
(`--reference-only`, `--family-only`, `--drift-exposure`, `--drift-allow-flips`), fixtures in the macbook
scratchpad (`drift_fixture.R`, `price_fill_fixture.R`). Node gotchas: AGENTS.md §2.
