# HANDOVER — KE-ENSO Explorer: boundaries, re-levelling and the county census table

**For:** the KE-ENSO notebook session, `atlas_nb-KE-enso`.
**From:** hazards_prototype / macbook, 2026-10-06, **updated 2026-10-07**. Producer-side answer to
the Defect 6 questions.
**Prior art — read this first:** `archive/dispatches/HANDOVER_2026-09-17_ke-enso-population-schema.md` already answers *(Archived 2026-10-07: its status block is superseded by the 2026-10-06 re-level; where the two differ, this file wins.)*
most of what you asked. Issues
[#28](https://github.com/AdaptationAtlas/hazards_prototype/issues/28) (closed, delivered 2026-09-17),
[#32](https://github.com/AdaptationAtlas/hazards_prototype/issues/32) (county grid-vs-census
disagreement, open), [#33](https://github.com/AdaptationAtlas/hazards_prototype/issues/33) (licence).

## Headline

Your diagnosis is right in substance, but the premise is out of date: **the re-levelling you are
asking for shipped on 2026-09-17.** Nothing needs to be run on your side or ours.

**One action is blocking, and it has not been done.** As of 2026-10-07 the files in
`data/KE-enso-explorer/` are still dated **9 September** — pre-#28 — and `notebook_v3.qmd` still
`FileAttachment`s them directly. So the Explorer is serving raw WorldPop levels (~55.1 M national
rather than 52.8 M) with none of the KNBS columns. **Defect 6 is still live in the published
Explorer.** Everything this document describes is data you are not yet reading. Section 2 has the
URLs and a vintage check.

Two defects in the live tables turned up while we verified this. **Both were ours and both are now
fixed, republished and verified on S3** (#42, #44) — they need nothing from you, and the earlier
instructions to hold off on totals and to hide `pop_method` are withdrawn.

---

## 1. Boundaries — keep the 290 IEBC constituencies, and label them as such

**Answer: keep 290. There is no migration to 345 planned, and this is a constraint rather than a
preference.**

- COD-AB adm2 for Kenya **is** the 290 IEBC parliamentary constituencies. The KNBS census reports
  **345 sub-county rows**. These are different universes, not different resolutions of the same one:
  the census set includes constituency splits (Embakasi E/W/C and similar) and **12 forest and
  national-park units** that are not constituencies at all.
- Normalised in-county name matching gets **183 of 290**. At county level the join is clean (47/47,
  only "Nairobi" vs "Nairobi City"). That asymmetry is why the whole method levels at adm1.
- This is independently corroborated, not just our finding: UNFPA's own caveat on the HDX
  `cod-ps-ken` release states that *"Administrative level 2 does not conform to the COD-AB."*
- **The binding blocker is geometry, not names.** KNBS publishes sub-county *names and counts*; it
  does not publish a 345-unit sub-county **boundary** dataset. There is no authoritative polygon set
  to zonal-aggregate against. Until one exists, the exposure engine physically cannot produce 345
  rows, however the output is labelled.

**So your proposed fix is the correct one.** Label Table 1.1 and Section 1 as **"IEBC parliamentary
constituencies (COD-AB adm2)"**, with the footnote you drafted. Marsabit's four units (Laisamis,
Moyale, North Horr, Saku) are correct and complete *for the electoral universe*; the seven KNBS
sub-counties you list are correct for the administrative universe. Both are right; they are not the
same thing, and the contradiction was purely a labelling error on the notebook side.

One consequence to design around: **do not offer a sub-county census population anywhere.** We do
publish a 345-row KNBS table (tier 17, `level=adm2`), but it is name-keyed, carries
`adm2_pcode_codab` only where a name resolved to exactly one COD-AB unit inside the same county, and
records how each row resolved in `codab_match`. It must not be joined to `adm2_pcode` as a
denominator.

---

## 2. Re-levelling — already done; re-pull rather than re-run

### What is actually live

Verified against S3 on 2026-10-06 by direct parquet read:

```
domain=exposure/type=intersect/region=kenya/processing=analysis-ready/
  exposure_totals.parquet        290 rows, 47 counties
  exposure_jrc_rp.parquet
  exposure_gfm_seasonal.parquet
```

`exposure_totals` now carries `pop_total`, `pop_total_grid`, `pop_scale_census`,
`pop_growth_county`, `pop_scale_adm1`, `pop_source`, `pop_year`, `pop_method`, `pop_grid_source`.
National `sum(pop_total)` = **52,837,534** against `sum(pop_total_grid)` = **55,119,798**.

`exposure_gfm_seasonal` is **year-matched**, as decided on 2026-09-17: `pop_source` and `pop_year`
vary by row across 2018-2025 (`knbs-census-2019` for 2018-19, `knbs-projection-<year>` from 2020).
**Read `pop_source` per row — do not sample one row and apply it to the file.** JRC and totals carry
a single reference year (currently 2026).

Marsabit on the live table, which answers your worked example directly:

| unit | `pop_total` | `pop_total_grid` | `pop_scale_census` | `pop_growth_county` |
|---|---|---|---|---|
| Laisamis | 119,248 | 82,385 | 1.2573 | 1.1512 |
| Moyale | 191,444 | 132,263 | 1.2573 | 1.1512 |
| North Horr | 134,950 | 93,233 | 1.2573 | 1.1512 |
| Saku | 83,666 | 57,803 | 1.2573 | 1.1512 |

`365,683 x 1.2573 = 459,774`, i.e. the KNBS 2019 county total you quote (459,785, to rounding),
recovered exactly. The levelling works; you are reading a file written before it was applied.

### What you must not write

- **`pop_pct` is bit-for-bit unchanged.** Every share, ranking and choropleth is unaffected by all of
  this. Drive everything you can off `pop_pct`.
- **The headcount change is not a flat percentage.** The per-county factor spans **0.324 to 1.394**.
  The *national denominator* moved by ~0.855, but exposure concentrates in low-factor counties, so
  measured on publish: GFM exposed **3,777,107 -> 2,451,666** (x0.649) and JRC **6,120,527 ->
  4,998,340** (x0.817). Do not write "counts are ~14% lower" anywhere.
- **Do not call these WorldPop counts any longer.** The level is KNBS; WorldPop supplies only the
  within-county share. Suggested label: *"People exposed — KNBS census level, distributed by WorldPop
  100 m"*.
- **Sub-county-vs-sub-county growth differences inside one county are an artefact.** Shares are held
  fixed at the gridded 2020 distribution because KNBS does not project below county. Between-county
  differences are real; within-county ones are not. Do not chart them.

### Two defects found while verifying — both now FIXED and verified live

*(Updated 2026-10-07. Both were ours, both are closed, and neither needs anything from you. Earlier
versions of this section told you to hold off on totals and not to display `pop_method`. **Both
instructions are withdrawn.**)*

**(a) Stray `i.pop_source` column — GONE.** `exposure_jrc_rp.parquet` and `exposure_totals.parquet`
carried a `data.table` join artefact alongside the real `pop_source`. Stripped and republished;
[#42](https://github.com/AdaptationAtlas/hazards_prototype/issues/42) closed. No table has any `i.`
column. You can `SELECT *` safely.

**(b) `pop_method` was mislabelled — CORRECTED.** The live tables used to report `county-level` while
the numbers were built with `county-growth`. The cglabs node proved it by value rather than by
label:

```
POP_METHOD=county-level   -> totals 52837534 -> 54226998  (x1.0263)
POP_METHOD=county-growth  -> totals 52837534 -> 52837534  (x1.0000)
```

`county-growth` reproduces every live value exactly — the census-anchored, licence-safe method.
The label now says so. Root cause was a variable-shadowing bug
([#44](https://github.com/AdaptationAtlas/hazards_prototype/issues/44)) that made the string
un-repairable by any re-level; fixed in the re-level script and the zonal engine.

**What this means for you:**

- **No licence problem with the exposure tables.** KNBS projections enter only as a dimensionless
  ratio; the 2019 census (CC0-1.0) supplies the level. (That question stays open only for
  `population_knbs_projections_*.parquet`, which you should not consume anyway — see section 3.)
- **Use the totals freely.** National `pop_total` is **52,837,534** and is not expected to move.
- **`pop_method` is now safe to display.** It reads `county-growth-from-2020`
  (`county-growth-from-2020-yearmatched` in GFM).
- A future move to `county-level` (county totals matching KNBS's published projections, 54,226,998
  nationally for 2026) is parked as low-priority
  [#43](https://github.com/AdaptationAtlas/hazards_prototype/issues/43). Seconds-long re-level if it
  ever happens; the schema does not move.

### ⚠ THE ONE BLOCKING ACTION — you are still serving pre-#28 data

Checked 2026-10-07: `data/KE-enso-explorer/exposure_*.parquet` in `atlas_nb-KE-enso` are all still
dated **9 September 2026**, and the notebook loads them directly:

```js
exp_gfm: FileAttachment("/data/KE-enso-explorer/exposure_gfm_seasonal.parquet"),
exp_jrc: FileAttachment("/data/KE-enso-explorer/exposure_jrc_rp.parquet"),
exp_tot: FileAttachment("/data/KE-enso-explorer/exposure_totals.parquet"),
```

Those files **predate the KNBS re-levelling entirely**. They carry raw WorldPop levels (~55.1 M
national, not 52.8 M) and have none of the `pop_source` / `pop_year` / `pop_method` /
`pop_scale_census` / `*_grid` columns. **This is the original Defect 6, still live in the
Explorer.** Everything above describes data you are not yet reading.

Re-pull all three:

```
https://digital-atlas.s3.amazonaws.com/domain=exposure/type=intersect/region=kenya/processing=analysis-ready/exposure_gfm_seasonal.parquet
https://digital-atlas.s3.amazonaws.com/domain=exposure/type=intersect/region=kenya/processing=analysis-ready/exposure_jrc_rp.parquet
https://digital-atlas.s3.amazonaws.com/domain=exposure/type=intersect/region=kenya/processing=analysis-ready/exposure_totals.parquet
```

Then confirm you have the right vintage — these are the live values as of 2026-10-07:

| check | expected |
|---|---|
| `exposure_totals` rows / counties | 290 / 47 |
| national `sum(pop_total)` | **52,837,534** |
| national `sum(pop_total_grid)` | 55,119,798 |
| columns with an `i.` prefix | **none**, in all three |
| `pop_method` (totals, jrc) | `county-growth-from-2020` |
| `pop_method` (gfm) | `county-growth-from-2020-yearmatched` |
| `pop_source` (totals, jrc) | `knbs-projection-2026`, `pop_year` 2026 |
| `pop_source` (gfm) | 7 distinct: census-2019 for 2018+2019, projection-2020…2025 |
| `ncol` (gfm / jrc / totals) | 26 / 23 / 18 |

If `pop_total_grid` is missing, you still have the old file.

## 3. County census lookup — yes to the structure, no to compiling your own

**Your proposed architecture is right and matches ours.** Official census statistics drive the
Section 1 KPI cards; the modelled exposure tables carry spatial hazard shares. Keep that split.

**But do not compile `knbs_county_census.parquet` yourself — we already publish it**, and have since
2026-09-17:

```
domain=exposure/type=population/source=knbs-census-2019/region=kenya/processing=analysis-ready/
  level=adm0/population_knbs_census_adm0.parquet          national totals
  level=adm1/population_knbs_census_adm1.parquet          47 counties: counts, households,
                                                          land area, density
  level=adm1/population_knbs_census_adm1_agesex.parquet   county x 5-yr age group x sex
  level=adm2/population_knbs_census_adm2_knbs.parquet     345 KNBS sub-counties (see section 1)
```

`population_knbs_census_adm1.parquet` covers **all four** of your KPI cards — county population, land
area, national population share, household count — keyed on `adm1_pcode` (IEBC COD-AB), so it joins
straight to `ken_adm1.geojson` and to the exposure tables. Licence is **CC0-1.0**, verified via the
KNBS organisation's own HDX release of the identical workbook (`license_id: other-pd-nr`). Clear to
use and to cite.

Two notes on it:

- **Source the census from us, not from the HDX mirror.** The HDX copy is defective: Tharaka-Nithi's
  *county* total (393,177) appears as a sub-county row, giving 346 rows and a national 47,957,473
  against the published 47,564,296. Our ingest reconciles exactly to 47,564,296.
- **Do not consume `population_knbs_projections_*.parquet`** for anything user-facing pending the
  licence question in section 2(b).

### Your land-area discrepancy is a boundary question, not a population one

76,028 km2 (your constituency sum; we read 76,029) against KNBS's 70,944 km2 for Marsabit is a
**geometry** difference between the COD-AB IEBC polygons and KNBS's published county areas.
`area_km2` in the exposure tables is the polygon area and **no amount of population re-levelling will
ever change it**. Take land area from `population_knbs_census_adm1.parquet` for the KPI card and
leave `area_km2` for internal density/share work only. Exactly as you proposed.

---

## 4. Before you write copy about Mandera, Wajir or Garissa — read this

Your percentages match ours. The live county ratios of census-level to gridded are Mandera 0.377
(2.65x overcount in WorldPop), Wajir 0.527 (1.90x), Garissa 0.622 (1.61x), against Marsabit 1.447
(WorldPop *undercounts* there). Those three counties carry roughly **46% of the national grid-versus-
census gap from about 5% of the population**; 29 of 47 counties agree within 15%.

**The Kenyan High Court quashed the 2019 census results for exactly those three counties.** *Sheikh
& 24 others v Kenya National Bureau of Statistics*, [2025] KEHC 3212 (KLR), 28 January 2025, which
found irregularities and ordered that the **2009** figures be used for constitutional purposes.
There is precedent: in 2009 the Planning Minister had already rejected returns from eight districts
(three in Mandera, Wajir East, Lagdera/Garissa, three in Turkana) and roughly 916,000 people were
cut from those three counties.

So "align to the official KNBS census" in Mandera, Wajir and Garissa means aligning to a
**judicially challenged** denominator, and our four largest data-derived outliers are precisely the
contested counties. This is a presentational and political call, **not a data fix**. It is escalated
to Pete under #32 and nobody should take a side in notebook copy. If you need to say anything there,
state the method and the uncertainty, and **check first whether the judgment has been appealed or
stayed** — our last check was 2026-09-17.

Note also that GRID3/WOPR is **not** a valid independent arbiter here: it is modelled from KNBS
microcensus data, so it shares lineage with the figures in dispute.

---

## 5. Corrections to two details in your write-up

- You refer to *"the `pop_total_grid` column in `exposure_totals.parquet`"*. In the file you are
  serving there is **no such column** — the pre-#28 schema has only `pop_total`, and that value *is*
  the raw gridded sum. The `*_grid` columns exist only from #28 onward. Your reading of the number
  was right; the column name belongs to the version you have not pulled yet.
- The WorldPop national figure is ~55.1-55.2 M on the constrained 2020 surface (we read 55,119,798
  summed over the 290 units). The stack also carries a GRID3/WOPR surface at ~55.9 M. Both run ~17%
  above the enumerated census, which is what motivated #28.

---

## Summary of actions

| | Action | Owner |
|---|---|---|
| 1 | **BLOCKING — re-pull tier 16 from S3; the 9 Sep copies are still live in the Explorer** | notebook |
| 2 | Relabel Table 1.1 / Section 1 as IEBC parliamentary constituencies, with footnote | notebook |
| 3 | Consume `population_knbs_census_adm1.parquet` for the Section 1 KPI cards; do not compile your own | notebook |
| 4 | Read `pop_source` / `pop_year` per row in the GFM table; never per file | notebook |
| 5 | Totals usable (national 52,837,534); `pop_method` now safe to display | notebook |
| 6 | ~~Drop the stray `i.pop_source` column; republish tier 16~~ **DONE 2026-10-06, verified on S3** | pipeline |
| 7 | ~~Correct the `pop_method` mislabel in the engine + 7b~~ **DONE 2026-10-07** (CDH record needed no change) | pipeline |
| 8 | Check whether [2025] KEHC 3212 was appealed or stayed before any public copy on #32 | Pete |
| 9 | Low-priority: whether the default ever moves to `county-level` (#43, gated on #33) | Pete |
