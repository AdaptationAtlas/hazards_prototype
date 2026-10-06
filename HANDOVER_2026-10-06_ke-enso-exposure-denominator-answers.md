# HANDOVER — KE-ENSO Explorer: boundaries, re-levelling and the county census table

**For:** the KE-ENSO notebook session, `atlas_nb-KE-enso`.
**From:** hazards_prototype / macbook, 2026-10-06. Producer-side answer to the Defect 6 questions.
**Prior art — read this first:** `HANDOVER_2026-09-17_ke-enso-population-schema.md` already answers
most of what you asked. Issues
[#28](https://github.com/AdaptationAtlas/hazards_prototype/issues/28) (closed, delivered 2026-09-17),
[#32](https://github.com/AdaptationAtlas/hazards_prototype/issues/32) (county grid-vs-census
disagreement, open), [#33](https://github.com/AdaptationAtlas/hazards_prototype/issues/33) (licence).

## Headline

Your diagnosis is right in substance, but the premise is out of date: **the re-levelling you are
asking for shipped on 2026-09-17.** The files in `data/KE-enso-explorer/` are dated **9 September**
and are pre-#28 — they predate the fix by eight days. Nothing needs to be run. You need to re-pull
tier 16 from S3.

Two defects in the live tables turned up while verifying this; both are ours, both are described
below, and one is a question for Pete before you write any headline number into copy.

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

### Two live defects found while verifying — please read before you consume

**(a) Stray `i.pop_source` column.** `exposure_jrc_rp.parquet` and `exposure_totals.parquet` each
carry an extra `i.pop_source` column alongside the real `pop_source`. It is a `data.table` join
artefact from the re-level step and should not be there. `exposure_gfm_seasonal.parquet` is clean.
Harmless if you select columns explicitly; it will surprise a schema-strict loader or a `SELECT *`
into a typed frame. Ours to fix — do not work around it permanently.

**(b) `pop_method = "county-level"` on projection-sourced rows — a question for Pete.** All three
live tables report `pop_method` as `county-level` (`county-level-yearmatched` in GFM). The pipeline
default is `county-growth`; the publish used an explicit override. The difference matters twice:

- *Numerically.* `county-growth` anchors on the census and uses projections only as a dimensionless
  ratio; `county-level` makes the published county total **equal** KNBS's published projection. For
  2025 that is 51.96 M versus 53.33 M — a ~1.4 M gap, the census-night-to-2020-base step.
- *Legally.* KNBS Volume XVI carries "(c) 2022 KNBS. All rights reserved" on p3, and we record it as
  `LicenseRef-KNBS-All-Rights-Reserved`. Under `county-growth` the projections never appear as a
  level, only as a ratio — derived analysis. Under `county-level` the published county totals **are**
  the copyrighted projections. The 2019 census half is CC0-1.0 and unaffected either way.

**Practical instruction: do not hard-code 52,837,534 or any county total from the live table into
notebook copy until Pete rules on this.** Drive totals off the census lookup in section 3, which is
CC0 and stable, and off `pop_pct` for every share. If the method changes it is a seconds-long
re-level, not a rebake, so the schema will not move — only the values and the `pop_source` /
`pop_method` strings.

---

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
| 1 | Re-pull tier 16 from S3; discard the 9 Sep copies | notebook |
| 2 | Relabel Table 1.1 / Section 1 as IEBC parliamentary constituencies, with footnote | notebook |
| 3 | Consume `population_knbs_census_adm1.parquet` for the Section 1 KPI cards; do not compile your own | notebook |
| 4 | Read `pop_source` / `pop_year` per row in the GFM table; never per file | notebook |
| 5 | Hold off hard-coding national/county totals pending 2(b) | notebook |
| 6 | Drop the stray `i.pop_source` column from JRC + totals; republish tier 16 | pipeline |
| 7 | Decide `county-growth` vs `county-level` for the published default | Pete |
| 8 | Check whether [2025] KEHC 3212 was appealed or stayed before any public copy on #32 | Pete |
