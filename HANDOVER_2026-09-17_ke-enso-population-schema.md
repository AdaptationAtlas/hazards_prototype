# HANDOVER — KE-ENSO Explorer: population denominator change (issue #28)

**For:** the KE-ENSO notebook session (antigravity), `atlas_nb-KE-enso`.
**From:** hazards_prototype / macbook, 2026-09-17. Producer-side only — no notebook code has been
touched from that session, and none will be.
**Code:** `develop` @ `a96e5b6` (feature), `64f8aa8` (re-level fix), `468c01a` (ingest hardening).
Issue
[#28](https://github.com/AdaptationAtlas/hazards_prototype/issues/28), comment
[5691901491](https://github.com/AdaptationAtlas/hazards_prototype/issues/28#issuecomment-5691901491).

> **Status, updated 2026-09-17 after the cglabs run — the two halves differ, so read both lines.**
>
> - **The KNBS population tables ARE LIVE.** Tiers 17 and 18 published and size-verified against S3
>   (4/4 and 3/3 objects). Everything under **Also newly published** below is queryable right now.
> - **The exposure intersect tables are NOT yet re-levelled.** Tier 16 still carries the old schema
>   and the old numbers (`pop_source = "worldpop"`, national `pop_total` 55,119,798). The re-level
>   hit a bug on the producer side — the published A/B tables never carried a `pop_total` column for
>   the script to work from — which is fixed (`64f8aa8`) and waiting on one short cglabs run.
>
> So: the new population data is available to build against today; the *exposure* population columns
> change shortly. Please don't hardcode against either shape — the change is additive apart from the
> `pop_source` values, so a notebook written against the list below works before and after. See
> **How to write it once** at the end.

## Why it is changing

The exposure stack carries two gridded population surfaces — WorldPop constrained (~55.2 M) and
GRID3/WOPR (~55.9 M) — and both run about **17 % above Kenya's enumerated 2019 census**
(47,564,296). Every absolute headcount the Explorer currently shows is therefore ~17 % above what a
Kenyan counterpart checks it against, and nothing in the published tables explains the gap.

The fix keeps the 100 m spatial detail and replaces only the level: the grid supplies the **share**
of a county's people living in each sub-county, KNBS supplies the **level**, and (for a projection
year) the county's **change over time**.

```
pop_adm2 = pop_adm2_grid × pop_scale_census × pop_growth_county
           100 m share      census / grid      county % change over time
```

County is the only level where this is defensible: COD-AB adm2 is 290 IEBC *constituencies*, the
census reports 345 KNBS *sub-counties*, only 183 names match, and KNBS does not project below county
at all.

## Schema delta — `domain=exposure/type=intersect/region=kenya/processing=analysis-ready/`

Applies to `exposure_gfm_seasonal.parquet`, `exposure_jrc_rp.parquet`, `exposure_totals.parquet`.

| Column | Status | Note |
|---|---|---|
| `pop_pct` | **unchanged, bit-for-bit** | Every share, ranking, choropleth and "% of sub-county exposed" is unaffected. The factors cancel. |
| `pop_exposed` | same column, **new values** | Drops ~14.5 % (×0.855) under the census default. |
| `pop_total` | same column, **new values** | National now exactly 47,564,296. |
| `pop_source` | **new values** | Was always `"worldpop"`. Now `"knbs-census-2019"` or `"knbs-projection-<year>"`. |
| `pop_method` | new | `"county-level"` or `"county-growth-from-2020"` — how the time factor was defined. |
| `pop_exposed_grid` | new | The old raw gridded pixel sum, i.e. today's `pop_exposed`. |
| `pop_total_grid` | new | The old raw gridded denominator. |
| `pop_scale_census` | new | Census county total / gridded county total (~0.855; constant in time). |
| `pop_growth_county` | new | County proportional change (1.0 for the census; 1.031-1.151 across counties for 2025). |
| `pop_grid_source` | new | `"worldpop-constrained-2020"` — which surface supplied the share. |

Identity that always holds: `pop_total = pop_total_grid × pop_scale_census × pop_growth_county`, and
`pop_scale_adm1` is that product. Nothing is discarded — the pre-change numbers remain available
under the `*_grid` names if you want a comparison view or a "raw gridded" toggle.

## What to put in the UI

Read `pop_source` and `pop_method` off the table rather than assuming. For the shipped default,
something like **"People exposed — 2019 census counts, distributed by WorldPop 100 m"**. Please do
not label these as WorldPop counts any more; the level is KNBS.

## Two things that are NOT available — please don't design for them

1. **No sub-county census join.** A KNBS sub-county table *is* published (tier 17, `level=adm2`,
   345 rows) but it lives in KNBS's own sub-county universe, is name-keyed, and carries
   `adm2_pcode_codab` only where a name matched exactly one COD-AB unit inside the same county
   (`codab_match` records how each row resolved). It must not be joined to `adm2_pcode` as a
   denominator.
2. **No sub-county projections.** For a projection year, sub-county shares are held fixed at the
   gridded 2020 distribution, so every sub-county in a county moves at exactly its county's rate.
   Sub-county-vs-sub-county growth differences *within* one county are an artefact of the method —
   don't chart them. *Between* counties the differences are real (2025 factors span 1.031-1.151).

## Also newly published — LIVE NOW, verified on S3 2026-09-17

Base: `https://digital-atlas.s3.amazonaws.com/`

```
domain=exposure/type=population/source=knbs-census-2019/region=kenya/processing=analysis-ready/
  level=adm0/population_knbs_census_adm0.parquet            national totals
  level=adm1/population_knbs_census_adm1.parquet            47 counties — counts, households, land area, density
  level=adm1/population_knbs_census_adm1_agesex.parquet     county × 5-yr age group × sex (22,770 rows)
  level=adm2/population_knbs_census_adm2_knbs.parquet       345 KNBS sub-counties (see caveat 1 above)

domain=exposure/type=population/source=knbs-projections-2020-2045/region=kenya/processing=analysis-ready/
  level=adm0/population_knbs_projections_adm0.parquet       national × year × age × sex
  level=adm1/population_knbs_projections_adm1.parquet       county × year × age × sex (45,684 rows)
  level=adm1/population_knbs_projections_adm1_totals.parquet county × year, All Ages (846 rows)
```

All keyed on `adm1_pcode` (IEBC COD-AB), so they join straight to `ken_adm1.geojson`. Projection
years are annual **2020-2035** plus **2040** and **2045** only — 2036-39 and 2041-44 are not
published, so a year slider must not assume a continuous range.

Metadata records: `metadata/cdh/kenya-population-knbs-census.yaml`,
`kenya-population-knbs-projections.yaml`, and the method statement in
`kenya-flood-exposure-intersect.yaml`.

## Open decision that could move the headline numbers once more

Whether the default denominator stays the 2019 census or becomes a projection year. Pete owns it.
2025 is either **51.96 M** (census-anchored growth) or **53.33 M** (KNBS's published projection) —
they differ because the second imports the ~2.6 % step KNBS puts between census night (Aug 2019) and
its own 2020 base.

Switching is a seconds-long re-level, not a rebake. **The schema above is stable either way**; only
the values and the `pop_source` / `pop_method` strings would move.

## How to write it once

- Drive every share, ratio and ranking off `pop_pct` — it does not move under any of these options.
- Format absolute headcounts from `pop_exposed` / `pop_total`, and caption them from `pop_source`
  (falling back to "WorldPop constrained 2020" if the column still reads `"worldpop"`, i.e. the
  republish has not happened yet).
- Treat `pop_*_grid`, `pop_scale_*`, `pop_growth_county`, `pop_method` and `pop_grid_source` as
  optional: present after the republish, absent before it.

Questions back to the pipeline side go through Pete rather than directly between sessions.
