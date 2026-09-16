# CDH metadata records — KE-ENSO Explorer datasets

15 CGIAR Climate Data Hub (CDH) **v0.3.0** metadata records, one per dataset the KE-ENSO Explorer uses.
Upgraded from v0.1.0 on 2026-09-14 after Brayden's review
([cdh-metadata-standard#32](https://github.com/CGIAR-Climate-Data-Hub/cdh-metadata-standard/issues/32)).
**The original 13 pass** the v0.3.0 schema + cross-field checks and `prettier --check` (what the catalog CI runs);
the two KNBS population records added on 2026-09-15 (issue #28) parse but have **not** been run through
the Node validator yet — do that before contributing them to cdh-catalog:

```sh
# one-off tooling checkout (Node >= 20)
git clone --depth 1 --branch v0.3.0 https://github.com/CGIAR-Climate-Data-Hub/cdh-metadata-standard.git /tmp/cdh-std
(cd /tmp/cdh-std && npm ci)

# validate (zsh: use an array — an unquoted $VAR list does not word-split)
files=(metadata/cdh/*.yaml); files=(${files:#*ensemble_season_trends*})
node /tmp/cdh-std/scripts/validate-yaml.js --profile /tmp/cdh-std/spec/schemas/profiles/cdh.schema.json $files
npx prettier@3 --check $files      # catalog CI formats with prettier defaults (no .prettierrc)
```

| record | dataset | cdh.domain | licence | S3 prefix |
|---|---|---|---|---|
| africa-precipitation-monthly-seasonal | CHIRPS-derived PTOT monthly + seasonal (was `africa-precipitation-chirps`) | climate | CC-BY-4.0 | `domain=climate/type=observational/source=chirps-chirts-era5/…/variable=PTOT` |
| africa-spei-drought | SPEI-03 / SPEI-12 | climate | CC-BY-4.0 | `…/variable=SPEI-03\|SPEI-12` |
| eastafrica-ndvi-modis | MODIS MOD13Q1 NDVI, MAM + OND means 2000–2025 | agricultural-production, climate | CC-BY-4.0 | `domain=climate/type=vegetation/source=modis-mod13q1` |
| eastafrica-flood-jrc | JRC return-period flood depth (7 RPs) | hydrology | CC-BY-4.0 | `domain=climate/type=flood/source=jrc-glofas` |
| eastafrica-wrsi-fews | FEWS WRSI cropland + rangeland, MAM + OND | agricultural-production | CC0-1.0 | `domain=climate/type=agriculture/source=fews-wrsi` |
| kenya-flood-gfm | Copernicus GFM observed flood (monthly, seasonal, history) | hydrology | LicenseRef-Copernicus-EMS (+ `rel: license` link) | `domain=climate/type=flood/source=glofas-gfm/region=kenya` |
| kenya-population-worldpop | WorldPop constrained 2020 | socioeconomic | CC-BY-4.0 | `domain=exposure/type=population/source=worldpop-constrained-2020` |
| kenya-population-grid3 | GRID3/WOPR bottom-up 2020 | socioeconomic | CC-BY-4.0 | `domain=exposure/type=population/source=grid3` |
| kenya-admin-codab | IEBC COD-AB adm1/adm2 | boundaries | CC-BY-3.0-IGO | `domain=boundaries/type=admin/source=iebc-codab/region=kenya` |
| kenya-roads-osm | OSM classified highways | socioeconomic | ODbL-1.0 | `domain=exposure/type=infrastructure/source=osm/region=kenya` |
| kenya-facilities-hotosm | HOTOSM health + schools | socioeconomic | ODbL-1.0 | `…/source=hotosm/region=kenya` |
| kenya-power-grid-kplc | KPLC transmission grid | socioeconomic | CC0-1.0 | `…/source=energydata-kplc/region=kenya` |
| kenya-flood-exposure-intersect | Pre-cooked flood × exposure adm2 tables | socioeconomic, hydrology | **CC-BY-4.0** (was ODbL-1.0) | `domain=exposure/type=intersect/region=kenya` |
| kenya-population-knbs-census | KNBS 2019 census counts, adm0/adm1/adm2-KNBS + age×sex | socioeconomic | LicenseRef-KNBS-Terms ⚠ | `domain=exposure/type=population/source=knbs-census-2019/region=kenya` |
| kenya-population-knbs-projections | KNBS Vol XVI county projections 2020–2045 | socioeconomic | LicenseRef-KNBS-Terms ⚠ | `domain=exposure/type=population/source=knbs-projections-2020-2045/region=kenya` |

⚠ **KNBS licence is an open question.** KNBS publishes no open-data licence: `knbs.or.ke/terms-and-conditions/`
returns 404 and the site footer asserts "All Rights Reserved". Both records therefore carry
`LicenseRef-KNBS-Terms` with the position stated in the `license` comment (attributed republication of
published aggregate statistics). Confirm with KNBS — or route the age/sex table via HDX `cod-ps-ken`
(UNFPA, CC-BY-3.0-IGO), which redistributes the same census figures under a clear licence — before
these two go to cdh-catalog. The three-record change of 2026-09-15 also edited
`kenya-population-worldpop`, `kenya-population-grid3` and `kenya-flood-exposure-intersect` (documenting
the ~17 % gap against the census, and the new levelled population columns).

`ensemble_season_trends.cdh.yaml` is a separate v0.0.1 draft for the CR-119 trends dataset (not part of this set; not upgraded).

## What changed v0.1.0 → v0.3.0 (2026-09-14)

Schema-driven (every record):
- `$schema:` line added; `cdh_schema_version` and all extension URLs → `v0.3.0`.
- `temporal.resolution` removed (cadence now lives on `type: temporal` dimensions with a `step`); snapshots use `temporal.date`, spans use `start_date` + `end_date` (`null` = open-ended).
- `cdh.use_cases` → `cdh.usage.intended_uses`; `cdh.not_recommended_for` → `cdh.usage.not_recommended_for`.
- Every `href_template` token now has a matching `dimensions[]` entry **with its values enumerated from the live S3 listing** (the cross-field check Brayden flagged). Monthly axes list every `YYYY-MM`; seasons are a domain axis (`type: season`), years a temporal axis with `step: P1Y`.
- `{variable}` expands over *all* `variables[]`, so GFM is split into per-variable assets (monthly-flooded, monthly-nobs, seasonal-*, history-*). The unpopulated 20 m `overpass` asset and second resolution entry were dropped.
- `variables[].unit` is now required; vector records use `"1"` for categorical columns, `{person}` / `{facility}` UCUM annotations for counts.
- Author lists are block lists of quoted `"Surname, I."` strings — a YAML flow list `[Funk, C.]` silently splits into two authors (bug present in the v0.1.0 records).
- Added `series: Africa Agriculture Adaptation Atlas`, HTTPS + S3 `locations`, per-file `file_size`, `updated: 2026-09-14`.

Review-driven (issue #32):
- **Renamed** `africa-precipitation-chirps` → `africa-precipitation-monthly-seasonal`: an aggregation product, not a child of CHIRPS; `derived_from` links to the catalog record `chirps-v3-daily`.
- **Licence** `kenya-flood-exposure-intersect` ODbL-1.0 → CC-BY-4.0 (derived aggregate statistics; inputs attributed in `note`). `kenya-roads-osm` / `kenya-facilities-hotosm` keep ODbL-1.0 (clips) and gain a `rel: license` link.
- `LicenseRef-Copernicus-EMS` (GFM) now carries the required `additional_links[] rel: license` → Copernicus data policy.
- NDVI record corrected: only MAM and OND are published (52 files), not 12 windows / annual.

## Contribution route (Brayden, issue #32 + email 2026-09-10/11)

- PR into [CGIAR-Climate-Data-Hub/cdh-catalog](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog), layout `records/<id>/<id>.yaml`, **one PR per dataset** so each can be reviewed/edited independently. CI: `Validate records` (v0.3.0 profile + cross-field) and `Format` (prettier defaults).
- Parent/child nesting (spec §4.8) is decided case-by-case at PR review; the website currently renders everything top-level. Submit all 13 at the top level and let review decide on nesting (population clips, admin boundaries).
- Brayden wants a call on how child datasets are shown (UI/UX + governance).

## Submitted to cdh-catalog (2026-09-14)

One PR per dataset from in-repo `submit/<id>` branches (Brayden's bot convention). Main ruleset: PR + 1 approving review + code-owner review + `validate / validate` check.

| record | PR | state |
|---|---|---|
| africa-precipitation-monthly-seasonal | [#30](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/30) | ready for review |
| africa-spei-drought | [#31](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/31) | ready for review |
| eastafrica-flood-jrc | [#32](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/32) | ready for review |
| eastafrica-ndvi-modis | [#33](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/33) | ready for review |
| eastafrica-wrsi-fews | [#34](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/34) | ready for review |
| kenya-admin-codab | [#35](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/35) | ready for review |
| kenya-facilities-hotosm | [#36](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/36) | ready for review |
| kenya-flood-exposure-intersect | [#37](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/37) | draft (awaiting GFM NDJ/DJF relabel) |
| kenya-flood-gfm | [#38](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/38) | draft (awaiting GFM NDJ/DJF relabel) |
| kenya-population-grid3 | [#39](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/39) | ready for review |
| kenya-population-worldpop | [#40](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/40) | ready for review |
| kenya-power-grid-kplc | [#41](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/41) | ready for review |
| kenya-roads-osm | [#42](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/42) | ready for review |

Follow-ups owned here: after cglabs completes `DISPATCH_cglabs_gfm_flood.md #9` (GFM END-year relabel + intersect rebuild), refresh the notes / `year` dimension in `kenya-flood-gfm.yaml` and `kenya-flood-exposure-intersect.yaml`, push to their `submit/` branches, mark ready. After `DISPATCH_cglabs_seasonal_rasters.md #7` (CHIRPS NDJ rebuild), NDJ values in the precipitation record extend to 2026 (one-line follow-up on #30).

## Open review notes (flag at submission)
- `href_template` assumes every value combination exists. Seasonal edge windows are absent where a record cannot cover all three months (CHIRPS NDJ/DJF 1981 and windows ending after 2026-04; WRSI only cropland-MAM has 2026; GFM NDJ/DJF stop at 2024). Noted in each record's `note`.
- **Season-year label bugs (both FIXED in code, commit d89054a; rebuilds dispatched):** GFM labelled NDJ/DJF by START year (now END year like CHIRPS); CHIRPS NDJ shifted only December (NDJ-Y was Nov(Y)+Dec(Y−1)+Jan(Y), notebook tracker V2-63). Records for GFM/intersect will be refreshed once cglabs republishes.
- `eastafrica-flood-jrc` declares no `temporal` (static return-period hazard); reviewer may prefer a nominal date.
