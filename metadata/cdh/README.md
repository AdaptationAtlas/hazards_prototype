# CDH metadata records — KE-ENSO Explorer datasets

13 CGIAR Climate Data Hub (CDH) **v0.3.0** metadata records, one per dataset the KE-ENSO Explorer uses.
Upgraded from v0.1.0 on 2026-09-14 after Brayden's review
([cdh-metadata-standard#32](https://github.com/CGIAR-Climate-Data-Hub/cdh-metadata-standard/issues/32)).
**All 13 pass** the v0.3.0 schema + cross-field checks and `prettier --check` (what the catalog CI runs):

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

## Open review notes (flag at submission)
- `href_template` assumes every value combination exists. Seasonal edge windows are absent where a record cannot cover all three months (CHIRPS NDJ/DJF 1981 and windows ending after 2026-04; WRSI only cropland-MAM has 2026; GFM NDJ/DJF stop at 2024). Noted in each record's `note`.
- **GFM season-year label convention needs checking**: CHIRPS labels DJF by the year it *ends* (5b script: DJF-1998 = Dec 1997 + Jan–Feb 1998). GFM seasonal files have NDJ/DJF for 2018–2024 with monthly data 2018-01..2025-12, which is consistent with *start-year* labelling — if so, the notebook's season alignment between PTOT and GFM is off by one year for those two windows. Verify in `python/ingest_flood_gfm.py`.
- `eastafrica-flood-jrc` declares no `temporal` (static return-period hazard); reviewer may prefer a nominal date.
