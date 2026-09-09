# CDH metadata records — KE-ENSO Explorer datasets

13 CGIAR Climate Data Hub (CDH) **v0.1.0** metadata records, one per dataset the KE-ENSO Explorer uses.
**All 13 validate** against the CDH CI profile schema (2026-09-09):

```
uvx check-jsonschema \
  --schemafile "https://cgiar-climate-data-hub.github.io/cdh-metadata-standard/v0.1.0/schemas/profiles/cdh.schema.json" \
  metadata/cdh/*.yaml            # -> "ok -- validation done"
```
(zsh: pass the files as an array — `files=(metadata/cdh/*.yaml)` — an unquoted `$VAR` list does not word-split.)

| record | dataset | cdh.domain | licence | S3 prefix |
|---|---|---|---|---|
| africa-precipitation-chirps | CHIRPS PTOT monthly+seasonal | climate | CC-BY-4.0 | `domain=climate/type=observational/source=chirps-chirts-era5/…/variable=PTOT` |
| africa-spei-drought | SPEI-03 / SPEI-12 | climate | CC-BY-4.0 | `…/variable=SPEI-03\|SPEI-12` |
| eastafrica-ndvi-modis | MODIS MOD13Q1 NDVI (seasonal) | agricultural-production | CC-BY-4.0 | `domain=climate/type=vegetation/source=modis-mod13q1` |
| eastafrica-flood-jrc | JRC return-period flood hazard | hydrology | CC-BY-4.0 | `domain=climate/type=flood/source=jrc-glofas` |
| eastafrica-wrsi-fews | FEWS WRSI cropland+rangeland | agricultural-production | CC0-1.0 | `domain=climate/type=agriculture/source=fews-wrsi` |
| kenya-flood-gfm | Copernicus GFM observed flood | hydrology | LicenseRef-Copernicus-EMS | `domain=climate/type=flood/source=glofas-gfm/region=kenya` |
| kenya-population-worldpop | WorldPop constrained 2020 | socioeconomic | CC-BY-4.0 | `domain=exposure/type=population/source=worldpop-constrained-2020` |
| kenya-population-grid3 | GRID3/WOPR bottom-up 2020 | socioeconomic | CC-BY-4.0 | `domain=exposure/type=population/source=grid3` |
| kenya-admin-codab | IEBC COD-AB adm1/adm2 | boundaries | CC-BY-3.0-IGO | `domain=boundaries/type=admin/source=iebc-codab/region=kenya` |
| kenya-roads-osm | OSM classified highways | socioeconomic | ODbL-1.0 | `domain=exposure/type=infrastructure/source=osm/region=kenya` |
| kenya-facilities-hotosm | HOTOSM health + schools | socioeconomic | ODbL-1.0 | `…/source=hotosm/region=kenya` |
| kenya-power-grid-kplc | KPLC transmission grid | socioeconomic | CC0-1.0 | `…/source=energydata-kplc/region=kenya` |
| kenya-flood-exposure-intersect | Pre-cooked flood×exposure adm2 tables | socioeconomic, hydrology | ODbL-1.0 | `domain=exposure/type=intersect/region=kenya` |

## To contribute to the CDH
Per https://cgiar-climate-data-hub.github.io/contribute/ — either route:
1. **Registry PR** — fork the CDH metadata registry, add these `.yaml`, open a PR. CI runs structural + schema + lineage checks; maintainers + domain reviewers approve → merge → DOI.
2. **Browser app** — https://cgiar-climate-data-hub.github.io/CDH-metadata-app/ generates the form from the schema and submits a record as a GitHub Issue.

## Open review notes (flag at submission)
- `kenya-flood-exposure-intersect` licence = `ODbL-1.0` (most-restrictive input, OSM/HOTOSM). It's *derived aggregate statistics*, not redistributed geometry — confirm the correct licence treatment for derived stats with CDH review.
- `kenya-flood-gfm` licence = `LicenseRef-Copernicus-EMS` (Copernicus free/full/open; attribution "Contains modified Copernicus EMS information [year]") — not a clean SPDX id.
