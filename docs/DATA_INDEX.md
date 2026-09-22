# Data index

**Generated — do not edit by hand.** Rebuild with:

```bash
Rscript R/checks/73_catalogue.R --render
```

Source of truth: `metadata/catalogue/*.json` (one file per dataset, so `git log` on a record is that dataset's change history). Schema: `metadata/catalogue/_SCHEMA.md`.

The **here** column reflects one host only — whichever machine rendered this. It is a snapshot, not a claim about the other servers.

| dataset | class | version | status | here | Atlas S3 | CDH | transfer |
|---|---|---|---|---|---|---|---|
| [`crop-vop-intld15`](../metadata/catalogue/crop-vop-intld15.json) | derived | 0.4.0 | current | present (6) | - | none | regenerate |
| [`exposure-admin-tables`](../metadata/catalogue/exposure-admin-tables.json) | derived | spam20-20_glw420-20 | current | empty (0) | - | none | regenerate |
| [`livestock-vop`](../metadata/catalogue/livestock-vop.json) | derived | 0.4.1 | current | empty (0) | - | none | regenerate |
| [`nexgddp-indices-monthly`](../metadata/catalogue/nexgddp-indices-monthly.json) | derived | nexgddp | current | absent | partial | none | regenerate |
| [`nexgddp-indices-seasonal`](../metadata/catalogue/nexgddp-indices-seasonal.json) | derived | nexgddp | current | absent | - | none | regenerate |
| [`afr-highlands`](../metadata/catalogue/afr-highlands.json) | external-raw | 1 | current | present (1) | yes | none | pull-from-origin |
| [`atlas-pop-worldpop`](../metadata/catalogue/atlas-pop-worldpop.json) | external-raw | worldpop_2020 | current | present (3) | yes | none | pull-from-origin |
| [`boundaries-gaul2024`](../metadata/catalogue/boundaries-gaul2024.json) | external-raw | gaul2024 | current | present (3) | yes | none | pull-from-origin |
| [`cattle-heatstress`](../metadata/catalogue/cattle-heatstress.json) | external-raw | 1 | current | present (6) | yes | none | pull-from-origin |
| [`faostat-bulk`](../metadata/catalogue/faostat-bulk.json) | external-raw | rolling | current | present (37) | - | none | pull-from-origin |
| [`ggcmi-crop-calendars`](../metadata/catalogue/ggcmi-crop-calendars.json) | external-raw | phase3_v1.01 | current | empty (0) | - | none | pull-from-origin |
| [`glps`](../metadata/catalogue/glps.json) | external-raw | 1 | current | present (1) | yes | none | pull-from-origin |
| [`glw4-2015`](../metadata/catalogue/glw4-2015.json) | external-raw | GLW4-2015 | superseded | present (8) | partial | none | pull-from-origin |
| [`glw4-2020`](../metadata/catalogue/glw4-2020.json) | external-raw | GLW4-2020 (D-DA, 10 km) | current | present (6) | - | none | pull-from-origin |
| [`hydrobasins`](../metadata/catalogue/hydrobasins.json) | external-raw | 1 | current | empty (0) | - | none | pull-from-origin |
| [`mapspam-2020v1r2`](../metadata/catalogue/mapspam-2020v1r2.json) | external-raw | 2020V1r2 | current | present (48) | yes | none | pull-from-origin |
| [`mapspam-2020v1r2-raw`](../metadata/catalogue/mapspam-2020v1r2-raw.json) | external-raw | 2020V1r2 | current | empty (0) | yes | none | pull-from-origin |
| [`nexgddp-cmip6-raw`](../metadata/catalogue/nexgddp-cmip6-raw.json) | external-raw | v1.1 | current | absent | - | none | pull-from-origin |
| [`solution-tables`](../metadata/catalogue/solution-tables.json) | external-raw | 1 | current | present (3) | yes | none | pull-from-origin |
| [`sos-season-start`](../metadata/catalogue/sos-season-start.json) | external-raw | 1 | current | present (1) | yes | none | pull-from-origin |
| [`eastafrica-flood-jrc`](../metadata/catalogue/eastafrica-flood-jrc.json) | published | 1 | current | absent | yes | authored-valid | regenerate |
| [`eastafrica-ndvi-modis`](../metadata/catalogue/eastafrica-ndvi-modis.json) | published | 1 | current | absent | yes | authored-valid | regenerate |
| [`eastafrica-wrsi-fews`](../metadata/catalogue/eastafrica-wrsi-fews.json) | published | 1 | current | absent | yes | authored-valid | regenerate |
| [`hazard-exposure`](../metadata/catalogue/hazard-exposure.json) | published | nex-gddp-cmip6 | current | absent | partial | none | regenerate |
| [`kenya-admin-codab`](../metadata/catalogue/kenya-admin-codab.json) | published | 1 | current | absent | yes | authored-valid | regenerate |
| [`kenya-facilities-hotosm`](../metadata/catalogue/kenya-facilities-hotosm.json) | published | 1 | current | absent | yes | authored-valid | regenerate |
| [`kenya-flood-exposure-intersect`](../metadata/catalogue/kenya-flood-exposure-intersect.json) | published | 1 | current | absent | yes | authored-valid | regenerate |
| [`kenya-flood-gfm`](../metadata/catalogue/kenya-flood-gfm.json) | published | 1 | current | absent | yes | authored-valid | regenerate |
| [`kenya-population-grid3`](../metadata/catalogue/kenya-population-grid3.json) | published | 1 | current | absent | yes | authored-valid | regenerate |
| [`kenya-population-knbs-census`](../metadata/catalogue/kenya-population-knbs-census.json) | published | 1 | current | absent | yes | authored-valid | regenerate |
| [`kenya-population-knbs-projections`](../metadata/catalogue/kenya-population-knbs-projections.json) | published | 1 | current | absent | yes | authored-valid | regenerate |
| [`kenya-population-worldpop`](../metadata/catalogue/kenya-population-worldpop.json) | published | 1 | current | absent | yes | authored-valid | regenerate |
| [`kenya-power-grid-kplc`](../metadata/catalogue/kenya-power-grid-kplc.json) | published | 1 | current | absent | yes | authored-valid | regenerate |
| [`kenya-roads-osm`](../metadata/catalogue/kenya-roads-osm.json) | published | 1 | current | absent | yes | authored-valid | regenerate |
| [`obs-chirps-chirts`](../metadata/catalogue/obs-chirps-chirts.json) | published | CHIRPS v3 | current | present (15) | yes | authored-valid | pull-from-origin |
| [`obs-spei`](../metadata/catalogue/obs-spei.json) | published | 1 | current | absent | yes | authored-valid | regenerate |
| [`timeseries-mean-month`](../metadata/catalogue/timeseries-mean-month.json) | published | nex-gddp-cmip6 | current | empty (0) | yes | stale-version | regenerate |
| [`cpc-roni`](../metadata/catalogue/cpc-roni.json) | referenced | n/a | current | n/a | - | none | pull-from-origin |
| [`fews-ndma-prices`](../metadata/catalogue/fews-ndma-prices.json) | referenced | n/a | current | n/a | - | none | pull-from-origin |
| [`hadisst-dmi`](../metadata/catalogue/hadisst-dmi.json) | referenced | n/a | current | n/a | - | none | pull-from-origin |
| [`harveststat-africa`](../metadata/catalogue/harveststat-africa.json) | referenced | n/a | current | n/a | - | none | pull-from-origin |
| [`kfssg-ipc`](../metadata/catalogue/kfssg-ipc.json) | referenced | n/a | current | n/a | - | none | pull-from-origin |
| [`knbs-gesi`](../metadata/catalogue/knbs-gesi.json) | referenced | n/a | current | n/a | - | none | pull-from-origin |

## Transfer strategy

How a dataset reaches a new host. The monthly indices are `regenerate`, not `must-transfer`, because they derive from a public archive that any host can fetch directly — which is why no host-to-host link is needed.

### pull-from-origin (22)

- **afr-highlands** — Small, public, fetched per host by the shared downloader.
- **atlas-pop-worldpop** — On Atlas S3, fetched by 0_server_setup.R section 3.8.
- **boundaries-gaul2024** — Small, already on Atlas S3, fetched independently by 0_server_setup.R section 3.1.
- **cattle-heatstress** — Small, public, fetched per host by the shared downloader.
- **cpc-roni** — Cited, not held. Nothing is stored on any Atlas host and nothing is republished; consult the source directly.
- **faostat-bulk** — Public bulk downloads, small, fetched by 0_server_setup.R section 3.5.
- **fews-ndma-prices** — Cited, not held. Nothing is stored on any Atlas host and nothing is republished; consult the source directly.
- **ggcmi-crop-calendars** — Small public archive.
- **glps** — Small, public, fetched per host by the shared downloader.
- **glw4-2015** — Still auto-downloaded by setup.
- **glw4-2020** — Public FAO release on Google Cloud Storage, CC-BY-4.0, ~70 MB for the six species. Every host fetches its own copy.
- **hadisst-dmi** — Cited, not held. Nothing is stored on any Atlas host and nothing is republished; consult the source directly.
- **harveststat-africa** — Cited, not held. Nothing is stored on any Atlas host and nothing is republished; consult the source directly.
- **hydrobasins** — Small, public, fetched per host by the shared downloader.
- **kfssg-ipc** — Cited, not held. Nothing is stored on any Atlas host and nothing is republished; consult the source directly.
- **knbs-gesi** — Cited, not held. Nothing is stored on any Atlas host and nothing is republished; consult the source directly.
- **mapspam-2020v1r2-raw** — On Atlas S3; fetched per host.
- **mapspam-2020v1r2** — On Atlas S3 already; every host fetches its own copy.
- **nexgddp-cmip6-raw** — Public, anonymous, no egress cost to us, and ships a per-file MD5 index of 36,165 entries. Any host fetches it directly; never copy it host-to-host.
- **obs-chirps-chirts** — Public HTTP archive; R/observational/1 already maintains a per-file manifest with sha256 and uses it as a resume gate.
- **solution-tables** — Small, public, fetched per host by the shared downloader.
- **sos-season-start** — Small, public, fetched per host by the shared downloader.

### regenerate (21)

- **crop-vop-intld15** — Deterministic from MapSPAM plus FAOSTAT.
- **eastafrica-flood-jrc** — Re-run the ingest against its public origin rather than copying; the ingest is the definition of the dataset.
- **eastafrica-ndvi-modis** — Re-run the ingest against its public origin rather than copying; the ingest is the definition of the dataset.
- **eastafrica-wrsi-fews** — Re-run the ingest against its public origin rather than copying; the ingest is the definition of the dataset.
- **exposure-admin-tables** — One 0.4.4 run from inputs that are all pull-from-origin.
- **hazard-exposure** — Output of R/3; rebuild rather than move. The published copy on Atlas S3 is the distribution channel, not a sync route.
- **kenya-admin-codab** — Re-run the ingest against its public origin rather than copying; the ingest is the definition of the dataset.
- **kenya-facilities-hotosm** — Re-run the ingest against its public origin rather than copying; the ingest is the definition of the dataset.
- **kenya-flood-exposure-intersect** — Recompute from the ingested layers.
- **kenya-flood-gfm** — Re-run the ingest against its public origin rather than copying; the ingest is the definition of the dataset.
- **kenya-population-grid3** — Re-run the ingest against its public origin rather than copying; the ingest is the definition of the dataset.
- **kenya-population-knbs-census** — Re-run the ingest against its public origin rather than copying; the ingest is the definition of the dataset.
- **kenya-population-knbs-projections** — Re-run the ingest against its public origin rather than copying; the ingest is the definition of the dataset.
- **kenya-population-worldpop** — Re-run the ingest against its public origin rather than copying; the ingest is the definition of the dataset.
- **kenya-power-grid-kplc** — Re-run the ingest against its public origin rather than copying; the ingest is the definition of the dataset.
- **kenya-roads-osm** — Re-run the ingest against its public origin rather than copying; the ingest is the definition of the dataset.
- **livestock-vop** — Deterministic from GLW4-2020 plus FAOSTAT - but only where GLW4-2020 is present, which is its own gap.
- **nexgddp-indices-monthly** — Derived from a public archive that any host can fetch directly. Regenerating on the target host avoids both a host-to-host link (CGlabs runs no SSH daemon) and paying to park it on Atlas S3. PASCAL has ~4x the cores at ~2x the clock, so it is also likely the faster route.
- **nexgddp-indices-seasonal** — One R/1 run from the monthly indices. Cheaper to recompute than to move.
- **obs-spei** — One obs.2 run from the observational record.
- **timeseries-mean-month** — Output of R/2.1.

### must-transfer (0)


## Open gaps

Recorded in the catalogue, surfaced here so they are not invisible.

### boundaries-gaul2024

- Disputed-territory duplicate rows are an open Atlas-wide convention question (CR-115), not a data defect here.

### cpc-roni

- Referenced by Atlas outputs but not inventoried, versioned or archived by this project - if the source moves, the citation breaks.

### crop-vop-intld15

- Currency basis must stay aligned with the livestock counterpart - a past mismatch (nominal USD vs constant international dollars) produced a roughly 7x error in cattle exposure.

### eastafrica-ndvi-modis

- Requires NASA Earthdata credentials, so a new host cannot regenerate it unattended.

### eastafrica-wrsi-fews

- Only the rangeland-MAM series (FEWS e2, end-of-season dekad 21) extends to 2026; the other three series reach 2026 once their end-of-season dekad (33/36) is published, typically late November to January.

### exposure-admin-tables

- Chosen as the cross-host equivalence proof for issue #29 because every input is pull-from-origin.

### faostat-bulk

- ROLLING RELEASE, NO VERSION PIN. FAOSTAT rewrites these URLs in place, so two hosts that ran setup months apart hold different data with no way to tell from the filename. This is a live reproducibility risk for value-of-production outputs.

### fews-ndma-prices

- Referenced by Atlas outputs but not inventoried, versioned or archived by this project - if the source moves, the citation breaks.

### ggcmi-crop-calendars

- Setup gates the download on a bare file count of 40, not on a manifest.

### glw4-2015

- Superseded by GLW4-2020 for exposure work but still downloaded on every fresh setup.

### glw4-2020

- Confirm on-disk filenames on a host that already holds these files. list.files(glw_dir, '.tif$') globs the whole directory, so a second copy under different names would be stacked alongside the first and silently double the species set. Evidence the products match: a fresh fetch reproduces a cattle density global sum of 2.0384e7, the exact value documented for the CGlabs copy at R/0.4.1_create_livestock_exposure.R:101.
- These are per-km2 DENSITY, not per-pixel counts; 0.4.1 multiplies by cellSize to convert. The 2015 vintage was already per-pixel. Do not mix them.

### hadisst-dmi

- Referenced by Atlas outputs but not inventoried, versioned or archived by this project - if the source moves, the citation breaks.

### harveststat-africa

- Referenced by Atlas outputs but not inventoried, versioned or archived by this project - if the source moves, the citation breaks.

### hazard-exposure

- No CDH record yet, despite being the flagship published product.

### kenya-flood-gfm

- Seasonal NDJ and DJF windows stop at 2024; edge windows that cannot be covered in full are absent by design.

### kenya-population-knbs-census

- KNBS publishes no open licence, so redistribution terms are unresolved.
- Cannot be joined at admin 2 - only 183 of 290 sub-counties match the Atlas boundary set.

### kenya-population-knbs-projections

- Source is a PDF with two known transcription defects.
- No open licence.
- County-growth and county-level projection methods differ by roughly 1.4 M people.

### kenya-roads-osm

- OSM is a rolling snapshot with no version pin; the ingest records the download date only.

### kfssg-ipc

- Referenced by Atlas outputs but not inventoried, versioned or archived by this project - if the source moves, the citation breaks.

### knbs-gesi

- Referenced by Atlas outputs but not inventoried, versioned or archived by this project - if the source moves, the citation breaks.

### livestock-vop

- Blocked on the glw4-2020 origin gap. Empty on the laptop.

### nexgddp-cmip6-raw

- Per-host on-disk footprint not inventoried; the pipeline only needs a subset.

### nexgddp-indices-monthly

- CGlabs-only today. Never staged on PASCAL or the laptop.
- Wall-clock cost of regeneration on PASCAL is unmeasured - size it on one GCM before committing to a full rebuild.

### nexgddp-indices-seasonal

- CGlabs-only today.

### sos-season-start

- Data/sos/sos.tif is built by R/0.2_create_sos_rast.R from a CGlabs-only raw tree (atlas_sos/seasonal_mean); the S3 copy and the derived raster are not the same thing.

### timeseries-mean-month

- Its CDH record is still at schema v0.0.1 with no data block, while every other record is v0.3.0.

---

Rendered 2026-09-22 on host `mac-pstewarda`.
