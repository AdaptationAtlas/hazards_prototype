# Data index

**Generated — do not edit by hand.** Rebuild with:

```bash
Rscript R/checks/73_catalogue.R --render
```

Source of truth: `metadata/catalogue/*.json` (one file per dataset, so `git log` on a record is that dataset's change history). Schema: `metadata/catalogue/_SCHEMA.md`.

The **here** column reflects one host only — whichever machine rendered this. It is a snapshot, not a claim about the other servers.

| dataset | class | version | status | here | Atlas S3 | CDH | transfer |
|---|---|---|---|---|---|---|---|
| [`crop-vop-intld15`](../metadata/catalogue/crop-vop-intld15.json) | derived | 0.4.0 | current | present (6) | - | - | regenerate |
| [`exposure-admin-tables`](../metadata/catalogue/exposure-admin-tables.json) | derived | spam20-20_glw420-20 | current | empty (0) | - | - | regenerate |
| [`livestock-vop`](../metadata/catalogue/livestock-vop.json) | derived | 0.4.1 | current | empty (0) | - | - | regenerate |
| [`nexgddp-indices-monthly`](../metadata/catalogue/nexgddp-indices-monthly.json) | derived | nexgddp | current | absent | partial | - | regenerate |
| [`nexgddp-indices-seasonal`](../metadata/catalogue/nexgddp-indices-seasonal.json) | derived | nexgddp | current | absent | - | - | regenerate |
| [`atlas-pop-worldpop`](../metadata/catalogue/atlas-pop-worldpop.json) | external-raw | worldpop_2020 | current | present (3) | yes | - | pull-from-origin |
| [`boundaries-gaul2024`](../metadata/catalogue/boundaries-gaul2024.json) | external-raw | gaul2024 | current | present (3) | yes | - | pull-from-origin |
| [`faostat-bulk`](../metadata/catalogue/faostat-bulk.json) | external-raw | rolling | current | present (37) | - | - | pull-from-origin |
| [`ggcmi-crop-calendars`](../metadata/catalogue/ggcmi-crop-calendars.json) | external-raw | phase3_v1.01 | current | empty (0) | - | - | pull-from-origin |
| [`glw4-2015`](../metadata/catalogue/glw4-2015.json) | external-raw | GLW4-2015 | superseded | present (8) | partial | - | pull-from-origin |
| [`glw4-2020`](../metadata/catalogue/glw4-2020.json) | external-raw | GLW4-2020 | current | empty (0) | - | - | pull-from-origin |
| [`mapspam-2020v1r2`](../metadata/catalogue/mapspam-2020v1r2.json) | external-raw | 2020V1r2 | current | present (48) | yes | - | pull-from-origin |
| [`nexgddp-cmip6-raw`](../metadata/catalogue/nexgddp-cmip6-raw.json) | external-raw | v1.1 | current | absent | - | - | pull-from-origin |
| [`eastafrica-flood-jrc`](../metadata/catalogue/eastafrica-flood-jrc.json) | published | 1 | current | absent | yes | yes | regenerate |
| [`eastafrica-ndvi-modis`](../metadata/catalogue/eastafrica-ndvi-modis.json) | published | 1 | current | absent | yes | yes | regenerate |
| [`eastafrica-wrsi-fews`](../metadata/catalogue/eastafrica-wrsi-fews.json) | published | 1 | current | absent | yes | yes | regenerate |
| [`hazard-exposure`](../metadata/catalogue/hazard-exposure.json) | published | nex-gddp-cmip6 | current | absent | partial | - | regenerate |
| [`kenya-admin-codab`](../metadata/catalogue/kenya-admin-codab.json) | published | 1 | current | absent | yes | yes | regenerate |
| [`kenya-facilities-hotosm`](../metadata/catalogue/kenya-facilities-hotosm.json) | published | 1 | current | absent | yes | yes | regenerate |
| [`kenya-flood-exposure-intersect`](../metadata/catalogue/kenya-flood-exposure-intersect.json) | published | 1 | current | absent | yes | yes | regenerate |
| [`kenya-flood-gfm`](../metadata/catalogue/kenya-flood-gfm.json) | published | 1 | current | absent | yes | yes | regenerate |
| [`kenya-population-grid3`](../metadata/catalogue/kenya-population-grid3.json) | published | 1 | current | absent | yes | yes | regenerate |
| [`kenya-population-knbs-census`](../metadata/catalogue/kenya-population-knbs-census.json) | published | 1 | current | absent | yes | yes | regenerate |
| [`kenya-population-knbs-projections`](../metadata/catalogue/kenya-population-knbs-projections.json) | published | 1 | current | absent | yes | yes | regenerate |
| [`kenya-population-worldpop`](../metadata/catalogue/kenya-population-worldpop.json) | published | 1 | current | absent | yes | yes | regenerate |
| [`kenya-power-grid-kplc`](../metadata/catalogue/kenya-power-grid-kplc.json) | published | 1 | current | absent | yes | yes | regenerate |
| [`kenya-roads-osm`](../metadata/catalogue/kenya-roads-osm.json) | published | 1 | current | absent | yes | yes | regenerate |
| [`obs-chirps-chirts`](../metadata/catalogue/obs-chirps-chirts.json) | published | CHIRPS v3 | current | present (15) | yes | yes | pull-from-origin |
| [`obs-spei`](../metadata/catalogue/obs-spei.json) | published | 1 | current | absent | yes | yes | regenerate |
| [`timeseries-mean-month`](../metadata/catalogue/timeseries-mean-month.json) | published | nex-gddp-cmip6 | current | empty (0) | yes | yes | regenerate |

## Transfer strategy

How a dataset reaches a new host. The monthly indices are `regenerate`, not `must-transfer`, because they derive from a public archive that any host can fetch directly — which is why no host-to-host link is needed.

### pull-from-origin (9)

- **atlas-pop-worldpop** — On Atlas S3, fetched by 0_server_setup.R section 3.8.
- **boundaries-gaul2024** — Small, already on Atlas S3, fetched independently by 0_server_setup.R section 3.1.
- **faostat-bulk** — Public bulk downloads, small, fetched by 0_server_setup.R section 3.5.
- **ggcmi-crop-calendars** — Small public archive.
- **glw4-2015** — Still auto-downloaded by setup.
- **glw4-2020** — Small enough to fetch per host once the origin is pinned down.
- **mapspam-2020v1r2** — On Atlas S3 already; every host fetches its own copy.
- **nexgddp-cmip6-raw** — Public, anonymous, no egress cost to us, and ships a per-file MD5 index of 36,165 entries. Any host fetches it directly; never copy it host-to-host.
- **obs-chirps-chirts** — Public HTTP archive; R/observational/1 already maintains a per-file manifest with sha256 and uses it as a resume gate.

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

### crop-vop-intld15

- Currency basis must stay aligned with the livestock counterpart - a past mismatch (nominal USD vs constant international dollars) produced a roughly 7x error in cattle exposure.

### eastafrica-ndvi-modis

- Requires NASA Earthdata credentials, so a new host cannot regenerate it unattended.

### eastafrica-wrsi-fews

- Only the cropland-MAM series extends to 2026; other series stop earlier.

### exposure-admin-tables

- Chosen as the cross-host equivalence proof for issue #29 because every input is pull-from-origin.

### faostat-bulk

- ROLLING RELEASE, NO VERSION PIN. FAOSTAT rewrites these URLs in place, so two hosts that ran setup months apart hold different data with no way to tell from the filename. This is a live reproducibility risk for value-of-production outputs.

### ggcmi-crop-calendars

- Setup gates the download on a bare file count of 40, not on a manifest.

### glw4-2015

- Superseded by GLW4-2020 for exposure work but still downloaded on every fresh setup.

### glw4-2020

- ORIGIN NOT PINNED. 0_server_setup.R downloads the 2015 vintage from Harvard Dataverse but never fetches GLW4-2020, so on CGlabs it was staged by some other route. Until this is recorded, GLW4-2020 is effectively must-transfer and cannot be reproduced on a new host.
- Empty on the laptop, which is why stage 0.4.4 reports NOT READY there.

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

### livestock-vop

- Blocked on the glw4-2020 origin gap. Empty on the laptop.

### nexgddp-cmip6-raw

- Per-host on-disk footprint not inventoried; the pipeline only needs a subset.

### nexgddp-indices-monthly

- CGlabs-only today. Never staged on PASCAL or the laptop.
- Wall-clock cost of regeneration on PASCAL is unmeasured - size it on one GCM before committing to a full rebuild.

### nexgddp-indices-seasonal

- CGlabs-only today.

### timeseries-mean-month

- Its CDH record is still at schema v0.0.1 with no data block, while every other record is v0.3.0.

---

Rendered 2026-09-18 on host `mac-pstewarda`.
