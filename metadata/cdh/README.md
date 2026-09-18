# CDH metadata records

CGIAR Climate Data Hub (CDH) **v0.3.0** metadata records for the datasets the Adaptation Atlas
publishes, reads or cites. **25 records validate against the strict profile; 3 are drafts.**

Last updated 2026-09-18.

```
metadata/cdh/*.yaml         strict — pass the full profile, submittable to cdh-catalog
metadata/cdh/draft/*.yaml   drafts — pass `--draft` only, each names its own blocker
```

The directory is the state. The profile sets `unevaluatedProperties: false`, so no `draft:` key can
exist on a record; promotion is the same file passing without `--draft`, then `git mv` up one level.

## Validating (this is exactly what catalog CI runs)

```sh
git clone --depth 1 --branch v0.3.0 \
  https://github.com/CGIAR-Climate-Data-Hub/cdh-metadata-standard.git /tmp/cdh-std
(cd /tmp/cdh-std && npm ci)

# strict — zsh: use an array, an unquoted $VAR list does not word-split
files=(metadata/cdh/*.yaml); files=(${files:#*ensemble_season_trends*})
node /tmp/cdh-std/scripts/validate-yaml.js \
  --profile /tmp/cdh-std/spec/schemas/profiles/cdh.schema.json $files

# drafts
node /tmp/cdh-std/scripts/validate-yaml.js --draft \
  --profile /tmp/cdh-std/spec/schemas/profiles/cdh.schema.json metadata/cdh/draft/*.yaml

npx prettier@3 --check $files metadata/cdh/draft/*.yaml   # prettier DEFAULTS, no .prettierrc
```

Schema errors short-circuit: cross-field rules only run once the schema passes, so a record can look
nearly clean and then fail again. `uvx check-jsonschema` alone is **not** enough — it misses the
cross-field checks.

## Records

| record | licence | status |
|---|---|---|
| `africa-precipitation-monthly-seasonal` | CC-BY-4.0 | **merged** (#30) |
| `africa-spei-drought` | CC-BY-4.0 | PR #31 |
| `eastafrica-flood-jrc` | CC-BY-4.0 | PR #32 — federated to Source Cooperative |
| `eastafrica-ndvi-modis` | CC-BY-4.0 | PR #33 |
| `eastafrica-wrsi-fews` | CC0-1.0 | PR #34 |
| `kenya-admin-codab` | CC-BY-3.0-IGO | PR #35 |
| `kenya-facilities-hotosm` | ODbL-1.0 | PR #36 |
| `kenya-flood-exposure-intersect` | CC-BY-4.0 | PR #37 *(draft — awaiting GFM relabel)* |
| `kenya-flood-gfm` | LicenseRef-Copernicus-EMS | PR #38 *(draft — awaiting GFM relabel)* |
| `kenya-population-worldpop` | CC-BY-4.0 | PR #40 |
| `kenya-power-grid-kplc` | CC0-1.0 | PR #41 |
| `kenya-roads-osm` | ODbL-1.0 | PR #42 |
| `enso-driver-roni` | CC0-1.0 | PR #43 — first federated record |
| `enso-driver-dmi` | CC0-1.0 | PR #44 — federated |
| `kenya-population-knbs-census` | CC0-1.0 | PR #45 |
| `harveststat-crop-production` | CC0-1.0 | PR #46 — federated |
| `kenya-food-insecurity-ipc` | CC-BY-NC-SA-3.0-IGO | PR #47 — federated, **NC + share-alike** |
| `kenya-county-gender-datasheets` | LicenseRef-KE-Gov-Statistics-Assumed-Open | PR #48 — federated |
| `kenya-market-prices-fews` | LicenseRef-FEWSNET-Data-Use-Policy | PR #50 — federated |
| `mapspam2020-adaptation-atlas-ssa` | CC-BY-4.0 | not submitted |
| `africa-admin-boundaries-gaul2024` | CC-BY-4.0 | not submitted |
| `africa-population-worldpop-aggregated` | CC-BY-4.0 | not submitted |
| `africa-hazard-exposure-nexgddp` | CC-BY-4.0 | not submitted — **see the defect note** |
| `kenya-population-knbs-projections` | LicenseRef-KNBS-All-Rights-Reserved | **must not be submitted** — see below |
| `kenya-population-grid3` | CC-BY-4.0 | **retired** — PR #39 closed, kept local |

Drafts, each blocked on one fact:

| draft | blocker |
|---|---|
| `enso-driver-western-v` | box coordinates, base period and SST product — all D409-side, not in any repository |
| `kenya-market-prices-ndma` | nothing ingests it; kept as a candidate source |
| `nexgddp-indices-monthly` | publication pending — the monthly rasters are not on cloud storage |

`ensemble_season_trends.cdh.yaml` is a separate **v0.0.1 draft** for the CR-119 trends dataset. Not
part of this set, not upgraded, still carries `license: ""` and `citation: ""` TODOs. Do not surface it.

## Conventions established here

**Federated records.** A dataset the Atlas cites but does not host gets a normal, strict-passing
record pointing at the provider: `data[].locations` is the provider's URL, there is no
`digital-atlas` entry, `series:` is omitted (it marks Atlas products), the provider is
`licensor`/`producer` and we are `point-of-contact` only — never `processor` or `custodian`, since we
process nothing and hold nothing. Volatility goes in `note`: CPC revises RONI, HadISST reissues, IPC
re-analyses. A processed copy of ours is a **separate** record with `derived_from`, not an asset on
the federated one.

**National subsets are first-class.** The CDH plans them, so a Kenya-scoped resource is its own
record rather than a child of a continental one. A different-resolution representation is likewise a
separate record — `kenya-population-worldpop` (100 m) and `africa-population-worldpop-aggregated`
(~9 km) are ninety times apart in grain and must not be substituted.

**Never assert a licence you cannot evidence.** An absent licence is a visible gap; a guessed one is
a wrong claim. Where a rights holder has published nothing and the data owner decides to proceed, say
so explicitly: `LicenseRef-KE-Gov-Statistics-Assumed-Open` means *"no rights stated, treated as
open"*, **not** *"the publishers granted an open licence"*, and the mandatory `rel: license` link
documents the **absence** of terms.

**Verify the ingest before authoring a source record.** A filename is not evidence. `kenya-market-prices-ndma`
was authored from a bulletin filename and submitted before anyone checked what the pipeline reads —
it reads FEWS NET. PR #49 was closed and the record demoted.

## Licence findings worth not re-deriving

- **KNBS census is `CC0-1.0`.** KNBS's own HDX organisation released the identical workbook under
  `license_id: other-pd-nr`, "Public Domain / No restrictions". Verified against the HDX API.
- **KNBS Volume XVI projections are All Rights Reserved.** Read from the PDF: *"© 2022 KNBS. All
  rights reserved … without the prior written permission of the Bureau."* The Open License Agreement
  is scoped to the Open Data **Platform** and does not reach a PDF on knbs.or.ke. No licensed mirror
  exists — UNFPA's `cod-ps-ken` is census-only. **Analysis is fine; redistribution is not.** The
  pipeline already does the safe thing: `POP_METHOD=county-growth` (the default) applies projections
  as a dimensionless ratio on a CC0 census anchor. `POP_METHOD=county-level` reproduces the published
  table and must not be used for anything published.
- **HarvestStat is `CC0-1.0`** — the Dryad landing page prints no licence, the **API** does. For
  Dryad-hosted datasets the API is authoritative where the page is silent.
- **IPC is `CC-BY-NC-SA-3.0-IGO`** — non-commercial *and* share-alike, the only such licence here.
  Cite and link freely; folding phases into a CC-BY-4.0 Atlas layer is not possible.
- **HadISST is UK Non-Commercial Government Licence.** The DMI axis was moved to NOAA CPC's ERSSTv6
  series to avoid it — which also put RONI and DMI on one reconstruction and one 1991-2020 base
  period, a correctness fix rather than a licence dodge. The consuming notebook has not migrated yet
  (hazards_prototype#36).
- **MapSPAM: ours is not IFPRI's.** `MapSPAM2020_AdaptationAtlas_SSA` is CC-BY-4.0 with DOI
  10.7910/DVN/Z0HK7R. The Hub's `spam2020` record is IFPRI's **global v2r2** under CC-BY-SA-4.0.
  Different scope, licence, format and DOI — the share-alike does **not** reach our exposure chain.

## Known upstream bug

`--draft` rejects `spatial.bbox` in both permitted forms while the strict profile accepts both:
`stripPresence()` removes the `minItems`/`maxItems` that discriminate the `oneOf`. Reported as
[cdh-metadata-standard#33](https://github.com/CGIAR-Climate-Data-Hub/cdh-metadata-standard/issues/33)
with a reproducer. Workaround: omit `bbox` from drafts and restore it at promotion.

## Contribution route

PR into [`cdh-catalog`](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog), layout
`records/<id>/<id>.yaml`, **one PR per dataset**, from in-repo `submit/<id>` branches. CI is
`Validate records` and `Format`. Main ruleset needs an approving review plus code-owner review, and
Pete cannot self-approve.

**Gotcha:** the catalog clone is shallow. `git checkout submit/<id>` fails until
`git fetch origin submit/<id>:refs/remotes/origin/submit/<id>` — and a chained command will carry on
regardless, so verify the branch updated before marking a PR ready.

## Open questions raised with the Hub

- Should federated records be segregated — directory, keyword, or `resource_type` convention? (#43/#44)
- Should licence **class** surface in the UI, now that NC/SA records sit beside permissive ones? (#47)
- Will the Hub carry **assumed-open** records at all, or should they be withheld pending confirmation? (#48)
- `records/mapspam2020/mapspam2020.yaml` has `id: spam2020`, breaking `records/<id>/<id>.yaml`. Any
  id-to-path resolution misses it.
