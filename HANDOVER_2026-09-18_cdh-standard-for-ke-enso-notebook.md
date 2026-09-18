# HANDOVER 2026-09-18 — CDH metadata standard for the KE-ENSO Explorer notebook

Audience: the session that owns `notebooks/KE-enso-explorer/` in `AdaptationAtlas/atlas_notebooks`
(issue #48). Author: hazards_prototype pipeline session. Nothing in `atlas_notebooks` was touched.

---

## 0. Answers up front

1. **The standard is already adopted and the records already exist.** 15 CDH **v0.3.0** records live
   at `metadata/cdh/*.yaml` in `AdaptationAtlas/hazards_prototype`; 13 are open PRs in the CDH
   catalog (`cdh-catalog` #30–#42). Do not re-author them — read them.
2. **There is no `sidecar:` field, and one cannot be invented.** The CDH profile sets
   `unevaluatedProperties: false`, so any undeclared top-level key is rejected. Verified this
   session: our two KNBS records carried a hand-rolled `licence_note:` key and **failed**
   (`must NOT have unevaluated properties: "licence_note"`). Fixed — see §7.
3. **6 of the 16 datasets in the ask have records. 10 do not, and 8 of those have no publish path
   in this repo at all** — they need a governance decision, not a template. §6.
4. **Asks 2 (UI framing) and 4 (drawer / `tab-methods` wiring) are the notebook session's, not
   mine** ([[feedback_scope_pipeline_not_notebooks]]). I answer the schema side of 2 in full and
   give the field-visibility split in §4, but I am not designing or wiring the drawer.

---

## 1. Standard definition and schema location

| Thing | Where |
|---|---|
| Standard + schema + tooling | `github.com/CGIAR-Climate-Data-Hub/cdh-metadata-standard`, **tag `v0.3.0`** |
| Normative spec | `spec/standard.md` (§4.2 = extension rule, §4.8 = parent/child) |
| Authoring guide | `spec/authoring-guide.md` |
| Core schema | `spec/schemas/core.schema.json` |
| Profile (what CI validates against) | `spec/schemas/profiles/cdh.schema.json` |
| Extensions | `spec/extensions/{cdh,datacube,climate,agriculture,classification}/schema.json` |
| Cross-field rules (not in JSON Schema) | `spec/checks/cross-field.js` |
| Controlled vocabularies | `vocab/{domain,geography,commodity,resource_type}.json` |
| Full exemplar | `examples/kitchen-sink/` |
| **Catalog** (where records are published) | `github.com/CGIAR-Climate-Data-Hub/cdh-catalog`, layout `records/<id>/<id>.yaml` |
| Owner | Brayden Youngberg (`bjyberg`, B.Youngberg@cgiar.org) |

Catalog CI = `Validate records` (v0.3.0 profile + cross-field, changed files only) and `Format`
(`npx prettier@3 --check`, **prettier defaults, no `.prettierrc`, printWidth 80**).

Reproduce CI locally — this is the only check that counts:

```sh
git clone --depth 1 --branch v0.3.0 \
  https://github.com/CGIAR-Climate-Data-Hub/cdh-metadata-standard.git /tmp/cdh-std
(cd /tmp/cdh-std && npm ci)

files=(metadata/cdh/*.yaml); files=(${files:#*ensemble_season_trends*})   # zsh: array, not $VAR
node /tmp/cdh-std/scripts/validate-yaml.js \
  --profile /tmp/cdh-std/spec/schemas/profiles/cdh.schema.json $files
npx prettier@3 --check $files
```

`uvx check-jsonschema` alone is **not** sufficient — it misses the cross-field rules. Schema errors
short-circuit: cross-field checks only run once the schema passes, so a record can look "nearly
clean" and then fail a second time.

### Required fields

**Core (`core.schema.json`, `required`):**
`cdh_schema_version`, `id`, `title`, `description`, `license`, `resource_type`, `keywords`,
`contact`, `data`.

**CDH profile adds:** `$schema`, `extensions`, `cdh`.

**Extension gates:**

| Extension | Declare in `extensions[]` when you use | Requires |
|---|---|---|
| `cdh` | always (profile-mandatory) | `cdh.domain` (vocab). `cdh.usage` optional; `additionalProperties: false` |
| `datacube` | any of `dimensions` / `variables` / `joins` present | — |

Every extension URL's version must equal `cdh_schema_version`.

**Sub-object requirements that bite:**

| Object | Required keys | Notes |
|---|---|---|
| `contact[]` | `organization`, `roles[]` | roles ∈ `licensor \| producer \| processor \| point-of-contact \| custodian`; `name`/`email`/`url` optional |
| `citation` | `authors[]`, `date` | `authors` must be a **block list of quoted `"Surname, I."`** — a flow list `[Funk, C.]` silently splits into two authors |
| `data[]` (dataAsset) | `name`, `locations[]` | `name` must be unique across `data[]` + `additional_assets[]` |
| `additional_assets[]` | `name`, `locations[]` | the standard's own sidecar hook — see §2 |
| `locations[]` | `url` | **first entry is canonical**; `title` = access label ("HTTPS", "S3") |
| `additional_links[]` | `rel`, `url` | only `rel`, `url`, `name`, `description` allowed |
| `processing[]` | at least one step with `id: source` | per-step: `id`, `description`, `date`, `code{url,version}`, `derived_from[{title,url}]` |
| `variables[]` | `unit` | `"1"` = dimensionless; UCUM annotations `{person}`, `{facility}` OK |
| `spatial.resolution` | exactly one entry (or an x+y pair), `type` required (`xy\|x\|y\|point\|polygon`) | a different resolution = a **separate record** |
| `temporal` | `date` **XOR** `start_date`+`end_date` | `end_date: null` = open-ended; `start_date` **requires** `end_date`. `temporal.resolution` was REMOVED in v0.3.0 — cadence goes on a `type: temporal` dimension with an ISO-8601 `step`. Omit `temporal` entirely for static layers (e.g. `eastafrica-flood-jrc`) |
| `citation` **or** `doi` | one of the two | enforced as an `anyOf` by the profile: a record with neither fails with `must have required property 'citation'` / `'doi'` / `must match a schema in anyOf` |
| `access` | — (default `public`) | `public \| restricted \| non-public`; `access_note` **required** when not public |

**Cross-field rules, all enforced:**

- `license: LicenseRef-*` ⇒ `additional_links[]` **must** contain `{rel: license, url}`.
  (This is exactly what our two KNBS records were missing.)
- Every `{token}` in `data[].href_template` must be a `dimensions[].name` **with its `values`
  enumerated**, or the reserved `{variable}` (which expands over **all** of `variables[]` — so when
  files differ per variable, split into per-variable assets, as `kenya-flood-gfm` does).
- `data[].processing_steps` ids must resolve to `processing[].id`.
- `created <= updated`; SPDX-valid licence ids; no duplicate names; no blank strings, empty arrays
  or stray nulls (sole exception `end_date: null`).
- `dimensions[].type`: `temporal` (values MUST be ISO strings `"YYYY"`, `"YYYY-MM"`; only this type
  may carry `step`), `z`, `location`, or a lowercase domain-axis name (`season`, `return-period`,
  `land-use`). `time` / `date` / `spatial` / `geometry` are rejected **as a type** (fine as a name).
  Cyclic labels (DJF, MAM, OND) are a domain axis `type: season`, **not** temporal.

---

## 2. Sidecars — the schema answer

### Verdict

**No embedded `sidecar:` block, and no ad-hoc `note:`-adjacent keys.** Empirically proven this
session against the real validator:

```
FAIL metadata/cdh/kenya-population-knbs-census.yaml
  /: must have required property 'additional_links'
  /: must match "then" schema
  /: must NOT have unevaluated properties: "licence_note"
```

So: everything the notebook drawer wants must go into an **existing** field, a **declared
extension**, or a **separate file registered as an asset**. There are four legitimate homes, and
between them they cover every category in the ask.

### 2a. Analytical caveats → `note` + `cdh.usage` (already done)

Free-prose caveats go in `note` (unstructured, unlimited) and the machine-readable
"don't do this" list goes in `cdh.usage.not_recommended_for[{use, reason, use_instead}]`.

Both of the caveats named in the ask are **already authored**, in `kenya-flood-gfm.yaml`:

- *Blank ≠ Zero* — `note`: "Pixel coding of the flooded layers: 0 = not flooded, 1 = flooded,
  255 = not observed (SAR gap or excluded) — 255 is absence of observation, NOT dry; the nobs layer
  gives the count of valid observations so no-flood is distinguishable from no-observation."
- *SAR orbit gaps / record length* — same `note`, plus
  `cdh.usage.not_recommended_for`: reading the history frequency as a return period, and splicing
  GFM onto the pre-2018 MODIS Global Flood Database (methodological break → apparent post-2018
  increases would be artefacts).

The same pattern carries the season-label trap (GFM labels NDJ/DJF by first month, CHIRPS by last)
and the KNBS admin-universe trap (345 KNBS sub-counties vs 290 COD-AB constituencies). **The drawer
should render `note` verbatim — it is where the load-bearing caveats already live.**

### 2b. Acquisition + processing chain → `processing[]`

Ordered, id'd steps; assets point back at them via `data[].processing_steps`. Shape:

```yaml
processing:
  - id: source                       # exactly one step must use id: source
    description: JRC / GloFAS global river flood hazard maps (hydrodynamic modelling of return-period flows).
    derived_from:                    # UPSTREAM DATA, not code
      - title: JRC Global Flood Maps (JRC Data Catalogue)
        url: https://data.jrc.ec.europa.eu/dataset/jrc-floods-floodmapgl_rp50y
  - id: crop-cog
    description: Crop to the Kenya window and write one COG per return period with internal overviews.
    code:
      url: https://github.com/AdaptationAtlas/hazards_prototype
      version: python/ingest_flood_jrc.py
```

Detrending / index construction belongs here too, as its own step (see the SPEI and
precipitation records, which carry the method in a step description).

### 2c. Analysis rationale → `description` + `cdh.usage.intended_uses`

`intended_uses` is explicitly documented as "illustrative and never exhaustive" — a use that is
absent is not excluded, a use that is listed is not endorsed for a particular decision. Proposal
writers should quote `description` + `intended_uses`, and are covered for the negative case by
`not_recommended_for`.

### 2d. A genuine paired file → `additional_assets[]` (recommended, and a live gap)

Core `additional_assets` is described in the schema as: *"Sidecar or auxiliary assets, e.g. metadata
files or thumbnails."* Shape (`required: [name, locations]`, `additionalProperties: false`):

```yaml
additional_assets:
  - name: build-sidecar-adm1
    description: Machine-readable build provenance emitted beside the parquet by the pipeline.
    roles: [metadata, describedby]        # suggested set, not closed: metadata, validation,
                                          # describedby, thumbnail, overview, visual, example
    media_type: application/json
    locations:
      - url: https://digital-atlas.s3.amazonaws.com/<prefix>/<file>.parquet.json
        title: HTTPS
      - url: s3://digital-atlas/<prefix>/<file>.parquet.json
        title: S3
```

**This is the gap worth closing.** The pipeline *already emits* JSON sidecars next to published
artefacts and **no CDH record declares any of them**:

| Writer | Sidecar |
|---|---|
| `R/observational/3_extract_obs_admin.R:582` | `<out>.parquet.json` — `file`, `admin_level`, `variables`, `aggregation`, `obs_base_rast`, `n_rows`, `year_range`, `build_time`, `parent_script` |
| `R/observational/2_calculate_obs_spei.R:464` | `_metadata.json` — `variable`, `method` ("Hargreaves PET (FAO-56 Allen 1998) → CWB → SPEI::spei (log-Logistic, ub-pwm)"), `reference_period`, `scale_months`, `distribution`, `fit`, `n_files`, `year_range`, `format`, `parent_script`, `date_created`, `notes` |
| `R/2.1_create_monthly_haz_tables.R:838,866` / `R/3_freq_x_exposure.R:1275` | `<tier>.parquet.json` — ensemble/GCM membership (issue #26 ask 4) |

Those files carry exactly the "acquisition + processing" payload the ask wants, in machine-readable
form, and the standard already has the slot for them. Recommended split:

- **Prose that a human must read → inside the record** (`note`, `description`, `cdh.usage`,
  `processing[].description`). One document, one review, one PR.
- **Build-time facts a machine should read → the existing `.json` sidecar, declared as an
  `additional_assets[]` entry with `roles: [metadata]`.** No schema change, no new convention.

Do **not** invent `<dataset>.sidecar.yaml`: a second YAML paired with the record duplicates fields
the record already owns and would drift from it.

### 2e. If structured extra fields really are needed → a versioned extension

`spec/extending.md`: one extension = one self-contained JSON Schema nesting **all** its fields under
a single top-level key named after the extension, published at a stable version-pinned URL, `$id`
set to that URL, declared in the record's `extensions[]`. *"A record is validated against the core
composed with exactly the extensions it declares — fields from an undeclared extension are
rejected."* Records submitted to the Hub with a non-CDH extension must ship the schema so CI can
register it via `--schemas`, and *"shared fields should become shared extensions"*.

So a CDH-wide `provenance`/`sidecar` extension is **Brayden's call**, not a local decision. If the
notebook needs a field that §2a–2d cannot hold, the route is: propose it on
`cdh-metadata-standard`, not add a key locally. Everything in the ask fits §2a–2d today.

---

## 3. Processed cloud assets and upstream sources

**Our processed assets → `data[].locations[]`, HTTPS canonical first, then S3.** Pattern used by all
15 records (hive-partitioned `digital-atlas` layout):

```yaml
data:
  - name: return-period
    description: Flood-depth COGs, one per return period.
    locations:
      - url: https://digital-atlas.s3.amazonaws.com/domain=climate/type=flood/source=jrc-glofas/region=east-africa/processing=return-period/variable=flood-depth/
        title: HTTPS
      - url: s3://digital-atlas/domain=climate/type=flood/source=jrc-glofas/region=east-africa/processing=return-period/variable=flood-depth/
        title: S3
    href_template: "rp={return_period}/flood-depth_rp{return_period}.tif"
    media_type: image/tiff; application=geotiff; profile=cloud-optimized
    file_size: 28 MB
    processing_steps: [source, crop-cog]
```

- `href_template` is **relative**, appended to each `locations[].url`; every `{token}` must be an
  enumerated dimension (§1). Values are substituted verbatim and **every combination is assumed to
  exist**, so jagged edges must be called out in `note` (CHIRPS NDJ/DJF edge windows; WRSI only
  cropland-MAM has 2026; GFM NDJ/DJF stop short).
- Media types in use: COG `image/tiff; application=geotiff; profile=cloud-optimized`;
  GeoJSON `application/geo+json`; Parquet `application/vnd.apache.parquet`.
- Path grammar (matches `R/observational/6_publish_obs_to_s3.R` tiers 1–18):
  `domain={climate|exposure|boundaries}/type=.../source=.../region=.../processing=.../variable=...`.

**Upstream source / creator — four different fields, do not conflate:**

| What | Field |
|---|---|
| Upstream **data** (landing page, DOI, Zenodo/HDX/JRC catalogue) | `processing[].derived_from[{title, url}]` |
| Upstream **code** (ours) | `processing[].code{url, version}` — `version` carries the script path |
| The **creator/licensor organisation** | `contact[]` with `roles: [licensor, producer]`; we are `roles: [processor, point-of-contact]` |
| The **paper** to cite | `citation{title, authors, date, publisher, url}` + `related_publications[{doi}]` |
| Licence text / data policy | `additional_links[{rel: license, url}]` (**mandatory** for `LicenseRef-*`) |
| Another CDH record | reference it by `id` in prose (`"record kenya-flood-gfm"`) — that is the current convention |

Licence rules Brayden set: a clip/subset **keeps** the source licence (OSM clip → `ODbL-1.0`); a
derived aggregate becomes `CC-BY-4.0` with inputs attributed in `note` (why
`kenya-flood-exposure-intersect` moved ODbL-1.0 → CC-BY-4.0). An aggregation of a source (seasonal
CHIRPS) is a **separate dataset** with `derived_from`, not a child — which is why
`africa-precipitation-chirps` was renamed `africa-precipitation-monthly-seasonal`.

---

## 4. Notebook integration — schema side only

**Held, not designed here:** whether a dataset name opens `openMethodDrawer`, and how Section 6/7
`tab-methods` links in, are notebook-repo decisions under issue #48 and belong to that session. I am
not specifying or wiring them. What I can give is the field split, because it follows from the schema:

**Always visible (one glance, before trusting a number):**
`title`, `description`, `license` (+ the `rel: license` link), attribution string from `note` where
one is required (Copernicus EMS, KNBS), `contact[]` producer/licensor, `spatial.resolution`,
temporal coverage, `citation`.

**Visible and never collapsed — the caveats:** `note`, and
`cdh.usage.not_recommended_for[{use, reason, use_instead}]`. These are the "Blank ≠ Zero", SAR-gap,
season-label and admin-universe traps. Collapsing them defeats the point of authoring them.

**Collapsible (deep technical review):** `processing[]` with `code`/`derived_from`,
`dimensions[]` with enumerated `values` (some are 500+ lines — the precipitation record's month axis
alone runs lines 102–714), full `variables[]` with units and dtypes, `joins`, `data[]` with
`href_template` / `file_size` / `media_type`, `related_publications`, `created`/`updated`,
`cdh_schema_version`.

**Rendering source of truth:** the record YAML, not a hand-maintained notebook table. Records are
one file per dataset at `records/<id>/<id>.yaml` in `cdh-catalog` once merged; until then the
authoritative copies are `hazards_prototype/metadata/cdh/<id>.yaml`. Whether the notebook vendors a
copy or fetches from the catalog is the notebook session's call — but a third hand-written copy of
this metadata inside the notebook will drift and should not be created.

---

## 5. Record inventory, mapped to the 16 datasets in the ask

All paths relative to `AdaptationAtlas/hazards_prototype`. Every record below is **v0.3.0, validated
`ok`, prettier-clean** (re-verified 2026-09-18).

| Ask | Record | File | cdh.domain | Licence | cdh-catalog PR |
|---|---|---|---|---|---|
| CHIRPS v3 | `africa-precipitation-monthly-seasonal` | `metadata/cdh/africa-precipitation-monthly-seasonal.yaml` | climate | CC-BY-4.0 | [#30](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/30) |
| SPEI-3 | `africa-spei-drought` | `metadata/cdh/africa-spei-drought.yaml` | climate | CC-BY-4.0 | [#31](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/31) |
| JRC GloFAS 10–500 yr | `eastafrica-flood-jrc` | `metadata/cdh/eastafrica-flood-jrc.yaml` | hydrology | CC-BY-4.0 | [#32](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/32) |
| MODIS MOD13Q1 NDVI | `eastafrica-ndvi-modis` | `metadata/cdh/eastafrica-ndvi-modis.yaml` | agricultural-production, climate | CC-BY-4.0 | [#33](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/33) |
| Copernicus GFM S1 SAR | `kenya-flood-gfm` | `metadata/cdh/kenya-flood-gfm.yaml` | hydrology | LicenseRef-Copernicus-EMS | [#38](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/38) *(draft)* |
| Subcounty flood × asset intersect | `kenya-flood-exposure-intersect` | `metadata/cdh/kenya-flood-exposure-intersect.yaml` | socioeconomic, hydrology | CC-BY-4.0 | [#37](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/37) *(draft)* |
| …its KNBS-2019 population level | `kenya-population-knbs-census` | `metadata/cdh/kenya-population-knbs-census.yaml` | socioeconomic | LicenseRef-KNBS-Open-License | not yet submitted |

Authored but **not** in the ask — the notebook may well be using them, so they are drawer-ready too:

| Record | File | Licence | PR |
|---|---|---|---|
| `eastafrica-wrsi-fews` (WRSI cropland + rangeland) | `metadata/cdh/eastafrica-wrsi-fews.yaml` | CC0-1.0 | [#34](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/34) |
| `kenya-admin-codab` (IEBC adm1/adm2) | `metadata/cdh/kenya-admin-codab.yaml` | CC-BY-3.0-IGO | [#35](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/35) |
| `kenya-facilities-hotosm` (health + schools) | `metadata/cdh/kenya-facilities-hotosm.yaml` | ODbL-1.0 | [#36](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/36) |
| `kenya-population-grid3` | `metadata/cdh/kenya-population-grid3.yaml` | CC-BY-4.0 | [#39](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/39) |
| `kenya-population-worldpop` | `metadata/cdh/kenya-population-worldpop.yaml` | CC-BY-4.0 | [#40](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/40) |
| `kenya-power-grid-kplc` | `metadata/cdh/kenya-power-grid-kplc.yaml` | CC0-1.0 | [#41](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/41) |
| `kenya-roads-osm` | `metadata/cdh/kenya-roads-osm.yaml` | ODbL-1.0 | [#42](https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/42) |
| `kenya-population-knbs-projections` | `metadata/cdh/kenya-population-knbs-projections.yaml` | LicenseRef-KNBS-Open-License | not yet submitted |

Also present: `metadata/cdh/ensemble_season_trends.cdh.yaml` — a **v0.0.1 draft** for the CR-119
trends dataset, not part of this set, not upgraded, still carries `license: ""` and `citation: ""`
TODOs. Do not surface it in the notebook.

Manifest with the full change log and validate commands: `metadata/cdh/README.md`.

---

## 6. The 10 datasets with no record — and why a template is not the blocker

I am not shipping stub YAML for these, because a CDH record **requires `data[]` with at least one
`locations[].url`**. A record for something we neither publish nor own would be a fabricated asset
entry. Each row below needs a decision first.

**Group A — ENSO/IOD driver indices: staged outside this pipeline.**
`NOAA CPC RONI`, `HadISST DMI`, `Funk Western-V`, `ENSO+IOD composite`. No ingest or publish path
exists in `hazards_prototype` (confirmed: nothing matching `roni`, `driver_indices`, `western` in
`python/` or `R/observational/`; the builder is `_sources/enso_drivers_build.py` in the **notebook**
repo, and `driver_indices.parquet` is externally staged). Two options, Pete's call:
(a) move the fetch into the pipeline, publish to `digital-atlas`, then author 1–2 records (the
composite is arguably a `derived_from` of the two index records, not its own dataset); or
(b) leave the bytes where they are and author a **federated** CDH record pointing at the provider.
**DECIDED 2026-09-18 (Pete): federation is allowed, so (b) is the route** — see §2c of the companion
handover for the exact record shape. Note the live gaps from
issue #48 either way: `dmi_conc`/`dmi_pred` are NULL for all of 2025, and RONI is stale at AMJ 2026
while CPC also revises past values.

**Group B — published by the Atlas, but no CDH record yet.**
`MapSPAM / GLW4 Value of Production`. Real, published, and the calculation model is documented
(FAOStat GPV distributed by production/head share; `glw_prop` cancels head-scale; GLW4-2020 `_Da` =
density), but it reaches S3 through the `hazard_exposure` chain, not the observational tiers. It
deserves a record; authoring it is a separate piece of work with a live caveat attached (the
crop-vs-livestock VoP currency mismatch: live artifact 2026-09-18 has crop PASS 1.007, livestock
**FAIL 1.198**). **Do not put VoP in the drawer as settled until that is resolved.**

**Group C — not in the pipeline at all; upstream-cite only for now.**
`KNBS NAPR (crops & livestock)` — only a one-off parse exists
(`scripts/2026-09-17_parse_knbs_maize_panel.py`, maize panel; and the season→year convention behind
it is a single national rule known to be wrong for Tana River / Meru / Kitui / Makueni, 6 years
only, moderate confidence). `HarvestStat`, `NDMA / FEWS NET market prices & terms of trade`,
`KFSSG / IPC phases`, `KNBS GESI` — **no ingest, no publish path, no record** (greps for
`harveststat`, `ndma`, `kfssg`, `gesi`, `terms of trade` return nothing outside `archive/`). If the
notebook reads them, it is reading externally staged files, and the notebook should cite the
upstream provider directly. Authoring CDH records for them means first deciding whether the Atlas
publishes them.

---

## 7. Changed in this repo this session

Two records were **failing the real validator** and had never been run through it (the manifest said
so: *"parse but have not been run through the Node validator yet"*). Both now pass.

`metadata/cdh/kenya-population-knbs-census.yaml`, `metadata/cdh/kenya-population-knbs-projections.yaml`:

1. Removed the hand-rolled top-level **`licence_note:`** key — rejected by
   `unevaluatedProperties: false`. Its full text is preserved, folded into `note` under a
   `LICENCE EVIDENCE.` lead paragraph. No information lost.
2. Added the **`additional_links[{rel: license}]`** that `license: LicenseRef-*` requires, pointing
   at the KNBS Open License Agreement (Open Data Platform terms of use,
   `https://kenya.opendataforafrica.org/gdlkmgb`), with the census record also linking KNBS's own
   HDX release (Public Domain / No restrictions) as corroboration.

Caveat on that URL: two independent searches attribute the KNBS Open License Agreement text to
`kenya.opendataforafrica.org/gdlkmgb`, but **the page 403s to automated fetches, so I could not read
the licence text directly**. Worth one human eyeball before these two records are submitted. The
projections record's link description states plainly that coverage of **Volume XVI is unconfirmed** —
the agreement is scoped to the Open Data Platform, and Vol XVI is a PDF on `knbs.or.ke` with no
licensed mirror (tracked: hazards_prototype issue #33).

Verification (all 15, 2026-09-18): `validate-yaml.js --profile cdh.schema.json` → **15 × `ok`**;
`prettier@3 --check` → **"All matched files use Prettier code style!"**.

Not done, deliberately: these two records are still **not submitted** to `cdh-catalog` (the other 13
are #30–#42). Submitting is outward-facing and needs Pete's go, plus the licence eyeball above.

---

## 8. Heads-up: a second metadata system is being built in this repo right now

Committed by another session as `c497739` ("feat(catalogue): dataset index") while this handover was
being written, and left alone: `R/checks/73_catalogue.R` + `metadata/catalogue/*.json` (30 records +
`_SCHEMA.md`) — a per-dataset JSON **project catalogue** for issue #29
item 2, answering "what datasets does this project have, where do they live, are they on Atlas S3,
**do they have CDH metadata**, how would they get onto another host". It already has an `--orphans`
mode that cross-checks against CDH, and a `--render` that rewrites `docs/DATA_INDEX.md`.

Different purpose from CDH (internal host/transfer inventory vs outward-facing published-dataset
metadata), overlapping content. Before the notebook wires anything, agree which of the two it reads —
`metadata/cdh/` is the outward-facing standard and the right source for a provenance drawer; the
catalogue is an internal inventory. Do not let a third copy appear inside the notebook.
