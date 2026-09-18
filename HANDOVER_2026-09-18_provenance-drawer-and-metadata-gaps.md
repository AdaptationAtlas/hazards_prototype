# HANDOVER 2026-09-18 (2) — provenance drawer contract, #29 integration, and how to handle missing metadata

Companion to `HANDOVER_2026-09-18_cdh-standard-for-ke-enso-notebook.md` (the standard itself).
Audiences: the KE-ENSO notebook session (§1), issue #29 (§4). Federation decision (§2c) taken by
Pete on 2026-09-18 and already folded in.

---

## 0. Three answers up front

1. **The standard already has a mechanism for incomplete metadata.**
   `validate-yaml.js --draft` = "prune blank placeholders and relax presence rules". It strips
   `required` but **still** checks field names, types, enums and every cross-field rule, and prints
   `ok <file> (draft)`. So a partial record is a first-class, checkable object — we do not need a
   parallel invention. §2.
2. **Federation is approved (Pete, 2026-09-18): we can author CDH records for datasets we do not
   host.** `data[].locations` points at the **upstream** URL, `access:` states the condition, and
   there is no `digital-atlas` entry. So the five upstream datasets get *real* records, not
   held-back drafts. The one absolute rule survives: **never fabricate a `digital-atlas` location
   for something we have not published.** §2c.
3. **#29 is already the right registry and is further along than I assumed** — 30 records, a
   `cdh_record` join, two-way orphan checks. It needs 7 small changes, not a redesign, and one of
   them is a live false-green. §4.

---

## 1. Ask 4 — the notebook integration standard

Scope note: this is the **contract**, not the wiring. Implementation lives in `atlas_notebooks`.

### 1a. Source-of-truth rule (the only rule that really matters)

> The notebook renders provenance from a **generated projection**. It never hand-maintains dataset
> metadata, and it never becomes the third copy.

Today there are already two stores in `hazards_prototype` — `metadata/cdh/*.yaml` (outward-facing,
published, standardised) and `metadata/catalogue/*.json` (internal inventory, #29). A hand-written
table inside `notebook_v3.qmd` would be a third, and it will drift the first time a licence or a
caveat changes. One generated file, one direction.

### 1b. The projection — one shape the notebook renders regardless of metadata maturity

Emit `provenance.json` (array, one entry per dataset the notebook cites) from #29's renderer (§4 R6).
Every field below is derivable from a CDH record, a catalogue record, or both:

```jsonc
{
  "key":   "kenya-flood-gfm",             // notebook's stable handle; == cdh id where one exists
  "title": "Kenya Observed Flood Extent (Copernicus GFM, Sentinel-1)",
  "state":   "authored",                  // metadata maturity: authored | draft | catalogue-only
  "hosting": "atlas",                     // whose bytes: atlas | federated
  "summary": "<description, 1-2 sentences>",

  "licence":     { "id": "LicenseRef-Copernicus-EMS", "url": "<rel: license link>",
                   "attribution": "Contains modified Copernicus Emergency Management Service information [year]" },
  "producer":    [ { "organization": "Copernicus Emergency Management Service (CEMS)", "roles": ["licensor","producer"] } ],
  "citation":    { "text": "<citation rendered>", "url": "<doi or landing page>" },

  "caveats":     [ "<each paragraph of note>" ],          // NEVER collapsed, never truncated
  "avoid":       [ { "use": "...", "reason": "...", "use_instead": "..." } ],   // cdh.usage.not_recommended_for
  "intended_uses": [ "..." ],

  "coverage":    { "spatial": "Kenya, ~111 m", "temporal": "2018-01 to 2025-12" },
  "assets":      [ { "name": "monthly-flooded", "https": "...", "s3": "...",
                     "media_type": "...", "href_template": "..." } ],
  "upstream":    [ { "title": "Copernicus GFM (EODC STAC)", "url": "..." } ],   // processing[].derived_from
  "processing":  [ { "id": "source", "description": "...", "code": "python/ingest_flood_gfm.py" } ],
  "technical":   { "dimensions": [...], "variables": [...], "joins": [...] },   // verbatim passthrough

  "record":      { "cdh": "metadata/cdh/kenya-flood-gfm.yaml",
                   "catalogue": "metadata/catalogue/kenya-flood-gfm.json",
                   "catalog_pr": "https://github.com/CGIAR-Climate-Data-Hub/cdh-catalog/pull/38" },
  "gaps":        [ "<catalogue gaps[] + 'no CDH record' etc.>" ]
}
```

Two **orthogonal** axes, both rendered. Conflating them was wrong: a federated dataset can have
perfect metadata, and an Atlas-hosted one can have none.

`state` = how mature the metadata is:

| `state` | Means | Drawer shows |
|---|---|---|
| `authored` | valid v0.3.0 CDH record exists | full drawer, no badge needed |
| `draft` | CDH record exists but only passes `--draft` | badge **"provisional metadata"** + `gaps[]` |
| `catalogue-only` | in #29's catalogue, no CDH record | badge **"metadata pending"**, render what exists |

`hosting` = whose bytes the reader is about to fetch:

| `hosting` | Means | Drawer shows |
|---|---|---|
| `atlas` | published to `s3://digital-atlas/...` by us | Atlas asset links + `href_template` |
| `federated` | record is ours, **data stays with the provider** (§2c) | badge **"external source"**, provider's link, **no Atlas asset links**, and the provider's own access/refresh terms |

So CPC RONI is `state: authored` + `hosting: federated` once its record is written — fully
documented, and unambiguous that the bytes come from CPC.

A dataset with no entry at all must render as **"provenance not documented"** — never as a blank or
an absent drawer. An undocumented dataset the reader cannot see is worse than a visible gap.

### 1c. Visible vs collapsible

**Always visible (the trust decision):** `title`, `summary`, `state` badge, `licence` (+ attribution
string where one is mandatory — Copernicus EMS and KNBS both are), `producer`, `coverage`,
`citation`.

**Visible, never collapsed — the caveats:** `caveats` and `avoid`. These carry "Blank ≠ Zero"
(GFM `255 = not observed, NOT dry`), the SAR-gap and record-length limits, the NDJ/DJF season-label
mismatch between GFM and CHIRPS, and the KNBS 345-vs-290 admin-universe trap. They exist precisely
because a reader will otherwise misread the number on screen. Collapsing them defeats authoring them.

**Collapsible (deep technical review):** `technical` (dimensions with enumerated values — the
precipitation record's month axis alone is 600+ lines), `assets` with `href_template` / `media_type`,
`processing` with code refs, `upstream`, `record` links, `intended_uses`, schema version and dates.

### 1d. Drawer ↔ master catalog (Section 6/7 `tab-methods`)

Same projection, two views, no second dataset:

- **Drawer** (`openMethodDrawer(key)`) — one dataset, opened from a dataset name anywhere in the
  notebook. Deep-linkable (`#dataset=kenya-flood-gfm`) so a caveat can be cited in an email.
- **`tab-methods`** — the index: one row per entry, columns `title`, `state`, `licence`, `coverage`,
  `gaps`, each row opening the same drawer. It is the master catalog **rendered from the projection**,
  so a new dataset appears in both by adding one record upstream, and the "what's missing" column is
  automatic rather than remembered.

Link out to the published CDH record (or its `cdh-catalog` PR) from the drawer footer; that is the
citable, outward-facing artefact.

### 1e. What the notebook must not do

- Not hand-write licences, citations or caveats inline.
- Not show an Atlas `s3://digital-atlas/...` link for a `federated` dataset — send the reader to
  the provider.
- Not silently drop a dataset that has no metadata — render the gap.
- Not paraphrase `note`. Render it verbatim; the wording is load-bearing and, for Copernicus/KNBS,
  legally required.

---

## 2. Missing metadata — the temporary system

### 2a. Use the standard's draft mode, not a new format

```sh
# relax presence rules, prune blank placeholders, keep every other check
node /tmp/cdh-std/scripts/validate-yaml.js --draft \
  --profile /tmp/cdh-std/spec/schemas/profiles/cdh.schema.json metadata/cdh/draft/*.yaml
#   ok  metadata/cdh/draft/enso-driver-indices.yaml (draft)
```

Convention (no schema violation, because the profile's `unevaluatedProperties: false` forbids any
`draft:`/`provisional:` key — **the directory is the state**):

- `metadata/cdh/draft/<id>.yaml` — passes `--draft` only. Never submitted to `cdh-catalog`.
- `metadata/cdh/<id>.yaml` — passes strict. Submittable.
- Promotion = *the same file* passes without `--draft` → `git mv` up one level → one PR.
- CI/local check runs both: strict over `metadata/cdh/*.yaml`, `--draft` over `metadata/cdh/draft/*.yaml`.
  A draft that no longer parses is still a failure.

Start from `templates/full-standard.yaml` in the standard repo (files under `templates/` are
auto-drafted, so the template itself demonstrates the mode).

**Verified 2026-09-18, not assumed.** A realistic partial RONI record (id, title, description,
keywords, `note` with the real caveats, upstream `contact`, `processing[id: source].derived_from`,
`cdh.domain` — no `license`, no `data`, no `citation`) behaves exactly as needed:

```
--- STRICT ---
FAIL draft-probe.yaml
  /: must have required property 'citation'
  /: must have required property 'doi'
  /: must match a schema in anyOf
  /: must have required property 'license'
  /: must have required property 'data'
--- DRAFT ---
ok   draft-probe.yaml (draft)
```

So the promotion gate is real and self-enforcing: the strict run lists precisely what is still
missing, which doubles as the dataset's to-do list.

### 2b. Minimum honest draft

Six things, all of which we genuinely know for every dataset in §5, and none of which require us to
publish anything:

1. `id`, `title`, `description` — what it is.
2. `license` + the `rel: license` link (omit rather than guess; a wrong licence is worse than a gap).
3. `contact[]` with the upstream `roles: [licensor, producer]`.
4. `processing[]` with `id: source` and `derived_from[{title,url}]` — where it actually comes from.
5. `note` — the caveats. **Write these first.** They are the highest-value field and need no
   infrastructure.
6. `cdh.domain`.

Leave `data`, `spatial`, `temporal`, `dimensions`, `variables` blank until real. Draft mode prunes
them; strict mode will demand them at promotion, which is the point.

### 2c. Federated records — DECIDED 2026-09-18 (Pete): yes, we can federate

A dataset we cite but do not host gets a **normal, strict-passing CDH record** that points at the
provider. Nothing is held back, and nothing is faked.

**Verified template — this exact file passes the strict profile + cross-field checks and
`prettier --check` (2026-09-18).** Copy it; it is the pattern for all of §5.

```yaml
$schema: https://cgiar-climate-data-hub.github.io/cdh-metadata-standard/v0.3.0/schemas/profiles/cdh.schema.json
cdh_schema_version: v0.3.0
extensions:
  - https://cgiar-climate-data-hub.github.io/cdh-metadata-standard/v0.3.0/extensions/cdh/schema.json

id: enso-driver-roni
title: NOAA CPC Relative Oceanic Nino Index (RONI)
description: >
  The Nino 3.4 sea-surface-temperature anomaly expressed relative to the tropical-mean SST anomaly,
  published monthly by the NOAA Climate Prediction Center as overlapping three-month windows. A
  federated record: the Adaptation Atlas cites this index but does not host or redistribute it.
resource_type: dataset
license: CC0-1.0 # work of the US Government, public domain
keywords:
  [enso, roni, sea surface temperature, climate index, seasonal forecasting]

note: >
  CPC REVISES PAST VALUES, so a refetch changes history - pin and record the fetch date with any
  analysis. RONI differs from the Nino 3.4 anomaly by up to 0.567 degC on a time-trending gap
  (about +0.20 degC in 1981-1995 falling to about -0.30 degC in 2015-2025), so mixing RONI and
  Nino 3.4 in one analogue-season comparison biases matches toward older years. Values are
  three-month means labelled by their centre month.

access: public

contact:
  - organization: NOAA Climate Prediction Center
    url: https://www.cpc.ncep.noaa.gov/
    roles: [licensor, producer]
  - name: Peter Steward
    organization: Alliance of Bioversity International and CIAT
    email: p.steward@cgiar.org
    roles: [point-of-contact]

citation:
  title: Relative Oceanic Nino Index (RONI)
  authors:
    - "NOAA Climate Prediction Center"
  date: "2026"
  publisher: NOAA National Weather Service, Climate Prediction Center
  url: https://www.cpc.ncep.noaa.gov/data/indices/RONI.ascii.txt

created: "2026-09-18"
updated: "2026-09-18"

temporal:
  start_date: "1950-01"
  end_date: null

processing:
  - id: source
    description: >
      CPC computes the Nino 3.4 SST anomaly and subtracts the tropical-mean (20S-20N) SST anomaly,
      publishing overlapping three-month means as a fixed-width ASCII table.
    derived_from:
      - title: NOAA CPC RONI
        url: https://www.cpc.ncep.noaa.gov/data/indices/RONI.ascii.txt

data:
  - name: roni-ascii
    description: Fixed-width ASCII table, one row per overlapping three-month window.
    locations:
      - url: https://www.cpc.ncep.noaa.gov/data/indices/RONI.ascii.txt
        title: HTTPS
    media_type: text/plain
    processing_steps: [source]

cdh:
  domain: [climate]
  usage:
    intended_uses:
      - Selecting analogue seasons for Kenyan seasonal outlooks in the KE-ENSO Explorer.
    not_recommended_for:
      - use: Mixing RONI with the raw Nino 3.4 anomaly in one time series or distance metric.
        reason: >
          The two differ by up to 0.567 degC and the gap trends over time, so a mixed series biases
          analogue matching toward older years.
        use_instead: One index end to end, RONI throughout.
```

Rules for a federated record:

1. **No `digital-atlas` location.** The absolute rule. If we later publish a processed copy, that is
   a **separate** record with `derived_from` pointing at the federated one — the same call Brayden
   made for seasonal CHIRPS vs `chirps-v3-daily` (an aggregation is a new dataset, not a child).
2. **Omit `series`.** `Africa Agriculture Adaptation Atlas` marks *our* products.
3. **Roles are honest:** provider = `licensor` / `producer`; we are `point-of-contact` for the
   record. Not `processor` (we processed nothing), not `custodian` (we hold nothing).
4. **`access` + `access_note` do real work.** `restricted` = discoverable but needs a request or
   authentication; `non-public` = catalogued but not obtainable through public channels. Both need
   `access_note` saying how to get it.
5. **Volatility belongs in `note`.** A federated record describes a moving target. CPC revises past
   RONI values; HadISST reissues; IPC re-analyses. Say so, or a reader assumes it is frozen.
6. **Drafts are still the on-ramp** — a federated record we have not finished is a draft like any
   other (§2a). Federation removes the *blocker*, not the need to do the work.

Still worth one line to Brayden, but as a **notification, not a question**: we intend to submit
federated records, and if `cdh-catalog` wants them segregated (a directory, a keyword, a
`resource_type` convention) that is his call on layout, not on permission.

---

## 3. Longer term

1. **Two stores, one join, explicit owners.** #29's catalogue = the registry (every dataset, every
   host, transfer strategy, stage wiring, status). CDH = the published, outward-facing record. Join
   stays `catalogue.cdh_record → cdh.id`. See the ownership table in §4 R7 — the failure mode to
   avoid is both stores holding a licence string.
2. **Generate, never duplicate.** The notebook projection (§1b), `docs/DATA_INDEX.md` and the
   drawer all come out of one renderer. Adding a dataset = one catalogue record (+ a CDH record when
   it is published). Nothing is typed twice.
3. **Make gaps fail, not just show.** `73_catalogue.R --gaps` is the right probe; wire it into the
   pre-publish checks so "published to S3 with no CDH record" blocks a release rather than being
   noted. Today the backlog is 6 published datasets with `cdh_record: null` (§4 R3).
4. **Close the machine-readable half.** The pipeline already emits `<file>.parquet.json` /
   `_metadata.json` sidecars (SPEI method, ensemble membership, build provenance) that no record
   declares. Register them as `additional_assets[{roles: [metadata]}]` — the standard's own sidecar
   slot. One-time edit per record, then the drawer can link real build provenance.
5. **Only then consider an extension.** If, after §2 and §4, there is still a structured field CDH
   has no home for, propose it to `cdh-metadata-standard` as a versioned extension
   (`spec/extending.md`: one top-level key, pinned `$id`, declared in `extensions[]`). Shared fields
   should become shared extensions — Brayden says so explicitly. Do not add local keys; they fail CI.

---

## 4. Recommendations to #29

Read-only review of `R/checks/73_catalogue.R` (277 lines) + `metadata/catalogue/` (30 records,
`_SCHEMA.md`), as committed in `c497739`. I changed nothing — it is another session's live work.

**R1 — `cdh_exists()` is a file-existence probe, and it is producing a false green.** `73_catalogue.R:85`
tests only `file.exists(metadata/cdh/<id>.{yaml,cdh.yaml})`, so `--status` prints `cdh: yes` for a
record that does not conform. Live case: `timeseries-mean-month.json` claims `ensemble_season_trends`,
which is a **v0.0.1 draft** carrying `license: ""` and `citation: ""` TODOs and was never upgraded to
v0.3.0. Reported as `yes`. Fix: probe `cdh_schema_version` and, where Node is available, validity —
distinguish `valid` / `draft` / `stale-version` / `missing`.

**R2 — derive a `cdh_state` column** (`authored-valid | draft | none`) rather than a boolean. It is
the same enum the notebook drawer needs (§1b), so one definition serves both.

**R3 — rank the gaps: "published to S3 but no CDH record" is the real backlog.** Six records have
`atlas_s3` set and `cdh_record: null` — `atlas-pop-worldpop`, `boundaries-gaul2024`, `glw4-2015`,
`hazard-exposure`, `mapspam-2020v1r2`, `nexgddp-indices-monthly`. Published Atlas data with no
outward-facing metadata is a more serious gap than an unpublished intermediate, and `--gaps` should
say so. (Good news: **no CDH orphans** — all 16 CDH ids are claimed by a catalogue record.)

**R4 — add the four fields a notebook needs that the catalogue lacks.** `_SCHEMA.md` has
`origin.license` and `origin.url` but no `citation`, no `attribution` string, no caveats, no
`access`. For a `catalogue-only` dataset the catalogue is the *only* store, so the
drawer has nothing to render. Either add them, or add `cdh_draft: "<path>"` pointing at a
`metadata/cdh/draft/<id>.yaml` and let CDH own those fields even before publication. **Prefer the
second** — it keeps one owner per field and gives the draft a promotion path.

**R5 — `class` needs a fourth value: `referenced`.** Current enum is
`external-raw | derived | published`, and `location.key`/`path` assume the bytes are on this host. A
dataset the Atlas *cites* but neither holds nor publishes (CPC RONI, HadISST DMI, HarvestStat,
NDMA/FEWS prices, KFSSG/IPC, KNBS GESI) cannot get an honest row today, so it stays invisible to the
catalogue — while being visible to notebook readers. Add `referenced` with `location: null` and
`transfer.strategy: pull-from-origin`.

**Now sharper, given the federation decision (§2c):** such a dataset will have `cdh_record` set
(a real, strict-passing record) while `atlas_s3` and `location` are both `null` — a combination the
current schema reads as two gaps. `--gaps` **must not flag a federated row** for having no local
copy and no S3 prefix; that is its correct steady state, not a deficiency. Suggest deriving
`hosting = atlas | federated` from `atlas_s3 != null` and exempting `federated` from those two gap
checks (the notebook needs the same flag — §1b).

**R6 — emit a notebook-facing projection from `--render`.** It already rewrites
`docs/DATA_INDEX.md`; add `metadata/catalogue/_projection.json` in the §1b shape, joining catalogue +
CDH. That single file is what `atlas_notebooks` consumes, which is what stops the third copy from
appearing. Keep it generated and gitignored-or-committed by preference, but never hand-edited.

**R7 — one owner per field.** The two stores overlap on `title`, `description`, `version`,
`origin.license`. Proposed rule:

| Field group | Owner | Note |
|---|---|---|
| licence, attribution, citation, caveats, intended/not-recommended uses, spatial, temporal, dimensions, variables, assets, processing | **CDH record** | authoritative; catalogue references by id |
| host location, transfer strategy, produced_by / consumed_by, status, supersedes, completeness, gaps, changelog | **catalogue** | CDH has no concept of these |
| title, description | catalogue **as fallback** | when `cdh_record` is set, CDH wins for display |
| `origin.license` | drop or mark derived | duplicating a licence in two stores is how they disagree |

**R8 — keep the per-file layout and `changelog[{date,change,ref}]`.** `git log -- metadata/catalogue/<id>.json`
as a dataset's history is the right call, and the `gaps[]` free-text is already carrying real
knowledge (`crop-vop-intld15`'s currency-mismatch warning is exactly the kind of thing that must not
live only in someone's memory). Do not normalise that away.

---

## 5. Triage of the 10 datasets with no CDH record

| Dataset | State now | This week | Longer term |
|---|---|---|---|
| CPC RONI | `federated`; fetched by the notebook's `_sources/enso_drivers_build.py`, nothing in this repo | **full federated record** (§2c), caveats first: stale at AMJ 2026, and **CPC revises past values** so a refetch changes history | if we later publish a processed copy, that is a SEPARATE record with `derived_from` → this one |
| HadISST DMI | `federated` | full federated record; hard caveat: **`dmi_conc`/`dmi_pred` NULL for all of 2025**, both seasons — 2025 is unusable as an analogue year and a z-scored distance over a NULL axis fails silently | as RONI |
| Funk Western-V | `federated` | full federated record; state the definition and the source explicitly — least standard index in the set | as RONI |
| ENSO+IOD composite | ours, derived, unpublished | draft, `derived_from` the two index records | likely **not** its own dataset — a derived view of RONI+DMI; decide before authoring |
| MapSPAM / GLW4 Value of Production | published via `hazard_exposure`; `crop-vop-intld15`, `livestock-vop`, `glw4-*`, `mapspam-2020v1r2` all `cdh_record: null` | **do not drawer as settled** — live artifact 2026-09-18 has crop PASS 1.007, livestock **FAIL 1.198** | author records after the currency mismatch is resolved; R3 priority |
| KNBS NAPR (crops & livestock) | one-off parse only (`scripts/2026-09-17_parse_knbs_maize_panel.py`, maize) | federated record for the KNBS source + the season→year caveat: one national rule (Y = OND(Y-1) + MAM(Y)), known wrong for Tana River / Meru / Kitui / Makueni, 6 years, moderate confidence | if the Atlas publishes the parsed panel, that is a second, derived record |
| HarvestStat | `federated`, no ingest | full federated record: caveats + citation + `access` | — |
| NDMA / FEWS NET prices & terms of trade | `federated`, no ingest | full federated record; `access` matters here — NDMA bulletins are not a clean public API | — |
| KFSSG / IPC phases | `federated`, no ingest | full federated record; caveat that IPC phases are a **classification, not a measurement** and are not comparable across analysis rounds | — |
| KNBS GESI | `federated`, no ingest | full federated record; licence needs the same KNBS scrutiny as the census/projections records | resolve with issue #33 |

Cheapest useful next step for the notebook: **write the `note` blocks** for all ten as drafts. They
need no publishing decision, no S3, and no #29 change, and they are the fields the drawer must never
collapse.

---

## 6. Changed / not changed

Changed here: this file only. `R/checks/73_catalogue.R` and `metadata/catalogue/*` are another
session's work (committed `c497739`) and were read, not touched. Nothing submitted to `cdh-catalog`.
No draft records authored yet — §5 is the work queue, and the ten `note` blocks are the first item.
