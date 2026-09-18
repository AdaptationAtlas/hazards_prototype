# Dataset catalogue — record schema

One JSON file per dataset, `metadata/catalogue/<id>.json`. One file per dataset on
purpose: `git log -- metadata/catalogue/<id>.json` is then the change history for that
dataset, and diffs stay readable when several people edit different datasets.

This is the **go-to place**. `R/checks/73_catalogue.R` reads these records, joins them to
the rest of the metadata, probes the live state, and renders `docs/DATA_INDEX.md`.

## How it joins to everything else

| field | joins to | meaning |
|---|---|---|
| `location.key` | `metadata/hosts.json` via `atlas_dir()` | where it sits on *this* host |
| `produced_by` / `consumed_by` | `metadata/stages.json` `id` | which pipeline stage writes / reads it |
| `cdh_record` | `metadata/cdh/<id>.yaml` `id` | the published CDH v0.3.0 metadata record |
| `atlas_s3.prefix` | `s3://digital-atlas/...` | the published copy, if there is one |

The checker reports **orphans in both directions** — a CDH record with no catalogue entry,
a stage input no record covers, a record pointing at a CDH file that does not exist. Gaps
are meant to be visible, not silently absent.

## Fields

```jsonc
{
  "schema_version": "1.0.0",
  "id":          "kebab-case, matches the filename",
  "title":       "human name",
  "description": "what it is, in a sentence or two",

  "class":       "external-raw | derived | published",
  "version":     "vintage or release, e.g. '2020V1r2', 'v1.1', '0.4.1'",
  "status":      "current | superseded | deprecated | unverified",
  "supersedes":    ["<id>"],
  "superseded_by": "<id> | null",

  "origin": {
    "type":  "public-s3 | public-http | dataverse | computed | restricted | unknown",
    "url":   "where it ultimately comes from",
    "checksum_manifest": "URL of a published checksum list, or null",
    "license": "SPDX id or short name, or null"
  },

  // The answer to "how does this get onto another host".
  "transfer": {
    "strategy": "regenerate | pull-from-origin | must-transfer",
    "rationale": "why that strategy",
    "approx_size": "human readable, or null if unmeasured",
    "regenerate_with": "the command or stage that rebuilds it, when strategy=regenerate"
  },

  "location":   { "key": "<atlas_dir key>", "path": "<working_dir-relative>",
                  "subpath": null, "glob": "*.tif", "recursive": false },
  "produced_by": ["<stage id>"],
  "consumed_by": ["<stage id>"],

  "atlas_s3":   { "prefix": "s3://digital-atlas/...", "complete": true,
                  "note": "..." },          // or null if not published
  "cdh_record": "<cdh id> | null",

  "completeness": { "min_files": 1 },
  "gaps":       ["anything known to be unverified or missing"],
  "changelog":  [ { "date": "YYYY-MM-DD", "change": "...", "ref": "#29" } ]
}
```

## Transfer strategies

- **`regenerate`** — derived from a public origin; rebuild it on the target host rather
  than copying bytes. Default for anything computed from an open archive. This is why
  the monthly indices do not need a host-to-host link.
- **`pull-from-origin`** — each host fetches independently from S3 or a public URL. Most
  inputs are already like this; `R/0_server_setup.R` §3 does it today.
- **`must-transfer`** — genuinely irreproducible, or so expensive to recompute that
  copying wins. Keep this list short and justify every entry.
