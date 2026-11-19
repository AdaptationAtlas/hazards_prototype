<!-- Pete: fill in AI use and tick the review box before opening. The open loop in README.md refuses to post until both are done. -->

- **Record ID:** `africa-hazard-exposure-livestock-numbers`
- **Description:** Adds `africa-hazard-exposure-livestock-numbers`: the hazard × exposure tables measured in livestock head, per species and system.
- **AI use:** TODO: written by AI / written with the help of AI / none

## Checklist

- [ ] Full record has been reviewed and approved by the human author
- [x] `cdh_schema_version` is the latest release of the standard (v0.5.0)
- [x] Links resolve (checked on 2026-11-19 with this catalog's link-check rules)
- [x] Title, description, and intended uses are generic: no product or project names

## Known gaps / notes

Derives from `africa-exposure-combined` — merge that first.

- The GAUL code columns (`gaul0_code`, `gaul1_code`, `gaul2_code`) are stored as `float64` in the files, and the record declares them as `float64`. The standard's examples use string keys; a later version can cast them.
- The GAUL 2024 boundary record is not in the catalog yet, so `foreign_keys` point at the boundary Parquet file by URL.
- These tables were built on 2026-10-19 from the exposure table as it stood on 2026-10-08. The area, production and head-count rows they use are unchanged in v2026.11 (later changes touched only livestock prices and the labels of the TLU total rows), so `derived_from` cites `africa-exposure-combined` v2026.11.
- The hazard side comes from the same NEX-GDDP-CMIP6 version-1 files as `africa-hazard-change-nexgddp` (newest of v1.2, v1.1, v1.0 per file; not the 2025 v2.0 files).
- `code.version` on each processing step is a commit whose copy of the script is identical to the one the processing server ran: checked against the server commit recorded in each build and publish log.

**Part of a set of ten records** (one PR each), all on schema v0.5.0 and all `version: v2026.11`, with no `previous_version`: none of them had a Hub record before (standard §4.7). Every object in the version is pinned by S3 versionId in [`atlas-hazards-2026.11_s3-versions.csv`](https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/metadata/cdh/pins/atlas-hazards-2026.11_s3-versions.csv).

**Hub verify script** (`cdh-metadata-verify` from `CGIAR-Climate-Data-Hub/skills` at `84cbf5b`) was run on all ten before opening. The HIGH findings it still reports are the first-year `timeframe` labels that standard v0.5.0 requires, and axes the files are split along (`period`, `severity`, `scenario`, `baseline_name`), whose values were checked across every file. Triage: [`verify-triage.md`](https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/metadata/cdh/submissions/v2026.11/verify-triage.md).

**Version history:** each record links its change notes with `rel: version-history`. They are kept [in the hazards_prototype repo, beside the data pipeline](https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/metadata/cdh/changelogs/README.md), as agreed on 2026-11-17.

**Merge order:** `africa-exposure-combined` and `africa-hazard-season-timeseries-nexgddp` first; the others derive from them by id.

Background and the remaining open questions: [catalog map briefing](https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/HANDOVER_2026-11-13_catalog-map.md#2-what-i-need-from-you).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
