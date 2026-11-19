<!-- Pete: fill in AI use and tick the review box before opening. The open loop in README.md refuses to post until both are done. -->

- **Record ID:** `africa-exposure-combined`
- **Description:** Adds `africa-exposure-combined`: crop and livestock exposure per administrative unit for Sub-Saharan Africa (MapSPAM 2020 crops, GLW4 2020 livestock; production, harvested area, value of production in two currencies, head counts). **This is the merge you asked for**: the old res-05 and res-25 drafts are one record with a structure per resolution (0.05° and 0.25°). Nine files: the combined table and the two single-currency extracts, each at both resolutions plus the legacy unsuffixed alias.
- **AI use:** TODO: written by AI / written with the help of AI / none

## Checklist

- [ ] Full record has been reviewed and approved by the human author
- [x] `cdh_schema_version` is the latest release of the standard (v0.5.0)
- [x] Links resolve (checked on 2026-11-19 with this catalog's link-check rules)
- [x] Title, description, and intended uses are generic: no product or project names

## Known gaps / notes

**Merge this one first** — the five hazard × exposure records derive from it by id.

- The GAUL code columns (`gaul0_code`, `gaul1_code`, `gaul2_code`) are stored as `float64` in the files, and the record declares them as `float64`. The standard's examples use string keys; a later version can cast them.
- The GAUL 2024 boundary record is not in the catalog yet, so `foreign_keys` point at the boundary Parquet file by URL.
- FAOSTAT publishes no release labels, so `derived_from` gives no version; its title records the bulk download of 2026-05-14 that every value build used (the processing server's copy was checked md5-identical to it on 2026-10-04).
- `code.version` on each processing step is a commit whose copy of the script is identical to the one the processing server ran: checked against the server commit recorded in each build and publish log.

**Part of a set of ten records** (one PR each), all on schema v0.5.0 and all `version: v2026.11`, with no `previous_version`: none of them had a Hub record before (standard §4.7). Every object in the version is pinned by S3 versionId in [`atlas-hazards-2026.11_s3-versions.csv`](https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/metadata/cdh/pins/atlas-hazards-2026.11_s3-versions.csv).

**Hub verify script** (`cdh-metadata-verify` from `CGIAR-Climate-Data-Hub/skills` at `84cbf5b`) was run on all ten before opening. The HIGH findings it still reports are the first-year `timeframe` labels that standard v0.5.0 requires, and axes the files are split along (`period`, `severity`, `scenario`, `baseline_name`), whose values were checked across every file. Triage: [`verify-triage.md`](https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/metadata/cdh/submissions/v2026.11/verify-triage.md).

**Version history:** each record links its change notes with `rel: version-history`. They are kept [in the hazards_prototype repo, beside the data pipeline](https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/metadata/cdh/changelogs/README.md), as agreed on 2026-11-17.

**Merge order:** `africa-exposure-combined` and `africa-hazard-season-timeseries-nexgddp` first; the others derive from them by id.

Background and the remaining open questions: [catalog map briefing](https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/HANDOVER_2026-11-13_catalog-map.md#2-what-i-need-from-you).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
