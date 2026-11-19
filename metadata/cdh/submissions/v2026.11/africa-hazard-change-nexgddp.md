<!-- Pete: fill in AI use and tick the review box before opening. The open loop in README.md refuses to post until both are done. -->

- **Record ID:** `africa-hazard-change-nexgddp`
- **Description:** Adds `africa-hazard-change-nexgddp`: annual hazard frequency, heat-stress % area (crop and livestock), and rainfall change, per administrative unit and 20-year window, by model and as an ensemble. Ten tables with different columns, so ten structures. The rebuilt set went live 2026-11-16 (duplicate rows removed; the old inflated country values for disputed-area claimants were among them, checked against an independent recomputation).
- **AI use:** TODO: written by AI / written with the help of AI / none

## Checklist

- [ ] Full record has been reviewed and approved by the human author
- [x] `cdh_schema_version` is the latest release of the standard (v0.5.0)
- [x] Links resolve (checked on 2026-11-19 with this catalog's link-check rules)
- [x] Title, description, and intended uses are generic: no product or project names

## Known gaps / notes

- `derived_from` gives NEX-GDDP-CMIP6 as version `1.0-1.2`. Every download script used for these files (AdaptationAtlas/hazards history, February to September 2025) took the newest version-1 revision of each file (v1.2, else v1.1, else the unsuffixed v1.0) and never looked for the v2.0 files NASA added in July 2025. Which revision each single file was is not logged.
- `code.version` on each processing step is a commit whose copy of the script is identical to the one the processing server ran: checked against the server commit recorded in each build and publish log.

**Part of a set of ten records** (one PR each), all on schema v0.5.0 and all `version: v2026.11`, with no `previous_version`: none of them had a Hub record before (standard §4.7). Every object in the version is pinned by S3 versionId in [`atlas-hazards-2026.11_s3-versions.csv`](https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/metadata/cdh/pins/atlas-hazards-2026.11_s3-versions.csv).

**Hub verify script** (`cdh-metadata-verify` from `CGIAR-Climate-Data-Hub/skills` at `84cbf5b`) was run on all ten before opening. The HIGH findings it still reports are the first-year `timeframe` labels that standard v0.5.0 requires, and axes the files are split along (`period`, `severity`, `scenario`, `baseline_name`), whose values were checked across every file. Triage: [`verify-triage.md`](https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/metadata/cdh/submissions/v2026.11/verify-triage.md).

**Version history:** each record links its change notes with `rel: version-history`. They are kept [in the hazards_prototype repo, beside the data pipeline](https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/metadata/cdh/changelogs/README.md), as agreed on 2026-11-17.

**Merge order:** `africa-exposure-combined` and `africa-hazard-season-timeseries-nexgddp` first; the others derive from them by id.

Background and the remaining open questions: [catalog map briefing](https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/HANDOVER_2026-11-13_catalog-map.md#2-what-i-need-from-you).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
