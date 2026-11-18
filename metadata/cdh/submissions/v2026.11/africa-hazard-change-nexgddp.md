Adds `africa-hazard-change-nexgddp`: annual hazard frequency, heat-stress % area (crop and livestock), and rainfall change, per administrative unit and 20-year window, by model and as an ensemble. Ten tables with different columns, so ten structures. The rebuilt set went live 2026-11-16 (duplicate rows removed; the old inflated country values for disputed-area claimants were among them, checked against an independent recomputation).

---

**Part of the Atlas hazards `v2026.11` set** — ten records, one PR each, all on schema **v0.5.0**, all `version: v2026.11` with no `previous_version` (none of these had a Hub record before; standard §4.7). Every object in the version is pinned by S3 versionId in [`atlas-hazards-2026.11_s3-versions.csv`](https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/metadata/cdh/pins/atlas-hazards-2026.11_s3-versions.csv).

**Version history:** each record links its change notes with `additional_links` `rel: version-history`, kept [in the hazards_prototype repo, beside the data pipeline](https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/metadata/cdh/changelogs/README.md) as you agreed on 2026-11-17.

Merge order: `africa-exposure-combined` and `africa-hazard-season-timeseries-nexgddp` first; the others derive from them by id. Validated locally with the v0.5.0 tooling (strict) and prettier, in this repo's `records/` layout.

Background and the remaining open questions: [catalog map briefing](https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/HANDOVER_2026-11-13_catalog-map.md#2-what-i-need-from-you).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
