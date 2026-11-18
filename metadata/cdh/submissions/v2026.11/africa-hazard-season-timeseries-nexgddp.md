Adds `africa-hazard-season-timeseries-nexgddp`: seasonal hazard index time series by country and first-level unit, 18-model NEX-GDDP-CMIP6 ensemble, 1981–2100, four SSPs, with anomalies against a 1981–2014 or 1995–2014 baseline. These are the files behind the Atlas climate-hazard tab.

The file names carry year ranges a template can't produce, so the record uses a `cdh-inventory` CSV committed beside it (10 files, all return 200).

**Merge before the trends and variability records**, which derive from it by id.

**Known data problem, caveated in `note`:** the country-level rows for Egypt, Kenya, Sudan and South Sudan (the disputed-area claimants) are collapsed into one wrong row per file. First-level units are fine. Question 7 in the briefing asks whether to fix before ingest or ingest with the caveat.

---

**Part of the Atlas hazards `v2026.11` set** — ten records, one PR each, all on schema **v0.5.0**, all `version: v2026.11` with no `previous_version` (none of these had a Hub record before; standard §4.7). Every object in the version is pinned by S3 versionId in [`atlas-hazards-2026.11_s3-versions.csv`](https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/metadata/cdh/pins/atlas-hazards-2026.11_s3-versions.csv).

**Version history:** each record links its change notes with `additional_links` `rel: version-history`, kept [in the hazards_prototype repo, beside the data pipeline](https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/metadata/cdh/changelogs/README.md) as you agreed on 2026-11-17.

Merge order: `africa-exposure-combined` and `africa-hazard-season-timeseries-nexgddp` first; the others derive from them by id. Validated locally with the v0.5.0 tooling (strict) and prettier, in this repo's `records/` layout.

Background and the remaining open questions: [catalog map briefing](https://github.com/AdaptationAtlas/hazards_prototype/blob/develop/HANDOVER_2026-11-13_catalog-map.md#2-what-i-need-from-you).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
