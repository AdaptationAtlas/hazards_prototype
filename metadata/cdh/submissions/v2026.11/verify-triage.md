# Hub verify script on the ten v2026.11 records — triage (2026-11-19)

Script: `cdh-metadata-verify/scripts/verify_record.py` from `CGIAR-Climate-Data-Hub/skills` at `84cbf5b`
(2026-11-14), run with the v0.5.0 standard tooling and the catalog's record ids plus these ten.

## What it found and what changed

**Fixed in the records:**

- **`africa-exposure-combined`: the six value-of-production files shared the full table's structure.**
  Each holds one `exposure` (`vop`) and one `unit`, and the constant-dollar files hold only
  `tech` = all, irr, rf-all, so the shared structure declared values those files do not contain
  (15 HIGH). Four structures were added (`vop-nominal-usd-2021-res-05/-res-25`,
  `vop-intld15-2021-res-05/-res-25`) and the six files and their aliases point at them.
- **Undeclared columns.** `africa-exposure-combined` now declares `iso3`, `admin0_name`,
  `admin1_name`, `admin2_name`, `gaul0_code`, `gaul1_code` and `stat`; the five hazard-exposure
  tiers declare the same location columns plus `model`, `hazard_vars`, `exposure_var` and (four of
  them) `exposure_unit`. Values were read from the published files.

**Not changed: the script disagrees with standard v0.5.0 or sampled only one file.**

- **`timeframe` values (24 HIGH across seven records).** The files store windows as text
  (`2021-2040`); the records label each window by its first year (`2021`) and say so in the
  description. Standard v0.5.0 requires this: temporal values are ISO 8601 dates and "range labels
  (`2020-2040`) are rejected"; writing the column's text fails the validator.
- **`cdh-inventory has no url column` (first run).** Standard v0.5.0 names the column `href`, which
  the three inventories use. With the script reading `href`, all 10 + 5 + 4 inventory rows resolve.
- **`period` is not a column (5 HIGH).** It is a path segment in `href_template`
  (`period={period}/…`), the axis the files are split along, not a column inside them.
- **`severity`, `scenario`, `baseline_name` values missing (8 HIGH).** The script opens one
  sampled file; these are the axes the files are split by. Read across every file in the three
  inventories, the values match the declared lists exactly; all six sampled template expansions of
  each exposure tier exist.
- **Folders could not be opened (8 MED, first run).** The script opened the folder URL rather than a
  file. With a sampled file opened instead, columns and values were checked as above.

**Left as they are (LOW):**

- `value` and the time-series statistics have no `unit`: each row's unit is in the `unit` /
  `exposure_unit` column or depends on the hazard, as the descriptions say.
- FAOSTAT has no `version`: FAOSTAT publishes none; the title records the bulk download date.
- Not-recommended uses are short phrases: the same style as the maintainer-edited records in the
  catalog.
- "2019" in the constant-dollar description is the FAOSTAT 2019-2023 price window, not a data year.
- One changelog link returned 503 once; it returns 200.

Result after the fixes: no HIGH or MED finding that reflects a record error. Parts 2 and 3 of the
skill (the adversarial read and the metadata-only usability test) were not run.
