# Issue #26 — `republish_A.R` looks for anomaly files that do not exist

Found incidentally while doing issue #29 P4 (resolving hardcoded publisher paths).
**Not caused by that change** — P4 touched exactly one line in this file, the `DIR`
constant, and the resolved path is byte-identical to the literal it replaced. This
failure predates it.

## Symptom

On CGlabs, at `2ab28a4`:

```
$ Rscript R/republish_A.R
[08:46:25] mode:DRY-RUN| aws:aws/dist/aws
[08:46:25] === pre-flight checks (all 5 files) ===
Error in check_file(local) :
  missing local file: /home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/hazard_timeseries_mean_month/haz_3months_adm_mean_1995-2014_anomaly-1995-2014_ensemble_seasons.parquet
```

Dry-run, so nothing was published. It stops in pre-flight on the first of five files.

## Mechanism

`R/republish_A.R:34` hardcodes the baseline label:

```r
BASELINE_KEY <- "1995-2014"   # issue #26: R/2.1 names anomalies by baseline window (anomaly-1995-2014)
```

and `:81` builds

```r
local <- file.path(DIR, sprintf("haz_3months_adm_mean_%s_anomaly-%s_ensemble_seasons.parquet", P, BASELINE_KEY))
```

But `R/2.1_create_monthly_haz_tables.R` does not hardcode that string. It derives it
from what is on disk — `:387`:

```r
baselines <- files[grep("historic", scenario), sort(unique(timeframe))]
```

and then builds the output name at `:660`:

```r
save_file2 := gsub(".parquet", paste0("_anomaly-", baseline, "_ensemble_seasons.parquet"), data)
```

So the `anomaly-<X>` token is **whatever timeframe string the historic files carry**, not
a fixed window. The comment on `:34` is written in the future tense — it describes the
naming R/2.1 *would* use once #26 lands, not what it emits today.

## What to check first

One command settles it:

```bash
ls /home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/hazard_timeseries_mean_month/ \
  | grep ensemble_seasons
```

- If they read `..._anomaly-historic_ensemble_seasons.parquet`, then R/2.1 is still
  labelling by the literal scenario token `historic`, and `republish_A.R` is simply ahead
  of the producer.
- If they read `..._anomaly-1995-2014_...` but for different `PERIODS`, it is a period
  mismatch instead, not a baseline one.
- If nothing matches at all, R/2.1 §3.3 has not been re-run since the last rename.

## Why this is #26 and not a publish bug

Issue #26 is the baseline mislabelling: anomalies computed against a stale 1981-2014
window while being presented as 1995-2014, with the fix being to carry the window in the
name. `republish_A.R` was written against the post-fix naming. Renaming the publisher to
match today's files would paper over the very thing #26 exists to correct, so the
sequencing matters:

1. settle what R/2.1 actually emits (command above);
2. decide whether the producer's label changes as part of #26;
3. only then align `republish_A.R` — or delete `BASELINE_KEY` and derive it from the
   files present, the way R/2.1 does.

Option 3 is worth considering on its own merits: a publisher that hardcodes a label its
producer derives will drift again the next time the window changes.

## Related, from the issue #29 work

- `metadata/catalogue/timeseries-mean-month.json` records this dataset, its S3 prefix,
  and that its CDH record (`ensemble_season_trends`) is still schema v0.0.1 with empty
  `license`/`citation` and no `data:` block, against 15 records at v0.3.0.
- `Rscript R/checks/73_catalogue.R --dataset timeseries-mean-month` prints the record.
- The publisher now resolves its tree via `atlas_dir("hazard_timeseries_mean_month")`,
  overridable with `HAZ_MEAN_MONTH_DIR`, so it can be pointed at a scratch copy for
  testing without editing code.
