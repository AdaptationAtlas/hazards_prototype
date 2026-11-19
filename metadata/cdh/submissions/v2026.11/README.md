# cdh-catalog submission — Atlas hazards `v2026.11`

Ten pull requests to `CGIAR-Climate-Data-Hub/cdh-catalog`, one per record, as the maintainer asked.
Each `<record-id>.md` here is the PR description. The record YAMLs (and three file-list CSVs) are in
`metadata/cdh/`. **Status (2026-11-19): not yet opened.** The records and these texts were revised on 2026-11-19 against the catalog's written rules (PR template, standard v0.5.0, maintainer edits to earlier records); review sheet `review.html` beside this file. All ten were built and validated in the
catalog's own `records/<id>/` layout with the v0.5.0 tooling (strict) and prettier; opening them was
held for Pete's explicit go.

## Order

Merge order matters for the cross-record links (CI does not check it):

1. `africa-exposure-combined` — the five hazard × exposure records derive from it.
2. `africa-hazard-season-timeseries-nexgddp` — trends and variability derive from it.
3. the other eight, any order.

## How to open them

From a fresh clone of `CGIAR-Climate-Data-Hub/cdh-catalog`, with `HP` set to this repo's root:

```bash
HP=/path/to/hazards_prototype
IDS="africa-exposure-combined africa-hazard-season-timeseries-nexgddp \
     africa-hazard-exposure-nexgddp africa-hazard-exposure-intld \
     africa-hazard-exposure-harvested-area africa-hazard-exposure-production \
     africa-hazard-exposure-livestock-numbers africa-hazard-season-trends-nexgddp \
     africa-hazard-season-variability-nexgddp africa-hazard-change-nexgddp"
# Guard: nothing is pushed until every PR text has its AI-use line filled and its review box ticked.
for id in $IDS; do
  f=$HP/metadata/cdh/submissions/v2026.11/$id.md
  if grep -q 'AI use:\*\* TODO' "$f" || grep -q '^- \[ \] Full record' "$f"; then
    echo "not signed off: $id"; STOP=1
  fi
done
if [ -n "$STOP" ]; then echo "stopping: fill in AI use and tick the review box first"; else
for id in $IDS; do
  git checkout -q -B submit/$id origin/main
  mkdir -p records/$id
  cp $HP/metadata/cdh/$id.yaml records/$id/
  [ -f $HP/metadata/cdh/$id-files.csv ] && cp $HP/metadata/cdh/$id-files.csv records/$id/
  git add records/$id && git commit -q -m "feat: add $id" \
    -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  git push -q origin submit/$id
  gh pr create --base main --head submit/$id --title "feat: add $id" \
    --body-file $HP/metadata/cdh/submissions/v2026.11/$id.md
done
fi
```

Re-validate first if any record changed since 2026-11-19 (commands in `../../README.md`, with the
tooling cloned at `--branch v0.5.0`).

After they are opened, record the PR numbers in the `metadata/cdh/README.md` record table.
