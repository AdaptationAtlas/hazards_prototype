# Dispatch: WRSI cropland/rangeland inversion — rebake + republish

**Status:** fix landed on `develop` @ `c6a5e3d`. Nothing re-baked or republished yet.

**Source.** `atlas_nb-KE-enso` notebook session found `REGION_MAP` in
`python/ingest_wrsi_fews.py` had FEWS's own cropland/rangeland codes swapped —
`e1`/`e2` (rangeland) were tagged `cropland`, `ek`/`et` (crop) were tagged
`rangeland`. Proof: Marsabit (~all rangeland) showed 1-3% coverage on the
`rangeland` layer and 98-100% on `cropland`. Full writeup:
`playbook/handovers/KE-enso-explorer/dispatches/2026-09-18_request-wrsi-crop-rangeland-inversion.md`
in the notebook repo (read-only source, not in this repo).

**What changed in the code** (`python/ingest_wrsi_fews.py`, commit `c6a5e3d`):

| code | old | new |
|---|---|---|
| `e1` | cropland/OND/dk36 | **rangeland**/OND/dk36 |
| `e2` | cropland/MAM/dk21 | **rangeland**/MAM/dk21 |
| `et` | rangeland/OND/dk36 | **cropland**/OND/dk36 |
| `ee` | not ingested | **new: cropland**/MAM/dk33 |
| `ek` | rangeland/MAM/dk27 | **dropped** (Ethiopian belg sorghum, ~0% of Kenya) |

The bake pipeline itself was not at fault — S3 objects are pixel-faithful
copies of the upstream FEWS product for the code they were given. Only the
code→domain label was wrong. No changes needed in `6_publish_obs_to_s3.R`
(`name_fn_wrsi` is crop-label agnostic).

---

## Block A — pull + smoke (read-only against upstream, one region/year)

```bash
cd <hazards_prototype>
git fetch origin && git checkout develop && git pull --ff-only
git log -1 --format='%H %s'   # expect c6a5e3d fix(wrsi): un-invert FEWS cropland/rangeland region codes

python3 python/ingest_wrsi_fews.py --smoke
```

Expect: one file `wrsi_rangeland_MAM_2015.tif` (region `e2`, now correctly
labelled rangeland — the smoke help text and REGION_MAP agree this is
rangeland/MAM/dk21).

**Report back:** smoke output, and confirm the written filename says
`rangeland`, not `cropland`.

---

## Block B — full ingest (all 4 codes x years 2003-2026)

```bash
cd <hazards_prototype>
python3 python/ingest_wrsi_fews.py --overwrite 2>&1 | tee /tmp/wrsi_full_$(date +%Y%m%d_%H%M).log
```

`--overwrite` is required — this re-derives files at paths that may already
hold the OLD (wrongly-labelled) content from the prior bake, and it also adds
the new `ee` code that never ran before. Without `--overwrite` the stale
`cropland_OND`/`rangeland_MAM` files (built from `et`/`ek`) would sit next to
correctly-labelled new ones instead of being replaced.

This hits the USGS archive ~4 codes x ~24 years = ~96 zip fetches. Budget
accordingly; it is not the multi-hour class of run.

**Report back:** tail of the log, count of `.tif` written per `wrsi_{crop}_*`
prefix (expect roughly even counts across `cropland_OND`, `cropland_MAM`,
`rangeland_OND`, `rangeland_MAM`), and any `no zip` / `no *eo.tif` lines.

---

## Block C — sanity check before publish (read-only, catches a repeat of the original bug)

```bash
cd <hazards_prototype>
Rscript -e '
f <- list.files("Data/wrsi_fews/WRSI", pattern = "^wrsi_.*\\.tif$", full.names = TRUE)
r <- do.call(rbind, lapply(f, function(p) {
  x <- terra::rast(p)
  data.frame(file = basename(p), valid_pct = round(100 * sum(!is.na(terra::values(x))) / terra::ncell(x), 1))
}))
print(r[order(r$file), ], row.names = FALSE)
'
```

Sanity: any `rangeland_*` file for an ASAL-heavy year should NOT be near-0%
nationally, and no `cropland_*` file should be near-100% nationally (that was
exactly the inverted signature). Spot check is enough — full per-county
breakdown was already done notebook-side.

**If any file looks inverted again — stop, do not publish, report back.**

---

## Block D — republish tier 8 to S3

```bash
cd <hazards_prototype>
Rscript R/observational/6_publish_obs_to_s3.R --tier 8 --full
```

This overwrites the existing (wrongly-labelled) S3 objects under
`domain=climate/type=agriculture/source=fews-wrsi/region=east-africa/` —
same paths, corrected content, plus new `crop=cropland/season=MAM` objects
that didn't exist before (from `ee`).

Per [[feedback_s3_uploader_no_verify]]: the uploader trusts per-file return
values and objects can silently drop. Run a local-vs-S3 diff after:

```bash
aws s3 ls --recursive s3://<bucket>/domain=climate/type=agriculture/source=fews-wrsi/region=east-africa/ \
  | awk '{print $4}' | sed 's|.*/||' | sort > /tmp/s3_wrsi.txt
ls Data/wrsi_fews/WRSI/*.tif | xargs -n1 basename | sort > /tmp/local_wrsi.txt
diff /tmp/local_wrsi.txt /tmp/s3_wrsi.txt && echo "IDENTICAL — publish verified"
```

**Report back:** publish log tail, and the diff (or confirmation it was empty).

---

## Notes

- Do not commit anything from this dispatch — only running scripts and
  reporting output. The code fix is already committed (`c6a5e3d`).
- No new branches. Stay on `develop`.
- `crop=` staying as the S3 key name (vs `domain=`) is intentional — the
  notebook reads `crop=` today and this dispatch is not a path-shape change.
