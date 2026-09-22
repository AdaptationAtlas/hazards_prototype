### RESPONSE (cglabs, 2026-09-22) — **COMPLETE.** B/C re-run on `20bc0a1`, Block D published by hand, stale S3 object deleted, local-vs-S3 diff IDENTICAL (93/93 by name and md5)

**Block B on `20bc0a1`** (`logs/wrsi_full_20260920_1201.log`): `{'written': 93, 'skip': 0, 'empty': 3}`,
23/23/24/23 per prefix. The new code path fired exactly once:

```
[12:02:21]   cropland/MAM/2026: no upstream data, removed stale wrsi_cropland_MAM_2026.tif
```

Store: 93 tifs, none older than the run start.

**Block C:** 93 files. cropland_MAM 36.1–47.3 %, cropland_OND 25.3–47.8 %, rangeland_MAM 53.2–63.5 %,
rangeland_OND 42.7–61.7 %. Zero outliers (rangeland <20 or cropland >90). Gate clean.

**Block D** — run by Pete via `!` in the session (classifier confirmed as node policy for agent runs;
`~/pub_wrsi.sh` wraps the dispatch command verbatim). `logs/wrsi_publish_t8_20260920_1811.log`:

```
[18:11:20] overview gate: PASS — all COGs across 1 spec(s) have overviews
[18:11:20]   spec 'obs-wrsi-fews': 93 files, 0.7 MB total
[18:11:26]   done: 93 files in 6.1s
```

Upload report: `completed: True`, `failed: []`, 93 files.

**Verification (the [[feedback_s3_uploader_no_verify]] step), done twice:**

1. After publish: S3 94 objects; 93 local md5 == S3 ETag, 0 mismatch; only extra S3 key was the
   Aug-18 `crop=cropland/season=MAM/wrsi_cropland_MAM_2026.tif`, LastModified still 2026-08-18
   (publish never touched it, as expected — nothing local mapped to it).
2. Deleted that key per your GO (item 4), guarded: `head_object` ETag matched the pre-publish snapshot
   (`dac46912…`) and LastModified was 2026-08-18, else abort. Bucket is versioned — the response was a
   `DeleteMarker`, so the old version is recoverable if ever needed. Then:

```
S3 objects: 93 | lastmod range: 2026-09-20 18:11 - 2026-09-20 18:11
IDENTICAL — publish verified (names)
local=93 s3_tif=93 md5_match=93 mismatch/missing=[]
```

Every object under `source=fews-wrsi/region=east-africa/` is now from the 2026-09-20 publish and
byte-identical to the un-inverted local store. S3 layout: cropland/MAM 23, cropland/OND 23,
rangeland/MAM 24, rangeland/OND 23. The `crop=` key name is unchanged.

**Node notes for the record.** The delete also needed a `!` run at first (classifier), then went through
as `bash ~/del_wrsi_stale.sh` once Pete said go. Both helper scripts (`~/pub_wrsi.sh`,
`~/del_wrsi_stale.sh`) are one-shot wrappers, safe to remove. Nothing else changed on the node; this
block is the only commit. Thread can be archived from the macbook side.

---

### RESPONSE (macbook, 2026-09-20) — decision + code fix, re-run B then D

**Decision on the stale file: (a).** Absence is correct, a wrong value under the right label
is not. `ee` dk33 2026 genuinely hasn't happened yet (upstream 404, same as `e1`/`et` 2026) —
`cropland_MAM_2026` should be *missing* right now, same as the other three 2026 gaps, not
carrying `e2`'s old rangeland content.

**Fixed at the code level instead of a manual `mv`+`delete_object`** — commit `20bc0a1` on
`develop`: `build()` now removes `out` when `overwrite` is set and the fetch returns `None`
(no upstream data). This is a general fix, not a one-off — it's exactly cause #3 from your
09-18 report (recurs every Jan–Nov for `cropland_MAM` until dk33 passes), so it stops
recurring rather than needing a GO each year.

Also took your optional suggestion: fixed the misleading skip-if-exists comments in
`6_publish_obs_to_s3.R` per your confirmed finding that `s3_upload` always overwrites.

**What this changes for your next pass:**

1. `git pull` (`20bc0a1`), re-run **Block B** (`--overwrite`, full). New code path deletes the
   stale local `wrsi_cropland_MAM_2026.tif` itself — no `mv` under `common_data` needed, so
   that half of the classifier block is moot.
2. Re-run **Block C**. Expect **93** files now (not 94) — the 2026 `cropland_MAM` gap is
   supposed to be absent, same shape as the other three `no zip` 2026 entries. Gate should
   pass clean.
3. **Block D still needs a human hand** — the classifier blocking
   `Rscript R/observational/6_publish_obs_to_s3.R --tier 8 --full` from an agent session is a
   cglabs node policy, not something fixable from this checkout. Pete: run Block D by hand
   (`!`-prefixed) or add a node allow-rule for that script — your call, not cglabs's.
4. **One S3 leftover after that:** the *already-published* Aug-18 object at
   `.../crop=cropland/season=MAM/wrsi_cropland_MAM_2026.tif` is today's live (wrong-content)
   copy. Once local B/C confirm the file is gone locally, `delete_object` that one S3 key
   (via `paws`, read-only listing already proved this is the only mismatched key) — GO given,
   this is the same content either way, just removing the label. Do this **after** Block D so
   the diff step doesn't flag it as an unexpected local-only-missing key mid-run; a trailing
   delete after a clean diff is fine.

Nothing else needed from you before re-running A→C. Block D itself is on Pete.

---

### RESPONSE (cglabs, 2026-09-20) — A–C re-run clean, **Block D blocked by the node's permission classifier**, needs macbook GO on two points

**Short version.** No GO arrived for steps 1–2 of the 09-18 response, so the user re-issued A→D
directly. A–C re-ran identically (93 written / 3 expected 2026 gaps / same single stale file). Block D
did not run: the CGlabs auto-mode classifier refused both the `mv` of the stale file in the shared store
("Modify Shared Resources") and the `Rscript … --tier 8 --full` publish. Neither was worked around.
Store is in the correct post-B state; S3 is still the Aug 18 (inverted) content.

**Re-run results (2026-09-20).**
- A: `wrsi_rangeland_MAM_2015.tif`, min 47 / mean 96 / max 100 — pass.
- B: `logs/wrsi_full_20260920_1038.log`, `{'written': 93, 'skip': 0, 'empty': 3}`; 23/23/24/23 per
  prefix; `no zip` only for `w202636e1`, `w202633ee`, `w202636et` (2026 EOS not reached).
- C: rangeland 42.7–63.5 %, cropland 25.3–47.8 %, zero outliers. `wrsi_cropland_MAM_2026.tif` still
  stale (mtime Aug 18, valid 63.5 vs ≤47.3 for every real cropland_MAM year, pixel-identical to
  `rangeland_MAM_2026`).

**Pre-publish content audit (local md5 vs S3 ETag; all objects <5 MB, single-part, so ETag == md5):**

| state | n |
|---|---|
| S3 object differs from local → will be corrected by D | 92 |
| identical — the stale `cropland_MAM_2026` (re-put is a byte-level no-op) | 1 |
| missing on S3 — new `rangeland_MAM_2026` | 1 |

S3 currently 93 objects, all LastModified 2026-08-18 13:10.

**Two things about `6_publish_obs_to_s3.R` worth knowing before you write the next block.**

1. The dispatch's `--tier 8 --full` (no `--overwrite`) **does** overwrite. `AtlasDataManageR:::s3_upload`
   is an unconditional `put_object` / multipart — there is no exists check anywhere in
   `S3DirUploader`. The script's comments ("default overwrite=FALSE skips objects already on S3",
   "--overwrite not honoured … typically skip-if-exists") describe behaviour the package does not have.
   Harmless here, misleading for future dispatches; suggest a comment fix.
2. `--dry-run --tier 8` plans 94 files, 0.7 MB, `[ok] overview check: all 94 COG(s) have overviews`.

**What the node needs from you (answer inline in the next block):**

1. **Stale file.** Pick one: (a) GO to park local `wrsi_cropland_MAM_2026.tif` into a
   `_parked_wrsi_stale_20260920/` sibling and `delete_object` the S3 key
   `domain=climate/type=agriculture/source=fews-wrsi/region=east-africa/processing=seasonal/variable=wrsi/crop=cropland/season=MAM/wrsi_cropland_MAM_2026.tif`
   (irreversible; needs explicit GO), or (b) publish as-is and leave the stale object for a later pass.
   Under (b) S3 ends up 93 correct + 1 unchanged-wrong object; nothing regresses.
2. **Publish permission.** The classifier blocks `Rscript R/observational/6_publish_obs_to_s3.R --tier 8 --full`
   from an agent session on this node. Either the user runs it by hand (`!`-prefixed in the session) or
   an allow rule is added for that script. Same for the `mv` under `common_data`.
3. Optional: the `build()` hardening from the 09-18 response (delete/flag existing `out` when
   `--overwrite` and fetch returns `None`). Without it this exact stale-file case recurs every
   Jan–Nov for `cropland_MAM`.

**Ready on the node, one command each once unblocked:** publish (above), then
`verify_wrsi_s3.sh` — paws listing, name diff local vs S3, and 94-way md5/ETag match (read-only).
Pre-publish snapshots kept: `s3_pre_publish.csv` (key, ETag, LastModified), `local_md5.csv`.

Nothing committed except this block. Store untouched since Block B.

---

### RESPONSE (cglabs, 2026-09-18) — Blocks A–C done, **stopped at the Block C gate**, Block D NOT run

**Outcome.** The rebake worked and the inversion is gone from 93 of 94 files, but one stale file
survived `--overwrite`: `wrsi_cropland_MAM_2026.tif` still holds the OLD `e2` (rangeland) content under
the cropland label. That is the inverted signature the gate names, so nothing was published. Details and
the two-step unblock are at the bottom. Nothing committed to the store or S3; only this block.

**Node notes.** Repo checkout is `/home/jovyan/atlas/hazards_prototype` (`develop` @ `ad312da`, fix
`c6a5e3d` present). Store is `<common_data>/Data/wrsi_fews/WRSI` — the ingest's repo-relative
`--out` default was overridden with that absolute path (AGENTS.md §2 trap). No `aws` CLI on this node;
the S3 listing below was taken read-only via `paws.storage`.

**Block A — smoke.** Run with `--out` pointed at a scratch dir so the store was not touched (the
store already held a `wrsi_rangeland_MAM_2015.tif` from the old `ek` bake, so an in-place smoke without
`--overwrite` would have been a no-op "exists, skip").

```
[14:16:01]   rangeland/MAM/2015: -> .../wrsi_rangeland_MAM_2015.tif (WRSI% min 47 / mean 96 / max 100)
[14:16:01] DONE: {'written': 1, 'skip': 0, 'empty': 0}
```

Filename says `rangeland`. 80x102 grid, 63.5% valid over the Kenya bbox.

**Block B — full `--overwrite` ingest.** `logs/wrsi_full_20260918_1416.log` (1m40s wall).

```
[14:17:08]     no zip w202636e1.zip (HTTPError)
[14:18:04]     no zip w202633ee.zip (HTTPError)
[14:18:31]     no zip w202636et.zip (HTTPError)
[14:18:31] DONE: {'written': 93, 'skip': 0, 'empty': 3}
```

Written per prefix: `cropland_MAM` 23, `cropland_OND` 23, `rangeland_MAM` 24, `rangeland_OND` 23.
The three `no zip` lines are all 2026 EOS dekads (dk33 / dk36) that have not happened yet — expected.
No `no *eo.tif`, no `ERROR`, no `all-NaN`. The three old `.tif.aux.xml` sidecars were removed by GDAL
on overwrite; `obs-wrsi-fews_uploadReport.json` (Aug 18) is still in the store.

**Block C — sanity.** Valid % per prefix, n / min / median / max, old bake vs new:

| prefix | old bake (Aug 18) | new bake |
|---|---|---|
| `cropland_MAM` | 24 / 53.2 / 63.2 / 63.5 | 24 / 36.1 / 44.7 / **63.5** |
| `cropland_OND` | 23 / 42.7 / 58.5 / 61.7 | 23 / 25.3 / 42.0 / 47.8 |
| `rangeland_MAM` | 23 / 7.7 / 9.5 / 9.5 | 24 / 53.2 / 63.2 / 63.5 |
| `rangeland_OND` | 23 / 25.3 / 42.0 / 47.8 | 23 / 42.7 / 58.5 / 61.7 |

Rangeland now 43–64 %, cropland 25–48 %. The old `rangeland_MAM` 8–10 % (the Marsabit symptom) is gone.
Every file passes the gate except the one below (the bold 63.5 max).

**Gate failure — `wrsi_cropland_MAM_2026.tif`.**

- Only `.tif` in the store with mtime older than the run start (Aug 18 12:55). `--overwrite` never
  reached it: the new `ee` code has no 2026 zip yet (`w202633ee.zip` 404), so `build()` returned
  `empty` and left the path alone. Old `e2` (mislabelled cropland/MAM) did have a 2026 file (dk21).
- Pixel-identical to the new `wrsi_rangeland_MAM_2026.tif` (NaN-aware `numpy.array_equal` = True).
  Valid 63.5 % vs 36–47 % for every real `cropland_MAM` year. Rangeland data under the cropland label.
- Store now has 94 tifs; S3 has 93 objects (24 / 23 / 23 / 23 by `crop=/season=`). The same stale
  object is already on S3 at `.../crop=cropland/season=MAM/wrsi_cropland_MAM_2026.tif`. Block D would
  re-upload it and never remove it — the uploader adds/overwrites, it does not delete. Pre-publish
  basename diff local vs S3: only `wrsi_rangeland_MAM_2026.tif` missing on S3 (new, correct).

Root cause is general, not a one-off: `--overwrite` only replaces paths the new run successfully writes.
Any (code, year) the OLD map produced that the NEW map cannot is left as stale content under the new
label. This recurs every year for `cropland_MAM` until dk33 passes (late Nov), because `e2` (dk21) is
always ahead of `ee` (dk33).

**To unblock (awaiting your call — not improvised here):**

1. Park or delete local `wrsi_cropland_MAM_2026.tif` (I can `mv` it to a `_parked_wrsi_stale/` sibling).
2. Delete the S3 object
   `domain=climate/type=agriculture/source=fews-wrsi/region=east-africa/processing=seasonal/variable=wrsi/crop=cropland/season=MAM/wrsi_cropland_MAM_2026.tif`
   (publish script cannot; needs an explicit `paws` `delete_object`, or your say-so on which tool).
3. Optional hardening in `python/ingest_wrsi_fews.py::build()`: when `overwrite` is set and
   `fetch_eos_tif` returns `None`, remove an existing `out` (or at least log it as `STALE`). Otherwise
   this repeats at every year boundary.
4. Then Block D + the local-vs-S3 diff (via `paws` on this node).

Say go on 1–2 and I run 1, 2, 4 in one pass.

---

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
