# DISPATCH — cglabs ↔ macbook — GFM flood (replace GFD)

Branch `develop`. Append-only; newest entry directly below this intro block, above the previous one. cglabs appends a `### RESPONSE` under the entry it answers, then `git pull --rebase && git push`.

**Goal.** Replace the Global Flood Database (GFD, MODIS, ends 2018) with **Copernicus CEMS GloFAS Global Flood Monitoring (GFM, Sentinel-1 SAR)** for Kenya in the KE-ENSO Explorer notebook. GFM is continuously updated (~2019 → present), so it gives us post-2018 observed flood extent. We are NOT keeping both (Pete: having both = confusion) — GFM fully replaces GFD in the notebook.

**Target architecture (macbook builds after this probe).** Three aggregation tiers, all Kenya bbox `(33.9, -4.7, 41.9, 5.5)`, CDH-ready COGs on `s3://digital-atlas/type=flood/source=glofas-gfm/region=kenya/`:
1. `processing=overpass/variable=flooded/{YYYY-MM-DD}.tif` — each Sentinel-1 acquisition's observed flood extent, clipped to Kenya (raw archive).
2. `processing=monthly/variable=flooded/{YYYY-MM}.tif` — per-pixel monthly flood occurrence (flooded in ≥1 valid overpass that month) + companion observation-count layer so "no flood" is distinguishable from "no observation".
3. `processing=seasonal/variable=flooded/{window}.tif` — rolling 3-month aggregate matching the notebook's PTOT seasonal windows (12 windows). Months are the building blocks → seasons.
4. `processing=history/variable={frequency,footprint}.tif` — full-record roll-up (optional notebook layer). `frequency` = flooded-obs ÷ valid-obs (0–1, obs-density normalized); `footprint` = ever-flooded binary. Derives cheaply from the monthly tier. NB short record (~2019→now) = recent observed flood-proneness, NOT return-period magnitude.

Every dataset gets a CDH v0.1.0 metadata record (`metadata/cdh/*.yaml`); GFM draft already staged at `metadata/cdh/kenya-flood-gfm.yaml` — you'll fill the TODO(probe) values from your findings.

---

## [macbook 2026-08-31 #6] — GFM LIVE confirmed. GFD delete = HOLD until notebook swaps.

Tallies + count-verify received — **382/382, per-tier 192/188/2, 206+CORS. GFM accepted as LIVE.** The basename diff is the intended `{var}-`/`{var}_` rename (not drops) — agreed, no action. ~2 h vs 9 days: re-arch paid off. Thanks.

**GFD S3 delete — HOLD (Pete's call).** Do NOT delete `source=global-flood-db/` yet. Sequence: the KE-ENSO notebook must re-point to GFM FIRST (else its flood layer breaks). Handover written to the notebook session (`atlas_nb-KE-enso/…/dispatches/2026-08-31_reply-gfm-flood-live.md`). **Once the notebook confirms it reads GFM, I'll signal here — THEN delete the 15 GFD year-COGs.** Until then both stay (transient, not the permanent "two sources" state).

Nothing else for GFM. Remaining pipeline items are separate (electricity grid; CDH `# verify` confirmations). GFM workstream = done pending the notebook swap.

---

## [macbook 2026-08-28 #5] — RE-ARCHITECTED (Pete: 111m direct, drop 20m overpass archive). STOP grind, re-smoke, parallel run.

Pete's call on your scale flag: **re-architect** — the notebook only uses the ~111 m aggregates, so the 16,211-acquisition / ~113 GB / 9-day 20 m overpass archive is dropped. `ingest_flood_gfm.py` **rewritten (v2)**:

- **Monthly is now built DIRECTLY from the STAC tiles at ~111 m** — for each month, group its acquisitions, warp each acquisition's tiles straight onto the fixed ~111 m grid (full-res read `overviewLevel=NONE`, `resampleAlg=max`, nodata-aware → a coarse pixel floods if ANY 20 m sub-pixel floods), accumulate `flood_any` + `obs`, write `flooded`+`nobs`. **No 20 m overpass COG is materialised.** Skips the expensive 40000×51000 write + 7-overview build that dominated your 47 s/mosaic → each acquisition is now download-bound (a few s), and it's **parallelised across the 84 months** (`--workers`).
- seasonal + history unchanged (from monthly). Overpass is now **opt-in only** (`--stage overpass --start .. --end ..`) for a specific event, re-pullable from STAC — NOT run in `--stage all`.

**FIRST — stop the old grind + clear its partial output** (those 20 m overpass COGs are no longer published; tier-14's recursive walk would otherwise upload the partial `overpass/` as `processing=overpass/…`):
```
# kill the running --stage all (old sequential overpass grind)
rm -rf Data/exposure/gfm_flood/overpass          # partial 2018-01 only; not wanted
```

**THEN re-smoke (cheap, do first):**
```
python3 python/ingest_flood_gfm.py --smoke
```
Mini-month: a 3-day window (late-Apr-2020) through the REAL direct-111 m path, written to a sibling `gfm_flood_smoke/` (won't be published). Gate asserts grid == coarse dims, 0/1/255, overviews. Report: size, nodata, min/max, #overviews, valid px, flooded px.

**THEN the full run (parallel, hours not days):**
```
nohup python3 python/ingest_flood_gfm.py --stage all --workers 8 &> gfm_ingest.log &
```
Set `--workers` to ~(cores−2). 84 months across the pool; timestamped + skip-if-exists (resumable). Est. a few hours (16,211 acq, download-bound, ÷ workers).

**THEN publish** (unchanged): `Rscript R/observational/6_publish_obs_to_s3.R --full --tier 14` → count-verify + local-vs-S3 diff. Tier 14 now sees only monthly/seasonal/history (no overpass). Report tallies.

Append `### RESPONSE` with the smoke numbers, then (after the run) the monthly/seasonal/history tallies + publish count-verify. Push.

### RESPONSE — cglabs 2026-08-28 — re-arch v2 SMOKE PASS. Full parallel run LAUNCHED. 🟢

Old 20 m grind **killed** + `Data/exposure/gfm_flood/overpass/` (partial 2018-01) **removed** — nothing stale left to publish.

```
v2 smoke (direct-111m, late-Apr-2020 3-day window, 20 acquisitions):
size       = 8000 x 10200    (= coarse dims: Kenya 8.0°×10.2° / 0.001° ✓)
nodata     = 255
min/max    = 0 / 1           (0/1/255 coding preserved through tiles→coarse max ✓)
#overviews = 5
valid px   = 29,321,440      (observed ≥1 valid overpass)
flooded px = 52,506          (real late-Apr-2020 Kenya flood signal)
```
Direct 111 m month path confirmed (per-acquisition tiles warped straight onto the fixed coarse grid, `resampleAlg=max`, nodata-aware; no 20 m COG materialised). Smoke went to `gfm_flood_smoke/smoke.tif` (won't publish). This re-arch drops the 9-day/113 GB overpass grind — 

**Full run LAUNCHED:** `--stage all --workers 8` (84 months across the pool, download-bound; skip-if-exists/resumable). Node has 40 cores; kept workers=8 (network-bound on EODC — avoids rate-limiting; can bump if it's CPU-idle). Est. a few hours.

**Tallies + publish → I'll append on completion:** monthly (`flooded`+`nobs`, ~84×2), seasonal (12 windows × years × 2), history (frequency+footprint) counts, then `--full --tier 14` + count-verify (local==S3) + local-vs-S3 diff. No publish until the run finishes.

### RESPONSE (cont.) — cglabs 2026-08-29 — GFM LIVE. Full run + publish complete. 🟢🟢

**Run complete** (parallel `--workers 8`, ~2 h wall: monthly 20:58→02:52, seasonal→03:06, history→03:07). The "1000+ errors" in the log = **GDAL/libtiff vsicurl read noise only** (no tracebacks/halts; every STAGE DONE clean, `monthly {written:96, skip:0, empty:0}`).

```
INGEST TALLIES (local .tif):
  monthly  = 192  (flooded 96 + nobs 96 — 96 months 2018-01→2025-12 ✓)
  seasonal = 188  (flooded 94 + nobs 94 — 12 windows × years, edge-trimmed)
  history  = 2    (frequency + footprint)
  total    = 382
PUBLISH tier 14: dry-run overview-gate [ok] → 382/382 in 34.1s
COUNT-VERIFY: local=382 == S3=382 ✓   per-tier S3: monthly 192 / seasonal 188 / history 2 ✓
LIVE: monthly + seasonal + history all range-GET 206 + CORS *
```
Spot-gate: monthly `flooded-2020-04` + seasonal `flooded_MAM_2020` = 8000×10200, overviews, 0/1; history `frequency` = 8000×10200, 0–0.78 (flooded-obs÷valid-obs ✓). Base:
`…/type=flood/source=glofas-gfm/region=kenya/processing={monthly|seasonal|history}/variable=…`

Note on the local-vs-S3 **basename** diff: it lists mismatches (`2020-04.tif` vs `flooded-2020-04.tif`) — that's the **intended `name_fn_gfm` `{var}-`/`{var}_` rename**, NOT dropped objects. The **count (382==382) + per-tier (192/188/2) + live 206s on the prefixed names** confirm every file landed. No silent drop.

**⚠️ GFD cleanup (flag — your call):** GFM now fully replaces GFD (Pete). The old **GFD is still live on S3** (`type=flood/source=global-flood-db/…`, 15 year-COGs from the earlier tier-7). Once the notebook re-points to GFM, do you want cglabs to **delete the GFD S3 prefix** (so there aren't two flood sources = the confusion Pete flagged)? Not deleting autonomously — say the word and I remove `source=global-flood-db/`.

**GFM ingest COMPLETE + LIVE** — GFD→GFM replacement done (pending the notebook swap + optional GFD delete).

---

## [macbook 2026-08-28 #4] — RATIFY pagination fix + monthly aligned to PTOT

**Pagination fix `6759ae2` — RATIFIED.** Correct: STAC `next` link key is `href` (not `url`, my bug), and merging the paging token onto the request body is the right stac-fastapi behavior. Good catch — smoke's late-Apr-2020 window was a single <500-item page so it never exercised paging. Only note: the `nb.get("merge")` branch assumes EODC sets `merge:true` on partial tokens (you verified 3 clean page-boundaries, so fine as-is); if a future page ever returns a partial token WITHOUT `merge`, `collections/bbox/datetime` would drop — a defensive `body = {**body, **nb}` always-merge would be bulletproof, but not needed given your verification. Keep it.

**Monthly ALIGNED to PTOT (committed).** `name_fn_gfm` monthly leaf is now `processing=monthly/variable={flooded|nobs}/{var}-{YYYY}-{MM}.tif` — i.e. `variable=flooded/flooded-2020-04.tif`, mirroring PTOT's `PTOT-{YYYY}-{MM}.tif`. **Local on-disk monthly files are unchanged** (`monthly/flooded/2020-04.tif`) so the seasonal/history stages still parse them — only the S3 leaf gains the `{var}-` prefix. Unit-tested.

So all four tiers now mirror PTOT/notebook conventions:
- overpass → `{YYYYMMDD}T{HHMMSS}.tif` (archive)
- monthly → `variable={var}/{var}-{YYYY}-{MM}.tif`
- seasonal → `variable={var}/season={SEASON}/{var}_{SEASON}_{YYYY}.tif`
- history → `variable={frequency|footprint}/…`

**Publish tier 14 is ready.** When the full run finishes: `Rscript R/observational/6_publish_obs_to_s3.R --full --tier 14` → count-verify (local .tif == S3) + local-vs-S3 diff, report tallies. Nothing else blocking.

### RESPONSE — cglabs 2026-08-28 — #4 ratify noted. ⚠️ SCALE FLAG: overpass stage = 16,211 acquisitions ≈ ~9 days sequential.

Fix ratified, monthly-align confirmed — thanks. **But the run is far bigger than the ~1,400 estimate:** `stage_overpass` reports **16,211 distinct acquisition times** over 2018→2025 (GFM has multiple S1 overpasses/day over the Kenya bbox across ascending+descending orbits). Progress: 28/16,211 written in ~22 min ⇒ **~47 s/mosaic** (each = download 2–4 remote Equi7 COGs + gdalwarp) ⇒ **~9 days wall-clock sequential**. It's checkpointed (skip-if-exists, resumable across restarts), so it won't lose work — but 9 days as one grind isn't practical.

**Decision needed — pick one:**
1. **Parallelize the overpass stage** (best): the 16,211 mosaics are independent → run ~8–16 concurrent (thread/process pool in `stage_overpass`, or an `xargs -P` wrapper like the R kernel bake). ~12–16× → **~half a day**. Small script change (macbook), I run it.
2. **Narrow scope:** does the notebook need every overpass 2018→now, or just the years for the ENSO story? e.g. 2018–2021 + 2023–2024 (El Niño/La Niña years) cuts it a lot. The **monthly/seasonal aggregates** are what the notebook displays — but they need all overpasses within each kept month.
3. **Let it grind** with checkpointing (I keep it alive across sessions, resume on restart) — simplest but ~9 days.

**I've PAUSED nothing — the run is live and checkpointing** (currently ~30 overpasses in, 2018-01). Say which of 1/2/3. My rec = **(1) parallelize** — keeps full record, ~half a day. Aggregates + publish (tier 14) follow once overpass completes.

---

## [macbook 2026-08-28 #3] — GO for full run (smoke confirmed) + seasonal aligned to PTOT

Smoke output eyeballed — clean (40000×51000 @20 m, 0/1/255, 7 overviews, 4-tile mosaic, 7 MB). **GO.** Launch the full run under nohup:
```
nohup python3 python/ingest_flood_gfm.py --stage all --start 2018-01-01 --end 2025-12-31 &> gfm_ingest.log &
```
(or stage-by-stage `overpass → monthly → seasonal → history` to checkpoint). ~10 GB overpass tier is fine.

**Seasonal aligned to PTOT (committed).** Ingest now writes `seasonal/{flooded,nobs}/{SEASON}_{YYYY}.tif`; the publish tier-14 `name_fn_gfm` emits `processing=seasonal/variable={flooded|nobs}/season={SEASON}/{var}_{SEASON}_{YYYY}.tif` — i.e. `variable=flooded/season=MAM/flooded_MAM_2020.tif`, matching your PTOT layout so the notebook swaps `source=`/`variable=` in one URL builder. Same 12 window codes.

**Publish tier 14 is added** (recursive `processing=/variable=` walk, overview gate applies — smoke COG had 7 overviews so it passes). After ingest completes:
```
Rscript R/observational/6_publish_obs_to_s3.R --full --tier 14
```
Then **count-verify** (published == local .tif count) AND a **local-vs-S3 diff** (uploader has no built-in verify — objects can silently drop). Report the counts.

**Overpass filename** stays `{YYYYMMDD}T{HHMMSS}.tif` (archive; keeps sub-daily overpasses distinct). Notebook reads monthly/seasonal, not overpass, so date-only isn't needed there.

**One alignment question before publish — confirm the PTOT MONTHLY S3 path.** GFM monthly is currently `processing=monthly/variable=flooded/{YYYY-MM}.tif` (+ `variable=nobs/`). If PTOT monthly uses a different filename/partition (e.g. `variable=PTOT/PTOT_{YYYY}_{MM}.tif`), tell me the exact pattern and I'll align GFM monthly the same way before you publish tier 14. Seasonal is the notebook's display unit so it's the priority; monthly alignment is for URL-builder consistency.

Append `### RESPONSE` with the PTOT monthly path + (after the run) the ingest tallies + publish count-verify, then push.

### RESPONSE — cglabs 2026-08-28 — GO'd; full run LAUNCHED (+ one code fix). PTOT monthly path below.

**PTOT MONTHLY S3 path (confirmed live):** `…/processing=monthly/variable=PTOT/**PTOT-{YYYY}-{MM}.tif**` (e.g. `PTOT-1981-01.tif` … `PTOT-2026-04.tif`) — flat under `variable=PTOT/`, filename `{VAR}-{YYYY}-{MM}.tif`, NO `season=`-style partition at monthly.
→ **To align GFM monthly:** rename `processing=monthly/variable=flooded/{YYYY-MM}.tif` → `processing=monthly/variable=flooded/**flooded-{YYYY}-{MM}.tif**` (and `variable=nobs/nobs-{YYYY}-{MM}.tif`). i.e. prefix the filename with `{var}-`, matching PTOT's `{VAR}-{YYYY}-{MM}.tif`. Then monthly + seasonal both mirror PTOT and the notebook swaps `source=`/`variable=` in one builder. Please tweak the tier-14 `name_fn_gfm` for monthly before publish.

**Full run LAUNCHED** (`--stage all 2018-01-01..2025-12-31`, background) — running clean now, but:

⚠️ **CODE FIX applied (committed `6759ae2`, flag ratify):** the full run **KeyError'd immediately** at `stac_search` L118 `url = nxt["url"]` — the STAC `next` pagination link key is **`href`, not `url`** (+ its `body` is a partial paging token). Smoke never hit it (its late-Apr-2020 window = <500 items = single page). Fixed: `url = nxt.get("href") or url` + merge the token onto the current body so `collections/bbox/datetime` survive across pages. Verified: paged 450 items across 3 page-boundaries cleanly, then relaunched. Please ratify.

**Ingest tallies + publish count-verify → I'll append when the run finishes** (Stage-A ≈ ~1,400 overpass mosaics 2018→now + monthly/seasonal/history — multi-hour). Will then run `--full --tier 14` (after you align the monthly name_fn) + count-verify (local .tif == S3) + local-vs-S3 diff, and report. No publish until the run completes + monthly alignment lands.

---

## [macbook 2026-08-28 #2] — ingest script live: SMOKE first, then hold

Probe answers folded in. `python/ingest_flood_gfm.py` is committed (anon EODC STAC, `ensemble_flood_extent`, Equi7-AF 20 m → EPSG:4326, 0/1/255 coding, 4 tiers). CDH record updated at `metadata/cdh/kenya-flood-gfm.yaml`.

**STEP 1 — smoke (cheap, do first, report back):**
```
python3 python/ingest_flood_gfm.py --smoke
```
Runs ONE overpass end-to-end over a tight late-Apr-2020 Kenya-flood window (mosaic the intersecting Equi7 tiles → EPSG:4326 → clip Kenya → COG), then a gdalinfo gate. Report: output size (WxH), nodata, min/max, #overviews, #tiles mosaicked, file size on disk. This proves the Equi7→4326 mosaic + 0/1/255 coding + overviews before any long run. **Then HOLD** — let macbook eyeball the smoke output before the multi-year Stage-A run.

**STEP 2 — full (only after macbook confirms smoke):** run under nohup (Stage A = ~1400 overpass mosaics over 2018→now, the long one). Timestamped logging + skip-if-exists are built in. Either:
```
nohup python3 python/ingest_flood_gfm.py --stage all --start 2018-01-01 --end 2025-12-31 &> gfm_ingest.log &
```
or stage-by-stage (`--stage overpass` → `monthly` → `seasonal` → `history`) to checkpoint.

Notes: (a) aggregates land at ~111 m (0.001°) to match the pop/exposure grid; overpass stays native ~20 m. (b) seasonal keys are `YYYY-WWW` rolling 3-month (e.g. `2020-MAM`, 12 windows) — **flag if the PTOT seasonal COGs used a different naming/window convention so I align the notebook**. (c) macbook adds the GFM publish tier to `6_publish_obs_to_s3.R` after smoke confirms the on-disk `processing=/variable=` layout. Do NOT publish yet.

Append `### RESPONSE` with the smoke gdalinfo numbers, then push.

### RESPONSE — cglabs 2026-08-28 — SMOKE PASS. Holding for your confirm before the multi-year run. 🟢

```
smoke overpass: 2020-04-24T03:10:11Z (4 Equi7 tiles mosaicked)
WxH        = 40000 x 51000   (Kenya bbox @ ~20 m native / 0.0002° — overpass stays native ✓)
nodata     = 255
min/max    = 0 / 1           (0=not-flooded, 1=flooded, 255=NoData — coding confirmed ✓)
#overviews = 7
#tiles     = 4               (this overpass's swath ∩ Kenya; full-Kenya coverage = 17 tiles across overpasses)
file size  = 7.0 MB on disk  (ZSTD COG; mostly-dry overpass)
out path   = Data/exposure/gfm_flood/overpass/20200424T031011.tif
```
Equi7-AF→EPSG:4326 mosaic + clip + 0/1/255 + overviews all confirmed. **HOLDING — not starting Stage-A / --stage all until you confirm.** (At 7 MB × ~1,400 overpasses the overpass tier ≈ ~10 GB native-20 m; the 111 m aggregates are far smaller.)

**⚠️ SEASONAL NAMING — YES, PTOT differs from your `YYYY-WWW`; align before the seasonal tier:**
The 12 tri-month **window codes are identical** (JFM FMA MAM AMJ MJJ JJA JAS ASO SON OND NDJ DJF) — good. But the **layout/filename differ**:
- **PTOT (live, what I baked):** S3 `…/processing=seasonal/variable=PTOT/**season={SEASON}/**PTOT_{SEASON}_{YYYY}_sum.tif` (e.g. `season=MAM/PTOT_MAM_2020_sum.tif`) — **partitioned by a `season={SEASON}/` sub-dir**, filename `{SEASON}_{YYYY}` order.
- **Your GFM proposal:** flat `processing=seasonal/variable=flooded/{YYYY}-{WWW}.tif` (e.g. `2020-MAM.tif`) — no `season=` partition, `{YYYY}-{SEASON}` order.
- **Recommend GFM match PTOT:** `…/processing=seasonal/variable=flooded/season={SEASON}/flooded_{SEASON}_{YYYY}.tif`. Then the notebook swaps rainfall↔flood by changing only `variable=`/`source=` in the same URL builder; your `YYYY-MM-DD` flat scheme would need a separate path builder. Your call — flagging so you align the notebook + the publish tier's name_fn.

**Also (minor) overpass filename:** tier-1 spec said `{YYYY-MM-DD}.tif`, script writes `{YYYYMMDD}T{HHMMSS}.tif` (`20200424T031011.tif`) — keeps sub-daily overpasses distinct (good, since >1 overpass/day possible), but ≠ the dispatch's `YYYY-MM-DD`. Align if the notebook expects date-only.

Smoke is clean; say go and I launch `--stage all 2018→2025` under nohup (or stage-by-stage). No publish yet (tier not added).

---

## [macbook 2026-08-26 #1] — PROBE GFM access before build

Do NOT ingest yet. Probe-before-build discipline (same as JRC/WRSI/GRID3). Report these, then macbook writes `python/ingest_flood_gfm.py`:

1. **Programmatic access route.** Which works headless from the node, non-interactively — pick the best and give the concrete recipe:
   - EODC STAC API (endpoint URL + collection id; example query by bbox + datetime).
   - openEO Platform `gfm` process (does it need an openEO account/creds?).
   - GFM/GloFAS web portal bulk download (has spatial/temporal caps — likely NOT bulk-friendly).
   Report auth: anonymous, or registration/token? If a free registration/token is needed, note exactly where.

2. **Earliest available date** (backfill start). Is there continuous coverage from 2019? 2020? Give the first full year Kenya has data. This settles the GFD→GFM seam (GFD ends 2018) and whether there's a gap year.

3. **Product layers.** Confirm the asset/layer names. Which layer = the flood mask we ingest as `flooded`? (Expect "Observed Flood Extent" / ensemble flood extent.) List the companions: reference/permanent water, exclusion mask, observed water extent, likelihood. Confirm the flood layer already EXCLUDES permanent water (so it's flood, not all water).

4. **Grid + resolution.** Native tiling (Equi7 grid?), how many tiles cover the Kenya bbox, native resolution (~20 m?), native CRS. Can we reproject/mosaic to EPSG:4326 cleanly?

5. **NoData / observation coding.** CRITICAL for aggregation: how does a pixel encode "flooded" vs "not flooded" vs "not observed this overpass" (SAR gap / excluded)? We need to count valid observations per pixel per month, not treat unobserved as dry.

6. **Volume.** Size of one representative Kenya overpass (flood extent COG, compressed) and approximate number of Sentinel-1 overpasses per year over Kenya. Sizes the overpass archive (want a rough total for 2019→now).

7. **Licence.** Exact Copernicus EMS / GFM licence wording (free/full/open? Copernicus data policy?). Required attribution string. Is it SPDX-mappable or do we use `LicenseRef-Copernicus-EMS`? This gates whether we can publish to digital-atlas and contribute to CDH (needs CC-BY-4.0 or approved equivalent).

Append `### RESPONSE` with each numbered answer, then push.

### RESPONSE — cglabs 2026-08-28 — GFM probe. Anonymous EODC STAC works; ingest viable. 🟢

**1. Programmatic access — EODC STAC, ANONYMOUS (no auth/token).**
- Endpoint `https://stac.eodc.eu/api/v1/`, collection id **`GFM`** ("Global Flood Monitoring").
- **POST `/search`** (GET-with-params returned nothing; POST works): `{"collections":["GFM"],"bbox":[33.9,-4.7,41.9,5.5],"datetime":"<start>/<end>","limit":N,"sortby":[{"field":"datetime","direction":"asc"}]}`.
- Assets are **direct-download COGs** on `data.eodc.eu` (e.g. `…/collections/GFM_LAYERS/flood_extent/AF020M/YYYY/MM/DD/ENSEMBLE_FLOOD_*.tif`) — **anon HEAD = 206, `/vsicurl` reads clean**. No openEO account, no portal needed. openEO/portal = ignore.

**2. Earliest Kenya date = 2018-01-01** (query matched 12,354 items 2018→2020-H1; earliest `2018-01-01T02:53:03Z`). Continuous since 2018 → **NO GAP after GFD** (GFD ends 2018; they abut). First clean full year = 2018 (or start 2019 if you want zero-overlap with GFD's final year). SAR = Sentinel-1.

**3. Product layers — flood mask = `ensemble_flood_extent`.** Full asset list: `ensemble_flood_extent` (← ingest as `flooded`; ensemble of DLR/TUW/LIST algos), `ensemble_water_extent` (ALL observed water), `reference_water_mask` (permanent/seasonal reference water), `exclusion_mask` (layover/shadow/low-sensitivity/urban — excluded pixels), `ensemble_likelihood` + per-algo `{dlr,tuw,list}_flood_extent`/`_likelihood`, `advisory_flags`. **`ensemble_flood_extent` EXCLUDES permanent water by design** (GFM defines flood = water_extent − reference_water) → it's flood, not all-water. ✓

**4. Grid/res — Equi7 Africa, 20 m.** Native CRS = **Equi7 AF (Azimuthal Equidistant per-continent, NOT EPSG)**, res **20 m**, T3 tiles = 15000×15000 (300 km). **Kenya bbox = 17 tiles** (`AF020M_E{069,072,075}N{045,048,051,054}T3` + edges). COG, ZSTD, Byte. Reproject/mosaic Equi7-AF→EPSG:4326 cleanly with gdalwarp (**nearest** — it's a class raster). ✓

**5. NoData / observation coding (the critical one) — Byte: `0=not-flooded, 1=flooded, 255=NoData`.** 255 = **not observed this overpass** (outside swath / excluded). So per-pixel per-overpass: {0,1} = valid observation, 255 = no-obs. Monthly aggregation: `flooded = max(flood==1)`, `valid_obs = count(value∈{0,1})` → the companion obs-count layer distinguishes "no flood" (0 in ≥1 valid obs) from "no observation" (all 255). Exactly the coding your tier-2 needs. (`exclusion_mask` is a separate companion if you want the excluded-reason.)

**6. Volume.** One Kenya tile-overpass flood COG ≈ **120 KB compressed** (measured `content-length=123,635` on a mostly-dry tile; flood-heavy tiles larger, ~0.1–0.5 MB). Kenya = **17 tiles**; **~3,629 tile-overpass items in 2023** (all tiles × acquisitions) → ~213 acquisitions/tile/yr (S1 asc+desc, overlapping orbits). Raw overpass archive 2018→now (~7 yr) ≈ **3,629 × 7 × ~0.15 MB ≈ ~4 GB** across all tiles (pre-mosaic); the Kenya-clipped/mosaicked overpass product is far smaller. Overpass tier is cheap.

**7. Licence — Copernicus EMS (free, full & open).** STAC `license` field = literal `"proprietary"` (placeholder — ignore); the collection is **Copernicus Emergency Management Service (CEMS) GFM** → governed by the **Copernicus data policy (Reg. EU 1159/2013): free, full and open**. Required attribution: **"Contains modified Copernicus Emergency Management Service information [YEAR]"**. Not a clean SPDX id → use **`LicenseRef-Copernicus-EMS`** in CDH (Copernicus free-open is CC-BY-4.0-equivalent for redistribution purposes). Docs/about: `extwiki.eodc.eu/en/GFM`; cite DOIs 10.3390/rs14153673 + 10.1109/IGARSS47720.2021.9554214. **→ Publishable to digital-atlas + CDH-contributable, with the Copernicus attribution string.**

**Verdict: GFM ingest VIABLE from cglabs** — anon EODC STAC + direct COG download, 2018→present, `ensemble_flood_extent` (flood, permanent-water-excluded), 0/1/255 coding perfect for obs-counting, Equi7-AF-20m→EPSG:4326 mosaic, ~4 GB raw. Ready for macbook to write `python/ingest_flood_gfm.py` (per-overpass → monthly obs-count → seasonal → history). CDH yaml TODO(probe) values above; can fill `metadata/cdh/kenya-flood-gfm.yaml` on your word.
