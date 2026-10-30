# DISPATCH — cglabs — rangeland FPAR track: re-ingest on this node (relay from PASCAL is not possible), validate, then S6 against the S3 stacks

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### MACBOOK 2026-10-30 — K0-K2 now, in parallel with the J chain; K3 after J6's annual12 stacks exist

**Why here.** PASCAL ran the full-Africa FPAR ingest + S6 cleanly (`DISPATCH_pascal_obs_fpar.md`, `dd892bb`), but it has
no AWS credentials and no S3 client, so its 143 MB cannot be relayed without putting bucket credentials on a shared
box. Re-ingest here instead: the run is **network-bound** (JRC served PASCAL at ~0.48 MB/s → 29.5 h for 756 dekads),
not CPU-bound, so it coexists with J2-J6. PASCAL's copy becomes the cross-node gate (K2).

**Rules.** Same as the S3 dispatch: writes only under `OR={common_data}/atlas_obs_hazards` (`Data/obs_fpar/`,
`manifests/`), never under the NEX trees; background with logs in `logs/obs_fpar_<step>_<STAMP>.log` + `.pid`;
`env | grep -E 'FORCE_OVERWRITE|DAYS_CHECK|AVAIL_SEED|R3_|ATLAS_CLIMDAT'` empty at launch. `PY` = the obs env python
(`pyobs`). Public repo: no IPs, hostnames, mount identifiers, usernames.

#### K0 — selftests
```bash
cd $REPO && git pull --ff-only && git log -1 --format='%h %s'
$PY python/obs/fpar_ingest.py selftest            # ALL FPAR SELFTEST ASSERTIONS PASSED (10)
Rscript R/obs/s6_rangeland_fpar.R --selftest     # ALL S6 SELFTEST ASSERTIONS PASSED (16)
```
**Stop** on any failure.

#### K1 — full ingest (background, resumable; ~30 h if JRC serves this node like PASCAL — report the rate)
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup $PY python/obs/fpar_ingest.py ingest --out-root $OR --years 2005:2025 --workers 4 \
      > logs/obs_fpar_ingest_$STAMP.log 2>&1 &
echo $! > logs/obs_fpar_ingest_$STAMP.pid
```
Default `--min-age 110`. Static layers first (mask ~20 min, phenology joint mode ~10 min on PASCAL). Any `FAIL`
goes to `$OR/manifests/fpar_failed_<stamp>.csv` and stops the run — stop and describe. Transient `RasterioIOError`
absorbed by the retry are fine (PASCAL saw 5). When done:
```bash
$PY python/obs/fpar_ingest.py validate --out-root $OR --years 2005:2025     # expect PASS; versions {V062: 477, V070: 279}, break 2018-04-01, non-CF 0, drift 0
```
Report: dekads written, wall-clock, s/dekad, the `phenology:` log line, the validate block, `du -sh $OR/Data/obs_fpar`.

#### K2 — cross-node gate against PASCAL (invariant: same code, same inputs → identical bytes)
PASCAL posts its per-file md5 list in its next RESPONSE (asked today). Compare your manifest's latest-row md5 per
file against it: **expected 762/762 identical** (756 dekads + 6 statics). Report the count and list any mismatch
with both md5s. A mismatch is a **stop** (it would mean JRC rewrote a dekad between the runs — then compare the
`last_modified`/`etag` columns and say which side is newer). Until PASCAL's list arrives, report your own digest:
latest manifest row per file, sorted by path, md5s concatenated and md5-hashed.

#### K3 — S6 against the real stacks (only after J6 has written `$OR/Data/obs_seasonal/chirps-chirts-agera5_2006-2025/annual12/{THI_max_max,NDWL0_mean}.tif`)
```bash
nohup Rscript R/obs/s6_rangeland_fpar.R --obs-root $OR --years 2005:2025 --token chirps-chirts-agera5 --rl-min 0.1 \
      > logs/obs_fpar_s6_$STAMP.log 2>&1 &
```
No `--no-thi`: S6 must find the stacks and write under `Data/hazard_risk/ggcmi-crop/` (30 bundles, set
`FPARz+THI-max+NDWL0`). Expect the PASCAL figures to reproduce exactly for the dry axis: 24,015 rangeland cells,
0 without a season, 0 ties, P(moderate) = 4/19 everywhere, 2011 the worst year. New here: the heat/wet slots are
real — report the domain share of the `dry+heat` vs `dry` classes for `cattle-tropical_moderate`. If the stacks are
absent, S6 stops by design — that is the signal to wait for J6, not to pass `--no-thi`.

**Stop and describe** on: a selftest failure; any `FAIL` row; validate FAIL; a K2 mismatch; S6 WARN lines
(ties or no-season cells > 1 %).

#### Then
Prepend `### RESPONSE` (K0-K2 when the ingest finishes; K3 later, its own block), commit only this file, push,
verify on `origin/develop`, delivery line.
