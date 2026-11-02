# DISPATCH — cglabs — rangeland FPAR track: re-ingest on this node (relay from PASCAL is not possible), validate, then S6 against the S3 stacks

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### RESPONSE — cglabs 2026-11-02 (2) — **K3a and K3b both PASS.** Every PASCAL figure reproduces: 24,015 rangeland cells, 0 without a season, 0 ties, P(moderate) = 0.211 = 4/19 everywhere, 30 bundles; and K3b's compare line is **exact** — Jaccard 0.615, trend +0.000, worst 2011,2015,2017. Finding: the dry axis almost never appears alone, because moderate THI-max is near-saturated. `Data/hazard_risk/ggcmi-crop` now holds **189** bundles.

Stamp `20261102_215657`. Run after `DISPATCH_cglabs_obs_s3_r3.md` RESPONSE (6): L3 was clean, and the `annual12` stacks S6 reads were rewritten by that rerun at 20:56, so S6 ran against post-soil-fill inputs.

#### K3a — V1, `min`, not detrended. PASS, 9.6 min.

| expected | measured |
|---|---|
| 24,015 rangeland cells (`rl >= 0.1`) | **24,015** |
| 0 cells without a valid season-1 window | **0** |
| 0 ties at the quantile | moderate **0**, severe **0**, extreme **0** |
| P(moderate) = 4/19 in every cell | min **0.211**, median **0.211**, max **0.211** |
| 30 bundles, set `FPARz+THI-max+NDWL0` | **30**, under `Data/hazard_risk/ggcmi-crop` |

No `WARN`. Also logged: season 1 length 8..33 dekads on 31,310 cells, season 2 on 3,625 (7..18); 110 two-season cells whose season 2 falls in the following calendar year; `deficit_q20` median −0.053 over rangeland cells, worst year median −0.117; cells with fewer than 19 distinct z: 0; consecutive-failure domain mean 0.052 against the no-persistence expectation 0.035.

(One caution for anyone grepping these logs: the diagnostic line `consecutive-failure frequency` contains the string `fail`. It is not an error.)

#### The heat/wet slots are real, and heat is near-saturated

Domain mean frequency per class for `cattle-tropical_moderate`, over the 24,014 valued cells:

| class | freq |
|---|---|
| any | 0.99963 |
| none | 0.00037 |
| **dry** | **0.00007** |
| heat | 0.53880 |
| wet | 0.00096 |
| **dry+heat** | **0.15078** |
| dry+wet | 0.00027 |
| heat+wet | 0.24934 |
| dry+heat+wet | 0.05941 |

**`dry+heat` / `dry` = 2154.** A dry rangeland year essentially never occurs without a moderate heat flag: moderate THI-max is exceeded in about 99.8 % of cell-years for cattle-tropical (heat 0.539 + heat+wet 0.249 + dry+heat 0.151 + dry+heat+wet 0.059), so the dry signal is almost entirely absorbed into the combination classes.

The dry axis itself is intact and the arithmetic closes:
- dry marginal = 0.00007 + 0.15078 + 0.00027 + 0.05941 = **0.21053**, i.e. 4/19 to five figures, exactly the baseline;
- `any + none` = **1.00000**.

So this is not a defect, but it is worth knowing before anyone reads the single-hazard `dry` layer as "drought frequency on rangeland" — at `moderate` it is nearly empty by construction, and the information sits in `dry+heat`. Whether the moderate THI-max threshold is the right one for a rangeland product is a science question, not a pipeline one; flagging it, not acting on it.

#### K3b — the `min --detrend` companion. PASS, 1.7 min.

`SENSITIVITY variant 'min-detrend' -> Data/obs_fpar/seasonal/2006-2025_min-detrend (no bundles)`, and indeed no bundles were written. 0 WARN. The same structural figures as K3a (24,015 cells, 0 without a season, 0 ties, P(moderate) 0.211), consecutive-failure domain mean 0.040 against 0.035.

Per start year (mean z | share flagged): 2006 0.017|0.177; 2007 −0.015|0.187; 2008 0.054|0.140; 2009 0.072|0.174; 2010 0.074|0.171; **2011 −0.373|0.313**; 2012 0.007|0.206; 2013 −0.017|0.162; 2014 −0.211|0.234; **2015 −0.340|0.309**; 2016 −0.069|0.220; **2017 −0.267|0.294**; 2018 −0.050|0.220; 2019 0.028|0.196; 2020 0.198|0.124; 2021 −0.211|0.253; 2022 −0.202|0.281; 2023 0.003|0.210; 2024 0.202|0.130.

**The compare reproduces PASCAL exactly:**

```
min-detrend  Jaccard 0.615 | keeps 0.761 of V1 | rho(mean z) 0.504 | trend +0.000 z/decade | splice 0.353 (expect 0.368) | worst 2011,2015,2017
min (V1)     (reference)                                            trend +0.273 z/decade | splice 0.299 (expect 0.368) | worst 2011,2006,2015
```

Jaccard **0.615**, trend **+0.000**, worst **2011,2015,2017** — all three as predicted. Written to `Data/obs_fpar/seasonal/variant_compare_2006-2025.csv`.

#### State, and one thing to decide before the publish

`$OR/Data/hazard_risk/ggcmi-crop` now holds **189** `*_int.tif` — the 159 from L2 plus K3a's 30 `FPARz+THI-max+NDWL0` bundles. So:

- the L3 publisher dry-run I reported as clean covers the **159**, and its candidate tiers do **not** contain the rangeland bundles;
- **any R/3 rerun from here reads 189**, and §4.1 will say so. That is correct and expected, not drift — but the tiers would then carry the rangeland set, and G6b would judge `cattle-*`/`sheep-*`/`goats-*` against an exposure basis that has no FPAR commodity. Whether the rangeland bundles belong in the same tier files, or in their own key, is your call and I have not assumed either.

Nothing running. Nothing published. No S3 write of any kind in this block.

---

### MACBOOK 2026-11-02 — K3 adds the companion detrended run (Pete's decisions a/b/c)

Pete decided the rangeland design today (evidence: PASCAL G0-G3, `7187826`): V1 yearly score `min` stays; the primary
product is **not** detrended; a **`min --detrend` companion** ships as a sensitivity layer; magnitude layers are the
product. So K3 is two runs, in this order, after J6's annual12 stacks exist:
```bash
# K3a — V1 (bundles, as already written in K3 below)
nohup Rscript R/obs/s6_rangeland_fpar.R --obs-root $OR --years 2005:2025 --token chirps-chirts-agera5 --rl-min 0.1 > logs/obs_fpar_s6_$STAMP.log 2>&1 &
# K3b — companion (no bundles; writes seasonal/2006-2025_min-detrend/)
nohup Rscript R/obs/s6_rangeland_fpar.R --obs-root $OR --years 2005:2025 --token chirps-chirts-agera5 --rl-min 0.1 --score min --detrend > logs/obs_fpar_s6det_$STAMP.log 2>&1 &
Rscript R/obs/s6_variant_compare.R --obs-root $OR --timeframe 2006-2025 --variants min-detrend
```
Expect K3b's compare line to reproduce PASCAL's exactly (Jaccard 0.615, trend +0.000, worst 2011,2015,2017), because the
inputs are byte-identical. A different number is a stop. Report both in the K3 RESPONSE.

### RESPONSE — cglabs 2026-11-02 — K0 PASS (10 + 16); K1 756/756 dekads in 1.9 h, validate PASS; **K2 PASS: 762/762 files byte-identical to PASCAL** (list digest `895eddeb1ca676420fdb402b5b04d1a3` on both nodes); K3 waits for J6

Pull `b645c96`. Stamp `20261031_074742`. The env grep was empty at launch. `PY` = `pyobs`.

**K0:**
- `fpar_ingest.py selftest`: 10 `ok`, `ALL FPAR SELFTEST ASSERTIONS PASSED`.
- `s6_rangeland_fpar.R --selftest`: 16 `ok`, `ALL S6 SELFTEST ASSERTIONS PASSED`.

**K1** (`logs/obs_fpar_ingest_<stamp>.log` + `.pid`, `--workers 4`, default `--min-age 110`):
- Start 2026-10-31 07:49:11, `756/756 dekads written` and `ingest done` at 09:45:20. **Wall-clock 1 h 56 min (6,969 s),
  9.2 s/dekad**, about 15× faster than PASCAL's 29.5 h.
- The manifest's source `size` column sums to 230.5 GB, i.e. 33 MB/s nominal. The reads are windowed, so the bytes
  actually transferred are lower. I did not measure them.
- 0 `FAIL`, 0 `RasterioIOError`/retry lines. No `fpar_failed_*.csv`.
- Statics: rangeland mask 07:49:12 → phenology started 07:50:06; phenology done 07:51:36.
- `phenology: 31310 rangeland cells with a season; 3625 with two; s1 range 10-67; OND-first cells (s2 in a later frame-year than s1; S6 pairs them with the next calendar year): 110`
  - `rl_min=0.0` at ingest; S6 applies `--rl-min 0.1`, hence PASCAL's 24,015 there.
- Manifest `fpar_manifest_20261031_074911_<pid>.csv`: 6 static + 756 dekad rows.

**Validate** (`logs/obs_fpar_validate_<stamp>.log`):
```
cutoff 2026-07-13 (from the manifests); dekads present 756/756
product versions: {'V062': 477, 'V070': 279}; version break(s) at ['2018-04-01']; non-CF dekads: 0
server drift: 0 of 756 present dekads were modified upstream since ingest
rangeland FPAR x100 domain mean over dekads: min 36.5 median 40.0 max 45.5
VALIDATE PASS
```
`du -sh $OR/Data/obs_fpar`: 134M.

**K2: cross-node gate against PASCAL's list** (`DISPATCH_pascal_obs_fpar.md`, RESPONSE 2026-10-31, `26b5577`):
- Latest manifest row per file: 762 here, 762 in PASCAL's list. **762/762 md5 identical; 0 only-here, 0 only-PASCAL.**
- My digest (md5s concatenated in path order, md5-hashed): `895eddeb1ca676420fdb402b5b04d1a3`, identical to PASCAL's.
- All 762 files re-hashed on disk against the manifest: 0 mismatches.

**K3:** waits for J6's `annual12/{THI_max_max,NDWL0_mean}.tif`. J6 is held behind the S3 J3 bundle-count stop (see
`DISPATCH_cglabs_obs_s3_r3.md`).

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
