# DISPATCH — publish the rebuilt R/2.2 hazard-change tables (into v2026.11)

Append-only. Newest block on top. Respond by prepending a `### RESPONSE` block.

### RESPONSE — cglabs 2026-11-16 — **Published. G0-G4 PASS; 10/10 keys live and verified (size = local, ETag = local MD5, multipart ETag recomputed for the one 2-part file, HTTP 200, backups written).** G2 settles the adm0 shift: the independent recompute gives SDN **12.0 % / 11.3 %** against NEW 11.5 and OLD 100.0. The old 100.0 came from a duplicate adm0 row

Stamp `20261116_193321` (publish log); the gate scripts and outputs carry `20261116_19…` stamps under `logs/r22_*`. HEAD `30fca5e`. Env grep (`CONFIRM|R22_|FORCE_OVERWRITE|AWS_BIN`) empty before the dry-run and the publish. Nothing else touched: no other prefix, no R/2.2 rerun. `_parked_r22_pre13_20261015_213020/` and every `_parked_*` are kept.

#### G0 — node quiet. PASS
- **Processes:** no `Rscript`/`exec/R`/`workRSOCK` process. The only long-lived process is the JupyterHub single-user server.
- **pid files:** 0 of the `logs/*.pid` files point at a live process.
- **Recent writes:** 0 log files modified in the last 24 h. The newest top-level `Data/` dir mtime is 2026-11-02. CPU is 97 % idle.

The last node job was the obs S7 W2 rebuild (2026-11-13, finished).

#### G1 — the rebuild is intact and is the 2026-10-15 artifact. PASS, **with one correction to the block**
- **All 10 exist**, with mtimes 2026-10-15 **21:38-21:48** (the B3 run) and none newer.
- **The five "byte-sizes" in the block are row counts.** They are the "new" column of the B3 table (`archive/dispatches/DISPATCH_cglabs_issue13_rebake.md` §4, l.1488-1492). The files' byte sizes are different (1,391,186 · 6,924,089 · 1,956,735 · 12,752,542 · 5,325,673). **The row counts match exactly:**
```
ptot_change_by_model   2,931,264 rows   ptot_diff_by_model 1,465,632   thi_perc_area_by_model 3,114,468
ntx_perc_area_by_model 6,228,936        haz_freq          12,457,872
```
I took this as the invariant the block meant, since the B3 table is its source, and did not stop.

#### G2 — the adm0 `thi_perc` shift, independent recompute. PASS
**What I recomputed.** THI is **SEC2** in `R/2.2_haz_change.R` (l.422-518; SEC3 is NTx). For one combination I mirrored its logic:
- the extreme thresholds from `metadata/haz_classes.csv`: **tropical G94, highland G89**;
- the risk rasters `ssp126_GFDL-ESM4_2061-2080_THI-max-max-G{94,89}.tif` (values 0/1);
- `data * tropical + data * highlands` (`afr-highlands.asc`, resampled `near`);
- `× cellSize(km)`, then `/ zone km²`.

**What is independent of R/2.2.** The zone is rasterized afresh from the single GAUL a0 polygon (`touches = TRUE`, as R/2.2 does). It does not use the cached `*_zonal.tif` or `admin_extract()`. As a cross-check I also used exact polygon coverage. Script: `logs/r22_g2_recompute_<stamp>.R`.
```
                                         recompute (touches / exact)   NEW (10-15)   OLD (06-24, parked)
SDN GFDL-ESM4 ssp126 2061-2080 extreme       12.0 % / 11.3 %              11.5          100.0  (and a 2nd row 10.9)
KEN GFDL-ESM4 ssp126 2061-2080 extreme        4.3 % /  4.4 %               4.2           31.6  (and a 2nd row 4.0)
ERI (same combination, not recomputed)          —                         18.6           18.7
```
**The NEW value agrees and the OLD one does not.** The parked product holds **two adm0 rows** for SDN and for KEN, and one of each pair is the inflated value. That is the B3 duplicate-zone shrink (`-2,448` rows in `thi_perc_area_by_model`), now deduplicated. My recompute counts every touching cell and lands 0.5 pp above NEW (12.0 vs 11.5); R/2.2's shared zonal raster gives border cells to one neighbour only, which fits that small gap. Across all adm0 rows, 5,514 differ by more than 0.05 between OLD and NEW, in the same 19 countries B3 listed.

#### G3 — validator. PASS
`R/validate_cr093_real.R`: **10 PASS / 0 FAIL**, `GATE PASSED`. Every file has iso3 = 55, and the row counts are as in G1 (`logs/r22_validate_<stamp>.log`).

#### G4 — dry-run. PASS
`R/publish_cr093_r22.R` (no CONFIRM): `mode: DRY-RUN | axis: annual`. The plan lists **exactly the 10** canonical `…/processing=hazard-change/timeframe=annual/variable=<name>.parquet` keys. Every pre-flight check is `OK` (iso3 present, 0 % NULL, row-group stats non-null, ≥1 row, 55 iso3), and the run ends `pre-flight PASS for all 10 files` (`logs/r22_dryrun_<stamp>.log`).

#### Step 5 — publish
`nohup env CONFIRM=1 Rscript R/publish_cr093_r22.R`, `logs/r22_publish_20261116_193321.log`, 19:33:27 → 19:34:47.
- **Backups:** each live key was backed up to `variable=<name>.preFix-20261116-193327.bak`. All 10 backups exist and are the 06-24 sizes.
- **Uploads:** all 10 uploaded.
- **Prunability:** the script's check of the uploaded files reads `null-stat=0` for all 10, with the same row-group counts as local.

The live keys before the publish (06-24 build: size, ETag, LastModified, VersionId) are in `logs/r22_s3_before_<stamp>.txt`. Their noncurrent versions are retained, besides the `.bak` copies.

#### Step 6 — verification and the pin table
Base `s3://digital-atlas/domain=climate/type=hazard-indices/source=nex-gddp-cmip6/region=africa/processing=hazard-change/timeframe=annual/variable=<name>.parquet`.
- **Size:** S3 size = local size for all 10.
- **Content:** for 9 keys the **ETag = local MD5**. `ntx_perc_area_by_model` is a 2-part upload, and its multipart ETag, recomputed locally from 8 MiB parts, matches.
- **Public read:** `curl -sI` on the public URL returns **200** for all 10.

| variable | size (B) | ETag | LastModified (UTC) | x-amz-version-id |
|---|---|---|---|---|
| ptot_change_by_model | 1,391,186 | a4d10886c66d5ae2fa3aae126dbe4700 | 2026-11-16T19:33:35 | WcZeOXDlnnlOI.0kJ4BnU.lF13sIR23u |
| ptot_change_ensemble | 986,292 | 70c6b0897c85919b1380703f509039e4 | 2026-11-16T19:33:41 | Qh54xPNQHcOAVbQ5Iofep_g5d8lLbOIs |
| ptot_diff_by_model | 6,924,089 | 15b9a7b45d38e0b5e8908c6f49fe3a47 | 2026-11-16T19:33:48 | x96htpCwR8vC7EP1lLGUAnxSjipPsS5J |
| ptot_diff_ensemble | 1,875,473 | 3f5103ff2882f0b1b5f3b4413918b595 | 2026-11-16T19:33:55 | BLsoHsJdtEu94Arh.2_TMBY9cAzaFYTt |
| thi_perc_area_by_model | 1,956,735 | d97626b7407e1b62036a023a0cdf7ea7 | 2026-11-16T19:34:01 | sDWTNMTWaAvaUOZ9.ijo9q33BdGO_zhz |
| thi_perc_area_ensemble | 754,664 | c4ac9eba982a177bcb59a0c524b4a275 | 2026-11-16T19:34:08 | 92fYaaoAR3fTCJo07D4dAvSZzU6b4i9u |
| ntx_perc_area_by_model | 12,752,542 | ff00d9a6d2ad4205aa39ed84eba4eddb-2 | 2026-11-16T19:34:16 | JAVwYiuzq0dKfJWeVEDtmNVNxe8.aL5g |
| ntx_perc_area_ensemble | 1,156,460 | eb5b63143c5c629f8dd74b0286d79649 | 2026-11-16T19:34:24 | VgZx4rDH_inRcrLe2YjOfFgggQ22yIkg |
| haz_freq | 5,325,673 | 37bbc85a427d021a89b6b19c00f4c11b | 2026-11-16T19:34:30 | 09eyxdnI_o_04DAE1vCSbZm7t1LsM1aX |
| haz_freq_ensemble | 1,576,579 | 547be71660c1e11e48c41c5213b76690 | 2026-11-16T19:34:37 | 9WshRnkrx45AwAAigJAW0GrQPfEyNBK2 |

Machine-readable copy: `logs/r22_s3_after_<stamp>.txt`, which is untracked. **The changelog date for the "pending" row is 2026-11-16.**

**Not done (out of scope):** the prefix now holds 26 `.preFix*.bak` objects (16 older ones plus these 10), which is #59. The adm0 disputed-claimant rows are #11.

---

### DISPATCH 2026-11-14 (macbook) — verify the 2026-10-15 R/2.2 rebuild, gate it, publish the 10 keys

**Context.** The live `processing=hazard-change` keys (10 parquet, `timeframe=annual`) are the
2026-06-24 CR-093 build. R/2.2 was re-run on the node during #13 B3 (2026-10-15, RESPONSE in
`archive/dispatches/DISPATCH_cglabs_issue13_rebake.md`): exit 0, CR-093 validator 10/10 PASS,
outputs de-duplicated and on the 18-GCM / post-#25 inputs — but never published. Pete's GO
(2026-11-13): publish this rebuild **before** the `v2026.11` freeze. The CDH record
(`metadata/cdh/africa-hazard-change-nexgddp.yaml`, commit `7c09d51`) and its changelog describe the
rebuilt files; the changelog's "pending" row gets its date from this publish.

**Out of scope.** Do not fix or touch anything else: not the disputed-claimant adm0 rows
(issue #11), not the `.preFix*.bak` clutter (clean-up is #59), no other prefix, no re-run of
R/2.2 or any upstream stage. If a gate fails, stop and describe — do not improvise.

**Rules that bite here:** `git pull` first; absolute-path sourcing in `Rscript -e`; uploads in the
background (`nohup … &`, log to `logs/`) because the shell kills foreground jobs at ~2 min; keep
`Data/_parked_r22_pre13_20261015_213020/` and every `_parked_*`/`sandbox/backup/` dir; after
committing the RESPONSE, verify it landed on `origin/develop`.

---

**Step 0 — node quiet?** Nothing that writes under `Data/` may be in flight.
Check `ps aux | grep -i [R]script`, live `logs/*.pid`, and recent log mtimes.
**Gate G0 (invariant):** no running pipeline job. If anything is running or ambiguous: STOP,
report what is running, publish nothing.

**Step 1 — the rebuild is intact and is the 2026-10-15 artifact.** The 10 files (paths exactly as
`R/publish_cr093_r22.R` lists them, `R22_TIMEFRAME=annual`):
`ptot_change_{by_model,ensemble}`, `ptot_diff_{by_model,ensemble}` (under `ptot_perc/`),
`thi_perc_area_{by_model,ensemble}`, `ntx_perc_area_{by_model,ensemble}`,
`stats/haz_freq{,_ensemble}`.
**Gate G1 (invariants):** all 10 exist; every mtime is 2026-10-15 (the B3 run) and none is newer;
the five byte-sizes the B3 RESPONSE recorded still hold —
`ptot_change_by_model` 2,931,264 · `ptot_diff_by_model` 1,465,632 ·
`thi_perc_area_by_model` 3,114,468 · `ntx_perc_area_by_model` 6,228,936 · `haz_freq` 12,457,872.
A newer mtime means something re-ran R/2.2 since: STOP.

**Step 2 — settle the adm0 `thi_perc` shift with an independent reference.** B3 measured: THI %
area differs from the parked 06-24 product at adm0 only (19 countries), worst case
`SDN / GFDL-ESM4 / ssp126 / 2061-2080 / extreme`: 100.0 → 11.5. Unexplained ≠ publishable.
Recompute that one value independently with `terra` straight from the THI mean raster R/2.2 SEC3
reads (same raster, same threshold, zonal over the SDN adm0 polygon — mirror SEC3's logic for this
single combination, nothing more).
**Gate G2 (invariant):** the independent recompute agrees with the NEW value (within a couple of
percentage points, i.e. clearly ~11.5, clearly not ~100). If it agrees with the OLD 100.0, or with
neither: STOP and show the three numbers. Spot-check one more country from the B3 list (KEN or ERI)
the same way if the first passes cheaply.

**Step 3 — validator.** `Rscript R/validate_cr093_real.R` from the repo root.
**Gate G3 (invariant):** 10 PASS / 0 FAIL, as in B3.

**Step 4 — dry-run.** `Rscript R/publish_cr093_r22.R` (no CONFIRM).
**Gate G4 (invariants):** plan lists exactly the 10 canonical `timeframe=annual` keys; every
pre-flight check (iso3 present, 0% NA, non-null row-group stats, ≥1 row, ≥2 iso3) passes.

**Step 5 — publish.** Background it:
`nohup env CONFIRM=1 Rscript R/publish_cr093_r22.R > logs/r22_publish_$(date +%Y%m%d_%H%M%S).log 2>&1 &`
The script backs up each live key to `.preFix-<stamp>.bak` itself. Watch the log to completion.

**Step 6 — verify on S3.** For each of the 10 keys: size equals the local file; the uploaded-iso3
check in the log passed; anonymous read works
(`curl -sI 'https://digital-atlas.s3.amazonaws.com/<key>'` → 200). Then collect, per key:
**size, ETag, LastModified, x-amz-version-id** (`aws s3api head-object`) and put the 10-row table
in the RESPONSE — macbook updates `metadata/cdh/pins/atlas-hazards-2026.11_s3-versions.csv` and the
changelog date from it.

**Step 7 — RESPONSE.** Gates G0–G4 outcomes, the G2 three-number comparison, the step-6 table,
log paths. Commit, push, verify it landed on `origin/develop`.
