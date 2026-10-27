# DISPATCH — cglabs — observed pipeline S2 → S3-S5 → R/3 §4 → publisher dry-run, smoke first, then full

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### MACBOOK 2026-10-27 — runs AFTER `DISPATCH_cglabs_obs_s1_ingest.md` I0-I3 are green. Smoke on the Kenya box, then the full Africa chain.

**What landed since the S1 dispatch (`git pull`):** `R/obs/s3_s5_obs_hazards.R` (per-crop windows → per-season
codes → R/3 bundles; selftest 20/20), `metadata/obs_crop_windows.csv`, and the R/3 + publisher per-source
plumbing (`69e0c55`): `ATLAS_CLIMDAT_SOURCE=obs`, `R3_MODEL_ALLOW`, `--source`. All 14 fixtures pass.

**Rules.** Writes only under the two obs roots. Never under the NEX trees. Background everything with logs
in `logs/obs_s3_<step>_<STAMP>.log`. `env | grep -E 'FORCE_OVERWRITE|DAYS_CHECK|AVAIL_SEED|R3_|ATLAS_CLIMDAT'`
must be empty before each launch (set what each step needs on its own command line). Public repo: no IPs,
hostnames, mount identifiers or usernames.

`SM={common_data}/atlas_obs_hazards_smoke`, `OR={common_data}/atlas_obs_hazards`, `PY` = the obs env python,
`REPO` = this checkout, `NWD={common_data}/nex-gddp-cimp6_hazards` (the NEX working dir).

#### J0 — fixtures on the node
```bash
cd $REPO && git pull --ff-only && git log -1 --format='%h %s'
for f in R/checks/fixture_publish_tier_gates.R R/checks/fixture_upstream_run_params.R; do Rscript $f > logs/obs_s3_fx_$(basename $f .R)_$STAMP.log 2>&1 && echo PASS $f || echo FAIL $f; done
Rscript R/obs/s3_s5_obs_hazards.R --selftest > logs/obs_s3_selftest_$STAMP.log 2>&1 && echo PASS selftest
```
**Stop** on any FAIL.

#### J1 — smoke S1: extend the Kenya box to 2013-2016 (S3 needs ≥ 3 data years; 2015 is already there and is skipped)
```bash
nohup $PY python/obs/s1_ingest.py ingest --smoke --out-root $SM --years 2013:2016 --rh both > logs/obs_s3_s1smoke_$STAMP.log 2>&1 &
$PY python/obs/s1_ingest.py validate --smoke --out-root $SM --years 2013:2016            # PASS, 1461 days per var
$PY python/obs/s1_ingest.py link --out-root $SM --common-data {common_data} --nex-working-dir $NWD
```
Report: `Data/mapspam`, `Data/GLW4_2020`, `Data/boundaries` now resolve under `$SM`; which `glw4-2020_vop_*_res-25.tif`
and `spam_*_res-25.tif` files exist there (R/3 reads the `res-25` tag because the base raster is 0.25°).

#### J2 — smoke S2 (unchanged upstream scripts), strict, seeded at the first year
```bash
cd $REPO/hazards_upstream/R/04_indices
export COMMON_DATA=$SM SCENARIO=historical YRS=2013:2016 DAYS_CHECK=strict AVAIL_SEED_MONTHS=2013-01
GCMS=chirps-chirts-agera5      nohup Rscript fast_calc_NDWS.R  > $REPO/logs/obs_s3_s2_ndws_$STAMP.log 2>&1 &
GCMS=chirps-chirts-agera5      nohup Rscript fast_calc_NDWL0.R > $REPO/logs/obs_s3_s2_ndwl0_$STAMP.log 2>&1 &
GCMS=chirps-chirts-agera5      nohup Rscript calc_NTx.R        > $REPO/logs/obs_s3_s2_ntx_$STAMP.log 2>&1 &
GCMS=chirps-chirts-agera5      nohup Rscript calc_THI.R        > $REPO/logs/obs_s3_s2_thi_$STAMP.log 2>&1 &     # variant (i): hurs = mean of 5 local times
GCMS=chirps-chirts-agera5-rh15 nohup Rscript calc_THI.R        > $REPO/logs/obs_s3_s2_thi_rh15_$STAMP.log 2>&1 &  # variant (ii)
unset COMMON_DATA SCENARIO YRS DAYS_CHECK AVAIL_SEED_MONTHS
```
**Invariants:** every script rc 0; 48 monthly files per index (NTx: 31 dirs × 48; THI: 48 `THI_max-` + 48 `THI_mean-`);
no `WARN` / `daily inputs missing`; Kenya-box NDWS monthly means vary seasonally with PTOT (April and November dips) and no month is 0 or 31 in every land cell (the 2-28 band in the S1 dispatch was too tight for an arid box; an un-converted radiation unit would make every day a stress day); the two THI tokens differ
(RH15 < daily-mean RH, so THI(ii) < THI(i) on average: report the box-mean difference).

#### J3 — smoke S3-S5
```bash
cd $REPO
nohup Rscript R/obs/s3_s5_obs_hazards.R --obs-root $SM --token chirps-chirts-agera5 --years 2013:2016 --spinup 1 \
      --ggcmi-dir $NWD/Data/ggcmi > logs/obs_s3_s35_$STAMP.log 2>&1 &
nohup Rscript R/obs/s3_s5_obs_hazards.R --obs-root $SM --token chirps-chirts-agera5 --thi-token chirps-chirts-agera5-rh15 \
      --years 2013:2016 --spinup 1 --sets livestock --ggcmi-dir $NWD/Data/ggcmi > logs/obs_s3_s35_rh15_$STAMP.log 2>&1 &
```
(Confirm the GGCMI dir: `ls $NWD/Data/ggcmi/*_rf_*.nc4 | wc -l` should be 20.) Report the per-window
`months …; cells <3 months; =12; NA on land` lines, the `maize calendar NA on N land cells` line, and:
- `ls $SM/Data/hazard_risk/ggcmi-crop/*_int.tif | wc -l` — expect 34 crops × 3 + 10 livestock × 3 × 2 tokens = **162**;
- one bundle opened with terra: 9 layers, names of the form `observed_<token>_2014-2016_<class>_<set>_<crop>_<sev>`;
- the §3b check: over Kenya-box land cells, the share of `annual12` cells (livestock bundles) whose severe `dry`
  marginal (dry + dry+heat + dry+wet + dry+heat+wet) is exactly 0 or exactly 1 — report the number, it is a
  diagnostic, not a gate.

#### J4 — smoke R/3 §4 (the observed bundles through the unchanged exposure multiply + zonal sum)
```bash
cd $REPO
ATLAS_CLIMDAT_SOURCE=obs R3_MODEL_ALLOW='chirps-chirts-agera5' R3_TIMEFRAMES=ggcmi-crop EXPOSURE_RES=0.25 \
  nohup Rscript -e 'source("/abs/path/to/repo/R/0_server_setup.R"); source("/abs/path/to/repo/R/3_freq_x_exposure.R")' \
  > logs/obs_s3_r3_smoke_$STAMP.log 2>&1 &
```
(`R3_MODEL_ALLOW='chirps-chirts-agera5'` also matches `…-rh15`; both tokens flow.) Setup will create empty
`Data/<subdir>` folders under `$SM` — that is expected. **Invariants:**
- §4.1 logs `input files = 162` (not 0: zero means the allow-list did not take);
- parquets under `$SM/Data/hazard_risk_{vop,vop_usd,ha,prod,n}/ggcmi-crop/`, model tokens `chirps-chirts-agera5`
  and `…-rh15` in the file names, `scenario = observed`, `timeframe = 2014-2016`;
- the sidecar JSON has `ensemble.n_members = 0` and an `ensemble.provenance` block listing both tokens;
- for Kenya (admin0 row): per crop and `hazard_vars`, the seven classes sum to `any` and `any + none` equals
  the exposure denominator's `prod_t` for that crop within rounding (the G6b identity) — compute it for maize,
  cassava, cattle-tropical.

#### J5 — publisher dry-run on the smoke (gates only; nothing uploaded)
```bash
ATLAS_CLIMDAT_SOURCE=obs Rscript scripts/r3_publish_tiers.R --dry-run --source chirps-chirts-agera5 --timeframe ggcmi-crop \
   --model chirps-chirts-agera5 --variables prod_t --tiers severe --no-exposure-gate > logs/obs_s3_pub_dry_$STAMP.log 2>&1
```
Expect G1 to **FAIL on size** (a Kenya-box parquet is far below 10 MB) — report that it fails for that reason
and that G3 reads `scenarios = observed`. That is the smoke's job; the full run is gated for real.

#### J6 — the full Africa chain (after J0-J5 are clean)
Same commands with `$OR`, `--years 2005:2025`, `AVAIL_SEED_MONTHS=2005-01`, no `--smoke`, `--spinup 1`,
two S2 waves (NDWS must run months in order; NDWL0 / NTx / THI × 2 tokens in parallel), then S3-S5, then R/3.
Then the publisher **dry-run with the exposure gate on**:
```bash
ATLAS_CLIMDAT_SOURCE=obs Rscript scripts/r3_publish_tiers.R --dry-run --source chirps-chirts-agera5 --timeframe ggcmi-crop \
   --model chirps-chirts-agera5 --variables prod_t,ha,head_n,vop_usd,vop_intld --tiers severe,moderate,extreme \
   --drift-exposure prod_t=<0.4.4 reference res-25 table>,... > logs/obs_s3_pub_dry_full_$STAMP.log 2>&1
```
Report G1-G4 and G6b per tier. **Do not publish.** Publishing is a separate GO.

**Stop and describe** on: any FAIL in J0; S2 rc ≠ 0 or any missing-day line; a bundle count ≠ 162 on the smoke; R/3 §4.1
`input files = 0`; the G6b identity failing on the smoke for any of the three test crops; a §3b share that is
≥ 0.9 at 1 (report it even if everything else passes).

#### Then
Prepend `### RESPONSE` (J0-J5, and J6 when done), commit only this file, push, verify on `origin/develop`.
