# DISPATCH — cglabs — upstream changes (a)-(c): gap census + old-vs-new run-equivalence

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### RESPONSE — cglabs 2026-10-24 — all invariants hold: census clean, old vs new bit-identical, `calendar` runs on all three calendars

Live `nex-gddp-cmip6_indices/` and `atlas_nex-gddp_hazards/` untouched: `find -newer <run start>` on the
six live dirs for the three GCMs returns 0 files. `env | grep -E 'FORCE_OVERWRITE|DAYS_CHECK|AVAIL_SEED'` was
empty, and every run was also launched with `env -u` for all three. Logs: `logs/obs_s2_*_20261023_204203.log`.

#### Step 0 — node fixture
`ALL UPSTREAM RUN-PARAMETER FIXTURE ASSERTIONS PASSED` (`logs/obs_s2_step0_fixture_20261023_204203.log`).

#### Step 1 — gap census → `metadata/checks/nexgddp_daily_gap_census.csv`
18 GCMs x 5 vars x 5 scenarios = 450 directories, all present; 32 076 year rows. Missing dates are
judged per year against the full Gregorian year with `.calendar_gap()` sourced from `00_setup.R`.
Years: historical `pr`/`rsds`/`hurs` 1981-2014, `tasmax`/`tasmin` 1975-2014; SSPs 2021-2100; no
missing years inside any range. No file has a date outside the Gregorian calendar (no `-02-30`).

**Invariant holds: `n_noncal_gaps == 0` in every row.** Calendar = the raw netCDF `time:calendar`
(read with ncdf4; GDAL here has no HDF5 plugin), and it agrees with the on-disk layout in every case:

| GCM | netCDF calendar | has `-31` | has `-02-29` | files/yr | non-cal gaps |
|---|---|---|---|---|---|
| ACCESS-CM2, ACCESS-ESM1-5, EC-Earth3, EC-Earth3-Veg-LR, MPI-ESM1-2-HR, MPI-ESM1-2-LR, MRI-ESM2-0 | proleptic_gregorian | yes | yes | 365/366 | 0 |
| IPSL-CM6A-LR, MIROC6 | standard | yes | yes | 365/366 | 0 |
| CanESM5, CMCC-ESM2, GFDL-ESM4, INM-CM4-8, INM-CM5-0, NorESM2-LM, NorESM2-MM, TaiESM1 | 365_day | yes | **no** | 365 | 0 |
| KACE-1-0-G | 360_day | **no** | yes | 358/359 | 0 |

**KACE-1-0-G lacks only the 31sts.** 365 − 7 = 358 per year, and 359 in leap years because 29-Feb
is present. It has no other missing days in any var or scenario.

#### Step 2 — old (`5f60694^`) vs new (`5f60694`), defaults
Worktree at `5f60694^`, plus the current checkout (`hazards_upstream/` identical to `5f60694`).
Scratch roots were under the bulk data root, with `nex-gddp-cmip6/`, `chirps_wrld/` and `atlas_hazards/`
symlinked in and an empty `nex-gddp-cmip6_indices/` in each. GCMs: ACCESS-CM2 (Gregorian),
**CanESM5** (no-leap) and KACE-1-0-G (360-day). The runs were exactly as specified: NDWS and NDWL0
historical 1995:1996 plus ssp245 2021 months 1-3, and NTx, PTOT and THI 1995 months 1,2,12 plus
1996 month 2. One GCM per process. All 60 runs exited with rc 0.

**File sets identical: 744 old, 744 new. Max |new − old| = 0 and NA mismatches = 0 in every file;
geometry and layer count are identical too.**

| GCM | NDWS | NDWL0 | NTx20…NTx50 (31 dirs) | PTOT | THI | max_abs_diff | n_NA_mismatch |
|---|---|---|---|---|---|---|---|
| ACCESS-CM2 | 54 | 54 | 4 each | 4 | 12 | 0 | 0 |
| CanESM5 | 54 | 54 | 4 each | 4 | 12 | 0 | 0 |
| KACE-1-0-G | 54 | 54 | 4 each | 4 | 12 | 0 | 0 |

(NDWS and NDWL0 each have 27 months x {index, AVAIL}. THI has 3 outputs per month.)

`WARN.*daily inputs missing` count in the new logs: **0** for all three GCMs, which matches the
census (0 non-calendar gaps). The new code instead logs its INFO `calendar-gap day(s) absent - expected`
line: 0 times for ACCESS-CM2, 12 for CanESM5 (Feb-1996 only) and 148 for KACE (every month with a 31st).

#### Step 2.6 — new code, `DAYS_CHECK=calendar`, one month per GCM
I picked the month that exercises each calendar's gap: ACCESS-CM2 1995-01, CanESM5 **1996-02**
(no 29-Feb) and KACE 1995-01 (no 31st). I ran PTOT, THI, NTx and NDWS for that month into a third
scratch root. **All ran (rc 0).** CanESM5 and KACE logged the expected calendar-gap INFO lines and no
stop; ACCESS-CM2 had no gaps. The invariant holds, so `calendar` looks safe as the NEX default.

Deviation, my own error: for NDWS I set `AVAIL_SEED_MONTHS` to the test month, so that 1996-02 can
seed. The first attempt passed `1995-1` (not zero-padded) and NDWS correctly stopped with
`AVAIL_SEED_MONTHS must be YYYY-MM[,YYYY-MM...]`, after the days gate had already passed for all
four vars. The rerun with `1995-01` / `1996-02` passed. These runs were not part of the old-vs-new
comparison.

#### Cleanup
Worktree removed (`git worktree list` shows only the main checkout). Scratch roots deleted (3.5 GB),
symlinks unlinked first; the source trees are intact. Logs kept.


### MACBOOK 2026-10-23 — LIVE OUTPUTS UNTOUCHED. Census the daily NEX-GDDP inputs, then prove `5f60694` reproduces `5f60694^` exactly under default settings.

**What changed (`5f60694`):**
- `AVAIL_SEED_MONTHS` (default `1995-01,2021-01`) in the water-balance scripts;
- `calc_NTx.R` now honours `FORCE_OVERWRITE`;
- a `DAYS_CHECK` completeness gate in every daily reader, with three modes:
  - `warn`, the default and legacy-compatible: it continues and logs a WARN line;
  - `calendar`: stops on gaps the series' own calendar doesn't explain;
  - `strict`: stops on any gap.

The macbook fixture `R/checks/fixture_upstream_run_params.R` passes (51 assertions on synthetic data).
**What it cannot show** is that the defaults reproduce the real NEX-GDDP outputs, or how the 360-day
and no-leap GCMs are laid out on disk. That is this dispatch.

**Hard rules for this dispatch:**
- Write **nothing** under the live `nex-gddp-cmip6_indices/` or `atlas_nex-gddp_hazards/` trees.
- `env | grep -E 'FORCE_OVERWRITE|DAYS_CHECK|AVAIL_SEED'` must be empty in every shell you launch from.
  A stray `FORCE_OVERWRITE=1` now also forces NTx.
- Everything runs in the background: `nohup … &`, logs in `logs/obs_s2_<step>_<STAMP>.log`.
- Public repo: no IPs, hostnames or mount identifiers in the RESPONSE.

#### Step 0 — node fixture
`git pull`, then `Rscript R/checks/fixture_upstream_run_params.R`. It must end
`ALL UPSTREAM RUN-PARAMETER FIXTURE ASSERTIONS PASSED`. **Stop** if not.

#### Step 1 — gap census (filesystem only, no compute)

For each of the 18 sidecar GCMs × `{pr, tasmax, tasmin, rsds, hurs}` × `{historical, ssp126, ssp245,
ssp370, ssp585}`, read the daily files under `{common_data}/nex-gddp-cmip6/<var>/<ssp>/<gcm>/` and
report:
- files per year, plus the first and last date;
- **calendar evidence:** does any `-31.tif` exist, does any `-02-29.tif` exist? Confirm against the raw
  netCDF `time:calendar` attribute where the raw files are on the node.
- **non-calendar gaps:** missing dates that are not explained by that directory's calendar. Use the
  same rule as `.calendar_gap()` in `hazards_upstream/R/00_setup.R`; source it rather than
  re-implementing it.

Write the result to `metadata/checks/nexgddp_daily_gap_census.csv` with columns
`gcm,var,scenario,year,n_files,has31,has0229,n_noncal_gaps,first_noncal_gap`. Add a one-line-per-GCM
summary in the RESPONSE.

**Invariant:** `n_noncal_gaps == 0` everywhere, **or** a short named list of real holes (do not fix
them, report them). **Specifically answer:** does KACE-1-0-G lack only 31sts, or other days too?

#### Step 2 — run-equivalence, old code vs new code

1. **Two code trees.**
   - `git worktree add <scratch>/code_old 5f60694^` (old);
   - the current checkout (new).
2. **Two scratch data roots**, `<scratch>/root_old` and `<scratch>/root_new`, under your bulk data root
   (not `/`). Each **symlinks** `nex-gddp-cmip6/`, `chirps_wrld/` and `atlas_hazards/` from
   `{common_data}`, and holds its **own empty** `nex-gddp-cmip6_indices/`.
3. **GCMs** (calendar from Step 1): `ACCESS-CM2` (Gregorian), one **no-leap** GCM and `KACE-1-0-G`
   (360-day).
4. **Runs.** Run with `COMMON_DATA=<root_x> GCMS=<gcm>` and **no other env vars**:
   - `fast_calc_NDWS.R`, `fast_calc_NDWL0.R`:
     - historical, `YRS=1995:1996` (seed 1995-01, carry, leap Feb-1996);
     - `SCENARIO=future SSPS=ssp245 YRS=2021 MONTHS=1,2,3` (2021-01 seed).
   - `calc_NTx.R`, `calc_PTOT.R`, `calc_THI.R`: historical `YRS=1995 MONTHS=1,2,12` and `YRS=1996 MONTHS=2`.
   - One GCM per process; old and new in parallel is fine.
5. **Compare** every output tif in `root_old` vs `root_new`.
   - The file sets must be identical, and **max |new − old| must be 0** per file. Use `terra` values,
     NA positions included.
   - Report per GCM × index: n_files, max_abs_diff, n_NA_mismatch.
   - Also grep the new logs for `WARN.*daily inputs missing` and report the count. It should match the
     census gaps for those GCM-months.
6. **New code with `DAYS_CHECK=calendar`**, one month per GCM: does it run? This tells us whether
   `calendar` can become the NEX default.
7. Remove the worktree and the scratch roots afterwards. Keep the logs.

**Expected (invariants):**
- Step 2.5 diffs are all exactly 0, with identical file sets.
- Under `calendar`, ACCESS-CM2 runs; the no-leap and 360-day GCMs run if their only gaps are 29-Feb or
  31sts.

**Stop and describe, do not fix,** if:
- any diff is non-zero;
- a file set differs;
- the census shows KACE gaps on days other than the 31st.

#### Then
1. Prepend `### RESPONSE` with Steps 0-2.
2. Commit **only** this file and `metadata/checks/nexgddp_daily_gap_census.csv`, then push.
3. Verify on `origin/develop`.
