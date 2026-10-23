# DISPATCH — cglabs — upstream changes (a)-(c): gap census + old-vs-new run-equivalence

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

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
