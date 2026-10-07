# DISPATCH — cglabs — Stage-0 water-balance v2 / HSH comparison harness

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### RESPONSE 2026-10-07 — cglabs — G0: `sroot_world.tif` MISSING and `sfcWind` processed for ONE GCM/year only → G1 STOPPED, not run. G2 run (new HSH + WBGT, ACCESS-ESM1-5 / historical / 1995, scratch root): numbers below. G3 sized for HSH/WBGT; water balance cannot be sized until G1 can run.

Node at `58221c9` (B5 RESPONSE commit; code state `c1dd906`). Nothing in either live indices tree was
written: every G2 output went to `nex-gddp-cimp6_hazards/sandbox/stage0_harness_20261007_184538/`, a
scratch `COMMON_DATA` root whose `nex-gddp-cmip6/` and `chirps_wrld/` are symlinks to the real inputs and
whose `nex-gddp-cmip6_indices/` is an empty real directory. Nothing published. R/2, R/3 not run.

**G0 — can v2 run here at all? Not today.**

| input | state on this node |
|---|---|
| `atlas_hazards/soils/sscp_world.tif` | present (2025-03-27, 11.7 MB) |
| `atlas_hazards/soils/ssat_world.tif` | present (2025-03-27, 12.4 MB) |
| `atlas_hazards/soils/sroot_world.tif` | **MISSING** — nothing resembling it under `atlas_hazards/soils/` (that dir holds only `africa_scp`, `africa_ssat`, `maize_soil_depth_{1,5}km`, `sscp_world`, `ssat_world`) |
| `wbkernel` R package | installed (`requireNamespace` TRUE) |
| `sfcWind` daily tifs, as `fast_calc_waterbalance.R` resolves them (`nex-gddp-cmip6/sfcWind/<ssp>/<gcm>/sfcWind_<date>.tif`) | **one window only: `historical/ACCESS-ESM1-5`, 365 files, all 1995.** No other GCM, no SSP. `sfcWind2/historical/ACCESS-ESM1-5` exists and is empty. |
| raw `sfcWind` netCDF (`nex-gddp-cmip6_raw/sfcWind2/historical/`) | 18 GCMs x 34 files (1981-2014, `*_v2.0.nc`), **historical only** — not processed to daily tifs, and nothing for any SSP. |
| `rsds` (v2 also needs it) | processed for 18 GCMs, historical + 4 SSPs; per-GCM file counts in `ssp245` are unequal (87,658 for 10 GCMs, 57,319-58,439 for 8), which is worth a completeness check before any full v2 run. |

So the dispatch's "smallest window" (ACCESS-ESM1-5, historical, 1995, one month) has wind, radiation
and two of the three soil rasters; it lacks the rooting-depth raster `sroot_world.tif`, which v2 reads
unconditionally at load (`fast_calc_waterbalance.R:39`). A full Stage-0 v2 refresh would additionally
need `sfcWind` processed for 18 GCMs x (historical + 4 SSPs); today that exists for 1 GCM-year.

**G1 — STOPPED, not run** (dispatch: do not source, derive or substitute). `compare_waterbalance_v2.sh`
was read, not executed. Two notes for when it can run: (a) it writes INTO the canonical
`nex-gddp-cmip6_indices/historical_<GCM>/{NDWS,NDWL0,NDWL50,AVAIL}` for the target month, backing up and
restoring the live tifs around the run — so on this node it should be run with `COMMON_DATA` pointed at a
scratch root like the one used for G2, not at the live tree; (b) its default seed month (1995-01) is
exactly the one month-window for which `sfcWind` is present, so no further wind processing is needed for
the G1 measurement itself once `sroot_world.tif` exists.

**G2 — HSH, new (`calc_HSH.R`, NWS Heat Index on Tmax + RHx) vs live (`nex-gddp-cmip6_indices/historical_ACCESS-ESM1-5/HSH`, daily-mean-T formula, files of 2025-08-01). ACCESS-ESM1-5, historical, all 12 months of 1995, 162,464 land pixels per month on the same grid (CHIRPS extent, 50S-50N global — the Africa crop happens in R/1).**

Run: `COMMON_DATA=<scratch> SCENARIO=historical GCMS=ACCESS-ESM1-5 YRS=1995:1995 FORCE_OVERWRITE=1 Rscript calc_HSH.R`
from `hazards_upstream/R/04_indices/`. Log `logs/stage0_g2_20261007_184538.log`; comparison
`logs/stage0_g2_compare_hsh_20261007_184538.log`. The live file for the same month is byte-identical in
`nex-gddp-cmip6_indices/` and in the prototype's `atlas_nex-gddp_hazards/cmip6/indices/..._1995_2014/`
tree (md5 checked for 1995-01), so this is the comparison against what R/1 consumes.

Shift = new − live, °C, per month (d_med = median over pixels; frac_gt1 = share of pixels moving more than +1 °C; frac_up = share moving up at all):

| stat | month | live med | new med | d_med | d_p05 | d_p95 | frac_gt1 | frac_lt_m1 | frac_up |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| HSH_mean | 01 | 17.18 | 22.75 | +5.47 | +0.89 | +8.74 | 0.944 | 0.000 | 0.991 |
| HSH_mean | 04 | 21.20 | 27.14 | +6.02 | +2.71 | +9.21 | 0.987 | 0.000 | 0.998 |
| HSH_mean | 07 | 24.32 | 30.49 | +6.43 | +2.81 | +8.95 | 0.988 | 0.000 | 0.998 |
| HSH_mean | 10 | 22.84 | 28.89 | +6.24 | +2.73 | +9.47 | 0.988 | 0.000 | 0.999 |
| HSH_max  | 01 | 21.32 | 27.04 | +6.46 | +0.74 | +10.49 | 0.935 | 0.000 | 0.987 |
| HSH_max  | 04 | 24.90 | 31.78 | +7.15 | +3.27 | +10.52 | 0.989 | 0.000 | 0.999 |
| HSH_max  | 07 | 27.94 | 34.98 | +7.23 | +3.16 | +10.19 | 0.988 | 0.000 | 0.998 |
| HSH_max  | 10 | 26.88 | 33.62 | +7.30 | +3.12 | +11.06 | 0.992 | 0.000 | 0.999 |

(All 12 months are in the comparison log; the other eight sit inside the ranges shown.)

- **Median shift, pooled over the 12 months:** HSH_mean **+6.1 °C** (monthly medians +5.5 to +6.6);
  HSH_max **+7.2 °C** (monthly medians +6.3 to +7.4).
- **Max shift:** +15.1 °C (HSH_mean), +23.3 °C (HSH_max, November). Min shift −2.0 / −2.6 °C.
- **Fraction moving more than 1 °C:** 0.98 of pixels in every month for both stats (lowest 0.93-0.94 in
  Dec/Jan HSH_mean, where cold northern-hemisphere pixels sit on the Steadman branch). Fraction moving
  *down* by more than 1 °C: 0.000-0.002.
- **Sign:** one-signed upward. 99.6-99.9 % of pixels move up; by latitude band (HSH_max, Jan and Jul):
  arid 15-30N d_med +6.2 / +7.0, humid −10..10 d_med +8.3 / +8.3, southern −35..−15 d_med +8.8 / +6.4,
  frac_up ≥ 0.997 in every band. No sign change between the arid band and the humid tropics; the humid
  tropics move most, as the RH adjustments predict. This is the direction the dispatch expected.
- **Tails.** The new HSH_max carries more extreme values: pixels above 45 °C in March 9,339 vs 1,125 live,
  above 60 °C 423 vs 43. The single highest new value is 97.0 °C (March, lon 103.1 lat 15.1 — outside
  Africa) where live gives 89.4 °C at the same cell: the inputs that day are tasmax ≈ 47 °C with hurs ≈ 94 %,
  a pairing both formulae extrapolate to a very large Heat Index. An input-tail artefact, present in both
  versions, not a defect in the fix. For Africa the highest new HSH_max in the window is 76.1 °C (October,
  lon 1.9 lat 13.9, Sahel; live 68.9).
- **Consequence to note for #13 (not judged here):** `metadata/haz_classes.csv` classes `HSH_max` as
  Moderate above 27 °C. A one-signed +7 °C median shift moves a large share of pixel-months across that and
  the higher thresholds, so the HSH frequency and the HSH-containing interaction tiers will change
  materially under the new producer, in the historical as well as the future period.

**G2 — WBGT (`calc_WBGT.R`, CHC shaded WBGTmax from the NWS Heat Index), same window. Runs, writes 61 files (mean, max, days28/30/32 x 12 months + 12 daily stacks), no error.**

| month | WBGT_mean Africa min / med / p95 / max (°C) | WBGT_max Africa min / med / p95 / max (°C) | days>30 Africa p95 / max |
|---|---|---|---|
| 01 | −23.3 / 21.5 / 26.8 / 30.6 | −15.6 / 24.5 / 29.7 / 33.8 | 0 / 25 |
| 04 | −2.0 / 24.3 / 28.9 / 31.1 | 4.9 / 26.6 / 31.6 / 33.8 | 7 / 26 |
| 07 | 0.9 / 26.1 / 29.3 / 32.5 | 4.8 / 27.9 / 30.9 / 33.8 | 6 / 31 |
| 10 | 3.6 / 25.2 / 30.1 / 32.4 | 9.3 / 27.7 / 33.1 / 33.8 | 17 / 31 |

- Plausible range for a shaded WBGT: Africa monthly medians 21-26 °C (mean) and 24-28 °C (max), p95 up to
  33 °C in October; negative values only in the Atlas / high-latitude winter cells (global min −75 °C over
  Siberia, same cells where HSH is strongly negative). Daily stacks have days-in-month layers; the day-count
  layers are within 0-31 everywhere.
- WBGT_max sits **below** HSH_max at every Africa pixel (July: median 9.7 °C below, p05 5.2, p95 13.6;
  fraction WBGT < HSH = 1.000), as it should.
- **One property to know, not a defect in the run:** WBGT_max tops out at exactly **33.8 °C** in every month,
  globally. That is the vertex of the CHC regression `WBGT = −0.0034·HI_F² + 0.96·HI_F − 34`
  (`heat_index.R:5`): it peaks at HI = 141 °F (60.7 °C) and *declines* for hotter Heat Index values, so the
  few hundred pixel-months with HI above ~61 °C (the 97 °C cell above among them) get a WBGT lower than a
  61 °C cell would. Whether to clamp or to cap HI before the regression is a method question for the
  macbook; today the function is faithful to the published form.

**G3 — what a refresh would cost (this node: 40 logical cores, 376 GiB, no swap).**

Measured, one GCM / one year / 12 months, single process each, in sequence:

| producer | wall-clock, 12 months | per month | RSS sampled mid-run | output per GCM-year (incl. daily) |
|---|---:|---:|---:|---:|
| `calc_HSH.R` (new) | 592 s | 49.3 s | 5.6 GB | ≈0.26 GB (matches live: 8.9 GB / 34 yr) |
| `calc_WBGT.R` (new) | 596 s | 49.7 s | 1.7 GB | ≈0.30 GB |

Scope R/1 → R/2 consume (`R/1_make_timeseries.R` drops only `_1981_2014`): 18 GCMs x (historical
1995-2014 = 20 yr + 4 SSPs x 2021-2100 = 320 yr) = **6,120 GCM-years = 73,440 GCM-months**. The Stage-0
tree as it stands carries historical 1981-2014 (34 yr) for HSH, so a like-for-like Stage-0 refresh is
6,372 GCM-years (+4 %).

- **(b) HSH + WBGT.** Each ≈ 1,000 core-hours serial (73,440 x 49.5 s). The scripts are single-threaded
  and parallelise at process level (one `Rscript` per GCM via `GCMS=`). At 18 processes per producer, both
  producers running side by side = 36 processes, peak RSS ≈ 18 x 5.6 + 18 x 1.7 ≈ 130 GiB, well inside
  376 GiB: **≈ 56 h ≈ 2.3 days wall-clock for both**, I/O permitting (inputs and outputs are on the NFS
  mount; the per-month time above already includes reading 3 x 31 daily tifs and writing a 31-layer daily
  stack, but 36 concurrent readers were not measured). HSH alone at 18-way is the same 2.3 days; running the
  two producers in sequence rather than side by side doubles it. Output: ≈ 1.6 TB HSH (replacing the live
  1.7 TB) + ≈ 1.8 TB WBGT (new). Disk now: 123T free of 192T (37% used).
- **(a) Water-balance trio (v2).** **Not sized** — G1 could not run (G0). Nothing on this node measures
  the v2 kernel's per-month cost, and the dispatch rules out a substitute input, so no number is offered.
  What can be said: a full v2 run needs `sfcWind` processed for all 18 GCMs x 5 scenarios (today 1
  GCM-year exists; raw netCDF exists for 18 GCMs x historical only, nothing for the SSPs, so the SSP wind
  would have to be downloaded as well as processed), plus `sroot_world.tif`. When `sroot_world.tif` is
  on the node, G1 as written (ACCESS-ESM1-5, 1995-01) is runnable without any further wind work, and that
  one month gives the per-month v2 cost directly.

**Decision inputs, as the dispatch framed them.** B6 (water balance): not runnable today; blocked on a
rooting-depth raster and, for anything beyond one GCM-year, on `sfcWind`. B7 (HSH): the fix is runnable
now, costs ≈ 2.3 days of node time for HSH + WBGT, and moves HSH by a one-signed +6 to +7 °C median, with
98 % of pixels moving more than 1 °C — large enough to change every HSH tier #13 publishes. No pass/fail
is given here, per the dispatch.

**Scratch left in place** for inspection: `nex-gddp-cimp6_hazards/sandbox/stage0_harness_20261007_184538/`
(557 MB; the two symlinks inside point at the live inputs — `rm -rf` the directory, not its targets).
Logs: `logs/stage0_g2_20261007_184538.log`, `logs/stage0_g2_compare_hsh_20261007_184538.log`, driver
`logs/stage0_g2_20261007_184538.sh`. Nothing published; live trees untouched (checked: no file under
either indices tree has an mtime after 18:45 today).

---


Thread opened 2026-10-07 (macbook). Decision behind it: Pete chose **"comparison harness first"** on
2026-10-07 — quantify the value move before committing to a Stage-0 refresh for issue #13
(`HANDOVER_2026-10-07.md` §2 B6/B7).

**This dispatch writes no product and publishes nothing.** It answers three questions and stops.
Nothing downstream depends on it except the #13 bake, which must not start until it is answered.

---

## Why this exists

Water-balance v2 and the #14 HSH fix are **already written** upstream and have never been run:

| piece | commits | what changed |
|---|---|---|
| v2 water balance | `e42110a` + harness `e52e84f` | NDWS/NDWL0/NDWL50 in one FAO-56 / AquaCrop pass, PET = FAO-56 Penman-Monteith (`et0_fao56.R`), single C++ pass via `wbkernel::wb_kernel_cpp`. Replaces the legacy eabyep heuristic and the three separate `fast_calc_NDWS/NDWL0/NDWL50.R`. |
| hazards#19 AVAIL | `a4ba707`, `bafe8c8` | deterministic AVAIL default-on; the classify-sum inflation root cause |
| #14 HSH | `d8e2bc0`, `f81e6d7` | `calc_HSH.R` now calls `heat_index_nws()` on Tmax with `rhx_from_daily()` — the NWS Steadman→Rothfusz path with the low-RH and high-RH adjustments, replacing daily-mean temperature. `calc_WBGT.R` came with it. |

So neither is a build decision. Both are **run** decisions, and the only thing missing is the
number: how much do the indices actually move, and is v2 even runnable on this node.

**v2 is a separate script, not a toggle.** Switching means running `fast_calc_waterbalance.R` in
place of the three legacy scripts. Nothing selects between them by env var.

---

## Gates — stop at each one

### G0. Can v2 run here at all?

v2 needs inputs the legacy trio does not. Check for them **before anything else**, because a
missing input here is the whole answer to "in or out":

```
Rscript -e '
root <- Sys.getenv("hazards_root", "")   # or the value 00_setup.R resolves
for (f in c("atlas_hazards/soils/sscp_world.tif",
            "atlas_hazards/soils/ssat_world.tif",
            "atlas_hazards/soils/sroot_world.tif")) {
  p <- file.path(root, f); cat(sprintf("%-46s %s\n", f, if (file.exists(p)) "present" else "MISSING"))
}
cat("wbkernel installed:", requireNamespace("wbkernel", quietly = TRUE), "\n")'
```

Then confirm a `sfcWind` daily input exists for one GCM / one scenario / one year, the same way
`fast_calc_waterbalance.R` resolves it (`ws_pth`, `ex(ws_pth, "sfcWind")`).

**Expected:** `sscp_world.tif` and `ssat_world.tif` are long-standing inputs and should be present.
`sroot_world.tif` (rooting depth, cm) is **new with v2** and may not be. `sfcWind` is a NEX-GDDP
variable the legacy trio never needed.

**STOP and report if `sroot_world.tif` or `sfcWind` is missing.** Do not try to source, derive or
substitute either one — whether they can be obtained is Pete's call, and it changes the answer to
B6. Report exactly which are missing and move to G2 (HSH), which does not depend on them.

### G1. The comparison, on a small window

`hazards_upstream/R/04_indices/compare_waterbalance_v2.sh` exists for this. Read it first, then run
it on **one GCM, one scenario, one year, one month** — the smallest window that produces numbers.
Log to `logs/wb_v2_compare_<STAMP>.log`.

> Background it. The interactive shell kills foreground commands after about two minutes
> (AGENTS.md §2), and this is longer than a gate check.

**Report, as invariants and not as target figures** (a gate that expects a slightly wrong number
turns a correct run into a false failure — three times in one week):

1. For each of NDWS, NDWL0, NDWL50: the distribution of legacy vs v2 over the window — median,
   IQR, and the fraction of pixels where the two differ by more than one day.
2. The **direction** of the move, and whether it is consistent in sign across the arid band and the
   humid tropics, or changes sign between them.
3. Whether v2 stays inside the physical bound (0 to days-in-month) everywhere.
4. Wall-clock for v2 over the window versus the legacy trio over the same window, so a full Stage-0
   re-run can be sized. Say how many GCM × scenario × year combinations a full refresh would be.

**Do not judge pass/fail.** There is no expected answer: the point is the magnitude. A large move is
not a defect — v2 is a different and better-founded method — but it decides whether #13 waits.

### G2. HSH, independent of G1

`calc_HSH.R` needs only inputs the pipeline already has. Run it for **one GCM / one scenario / one
year** into a scratch output directory — **not** over the live indices tree — and compare against
the live HSH for the same window.

Report: median and max shift in HSH (°C), the fraction of pixels moving more than 1 °C, and whether
the shift is one-signed. The fix adds the Rothfusz corrections and moves the input from daily-mean
to Tmax-with-RH-at-Tmax, so expect HSH to come out **higher**; a shift that is predominantly
downward is a finding worth stopping on.

Also confirm `calc_WBGT.R` runs and writes a plausible WBGT (shaded/indoor, °C) over the same
window. It is new, so there is nothing to compare against — a sanity range is enough.

### G3. What a refresh would cost

With G1's and G2's timings, state the wall-clock for a full Stage-0 refresh of (a) the
water-balance trio and (b) HSH/WBGT, across all GCMs, scenarios and years that `hazards_prototype`
R/1→R/2 consume. Note peak memory: **no swap, 376 GiB / 40 logical cores, OOM is an instant kill.**

---

## What NOT to do

- **Do not write into the live indices tree.** Every output here goes to a scratch directory.
- **Do not run R/2 or R/3.** The #13 bake is a separate dispatch and must not start until this one
  is answered.
- **Do not publish anything.** Nothing in this thread reaches S3.
- **Do not switch Stage 0 over to v2.** That is the decision this dispatch informs, not its job.
- **Do not improvise around a missing input.** Report it and stop at that gate.

## Reporting

Prepend a `### RESPONSE <date>` block with the numbers for G0-G3, then verify it landed on
`origin/develop`: `git fetch`; `git log origin/develop..HEAD` empty;
`git show origin/develop:DISPATCH_cglabs_stage0_wb_harness.md | grep -c RESPONSE`.

Commit trailer: `Co-Authored-By: <your own model name> <noreply@anthropic.com>`.
