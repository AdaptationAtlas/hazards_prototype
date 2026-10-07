# DISPATCH — cglabs — Stage-0 water-balance v2 / HSH comparison harness

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

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
