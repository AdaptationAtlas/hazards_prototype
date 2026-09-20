# DISPATCH — cglabs — Track-1 NDWS / hazard_exposure resume

> 🛑 **DO NOT FIRE ANY STEP IN THIS FILE YET.**
> The cglabs node is running the issue #26 R/2.1 FORCE rebake until ~2026-09-19
> (`logs/R21_pushdown_20260918_045834.log`). Nothing here runs until **both**:
> 1. `archive/dispatches/DISPATCH_cglabs_issue26_r21_rebake.md` reports the rebake **done**, and
> 2. **p.steward gives an explicit GO** on this file.
>
> Steps 1 and 2 are minutes of light I/O. Step 3 is a publish and needs its own GO.
> The copy-paste prompt is at the bottom, under the same condition.

---

## MACBOOK — 2026-09-18 — the premise changed: Track-1's NDWS objective is **already live**

Before writing a resume plan I checked what is actually published, because the
resume state I was handed (`R/2` partial at `dcb5c1d`, pending `R/3` → publish →
CR-068) is from **2026-06-30** and has been superseded twice since.

**Measured against the live product, not inferred.** The notebook-facing object is

```
s3://digital-atlas/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/
  processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=jagermeyr/
  model=ENSEMBLEmean/severity={severe,moderate,extreme}/int=multi-hazard.parquet
```

three objects, 2026-09-16 16:58–16:59, 62.8 / 64.2 / 64.1 MB — the issue #9 publish.
Querying it directly over httpfs (adm0, `hazard_vars='NDWS+NTx35+NDWL0'`, VoP-weighted
frequency as `dry-union / (any + none)`):

| scenario | timeframe | freq(any) | freq(NDWS dry-union) |
|---|---|---|---|
| historic | 1995-2014 | 0.4194 | **0.1616** |
| ssp126 | 2081-2100 | 0.4969 | 0.1650 |
| ssp245 | 2081-2100 | 0.5804 | 0.1665 |
| ssp370 | 2081-2100 | 0.6707 | 0.1731 |
| ssp585 | 2081-2100 | 0.7576 | 0.1893 |

A saturated NDWS reads ≈ 1.0. Historic is **0.1616**, and the future values rise
monotonically with forcing (0.149 → 0.189) instead of pinning. `hazard='none'` is
present on every `hazard_vars` group, both NDWS compounds are there
(`NDWS+NTx35+NDWL0` 26,556,210 rows crop-side, `NDWS+THI-max+NDWL0` 7,810,650
livestock-side — NDWS still reaches R/3 only via `_int` compounds), and all five
scenarios are in the one file (historic 3,997,215 rows; each ssp 15,988,860).

**So the hazards#19 NDWS de-saturation shipped inside the issue #9 publish on
2026-09-16.** The chain did not stop at R/2 — R/2 completed 2026-06-30, R/3 and the
recovery ran through July, and the publish went out in September. The handover
records the CR-068 Angola probes green at the same time (every crop ≤ 100 %, zero NaN
at adm0 and adm1, adm0 = sum of adm1 to the dollar) and #12 verified resolved across
5,281 admin1 × crop pairs.

**There is therefore no R/2 remainder to run and no R/3 to re-run for Track-1.**
Re-baking would burn a day to reproduce a result already in the product. What is
below is what is genuinely left, which is smaller and different.

### One thing I could not check from here

I have no read access to the node's filesystem, so "what is on disk vs what the
dispatch log claims" is **not** something I can settle from the macbook. Step 1 is
that audit, as a node-side read-only job with a kill-gate, and its output is the
thing that either confirms the above or contradicts it. Treat my conclusion as
evidence from the published artifact only.

---

## What is actually outstanding

| # | Gap | Owner |
|---|---|---|
| A | **No `.json` sidecar is published** for the three live objects, so ensemble membership is unrecoverable from the product. This is #26 ask 4 applied here. | fixed in `0057d65`, ships in step 2 |
| B | `period=annual` is **not published** under `source=nex-gddp-cmip6` — only `jagermeyr`. The old `atlas_cmip6` tree carried both. | consumer decision (Brayden / p.steward) |
| C | `variable=vop_intld15` is **absent** under `nex-gddp-cmip6`. The live intld product is still `atlas_cmip6`, 2025-06/07. It therefore carries the **pre-fix VoP basis** — measured 2026-09-18, published livestock VoP is median **1.198** vs FAOStat const-I$ where the node's fixed output is 1.00. Publishing it is the VoP currency fix going live. | **p.steward** — wholesale overwrite, see issue #30 comment |
| D | `model=historic`, `ENSEMBLE`, `ENSEMBLEsd` and per-GCM are not published under the new source. Historic is a `scenario` row inside `ENSEMBLEmean`, which the notebook evidently reads. `R/derive_historic_model_parquet.R` may now be unnecessary. | flag, do not decide |
| E | `value_sd` is absent from the live schema, because `ENSEMBLEmean` is published rather than the merged `ENSEMBLE`. | flag |

Steps 1–2 close **A**. **C** is the one with real value behind it and it is Pete's
call, not this dispatch's.

---

## STEP 1 — node audit + ensemble-membership kill-gate (read-only, ~2 min)

Confirms the node's state against the published artifact, and gates the membership
parser on real filenames — which is the part I could **not** verify from the macbook.

```bash
cd "$project_dir" && git pull   # expect 0057d65 or later
export TS=$(date +%Y%m%d_%H%M%S)

# 1a) what the node holds, and when it was written
Rscript scripts/stamp_ensemble_membership.R --dry-run \
  |& tee logs/track1_stamp_dryrun_$TS.log
```

**KILL-GATE 1a.** The dry run prints, per `hazard_risk_vop*/<timeframe>` folder, the
member count and the GCM names it parsed. Required:

- `n_members` is **18** for every folder it reports, and
- the names look like GCMs (`ACCESS-ESM1-5`, `EC-Earth3`, `MPI-ESM1-2-HR`, `TaiESM1`, …),
  not crops, severities or `NA`.

If it prints `NONE FOUND`, or a count that is not 18, or anything that is not a GCM
name: **stop and paste the log.** The parser takes the model token with the same
anchored `tstrsplit` field-2 rule R/3 §4.2 uses for `model`, and I validated it only
against filename shapes reconstructed from your logs — real per-GCM `_int` basenames
were not reachable from my machine. A wrong parse here would stamp a false membership
claim into a published product, which is worse than no claim.

```bash
# 1b) the three live objects vs the node's tier files
ls -l --time-style=long-iso \
  "$(Rscript -e 'source("R/0_server_setup.R");cat(file.path(atlas_dirs$data_dir$hazard_risk_vop_usd,"jagermeyr"))' 2>/dev/null)"/haz-freq-exp_vop_nominal-usd-2021_ENSEMBLEmean_int_adm_*.parquet*

aws s3 ls --recursive \
  "s3://digital-atlas/domain=hazard_exposure/source=nex-gddp-cmip6/" | sort
```

Report: node mtimes and sizes against the three live objects (62.8 / 64.2 / 64.1 MB,
2026-09-16 16:58–16:59). Equal sizes mean the node still holds exactly what was
published and step 2 is a sidecar-only operation. **Different sizes mean the node has
moved on since the publish** — say so and stop; that changes step 2 from "ship the
sidecar" to "decide whether to republish", which is Pete's call.

---

## STEP 2 — stamp and ship the sidecars (needs GO; ~1 min, no parquet touched)

Only after KILL-GATE 1a is green and 1b shows the node matching live.

```bash
Rscript scripts/stamp_ensemble_membership.R |& tee logs/track1_stamp_$TS.log
# expect: VERDICT PASS, 18 members per folder

Rscript scripts/r3_publish_tiers.R --sidecar-only --dry-run \
  |& tee logs/track1_sidecar_dry_$TS.log
Rscript scripts/r3_publish_tiers.R --sidecar-only \
  |& tee logs/track1_sidecar_$TS.log
```

`--sidecar-only` uploads `int=multi-hazard.parquet.json` beside each tier and **does
not touch the live parquet** — no re-upload of ~190 MB of byte-identical data and no
overwrite of a known-good object. It refuses to publish any sidecar whose `ensemble`
block is missing or whose count is not 18, so a partial-ensemble claim cannot ship.

**KILL-GATE 2.** Each tier must log `SIZE MATCH` and `HTTP 206`. Then confirm from
outside:

```bash
for sev in severe moderate extreme; do
  curl -s "https://digital-atlas.s3.amazonaws.com/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=jagermeyr/model=ENSEMBLEmean/severity=$sev/int=multi-hazard.parquet.json" \
    | python3 -c "import json,sys; d=json.load(sys.stdin); e=d.get('ensemble',{}); print('$sev', e.get('n_members'), len(e.get('members',[])))"
done
```

Expect `18 18` on all three lines. Anything else, paste it.

---

## STEP 3 — gap C, the intld publish (**needs a separate p.steward GO — do not run on this file's GO**)

This is the VoP currency fix going live. It overwrites a live product wholesale, so it
is deliberately not wired up here. What it would need, when Pete decides:

1. `Rscript R/checks/vop_align_live_gate.R` from any machine first — records the
   pre-publish basis of the live artifact (currently crop PASS 1.007 / livestock FAIL
   1.198), so the change is measurable afterwards rather than asserted.
2. `Rscript R/qaqc_vop_vs_faostat.R` on the node — the node-side twin, on the rasters
   R/3 actually consumed. Expect livestock ~1.00, crop ~0.99.
3. The `0.4.4:345` unit allow-list decision on **#30** must be made first, because the
   exposure reference and the hazard product would otherwise be published on
   different intld vintages — which is the whole of #30.
4. Only then a publish, and it must back up the live objects the way
   `r3_publish_tiers.R` already does (`sandbox/backup/issue9_<stamp>/`).

Do not begin any of this without Pete naming it.

---

## STEP 4 — CR-068 regression probes + local-vs-S3 verify (after any publish, incl. step 2)

These are now a **regression** check, not a first run: the probes came back green on
2026-09-16 and #12 was verified resolved. Run them again after anything ships so a
regression is caught at the point it is introduced.

```bash
atlas_notebooks/scripts/probe_no_hazard_arithmetic_quick.sh AGO |& tee logs/track1_probe1_$TS.log
atlas_notebooks/scripts/probe_cross_parquet_vop_drift.sh   AGO |& tee logs/track1_probe2_$TS.log
```

**Two known probe-script bugs — the probes are wrong, not the data.** Documented at
`R/NEXT_FULL_REBAKE.md:142-145`; they live in `atlas_notebooks`, which is out of
scope for this repo, so read around them rather than fixing them:

- `probe_no_hazard_arithmetic_quick.sh` hardcodes `hazard_vars='NDWS+NTx35+NDWL0'`, a
  **crop** combo, so all 10 livestock commodities report `no_hazard_row`. Expected, not
  a finding. The livestock combo is `NDWS+THI-max+NDWL0`.
- `probe_cross_parquet_vop_drift.sh` Query C sums `admin1_name IS NOT NULL`, which also
  picks up **adm2** rows, so NaN propagates and it reports a false
  `admin1-sum ≠ admin0-row`. Restricted to true adm1 it matches exactly.

Reference baselines (AGO, historic 1995-2014) are in the CR-068 memory: pre-bake rice
203.55 %, sugarcane 117.9 %, pearl-millet 107.9 %, tobacco 105.3 %, maize 100.8 %; NaN
561/6,021. Post-#9 all crops ≤ 100 % and NaN zero. **A crop back above 100 %, or NaN
back above zero, is a regression — stop and report, do not proceed.**

Then the local-vs-S3 verify, because per-file upload returns are not proof that objects
landed:

```bash
aws s3 ls --recursive "s3://digital-atlas/domain=hazard_exposure/source=nex-gddp-cmip6/" \
  | awk '{print $3, $4}' | sort > /tmp/s3_he_$TS.txt
cat /tmp/s3_he_$TS.txt
```

Every object that step 2 claimed to upload must appear with a non-zero size, and each
`.parquet` must still be paired with its `.json`. Re-running the publisher is cheap and
backs up first, so a missing object is fixed by re-running, not by hand.

---

## Logging convention for every step above

Every script here already timestamps each line and prints per-section elapsed time. Keep
the `|& tee logs/<name>_$TS.log` on each command so durations are recoverable afterwards,
and paste the **tail plus any FAIL/WARN lines**, not just the verdict.

---

## COPY-PASTE PROMPT FOR CGLABS

> 🛑 **FIRE ONLY AFTER** `archive/dispatches/DISPATCH_cglabs_issue26_r21_rebake.md` reports the #26 R/2.1
> rebake **done**, **and** p.steward has given an explicit GO on
> `DISPATCH_cglabs_track1_ndws_resume.md`. Until both hold, do not run this.

```
Read DISPATCH_cglabs_track1_ndws_resume.md in hazards_prototype (top block, macbook
2026-09-18), then git pull develop (expect 0057d65 or later).

Context you need: Track-1's hazards#19 NDWS fix is ALREADY LIVE — it shipped inside the
issue #9 publish on 2026-09-16. I verified that from the published parquet (historic
NDWS dry-union frequency 0.1616, not ~1.0; futures 0.149->0.189 rising with forcing;
hazard='none' present; all five scenarios in one file). Do NOT re-bake R/2 or R/3.

Run STEP 1 only (read-only, ~2 min):
  Rscript scripts/stamp_ensemble_membership.R --dry-run

KILL-GATE: it must print n_members = 18 per hazard_risk_vop*/<timeframe> folder, and the
parsed names must look like GCMs (ACCESS-ESM1-5, EC-Earth3, MPI-ESM1-2-HR, TaiESM1, ...).
If it prints NONE FOUND, a count that is not 18, or anything that is not a GCM name:
STOP and paste the log. That parser was validated only against filename shapes I
reconstructed from your logs — real per-GCM _int basenames were not reachable from the
macbook, and a wrong parse would stamp a false ensemble claim into a published product.

Then STEP 1b: list the node's three ENSEMBLEmean tier parquets with sizes and mtimes, and
list s3://digital-atlas/domain=hazard_exposure/source=nex-gddp-cmip6/ recursively. The
live objects are 62.8 / 64.2 / 64.1 MB from 2026-09-16 16:58-16:59. If the node's sizes
differ, say so and STOP — that turns step 2 into a republish decision, which is Pete's.

Do NOT run step 2 (sidecar publish) until I confirm gate 1a is green. Do NOT run step 3
(the intld publish) at all — it needs its own explicit go from p.steward and the #30
unit-allow-list decision first. Nothing else in the repo is in scope for this dispatch;
the #26 session owns R/2.1 and its publish scripts.

Paste: the dry-run log tail, the two listings, and any FAIL/WARN lines.
```
