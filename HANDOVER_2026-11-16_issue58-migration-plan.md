# Topic briefing — issue #58: what CGlabs does today, and how each piece moves to PASCAL

**Date:** 2026-11-16
**Shape:** topic briefing (`HANDOVER_<date>_<topic>.md`). Companion to
`HANDOVER_2026-11-16_issue58-pascal-migration.md`, which holds the thread state, the ranked blockers
and the next steps. This file is the inventory: every piece of work CGlabs performs today, what
happens to it, what stands in the way, and in what order.
**For:** the macbook session running #58, and anyone deciding sequencing or budget.
**Status:** plan only. Nothing here has been executed, and phase C has not started.

## 1. Scope and the rule that shapes it

PASCAL is the candidate production host. The question this plan answers is not "how do we copy
CGlabs" but "what must exist on PASCAL before CGlabs stops being needed".

**The governing rule is regenerate, not transfer.** CGlabs holds roughly 1.4 TB under
`common_data` (637 G in the NEX-GDDP hazards working directory, 759 G in the legacy
`hazards_prototype` tree, `server-environment-cglabs.md` §4). Almost all of it is derived: it can be
rebuilt from pinned inputs by running the pipeline. Transferring it would move a terabyte of
intermediates across an internal link and would still leave PASCAL unproven. Regenerating it both
delivers the data and demonstrates that PASCAL can produce it — which is the actual acceptance
condition for the move.

The exception is anything that **cannot be regenerated**, listed in §4. That set is small, and it is
the only part of the move with a deadline imposed by someone other than us.

## 2. The inventory

Disposition is one of **move** (copy the bytes or the credential; nothing else will do),
**regenerate** (rebuild on PASCAL from pinned inputs), or **drop** (nothing on PASCAL needs it).

### 2.1 Acquisition and inputs

| # | What CGlabs does today | Disposition | Blocker | Notes |
|---|---|---|---|---|
| 1 | Fetches pinned source data via `R/00_acquire.R` with per-object manifests (size + multipart ETag) | **regenerate** | none | Already proven on PASCAL: MapSPAM 48/48 verified, FAOSTAT re-fetched with `drift 0` (P2, P7). 14 of 43 records carry a manifest. |
| 2 | Holds MapSPAM, GLW4 2020, GAUL boundaries, soils | **regenerate** | none | PASCAL holds these and they are byte-verified against the manifests. |
| 3 | Holds the FAOSTAT bulk CSVs | **move or snapshot** | blocker 4 | FAO rewrites bulk files in place. See §4. |
| 4 | Holds the GGCMI crop calendars | **unknown — test first** | PIK reachability, untested from PASCAL | No manifest exists. The reachability failure is proven only for the macbook; PASCAL has never tried. A read-only manifest build from PASCAL decides whether this is a regenerate or a snapshot. See §4. |
| 5 | Holds CHIRPS / CHIRTS-ERA5 / AgERA5 via the CDH Hugging Face Zarr cubes | **regenerate** | none | Public, versioned, re-readable. PASCAL's `obs` conda env has GDAL 3.13.3 and zarr 3.4.1, which is what the reader needs. |

### 2.2 The NEX-GDDP climate and index trees

| # | What CGlabs does today | Disposition | Blocker | Notes |
|---|---|---|---|---|
| 6 | Holds the NEX-GDDP-CMIP6 daily inputs | **regenerate** | download time | Public. Re-downloading is bandwidth, not risk. |
| 7 | Runs `hazards_upstream` 01→04 to produce the monthly index trees | **regenerate** | blocker 3 (unmeasured), compute cap | This is phase C. **Not blocked by the GDAL bug** — `hazards_upstream` contains no sum-resample. Needs `wbkernel` installed (P9). |
| 8 | Holds the monthly index trees themselves | **regenerate** | follows item 7 | The bulk of the 637 G. |
| 9 | Holds the legacy `common_data/hazards_prototype` tree (759 G) | **drop** | none | Superseded working directory. Confirm with Pete before anything is deleted on CGlabs; nothing on PASCAL needs it. |

### 2.3 The exposure and economics chain (R/0.4.x)

| # | What CGlabs does today | Disposition | Blocker | Notes |
|---|---|---|---|---|
| 10 | `0.4.0` crop VoP intld15, `0.4.2` VoP nominal USD, `0.4.3` FAO production CV, `0.4.4` process exposure, `0.4.5` FAOSTAT long | **regenerate** | **blocker 1 (GDAL)** | These four carry 15 sum-resamples with no mass check (next step 4 in the thread briefing). |
| 11 | `0.4.1` livestock exposure | **regenerate** | **blocker 1 (GDAL)** | Guarded since `03a5705`: `R/mass_check.R` stops if mass drifts >0.5 %. On today's PASCAL it would stop, correctly. |
| 12 | Phase B value gate (`72_crosshost_fingerprint.R`) | **regenerate** | CGlabs half not run | PASCAL half done, inputs 13/13 identical. CGlabs half is ~2 min read-only, waiting on CGlabs being free. |

### 2.4 The bakes (R/1 – R/4)

| # | What CGlabs does today | Disposition | Blocker | Notes |
|---|---|---|---|---|
| 13 | `R/1` hazard timeseries (+ `1.2` ISIMIP, `1.3` CropSuite) | **regenerate** | follows item 7 | Consumes the index trees. |
| 14 | `R/2` hazard frequency, `2.1` monthly tables, `2.2` hazard change | **regenerate** | needs `trendkernel` on PASCAL (P9); compute cap | `2.1` calls `trendkernel::mk_sen_cpp` through the namespace, so it must be an installed package, not sourced C++. |
| 15 | `R/3` frequency × exposure (+ `3.1` season weightings) | **regenerate** | **blocker 5 (compute cap)** | R/3 §4.1 is the known OOM risk. CGlabs gives one job 40 cores / 376 GiB; a PASCAL hub session caps at 20 CPU / 96 GiB. Needs a memory profile before it is trusted. |
| 16 | `R/4` ROI and `4.1` economic indicators | **regenerate** | none known | Small relative to R/3. |

### 2.5 The observed pipeline

| # | What CGlabs does today | Disposition | Blocker | Notes |
|---|---|---|---|---|
| 17 | S1 ingest (`python/obs/s1_ingest.py`): CDH Zarr → daily 0.25° GeoTIFFs, ~38k files, ~18 GB | **regenerate** | none | Must run in the `obs` conda env (GDAL 3.13.3). PASCAL and the macbook already produced byte-identical S1 smoke outputs (`DISPATCH_pascal_obs_s1_repro.md`). |
| 18 | S2 monthly indices for the observed token (upstream `fast_calc_*`) | **regenerate** | follows item 17 | Same code path as item 7. |
| 19 | S3–S5 observed hazards (`R/obs/s3_s5_obs_hazards.R`) | **regenerate** | follows item 18 | |
| 20 | S6 rangeland FPAR (`R/obs/s6_rangeland_fpar.R`) | **regenerate** | none | PASCAL has already run the S6 variant comparison (2026-11-02), so the code path is exercised there. |
| 21 | S7 trends, normals and anomalies (`R/obs/s7_trends_anomalies.R`), 3.6 GB of outputs | **regenerate** | follows item 19 | Cheap to rebuild: 37 min for the main token, 8 min for `-rh15`. Selftest passes on PASCAL (22 ok). |
| 22 | The observed admin tables (`_admin_parts`, three weightings, three levels) | **regenerate** | follows item 21 | Produced by `--admin-only`; 37 min for both tokens. |

### 2.6 Publishing and catalogue

| # | What CGlabs does today | Disposition | Blocker | Notes |
|---|---|---|---|---|
| 23 | Publishes to `s3://digital-atlas` (`scripts/r3_publish_tiers.R`, `r21_publish_to_s3.R`, `r3_publish_moderate_extreme.R`, `R/build_publish_C.R`) via `AtlasDataManageR` / paws | **move** | **blocker 2 (no S3 write credentials on PASCAL)** | The 2026-10-30 "no credentials on PASCAL" decision has to be revisited, or publishing stays on CGlabs after everything else has moved. |
| 24 | Writes the CDH catalogue records and changelogs (`metadata/cdh/`) | **move** | follows item 23 | Repo-resident, so the move is the credential and the habit, not the bytes. |
| 25 | Keeps `sandbox/backup/` and `_parked_*` directories | **drop** | Pete's call | Retain until explicitly released (AGENTS.md §1). |

### 2.7 Host services and facts

| # | What CGlabs does today | Disposition | Blocker | Notes |
|---|---|---|---|---|
| 26 | Long jobs under `nohup`, logs in `logs/` | **regenerate** | none | Works the same on PASCAL. Hub sessions survive job death; a session kill does not. |
| 27 | Runs R 4.4 with a complete package set | **regenerate** | none, after P9 | PASCAL is R 4.6.1 with 1,671 of 1,678 shared packages loading after IT's rebuild (SR-167702). P9 closes the remainder. |
| 28 | Gives one job 40 cores / 376 GiB | **no equivalent** | **blocker 5** | PASCAL hub: 20 CPU / 96 GiB, four sessions. The uncapped routes (SSH, RStudio) are office-only. This is a scheduling constraint on items 7, 15 and 18. |

## 3. Order of work

The ordering is driven by three things: what unblocks the most downstream items, what has a clock
running on it, and what can be measured cheaply before committing to a long run.

1. **P9 — finish the PASCAL R environment.** Confirm the five IT-installed packages load from the
   shared library, drop them from the personal library, install `wbkernel` and `trendkernel` from the
   checkout, re-audit. Unblocks items 7, 14 and 18. Minutes, mostly read-only.
2. **FAOSTAT snapshot.** The only item with a proven external clock (§4). Needs a CGlabs run and a
   publish GO, so it should be requested early even though CGlabs is busy.
3. **GGCMI reachability probe from PASCAL.** Read-only, minutes, no dependencies: build the `ggcmi`
   manifest with `75_catalogue_manifest.R` and see whether PIK answers from that network. It decides
   whether item 4 is a regenerate or a second snapshot, and it is cheap enough that assuming the answer is
   the wrong call. Fold into the block after P9.
4. **Phase C measurement.** One GCM × one scenario through `hazards_upstream` 01→04 on a Large
   profile, recording wall time, peak RSS and output bytes per stage. This is the evidence behind
   every "regenerate" in §2.2 and §2.5, and the input to the §5 estimate. **Awaiting Pete's GO.**
5. **GDAL upgrade on PASCAL** (IT, SR-167702, requested 2026-11-14). Unblocks items 10, 11 and the
   second half of phase B. Until it lands, nothing in R/0.4.x can be trusted on PASCAL — and with
   `mass_check()` in place, 0.4.1 will stop rather than produce wrong numbers.
6. **Extend `mass_check()`** to the 15 unguarded sum-resamples in 0.4.0 / 0.4.2 / 0.4.4. Macbook
   work, no node time, and it converts blocker 1 from a silent risk into a loud one everywhere.
7. **Phase B, both halves.** CGlabs half (~2 min, read-only) when CGlabs is free; PASCAL half re-run
   after the GDAL upgrade, expecting a clean value match.
8. **Regenerate the index trees** (item 7, then 8) at full scale, once phase C has sized it.
9. **Regenerate the bakes** R/1 → R/4 (items 13–16), with R/3 §4.1 profiled against the 96 GiB cap
   before the full run.
10. **Regenerate the observed pipeline** S1 → S7 (items 17–22). Independent of the NEX trees, so it
   can run in a second hub session alongside step 7 or 8, subject to the four-session cap.
11. **Publishing decision and first PASCAL publish** (items 23, 24). This is the last gate, because
    until it is resolved PASCAL can produce everything and still not ship it.
12. **Release CGlabs storage** (items 9, 25) only after a full PASCAL product has been published and
    verified.

Steps 1, 2, 3, 5 and 6 are in flight or ready now and do not contend for the same resources.
Step 4, phase C, is the one that needs a decision.

## 4. What cannot be regenerated

Three things, and they are the reason the move is not purely a compute exercise.

- **FAOSTAT bulk CSVs.** FAO rewrites its bulk files in place, with no version history. The VoP
  products already published cannot be reproduced once FAO moves to the next release. Phase B
  worked around this by re-fetching on both hosts on the same day (P7), which is a coincidence, not a
  method. **Fix:** a hashed snapshot of the five CSVs on Atlas S3, added to the `#58` manifest set, so
  every host fetches the pinned release. Needs a CGlabs run and a publish GO.
- **GGCMI crop calendars — not yet established.** No manifest exists, and the thread has been writing
  "no host reaches PIK" when what was actually measured is that **the macbook does not**. PASCAL sits on a
  different network and has never been asked. **Before this is called unregenerable**, a later PASCAL block
  should run `R/checks/75_catalogue_manifest.R` against the `ggcmi` record, read-only, and report whether
  PIK answers. If it does, GGCMI becomes a regenerate with a manifest like every other pinned input. If it
  does not, it joins FAOSTAT as a snapshot-to-S3 item and the CGlabs bytes may be the only copy we control.
- **S3 write credentials.** Not data, but equally unregenerable from PASCAL's side: they are issued,
  not computed. Blocker 2.

Everything else in §2 is a function of pinned inputs and code in this repository.

## 5. What this plan does not yet know

- **Whether PIK answers from PASCAL** (step 3). Unmeasured, and it changes the disposition of item 4.
- **The size of phase C.** Wall time, peak RSS and output bytes per stage for one GCM × one scenario
  are unmeasured. Without them, steps 7–9 cannot be scheduled and the four-session cap cannot be
  turned into a calendar.
- **Whether R/3 §4.1 fits in 96 GiB.** It was tuned against 376 GiB on CGlabs, where §4.2 is already
  pinned to a single worker for memory reasons.
- **Whether the GDAL upgrade lands.** If IT cannot upgrade, the fallback is `aggregate(fact = n)`,
  which is only valid at integer resolution ratios, and a review of all 19 sum-resample call sites to
  see how many qualify. That would be a code change of real size, so it is worth knowing early.
- **Whether PASCAL becomes the publisher or only the producer.** A split where PASCAL computes and
  CGlabs publishes is viable for a while and removes blocker 2 from the critical path, at the cost of
  keeping a CGlabs dependency alive.

## 6. Where the facts come from

`server-environment-cglabs.md` §3, §4, §9 (CGlabs hardware, storage, gotchas);
`server-environment.md` §1–§5 and `metadata/hosts.json` (PASCAL hardware, profiles, R and Python
environments, GDAL versions); `HANDOVER_2026-11-16_issue58-pascal-migration.md` (thread state and
ranked blockers); `DISPATCH_pascal_issue29_followups.md` P0–P8 (acquire-layer and R-environment
evidence, the GDAL sum bug); `PLAN_obs_hazard_pipeline.md` (observed pipeline stages and sizes);
`DISPATCH_cglabs_obs_long_series.md` (S7 timings and output sizes).
