# Topic briefing — issue #58: cross-host portability, and moving production off CGlabs

**Date:** 2026-11-16
**Shape:** topic briefing (`HANDOVER_<date>_<topic>.md`). It replaces
`HANDOVER_2026-10-09_issue29-portability-followups.md` (now in `archive/dispatches/`) as the entry
point for this thread. The session handover for everything else is still the newest
`HANDOVER_<date>.md`.
**For:** the macbook session that picks up #58. `AGENTS.md` governs: dispatch loop, gates, public-repo
hygiene, commit trailer.

## 1. Why this thread matters now

**Pete, 2026-11-13: moving our pipeline off CGlabs is now critical.** PASCAL is the candidate
production host. Until now #58 was a portability nice-to-have; from here it is a migration with a
deadline Pete will set. The order of work below is ranked by what blocks a full move.

Do not write the reason for the urgency into the public repo. "CGlabs availability beyond 2026 is not
assured" is the most that goes in an issue or commit, and only with Pete's OK.

## 2. Where the thread stands

| Item | State (2026-11-16) |
|---|---|
| **Phase A** (checksum manifests, host facts, run_hazard_pipeline fix) | Done. 14/43 records carry a manifest; `R/00_acquire.R` verifies every download. |
| **PASCAL R stack** | Fixed by IT (SR-167702): 1,671 of 1,678 shared packages load. The 5 packages Pete asked for were installed on 2026-11-12. Personal library (`R_LIBS_USER`) still holds 6 packages; it can go once the 5 are confirmed (step 2 below). |
| **Phase B** (value-level cross-host gate, R/0.4.1) | PASCAL half done (P5–P8). Inputs 13/13 identical to the macbook after a same-day FAOSTAT refresh on both. FAOSTAT-derived economics identical to the digit. Every remaining difference traces to **one cause**: PASCAL's system **GDAL 3.4.3** returns wrong sums from `gdalwarp -r sum` whenever the source has a NoData mask (max 7,399 in a cell that holds at most 9; GDAL 3.13 on the same machine gives 9). terra and PROJ are cleared. IT asked to upgrade GDAL on SR-167702 (sent 2026-11-14). CGlabs half not run (CGlabs busy; it runs GDAL 3.11, so its live outputs are expected clean). |
| **Guard** | `R/mass_check.R` (`03a5705`): every `method = "sum"` resample in 0.4.1 now stops if mass is off by >0.5 % against the source cropped to the grid. Fixture `R/checks/fixture_mass_check.R`. 0.4.0 / 0.4.2 / 0.4.4 still carry 15 unguarded sum-resamples. |
| **Phase C** (regenerate monthly indices on PASCAL) | Not started. **Not blocked by the GDAL bug**: `hazards_upstream` has no sum-resample. Needs `wbkernel` (in this repo, `./wbkernel`) installed on PASCAL. |
| **IT ticket SR-167702** | Open. Answered: packages, idle jobs ("process can only die if the Jupyterhub session is killed"), 4 × 20 CPU / 96 GB sessions, no proxy. Pending: GDAL upgrade. Follow-ups go as in-line replies on that thread, and Pete sees the draft first. |

## 3. Blockers for a full move, ranked

1. **GDAL 3.4.3 on PASCAL.** Breaks every sum-resample (19 call sites in R/0.4.x + haz_functions.R). IT upgrade requested. Workaround if slow: `aggregate(fact = n)` works only at integer resolution ratios; `-srcnodata None` is not reachable from terra.
2. **No S3 write credentials on PASCAL.** Publishing is CGlabs-only today (`AtlasDataManageR`/paws). The 2026-10-30 "no creds on PASCAL" decision has to be revisited if PASCAL becomes the publisher.
3. **Phase C never run.** Time, RAM and size to regenerate one GCM's monthly indices through `hazards_upstream` 01→04 are unmeasured. This is the evidence behind "regenerate, not transfer" for ~1.4 TB of CGlabs data plus the index trees.
4. **FAOSTAT provenance.** FAO rewrites its bulk files in place; published VoP cannot be reproduced once FAOSTAT moves. A hashed snapshot on Atlas S3 would fix it (CGlabs + publish GO).
5. **Compute cap.** PASCAL hub sessions are capped at 20 CPU / 96 GiB, four at a time; the uncapped routes (SSH, RStudio) are office-only. CGlabs gives one job 40 cores / 376 GiB. R/3 §4.1 is the known OOM risk.

## 4. Next steps, in order

1. **Migration plan** (macbook). A table of everything CGlabs does today — R/1–R/3 bakes, the observed pipeline (`R/obs`, `python/obs`), publishing, the index trees, the data on disk — with move / regenerate / drop per item, its blocker, and the order. Post it as a topic briefing and summarise on #58.
2. **PASCAL P9** (short, mostly read-only): confirm the 5 IT-installed packages load from the shared library; delete them from `R_LIBS_USER`; install `wbkernel` and `trendkernel` from the checkout (`R CMD INSTALL wbkernel --library=$R_LIBS_USER`); re-run `R/checks/76_r_env_audit.R` — expect `0.4-chain` and `both` clean, `upstream` MISSING only the climate4R/download packages outside 01→04.
3. **Phase C** (PASCAL, **Large profile**, hours): one GCM × one scenario through `hazards_upstream` 01→04. Run controls are in `hazards_upstream/R/00_setup.R` (`COMMON_DATA`, `GCMS`, `SCENARIO`, `SSPS`, `YRS`). Record wall time, peak RSS and output bytes per stage. Write the dispatch only after reading `02_preprocess_data/preprocess_nex-gddp-cmip6_daily_data_v2_0.R` and the 04 scripts; write a synthetic probe first (AGENTS.md rule).
4. **Extend `mass_check()`** to the 15 sum-resamples in 0.4.0 / 0.4.2 / 0.4.4, with the fixture's static check widened.
5. **Later:** CGlabs half of phase B (`72_crosshost_fingerprint.R --stage 0.4.1`, read-only, ~2 min); re-run PASCAL phase B after the GDAL upgrade (expect a clean value match); the S3 write-credentials decision; the FAOSTAT snapshot; `ggcmi` manifest from a host that reaches PIK.

## 5. Where things are

- Dispatch thread: `DISPATCH_pascal_issue29_followups.md` (newest block on top; P0–P8 and their RESPONSEs).
- Issue: [#58](https://github.com/AdaptationAtlas/hazards_prototype/issues/58).
- Tools built for this thread: `R/checks/75_catalogue_manifest.R` (manifests), `76_r_env_audit.R` (pipeline packages), `77_r_lib_sweep.R` (whole library), `72_crosshost_fingerprint.R --stage 0.4.1 | 0.4.1-inputs | --compare` (value gate), `probe_058_sum_resample.R` (GDAL sum bug), `R/mass_check.R`.
- Reference fingerprints: `metadata/crosshost/` (macbook and PASCAL, 2026-11-12 FAOSTAT).
- Host facts: `server-environment.md` §4, §8, §9 and `metadata/hosts.json` (afrilabs stanza). **Not yet folded in:** the GDAL 3.4.3 sum bug, the 4-session cap, IT's answers of 11-09 and 11-12.

## 6. Things that cost a round

- A read-only node dispatch must say "do not source `R/0_server_setup.R`": its `pacman::p_load()` installs.
- PASCAL's `71_stage_ready.R` said READY while 0.4.1 then died on a missing FAOSTAT file. Fixed (`f5d0939`), but the lesson stands: a readiness check must list every file the script opens.
- A synthetic test without NoData could not reproduce the GDAL bug. Probe with the real input shape (NoData mask, real geometry), not a clean synthetic.
- `terra::resample(method = "sum")` cannot be trusted across hosts without a mass check. The old 0.4.1 warning compared a global source with a partial grid, fired everywhere, and was ignored for months.
- zsh does not word-split `$F="Rscript …"`; dispatch commands are written for bash.
- Node sessions idle after each RESPONSE; Pete relays prompts. End every macbook turn with a copy-paste prompt.
