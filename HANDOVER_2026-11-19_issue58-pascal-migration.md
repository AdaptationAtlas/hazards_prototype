# Topic briefing — issue #58: moving production off CGlabs; PASCAL thread state

**Date:** 2026-11-19
**Shape:** topic briefing (`HANDOVER_<date>_<topic>.md`). Entry point for the #58 thread. It replaces
the 2026-11-16 briefing of the same name (now in `archive/dispatches/`). Its companion is
`HANDOVER_2026-11-16_issue58-migration-plan.md`, the 28-item inventory of what CGlabs does today and
how each piece moves. The session handover for everything else is the newest `HANDOVER_<date>.md`.
**For:** the session that picks up #58 (Claude desktop app from here). `AGENTS.md` governs: dispatch
loop, gates, public-repo hygiene, commit trailer.

## 1. Why this thread matters

**Pete, 2026-11-13: moving our pipeline off CGlabs is now critical.** PASCAL is the production host
in waiting. Do not write the reason into the public repo; "CGlabs availability beyond 2026 is not
assured" is the most that goes in an issue or commit, and only with Pete's OK.

## 2. State, 2026-11-19

| Item | State |
|---|---|
| **Phase A** (manifests, host facts, run_hazard_pipeline) | Done. |
| **PASCAL R environment** | Done (P9, 2026-11-17). Shared library rebuilt by IT; 0.4 chain and `hazards_upstream` 01→04 have no BROKEN or MISSING package. Personal library holds only `wbkernel` and `trendkernel` from this repo. The bakes (R/1–R/4) still need `doFuture` and `jrvFinance` (CRAN; next IT request). |
| **Phase B** (value gate, R/0.4.1) | PASCAL half done (P5–P8). All 13 inputs byte-identical to the macbook; FAOSTAT economics identical. Every output difference traces to **GDAL 3.4.3's `-r sum` over-count with NoData sources**. IT asked to upgrade GDAL (SR-167702, 2026-11-14), no reply yet. CGlabs half not run (CGlabs busy; runs GDAL 3.11). |
| **Guard** | `R/mass_check.R` on all four sum-resamples in 0.4.1 (`03a5705`). 15 sites in 0.4.0/0.4.2/0.4.4 still unguarded. |
| **Phase C** (regenerate one GCM-year on PASCAL) | **In flight.** P10 (`ea8ebba`): download of ACCESS-ESM1-5 / historical / 1995, six variables, 1.56 GB in **169 s** (~10 MB/s); preprocess stopped on a second GDAL fault (time axis as POSIXct → colons in file names). Fixed `d5c9e9d`; **P11 is the newest block and waits for PASCAL** (preprocess → ten indices → C5 table). Water-balance indices and TAI are out of P10/P11 scope: soils, `sroot` and `ET_SolRad` are not on PASCAL. |
| **IT ticket SR-167702** | Answered: R rebuild, packages, 4 sessions × 20 CPU / 96 GB, jobs die only with the hub session, no proxy. **Pending: the GDAL upgrade.** Replies go in-line on the ticket, and Pete sees the draft first. |

## 3. Blockers for a full move, ranked

1. **GDAL 3.4.3 on PASCAL.** Two faults (`server-environment.md` §8 item 9). The sum fault corrupts every `method = "sum"` resample (19 call sites in R/0.4.x + `haz_functions.R`); the netCDF time fault is fixed in our code. The proper fix is IT's upgrade; the apt sources offer only 3.4.1, so it needs another package source.
2. **No S3 write credentials on PASCAL.** Publishing is CGlabs-only. The 2026-10-30 decision has to be revisited if PASCAL publishes.
3. **Phase C unmeasured past the download.** Preprocess and index cost per GCM-year is what P11 returns. Download is not the obstacle: ~6,000 GCM-years ≈ 9.4 TB raw at this rate, deletable after 02.
4. **Inputs that cannot be regenerated:** FAOSTAT (rewritten in place upstream; hashed snapshot on Atlas S3 needed), soils rasters (23 MB from CGlabs), `ET_SolRad`, GGCMI manifest (PIK reachability from PASCAL untested).
5. **Compute cap.** 20 CPU / 96 GiB per session, four sessions. CGlabs gives one job 40 cores / 376 GiB; R/3 §4.1 is the known OOM risk.

## 4. Next steps, in order

1. **Read PASCAL's P11 RESPONSE** when it lands (`DISPATCH_pascal_issue29_followups.md`, newest block). Check it against the P11 invariants (365 tifs per variable, every index `exit=0`, no `daily inputs missing`), run the IP grep, then turn the C5 table into the regenerate-vs-transfer estimate: per GCM-year wall, RSS and bytes × 18 GCMs × (20 historical + 4 × 80 future). Update the migration plan (items 13–16) and comment on #58.
2. **Compare PASCAL's 1995 indices with CGlabs' copy of the same GCM-year** once CGlabs is free: a read-only fingerprint on both (`72_crosshost_fingerprint.R` needs a `--stage upstream-04` target; add it) and `--compare`. That is the first producer-side value gate.
3. **Soils to PASCAL** (23 MB): when CGlabs is free, a one-line copy of `sscp_world.tif` and `ssat_world.tif` to a sandbox S3 key, then `atlas_require` on PASCAL. Then the water-balance indices run on the P10 root (AVAIL seed 1995-01 is in place).
4. **IT follow-ups**, in-line on SR-167702, drafts to Pete first: chase the GDAL upgrade; ask for `doFuture` and `jrvFinance`.
5. **Extend `mass_check()`** to the 15 unguarded sum-resamples.
6. **Later:** CGlabs half of phase B; PASCAL phase B re-run after the GDAL upgrade; S3 write-credentials decision; FAOSTAT snapshot; the `-maxdepth` fix in any block that reuses the CHIRPS `find`.

## 5. Where things are

- Dispatch thread: `DISPATCH_pascal_issue29_followups.md`, newest block on top, P0–P11.
- Issue: [#58](https://github.com/AdaptationAtlas/hazards_prototype/issues/58).
- Tools: `R/checks/75_catalogue_manifest.R`, `76_r_env_audit.R` (scope now includes the bakes and `R/obs`), `77_r_lib_sweep.R`, `72_crosshost_fingerprint.R --stage 0.4.1 | 0.4.1-inputs | --compare`, `probe_058_sum_resample.R`, `R/mass_check.R`, `fixture_mass_check.R`, `fixture_upstream_acquire_controls.R`, `fixture_upstream_run_params.R`.
- `hazards_upstream` run controls: `COMMON_DATA`, `GCMS`, `SCENARIO`, `SSPS`, `YRS`, `VARS`, `MONTHS`, `DRY_RUN`, `PROBE_WORKERS`, `DAYS_CHECK` (`00_setup.R`). The canonical acquire scripts are `01_download_data/download_manual_nex_gddpCMIP6.R` and `02_preprocess_data/preprocess_nex-gddp-cmip6_daily_data.R` (not the `_v2_0` variants, which carry a CGlabs path literal).
- PASCAL phase C root: `$CD = <bulk>/atlas/hazards_upstream_pascal`, holding six raw `.nc`, the real CHIRPS extent file, and (after P11) the daily tifs and indices.
- Reference fingerprints: `metadata/crosshost/`.
- Host facts: `server-environment.md` §3, §4, §8, §9; `metadata/hosts.json` afrilabs stanza. Folded in through P10.
- IT thread: Outlook, subject `SR-167702 - PASCAL: help with R package failures and server access`.

## 6. Things that cost a round

- A read-only node dispatch must say "do not source `R/0_server_setup.R`" (`pacman::p_load()` installs).
- `71_stage_ready.R` must list every file a script opens (fixed for 0.4.1 in `f5d0939`).
- Probe with the real input shape: a clean synthetic raster could not reproduce the GDAL sum fault (NoData was the trigger), and a Date-axis `.nc` could not reproduce the time fault (a POSIXct axis does).
- `terra::resample(method = "sum")` is not trustworthy across hosts without a mass check.
- `find -maxdepth 3` missed the CHIRPS file five levels down; the node searched wider rather than writing the stand-in. Give nodes the reason for a command, not just the command.
- A dispatch's "expect" that names a file-suffix the bucket does not guarantee becomes a false stop; state invariants (six files, one per variable), not spellings.
- zsh does not word-split `$F="Rscript …"`; dispatch commands are written for bash.
- Node sessions idle after each RESPONSE; Pete relays prompts. End every turn with a copy-paste prompt. Pete starts the hub session; a profile change is stop + start the server.
