# Topic briefing — issue #58: moving production off CGlabs; PASCAL thread state

**Date:** 2026-11-19 (updated the same day after P11 and P12)
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
| **PASCAL R environment** | Done (P9, 2026-11-17; re-audited after the GDAL upgrade in P12 G2). Shared library rebuilt by IT; 0.4 chain and `hazards_upstream` 01→04 have no BROKEN or MISSING package. One known BROKEN outside that path: `rgdal` (retired from CRAN, built for GDAL 3.2), used only by the legacy `download_rawCMIP6.R`, which also needs five MISSING climate4R packages. Personal library holds only `wbkernel` and `trendkernel` from this repo. The bakes (R/1–R/4) still need `doFuture` and `jrvFinance` (CRAN; next IT request). |
| **Phase B** (value gate, R/0.4.1) | PASCAL half done (P5–P8). All 13 inputs byte-identical to the macbook; FAOSTAT economics identical. Every output difference traced to **GDAL 3.4.3's `-r sum` over-count with NoData sources**. **GDAL is now 3.8.4** (IT 2026-11-18); the sum-resample probe matches the macbook to the digit (P12 G1). **P13 `[M-20261119-i58-2]` closed it on GDAL 3.8.4**: inputs 13/13 `same`, res-05 PASS, res-25 all `same` but `shoat_prop` (3.95e-06, rounding order in a ratio; accepted), all four `mass_check` sites conserve mass. CGlabs half not run (CGlabs busy; runs GDAL 3.11). |
| **Guard** | `R/mass_check.R` on **all eleven** sum-resamples: 0.4.1 (4, `03a5705`), and since 2026-11-20 0.4.0 (1), 0.4.4 (3) and `haz_functions.R` (3: `ExtractHaz`, `read_spam`, via `.mass_check_fn()`). The old global-total warnings there are gone; a failure now stops. `fixture_mass_check.R` asserts the wiring in all four files. 0.4.2 has none. Not yet exercised on real data: the first 0.4.0/0.4.4 run on PASCAL is the test. |
| **Phase C** (regenerate one GCM-year on PASCAL) | **Measured (P11 on GDAL 3.4.3, P12 on 3.8.4, both 2026-11-19).** Ten indices end to end, every script exit 0; daily and index tifs byte-identical across the two GDALs. Per GCM-year: 11,330 s (download 169, `02` 8,652 single-threaded, `04` 2,509), peak 19.7 GiB, 5.16 GB. Full matrix: ≈ 200 days on one Large session against ≈ 3 days to transfer the CGlabs tree — migration plan §2.2a. PASCAL's 1995 fingerprint: `metadata/crosshost/fp_up04_pascal_ACCESS-ESM1-5_historical_1995.tsv` (552 artefacts). History: P10 (`ea8ebba`): download of ACCESS-ESM1-5 / historical / 1995, six variables, 1.56 GB in **169 s** (~10 MB/s); preprocess stopped on a second GDAL fault (time axis as POSIXct → colons in file names). Fixed `d5c9e9d`. Water-balance indices and TAI are out of P10/P11 scope: soils, `sroot` and `ET_SolRad` are not on PASCAL. |
| **IT ticket SR-167702** | Answered: R rebuild, packages, 4 sessions × 20 CPU / 96 GB, jobs die only with the hub session, no proxy. **2026-11-18: GDAL upgraded to 3.8.4 and the modules recompiled** (verified P12 G0–G1; PROJ moved to 9.3.1). Open: `doFuture`, `jrvFinance`. Pete sent our reply on 2026-11-19. Replies go in-line on the ticket, and Pete sees the draft first. |

## 3. Blockers for a full move, ranked

1. ~~GDAL 3.4.3 on PASCAL~~ — **cleared**: 3.8.4 since 2026-11-18, sum-resample probe correct (P12 G1). Phase B re-run (P13) confirms it on real products. The noon POSIXct time axis persists on 3.8.4, so the date-only fix in `02` is permanent.
2. **No S3 write credentials on PASCAL.** Publishing is CGlabs-only. The 2026-10-30 decision has to be revisited if PASCAL publishes.
3. **Regenerating the index trees costs months** (migration plan §2.2a): ≈ 200 days on one Large session, bound by `02`'s single thread and 19.7 GiB peak. Transfer is days. **Decided 2026-11-19: transfer** (migration plan §2.2a); regeneration kept as the proof on sample GCM-years.
4. **Inputs that cannot be regenerated:** FAOSTAT (rewritten in place upstream; hashed snapshot on Atlas S3 needed), soils rasters (23 MB from CGlabs), `ET_SolRad`. GGCMI is off this list: PIK answers from PASCAL and the manifest is built (P16).
5. **Compute cap.** 20 CPU / 96 GiB per session; four sessions across all users, so plan on one. CGlabs gives one job 40 cores / 376 GiB; R/3 §4.1 is the known OOM risk.

## 4. Next steps, in order

1. **P15 duplicate census** `[M-20261119-i58-4]` is now run from a separate macbook session (codes `M-YYYYMMDD-i58dup-n`): a metadata-only census of the whole shared store (`scripts/storage_census.py`; P14 superseded). Owners appear as `owner-NN`; the username mapping stays on the node under `logs/p15_*` (git-ignored) for Pete to contact owners. Its numbers feed the IT storage-cost question on SR-167702.
2. **Items 7–8 decided: transfer** (2026-11-19, migration plan §2.2a). Comment on #58 with the measured figures and the decision (Pete sees the comment first). The transfer itself needs CGlabs time: a read-only `du` and manifest of the index tree, then the route (S3 or direct), then copy and verify on PASCAL.
3. **Compare PASCAL's 1995 indices with CGlabs' copy of the same GCM-year** once CGlabs is free: `72_crosshost_fingerprint.R --stage upstream-04 --root <common_data> --out fp_up04_cglabs_ACCESS-ESM1-5_historical_1995.tsv` read-only there, then `--compare` against the PASCAL TSV. The first producer-side value gate. Add a read-only `du` of CGlabs' index tree per GCM-year, to reconcile 637 G with §2.2a.
4. **Soils to PASCAL** (23 MB): when CGlabs is free, a one-line copy of `sscp_world.tif` and `ssat_world.tif` to a sandbox S3 key, then `atlas_require` on PASCAL. Then the water-balance indices run on the P10 root (AVAIL seed 1995-01 is in place).
5. **IT follow-ups**: reply sent by Pete (2026-11-19). Wait for `doFuture` and `jrvFinance`, then re-run `76_r_env_audit.R` on PASCAL before the R/1–R/4 bakes.
6. ~~**Extend `mass_check()`**~~ **Done 2026-11-20**: all seven sites guarded. If the first real 0.4.0/0.4.4 run on PASCAL stops on a ratio just outside 0.5 %, suspect edge cells at a partial extent before GDAL; `ATLAS_MASS_CHECK=warn` lets that run finish for diagnosis.
7. **Later:** CGlabs half of phase B; parallelise `02` per variable if regeneration is chosen; S3 write-credentials decision; FAOSTAT snapshot; the `-maxdepth` fix in any block that reuses the CHIRPS `find`.

## 5. Where things are

- Dispatch thread: `DISPATCH_pascal_issue29_followups.md`, newest block on top, P0–P16 (P14 superseded; P16 = GGCMI reachability, settled).
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
