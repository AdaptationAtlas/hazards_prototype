# Upgrade roadmap — hazards_upstream/ (Stage 0 index producer)

Phase-0 audit output, 2026-06-24 (multi-agent workflow, 7 stage auditors → consolidate;
110 findings). READ-ONLY audit — no edits made. Scope: `hazards_upstream/R/` (the
vendored AdaptationAtlas/hazards nexgddp index producer). Stage 1+ (this repo's `R/`)
is a separate later pass.

## Executive summary
~45 mostly-standalone, terra/GDAL/network-I/O-bound scripts across 7 sub-stages
(download → preprocess → bias-correct → indices → final-maps → metadata → bucket-upload).
The roadmap is **correctness/operability + de-duplication, NOT compute speed** — the heavy
math is already compiled terra/fields; the ONLY genuine R-level per-cell/per-group
arithmetic kernel stage-wide is the soil-water-capacity loop in `createAfricaSoils.R`.
Three issues are publish-or-safety blockers and lead: (1) a live CDS API key committed in
`download_AgERA5.R`; (2) an unguarded irreversible `rm -r`; (3) S3 uploads that never set
`acl='public-read'` and never check success (private + silent-failure publishes). Cross-cutting:
hardcoded paths instead of `atlas_dirs`; no timestamped logging; no `SKIP_/FORCE_OVERWRITE`
controls; silent `file.exists` filters + `tryCatch→NULL` that should be loud `stopifnot`;
heavy copy-paste. The water-balance scripts (NDWS/NDWL0/NDWL50) carry soil-moisture state via
global `<<-` + lexical-last AVAIL lookup + missing `overwrite=TRUE` — **the hazards#19 NDWS
saturation failure class**.

## 🚩 STALE / DEFERRED — upload system (Pete, 2026-06-24, do NOT fix now)
`R/07_bucket_uploads/` (`upload_AWS.R`, `upload_GoogleCloud.R`) target the **legacy
MVP-era path** `s3://digital-atlas/Updates_for_MVP_Release/1_hazards/` (and `gs://adaptation-atlas/`)
— NOT the current `domain=climate/.../processing=analysis-ready/` STAC layout. They are
**flagged STALE**; the current Stage-0 → S3 publish route is elsewhere/unknown. The
**entire pipeline's upload/publish system (Stage 0 AND hazards_prototype push_to_s3.R)
needs a holistic revision — that is a SEPARATE future project, not part of this workout.**
So: the rank-2 ACL/success-gate fixes on the 07 uploaders and the rank-9 uploader
de-dup are **DEFERRED** (not done now). The rank-2 `rm -r` guard is a separate safety
item and is NOT part of this deferral. See [[project-pipeline-upload-revision]].

## ⚠️ SECURITY — act immediately
`R/01_download_data/download_AgERA5.R` L15-16: live ECMWF/CDS credential committed (and now in
`hazards_prototype` history too via the subtree vendor). **Rotate the key at CDS now** (Pete);
replace literals with `Sys.getenv("CDS_UID")/("CDS_KEY")` + `stopifnot`. (Secret value NOT
reproduced here on purpose.)

## Top priorities (ranked impact × low-effort-first)
| # | title | where | dim | impact | effort |
|---|---|---|---|---|---|
| 1 | Rotate + de-hardcode CDS key | 01/download_AgERA5.R L15-16 | security | high | S |
| 2 | Guard rm -r + ACL/success gates on uploads | 02/free-up_space L17-20; 07/upload_AWS L49,60; 07/upload_GoogleCloud | correctness | high | S |
| 3 | Shared `.log` + `SKIP_/FORCE_OVERWRITE` env controls; drop `warn=-1` + `rm(list=ls())` | all 01-07 | standardization | high | M |
| 4 | Source `atlas_dirs`; kill hardcoded absolute paths | 01,02,03(all),04(all),05,06(all) | correctness | high | M |
| 5 | Silent filters → loud `stopifnot` kill-gates (+ fix OR→AND corruption test) | 01,03,04,06,02 | correctness | high | M |
| 6 | Fix dead/wrong `pr2` unit branch while collapsing the 2 NEX-GDDP converters | 02/preprocess_nex-gddp..._daily_data.R + _v2_0.R | correctness | high | M |
| 7 | Templatize 14 near-identical 06_metadata files → driver + config | 06/meta_*.R (2351L, ~90% boilerplate) | structure | high | L |
| 8 | Water-balance state refactor (fixes hazards#19) | 04/fast_calc_NDWS/NDWL0/NDWL50.R | correctness | high | L |
| 9 | De-dup remaining clone families (masks, daily-converters, uploaders, parsers) | 02,03,07 | structure | medium | L |
| 10 | Parallelise outer grids + bounded-parallel downloads (I/O wins, not Rcpp) | 01,03,05 | efficiency | medium | M |

## Rcpp candidates (the WHOLE list — only one)
- `02/createAfricaSoils.R` soil-water-capacity loop (L112-125): genuine per-group scalar
  arithmetic (group_split(id) + map over >1e5 5km Africa land pixels, depth-interp + trapezoidal
  sum). **Try `data.table` grouped op (`dt[, f(...), by=id]`) FIRST**; only port to an INSTALLED
  Rcpp package (not sourceCpp — breaks future workers) if profiling still shows the inner loop
  dominating. Validate vs current output on a 100-pixel sample before any long run.
- Everything else is terra/GDAL/fields/network I/O-bound — Rcpp buys nothing. The `cmpfun`
  wrappers on terra closures (04 HSH/THI) are cargo-cult.

## Cross-stage patterns (fix once, helps all)
- Hardcoded absolute paths in EVERY sub-stage (`/home/jovyan`, `~/common_data`,
  `~/Repositories/hazards`, SMB/UNC `//catalogue`, IP-mount `//192.168.20.97`) — highest-frequency defect.
- Silent-failure idiom everywhere (`fls<-fls[file.exists]`→unguarded `rast()`; `tryCatch→NULL`;
  unchecked `system()`/`put`/`gsutil`; magic completeness counts) → loud `stopifnot`.
- Fragile filename/version parsing (positional split, unescaped `.` regex) — same R/2 landmine class;
  standardize on anchored `([0-9]{4})_([0-9]{2})` + `fixed=TRUE`.
- Massive copy-paste: 14 metadata, 2 NEX-GDDP downloaders, 2 daily-converters, 3 masks, 3 water-balance,
  2 uploaders, + the 18-GCM vector & scenario/yrs config verbatim in all 13 04_indices files.
- No timestamped logging + no env controls + `warn=-1`/`rm(list=ls())` in nearly every stage → one
  shared setup/logging helper fixes all at once.

## Quick wins (do alongside Phase 1-2)
Rotate CDS key + Sys.getenv (01); `acl='public-read'` on both put_object (07 AWS L49,60); DRY_RUN +
unlink + `stopifnot(dir.exists)` on rm -r (02); `stopifnot(length(fls)>0)` after every file.exists
filter (04 all, 06 L85); `overwrite=TRUE` on AVAIL/index writeRaster (04 fast_calc_* L181-183); fix
OR→AND corruption test + guard SD heuristic for n<2 (03 identifyCorruptedFiles); `seq_along` for the
1:0 empty-dir bug (07,04); `terra::tmpFiles(remove=TRUE)` for tempdir cleanup (04); delete confirmed
dead code (04 calc_THI L23; unused Vectorize defs 01/03/04; commented PDF block 03 L311-329); fix
wrong copy-paste headers (06 meta_NDWL0, meta_NTx35 says 'NTx40'); anchor NEX-GDDP v2 version parse;
guard chirps gunzip behind file.exists.

## Sequencing (each phase independently validatable; cheap high-impact first)
1. **Safety/secrets/publish-gates** (ranks 1,2). Validate: failed put aborts loud; published object
   publicly readable; rm -r in DRY_RUN only prints targets. No run dependency — shippable now.
2. **Shared infra**: one sourced helper (`.log` Sys.time+elapsed START/DONE; env run-controls
   SKIP_/FORCE_OVERWRITE/scenario/yrs/gcms; `atlas_dirs` paths); drop `warn=-1`+`rm(list=ls())`; add
   `stopifnot` file-count gates + OR→AND fix (ranks 3,4,5). Validate: short single-GCM/single-month
   run shows timestamped markers, honours env toggles, fails loud on a missing input, resolves paths
   off the jovyan container. GATE before any long run.
3. **De-dup the bug-bearing clone families** folding fixes in: NEX-GDDP converter pair (pr2 unit bug),
   14 metadata → driver+config (headers/.tiff mismatch, vendor+pin metadata.R), then masks/uploaders/
   parsers (ranks 6,7,9). Validate: byte-compare templated output vs legacy per-file on one
   index/mask/manifest before deleting originals.
4. **Water-balance state refactor** (rank 8): explicit-arg state, deterministic prior-month AVAIL +
   stopifnot, overwrite=TRUE, merge NDWL0/NDWL50. Validate: NDWS on a known window shows no saturation
   (hazards#19 regression check); resumed mid-stream run reproduces clean-run bit-for-bit.
5. **Performance**: bounded-parallel NEX-GDDP downloads + hoist/flatten future plans + parallelise 05
   outer grid (rank 10); then the single createAfricaSoils data.table/Rcpp kernel — validated vs
   100-pixel sample, packaged as an installed lib if it goes to C++.

## Also flagged
Unpinned runtime supply-chain deps: `source("https://raw.githubusercontent.com/.../metadata.R")` (06,
unpinned, fails offline on cglabs), remote AWCPTF.R over https (02), `geodata:::` / `future:::` triple-
colon internals (01,03). Vendor + pin into `references/`.
