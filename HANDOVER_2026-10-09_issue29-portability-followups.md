# Topic briefing — issue #29 (cross-host portability): state and open follow-ups

**Date:** 2026-10-09
**Shape:** topic briefing (`HANDOVER_<date>_<topic>.md`), not a session handover. The session
entry point remains `HANDOVER_2026-10-07.md` (issue #13 rebake, in flight on CGlabs).
**For:** a Codex session picking up the portability / multi-server thread. Codex has no access to the
macbook's Claude memory, so everything that lived only there is copied below. `AGENTS.md` still
governs: dispatch loop, gates, public-repo hygiene. Sign commits with your own model name.

## 1. What #29 was

[#29](https://github.com/AdaptationAtlas/hazards_prototype/issues/29) — "Portable data layer: run the
hazards pipeline on Afrilabs, and make data location declarative and syncable across hosts".
**Closed 2026-09-23.** All five items were delivered.

| Item | Delivered as |
|---|---|
| 1. Path resolver | `R/00_paths.R` (pure: no setwd, no dir.create, no network) + `metadata/hosts.json` (host profiles) + golden file `metadata/hosts_expected.tsv`; regression check `R/checks/70_path_snapshot.R --resolver-only --check` (set `ATLAS_HOST=cglabs` to test CGlabs resolution off-node) |
| 2. Dataset catalogue | `metadata/catalogue/<id>.json` (43 records, schema in `_SCHEMA.md`); driver `R/checks/73_catalogue.R` (`--status --dataset --gaps --transfer --orphans --render`); renders `docs/DATA_INDEX.md` |
| 3. Acquisition ("sync") | `R/00_acquire.R` — one downloader, 7 methods, driven by each record's `acquire` block. Entry points `atlas_require(id)`, `atlas_require_stage(stage)`, `ATLAS_PREFETCH=all\|id,id`. Recipe lives in git; a per-host **receipt** is appended to `<working_dir>/Data/_acquisition/<id>.jsonl`. `0_server_setup.R` §3 now downloads nothing by default |
| 4. Stage readiness | `metadata/stages.json` + `R/checks/71_stage_ready.R` (per shard) |
| 5. Stage PASCAL | Afrilabs profile verified 2026-09-18; stage 0.4.1 went NOT READY → READY via `atlas_require_stage` (fetched only `glw4-2020`, byte-identical to the macbook copy); cross-host gate `R/checks/72_crosshost_fingerprint.R --stage 0.4` passed (36 shared artefacts, 0 shape mismatches) |

Guard against regressions: `R/checks/74_no_host_literals.R` fails on any absolute host path outside a
documented allow-list.

**Key design decision — "sync" means regenerate, not transfer.** There is no host-to-host link:
CGlabs has no sshd, and PASCAL's SSH is reachable from the office network only. The raw climate data
(NEX-GDDP-CMIP6) is public on AWS Open Data with an MD5 index, so a new host fetches raw data and runs
`hazards_upstream` 01→04 itself. PASCAL therefore becomes a *producer* host. Atlas S3 is not used as a
transfer medium (too expensive for monthly indices).

Thread records: `archive/dispatches/DISPATCH_cglabs_issue29_paths.md`,
`archive/dispatches/DISPATCH_pascal_issue29_profile.md` (index rows in `archive/dispatches/README.md`).
Host documentation: `server-environment.md` (Afrilabs/PASCAL), `server-environment-cglabs.md`.

## 2. Host facts that cost a round to learn

- **Afrilabs `common_data` = `/cluster01/Workspace/common`.** `working_dir` is a *sibling* under
  `/cluster01/Workspace`, not a child, so it cannot be templated off `{common_data}`. `Workspace` and
  `workspace` are the same inode; the old bug was the subtree, not the casing.
- **PASCAL `~/.Renviron` sets `project_dir`, and R applies it after the shell env**, so
  `project_dir=… Rscript …` is silently ignored. Use `ATLAS_PROJECT_DIR` (checked first by
  `atlas_repo_root()`; that ordering is load-bearing).
- **The CGlabs clock runs about 2 h 56 min behind the macbook.** Every dispatch block should ask the
  node for `git log --oneline -1` **and** its branch — the PASCAL checkout once sat on
  `docs/server-environment` while reporting "Already up to date".
- PASCAL reported results as a **comment on issue #29**, not as a commit. Watch issue comments too.
- `R/0_server_setup.R` still derives `Cglabs` / `Aflabs` from `atlas_host_id()`; they are kept
  because `R/checks/9_mass_conservation_check.R` and `68_categorisation_stage{1,2,3}.R` branch on them.

## 3. Open follow-ups (none blocking; none started)

Ranked by value. None has a GitHub issue yet.

1. **Catalogue checksums.** 42 of 43 records have `"checksum_manifest": null` (only NEX-GDDP has one).
   The same MapSPAM processed tree holds 72 files on CGlabs, 48 on the macbook and 36 on PASCAL, and
   nothing detects it. Populate `origin.checksum_manifest`, or store S3 ETags/object counts, so that
   `atlas_acquire` verifies at download time. Suggested first step: file an issue.
2. **Value-level cross-host gate.** `72_crosshost_fingerprint.R` compares shape (dim/ext/res/crs/nlyr)
   only, and only *inputs*: stage 0.4 has never run on PASCAL (`Data/exposure` and
   `GLW4_2020/processed` are empty there). Running `R/0.4.1_create_livestock_exposure.R` on PASCAL
   (READY since 2026-09-18) and fingerprinting values against CGlabs would make it a real output check.
   Needs a PASCAL dispatch; Pete runs it.
3. **Block D — regeneration sizing.** Never run: time/size to regenerate monthly indices for one GCM
   on PASCAL via `hazards_upstream` 01→04. This is the evidence behind "regenerate, not transfer".
4. **Two corrupt MapSPAM tiles on PASCAL** (`variable=phys-area_ha/spam_phys-area_ha_{rf-all,rf-highinput}.tif`,
   truncated). No stage consumes them. Fix on-node: `atlas_acquire("mapspam-2020v1r2", force = TRUE)`.
5. **`run_hazard_pipeline.sh.txt`** — the Afrilabs path block is immediately overwritten by the
   CGlabs block. Still broken on develop; small fix.
6. **P3 left as-is:** `R/checks/68_categorisation_stage1.R` `resolve_dir()` (L147–175) is on the
   `74` allow-list. Its candidate order is the *opposite* of `R/observational/_bootstrap.R`, and must
   stay that way if it is ever ported.
7. **`glw4-2020`** is the only catalogue record with no acquire recipe (it arrived on CGlabs by an
   unrecorded route). Now fetchable, but its origin is still undocumented in the record.

**Future, not scoped:** load distribution across heterogeneous compute (CGlabs CPU, PASCAL CPU,
GPUs elsewhere). The sharding axes already exist as run controls: `GCMS`, `SCENARIO`, `SSPS`, `YRS`,
`MONTHS`, `FORCE_OVERWRITE`. Pete has said to wait until the pipeline internals stabilise. The
publish-layer revision is a separate future project and stays out of scope.

## 4. Things to know about this checkout

- **Do not touch the #13 rebake.** It is running on CGlabs now (latest: `2b05f4f`, B1 annual
  complete). Portability work must not edit R/2 or R/3 while it is in flight.
- Other sessions edit this working tree concurrently. Commit with explicit `git add <paths>`, never
  `-a` / `-A`. At the time of writing, `HANDOVER_2026-10-07_gcf-theme2-extremes-inventory.md` and
  `metadata/checks/` are untracked and belong to another session — leave them.
- Before committing anything containing node output, run the internal-IP grep in `AGENTS.md` §3.
- Test a change by running the **patched artifact**, not a hand-written equivalent. P2 shipped
  broken once (`c(data.table, …)` unquoted) because its gate called the function with hand-typed args.
