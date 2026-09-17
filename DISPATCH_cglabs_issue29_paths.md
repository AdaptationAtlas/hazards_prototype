# Dispatch: issue #29 — path resolver, CGlabs verification

**Status:** P0 landed on `develop`. Blocks A and C below are runnable now. Block B
waits until P1 (the `R/0_server_setup.R` rewiring) lands.

**What P0 added.** A declarative path resolver, `R/00_paths.R`, plus host profiles in
`metadata/hosts.json`. **Nothing sources it yet** — `R/0_server_setup.R` is untouched, so
P0 cannot change any path on any host. The point of this dispatch is to prove that
*before* P1 wires it in.

**Safety.** Blocks A and B are read-only with one caveat, stated inline. Block C runs a
stage and writes outputs, so it is gated on an explicit output root.

---

## Block A — verify the resolver reproduces CGlabs paths (read-only, ~2 seconds)

`R/00_paths.R` is pure: no `setwd`, no `dir.create`, no network. This block sources only
that file. It cannot affect an in-flight run.

```bash
cd <hazards_prototype>
git fetch origin && git checkout develop && git pull --ff-only

# A1. Golden-file check: does the resolver still produce the committed values
#     for every host x climdat_source? Expect "PASS". Exits 0.
Rscript R/checks/70_path_snapshot.R --resolver-only --check

# A2. Print how THIS box resolves, and confirm the host profile matched.
#     Expect host=cglabs, and every ok/MISS line to look right.
Rscript R/checks/70_path_snapshot.R --resolver-only --out - | awk -F'\t' '$1=="cglabs"'
Rscript -e 'source("R/00_paths.R"); atlas_describe()'
```

**Report back:** the full output of A1 and the `atlas_describe()` table from A2.

The four values that matter most — these are the literals currently hardcoded at
`R/0_server_setup.R:109,113,150,151,156,157` and must come back byte-identical:

| key | climdat_source | expected |
|---|---|---|
| `working` | nexgddp | `/home/jovyan/common_data/nex-gddp-cimp6_hazards` |
| `working` | atlas_delta | `/home/jovyan/common_data/hazards_prototype` |
| `indices` | nexgddp | `/home/jovyan/common_data/atlas_nex-gddp_hazards/cmip6/indices` |
| `indices` | atlas_delta | `/home/jovyan/common_data/atlas_hazards/cmip6/indices` |

If any row differs, **stop and report** — do not proceed to P1.

---

## Block B — `pre` / `post` snapshot of the real setup (run around P1)

This block sources the actual `R/0_server_setup.R`, so read the caveat:

> **Caveat.** `0_server_setup.R` calls `dir.create()` (no-op where the tree exists) and
> runs its section-3 downloads, which are all skip-if-exists. It writes no pipeline
> output and starts no computation. Running it mid-flight is safe, but it is not
> literally read-only, so run it when convenient rather than during a delicate window.

```bash
cd <hazards_prototype>

# B1. BEFORE P1 lands — capture the baseline from the current checkout.
Rscript R/checks/70_path_snapshot.R --out /tmp/issue29_pre.tsv

# B2. AFTER P1 lands — pull, recapture, diff.
git pull --ff-only
Rscript R/checks/70_path_snapshot.R --out /tmp/issue29_post.tsv
diff -u /tmp/issue29_pre.tsv /tmp/issue29_post.tsv && echo "IDENTICAL — gate passed"
```

**Gate: on CGlabs the diff must be empty.** No exceptions, no "only the raw dirs
changed". An empty diff is the whole acceptance criterion for P1 on this host.

**Report back:** both TSVs, and the diff (or confirmation that it was empty).

---

## Block C — stage-readiness and the cross-host proof

C1 is read-only and answers "could this host run stage X".

```bash
cd <hazards_prototype>
Rscript R/checks/71_stage_ready.R --list
Rscript R/checks/71_stage_ready.R --all           # exits non-zero if anything is missing
Rscript R/checks/71_stage_ready.R --stage 3 --shard timeframe=annual
```

C2 fingerprints the exposure-chain artefacts so CGlabs and the laptop can be compared.
This is the equivalence proof for the refactor. **Read-only** — it only reads existing
artefacts and writes one TSV.

```bash
# Roughly 2 s per single-layer raster and ~2 s per 42-layer stack; the laptop
# took ~112 s for 48 MapSPAM stacks. Budget a few minutes, not hours.
Rscript R/checks/72_crosshost_fingerprint.R --stage 0.4 --out /tmp/fp_cglabs.tsv
```

**Report back:** `/tmp/fp_cglabs.tsv`, plus the `71_stage_ready.R --all` output.

The comparison happens on the laptop side: `diff -u /tmp/fp_cglabs.tsv /tmp/fp_mac.tsv`.
Expect the `shape` column to match exactly. The `value` column may differ where the two
hosts hold different data vintages — that is a staging difference, not a resolver bug, and
the artefact list itself will show it.

---

## Notes

- **Do not run `0.4.x` to regenerate anything on CGlabs for this.** Block C fingerprints
  what is already on disk. If a re-run is wanted later it needs `FORCE_OVERWRITE=1` and a
  dedicated output root, so it cannot collide with the idempotent skip-if-exists
  production tree.
- `metadata/hosts.json` carries the Afrilabs/PASCAL stanza marked `"status": "UNVERIFIED"`.
  Its `working_dir` is copied verbatim from the old `R/0_server_setup.R:135` and has not
  been confirmed on that node. Two descriptions of that filesystem disagree. Correcting it
  is a JSON edit, no R changes.
