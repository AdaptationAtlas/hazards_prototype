# Dispatch: PASCAL — pin the Afrilabs host profile (issue #29)

**For:** the Claude session running on Afrilabs / PASCAL.
**Ask:** establish on the node which filesystem paths are authoritative, report them
back, and (optionally) prove one stage runs. Blocks A and B are **read-only**.

**Why it needs doing on the node.** `metadata/hosts.json` carries an `afrilabs` stanza
marked `"status": "UNVERIFIED"`. Its paths were copied verbatim from the old
`R/0_server_setup.R:135` and have never been confirmed. Two descriptions disagree:

- `hosts.json` / old setup: `common_data = /cluster01/workspace/atlas`,
  `working_dir = /cluster01/workspace/atlas/hazards_prototype`
- Pete: `common_space/Workspace/common` (shared data), `common_space/Workspace/atlas`
  (Atlas tree)
- `server-environment.md` (branch `docs/server-environment`): `/cluster01` holds `Data`,
  `Projects` and `Workspace` trees and is world-writable

Nobody off-node can tell which is right, and the design deliberately does not depend on
guessing: it is one JSON stanza, not code.

---

## Context — what already exists

Issue #29 P1-P5 are done and CGlabs-verified. Relevant pieces:

- `R/00_paths.R` — pure resolver (no `setwd`, no `dir.create`, no network). Host profiles
  in `metadata/hosts.json`. `atlas_describe()` prints how everything resolved.
- `R/00_acquire.R` + `metadata/catalogue/*.json` — one downloader, recipes per dataset,
  receipts written beside the data. **All 43 datasets now have an acquire recipe.**
- `R/checks/71_stage_ready.R` — "can this host run stage X", per shard.
- `R/checks/70_path_snapshot.R --resolver-only --check` — golden-file regression test.

**PASCAL has no monthly indices, and that is by design.** They are `regenerate`, not
`must-transfer`: derived from NEX-GDDP-CMIP6, which is public on AWS Open Data with a
published MD5 index. See `metadata/catalogue/nexgddp-indices-monthly.json`. Do not plan a
host-to-host copy — CGlabs runs no SSH daemon.

---

## Block A — probe the filesystem (read-only)

```bash
hostname; whoami; echo "HOME=$HOME"

echo "--- candidate roots ---"
for d in /cluster01 /cluster01/workspace /cluster01/workspace/atlas \
         /cluster01/Data /cluster01/Projects /cluster01/Workspace \
         "$HOME/common_space" /common_space; do
  [ -e "$d" ] && echo "EXISTS  $d" || echo "absent  $d"
done

echo "--- anything named common_space / workspace ---"
find / -maxdepth 3 \( -iname 'common_space' -o -iname 'workspace' \) 2>/dev/null | head

echo "--- one level down each real root ---"
for d in /cluster01 /cluster01/Workspace /cluster01/workspace; do
  [ -d "$d" ] && { echo "== $d"; ls -la "$d" 2>/dev/null | head -12; }
done

echo "--- space ---"
df -h /cluster01 "$HOME" 2>/dev/null

echo "--- repo checkout ---"
find "$HOME" /cluster01 -maxdepth 4 -type d -name hazards_prototype 2>/dev/null | head
```

## Block B — prove a path set works before changing any file

The resolver fails loudly on an unknown host and prints the stanza to paste. Env
overrides outrank the profile, so a candidate root can be tested without editing
anything:

```bash
cd <repo>
git fetch origin && git checkout develop && git pull --ff-only
git log --oneline -1          # confirm you are on the commit you think you are

Rscript -e 'source("R/00_paths.R"); atlas_describe()'

# then, with whatever Block A showed is the real bulk root:
COMMON_DATA=/actual/bulk/root Rscript -e 'source("R/00_paths.R"); atlas_describe()'
```

Every line should read `ok` or `MISS` — `MISS` is fine for trees not staged yet. An
`ERR`, or a hard stop, is the interesting case.

## Block C — report back

Four facts settle the stanza:

| field | question |
|---|---|
| `project_dir` | absolute path of the git checkout on this node |
| `common_data` | the shared bulk root that should hold `nex-gddp-cmip6`, `atlas_sos`, `chirts`, … |
| `working_dir` | where `Data/` should hang off (may equal `common_data/hazards_prototype`) |
| free space | `df -h` on the bulk mount |

Plus: does `common_space` exist as a real path, or is it another view of `/cluster01`?

**Do not commit internal addresses.** This repo is public. `hosts.json` holds filesystem
paths only — no hostnames, IPs or NFS server names. Report those in the message, not in a
file.

---

## Optional Block D — prove a stage end to end

Only after A-C. The exposure chain is the designed proof: every input is
`pull-from-origin`, nothing needs CGlabs.

```bash
cd <repo>
Rscript R/checks/73_catalogue.R --status        # what this host already has
Rscript R/checks/71_stage_ready.R --stage 0.4.1 # what is missing

# fetch exactly what that stage needs, nothing else (~70 MB GLW4 + FAOSTAT + boundaries)
Rscript -e 'source("R/00_paths.R"); source("R/00_acquire.R"); atlas_require_stage("0.4.1")'

Rscript R/checks/71_stage_ready.R --stage 0.4.1  # expect READY
```

Then the cross-host equivalence check against CGlabs:

```bash
Rscript R/checks/72_crosshost_fingerprint.R --stage 0.4 --out /tmp/fp_pascal.tsv
```

Shape (`dim`/`ext`/`res`/`crs`) must match CGlabs exactly. `value` may differ where the
two hosts hold different data vintages — that is a staging difference, not a resolver bug,
and the artefact list will show which.

---

## What happens next

With Block C's four facts, the macbook session edits `metadata/hosts.json`: sets
`common_data` and both `trees` entries, flips `"status"` to `"verified"`, drops the
`_unverified` block, and regenerates `metadata/hosts_expected.tsv`. One stanza, no code.

**Note on the `trees` pin.** Every host except CGlabs pins BOTH `nexgddp` and
`atlas_delta` to the same `working_dir`. The climdat fork only ever applied inside the
CGlabs branch of the old chain; letting the defaults template apply here would split
PASCAL's tree in two. Keep both entries identical unless PASCAL genuinely needs two.
