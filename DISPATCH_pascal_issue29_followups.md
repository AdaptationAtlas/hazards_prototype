# DISPATCH — pascal — #58 (#29 follow-ups) phase A: R environment audit + MapSPAM re-fetch

**Append-only; newest block on top. Prepend `### RESPONSE — pascal` to answer.**

### RESPONSE — pascal 2026-11-11 — P6: IT's rebuild verified, **588 failures down to 7**. P5: **STOPPED at P5.1**, a FAOSTAT world-prices file 0.4.1 needs is absent and `71_stage_ready.R` does not check for it.

Two results, in the order the 2026-11-11 note asks for.

**P6 is good news.** The shared library, tested as an ordinary account with the personal library excluded, now loads 1,671 of 1,678 packages. `arrow`, `geoarrow` and `duckdb` are present, built under R 4.6.1, and all three load. A draft reply to IT is at the end of this block.

**P5 stopped at a stop rule.** 0.4.1 failed partway through res-25 on a missing input, so there is no `COMPLETE` line, res-05 never started, and the two output fingerprints do not exist. The inputs fingerprint was produced and compared, because that is what describes the stop. The eight hash-pinned inputs are `same` as the macbook's. No fix was improvised.

---

## P6 — shared-library retest

#### Account, R version and library paths

Run as an ordinary account, not root. The active R is **4.6.1**, so the "if 4.6.1 is unavailable, stop there" branch does not apply.

```
R version 4.6.1 (2026-06-24) -- "Happy Hop"
```

Library paths with the personal library excluded (`R_LIBS_USER` pointed at a path that does not exist, plus `--no-environ --no-init-file`):

```
/usr/local/lib/R/site-library
/usr/lib/R/site-library
/usr/lib/R/library
```

The personal library normally sits first, ahead of those three. It was left in place and untouched; it was only hidden for the duration of these tests.

#### The seven packages IT asked about

Each loaded in its own fresh R with the personal library hidden, so every result below is the shared library's own:

```
rlang       1.3.0     built R 4.6.1   LOADS
data.table  1.18.6.1  built R 4.6.1   LOADS
base64enc   0.1-6     built R 4.6.1   LOADS
RGtk2       2.20.36   built R 4.1.0   FAILS  undefined symbol: Rf_EncodeReal (in RGtk2)
arrow       26.0.1    built R 4.6.1   LOADS
geoarrow    0.4.4     built R 4.6.1   LOADS
duckdb      2.0.0     built R 4.6.1   LOADS
```

**One correction for IT, in their favour.** The note said `arrow`, `geoarrow` and `duckdb` were installed under R 4.5.6. On disk their `DESCRIPTION` files record `Built: R 4.6.1`, and all three load under R 4.6.1. Whatever the build pipeline reported, the result in the shared library is correct and working. Nothing needs redoing for those three.

`RGtk2` is the only one of the seven still failing, and it is unchanged from 2026-11-05: still built under R 4.1.0, still missing `Rf_EncodeReal`.

#### Full sweep, against the 2026-11-05 baseline

`R/checks/77_r_lib_sweep.R`, read-only, 6 workers, 632.9 s. Log and TSV kept in `logs/`.

```
[2026-11-11 08:51:36] +   3.1s | R 4.6.1; library has 1678 package(s); 6 worker(s)
[2026-11-11 08:51:36] +   3.1s |   built under R 4.1   compiled   38  pure-R  103
[2026-11-11 08:51:36] +   3.1s |   built under R 4.5   compiled   12  pure-R    8
[2026-11-11 08:51:36] +   3.1s |   built under R 4.6   compiled  620  pure-R  897
[2026-11-11 09:02:06] + 632.9s | loaded OK 1671   BROKEN 7   (of 1678)
[2026-11-11 09:02:06] + 632.9s |   direct (own shared object fails): 2
[2026-11-11 09:02:06] + 632.9s |   via another package's shared object: 2
[2026-11-11 09:02:06] + 632.9s |   missing dependency: 2   other: 1
[2026-11-11 09:02:06] + 632.9s | ROOTS (direct failures) and the symbol each lacks:
[2026-11-11 09:02:06] + 632.9s |   pryr                   built R 4.5  undefined symbol: EXTPTR_PROT
[2026-11-11 09:02:06] + 632.9s |   RGtk2                  built R 4.1  undefined symbol: Rf_EncodeReal
[2026-11-11 09:02:06] + 632.9s | knock-on failures by root:
[2026-11-11 09:02:06] + 633.0s |   RGtk2                  2 package(s)
[2026-11-11 09:02:06] + 633.0s | undefined symbols among roots: EXTPTR_PROT=1  Rf_EncodeReal=1
```

Side by side:

| | 2026-11-05 | 2026-11-11 |
|---|---|---|
| packages | 1,653 | 1,678 |
| load OK | 1,065 | **1,671** |
| BROKEN | 588 | **7** |
| direct roots | 4 | **2** |
| built under R 4.6 | **0** | **1,517** |

1,517 of 1,678 packages are now built under R 4.6, against none a week ago. That is the whole of the fix, and it worked.

#### The seven that still fail, split as requested

**Genuine failures (4)** — these are compiled against an older R and were not rebuilt:

```
RGtk2          built 4.1  direct     undefined symbol: Rf_EncodeReal
pryr           built 4.5  direct     undefined symbol: EXTPTR_PROT
gWidgetsRGtk2  built 4.1  via:RGtk2  undefined symbol: Rf_EncodeReal
rggobi         built 4.1  via:RGtk2  undefined symbol: Rf_EncodeReal
```

`RGtk2` is the root of three of the four. It binds to GTK2, which is long out of support, and it was already irrelevant to this pipeline in the P3 report. `pryr` is a developer convenience package. **Neither blocks any Atlas work**, and neither needs rebuilding on our account.

**Absent dependencies, not failures (2)** — the package itself is fine; something it needs was never installed:

```
ANCOMBC     there is no package called 'CVXR'
mlr3proba   there is no package called 'mlr3'
```

**Version skew, not an ABI problem (1)**:

```
climateR    object '%>%' is not exported by 'namespace:sf'
```

`climateR` expects an older `sf` that re-exported the pipe. The shared `sf` is now 1.1.3 and does not. This is a package-version mismatch, not a compilation problem, and it would be fixed by updating `climateR` rather than rebuilding anything.

**None of the seven is used by this pipeline.** Everything the 0.4 chain and `hazards_upstream` 01 to 04 reference now loads from the shared library.

#### What this means for the personal library

The personal library holds 14 packages, installed on 2026-11-05 as a workaround. Comparing each against the rebuilt shared library:

```
arrow         user 26.0.0    site 26.0.1    site is newer; the personal copy now shadows it
base64enc     user 0.1-6     site 0.1-6     same
data.table    user 1.18.6.1  site 1.18.6.1  same
dplyr         user 1.2.1     site 1.2.1     same
duckdb        user 2.0.0     site 2.0.0     same
geoarrow      user 0.4.4     site 0.4.4     same
rlang         user 1.3.0     site 1.3.0     same
vctrs         user 0.7.3     site 0.7.3     same
MetBrewer     user 0.2.0     site ABSENT    still needed
miceadds      user 3.20-10   site ABSENT    still needed
paws.common   user 0.9.0     site ABSENT    still needed
paws.storage  user 0.11.0    site ABSENT    still needed
s3fs          user 0.1.7     site ABSENT    still needed
sfarrow       user 0.4.1     site ABSENT    still needed
```

Seven are now redundant, one (`arrow`) actively shadows a newer shared copy, and six are still only in the personal library. **The personal library was left intact**, as instructed, pending this verification. The tidy-up, once Pete is happy, is to delete the eight redundant or shadowing entries and ask IT to add the remaining six to the shared library; they are all on CRAN.

#### Draft reply to IT — for Pete's review, not sent

> Subject: R 4.6.1 shared-library rebuild — verified on our side, with two leftovers
>
> Thank you for the rebuild, and for the detail in your reply. We have re-tested on an ordinary user account against system R 4.6.1 and the shared site library, with our personal R library excluded so that nothing of ours could mask the result. The improvement is clear and we are no longer blocked.
>
> **Measured results.** Before the rebuild, 588 of 1,653 packages in the shared library failed to load; none had been built under R 4.6. After it, 7 of 1,678 fail and 1,517 are built under R 4.6. The packages you asked us to check — `arrow`, `geoarrow` and `duckdb` — are present and all three load. One small correction, in your favour: their `DESCRIPTION` files record `Built: R 4.6.1`, not 4.5.6, so whatever the build log reported, what is installed is correct.
>
> **The seven that still fail**, for completeness, none of which blocks our work:
> - `RGtk2`, `gWidgetsRGtk2`, `rggobi` — `RGtk2` is still built under R 4.1 and is missing the symbol `Rf_EncodeReal`; the other two fail only because they load it. `RGtk2` depends on GTK2, which is out of support, so we would suggest retiring it rather than rebuilding it.
> - `pryr` — still built under R 4.5, missing `EXTPTR_PROT`. A developer convenience package; worth a rebuild only if someone asks for it.
> - `ANCOMBC` and `mlr3proba` — not build problems. Each needs a package that is not installed: `CVXR` and `mlr3` respectively.
> - `climateR` — not a build problem either. It expects an older `sf` that re-exported the `%>%` pipe; the current `sf` 1.1.3 does not. Updating `climateR` would fix it.
>
> **Two requests, both about notice rather than action.**
> 1. Please do let us have advance notice of R upgrades, and of routine shared-library rebuilds as well. The upgrade on 2026-10-26 took our pipeline out for about ten days, and a day's warning would have let us pin our dependencies first. Freshservice notification would suit us well, thank you for offering it.
> 2. When a rebuild happens, a one-line note of which R version the library was built against would let us check compatibility in minutes rather than re-running a full sweep.
>
> **A few points from your reply we would still value an answer on.**
> - *Idle culling.* You confirmed that background jobs survive while the Hub server stays running. Could you tell us what makes the server stop — is there an idle timeout, and does an unattended long-running job count as activity? Some of our ingests run for many hours with no interactive session attached, and we need to know whether to expect them to be killed.
> - *Session sizing.* Thank you for confirming 20 CPUs and 96 GB per session with four sessions available. Our current profile gives us 8 CPUs and 32 GB. Could you tell us how to select the larger profile, or whether it needs to be enabled for the account?
> - *Remote compute access.* Is there a supported way to reach this environment other than through the browser — SSH, or a scheduled batch submission? Several of our jobs would be better run as scheduled work than in a notebook session.
> - *The slow external route.* No action needed for now. We have since measured 2.5 MB/s from our object store on the same node, roughly twice what we get elsewhere, so the slowness we reported looks specific to that one external source rather than to the network here. We will come back with a concrete trace if it recurs.
>
> Thanks again — the rebuild has unblocked the work.

---

## P5 — phase B: STOPPED at P5.1

**Which R stack P5 ran on:** the shared library **plus** the personal library, in the normal order with the personal library first. The `_env` rows in the inputs TSV record the versions that were actually used. Only `arrow` differs between the two libraries, where the personal 26.0.0 shadowed the shared 26.0.1; for every other `_env` package the two libraries now hold the same version, so the stack P5 ran on is effectively the rebuilt shared library.

#### P5.0 — readiness and pinned inputs

Branch `develop`, `git log --oneline -1` = `c3aa381`.

`71_stage_ready.R --stage 0.4.1` reported **READY**.

Pinned-input verification:

```
glw4-2020            ok=6   PASS
afr-highlands        ok=1   PASS
boundaries-gaul2024  FAIL - 1 object(s) missing or wrong
```

The boundaries failure was a size mismatch on one file, `atlas_gaul24_a0_africa.parquet`, local 31,935,988 bytes against a manifest 28,780,999. P5.0 names the remedy, so it was applied: `atlas_require("boundaries-gaul2024")`, a verified fetch, which re-fetched that one object and left the other two alone.

```
WARN  |   wrong size, re-fetching: …/atlas_gaul24_a0_africa.parquet (31,935,988 bytes, manifest 28,780,999)
INFO  |   3 object(s): 1 fetched (1 re-fetched), 2 already present, 169,268,377 bytes
INFO  |   verified: hash 1, size only 2, drift 0, no manifest 0
```

Re-verify: `ok=3`, `PASS`. **All three pinned records then verified clean**, so this was not a stop. Worth noting for the record that a local boundaries copy had drifted from the origin and nobody had spotted it; the manifest caught it.

#### P5.1 — the stop

0.4.1 was launched exactly as written, both resolutions in one background job with a `.pid`, polled with `kill -0`. **It did not reach `COMPLETE`.** res-25 failed partway through, and because the loop carries `|| exit 1`, res-05 never started.

```
[09:04:12] [0.4.1] loading FAOSTAT data (VoP, prices, production)
Error in fread(fao_econ_file_world) : 
  File 'Data/fao/Prices_E_All_Data_(Normalized).csv' does not exist or is non-readable.
Execution halted
```

**A missing `COMPLETE` is a listed stop rule, so P5 stops here and nothing was improvised.** In particular no FAOSTAT bulk fetch was attempted: that would have pulled the current FAOSTAT release and changed the very inputs phase B is comparing, turning a clean finding into a muddled one.

`grep -il install` over both run logs returned nothing. **`pacman` installed nothing**, as P4 intended.

**The finding underneath the stop.** `R/0_server_setup.R:576` defines

```r
fao_econ_file_world <- file.path(fao_dir, "Prices_E_All_Data_(Normalized).csv")
```

That file is not in `Data/fao` on this node. What is there is `Prices_E_Africa_NOFLAG.csv`, `Prices_E_Africa.csv`, the deflators and the production files — the Africa cut, not the world file. And `71_stage_ready.R --stage 0.4.1` checks `Prices_E_Africa_NOFLAG.csv`, `Production_Crops_Livestock_E_Africa_NOFLAG.csv`, `Deflators_E_All_Data_(Normalized).csv` and the boundaries parquets, but **not** the world prices file. So 71 reports READY for a stage that cannot run.

That is a gap in the readiness check rather than a problem with this node, and it is worth fixing centrally: 71 should check every file 0.4.1 actually opens. `72_crosshost_fingerprint.R:150` already lists `Prices_E_All_Data_(Normalized).csv` among the five FAOSTAT inputs it fingerprints, so the two checks already disagree about what 0.4.1 needs.

**The one value that did come out before the stop**, reported because the dispatch asks for it as a cross-host number:

```
[0.4.1] mass not conserved on GLW resample:
  poultry=1.8928  sheep=1.9041  pigs=1.7754  goats=1.9926  cattle=1.9048
```

Against the macbook's res-25 ratios of `poultry=0.9460, sheep=0.9513, pigs=0.8871, goats=0.9954, cattle=0.9520`, **every one of these is almost exactly twice the macbook's** — the five ratios of pascal to macbook are 2.0008, 2.0016, 2.0014, 2.0018 and 2.0008. A factor that constant across five independent species is not a numerical difference; it points at the denominator, meaning the global GLW source mass the warning divides by, rather than at the resampling. It is reported as a finding and left alone. It may well resolve once 0.4.1 runs to completion, but it should be looked at either way.

#### What was written, and what was not

Only one partial output exists, inside the permitted 0.4.1 output tree:

```
Data/GLW4_2020/intermediate/livestock_masks_res-25.tif   610,455 bytes
Data/GLW4_2020/processed/                                empty
```

The run log records `livestock_masks.tif exists=FALSE` before writing, so nothing was overwritten. **These res-25 intermediates are incomplete and must not be used**; the next clean run with `FORCE_OVERWRITE=1` will replace them.

Nothing was written outside the permitted places. No S3 writes. The personal R library is intact.

#### P5.2 — fingerprints

Only the inputs fingerprint could be produced, since the two output fingerprints need outputs that do not exist.

```
metadata/crosshost/inputs_041_pascal.tsv    25 rows  (12 artefact + 13 _env)
metadata/crosshost/fp_041_pascal_res-25.tsv NOT PRODUCED - run stopped
metadata/crosshost/fp_041_pascal_res-05.tsv NOT PRODUCED - run stopped
```

25 rather than the expected 26, and the missing row is exactly the missing file: `Data/fao/Prices_E_All_Data_(Normalized).csv`. The other 12 artefact rows and all 13 `_env` rows are present.

#### P5.3 — inputs compare against the macbook

```
env-diff=9  env-same=4  ONLY-A=1  same=8  SHAPE-DIFF=4
```

**Step 1, the pinned inputs: PASS.** All eight are `same` — the six GLW tifs, the highlands file and the admin0 boundaries. These are the inputs the host gate depends on, and they are byte-identical to the macbook's. The #58 manifests did their job, including catching the drifted boundaries file first.

**The five FAOSTAT CSVs all differ:**

```
SHAPE-DIFF Data/fao/Prices_E_Africa_NOFLAG.csv
ONLY-A     Data/fao/Prices_E_All_Data_(Normalized).csv
SHAPE-DIFF Data/fao/Production_Crops_Livestock_E_Africa_NOFLAG.csv
SHAPE-DIFF Data/fao/Value_of_Production_E_Africa.csv
SHAPE-DIFF Data/fao/Value_of_Production_E_All_Area_Groups.csv
```

So this node holds a **different FAOSTAT release** from the macbook's 2026-05-14 snapshot, not merely a different value here and there — the shapes differ, meaning different row counts. By the dispatch's own rule this is an input difference, not a host difference, and had 0.4.1 completed, the six `glw4-2020_vop_*` rasters and the `-prices-used.csv` files could not have matched the macbook. **Step 3 of the comparison was therefore never going to be a host test on this node as it stands.** Whoever runs phase B next should decide whether to pin FAOSTAT across hosts first; otherwise only the three GLW-derived outputs can carry the gate.

**Step 2, the host gate, could not be evaluated**, since `livestock_number_number_res-*`, `livestock_masks_res-*` and `shoat_prop_res-*` were never produced.

#### `env-diff` rows, all nine

```
arrow        mac 24.0.0                 pascal 26.0.0
data.table   mac 1.18.4                 pascal 1.18.6.1
GDAL         mac 3.8.5                  pascal 3.4.3
git          mac 3ccda81                pascal c3aa381
platform     mac aarch64-apple-darwin23 pascal x86_64-pc-linux-gnu
PROJ         mac 9.5.1                  pascal 8.2.1
R            mac 4.6.0                  pascal 4.6.1
sf           mac 1.1.2                  pascal 1.1.3
terra        mac 1.9.34                 pascal 1.9.50
```

Four `_env` rows match: `dplyr` 1.2.1, `rlang` 1.3.0, `tidyr` 1.3.2 and `vctrs` 0.7.3.

**The `terra` and `sf` numbers have moved since the dispatch was written.** P5 anticipated terra 1.8.93 and sf 1.0.24 here; IT's rebuild has taken them to **terra 1.9.50** and **sf 1.1.3**, now *ahead* of the macbook's 1.9.34 and 1.1.2 rather than behind. GDAL 3.4.3 and PROJ 8.2.1 are system libraries and did not change. The raster stack is therefore closer across hosts than phase B assumed, which makes the eventual value comparison more meaningful, not less.

#### To finish P5

One thing is needed, and it is a decision rather than a command: how to obtain `Prices_E_All_Data_(Normalized).csv` on this node without changing the other four FAOSTAT files, since those four already differ from the macbook's and fetching the bulk record would move them again. Options, for the macbook to choose between:

1. **Pin FAOSTAT.** Add the five CSVs to a `#58` manifest with hashes, as GLW and boundaries already are, and have every host fetch that pinned set. This makes the VoP outputs a real host gate instead of an input comparison, and it is the only option that makes phase B say what it was built to say.
2. **Fetch the current bulk release here** and accept that all five FAOSTAT inputs differ from the macbook's, so only the three GLW-derived outputs carry the gate.
3. **Copy the macbook's five CSVs** to this node, which would make the FAOSTAT inputs identical, at the cost of being a manual step outside the acquire layer.

Option 1 is the one worth the effort. Say which you want and P5 can be re-run; the 0.4.1 run itself is 2 to 3 minutes.

Separately, `71_stage_ready.R` should be taught about the world prices file so that READY means ready. That is a one-line change in the 0.4.1 stage definition, and it is not PASCAL's to make.

#### Logs kept

`logs/r_lib_sweep_20261111_p6.{log,tsv,pid}`, `logs/phaseB_041_20261111_090330.{log,pid}`, `logs/phaseB_041_0.25_20261111_090330.log`, `logs/p5_require_boundaries_20261111.log`.

---

### MACBOOK 2026-11-09 — P6: PASCAL shared R library retest and support reply (#58)

**Order (added 2026-11-11, formatting fix only):** run this P6 block **first**, then P5 below. P5 is unchanged.
Record which R stack P5 then runs on: the shared library only, or the shared library plus the personal library.
Its `_env/*` rows capture the versions either way.

IT reports recompilation and says arrow, geoarrow and duckdb were installed under R 4.5.6. Please verify on PASCAL using an ordinary account, system R 4.6.1 and the shared site library. Exclude the personal R library for this test and record the active version and library paths (redact personal paths). If 4.6.1 is unavailable, report which version is active and stop there.

Run the existing read-only R/checks/77_r_lib_sweep.R, compare with 588 failures among 1,653 packages, and explicitly test rlang, data.table, base64enc, RGtk2, arrow, geoarrow and duckdb. Record failures and absent packages separately, with concise errors. Leave the personal workaround intact pending verification.

IT also offered Freshservice notification after upgrades, said bash jobs should survive while the Hub server stays running, confirmed 20 CPUs / 96 GB per session with four sessions, reported no proxy or firewall limit, and could not explain the slow JRC route. Prepare a short editable ticket reply for Pete's review. Thank IT, summarize measured results, ask for advance notice before upgrades and routine shared-library rebuilds, and cover remaining unanswered points (including idle culling and remote compute access) where relevant. Draft only. Add results in a RESPONSE block.


### MACBOOK 2026-11-06 — P5 (phase B, PASCAL half): run R/0.4.1 at both resolutions, fingerprint inputs and outputs, value-compare against the macbook. **Small profile is fine.**

**Why.** P4 made the 0.4 chain load here, so phase B can start. Phase B turns the 2026-09-23 cross-host gate, which
compared shape only and only inputs, into a **value** gate on real outputs. R/0.4.1 is unchanged since CGlabs last ran
it (`931e5a7`, 2026-11-02, both resolutions, for #54), and its run takes 1-3 min. `72_crosshost_fingerprint.R` gained
three things:
- `--stage 0.4.1` (only what 0.4.1 writes, for one grid);
- `--stage 0.4.1-inputs` (md5 of every input 0.4.1 reads);
- `--compare`, the gate: shape exact, `n_nonNA` exact, sums within `--rtol 1e-6`. Version rows (`_env/*`) are reported
  but never gated.

The macbook ran the same code and committed its reference fingerprints to `metadata/crosshost/`. The CGlabs half
comes later; CGlabs is busy.

**What the gate can and cannot say.** 0.4.1 has two kinds of output:
- **Built from GLW, highlands and boundaries only:** `livestock_number_number`, `livestock_masks`, `shoat_prop`. Those
  inputs are hash-pinned to their origin by the #58 manifests, so they are identical on every host by construction.
  These outputs are the host gate. They must be `same` or `within-tol`.
- **Built from FAOSTAT as well:** the six `glw4-2020_vop_*` rasters and the `-prices-used.csv` files. FAOSTAT is a
  rolling release (its archives change in place); the macbook's CSVs date from 2026-05-14. If your FAOSTAT md5s match
  the macbook's, these outputs must match too. If they differ, a VoP difference is an **input** difference, not a host
  one: report it, and it is not a failure.

**Rules.**
- Writes go only to your own 0.4.1 output directories (`<working_dir>/Data/GLW4_2020/{processed,intermediate}`),
  `logs/`, and three new files in `metadata/crosshost/`.
- Installs: none. If the 0.4.1 log shows `pacman` installing anything, stop. P4 left nothing for it to install.
- Background jobs with `nohup` and a `.pid` file, polled with `kill -0` or the log tail, never `wait`.
- No S3 writes.
- No usernames, hostnames or IPs in the RESPONSE or in the TSVs. The TSVs hold paths relative to `<working_dir>`
  only; check them by eye before committing.

#### P5.0 — pull, readiness, pinned inputs
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD
git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD      # develop
Rscript R/checks/71_stage_ready.R --stage 0.4.1 2>&1 | tail -5
for id in glw4-2020 afr-highlands boundaries-gaul2024; do Rscript R/checks/75_catalogue_manifest.R --verify $id --hash 2>&1 | tail -2; done
```
**What to expect:**
- 71 reports READY;
- all three `--verify` lines end `PASS`. A `missing` or `size` row there means a local copy differs from the origin.
  Fix it with `Rscript -e 'source("R/00_paths.R"); source("R/00_acquire.R"); atlas_require("<id>")'`, which is a
  verified fetch, then verify again.

If 71 is NOT READY, run `atlas_require_stage("0.4.1")` the same way (verified fetches only) and say what it fetched.
If that includes `faostat-bulk`, you now hold the **current** FAOSTAT release. That is fine; just say so.

#### P5.1 — run 0.4.1, res-25 then res-05 (about 2 + 3 min)
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup bash -c 'for RES in 0.25 0.05; do EXPOSURE_RES=$RES FORCE_OVERWRITE=1 Rscript -e "source(\"$PWD/R/0_server_setup.R\"); source(\"$PWD/R/0.4.1_create_livestock_exposure.R\")" > logs/phaseB_041_${RES}_'$STAMP'.log 2>&1 || exit 1; done' > logs/phaseB_041_$STAMP.log 2>&1 &
echo $! > logs/phaseB_041_$STAMP.pid
# poll: kill -0 $(cat logs/phaseB_041_$STAMP.pid) 2>/dev/null && echo running || grep -hE "COMPLETE|mass not conserved|rror" logs/phaseB_041_*_$STAMP.log
grep -il "install" logs/phaseB_041_*_$STAMP.log      # expect no output
```
**What to expect:**
- one `COMPLETE` line per resolution, and no `Error`;
- **one `mass not conserved on GLW resample` warning per resolution. It is expected:** the warning compares global GLW
  mass with what lands on the grid, and the res-25 grid is a 50°S-50°N band. Report its ratios verbatim; they are a
  cross-host value too. For res-25 the macbook got
  `poultry=0.9460, sheep=0.9513, pigs=0.8871, goats=0.9954, cattle=0.9520`. For res-05, an Africa-only grid, it got
  `poultry=0.1579, sheep=0.4337, pigs=0.0588, goats=0.4799, cattle=0.2606`.

#### P5.2 — fingerprint and commit
```bash
mkdir -p metadata/crosshost
Rscript R/checks/72_crosshost_fingerprint.R --stage 0.4.1-inputs --out metadata/crosshost/inputs_041_pascal.tsv
Rscript R/checks/72_crosshost_fingerprint.R --stage 0.4.1 --res res-25 --out metadata/crosshost/fp_041_pascal_res-25.tsv
Rscript R/checks/72_crosshost_fingerprint.R --stage 0.4.1 --res res-05 --out metadata/crosshost/fp_041_pascal_res-05.tsv
```
Expect 15 artefact rows + 13 `_env` rows per output TSV, and 13 + 13 for the inputs. If the counts differ, list what
is missing.

#### P5.3 — compare against the macbook
```bash
C="Rscript R/checks/72_crosshost_fingerprint.R --compare"; M=metadata/crosshost
$C $M/inputs_041_mac.tsv       $M/inputs_041_pascal.tsv       2>&1 | cut -c31-
$C $M/fp_041_mac_res-25.tsv    $M/fp_041_pascal_res-25.tsv    2>&1 | cut -c31-
$C $M/fp_041_mac_res-05.tsv    $M/fp_041_pascal_res-05.tsv    2>&1 | cut -c31-
```
**Read the result in this order:**
1. **Inputs.** The 6 GLW tifs, the highlands file and the admin0 boundaries must be `same`. These are the pinned
   inputs, and anything else is a stop. For the 5 FAOSTAT CSVs, report `same` or `VALUE-DIFF`. That decides step 3.
2. **The host gate.** `livestock_number_number_res-*`, `livestock_masks_res-*` and `shoat_prop_res-*` must be `same`
   or `within-tol` at both resolutions. Anything else is a **finding: report it, don't fix it.** Give the
   `max_rel_diff` and the `_env` differences printed beside it.
3. **VoP outputs.** If FAOSTAT was `same` in step 1, these must also be `same` or `within-tol`. If it was
   `VALUE-DIFF`, list their statuses and largest relative differences as input-driven, not as a failure.

Also report:
- every `env-diff` line, i.e. which library versions differ from the macbook. The macbook runs terra 1.9.34 with
  GDAL 3.8.5 on arm64; you run terra 1.8.93 with GDAL 3.4.3. A sub-tolerance difference is therefore plausible and
  fine; a larger one is the finding phase B exists to make.
- the two `mass not conserved` lines, set against the macbook's res-25 ratios above.

**Stop and describe** on any of:
- not on `develop`;
- a pinned input that will not verify;
- `pacman` installing anything;
- a missing `COMPLETE`;
- any write outside the places listed under Rules.

A host-gate difference is a **finding** to report, not a stop.

**To answer:**
1. Prepend one `### RESPONSE — pascal` (P5) to this file.
2. Run the IP grep over the response **and** the three TSVs.
3. Commit this file plus `metadata/crosshost/{inputs_041_pascal,fp_041_pascal_res-25,fp_041_pascal_res-05}.tsv`, and
   nothing else.
4. Push, then verify it landed.
5. End with a delivery line.

### RESPONSE — pascal 2026-11-05 (3) — P4: three install rounds, all binary, **`0.4-chain` BROKEN 0 MISSING 0** and **BROKEN 0 across all 70 packages**. Phase B is unblocked.

Every gate passes, and the result is better than the dispatch allowed for: the final audit has **BROKEN 0** in all three scopes, not only in `0.4-chain`. `both` has MISSING 0 as well. The nine remaining MISSING rows are all `upstream`-only.

No stop rule fired. Nothing was installed into the site or system library, nothing ran as root, and `R/0_server_setup.R` was not sourced.

#### P4.0 — before-state

Branch `develop`, `git log --oneline -1` = `bde991c`.

```
user lib first: TRUE
data.table  1.18.6.1  R 4.6.1; x86_64-pc-linux-gnu; 2026-10-28 12:58:17 UTC; unix
```

One package in the user library, as expected.

#### P4.1 — round 1

**Binary, not source.** Posit Package Manager serves R 4.6 jammy binaries for every package requested, so the slow `arrow`-from-source path never came up.

```
binary: 11    source: 0
```

Eleven, because `install.packages` pulled in one dependency that was not present anywhere, `paws.common`. All ten requested packages installed:

```
rlang          1.3.0
base64enc      0.1.6
arrow          26.0.0
geoarrow       0.4.4
duckdb         2.0.0
sfarrow        0.4.1
MetBrewer      0.2.0
miceadds       3.20.10
s3fs           0.1.7
paws.storage   0.11.0
```

**Round 1 wall time: about 20 s.** No errors, no warnings, nothing unavailable.

#### P4.2 — second and third layers

The dispatch allowed at most two extra rounds. Both were needed, and the third round cleared everything.

**Audit after round 1** — `OK 46   BROKEN 15   MISSING 9   (of 70)`, `0.4-chain OK 23  BROKEN 5  MISSING 0`. Every MISSING row in the 0.4 chain was gone, but a second-layer root appeared:

```
      6 vctrs
```

**Round 2: `P4_PKGS=vctrs`.** One binary, about 7 s, `vctrs 0.7.3`.

**Audit after round 2** — `OK 57   BROKEN 4   MISSING 9   (of 70)`, `0.4-chain OK 27  BROKEN 1  MISSING 0`. A third-layer root:

```
      2 dplyr
```

Four packages were still BROKEN and all four traced to that one root. `dplyr` and `tidyr` named `dplyr.so` directly; `tidyverse` failed in `.onAttach`; and `miceadds` reported `package 'mice' could not be loaded`, which is not a missing dependency. Loading `mice` on its own gave:

```
unable to load shared object '/usr/local/lib/R/site-library/dplyr/libs/dplyr.so':
  /usr/local/lib/R/site-library/dplyr/libs/dplyr.so: undefined symbol: R_shallow_duplicate_attr
```

So the same root, one level further down, and a **third symbol**: `R_shallow_duplicate_attr`, alongside the `SETLENGTH` and `Rf_EncodeReal` found in P3.

**Round 3: `P4_PKGS=dplyr`.** One binary, about 7 s, `dplyr 1.2.1`.

Summary of the three rounds:

| round | packages installed | binary / source | wall |
|---|---|---|---|
| 1 | `rlang`, `base64enc`, `arrow`, `geoarrow`, `duckdb`, `sfarrow`, `MetBrewer`, `miceadds`, `s3fs`, `paws.storage`, plus `paws.common` | 11 / 0 | about 20 s |
| 2 | `vctrs` | 1 / 0 | about 7 s |
| 3 | `dplyr` | 1 / 0 | about 7 s |

**This is the layering P3 predicted.** P3 reported four direct roots and said the figure was a lower bound, because the classifier names only the first shared object that fails. Two more roots surfaced here once `rlang` loaded: `vctrs`, then `dplyr`. The same thing will happen to IT, so the node-wide fix is a rebuild-and-re-sweep loop, not a one-shot list of four.

#### Final audit, verbatim

`logs/r_env_audit_p4_20261105_151730.{tsv,log}`:

```
[2026-11-05 15:16:51] +   1.1s | scope: 76 file(s); 70 distinct package(s)
[2026-11-05 15:17:53] +  62.8s | OK 61   BROKEN 0   MISSING 9   (of 70)
[2026-11-05 15:17:53] +  62.8s |   0.4-chain OK 28  BROKEN 0  MISSING 0
[2026-11-05 15:17:53] +  62.8s |   both      OK 13  BROKEN 0  MISSING 0
[2026-11-05 15:17:53] +  62.8s |   upstream  OK 20  BROKEN 0  MISSING 9
```

No BROKEN row anywhere, so no site-library root is still failing for anything the pipeline touches. The nine MISSING rows are all `upstream`, all packages outside the 0.4 chain, and none was in P4's list:

```
aws.s3           upstream   1:upload_AWS.R
climate4R.UDG    upstream   1:download_rawCMIP6.R
downscaleR       upstream   1:download_rawCMIP6.R
inborutils       upstream   1:download_cropCalendar.R
loadeR           upstream   1:download_rawCMIP6.R
transformeR      upstream   1:download_rawCMIP6.R
tsbox            upstream   2:preprocess_nex-gddp-cmip6_daily_data_v2_0.R;preprocess_nex-gddp-cmip6_daily_data.R
visualizeR       upstream   1:download_rawCMIP6.R
wbkernel         upstream   2:fast_calc_NDWS.R;fast_calc_waterbalance.R
```

Six of the nine belong to `download_rawCMIP6.R` and `download_cropCalendar.R`, outside 01 to 04. **`wbkernel` is the one that matters for phase C**, needed by `fast_calc_NDWS.R` and `fast_calc_waterbalance.R`. It is not on CRAN, so it needs its own decision rather than another `install.packages` line. `tsbox` and `aws.s3` are CRAN packages, but neither is in 01 to 04.

#### P4.3 — gates

All twelve smoke-test packages load:

```
rlang       OK 1.3.0
dplyr       OK 1.2.1
tidyr       OK 1.3.2
ggplot2     OK 4.0.1
purrr       OK 1.2.1
wbstats     OK 1.1
arrow       OK 26.0.0
geoarrow    OK 0.4.4
duckdb      OK 2.0.0
data.table  OK 1.18.6.1
terra       OK 1.8.93
sf          OK 1.0.24
```

```
arrow parquet: TRUE
```

```
14 packages in user lib
146M	~/R/x86_64-pc-linux-gnu-library/4.6
```

Worth noting which library each gate package came from: only `rlang`, `dplyr`, `arrow`, `geoarrow`, `duckdb` and `data.table` are user-library copies. `tidyr`, `ggplot2`, `purrr`, `wbstats`, `terra` and `sf` are the original site builds, now loading because the roots beneath them are fixed. Fourteen user-library packages were enough to unbreak fifty-odd site ones.

#### The user library, for the day IT rebuilds the site library

Everything now in `R_LIBS_USER`. All fourteen are built under R 4.6.0 or 4.6.1, so all are ABI-compatible with the running 4.6.1. **When the site library is rebuilt, this whole directory should be deleted**, so that the node goes back to one copy of each package and these per-user versions stop shadowing the site ones.

```
package        version    built    build date
arrow          26.0.0     R 4.6.0  2026-10-25
base64enc      0.1-6      R 4.6.0  2026-08-12
data.table     1.18.6.1   R 4.6.1  2026-10-28
dplyr          1.2.1      R 4.6.0  2026-08-12
duckdb         2.0.0      R 4.6.0  2026-10-24
geoarrow       0.4.4      R 4.6.0  2026-09-17
MetBrewer      0.2.0      R 4.6.0  2026-04-24
miceadds       3.20-10    R 4.6.0  2026-10-18
paws.common    0.9.0      R 4.6.0  2026-10-06
paws.storage   0.11.0     R 4.6.0  2026-10-08
rlang          1.3.0      R 4.6.0  2026-08-12
s3fs           0.1.7      R 4.6.0  2026-04-24
sfarrow        0.4.1      R 4.6.0  2026-04-24
vctrs          0.7.3      R 4.6.0  2026-08-12
```

Two version changes are worth flagging before phase B runs, because they are not like-for-like replacements of the site builds:

- **`dplyr` 1.2.1 in the user library against 1.1.4 in the site library.** A minor-version step, picked up because P3M's `latest` snapshot was used rather than a pin. Phase B compares values across hosts, so if cglabs or the macbook is on `dplyr` 1.1.4, that difference is now in the comparison. Flagging it rather than acting on it; say the word and the pin can be moved.
- **`rlang` 1.3.0 against the site library's 1.1.7**, and **`vctrs` 0.7.3 against 0.7.1**, for the same reason. These are lower-level and less likely to move a number, but they are the same kind of difference.

Everything else either did not exist here before, or matches what the site library held.

#### Notes

- **One write fell outside `R_LIBS_USER` and `logs/`**, and it is inherent to `install.packages`: R staged each downloaded tarball under its own session temporary directory before unpacking. Those directories are removed when the session ends and hold nothing of value. Nothing else outside the two permitted places was touched, and no S3 writes were made. Flagging it for completeness rather than as a problem.
- **No prompt to install as root or into the site library appeared at any point.** The `lib =` argument was explicit on every call, and `R_LIBS_USER` is first in `.libPaths()`.
- **Phase B is unblocked.** R/0.4.1 and the rest of the 0.4 chain now have every package they reference, and the 0.4 chain has no BROKEN and no MISSING row.
- **Phase C still needs `wbkernel`**, which was deliberately outside P4.
- **The IT ticket should gain one line from this**: the roots do not surface all at once. Four were visible in P3; fixing them exposed `vctrs`, and fixing that exposed `dplyr`, with a third missing symbol, `R_shallow_duplicate_attr`. The site-library rebuild needs the sweep re-run after each pass until it comes back clean.

#### Logs kept

`logs/p4_install.R`, `logs/p4_install_r1_20261105_151250.{log,pid}`, `logs/p4_install_r2_20261105_151500.{log,pid}`, `logs/p4_install_r3_20261105_151700.{log,pid}`, and the three audits `logs/r_env_audit_p4_20261105_{151330,151530,151730}.{tsv,log}`.

---

### MACBOOK 2026-11-05 — P4: per-user rebuild so the 0.4 chain loads (installs, Pete's GO). **Small profile is fine.**

**Why.** Your P3 sweep shrank the problem to four compiled roots. `data.table` is already rebuilt in your user
library, `RGtk2` is irrelevant, and the IT ticket is going in for the node-wide fix. Phase B (R/0.4.1 on this node)
should not wait for IT, so P4 does the same per-user repair you did for `data.table` and goes no further:
- rebuild `rlang` and `base64enc`;
- install the eight packages the 0.4 chain needs that are MISSING here: `arrow`, `geoarrow`, `duckdb`, `sfarrow`,
  `MetBrewer`, `miceadds`, `s3fs`, `paws.storage`;
- go round again for any second-layer roots that only show once `rlang` loads.

`wbkernel` (phase C) is **not** part of P4.

**Rules.**
- **Install only** into `R_LIBS_USER` (`~/R/x86_64-pc-linux-gnu-library/4.6`). Never into the site or system library,
  and never as root.
- Do not source `R/0_server_setup.R`.
- Writes go only to `R_LIBS_USER` and `logs/`.
- Installs run in the background with `nohup` and a `.pid` file, polled with `kill -0` or the log tail, never `wait`.
- No usernames, hostnames or IPs in the RESPONSE.

#### P4.0 — pull, and record the before-state
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD
git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD    # develop
Rscript -e 'l <- Sys.getenv("R_LIBS_USER"); cat("user lib first:", normalizePath(.libPaths()[1]) == normalizePath(l), "\n"); print(installed.packages(lib.loc = l)[, c("Version", "Built")])'
```
Expect `user lib first: TRUE` and one row, `data.table`.

#### P4.1 — round 1 (Posit Package Manager binaries for Ubuntu 22.04)
```bash
cat > logs/p4_install.R <<'RS'
lib <- Sys.getenv("R_LIBS_USER")
options(repos = c(P3M = "https://packagemanager.posit.co/cran/__linux__/jammy/latest"),
        HTTPUserAgent = sprintf("R/%s R (%s)", getRversion(),
          paste(getRversion(), R.version$platform, R.version$arch, R.version$os)))
pk <- strsplit(Sys.getenv("P4_PKGS"), ",")[[1]]
cat("installing into user lib:", paste(pk, collapse = " "), "\n")
install.packages(pk, lib = lib, Ncpus = 4)
for (p in pk) cat(sprintf("%-14s %s\n", p, tryCatch(as.character(packageVersion(p, lib.loc = lib)), error = function(e) "NOT INSTALLED")))
RS
STAMP=$(date +%Y%m%d_%H%M%S)
P4_PKGS=rlang,base64enc,arrow,geoarrow,duckdb,sfarrow,MetBrewer,miceadds,s3fs,paws.storage \
  nohup Rscript logs/p4_install.R > logs/p4_install_r1_$STAMP.log 2>&1 &
echo $! > logs/p4_install_r1_$STAMP.pid
# poll: kill -0 $(cat logs/p4_install_r1_$STAMP.pid) 2>/dev/null && echo running || tail -15 logs/p4_install_r1_$STAMP.log
```
**Binary or source?** Count the two kinds of install line in the log:
```bash
grep -c "installing \*binary\* package" logs/p4_install_r1_$STAMP.log
grep -c "installing \*source\* package" logs/p4_install_r1_$STAMP.log
```
If Posit Package Manager has no R 4.6 binaries for this platform, the install falls back to source. That is
allowed, but slower, and `arrow` from source can take 30-60 min. Report which way it went.

`install.packages` pulls in any dependency that is **not installed**. It does **not** rebuild dependencies that are
installed but broken in the site library. P4.2 covers those.

#### P4.2 — second layer, at most two more rounds
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
Rscript R/checks/76_r_env_audit.R --out logs/r_env_audit_p4_$STAMP.tsv > logs/r_env_audit_p4_$STAMP.log 2>&1
grep -E "OK [0-9]+ +BROKEN" logs/r_env_audit_p4_$STAMP.log
# roots still failing: the package whose shared object fails, taken from each BROKEN row's error
awk -F'\t' '$2=="BROKEN"' logs/r_env_audit_p4_$STAMP.tsv | grep -oE "site-library/[A-Za-z0-9.]+/libs" | cut -d/ -f2 | sort | uniq -c
```
The audit run takes a little over a minute; if your shell kills foreground commands sooner, run it with `nohup`.

If any roots are listed, install exactly those with the same script: `P4_PKGS=<root1>,<root2> nohup Rscript logs/p4_install.R …`, with
log `p4_install_r2_<STAMP>`. Then re-run the audit. At most **two** extra rounds. If BROKEN rows still name a
site-library root after round 3, stop and report them.

Packages that stay BROKEN through a **missing dependency** go into the report. Do not chase them beyond the list
above.

#### P4.3 — gates
```bash
Rscript -e 'for (p in c("rlang","dplyr","tidyr","ggplot2","purrr","wbstats","arrow","geoarrow","duckdb","data.table","terra","sf")) cat(sprintf("%-11s %s\n", p, tryCatch({suppressPackageStartupMessages(library(p, character.only = TRUE)); paste("OK", packageVersion(p))}, error = function(e) "FAIL")))'
Rscript -e 'cat("arrow parquet:", arrow::arrow_info()$capabilities[["parquet"]], "\n")'
Rscript -e 'l <- Sys.getenv("R_LIBS_USER"); ip <- installed.packages(lib.loc = l); cat(nrow(ip), "packages in user lib\n")'
du -sh ~/R/x86_64-pc-linux-gnu-library/4.6
```
**What to expect** (invariants):
- the final 76 audit shows **`0.4-chain` BROKEN 0 and MISSING 0**;
- `both` shows BROKEN 0. Its only MISSING row may be `wbkernel`, which is phase C and not P4's job;
- all twelve packages in the smoke test print `OK`;
- `arrow parquet: TRUE`.

`upstream`-only rows can stay BROKEN or MISSING; report the counts.

**Report:**
- binary vs source, with the install wall time per round;
- the root list and install line for each round of P4.2;
- the final 76 summary lines verbatim;
- the P4.3 output;
- the user-library package count and size;
- a `package, version, built` list of everything now in the user library, in a fenced block. When IT rebuilds the
  site library, that list says what to delete.

**Stop and describe** on any of:
- not on `develop`;
- any write outside `R_LIBS_USER` or `logs/`;
- any prompt to install as root or into the site library;
- roots still failing after round 3;
- a 0.4-chain gate failing for any reason other than a listed missing dependency.

**To answer:**
1. Prepend one `### RESPONSE — pascal` (P4) to this file.
2. Run the IP grep.
3. Commit only this file, then push.
4. Verify it landed.
5. End with a delivery line.

Keep `logs/p4_*`.

### RESPONSE — pascal 2026-11-05 (2) — P3: 1,653 packages swept, **588 BROKEN, but only 4 direct roots**. R was upgraded on 2026-10-26.

The ticket is smaller than the breakage looks. 588 of 1,653 site-library packages fail to load for a plain account, and 584 of those 588 fail only because one of **four** compiled packages fails underneath them. Nothing is a missing dependency, and nothing is unexplained.

No stop rule fired. Read-only throughout: nothing installed, `R/0_server_setup.R` not sourced, and `logs/` was the only thing written.

#### P3.1 — pull

Branch `develop`, `git log --oneline -1` = `063feaa` (the commit that added the P3 block).

#### P3.2 — site-library sweep

`R/checks/77_r_lib_sweep.R --lib /usr/local/lib/R/site-library --workers 6`, run with `nohup` and a `.pid`, polled with `kill -0`. 581.9 s wall, about 9 min 42 s. Log `logs/r_lib_sweep_20261105_141328.log`, TSV `logs/r_lib_sweep_20261105_141328.tsv` (1,653 rows plus header), both kept.

The log, from the `R 4.6.1; library has …` line to the end, verbatim:

```
[2026-11-05 14:14:05] +  36.0s | R 4.6.1; library has 1653 package(s); 6 worker(s)
[2026-11-05 14:14:05] +  36.0s |   built under R 4.1   compiled   64  pure-R  253
[2026-11-05 14:14:05] +  36.0s |   built under R 4.5   compiled  585  pure-R  751
[2026-11-05 14:23:11] + 581.8s | loaded OK 1065   BROKEN 588   (of 1653)
[2026-11-05 14:23:11] + 581.8s |   direct (own shared object fails): 4
[2026-11-05 14:23:11] + 581.8s |   via another package's shared object: 584
[2026-11-05 14:23:11] + 581.8s |   missing dependency: 0   other: 0
[2026-11-05 14:23:11] + 581.8s | ROOTS (direct failures) and the symbol each lacks:
[2026-11-05 14:23:11] + 581.8s |   base64enc              built R 4.1  undefined symbol: SETLENGTH
[2026-11-05 14:23:11] + 581.8s |   data.table             built R 4.5  undefined symbol: SETLENGTH
[2026-11-05 14:23:11] + 581.8s |   RGtk2                  built R 4.1  undefined symbol: Rf_EncodeReal
[2026-11-05 14:23:11] + 581.8s |   rlang                  built R 4.5  undefined symbol: SETLENGTH
[2026-11-05 14:23:11] + 581.9s | knock-on failures by root:
[2026-11-05 14:23:11] + 581.9s |   rlang                  503 package(s)
[2026-11-05 14:23:11] + 581.9s |   data.table             65 package(s)
[2026-11-05 14:23:11] + 581.9s |   base64enc              14 package(s)
[2026-11-05 14:23:11] + 581.9s |   RGtk2                  2 package(s)
[2026-11-05 14:23:11] + 581.9s | undefined symbols among roots: Rf_EncodeReal=1  SETLENGTH=3
[2026-11-05 14:23:11] + 581.9s | wrote logs/r_lib_sweep_20261105_141328.tsv
```

All three expectations from the dispatch hold. The library has 1,653 packages, close to the 1,650 predicted. `rlang` and `data.table` are both `direct` roots with `SETLENGTH`. `terra`, `sf`, `exactextractr` and `stringi` all load, each built under R 4.5:

```
exactextractr	4.5	yes	OK
sf	4.5	yes	OK
stringi	4.5	yes	OK
terra	4.5	yes	OK
```

Four points for the ticket.

**1. There are four direct roots, and two symbols between them.** `SETLENGTH` breaks `rlang`, `data.table` and `base64enc`; `Rf_EncodeReal` breaks `RGtk2`. Both are symbols R 4.6 no longer exports in the form these builds were compiled against. One root, `rlang`, accounts for 503 of the 584 knock-on failures on its own.

**2. Nothing is a missing dependency, and nothing is unclassified.** `missing dependency: 0   other: 0`. Every one of the 588 failures is the 4.5.1 to 4.6.1 ABI break and nothing else. There is no second, unrelated problem hiding in the count.

**3. Not one package in the library was built under R 4.6.** The tally is 1,336 built under R 4.5 and 317 under R 4.1; the running R is 4.6.1. The 1,065 that load do so because they do not touch the removed symbols, not because they are current. That is the real state of the library: it is entirely a 4.5-and-earlier library being run by 4.6.1.

**4. The figure of 4 roots is a lower bound, and the ticket should say so.** The classifier reports the first shared object that fails. A package whose own `.so` is also broken, but which loads `rlang` before reaching it, is counted as `via:rlang`. 158 of the 588 broken packages need compilation themselves (142 built under R 4.5, 16 under R 4.1), so some of them may turn out to be roots once `rlang` is fixed. The honest ask is: rebuild these four first, then re-run this sweep, which takes ten minutes and will show whether a second layer exists.

Full status tally, as `built / compiled / status`:

```
    443  4.5  yes  OK
    417  4.5  no   OK
    334  4.5  no   BROKEN
    157  4.1  no   OK
    142  4.5  yes  BROKEN
     96  4.1  no   BROKEN
     48  4.1  yes  OK
     16  4.1  yes  BROKEN
```

430 of the 588 broken packages are pure R. They hold no compiled code of their own and would load the moment the four roots are rebuilt.

#### P3.3 — when R was upgraded

`/var/log/dpkg.log*` is readable by any account on this node, so both commands returned an answer.

```
2026-10-26 19:18:26 upgrade r-base-core:amd64 4.5.1-1.2204.0 4.6.1-6.2204.0
```

That is the only `upgrade` or `install` line for `r-base-core` in the retained dpkg logs. The site library was not touched at the same time, which is the whole cause.

```
drwxr-xr-x 7 root root 4096 2026-10-26 19:18 /usr/lib/R/library/base
```

The base library directory carries the same minute, so it confirms the dpkg entry rather than adding a date of its own.

**The upgrade was ten days ago, on 2026-10-26.** It lines up with the first `SETLENGTH` failures seen in the FPAR thread, and it means nothing on this node has run against a consistent R since then.

#### What this means for the IT ask

The ticket can now be specific and small. The request is not "rebuild the site library"; it is:

- rebuild four compiled packages against R 4.6.1 — `rlang`, `data.table`, `base64enc`, `RGtk2` — which should clear about 584 knock-on failures;
- then re-run `R/checks/77_r_lib_sweep.R` to confirm, and to expose any second layer of roots that the first four are currently masking;
- and, separately, decide whether the whole library, all 1,653 packages built under R 4.1 or R 4.5, should be rebuilt as a batch, since not one of them has been rebuilt for 4.6.

The per-user workaround stays available but does not scale: it was enough to put `data.table` 1.18.6.1 into a per-user library for the FPAR work, but doing the same for every account on this node is not a fix.

#### Logs kept

`logs/r_lib_sweep_20261105_141328.log`, `logs/r_lib_sweep_20261105_141328.tsv`, `logs/r_lib_sweep_20261105_141328.pid`.

---

### MACBOOK 2026-11-05 — P3: whole site-library sweep before the IT ticket (read-only). **Small profile is fine.**

**Why.** Thank you for P0-P2. MapSPAM is fixed, and the new acquire layer has passed on its first node run. Before Pete
asks IT for a site-library rebuild, the ticket has to say how much is actually broken **for every account**, not
just for the 70 packages the pipeline uses. P1 tested those 70, with the per-user library first. P3 tests every
package in the site library as a plain account sees it.

**Rules.** Same as P0-P2:
- read-only;
- install nothing;
- do not source `R/0_server_setup.R`;
- writes go only to `logs/`;
- background jobs with `nohup` and `.pid`, polled with `kill -0` or the log tail, never `wait`;
- no usernames, hostnames or IPs in the RESPONSE.

#### P3.1 — pull
```bash
cd <repo> && git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD   # develop
```

#### P3.2 — sweep the site library (a few minutes at 6 workers)
`R/checks/77_r_lib_sweep.R` loads every package in the library, each in a fresh
`Rscript --no-environ --no-init-file` with the per-user library hidden, so your `data.table` rebuild cannot mask
the broken site build. It classifies each failure as:
- `direct`: the package's own `.so` fails;
- `via:<pkg>`: another package's `.so` fails while loading it;
- `dep`: a dependency is missing;
- `other`: anything else.

It also tallies the library by the R version each package was built under.
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup Rscript R/checks/77_r_lib_sweep.R --lib /usr/local/lib/R/site-library --workers 6 \
  --out logs/r_lib_sweep_$STAMP.tsv > logs/r_lib_sweep_$STAMP.log 2>&1 &
echo $! > logs/r_lib_sweep_$STAMP.pid
# poll: kill -0 $(cat logs/r_lib_sweep_$STAMP.pid) 2>/dev/null && echo running || tail -40 logs/r_lib_sweep_$STAMP.log
```
**What to expect:**
- about 1,650 packages, nearly all built under R 4.5;
- `rlang` and `data.table` among the `direct` roots, with `SETLENGTH`;
- `terra`, `sf`, `exactextractr` and `stringi` load.

The number to find out is how many **direct** roots there are, and which symbols they lack.

**Report:**
- the log from the `R 4.6.1; library has …` line to the end, verbatim. It holds the built-under tally, the counts,
  every direct root with its symbol, the knock-on counts per root, and the dep/other rows;
- not the full TSV. Keep it in `logs/`.

#### P3.3 — when R was upgraded (read-only)
```bash
zgrep -h -E " (upgrade|install) r-base-core" /var/log/dpkg.log* 2>/dev/null | sort | tail -5
ls -ld --time-style=long-iso /usr/lib/R/library/base
```
Report both outputs, with versions and dates. If `/var/log/dpkg.log*` is not readable, say so. That is an answer,
not a failure.

**Stop and describe** on any of:
- not on `develop`;
- the sweep dying before its summary;
- anything written outside `logs/`.

**To answer:**
1. Prepend one `### RESPONSE — pascal` (P3) to this file.
2. Run the IP grep.
3. Commit only this file, then push.
4. Verify it landed.
5. End with a delivery line.

### RESPONSE — pascal 2026-11-05 — P0-P2 done. P1 audit: OK 37 / BROKEN 16 / MISSING 17. P2 re-fetch: **PASS, ok=48**, both truncated tiles now match the reference.

All of P0, P1 and P2 ran to completion. No stop rule fired. Nothing was installed, `R/0_server_setup.R` was not sourced, and every write landed inside the three permitted places.

---

#### P0 — pull, identity, limits

- Branch: `develop`.
- `git log --oneline -1` at the time of the run: `6765459` (this file's MACBOOK block was present).
- Limits, read from the cgroup v2 path for this process: `cpu.max` = `800000 100000` (8 CPUs), `memory.max` = `34359738368` (32 GiB). This is the Small profile. Note for anyone reading the logs later: `nproc` on this node reports 80 and is always wrong, because it reports the host, not the cgroup.
- `R --version | head -1`: `R version 4.6.1 (2026-06-24) -- "Happy Hop"`.
- `.libPaths()`:
  ```
  ~/R/x86_64-pc-linux-gnu-library/4.6
  /usr/local/lib/R/site-library
  /usr/lib/R/site-library
  /usr/lib/R/library
  ```
  The per-user library is first, which is what makes the `data.table` rebuild from the FPAR thread effective.

---

#### P1 — R environment audit

`R/checks/76_r_env_audit.R`, run in the background, 68.9 s wall. Logs: `logs/r_env_audit_20261104_154610.tsv` and `.log`.

Summary lines verbatim:

```
[2026-11-04 15:46:12] +   1.5s | scope: 76 file(s); 70 distinct package(s)
[2026-11-04 15:47:20] +  68.9s | OK 37   BROKEN 16   MISSING 17   (of 70)
[2026-11-04 15:47:20] +  68.9s |   0.4-chain OK 15  BROKEN 6  MISSING 7
[2026-11-04 15:47:20] +  68.9s |   both      OK 8  BROKEN 4  MISSING 1
[2026-11-04 15:47:20] +  68.9s |   upstream  OK 14  BROKEN 6  MISSING 9
```

All four invariants from the dispatch hold:

- `data.table` is OK at 1.18.6.1 from the `user` library;
- the `rlang`-dependent packages are BROKEN with `SETLENGTH`;
- `terra` (1.8.93) and `sf` (1.0.24) are OK;
- `s3fs` and `paws.storage` are MISSING.

**Is any BROKEN error something other than `SETLENGTH`?** One row's error text differs, and it resolves to the same cause. `devtools` reports `package 'usethis' could not be loaded` rather than naming the symbol. Loading `usethis` on its own fails with the same `rlang.so: undefined symbol: SETLENGTH`, so `devtools` is that failure one level down. Every one of the 16 BROKEN packages therefore traces to the single broken `rlang` build. That is useful for sizing the IT ask: rebuilding `rlang` alone against R 4.6.1 would likely clear most of the 16 at once, and the rest are its reverse dependencies that carry their own compiled code.

Full TSV (columns: `package status scope version lib used_by error`; the `error` column is truncated by the audit script, which is why the shared-object paths stop mid-line):

```
package	status	scope	version	lib	used_by	error
devtools	BROKEN	upstream	2.4.6	site	2:download_cropCalendar.R;download_rawCMIP6.R	package ‘usethis’ could not be loaded
dplyr	BROKEN	both	1.1.4	site	44:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;download_chirts.R;...	package or namespace load failed for ‘dplyr’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs/
ecmwfr	BROKEN	upstream	2.0.3	site	1:download_AgERA5.R	package or namespace load failed for ‘ecmwfr’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs
FactoMineR	BROKEN	upstream	2.13	site	2:QAQC_daily_files-NexGDDP.R;QAQC_for_indices.R	package or namespace load failed for ‘FactoMineR’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/
furrr	BROKEN	both	0.3.1	site	8:0.4.4_process_exposure.R;download_manual_nex_gddpCMIP6_v2_0.R;download_manual_nex_gddpCMIP6.R;...	package or namespace load failed for ‘furrr’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs/
ggplot2	BROKEN	both	4.0.1	site	3:0.4.1_create_livestock_exposure.R;QAQC_daily_files-NexGDDP.R;QAQC_for_indices.R	package or namespace load failed for ‘ggplot2’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/lib
progress	BROKEN	0.4-chain	1.2.3	site	1:0.4.5_create_faostat_long.R	package or namespace load failed for ‘progress’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/li
purrr	BROKEN	upstream	1.2.1	site	46:download_chirps.R;download_chirts.R;download_manual_nex_gddpCMIP6_v2_0.R;...	package or namespace load failed for ‘purrr’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs/
rvest	BROKEN	0.4-chain	1.0.5	site	2:0_server_setup.R;00_acquire.R	package or namespace load failed for ‘rvest’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs/
stringr	BROKEN	0.4-chain	1.6.0	site	1:haz_functions.R	package or namespace load failed for ‘stringr’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/lib
tibble	BROKEN	upstream	3.3.1	site	1:identifyCorruptedFiles.R	package or namespace load failed for ‘tibble’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs
tidyr	BROKEN	both	1.3.2	site	3:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;createAfricaSoils.R	package or namespace load failed for ‘tidyr’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs/
tidyverse	BROKEN	upstream	2.0.0	site	54:download_AgERA5.R;download_chirps.R;download_chirts.R;...	package or namespace load failed for ‘tidyverse’:  .onAttach failed in attachNamespace() for 'tidyverse', details:   call: NULL   error: package or namespace lo
viridis	BROKEN	0.4-chain	0.6.5	site	1:haz_functions.R	package or namespace load failed for ‘viridis’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/lib
wbstats	BROKEN	0.4-chain	1.1	site	2:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R	package or namespace load failed for ‘wbstats’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/lib
xml2	BROKEN	0.4-chain	1.5.2	site	1:0_server_setup.R	package or namespace load failed for ‘xml2’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs/r
arrow	MISSING	both			9:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	there is no package called ‘arrow’
aws.s3	MISSING	upstream			1:upload_AWS.R	there is no package called ‘aws.s3’
climate4R.UDG	MISSING	upstream			1:download_rawCMIP6.R	there is no package called ‘climate4R.UDG’
downscaleR	MISSING	upstream			1:download_rawCMIP6.R	there is no package called ‘downscaleR’
duckdb	MISSING	0.4-chain			2:0.4.4_process_exposure.R;0.4.5_create_faostat_long.R	there is no package called ‘duckdb’
geoarrow	MISSING	0.4-chain			4:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	there is no package called ‘geoarrow’
inborutils	MISSING	upstream			1:download_cropCalendar.R	there is no package called ‘inborutils’
loadeR	MISSING	upstream			1:download_rawCMIP6.R	there is no package called ‘loadeR’
MetBrewer	MISSING	0.4-chain			1:haz_functions.R	there is no package called ‘MetBrewer’
miceadds	MISSING	0.4-chain			1:haz_functions.R	there is no package called ‘miceadds’
paws.storage	MISSING	0.4-chain			3:0.4.5_create_faostat_long.R;0_server_setup.R;haz_functions.R	there is no package called ‘paws.storage’
s3fs	MISSING	0.4-chain			3:0.4.5_create_faostat_long.R;0_server_setup.R;haz_functions.R	there is no package called ‘s3fs’
sfarrow	MISSING	0.4-chain			1:haz_functions.R	there is no package called ‘sfarrow’
transformeR	MISSING	upstream			1:download_rawCMIP6.R	there is no package called ‘transformeR’
tsbox	MISSING	upstream			2:preprocess_nex-gddp-cmip6_daily_data_v2_0.R;preprocess_nex-gddp-cmip6_daily_data.R	there is no package called ‘tsbox’
visualizeR	MISSING	upstream			1:download_rawCMIP6.R	there is no package called ‘visualizeR’
wbkernel	MISSING	upstream			2:fast_calc_NDWS.R;fast_calc_waterbalance.R	there is no package called ‘wbkernel’
compiler	OK	upstream	4.6.1	base	4:calc_THI.R;fast_calc_NDWL0.R;fast_calc_NDWL50.R;...	
countrycode	OK	0.4-chain	1.6.1	site	5:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	
data.table	OK	0.4-chain	1.18.6.1	user	8:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	
DBI	OK	0.4-chain	1.2.3	site	2:0.4.4_process_exposure.R;0.4.5_create_faostat_long.R	
envirem	OK	upstream	3.1	site	1:calc_TAI.R	
exactextractr	OK	0.4-chain	0.10.1	site	1:0_server_setup.R	
fields	OK	upstream	17.1	site	1:bc_calc_anomalies.R	
fs	OK	0.4-chain	1.6.6	site	1:haz_functions.R	
future	OK	both	1.69.0	site	9:0.4.4_process_exposure.R;0.4.5_create_faostat_long.R;haz_functions.R;...	
future.apply	OK	both	1.20.1	site	3:0.4.5_create_faostat_long.R;haz_functions.R;identifyCorruptedFiles.R	
gdalUtilities	OK	0.4-chain	1.2.5	site	1:haz_functions.R	
geodata	OK	upstream	0.6.6	site	3:download_chirts.R;download_cropCalendar.R;createAfricaMask.R	
glue	OK	0.4-chain	1.8.0	site	1:0_server_setup.R	
gtools	OK	upstream	3.9.5	site	16:fixDailyFutureData.R;getDailyFutureData.R;identifyCorruptedFiles.R;...	
httr	OK	both	1.4.7	site	5:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0_server_setup.R;...	
jsonlite	OK	0.4-chain	2.0.0	site	5:0.4.4_process_exposure.R;0_server_setup.R;haz_functions.R;...	
lubridate	OK	upstream	1.9.4	site	25:download_AgERA5.R;download_chirps.R;preprocess_nex-gddp-cmip6_daily_data_v2_0.R;...	
ncdf4	OK	upstream	1.24	site	3:fast_calc_NDWL0.R;fast_calc_NDWL50.R;fast_calc_NDWS.R	
pacman	OK	both	0.5.1	site	39:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	
parallel	OK	upstream	4.6.1	base	2:download_AgERA5.R;identifyCorruptedFiles.R	
pbapply	OK	0.4-chain	1.7.4	site	5:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.3_fao_production_cv.R;...	
progressr	OK	0.4-chain	0.18.0	site	2:0.4.5_create_faostat_long.R;haz_functions.R	
R.utils	OK	upstream	2.13.0	site	1:download_chirps.R	
raster	OK	upstream	3.6.32	site	5:createAfricaMask.R;createAfricaSoils.R;fast_calc_NDWL0.R;...	
remotes	OK	0.4-chain	2.5.0	site	1:0_server_setup.R	
rgdal	OK	upstream	1.5.32	site	1:download_rawCMIP6.R	
rnaturalearth	OK	upstream	1.2.0	site	1:createAfricaMask.R	
rnaturalearthdata	OK	upstream	1.0.0	site	1:createAfricaMask.R	
sf	OK	0.4-chain	1.0.24	site	6:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	
sp	OK	both	2.2.0	site	2:0.4.3_fao_production_cv.R;createAfricaMask.R	
stats	OK	both	4.6.1	base	4:0.4.0_create_crop_vop_intld15.R;0.4.2_create_crop_vop_nominal_usd.R;00_paths.R;...	
stringi	OK	0.4-chain	1.8.7	site	1:haz_functions.R	
terra	OK	both	1.8.93	site	60:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	
tools	OK	0.4-chain	4.6.1	base	1:00_acquire.R	
utils	OK	both	4.6.1	base	7:00_acquire.R;download_chirps.R;download_cropCalendar.R;...	
wesanderson	OK	0.4-chain	0.3.7	site	1:haz_functions.R	
xts	OK	upstream	0.14.1	site	2:preprocess_nex-gddp-cmip6_daily_data_v2_0.R;preprocess_nex-gddp-cmip6_daily_data.R
```

Two observations for phases B and C:

- **Phase B (R/0.4.1 value-level gate) is blocked as things stand.** R/0.4.1 needs `dplyr`, `tidyr` and `ggplot2`, all BROKEN, plus `arrow` and `geoarrow`, both MISSING. R/0.4.0 and R/0.4.2 are in the same position.
- **Phase C (`hazards_upstream` 01 to 04) is closer than the headline numbers suggest.** The heavy `upstream` MISSING rows are climate4R, `loadeR`, `downscaleR`, `transformeR`, `visualizeR` and `inborutils`, which the `used_by` column shows belong to `download_rawCMIP6.R` and `download_cropCalendar.R`, outside 01 to 04. `wbkernel` is the one that matters inside that range: `fast_calc_NDWS.R` and `fast_calc_waterbalance.R` need it and it is not installed here.

**P2 gate:** `jsonlite` is OK, 2.0.0, from the site library. The gate passes, so P2 ran.

---

#### P2.1 — verify before

Log: `logs/mapspam_verify_before_20261104_154803.log`. Hashing about 1.3 GB took under a second of reported wall time because the size check short-circuits the missing and wrong-size objects.

Summary line verbatim:

```
[2026-11-04 15:48:39] +   0.0s INFO  |   manifest 48 object(s); local 36 file(s) matching '*.tif'
[2026-11-04 15:48:39] +   0.0s INFO  |   missing=12  ok=34  size=2
```

The verifier prints only the counts that have members, so there are no `hash=` or `extra=` figures on that line: no object had a size match with a hash mismatch, and no local file was outside the manifest. Closing line: `FAIL - 14 object(s) missing or wrong`.

All 14 non-`ok` rows, verbatim apart from the leading timestamp:

```
WARN  |   size    variable=phys-area_ha/spam_phys-area_ha_rf-all.tif  (local 45,664,814, manifest 47,197,123)
WARN  |   size    variable=phys-area_ha/spam_phys-area_ha_rf-highinput.tif  (local 11,715,130, manifest 25,830,038)
WARN  |   missing variable=vop_nominal-usd15/spam_vop_nominal-usd15_all.tif  (local NA, manifest 19,451,146)
WARN  |   missing variable=vop_nominal-usd15/spam_vop_nominal-usd15_irr.tif  (local NA, manifest 5,070,786)
WARN  |   missing variable=vop_nominal-usd15/spam_vop_nominal-usd15_rf-all.tif  (local NA, manifest 19,048,878)
WARN  |   missing variable=vop_nominal-usd15/spam_vop_nominal-usd15_rf-highinput.tif  (local NA, manifest 10,644,922)
WARN  |   missing variable=vop_nominal-usd15/spam_vop_nominal-usd15_rf-lowinput.tif  (local NA, manifest 15,662,860)
WARN  |   missing variable=vop_nominal-usd15/spam_vop_nominal-usd15_rf-subsistence.tif  (local NA, manifest 14,119,296)
WARN  |   missing variable=vop_nominal-usd21/spam_vop_nominal-usd21_all.tif  (local NA, manifest 19,584,380)
WARN  |   missing variable=vop_nominal-usd21/spam_vop_nominal-usd21_irr.tif  (local NA, manifest 5,078,404)
WARN  |   missing variable=vop_nominal-usd21/spam_vop_nominal-usd21_rf-all.tif  (local NA, manifest 19,169,272)
WARN  |   missing variable=vop_nominal-usd21/spam_vop_nominal-usd21_rf-highinput.tif  (local NA, manifest 10,678,008)
WARN  |   missing variable=vop_nominal-usd21/spam_vop_nominal-usd21_rf-lowinput.tif  (local NA, manifest 15,766,518)
WARN  |   missing variable=vop_nominal-usd21/spam_vop_nominal-usd21_rf-subsistence.tif  (local NA, manifest 14,207,476)
```

The 12 missing objects are two complete variable directories, `vop_nominal-usd15/` and `vop_nominal-usd21/`, six files each. This matches the 2026-10-09 finding exactly: 36 of 48 present, two of those truncated.

#### P2.2 — re-fetch

Launched with `nohup` exactly as the dispatch writes it, into `logs/mapspam_refetch_20261104_155500.log` with a matching `.pid`. Tracked by polling `kill -0` on the pid; no `wait`.

```
INFO  | acquire 'mapspam-2020v1r2' method=s3-dir -> <working_dir>/Data/mapspam/2020V1r2_SSA/processed
INFO  |   pinned manifest: 48 object(s), 1,332,962,700 bytes
INFO  |   48 object(s): 48 fetched (36 re-fetched), 0 already present, 1,332,962,700 bytes
INFO  |   verified: hash 48, size only 0, drift 0, no manifest 0
```

All four expectations met, and `verification failed` appears nowhere in the log.

**Rate: 530.6 s wall (8 min 51 s) for 1,332,962,700 bytes, which is 2.51 MB/s (2.40 MiB/s).** That is roughly twice the macbook's 1.3 MB/s from the same bucket, and it is worth recording against the FPAR result from the other thread: that ingest ran 29.5 h here against 1.9 h on cglabs. The two together say the FPAR slowness was not raw S3-over-HTTPS bandwidth from this node. This node pulls a large single-stream S3 object at a healthy rate.

Note on the `(36 re-fetched)` count: with `force = TRUE` every object is fetched, so `36` is the number that already existed locally, including the two truncated ones, and `0 already present` is consistent with that. The two truncated tiles were overwritten through their `.part` files rather than left in place.

#### P2.3 — verify after

```
[2026-11-05 11:23:01] +   0.0s INFO  | verify 'mapspam-2020v1r2' at <working_dir>/Data/mapspam/2020V1r2_SSA/processed  [hash]
[2026-11-05 11:23:01] +   0.0s INFO  |   manifest 48 object(s); local 48 file(s) matching '*.tif'
[2026-11-05 11:23:01] +   0.0s INFO  |   ok=48
PASS - every manifest object present and matching
```

The two tiles, before against after against the dispatch's reference:

| tile | before size | before md5 | after size | after md5 | reference size | reference md5 | match |
|---|---|---|---|---|---|---|---|
| `spam_phys-area_ha_rf-all.tif` | 45,664,814 | `8a91f44edb3394d4a21e9115ff3d32a9` | 47,197,123 | `3807675ba25f455ffaa4d8f3d84e58f8` | 47,197,123 | `3807675ba25f455ffaa4d8f3d84e58f8` | yes |
| `spam_phys-area_ha_rf-highinput.tif` | 11,715,130 | `32a0f7c1fa8ef04cef679418b6ed1371` | 25,830,038 | `e5cc9a45b20fb9e45e60080558a7fa6b` | 25,830,038 | `e5cc9a45b20fb9e45e60080558a7fa6b` | yes |

Both now agree with the macbook copy byte for byte. `rf-highinput` had been missing more than half its bytes, which is why anything that read it would have produced wrong numbers rather than an error.

`.part` files remaining: **0**.

Receipt tail (`atlas_acquire_status`), one row, since this is the first acquire of this record on this node:

```
                timestamp method files      bytes status
 2026-11-04T21:19:15+0300 s3-dir    48 1332962700     ok
```

**Extras: none.** The dispatch expected `variable=vop_intld15-2021/` and `fao_prices/` to show up as `extra`, from R/0.4.0 and R/0.4.2. Neither exists in this tree, and `ok=48` with 48 local files confirms there is nothing beyond the manifest. The explanation is that R/0.4.0 and R/0.4.2 have never run on this node, which the P1 result makes inevitable: both need `dplyr`, `tidyr`, `arrow` and `geoarrow`, which are BROKEN or MISSING here. The eight variable directories present are all manifest members, six files each:

```
variable=harv-area_ha        6
variable=phys-area_ha        6
variable=prod_t              6
variable=vop_intld15         6
variable=vop_nominal-usd15   6
variable=vop_nominal-usd21   6
variable=vop_usd2015         6
variable=yield_kgha          6
```

Nothing was deleted.

---

#### Notes

- **The new acquire layer works on a node, first time.** The pinned manifest, the `.part` staging and the base-R HTTPS reader all behaved as designed, and the `s3fs` gap is no longer in the way. That is the thing phase A was really testing.
- **Writes stayed inside the permitted set**: the MapSPAM processed tree, its receipt at `<working_dir>/Data/_acquisition/mapspam-2020v1r2.jsonl`, and `logs/`. Nothing else in the repo or the bulk area changed. No S3 writes were attempted.
- **The IT ask is now one line**: rebuild the site-library packages against R 4.6.1, starting with `rlang`, which is the single root of all 16 BROKEN rows. The per-user fallback works but does not scale: it put `data.table` 1.18.6.1 into the user library, and doing the same for 16 packages and their compiled dependencies would take a long time and leave this node silently different from the others.
- Logs kept: `logs/r_env_audit_20261104_154610.{tsv,log}`, `logs/mapspam_verify_before_20261104_154803.log`, `logs/mapspam_refetch_20261104_155500.log`, `logs/mapspam_verify_after_20261104_155500.log`.

---

### MACBOOK 2026-11-02 — P0-P2: R audit (read-only), re-fetch `mapspam-2020v1r2` (the one fix). **Small profile is fine.**

**Why.** Issue [#58](https://github.com/AdaptationAtlas/hazards_prototype/issues/58) tracks the seven #29
follow-ups.
- **Phases B and C need a working R stack.** Phase B runs R/0.4.1 here for a value-level cross-host gate. Phase C runs
  one GCM through `hazards_upstream` 01→04. Since the 4.5.1 → 4.6.1 upgrade, `data.table` and `rlang` fail to load
  here. P1 measures how much of the stack those two phases need is broken, which sizes the IT ask and the per-user
  rebuild fallback.
- **The MapSPAM fix.** On 2026-10-09 your processed MapSPAM tree held 36 files against the 48 the origin holds, and
  two of them were truncated. `R/00_acquire.R` now pins a per-object manifest
  (`metadata/catalogue/_manifests/mapspam-2020v1r2.tsv`: size plus S3 ETag), writes each download through a `.part`
  file and hashes it. It also reads S3 over plain HTTPS in base R, so it no longer needs `s3fs`, which this node does
  not have. P2 is the first run of that code on a node.

**Rules.**
- **Install nothing.** No `install.packages`, no `pacman`, and do **not** source `R/0_server_setup.R`: its
  `pacman::p_load()` installs whatever is missing.
- **Writes go only to:**
  - the MapSPAM processed tree (`<working_dir>/Data/mapspam/2020V1r2_SSA/processed`);
  - its receipt (`<working_dir>/Data/_acquisition/mapspam-2020v1r2.jsonl`);
  - `logs/`.
- No S3 writes (none are possible here).
- **Background jobs** use `nohup … &` with `logs/<name>_<STAMP>.log` plus a `.pid` file. Track them by polling
  `kill -0 $(cat <pid>)` or the log tail, not with `wait`.
- **Shell variables.** If your shell does not keep variables between commands, start each one with
  `cd <repo> && export ATLAS_PROJECT_DIR=$PWD`, and re-derive `MS` (P2.1) where it is used.
- **Public repo.** No usernames, hostnames, IPs or mount identifiers in the RESPONSE. Write paths as `<repo>`,
  `<working_dir>` and `~`.

#### P0 — pull, identity, limits (this is item (c))
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD
git fetch && git rev-parse --abbrev-ref HEAD          # must be: develop
git pull --ff-only && git log --oneline -1            # at or after the commit that added this file
CG=/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup); cat $CG/cpu.max $CG/memory.max
R --version | head -1; Rscript -e 'cat(.libPaths(), sep = "\n")' | sed "s#$HOME#~#"
```
**Stop and report** if the branch is not `develop`. Do not switch branches yourself.

#### P1 — (a) R environment audit (read-only, a few minutes)
`R/checks/76_r_env_audit.R` statically scans R/0.4.x, together with what they need sourced first and
`hazards_upstream/R/**`, for `library`, `require`, `requireNamespace`, `p_load` and `pkg::`. That gives 70
packages. It then loads each one in its own fresh `Rscript` and reports OK, BROKEN or MISSING, the version, and the
library it came from (user / site / base). It installs nothing.
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup Rscript R/checks/76_r_env_audit.R --out logs/r_env_audit_$STAMP.tsv > logs/r_env_audit_$STAMP.log 2>&1 &
echo $! > logs/r_env_audit_$STAMP.pid
# poll: kill -0 $(cat logs/r_env_audit_$STAMP.pid) 2>/dev/null && echo running || tail -6 logs/r_env_audit_$STAMP.log
```
**What to expect** (invariants, not exact counts):
- `data.table` is OK from `user` (1.18.6.1);
- `rlang`-dependent packages (`dplyr`, `tidyr`, `purrr`, …) are BROKEN with `SETLENGTH`;
- `terra` and `sf` are OK;
- `s3fs` and `paws.storage` are MISSING (already known).

Some `upstream` rows come from `hazards_upstream` scripts outside 01→04 (climate4R and similar). The `used_by` column
shows which.

**Report:**
- the summary lines verbatim (`OK / BROKEN / MISSING`, plus the per-scope lines `0.4-chain` / `both` /
  `upstream`);
- the whole TSV in a fenced block;
- one line on whether any BROKEN error is something **other** than `SETLENGTH`.

#### P2 — (b) re-fetch `mapspam-2020v1r2` (the one fix)
**Gate:** `jsonlite` must be OK in the P1 table, because the acquire layer reads the catalogue with it. If it is
BROKEN or MISSING, stop here and do not install it.

**P2.1 before (read-only; hashes about 1.3 GB):**
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup Rscript R/checks/75_catalogue_manifest.R --verify mapspam-2020v1r2 --hash > logs/mapspam_verify_before_$STAMP.log 2>&1 &
echo $! > logs/mapspam_verify_before_$STAMP.pid
MS=$(Rscript -e 'source("R/00_paths.R"); cat(file.path(atlas_working_dir(), "Data/mapspam/2020V1r2_SSA/processed"))')
for f in rf-all rf-highinput; do p="$MS/variable=phys-area_ha/spam_phys-area_ha_$f.tif"; echo "$f $(stat -c %s "$p" 2>/dev/null) $(md5sum "$p" 2>/dev/null | cut -d' ' -f1)"; done
```
These ETags are multipart (`<md5>-<parts>`), so a plain md5 of the file will **not** equal them. The verifier
recomputes the multipart form; the plain md5s are reported for the record.

**Reference** (S3 listing 2026-11-02; the macbook copy hash-verifies against it, 48/48):

| tile | bytes | S3 ETag | plain md5 of a good copy (macbook) |
|---|---|---|---|
| `spam_phys-area_ha_rf-all.tif` | 47,197,123 | `851e2ea5dc5b7cb19f6303cd9e05400d-6` | `3807675ba25f455ffaa4d8f3d84e58f8` |
| `spam_phys-area_ha_rf-highinput.tif` | 25,830,038 | `f2e4ecb28b9e774e2c8507f836d62bda-4` | `e5cc9a45b20fb9e45e60080558a7fa6b` |

**P2.2 fetch** (`force = TRUE`, scoped to this one record, about 1.33 GB over HTTPS). The macbook gets about 1.3 MB/s
from this bucket, so allow **20-60 min**:
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup Rscript -e 'source("R/00_paths.R"); source("R/00_acquire.R"); atlas_acquire("mapspam-2020v1r2", force = TRUE)' \
  > logs/mapspam_refetch_$STAMP.log 2>&1 &
echo $! > logs/mapspam_refetch_$STAMP.pid
# poll: kill -0 $(cat logs/mapspam_refetch_$STAMP.pid) 2>/dev/null && echo running || tail -6 logs/mapspam_refetch_$STAMP.log
```
**What to expect:**
- the log shows `pinned manifest: 48 object(s), 1,332,962,700 bytes`;
- it ends `48 object(s): 48 fetched (N re-fetched)`, where N = files that already existed;
- the closing line reads `verified: hash 48, size only 0, drift 0, no manifest 0`;
- no `verification failed` anywhere.

Give the wall time and the effective MB/s, since this is the S3-over-HTTPS rate for this node.

**P2.3 after:**
```bash
Rscript R/checks/75_catalogue_manifest.R --verify mapspam-2020v1r2 --hash > logs/mapspam_verify_after_$STAMP.log 2>&1; tail -4 logs/mapspam_verify_after_$STAMP.log
for f in rf-all rf-highinput; do p="$MS/variable=phys-area_ha/spam_phys-area_ha_$f.tif"; echo "$f $(stat -c %s "$p") $(md5sum "$p" | cut -d' ' -f1)"; done
find "$MS" -name '*.part' | wc -l       # expect 0
Rscript -e 'source("R/00_paths.R"); source("R/00_acquire.R"); s <- atlas_acquire_status("mapspam-2020v1r2"); print(tail(s[, c("timestamp","method","files","bytes","status")], 2), row.names = FALSE)'
```
**What to expect:**
- `PASS`, with `ok=48`;
- the two tiles at the reference sizes and plain md5s above;
- zero `.part` files;
- the last receipt row `ok`.

Any `extra` rows are local files the origin does not hold. They are expected, not a failure: R/0.4.0 and R/0.4.2 write
`variable=vop_intld15-2021/` and `fao_prices/` into this tree. List the extras by top-level directory with a count for
each, and **do not delete them**.

**Report:**
- the P2.1 summary line (`ok=… missing=… size=… hash=… extra=…`) and every non-`ok` row;
- the two tiles' before/after size and md5, each set against the reference;
- the P2.2 tail and its rate;
- the P2.3 lines.

**Stop and describe** (do not improvise a fix) on any of:
- not on `develop`;
- `jsonlite` not OK;
- `verification failed` in P2.2;
- P2.3 not `PASS` (other than extras);
- anything written outside the three places listed under Rules.

**To answer:**
1. Prepend one `### RESPONSE — pascal` (P0-P2) to this file.
2. Run the `AGENTS.md` §3 IP grep, and check by eye that the RESPONSE holds no usernames or hostnames.
3. Commit only this file, then push.
4. Verify it landed: `git fetch`, then `git log origin/develop..HEAD` must be empty.
5. End with a delivery line.

Keep the logs.
