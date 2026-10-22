# DISPATCH — cglabs — fill real paths and data locations into the pipeline map

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### MACBOOK 2026-10-22 — READ-ONLY inventory. Verify §3 of the pipeline map against the node and fill in what exists.

**Why.** `HANDOVER_2026-10-22_hazard-pipeline-map-and-obs-baseline.md` §3 maps each pipeline step to a
server location. The paths were derived **from code** on the macbook (`metadata/hosts.json`,
`R/00_paths.R`, the `file.path()` calls in R/1-R/3) and have not been checked against disk. The map
will be handed to the Carbon Credits for Adaptation analyst and used to design the simplified observed
baseline (§4). It has to say what is actually there.

**Scope: read-only.**
- No deletes, moves or renames.
- No R/1-R/3 runs and no S3 writes.
- Anything that walks a large tree (a `du` or `find` over `Data/` or `indices*`) goes in the background
  with `nohup … &` and a log in `logs/pipeline_map_<STAMP>.log`, because of the two-minute shell kill.
  Prefer `find -maxdepth` + `wc -l` over a recursive `du` where a count is enough.

**Public repo.** Write paths in their `{common_data}/…` form or as `/home/jovyan/common_data/…`.
- Do **not** paste mount identifiers, NFS server names, hostnames or IPs.
- Run the `git grep` IP check from `AGENTS.md` §3 before committing.

#### Step A — the §3 table, row by row (steps 0-5)

For **each** row of the §3 table, report:
- **A1.** The real directory: confirm the derived path, or give the correct one. Note any per-host
  difference from `metadata/hosts.json`.
- **A2.** One **real example filename** per `<period>` (`jagermeyr`, `annual`), as it sits on disk.
- **A3.** The file count per period, and the total size (approximate is fine; say how you measured it).
- **A4.** The layer count and layer names of the example file (`terra::nlyr()` / `names()`): for
  example 19 seasons for steps 1-2a, 9 classes for 3b, and the scenario × timeframe layers for 3c.
- **A5.** The `mtime` range (oldest → newest), so we can see which files are the #13 bake (2026-10)
  and which are older leftovers.

Also report any directory that exists under `Data/` or `indices_seasonal/` and is **not** in the table,
with a one-line description.

**Expected invariants. These are not exact figures; stop and describe if one fails:**
- **Step 2a (`hazard_timeseries_class`) and 3b (`hazard_timeseries_int`) files are per-GCM.** The set of
  GCM tokens equals the 18 in the published sidecar.
- **Step 3a (the 1/10/100 interaction code) has no files anywhere.** If you find per-season code
  rasters, that contradicts the map. Report it.
- **Step 3c bundles (`hazard_risk/<period>/*_int.tif`) contain only the #13 hazard sets**
  (`NDWS+NTx35+NDWL0`, `NDWS+NTxM+NDWL0`, `PTOT-L+NTxM+PTOT-G`, `NDWS+THI-max+NDWL0`,
  `PTOT-L+THI-max+PTOT-G`). The `NTxS` stacks were removed on 2026-10-20.

#### Step B — observed and other source trees (feeds §2 and §4)

- **B1. Observed pipeline:** `Data/chirts_chirps_hist/` (or wherever `atlas_dir("chirts_chirps_hist")`
  resolves).
  - The variables present, the year range per variable, and whether anything **daily** exists there.
- **B2. Raw CHIRPS / CHIRTS trees** (`chirps_raw`, `chirts_raw` in `hosts.json`):
  - which product and version (CHIRPS v2.0 vs v3; CHIRTS v1.0 vs ERA5);
  - daily or monthly;
  - year range;
  - approximate size.
  - **Is any CHIRPS v3 daily or CHIRTS-ERA5 daily on the node?**
- **B3. The `atlas_delta` tree:** `{common_data}/atlas_hazards/cmip6/indices` and `indices_seasonal`.
  - Does it exist?
  - Which indices does it hold, and which historical source and years (CHIRPS v2.0 / CHIRTS v1.0 /
    AgERA5)?
  - Approximate size.
- **B4. ERA5 / AgERA5:** is any radiation, humidity or wind data on the node, and where? This decides
  whether NDWS or THI can be built on an observed baseline.
- **B5. GLPS:** `atlas_dirs$data_dir$GLPS` (`R/0_server_setup.R:353-355`). Is it populated? List the
  files.
- **B6. GGCMI Phase 3 calendars:** which crop calendar files are on the node (`ggcmi_dir`)? Only
  `mai_rf_…` is used today.

#### Step C — write it in

1. Edit `HANDOVER_2026-10-22_hazard-pipeline-map-and-obs-baseline.md` §3. Replace each derived path
   with the verified one, and add the example filename, counts and layer structure. Use a column, or a
   short "verified on node 2026-10-xx" sub-table under the main table, whichever reads better.
2. Correct §2 where B1-B4 contradict it.
3. Add the B4-B6 findings to §4.2.
4. Mark anything you could not verify as **unverified** rather than leaving the derived value standing
   as fact.
5. Prepend a `### RESPONSE` block here. Summarise what matched, what did not, and the invariant
   outcomes.
6. Commit and push both files. Then verify the push landed: `git fetch`;
   `git log origin/develop..HEAD` empty;
   `git show origin/develop:DISPATCH_cglabs_pipeline_map_paths.md | grep -c RESPONSE`.

**Stop and report rather than improvise** if:
- a step-3c bundle carries an unexpected hazard set;
- per-season interaction codes exist on disk;
- the GCM set in 2a/3b is not the published 18.
