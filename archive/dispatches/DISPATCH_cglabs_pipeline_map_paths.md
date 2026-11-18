# DISPATCH — cglabs — fill real paths and data locations into the pipeline map

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### RESPONSE — cglabs 2026-10-22 — inventory done, read-only, all three invariants pass

**Scope kept.**
- No deletes, moves, renames, R/1-R/3 runs or S3 writes.
- Large walks ran under `nohup` and logged to `logs/pipeline_map_20261022_073546.log` and `…_codes.log`.
  `logs/` is gitignored.
- The per-directory listings that the figures come from were kept out of the repo.

**Where it is written.**
- Handover §3: verified roots, a corrected path column, and a new §3.1 with the example file, counts,
  size, layers and mtime range per step and per period, plus the invariant evidence and the unlisted
  directories.
- §2: corrected with B1-B3, and a new raw-trees table.
- §4.2: B4-B6 added.
- §1.1: GLPS row corrected.

**What matched the derived map.**
- All three roots (`INDICES`, `SEASONAL`, `DATA`) are correct for this node, which resolves to the
  `nexgddp` defaults.
- The directory for every step 1-5 exists where the map says.
- Layer structure is as described:
  - steps 1 and 2a have 19 seasons;
  - step 3b has 9 classes;
  - the 3c `_int` bundles have 153 layers (17 scenario × timeframe × 9 classes).
- Step 3a: no files, as the map says.

**What did not match (corrected in §3):**
- **Step 0 file name:** the month is zero-padded, `<IDX>-YYYY-MM.tif`, not `-M`. There are also
  `historical_<gcm>_1981_2014/` dirs next to `_1995_2014`.
- **Naming switches between steps 1 and 2a.**
  - Steps 0-1 use `historical_<gcm>_1995_2014`.
  - From step 2a on, files use `historic_<gcm>_1995-2014`.
  - The map's single `<scen>_<gcm>_<yyyy-yyyy>` example only holds from 2a onward.
- **Step 3c also holds per-GCM bundles** (`<crop>_<gcm>_<sev>_<set>_int.tif` for all 18), not only
  ensemble ones. It also holds 2,640 **solo** bundles per period (`<crop>_<model>_<sev>.tif`, 51
  layers), and 240 leftover 2025-08 `haz-freq_<gcm>_*_adm_*.parquet` files.
- **`NDWS+NTx35+NDWL0` exists in 3c only for `generic-crop`.** The map's example
  `maize_ENSEMBLEmean_severe_NDWS+NTx35+NDWL0_int.tif` does not exist, so I changed the example to the
  `NTxM` set. Step 4 nonetheless has that set for 34 crops; the mechanism is unverified.
- **Step 4 is ensemble-only** (`ENSEMBLEmean`/`ENSEMBLEsd`).
- **Step 5 parquets sit inside `hazard_risk_<v>/<period>/`**, and the model token also takes the value
  `ENSEMBLE`.
- **`hazard_risk_vop_usd/` is incomplete, consistent with #47 in flight.** `jagermeyr/` is empty, and
  `annual/` holds only the `ENSEMBLEmean`/`extreme` parquet, dated 2026-10-22.
- **Step 1 was not rebaked in #13.** Its mtimes are 2025-07/08, plus 2026-06.
  - Steps 2a and 2b are all the 2026-10-08/09 bake.
  - In step 3b, 56,100 of 89,760 files per period are 2026-10. The other 33,444 are 2026-05 combos
    (`PTOT+NTxNN+PTOT`, `THI` sets) that the #13 bake did not touch.

**Invariants:**
1. **2a/3b GCM set = the published 18: PASS.**
   - `hazard_timeseries_class` holds exactly the 18 sidecar `ensemble.members`, with no ensemble files.
   - `hazard_timeseries_int` holds the 18 plus `ENSEMBLEmean`/`ENSEMBLEsd`.
   - Steps 0 and 1 carry the same 18.
2. **No per-season interaction codes on disk: PASS.**
   - A name search over the nexgddp working dir, the `hazards_prototype` working dir and `atlas_hazards`
     found only FAO `*Codes.csv`.
   - Every sampled 3b file, from both the 2026-05 and 2026-10 bakes, has the 9 frequency layers.
     `hazard_risk/*/intermediate/` is empty.
   - The check is by name and layer structure, not a value scan of every file.
3. **3c `_int` bundles hold only the five #13 sets: PASS.** There is no `NTxS` anywhere in
   `hazard_risk/<period>/`.

**Step B:**
- **B1.** `DATA/chirts_chirps_hist/` holds monthly data only, with **nothing daily**:
  - `PTOT` 1981-01 → 2026-04 (CHIRPS v3.0 monthly Africa, per `manifest.csv`);
  - `TMAX`/`TMIN`/`TAVG` 1980-01 → 2026-04 (CHIRTS-ERA5 monthly; `TAVG` derived);
  - `SPEI-01/03/06/12/24` 1981-01 → 2026-04;
  - seasonal 3-month `PTOT` sums for 1981-2025, and admin parquets;
  - about 31 GB in all.
- **B2. No CHIRPS v3 daily and no CHIRTS-ERA5 daily on the node.**
  - `chirps_wrld` is CHIRPS v2.0 daily tif for 1981-2023, about 0.9 TB.
  - `chirps_af/netcdf` is CHIRPS v2.0 daily (from the netCDF `version` attribute) for 1981-2025, about
    48 GB. `chirps_prec_2026.nc` is 0 bytes.
  - `chirts` is CHIRTSdaily v1.0 `Tmax`/`Tmin`/`RHum` daily for 1983-2016, about 2.8 TB.
  - AgWise download scripts for both new products sit at the `{common_data}` root (2026-05-28). Their
    relative landing dir does not exist within 4 levels of home or `{common_data}`.
- **B3.** `atlas_delta` exists, about 2.5 TB in all:
  - `historical/` holds 64 monthly indices for 1995-2014 (and `PRCPTOT` for 1981-2022). Its source is
    CHIRPS v2.0 / CHIRTS v1.0 by code only; the files carry no source tag.
  - `historical-AgERA5/` holds 11 indices for 1981-2022.
  - The futures are deltas for 5 GCMs plus `ENSEMBLE`.
- **B4.** AgERA5 v1.0 daily is on the node at `{common_data}/ecmwf_agera5`, about 1.4 TB, for 1981-2022.
  It has solar radiation, RH, 10 m wind and Tmax/Tmin/Tmean. ERA5 proper is six country folders only;
  its variables are unverified.
- **B5.** GLPS is populated: `glps_gleam_61113_10km.tif`, global, 10 km, classes 1-15+. The legend is
  not on disk.
- **B6.** GGCMI Phase 3 has 40 calendars on the node: 20 crops × rf/ir, `v1.01`. Only `mai_rf` is read.

**Not checked:**
- the S3 copies for step 5;
- the `ecmwf_agera5/netcdf/*` contents;
- the `ecmwf_era5` variables;
- the `chirps_af/zarr` and `chirps_af/raw` sizes;
- whether `atlas_crop_calendar/raw_jagermeyr` is file-identical to `DATA/ggcmi/`.

All of these are marked **unverified** in the handover.

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
