# DISPATCH — pascal — a per-user `obs` conda env so PASCAL can read the CDH Zarr cubes

**Append-only; newest block on top. Prepend a `### RESPONSE — pascal` block to answer.**

### MACBOOK 2026-10-24 — build `~/.conda/envs/obs` (no root, no system change), verify the cube read, then run the S1 reference gate on this node alone

**Why.** PASCAL cannot open the cubes: system GDAL is 3.4.3 and the default `python3` has no `zarr` or
`numpy` (`DISPATCH_obs_hf_probe.md`, pascal block items 2-3). A per-user env fixes both without
touching the system R/GDAL stack, which `server-environment.md` §8.4 says to leave alone because
rebuilding it would shadow ~1,680 site-library packages. With the env in place, PASCAL becomes a real
fallback for the cube route and can run the S1 independent-reference gate **by itself**. It has the CHC
GeoTIFF (2015-06-15 Tmax, byte-identical to CHC) on disk; the env reads the cube side.

**Rules.**
- **No root, no system packages, no edits to `/opt/conda`.**
- **Do not modify `~/.conda/envs/sci`.** Other kernels use it. Create a **new** env.
- conda-forge only, as already set in `~/.condarc` (`server-environment.md` §6).
- Everything reversible: `conda env remove -p ~/.conda/envs/obs` or `rm -rf` undoes it.
- Background anything over about a minute, with logs in `logs/pascal_obs_env_<STAMP>.log`.
- Public repo: no IPs, hostnames, mount identifiers or usernames in the RESPONSE.
- Say which session type and profile you are in.

#### E1 — inspect (no change)
- `which conda mamba micromamba`.
- Is `~/.conda/envs/sci` present and activatable? Report its `gdalinfo --version`, `python -V` and `numpy`
  version.
- Home free space (`df -h ~`). `/home` is about 71 % full; the env should be about 1-2 GB, so say
  whether the env and its package cache should go under the bulk data root instead (`conda create -p
  <bulk>/...`, `CONDA_PKGS_DIRS=<bulk>/...`). **Prefer the bulk root if home has under ~20 GB free.**

#### E2 — build (dry-run first)
```bash
conda create -p <ENV> -c conda-forge --override-channels \
  "python=3.12" "zarr>=3" xarray numpy fsspec aiohttp requests "gdal>=3.11" "libgdal-zarr" --dry-run
```
- `libgdal-zarr` may be bundled into `libgdal` on conda-forge. If the solver rejects it, drop it and
  confirm the driver with `gdalinfo --formats | grep -i zarr` after the build.
- Report the solver's GDAL / zarr / numpy versions and the download size. Then run it for real.
- Do **not** register a Jupyter kernel or touch `~/.Renviron` / `RETICULATE_PYTHON`.

#### E3 — verify the cube read (pinned revisions)
CHIRTS cube: `https://huggingface.co/datasets/aaguilar90/chirts-era5-daily/resolve/c44a0b58772e32f7cbb2b5a740d5e6586ceeb054/chirts-era5-daily.zarr`.

In the env, with Python + xarray (`zarr.storage.FsspecStore` / `xr.open_zarr(..., storage_options=...)`
over HTTPS):
- open the store and print the variables, dims, coordinate ranges and the `scale_factor` attributes;
- select **Tmax, 2015-06-15**, Africa box lon −18…52, lat −35…38, and report:
  - shape;
  - min / mean / max **after** xarray's decode, which applies the scale; do not scale again;
  - whether lat runs north→south.

  **Invariant:** mean Africa Tmax is roughly 25-35 °C.

Then GDAL 3.13 from the env: `gdalinfo 'ZARR:"/vsicurl/<store>":/Tmax'`. **Does it report a real
geotransform and CRS?** CGlabs' GDAL 3.11.3 did not; it reported extent `0,7200,0,2600` and no CRS.
Report what 3.13 does.

#### E4 — the S1 independent-reference gate (`PLAN_obs_hazard_pipeline.md` §4)
Compare the cube's Tmax for 2015-06-15 against the node's CHC GeoTIFF for the same day
(`chirts_raw/tmax/…/CHIRTS-ERA5.daily_Tmax.2015.06.15.tif`) over the Africa box, cell by cell on the
0.05° grid:
- report n_cells, max |diff| and mean diff;
- align by **coordinates**, not array index; state how;
- NA handling: report cells NA in one and not the other.

**Invariant:** max |diff| ≤ 0.05 °C (the int16 × 0.1 quantum, rounded) and NA patterns agree except
ocean.

Repeat for one more date (2020-12-31, the end of the node's mirror) and for **Tmin**.

**Stop and describe** if any |diff| > 0.05. That would mean the cube is not the CHC data, or the
alignment or scale is wrong; we need to know which.

#### Then
1. Prepend `### RESPONSE — pascal` with E1-E4 and the env's location.
2. Add an `obs` env entry to `server-environment.md` §5 (path pattern, purpose, key versions; no
   username).
3. Commit **only** this file and `server-environment.md`, then push.
4. Verify on `origin/develop`.
