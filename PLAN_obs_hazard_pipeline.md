# PLAN — a streamlined observed hazard pipeline (CHIRPS v3 + CHIRTS-ERA5)

**Status:** draft for decision, 2026-10-22. Nothing is built.
**Inputs to this plan:**
- `HANDOVER_2026-10-22_hazard-pipeline-map-and-obs-baseline.md`, with the node-verified inventory
  (`DISPATCH_cglabs_pipeline_map_paths.md`);
- the CHC server, checked 2026-10-22.

**Goal:** baseline hazard frequency × exposure for a small set of heat and drought hazards, for crops
and livestock, from observed data. It must be comparable with the NEX-GDDP product, and it must keep
the per-season series that the current chain throws away.

---

## 1. Can CHIRPS + CHIRTS supply the inputs? Mostly; solar radiation is the gap

| index | inputs (as coded upstream) | CHIRPS v3 / CHIRTS-ERA5 | gap and options |
|---|---|---|---|
| `PTOT`, `NDD`, dry spells | daily P | CHIRPS v3 daily, `rnl` (1981-) or `sat` (1998-); a **0.25° `p25` version exists** (`data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/final/rnl/p25/`) | none. **Caveat:** daily CHIRPS is pentad totals split by ERA5 timing, so CHC itself warns about artificial wet runs. Dry-spell indices inherit that. Pentad and monthly totals are robust. |
| `NTx35`, crop `NTx*` | daily Tmax | CHIRTS-ERA5 daily Tmax (`experimental/CHIRTS-ERA5/tmax/tifs/daily/`, 1959-, 0.05°, ~27 MB/day global) | none |
| `NDWS`, `NDWL0` (legacy kernel, the one in production) | daily P, Tmax, Tmin, **`rsds`** (`peest2` Priestley-Taylor, `hazards_upstream/R/04_indices/fast_calc_NDWS.R:22-75`), soils `sscp_world.tif` / `ssat_world.tif` (on the node) | P, Tmax, Tmin yes; **solar radiation no** | **`rsds` is the only missing input** (options in §1.1) |
| `NDWS` v2 (FAO-56, unrun, #45) | + `hurs`, `sfcWind`, `sroot_world.tif` | humidity possibly via CHIRTS-ERA5 vpd; wind no | not for v1 |
| `THI` (livestock) | daily Tmax + RH | Tmax yes. CHIRTS-ERA5 publishes **vpd** (from ERA5 dewpoint; per `aaa.readme.txt`), from which mean RH is derivable, but **the vpd files were not found on the server listing**. A daily heat index `hi` (from Tmax + RH at Tmax) is listed. | RH: **unverified** in CHIRTS-ERA5; AgERA5 RH on the node 1981-2022; CHIRTS v1.0 RHum on the node 1983-2016 |
| SPEI | monthly P, Tmax, Tmin (Hargreaves) | **already built**, 0.05°, 1981-2026 | none |

### 1.1 Options for solar radiation

| option | coverage | effort | consistency |
|---|---|---|---|
| **A. AgERA5 `solar_radiation_flux`, already on the node** (`{common_data}/ecmwf_agera5`, v1.0 daily) | 1981-2022 | low: resample to the grid | good. A reanalysis like the radiation the GCMs bias-correct toward. |
| B. extend past 2022 from AgERA5 via CDS | 2023- | low, but **needs a CDS credential**, which `HANDOVER_2026-10-20.md` §0 says to revoke and not reissue | same as A |
| C. ERA5 `ssrd` from a public mirror (the hazards upstream is already moving downloads from CDS to AWS Open Data) | 1940- | medium; **availability of `ssrd` on the mirror unverified** | same reanalysis family |
| D. estimate from the diurnal range (Hargreaves-Samani: Rs = kRs·√(Tmax−Tmin)·Ra) | any | none | CHIRTS-only, but less accurate, and it changes NDWS relative to NEX-GDDP |

**Recommendation: A for v1.** It fully covers 1991-2020 and 1995-2014. Decide B or C only when a
post-2022 baseline is wanted.

---

## 2. The pipeline

Each stage is one script, idempotent, with `--smoke` (a Kenya box) and `--full`, timestamped logging
(as in `R/observational/`).

| stage | does | output |
|---|---|---|
| **S1 ingest** | download and crop to Africa; aggregate to the **0.25° hazard grid** (`metadata/base_rast_nexgddp.tif`). CHIRPS uses the native `p25`; CHIRTS-ERA5 0.05° → mean over 5×5; AgERA5 resampled. | daily cubes, one netCDF/zarr per variable-year (about 140 MB each at 0.25°), not one tif per day |
| **S2 daily → monthly index** | **reuse the upstream index code unchanged**: `calc_NTx.R`, `fast_calc_NDWS.R`/`NDWL0.R` with `wbkernel` and the AVAIL fix, `calc_THI.R`, `calc_PTOT.R`, `calc_NDD.R`, with input roots parameterised | `<IDX>-YYYY-MM.tif`, the same layout as NEX-GDDP step 0. That makes observed vs NEX-GDDP a like-for-like comparison. |
| **S3 monthly → per-season value, per crop window** | crops: the **GGCMI Phase 3 rainfed calendar of that crop** (20 on the node; maize only today). Perennials: 12 months. Livestock: 12 months, or a rangeland season later. Same stats as now (`haz_metadata.csv`). | one stack per index × window, one layer per season |
| **S4 classify + encode** | fixed thresholds (`haz_classes.csv`), then **per season**: `code = dry·1 + heat·2 + wet·4` as `uint8` | **one code stack per crop-window × severity, one layer per season. Saved.** This is the step the current chain does not keep. |
| **S5 frequencies** | class frequencies (`none`, `dry`, `heat`, `dry+heat`, …), `any`, plus single-hazard marginals; written with R/3 `_int` naming | rasters R/3 §4 reads unchanged |
| **S6 × exposure** | `R/3_freq_x_exposure.R` §4 as is (all five tiers) | admin parquets, same schema |

What this removes relative to R/1-R/2:
- the single maize window;
- the in-memory-only combination code;
- the classification of every threshold any crop uses (S4 classifies per crop-window only);
- the per-GCM × scenario fan-out (one observed series).

Run-length (consecutive failed seasons) and mixed-severity classes become cheap reads of the S4
stacks.

---

## 3. v1 variable set

| | heat | drought | wet |
|---|---|---|---|
| **crops** | `NTx35` (generic). Crop-specific thresholds wait for the #24/#25 agronomy review, because EcoCrop `Topt,max` gives perennials 0-63 % depending on the family. | `NDWS` on each crop's own window (needs radiation, option A) | `NDWL0` (optional in v1) |
| **livestock** | `THI` with Tmax + RH (RH source to settle; THI on Tmax with daily-mean RH overstates; see #48) | 12-month SPEI-12, or 12-month `NDWS` with **percentile** classes, masked to grazing systems (GLPS on the node, legend to source) | — |

**Fallback if radiation is deferred:** SPEI-3 over the crop window as the crop drought axis. It is
already built. Its classes are standardised to 1991-2020, so it cannot be compared with NEX-GDDP's
absolute classes.

---

## 4. Decisions needed

1. **Grid.** 0.25°, comparable with NEX-GDDP and about 25× cheaper (recommended for v1), or 0.05°.
2. **Baseline years.** 1995-2014 (matches CMIP6 historical) **and** 1991-2020 (WMO) are both covered by
   AgERA5. A recent window (2005-2024) needs radiation past 2022.
3. **Radiation source:** A now; B or C for post-2022.
4. **Day-count aggregation.** Keep "mean of monthly counts" (comparable with NEX-GDDP), or switch to
   days per season (more interpretable, but breaks comparability and needs new thresholds). Keep it for
   v1.
5. **Livestock drought:** SPEI-12 or `NDWS` with percentile classes, tied to the #31 validation.

## 5. Effort (estimates, one analyst plus node time)

| piece | estimate |
|---|---|
| S1 ingest (the long pole is about 600-700 GB of global 0.05° CHIRTS-ERA5 daily for 1991-2024 × Tmax/Tmin, cropped on the fly; CHIRPS `p25` is small) | about 1 week, mostly unattended |
| S2 parameterise upstream index scripts for observed inputs + smoke | about 1 week |
| S3-S5 new scripts + fixtures | about 1.5 weeks |
| S6 + gates (G6b against the exposure reference) | a few days |
| **first crops baseline (NTx35 × NDWS, 1995-2014 and 1991-2020)** | **about 4 weeks** |
| livestock THI | +1 week once the RH source is settled |
