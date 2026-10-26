# Zambia: admin2 and gridded climate-hazard downloads from the Atlas S3 bucket

*Topic briefing for Shalika Vyas and Pedro (Zambia analysis). Written 2026-10-26. Everything below was
checked against the live bucket on that date: all keys read anonymously over HTTPS, no credentials needed.*

## Why the Atlas stops at admin1

The Atlas climate view reads one product, `ensemble_season_timeseries.parquet`, which is built at
admin0 and admin1 only. There is no admin2 in that table, so the UI cannot go further. Admin2 lives
in a different product, described in section 1.

All products are Africa-wide. Filter to Zambia with `iso3 = 'ZMB'`.

## 1. Admin2 tables (current, October 2026 bake)

Hazard frequency × exposure, zonal-summed to GAUL 2024 admin2 (115 admin2 units for Zambia), with
admin1 and admin0 totals in the same file (rows with `admin2_name IS NULL`).

What is in it:

| field | values |
|---|---|
| heat index | `NTx35` (days above 35 °C, generic), `NTxM` (crop-specific maize thresholds), `THI-max` (livestock) |
| other hazards | `NDWS` (dry), `NDWL0` / `PTOT-G` (wet), `PTOT-L` (low rainfall) |
| `hazard_vars` | the dry+heat+wet set, e.g. `NDWS+NTxM+NDWL0`, `PTOT-L+THI-max+PTOT-G` |
| `hazard` | `any`, `none`, `dry`, `heat`, `wet`, `dry+heat`, `dry+wet`, `heat+wet`, `dry+heat+wet` |
| crops | 44 crops and livestock classes (`crop` column) |
| scenarios | historic 1995-2014; ssp126/245/370/585 × 2021-2040, 2041-2060, 2061-2080, 2081-2100 |
| model | `ENSEMBLEmean` (18 NEX-GDDP-CMIP6 GCMs); `ENSEMBLEsd` also published |
| severity | `severe`, `moderate`, `extreme` (threshold tier) |

Key pattern (one file per exposure variable × severity):

```
https://digital-atlas.s3.amazonaws.com/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=<v>/period=annual/model=ENSEMBLEmean/severity=<severe|moderate|extreme>/int=multi-hazard.parquet
```

`<v>` is one of:

| `<v>` | exposure unit |
|---|---|
| `vop_nominal-usd21` | value of production, nominal USD 2021 |
| `vop_intld15-21` | value of production, constant 2015 international dollars |
| `harv-area_ha` | harvested area, ha |
| `prod_t` | production, tonnes |
| `head_n` | livestock head count |

Each parquet has a `.json` sidecar next to it (same key + `.json`) describing columns, thresholds
and ensemble members.

**Caveat.** `value` is exposure-weighted: e.g. USD of maize production in pixels whose hazard
frequency falls in the severity tier. It is not the raw index. To get the share of value at risk
from heat for an admin2 × crop, divide the `hazard = 'heat'` row by the `any` + `none` rows.

Pull Zambia with DuckDB (CLI, R `duckdb`, or Python `duckdb`):

```sql
INSTALL httpfs; LOAD httpfs;
SELECT *
FROM read_parquet('s3://digital-atlas/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=annual/model=ENSEMBLEmean/severity=severe/int=multi-hazard.parquet',
                  hive_partitioning = false)
WHERE iso3 = 'ZMB'
  AND admin2_name IS NOT NULL
  AND hazard_vars = 'NDWS+NTxM+NDWL0';
```

Or in R with arrow:

```r
library(arrow)
u <- "https://digital-atlas.s3.amazonaws.com/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=annual/model=ENSEMBLEmean/severity=severe/int=multi-hazard.parquet"
download.file(u, "zmb_haz.parquet", mode = "wb")
d <- read_parquet("zmb_haz.parquet")
d <- d[d$iso3 == "ZMB" & !is.na(d$admin2_name), ]
```

## 2. Gridded GeoTIFFs (0.25°, Africa-wide, 2024 bake)

Ensemble-mean index values and threshold-exceedance frequencies per scenario × timeframe:

```
https://digital-atlas.s3.amazonaws.com/hazards/hazard_timeseries_mean/annual/historical_NTx35_mean.tif
https://digital-atlas.s3.amazonaws.com/hazards/hazard_timeseries_mean/annual/ssp245_ENSEMBLEmean_2021-2040_NTx35_mean.tif
https://digital-atlas.s3.amazonaws.com/hazards/hazard_timeseries_risk/annual/ssp245_ENSEMBLEmean_2021-2040_NTx35_mean_G14.tif
```

- `hazard_timeseries_mean/` = mean index value over the window (e.g. mean annual NTx35 days).
- `hazard_timeseries_risk/` = fraction of years exceeding the threshold in the suffix (`G14`
  = more than 14 days; `G27`/`G32`/`G41` for HSH; `G20` etc. for NDWS).
- Index tokens: `NTx35_mean`, `NTx40_mean`, `THI_max-max`, `THI_mean-mean`, `HSH_max-max`,
  `HSH_mean-mean`, `NDWS_mean`, `NDWL0_mean`, `PTOT_sum`, `TAI_mean`, `TAVG_mean`.
- Windows: `historical_<IDX>.tif` (1995-2014), and `ssp245` / `ssp585` × `2021-2040`, `2041-2060`
  as `<ssp>_ENSEMBLEmean_<window>_<IDX>.tif`. No ssp126/370, no 2061+ in this set.
- Swap `annual` for `jagermeyr` to get crop-season (Jägermeyr calendar) windows.

The same folder holds raw-index admin2 means already computed from these rasters (wide table,
one column per scenario × window × index, GAUL admin2, Africa-wide):

```
https://digital-atlas.s3.amazonaws.com/hazards/hazard_timeseries_mean/annual/haz_means_adm2.parquet
```

It is GeoParquet; in DuckDB run `SET enable_geoparquet_conversion = false;` before reading.
`haz_means_adm1.parquet` and `haz_means_adm0.parquet` sit beside it.

Read a raster in place and clip:

```r
library(terra)
r <- rast("/vsicurl/https://digital-atlas.s3.amazonaws.com/hazards/hazard_timeseries_mean/annual/ssp245_ENSEMBLEmean_2021-2040_NTx35_mean.tif")
zmb <- geodata::gadm("ZMB", level = 2, path = tempdir())
r_zmb <- mask(crop(r, zmb), zmb)
adm2 <- zonal(r_zmb, zmb, fun = "mean")   # raw-index admin2 means
```

**Caveat.** Everything in this section is the 2024 bake. It predates the ensemble and threshold
fixes behind the section 1 tables, so treat it as indicative; admin2 means from it will not
reconcile exactly with section 1. The current-vintage rasters are not yet published.

Per-GCM monthly indices (large) are under
`https://digital-atlas.s3.amazonaws.com/hazards/cmip6_indices/<scenario>_<gcm>_<y1>_<y2>/<INDEX>/`.

## 3. Observed (CHIRPS v3 / CHIRTS-ERA5), monthly COGs

Africa-wide monthly `PTOT`, `SPEI-03`, `SPEI-12`:

```
https://digital-atlas.s3.amazonaws.com/domain=climate/type=observational/source=chirps-chirts-era5/region=africa/processing=monthly/variable=<PTOT|SPEI-03|SPEI-12>/<VAR>-YYYY-MM.tif
```

Admin0/admin1 monthly and period summaries sit beside them under `processing=admin-monthly/` and
`processing=admin-periods/`. No observed admin2 product is published.

## 4. If you need something not here

Current-vintage gridded heat layers clipped to Zambia, or raw-index (not exposure-weighted) admin2
values, can be cut from the processing server on request: say which indices, scenarios and windows.
