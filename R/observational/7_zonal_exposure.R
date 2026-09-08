# 7_zonal_exposure.R
# -----------------------------------------------------------------------------
# Pre-cook the flood x exposure intersect as small per-adm2 parquet tables, so the
# KE-ENSO notebook reads tiny tables (DuckDB-WASM) instead of doing 53 MB-grid /
# 100 m-pop / 111 m-flood math client-side. Request: KE-ENSO dispatch
# 2026-09-01_request-precooked-exposure-tables.md. Zonal unit = IEBC adm2 (290),
# keyed adm2_pcode (+ adm1_pcode, names).
#
# Products (-> Data/exposure/intersect/, then publish tier 16):
#   A exposure_gfm_seasonal.parquet  one row per adm2 x season x year (GFM observed flood)
#   B exposure_jrc_rp.parquet        one row per adm2 x return-period (JRC modelled hazard)
#     exposure_totals.parquet        one row per adm2 (denominators, static)
#
# Metrics (per adm2 x scenario):
#   flooded_km2 / flood_prone_km2, observed_pct (GFM SAR coverage from nobs, 1.0 for JRC),
#   flooded_pct_observed, pop_exposed + pop_pct (WorldPop constrained; pop_source col),
#   health_n_exposed, schools_n_exposed, roads_km_exposed, grid_km_exposed (+ _hv 132/220 kV).
# Exposure rule: raster cell / asset geometry intersecting the flood mask
#   (GFM: flooded==1; JRC: depth>0). Pop is pixel-sum weighted (mask resampled to the pop grid).
#
# SMOKE (validate + TIME the heavy line-intersect before the full ~100-raster run):
#   SMOKE_ZONAL=1 Rscript R/observational/7_zonal_exposure.R   # 1 GFM season + 1 JRC RP -> intersect_smoke/
# FULL:
#   Rscript R/observational/7_zonal_exposure.R                 # all GFM seasons + all JRC RPs
# Publish: R/observational/6_publish_obs_to_s3.R --full --tier 16
# -----------------------------------------------------------------------------
t0 <- Sys.time()
log_step <- function(m) cat(format(Sys.time(), "[%H:%M:%S] "),
                            sprintf("(+%5.1fs) ", as.numeric(difftime(Sys.time(), t0, units = "secs"))),
                            m, "\n", sep = "")

suppressPackageStartupMessages({
  library(terra); library(sf); library(data.table); library(arrow)
})
sf_use_s2(FALSE)  # planar ops on the small Kenya extent; avoids s2 edge errors on clipped vectors

project_dir <- if (nzchar(Sys.getenv("project_dir"))) Sys.getenv("project_dir") else getwd()
source(file.path(project_dir, "R", "0_server_setup.R"))

SMOKE <- Sys.getenv("SMOKE_ZONAL") == "1"
KM2_PER_M2 <- 1e-6

# ---- paths (mirror the tier local_dirs in 6_publish_obs_to_s3.R) --------------
exp_root <- file.path(dirname(chirts_chirps_hist_dir), "exposure")
paths <- list(
  adm2   = file.path(exp_root, "admin_codab", "ken_adm2.geojson"),
  pop    = file.path(exp_root, "worldpop", "population_2020.tif"),
  roads  = file.path(exp_root, "osm_roads", "kenya_roads.geojson"),
  health = file.path(exp_root, "hotosm", "health.geojson"),
  schools= file.path(exp_root, "hotosm", "schools.geojson"),
  grid   = file.path(exp_root, "grid", "kenya_power_grid.geojson"),
  gfm_fl = file.path(exp_root, "gfm_flood", "seasonal", "flooded"),
  gfm_nb = file.path(exp_root, "gfm_flood", "seasonal", "nobs"),
  jrc    = file.path(dirname(chirts_chirps_hist_dir), "flood_jrc", "JRC")
)
out_dir <- file.path(exp_root, if (SMOKE) "intersect_smoke" else "intersect")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
for (p in c("adm2", "pop", "roads", "health", "schools", "grid")) {
  if (!file.exists(paths[[p]])) stop(sprintf("missing input %s: %s", p, paths[[p]]))
}

# ---- load adm2 + resolve pcode/name columns ---------------------------------
log_step("loading adm2 + exposure vectors")
adm2 <- st_read(paths$adm2, quiet = TRUE) |> st_make_valid()
cn <- tolower(names(adm2)); names(adm2) <- cn
pc2 <- grep("^adm2_pcode$", cn, value = TRUE); pc1 <- grep("^adm1_pcode$", cn, value = TRUE)
nm2 <- grep("^adm2_en$|^adm2_name$|^adm2nm", cn, value = TRUE)[1]
nm1 <- grep("^adm1_en$|^adm1_name$|^adm1nm", cn, value = TRUE)[1]
if (!length(pc2)) stop("adm2 has no adm2_pcode column")
adm2$adm2_pcode <- adm2[[pc2]]
adm2$adm1_pcode <- if (length(pc1)) adm2[[pc1]] else NA_character_
adm2$adm2_name  <- if (!is.na(nm2)) adm2[[nm2]] else adm2$adm2_pcode
adm2$adm1_name  <- if (!is.na(nm1)) adm2[[nm1]] else NA_character_
adm2 <- adm2[, c("adm2_pcode", "adm1_pcode", "adm2_name", "adm1_name")]
adm2$adm2_idx <- seq_len(nrow(adm2))
key_dt <- as.data.table(st_drop_geometry(adm2))
log_step(sprintf("  adm2 units: %d", nrow(adm2)))

roads  <- st_read(paths$roads,  quiet = TRUE) |> st_make_valid() |> st_transform(4326)
grid   <- st_read(paths$grid,   quiet = TRUE) |> st_make_valid() |> st_transform(4326)
# HOTOSM facilities are MIXED geometry (POINT + POLYGON/LINE building footprints) -> reduce each
# to one representative point (fixes vect() coercion on mixed sf; 1 point per facility semantics).
to_pts <- function(x) st_set_geometry(x, st_point_on_surface(st_geometry(st_make_valid(x))))
health <- st_read(paths$health, quiet = TRUE) |> st_transform(4326) |> to_pts()
schools<- st_read(paths$schools,quiet = TRUE) |> st_transform(4326) |> to_pts()
if (!"voltage_kv" %in% tolower(names(grid))) { names(grid) <- tolower(names(grid)) }

# spatial-join each vector to its adm2 once (centroid-in-adm2 for points; st_join for lines)
tag_adm2 <- function(x, pt = FALSE) {
  g <- if (pt) st_geometry(x) else st_point_on_surface(st_geometry(x))  # pt: points already; use their geometry (was `x` = whole sf -> empty join -> 0 rows)
  j <- st_join(st_sf(geometry = g), adm2["adm2_pcode"], join = st_within)
  x$adm2_pcode <- j$adm2_pcode
  x[!is.na(x$adm2_pcode), ]
}
roads  <- tag_adm2(roads);  grid <- tag_adm2(grid)
health <- tag_adm2(health, pt = TRUE); schools <- tag_adm2(schools, pt = TRUE)

# ---- population (grid = pop 100 m) + adm2 rasterized to each grid ------------
log_step("rasterizing adm2 to pop grid + computing totals")
pop <- rast(paths$pop)
adm2_v <- vect(adm2)
adm2_rast_pop <- rasterize(adm2_v, pop, field = "adm2_idx")
pop_by_idx <- terra::zonal(pop, adm2_rast_pop, "sum", na.rm = TRUE)
setDT(pop_by_idx); setnames(pop_by_idx, c("adm2_idx", "pop_total"))

# ---- static totals (denominators) -------------------------------------------
line_km <- function(x) as.numeric(sum(st_length(x))) / 1000
totals <- key_dt[, .(adm2_pcode, adm1_pcode, adm2_name, adm1_name, adm2_idx)]
totals <- pop_by_idx[totals, on = "adm2_idx"]
area_by_idx <- terra::zonal(cellSize(pop, unit = "km"), adm2_rast_pop, "sum", na.rm = TRUE)
setDT(area_by_idx); setnames(area_by_idx, c("adm2_idx", "area_km2"))
totals <- area_by_idx[totals, on = "adm2_idx"]
rd_tot <- as.data.table(roads)[, .(roads_km_total = line_km(.SD$geometry)), by = adm2_pcode]
gr_tot <- as.data.table(grid)[,  .(grid_km_total  = line_km(.SD$geometry)), by = adm2_pcode]
he_tot <- as.data.table(st_drop_geometry(health))[,  .(health_n_total  = .N), by = adm2_pcode]
sc_tot <- as.data.table(st_drop_geometry(schools))[, .(schools_n_total = .N), by = adm2_pcode]
for (t in list(rd_tot, gr_tot, he_tot, sc_tot)) totals <- t[totals, on = "adm2_pcode"]
num0 <- function(x) fifelse(is.na(x), 0, as.numeric(x))
for (c in c("roads_km_total","grid_km_total","health_n_total","schools_n_total","pop_total","area_km2"))
  totals[[c]] <- num0(totals[[c]])
log_step(sprintf("  totals: pop %.0f, roads %.0f km, grid %.0f km, health %d, schools %d",
                 sum(totals$pop_total), sum(totals$roads_km_total), sum(totals$grid_km_total),
                 sum(totals$health_n_total), sum(totals$schools_n_total)))

# ---- zonal engine: one flood raster -> per-adm2 metrics ----------------------
# mask_flooded: SpatRaster logical TRUE where the asset is exposed.
# obs_rast: SpatRaster logical TRUE where observed (GFM: value!=255; JRC: always TRUE), or NULL.
# lines: rasterised length (rasterizeGeom length-per-cell, precomputed ONCE per grid) x flood mask,
# zonal-summed by adm2 — turns the hours-long vector st_intersection into a seconds-long raster op
# (cell-resolution approx: a line in a flooded cell counts its full in-cell length). len_* are km/cell.
zonal_one <- function(flood, mask_flooded, obs_rast, adm2_rast, csize,
                      len_roads, len_grid, len_grid_hv) {
  zsum <- function(x, nm) { z <- terra::zonal(x, adm2_rast, "sum", na.rm = TRUE); setDT(z); setnames(z, c("adm2_idx", nm)); z }
  fa <- zsum(csize * mask_flooded, "flooded_km2")
  oa <- if (!is.null(obs_rast)) zsum(csize * obs_rast, "observed_km2") else NULL
  # pop-weighted exposure (mask resampled to pop grid, nearest)
  mpop <- resample(mask_flooded, pop, method = "near")
  pe   <- terra::zonal(pop * mpop, adm2_rast_pop, "sum", na.rm = TRUE)
  setDT(pe); setnames(pe, c("adm2_idx", "pop_exposed"))
  # lines: exposed length = per-cell line length where flooded
  rd <- zsum(len_roads   * mask_flooded, "roads_km_exposed")
  gr <- zsum(len_grid    * mask_flooded, "grid_km_exposed")
  gh <- zsum(len_grid_hv * mask_flooded, "grid_km_exposed_hv")
  # facilities: point in flooded cell (keyed on adm2_pcode)
  in_flood_pts <- function(pts) {
    if (!nrow(pts)) return(data.table(adm2_pcode = character(), n = integer()))
    v <- terra::extract(mask_flooded, vect(pts))[, 2]
    d <- data.table(adm2_pcode = pts$adm2_pcode, hit = !is.na(v) & v > 0)
    d[hit == TRUE, .(n = .N), by = adm2_pcode]
  }
  he <- in_flood_pts(health); sc <- in_flood_pts(schools)

  # assemble on the full adm2 key (raster metrics on adm2_idx; facilities on adm2_pcode)
  r <- copy(key_dt)
  for (z in list(fa, oa, pe, rd, gr, gh)) if (!is.null(z)) r <- z[r, on = "adm2_idx"]
  r[he, health_n_exposed := i.n, on = "adm2_pcode"]
  r[sc, schools_n_exposed := i.n, on = "adm2_pcode"]
  for (c in c("flooded_km2","observed_km2","pop_exposed","health_n_exposed",
              "schools_n_exposed","roads_km_exposed","grid_km_exposed","grid_km_exposed_hv"))
    if (c %in% names(r)) r[[c]] <- num0(r[[c]])
  r
}

parse_gfm <- function(f) { b <- sub("\\.tif$", "", basename(f)); list(season = sub("_.*$", "", b), year = as.integer(sub("^.*_", "", b))) }

# line geometries as SpatVector (once) for rasterizeGeom length-per-cell
roads_v   <- vect(roads)
grid_v    <- vect(grid)
grid_hv_v <- vect(grid[which(grid$voltage_kv >= 132), ])   # which() drops NA-voltage rows

# ---- A. GFM observed flood: adm2 x season x year -----------------------------
gfm_files <- sort(list.files(paths$gfm_fl, "\\.tif$", full.names = TRUE))
if (SMOKE) gfm_files <- gfm_files[1]
log_step(sprintf("A. GFM seasonal: %d rasters", length(gfm_files)))
adm2_rast_gfm <- NULL; csize_gfm <- NULL; len_r_gfm <- len_g_gfm <- len_gh_gfm <- NULL
A <- vector("list", length(gfm_files))
for (i in seq_along(gfm_files)) {
  f <- gfm_files[i]; meta <- parse_gfm(f)
  flood <- rast(f)
  if (is.null(adm2_rast_gfm)) {
    adm2_rast_gfm <- rasterize(adm2_v, flood, field = "adm2_idx"); csize_gfm <- cellSize(flood, unit = "km")
    len_r_gfm  <- rasterizeGeom(roads_v,   flood, "length", "km")
    len_g_gfm  <- rasterizeGeom(grid_v,    flood, "length", "km")
    len_gh_gfm <- rasterizeGeom(grid_hv_v, flood, "length", "km")
    log_step("  precomputed GFM-grid line-length rasters (roads/grid/grid_hv)")
  }
  mask_flooded <- flood == 1
  obs_rast <- flood != 255
  r <- zonal_one(flood, mask_flooded, obs_rast, adm2_rast_gfm, csize_gfm, len_r_gfm, len_g_gfm, len_gh_gfm)
  r[, `:=`(season = meta$season, year = meta$year)]
  A[[i]] <- r
  log_step(sprintf("  [%d/%d] %s_%d: flooded %.0f km2, pop_exp %.0f",
                   i, length(gfm_files), meta$season, meta$year, sum(r$flooded_km2), sum(r$pop_exposed)))
}
A <- rbindlist(A)
A <- totals[, .(adm2_pcode, area_km2, pop_total)][A, on = "adm2_pcode"]
A[, `:=`(observed_pct = pmin(fifelse(area_km2 > 0, observed_km2 / area_km2, NA_real_), 1),  # clamp grid-mismatch rounding
         flooded_pct_observed = pmin(fifelse(observed_km2 > 0, flooded_km2 / observed_km2, NA_real_), 1),
         pop_pct = fifelse(pop_total > 0, pop_exposed / pop_total, NA_real_),
         pop_source = "worldpop")]
Acols <- c("adm2_pcode","adm1_pcode","adm2_name","adm1_name","season","year",
           "flooded_km2","observed_pct","flooded_pct_observed","pop_exposed","pop_pct","pop_source",
           "roads_km_exposed","health_n_exposed","schools_n_exposed","grid_km_exposed","grid_km_exposed_hv")
A <- A[, ..Acols]

# ---- B. JRC modelled hazard: adm2 x return-period ----------------------------
jrc_files <- sort(list.files(paths$jrc, "flood-depth_rp[0-9]+\\.tif$", full.names = TRUE))
if (SMOKE) jrc_files <- grep("rp100", jrc_files, value = TRUE)[1]
log_step(sprintf("B. JRC return-period: %d rasters", length(jrc_files)))
adm2_rast_jrc <- NULL; csize_jrc <- NULL; len_r_jrc <- len_g_jrc <- len_gh_jrc <- NULL
B <- vector("list", length(jrc_files))
for (i in seq_along(jrc_files)) {
  f <- jrc_files[i]; rp <- as.integer(sub(".*_rp([0-9]+)\\.tif$", "\\1", basename(f)))
  depth <- rast(f)
  if (is.null(adm2_rast_jrc)) {
    adm2_rast_jrc <- rasterize(adm2_v, depth, field = "adm2_idx"); csize_jrc <- cellSize(depth, unit = "km")
    len_r_jrc  <- rasterizeGeom(roads_v,   depth, "length", "km")
    len_g_jrc  <- rasterizeGeom(grid_v,    depth, "length", "km")
    len_gh_jrc <- rasterizeGeom(grid_hv_v, depth, "length", "km")
    log_step("  precomputed JRC-grid line-length rasters (roads/grid/grid_hv)")
  }
  mask_prone <- depth > 0
  r <- zonal_one(depth, mask_prone, NULL, adm2_rast_jrc, csize_jrc, len_r_jrc, len_g_jrc, len_gh_jrc)
  setnames(r, "flooded_km2", "flood_prone_km2")
  r[, rp := rp]
  B[[i]] <- r
  log_step(sprintf("  [%d/%d] rp%d: prone %.0f km2, pop_exp %.0f",
                   i, length(jrc_files), rp, sum(r$flood_prone_km2), sum(r$pop_exposed)))
}
B <- rbindlist(B)
B <- totals[, .(adm2_pcode, pop_total)][B, on = "adm2_pcode"]
B[, `:=`(pop_pct = fifelse(pop_total > 0, pop_exposed / pop_total, NA_real_), pop_source = "worldpop")]
Bcols <- c("adm2_pcode","adm1_pcode","adm2_name","adm1_name","rp",
           "flood_prone_km2","pop_exposed","pop_pct","pop_source",
           "roads_km_exposed","health_n_exposed","schools_n_exposed","grid_km_exposed","grid_km_exposed_hv")
B <- B[, ..Bcols]

# ---- write parquet -----------------------------------------------------------
write_parquet(A,      file.path(out_dir, "exposure_gfm_seasonal.parquet"))
write_parquet(B,      file.path(out_dir, "exposure_jrc_rp.parquet"))
write_parquet(totals[, !"adm2_idx"], file.path(out_dir, "exposure_totals.parquet"))
log_step(sprintf("WROTE -> %s  (A %d rows, B %d rows, totals %d rows)%s",
                 out_dir, nrow(A), nrow(B), nrow(totals), if (SMOKE) "  [SMOKE]" else ""))
if (SMOKE) { cat("\n--- A head ---\n"); print(head(A, 3)); cat("\n--- B head ---\n"); print(head(B, 3)) }
cat("\nNext: publish with  Rscript R/observational/6_publish_obs_to_s3.R --full --tier 16\n")
