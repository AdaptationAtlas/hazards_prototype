# Please run 0_server_setup.R before executing this script
# To generate crop & livestock vop you will need to run scripts 0.4.1 & 0.4.2
# a) Load R functions & packages ####
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))

# List of packages to be loaded
packages <- c("terra",
              "data.table",
              "exactextractr",
              "arrow",
              "geoarrow",
              "pbapply",
              "future",
              "future.apply",
              "furrr",
              "progressr")


pacman::p_load(packages,character.only=T)

# v9: bump GDAL block-cache budget so MapSPAM + GLW extraction doesn't
# spill to disk under parallel load. Same 60000 MB budget used by the
# observational pipeline.
terra::gdalCache(60000)

# Tiny progress logger so the run shows clear checkpoints in the log.
# Mirrors .log041() in 0.4.1_create_livestock_exposure.R.
.log044 <- function(msg) {
  cat(sprintf("[%s] [0.4.4] %s\n",
              format(Sys.time(), "%H:%M:%S"), msg))
  flush.console()
}
.log044(sprintf("script start (FORCE_OVERWRITE=%s -> overwrite=%s)",
                Sys.getenv("FORCE_OVERWRITE", "<unset>"),
                atlas_env_flag("FORCE_OVERWRITE", strict = TRUE)))

# b) Load functions & wrappers ####
source(url("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/R/haz_functions.R"))
# Shared parquet-writer for DuckDB-WASM pushdown (multi-row-group + stats).
# Sourced after 0_server_setup.R so project_dir is defined.
pacman::p_load(DBI, duckdb)
source(file.path(project_dir, "R", "_helpers.R"))


# 0) Load and prepare admin rasters ####
.log044("section 0: loading admin rasters + geographies")
  ## 0.0) Zonal base raster ####
  # Issue #30, p.steward 2026-09-23. This script extracts EXPOSURE rasters
  # (MapSPAM 0.05deg, GLW 0.083deg) onto admin units. It has no reason to use
  # the HAZARD grid, and under climdat_source = nexgddp `base_rast_path` IS the
  # hazard grid (0.25deg). Rasterising GAUL24 at 0.25deg loses about a third of
  # adm2 polygons and 16 adm1 polygons - they never own a cell, so they never
  # get a row (node 4,342 adm2 vs live 6,482 per crop x tech). The live
  # crop-livestock_all was built at 0.05deg (main, atlas_delta base).
  #
  # So the zonal grid is fixed at 0.05deg here regardless of climdat_source,
  # defaulting to the atlas_delta base raster the live object was built on.
  # EXPOSURE_ZONAL_BASE_RAST overrides it for a deliberate experiment. Inputs
  # that 0.4.0/0.4.1 already resampled to 0.25deg are sum-resampled back up in
  # sections 1/2: mass-conserving, area-weighted within each 0.25deg cell, so
  # their sub-national split is coarser than the grid suggests. Nominal-USD
  # (0.4.2) and prod/area inputs are native 0.05deg. R/3 stays on the hazard grid.
  # p.steward 2026-09-23: the exposure tables exist at BOTH resolutions, with
  # the resolution explicit in every output name. One resolution per
  # invocation, chosen by EXPOSURE_ZONAL_RES = 0.05 | 0.25. No default: a run
  # that does not say which grid it is on is how the live 0.05deg reference
  # and the 0.25deg product came to disagree without anyone noticing.
  #   0.05 -> metadata/base_raster.tif        (Atlas exposure grid; live reference)
  #   0.25 -> metadata/base_rast_nexgddp.tif  (NEX-GDDP hazard grid; R/3)
  # EXPOSURE_ZONAL_BASE_RAST overrides the raster for a deliberate experiment.
  # Suffix follows the Atlas precedent gaul24_a{level}_res-05.tif: res-05, res-25.
  .eg <- exposure_grid(caller = "0.4.4 section 0")   # EXPOSURE_RES (or EXPOSURE_ZONAL_RES) = 0.05 | 0.25, required
  zonal_base_path <- .eg$path; base_rast <- .eg$rast; .zonal_res_deg <- .eg$res_deg; .zonal_res_tag <- .eg$tag
  .log044(sprintf("section 0: zonal base = %s | res %s | ext %s | tag %s",
                  zonal_base_path, paste(signif(terra::res(base_rast), 4), collapse = "x"),
                  paste(signif(as.vector(terra::ext(base_rast)), 6), collapse = ","), .zonal_res_tag))
  if (exists("base_rast_path") && !isTRUE(all.equal(terra::res(terra::rast(base_rast_path)), terra::res(base_rast)))) {
    .log044(sprintf("section 0: NOTE hazard grid (base_rast_path) is %s deg; this script's zonal grid is %s deg by design",
                    signif(terra::res(terra::rast(base_rast_path))[1], 4), signif(terra::res(base_rast)[1], 4)))
  }

  ## 0.1) Geographies #####
overwrite_boundary_zones<-T

Geographies<-lapply(1:length(geo_files_local),FUN=function(i){
  file<-geo_files_local[i]
  data<-arrow::open_dataset(file)
  data <- data |> sf::st_as_sf() |> terra::vect()
  data$zone_id <- ifelse(!is.na(data$gaul2_code), data$gaul2_code,
                         ifelse(!is.na(data$gaul1_code), data$gaul1_code, data$gaul0_code))        
  data
})
names(Geographies)<-names(geo_files_local)

base_rast<-terra::rast(zonal_base_path)+0

# The zonal rasters are cached under a RESOLUTION-TAGGED name. The plain
# `<name>_zonal.tif` in boundaries_int_dir is the HAZARD-grid cache shared with
# R/2.1, R/2.2, R/3 and R/3.1 (all 0.25deg under nexgddp); writing a 0.05deg
# raster there would silently hand them the wrong grid.
boundaries_zonal<-lapply(1:length(Geographies),FUN=function(i){
  file_path<-file.path(boundaries_int_dir,paste0(names(Geographies)[i],"_zonal_",.zonal_res_tag,".tif"))
  if(!file.exists(file_path)|overwrite_boundary_zones==T){
    zones<-Geographies[[i]]
    zone_rast <- rasterize(
      x      = zones, 
      y      = base_rast, 
      field  = "zone_id", 
      background = NA,    # cells not covered by any polygon become NA
      touches    = TRUE   # optional: count cells touched by polygon boundaries
    )
    terra::writeRaster(zone_rast,file_path,overwrite=T)
  }
  file_path
})
names(boundaries_zonal)<-names(Geographies)

boundaries_index<-lapply(1:length(Geographies),FUN=function(i){
  data.frame(Geographies[[i]])[,c("iso3","admin0_name","admin1_name","admin2_name","zone_id", "gaul0_code", "gaul1_code", "gaul2_code")]
})

names(boundaries_index)<-names(Geographies)

# 1) Crop (MapSPAM) extraction by vector boundaries #####
# Set FORCE_OVERWRITE=1 in env to force regen of all gated outputs.
# Used by the issue #9 rebake runbook so the v9 mass-conserving fix
# actually lands in every parquet.
overwrite_spam <- atlas_env_flag("FORCE_OVERWRITE", strict = TRUE)
version_spam<-1
source_year_spam<-list(spam_year=2020,fao_price="varies")

files<-list.files(mapspam_pro_dir,".tif$",recursive=T,full.names=T)
files<-grep("variable",files,value=T)
# Both resolutions of the exposure rasters sit side by side (suffix res-05 / res-25).
# Keep this grid's files plus untagged native ones (prod_t, harv-area, ...); never the other grid's.
.n_files0 <- length(files); .files_all <- files   # pre-filter listing, for the twin check below
files <- files[!grepl("_res-[0-9]{2}\\.tif$", files) | grepl(paste0("_", .zonal_res_tag, "\\.tif$"), files)]
.log044(sprintf("section 1: %d tifs -> %d after keeping %s + untagged", .n_files0, length(files), .zonal_res_tag))
# A legacy UNTAGGED twin of a tagged raster (pre-2026-09-23 output left on disk) would be
# extracted as a second "native" copy and double the rows. Refuse; move it aside.
.twins <- intersect(sub("_res-[0-9]{2}\\.tif$", ".tif", .files_all[grepl("_res-[0-9]{2}\\.tif$", .files_all)]), .files_all[!grepl("_res-[0-9]{2}\\.tif$", .files_all)])   # ANY tagged twin, either grid
if (length(.twins)) stop("section 1: untagged legacy twin(s) of tagged rasters present - move aside before extracting:\n  ", paste(basename(.twins), collapse = "\n  "))
# Remove yield (one reason for this is that stat<-"mean" returns NA and needs debugging)
files<-files[!grepl("yield",files)]

field_descriptions <- data.table::data.table(
  field_name = c(
    "iso3", "admin0_name", "admin1_name", "admin2_name",
    "gaul0_code", "gaul1_code", "gaul2_code", "crop",
    "value", "stat", "exposure", "unit", "tech"
  ),
  type = c(
    "character", "character", "character", "character",
    "numeric", "numeric", "numeric", "factor",
    "numeric", "character", "character", "character", "character"
  ),
  description = c(
    "ISO 3166-1 alpha-3 country code (e.g., 'KEN' for Kenya).",
    "National-level administrative name (admin0), typically the country.",
    "Subnational administrative name (admin1, e.g., province or state); may be NA.",
    "Local administrative name (admin2, e.g., district); may be NA.",
    "GAUL (Global Administrative Unit Layer) code for admin0.",
    "GAUL code for admin1; may be NA.",
    "GAUL code for admin2; may be NA.",
    "Crop or system name (e.g., 'maize', 'rest of crops'); may include livestock categories.",
    "Extracted exposure value (e.g., harvested area, production); units defined in `unit`.",
    "Statistic used for zonal summary (e.g., 'sum', 'mean').",
    "Type of exposure indicator (e.g., 'harv-area', 'vop').",
    "Measurement unit of the value (e.g., 'ha' for hectares, 'usd15' for USD 2015).",
    "MapSPAM production system category (e.g., 'all', 'rainfed'); not applicable for livestock."
  )
)

# v9: parallelize MapSPAM extraction across files. Each file -> own
# admin_extract_wrap call -> own output parquet + attr JSON. No
# contention. Conservative 8 workers to keep RAM headroom for the
# zonal vector raster.
spam_workers <- min(8L, length(files))
set_parallel_plan(n_cores = spam_workers, use_multisession = TRUE)
.log044(sprintf("section 1: MapSPAM extraction over %d files, %d workers",
                length(files), spam_workers))
.n_spam <- length(files)
spam_extracted <- rbindlist(furrr::future_map(seq_along(files), function(i) {
  file <- files[i]
  file_base <- gsub(".tif", "", basename(file))
  file_base <- sub("_res-[0-9]{2}$", "", file_base)   # tag re-applied by the cache name below
  cat(sprintf("[%s] [0.4.4] MapSPAM %d/%d %s\n",
              format(Sys.time(), "%H:%M:%S"),
              i, .n_spam, basename(file)))
  flush.console()
  var <- unlist(tstrsplit(file_base, "_", keep = 2))
  unit <- unlist(tstrsplit(file_base, "_", keep = 3))
  tech <- unlist(tstrsplit(file_base, "_", keep = 4))

  if (var == "yield") {
    stop("Use of stat == mean currently returns NA values. Remove yield from input data or debug error.")
    stat <- "mean"
  } else {
    stat <- "sum"
  }

  data <- terra::rast(file)
  # MapSPAM tifs ship at MapSPAM-native (10km global) extent;
  # boundaries_zonal is at base_rast (NEX-GDDP, Africa) extent. Defensive
  # resample so terra::zonal() inside admin_extract_wrap doesn't halt
  # with "[zonal] extents do not match". Use method="sum" so the area
  # totals are mass-conserved (issue #9 pattern).
  zone_rast_geom <- terra::rast(boundaries_zonal[[1]])
  if (!terra::compareGeom(data, zone_rast_geom, stopOnError = FALSE)) {
    data <- terra::resample(data, zone_rast_geom, method = "sum")
  }

  result <- admin_extract_wrap(data = data,
                               save_dir = dirname(file),
                               filename = paste0(file_base, "_", .zonal_res_tag),   # per-tif cache is resolution-specific
                               FUN = stat,
                               append_vals = c(exposure = var, unit = unit, tech = tech),
                               var_name = "crop",
                               round = 1,
                               boundaries_zonal = boundaries_zonal,
                               boundaries_index = boundaries_index,
                               overwrite = overwrite_spam)
  
  attr_file<-file.path(dirname(file),paste0(file_base,"_",.zonal_res_tag,"_adm_",stat,".parquet.json"))
  
  filter_colnames<-c("crop","stat","exposure","unit","tech")
  filters <- lapply(filter_colnames, function(split_col) {
    unique(result[[split_col]])
  })
  names(filters) <- filter_colnames
  
  if(file.exists(attr_file)){
    attr_dat<-jsonlite::read_json(attr_file)
    date_created<-unlist(attr_dat$date_created)
    version_attr<-unlist(attr_dat$version)
    if(overwrite_spam|version_attr!=version_spam){
      date_created<-Sys.time()
      update_attr_flag<-T
    }else{
      update_attr_flag<-F
    }
  }else{
    update_attr_flag<-T
    date_created<-Sys.time()
  }

  if(update_attr_flag){
  attr_info <- list(
    source = list(input_raster=atlas_data$mapspam_2020v1r2$name,extraction_vect=atlas_data$boundaries$name),
    source_year = list(input_raster=source_year_spam),
    date_created = date_created,
    field_descriptions = field_descriptions,
    filters = filters,
    version = version_spam,
    parent_script = "R/0.6_process_exposure.R",
    variable = var,
    unit = unit,
    technology = tech,
    stat=stat,
    notes = paste0("A table of mapspam crop values (",var,") extracted by boundary vectors then summarized (fun = ",stat,").")
  )
  
  write_json(attr_info, attr_file, pretty = TRUE)
  }
  
  return(result)

}, .options = furrr::furrr_options(seed = TRUE, stdout = FALSE)))
future::plan(future::sequential)

# 2) Livestock (GLW) extraction by vector boundaries #####
version_glw<-"glw4-2020_atlasv1"
overwrite_glw <- atlas_env_flag("FORCE_OVERWRITE", strict = TRUE)

# 0.4.1 writes its livestock outputs under glw2020_pro_dir
# (Data/GLW4_2020/processed), not glw_pro_dir (the 2015 GLW4 dir).
livestock_no_file <- file.path(glw2020_pro_dir, paste0("livestock_number_number_", .zonal_res_tag, ".tif"))   # 0.4.1 output for THIS grid
if (!file.exists(livestock_no_file)) {
  stop("Run script 0.4.1_create_livestock_exposure.R first")
}

files <- list.files(glw2020_pro_dir, ".tif$", recursive = TRUE, full.names = TRUE)
.n_files0 <- length(files)
# 0.4.1 is the ONLY producer under glw2020_pro_dir and every one of its outputs now
# carries a resolution tag, so an untagged tif here is always a legacy leftover
# (e.g. the old-layout variable=number_number/*.tif) and would be extracted as a
# second copy: Block H's res-05 table came out exactly one `number` block
# (87,036 rows) larger than live for this reason. Unlike section 1 there are no
# legitimate untagged native inputs here - keep THIS tag only, refuse anything else.
.untagged <- files[!grepl("_res-[0-9]{2}\\.tif$", files)]
if (length(.untagged)) stop("section 2: untagged legacy raster(s) under glw2020_pro_dir - 0.4.1 writes tagged files only; move these aside:\n  ",
                            paste(sub(paste0("^", glw2020_pro_dir, "/?"), "", .untagged), collapse = "\n  "))
files <- files[grepl(paste0("_", .zonal_res_tag, "\\.tif$"), files)]
.log044(sprintf("section 2: %d tifs -> %d tagged %s", .n_files0, length(files), .zonal_res_tag))
# Exactly one livestock-number raster, and it must be the one section 0's grid asked for.
.num <- files[grepl("number_number", basename(files))]
if (!identical(normalizePath(.num, mustWork = FALSE), normalizePath(livestock_no_file, mustWork = FALSE))) {
  stop("section 2: expected exactly one livestock number raster (", basename(livestock_no_file), "), found: ", paste(basename(.num), collapse = ", "))
}

# v9: parallel GLW extraction, mirroring the MapSPAM block above.
glw_workers <- min(8L, length(files))
set_parallel_plan(n_cores = glw_workers, use_multisession = TRUE)
.log044(sprintf("section 2: GLW extraction over %d files, %d workers",
                length(files), glw_workers))
.n_glw <- length(files)
glw_extracted <- rbindlist(furrr::future_map(seq_along(files), function(i) {
    file <- files[i]
    cat(sprintf("[%s] [0.4.4] GLW %d/%d %s\n",
                format(Sys.time(), "%H:%M:%S"),
                i, .n_glw, basename(file)))
    flush.console()

    if(grepl("number",file)){
      source_year_glw<-list(glw=2020)
    }else{
      source_year_glw<-list(glw=2020,faostat_vop=gsub(".tif","",tail(unlist(strsplit(basename(file),"-")),1)))
    }

    file_base<-gsub(".tif","",basename(file))
    file_base <- sub("_res-[0-9]{2}$", "", file_base)   # tag re-applied by the cache name below
    var<-unlist(tstrsplit(basename(file),"_",keep=2))
    unit<-gsub(".tif","",unlist(tstrsplit(basename(file),"_",keep=3)))
    tech<-NA

    stat<-"sum"
    
    data <- terra::rast(file)
    # Defensive: ensure GLW data is on the same grid as boundaries_zonal
    # (issue #9 / [zonal] extents do not match). 0.4.1 already resamples
    # to base_rast so this should be a no-op, but cheap to verify.
    .zone_geom <- terra::rast(boundaries_zonal[[1]])
    if (!terra::compareGeom(data, .zone_geom, stopOnError = FALSE)) {
      data <- terra::resample(data, .zone_geom, method = "sum")
    }

    result <- admin_extract_wrap(data = data,
                                 save_dir = dirname(file),
                                 filename = paste0(file_base, "_", .zonal_res_tag),   # per-tif cache is resolution-specific
                                 FUN = stat,
                                 append_vals = c(exposure = var, unit = unit, tech = tech),
                                 var_name = "crop",
                                 round = 1,
                                 boundaries_zonal = boundaries_zonal,
                                 boundaries_index = boundaries_index,
                                 overwrite = overwrite_glw)
    
    attr_file<-file.path(dirname(file),paste0(file_base,"_",.zonal_res_tag,"_adm_",stat,".parquet.json"))
    
    filter_colnames<-c("crop","stat","exposure","unit","tech")
    filters <- lapply(filter_colnames, function(split_col) {
      unique(result[[split_col]])
    })
    names(filters) <- filter_colnames
    
    
    if(file.exists(attr_file)){
      attr_dat<-jsonlite::read_json(attr_file)
      date_created<-unlist(attr_dat$date_created)
      version_attr<-unlist(attr_dat$version)
      if(overwrite_glw|version_attr!=version_glw){
        date_created<-Sys.time()
        update_attr_flag<-T
      }else{
        update_attr_flag<-F
      }
    }else{
      update_attr_flag<-T
      date_created<-Sys.time()
    }
    
    if(update_attr_flag){
      attr_info <- list(
        source = list(input_raster="GLW4",extraction_vect=atlas_data$boundaries$name),
        source_year = list(input_raster=source_year_glw),
        date_created = date_created,
        field_descriptions = field_descriptions,
        filters = filters,
        version = version_glw,
        parent_script = "R/0.6_process_exposure.R",
        variable = var,
        unit = unit,
        technology = tech,
        stat=stat,
        notes = paste0("A table of glw livestock values (",var,") extracted by boundary vectors then summarized (fun = ",stat,"). Note this analysis uses density adjusted (da) GLW values.")
      )
      
      write_json(attr_info, attr_file, pretty = TRUE)
    }
    
    return(result)

  }, .options = furrr::furrr_options(seed = TRUE, stdout = FALSE)))
future::plan(future::sequential)
  
# 3) Combine exposure totals by admin areas ####
.log044("section 3: merging MapSPAM + GLW exposure tables by admin areas")
  # 3.1) Original recipe ####
file<-paste0(exposure_dir,"/exposure_adm_sum_spam20-20_glw420-20_",.zonal_res_tag,".parquet")

if(!file.exists(file)|overwrite_glw|overwrite_spam){
  
  exposure_adm_sum_tab<-rbind(
    spam_extracted,
    glw_extracted
  )
  
  # Subset units
  # ---------------------------------------------------------------------------
  # Issue #30. p.steward decision 2026-09-18: THE VINTAGE STAYS IN THE NAME -
  # the same call made for the #26 baseline windows. A unit label that hides
  # which vintage it describes cannot be reconciled against anything.
  #
  # What stood here was the defect. `units` was c(<harmonised label> = <source
  # unit>); the subset kept rows whose unit matched a VALUE, and a rename loop
  # further down rewrote the survivor to the vintage-less NAME:
  #     nominal-usd-2020 -> usd    intld15-2020 -> intld15    intld15 -> intld15
  # When 0.4.0 and 0.4.1 moved to `intld15-2021` / `nominal-usd-2021`, neither
  # was in the list. Every current row was dropped by the subset, and what
  # survived under the label `intld15` was an older on-disk vintage (the
  # S3-legacy spam_vop_intld15_all.tif). So the published crop-livestock_all
  # carried `intld15` rows of one vintage while the S3 key beside it claimed
  # `vop_intld15-2021`, and the publish gate compared unlike vintages. The
  # rename also disagreed with this block's own sidecar, which records
  # unique(unit) BEFORE the loop ran. nominal-USD escaped the whole thing only
  # because section 3.2 filters `unit == "nominal-usd-2021"` explicitly.
  #
  # Now: an explicit expected-unit list, NO renaming, and a hard stop when an
  # expected unit is missing from the extraction. A silent drop cost months
  # here; an abort costs one log line. EXPOSURE_UNITS_LENIENT=1 downgrades the
  # stop to a warning for a deliberate partial run.
  units <- strsplit(Sys.getenv("EXPOSURE_UNITS",
                               "number,ha,t,nominal-usd-2021,intld15-2021"), ",")[[1]]
  present_units <- exposure_adm_sum_tab[, sort(unique(unit))]
  missing_units <- setdiff(units, present_units)
  dropped_units <- setdiff(present_units, units)
  .log044(sprintf("section 3.1: units present in extraction = %s", paste(present_units, collapse = ", ")))
  .log044(sprintf("section 3.1: units kept (EXPOSURE_UNITS) = %s", paste(units, collapse = ", ")))
  if (length(dropped_units)) {
    .log044(sprintf("section 3.1: units DROPPED = %s", paste(dropped_units, collapse = ", ")))
  }
  if (length(missing_units)) {
    .msg <- sprintf(paste0("section 3.1: expected unit(s) absent from the extraction: %s (present: %s). ",
                           "This is the issue #30 failure mode - a producer changed vintage and the ",
                           "combined table would silently carry an older one. Re-bake 0.4.0/0.4.1/0.4.2, ",
                           "or set EXPOSURE_UNITS to the vintages actually on disk."),
                    paste(missing_units, collapse = ", "), paste(present_units, collapse = ", "))
    if (nzchar(Sys.getenv("EXPOSURE_UNITS_LENIENT"))) .log044(paste("WARNING -", .msg)) else stop(.msg)
  }
  exposure_adm_sum_tab<-exposure_adm_sum_tab[unit %in% units]
  
  
  
  # Order to optimize parquet performance
  exposure_adm_sum_tab<-exposure_adm_sum_tab[order(iso3,admin0_name,admin1_name,admin2_name,exposure,unit,tech,crop)]

  filter_colnames<-c("crop","stat","exposure","unit","tech")
  filters <- lapply(filter_colnames, function(split_col) {
    unique(exposure_adm_sum_tab[[split_col]])
  })
  names(filters) <- filter_colnames
  
  
    attr_info <- list(
      source = list(input_raster1=atlas_data$mapspam_2020v1r2$name,
                    input_raster2="GLW4-2020",
                    extraction_vect=atlas_data$boundaries$name),
      source_year = list(input_raster1=c(spam_year=2020,fao_vop="see unit"),input_raster2=c(glw_year=2020,fao_price="see unit")),
      date_created = Sys.time(),
      zonal_grid = list(resolution_deg = .zonal_res_deg, tag = .zonal_res_tag, base_raster = basename(zonal_base_path), note = "Admin zones rasterised at this resolution; inputs on a coarser grid were sum-resampled (area-weighted) to it in sections 1/2."),
      field_descriptions = field_descriptions,
      filters = filters,
      version = list(input_version1=version_spam,input_version2=version_glw),
      parent_script = "R/0.6_process_exposure.R",
      variable = exposure_adm_sum_tab[,unique(exposure)],
      unit = exposure_adm_sum_tab[,unique(unit)],
      technology = exposure_adm_sum_tab[,unique(tech)],
      # MapSPAM + GLW extractions both use FUN="sum"; literal here so we
      # don't depend on `stat` leaking out of the (now-multisession)
      # furrr_map function scope.
      stat = "sum",
      notes = paste0("A merged table of all mapspam crop x technology and glw livestock values extracted by boundary vectors then summarized.")
    )

    attr_file<-paste0(file,".json")

    write_json(attr_info, attr_file, pretty = TRUE)

    # No unit renaming: the vintage stays in the name (see the subset above).
    # `unit` reaches the parquet exactly as the producer wrote it, which is also
    # what this block's sidecar already recorded.

    # `unit_full` (issue #30, p.steward 2026-09-22). The published
    # crop-livestock_all was built from `main`, whose 0.4.4 preserved the
    # pre-flatten unit in `unit_full`; consumers read it and
    # scripts/r3_publish_tiers.R refuses a schema that differs from live. `unit`
    # already carries the vintage here, so `unit_full` is an identical copy kept
    # for back-compat, not a second source of truth.
    #
    # Do NOT add the S3 key's hive columns (domain/type/source/region/processing)
    # here. They are not stored in the live file - the live STORED schema is 14
    # columns (parquet_schema, 2026-09-23) - DuckDB synthesises them from the
    # `key=value/` path at read time. Storing them made the column gate fail
    # (595090a, reverted).
    exposure_adm_sum_tab[, unit_full := unit]
    .log044(sprintf("section 3.1: %d columns -> %s", ncol(exposure_adm_sum_tab),
                    paste(names(exposure_adm_sum_tab), collapse = ", ")))

    exposure_adm_sum_tab[,crop:=gsub("_| ","-",crop)]

    write_parquet_pushdown(
      exposure_adm_sum_tab, file,
      sort_by         = c("iso3", "admin0_name", "admin1_name", "admin2_name",
                          "exposure", "unit", "tech", "crop"),
      verify_stats_on = c("iso3", "exposure", "unit", "crop")
    )
}
  
  
  # 3.2) Specific data for economic returns notebook ####
  
  file<-paste0(exposure_dir,"/vop_nominal-usd-2021_adm_sum_spam20_glw420_",.zonal_res_tag,".parquet")
  
  if(!file.exists(file)|overwrite_glw|overwrite_spam){
      
      exposure_adm_sum_tab<-rbind(
        spam_extracted,
        glw_extracted
      )
      
      # Subset units
      exposure_adm_sum_tab<-exposure_adm_sum_tab[unit=="nominal-usd-2021"]
      
      # Order to optimize parquet performance
      exposure_adm_sum_tab<-exposure_adm_sum_tab[order(iso3,admin0_name,admin1_name,admin2_name,exposure,unit,tech,crop)]
      
      filter_colnames<-c("crop","stat","exposure","unit","tech")
      filters <- lapply(filter_colnames, function(split_col) {
        unique(exposure_adm_sum_tab[[split_col]])
      })
      names(filters) <- filter_colnames
      
      attr_info <- list(
        source = list(input_raster1=atlas_data$mapspam_2020v1r2$name,
                      input_raster2="GLW4",
                      extraction_vect=atlas_data$boundaries$name),
        source_year = list(input_raster1=c(spam_year=2020,fao_vop=2021),input_raster2=c(glw_year=2020,fao_price=2021)),
        date_created = Sys.time(),
        zonal_grid = list(resolution_deg = .zonal_res_deg, tag = .zonal_res_tag, base_raster = basename(zonal_base_path), note = "Admin zones rasterised at this resolution; inputs on a coarser grid were sum-resampled (area-weighted) to it in sections 1/2."),
        field_descriptions = field_descriptions,
        filters = filters,
        version = list(input_version1=version_spam,input_version2=version_glw),
        parent_script = "R/0.6_process_exposure.R",
        variable = exposure_adm_sum_tab[,unique(exposure)],
        unit = exposure_adm_sum_tab[,unique(unit)],
        technology = exposure_adm_sum_tab[,unique(tech)],
        stat = "sum",  # MapSPAM + GLW extractions both use FUN="sum" (see L343 note)
        notes = paste0("A merged table of all mapspam crop x technology and glw livestock values extracted by boundary vectors then summarized.")
      )
      
      attr_file<-paste0(file,".json")
      
      write_json(attr_info, attr_file, pretty = TRUE)
      
      exposure_adm_sum_tab[, unit_full := unit]   # back-compat copy, see section 3.1
      exposure_adm_sum_tab[,crop:=gsub("_| ","-",crop)]

      write_parquet_pushdown(
        exposure_adm_sum_tab, file,
        sort_by         = c("iso3", "admin0_name", "admin1_name", "admin2_name",
                            "exposure", "unit", "tech", "crop"),
        verify_stats_on = c("iso3", "exposure", "unit", "crop")
      )
    }
  

  # 3.3) Constant international dollar twin of 3.2 ####
  # ---------------------------------------------------------------------------
  # Issue #30. `variable=vop_intld15-2021.parquet` has been live under
  # domain=exposure/type=combined since 2025-11-03 with NO producer anywhere in
  # this repo - an S3 key asserting a vintage that nothing in the pipeline
  # maintains, and whose rows in fact carry the vintage-less `intld15`. It is
  # the const-I$ member of the per-unit family that 3.2 already writes for
  # nominal USD, so it gets the same producer rather than being left orphaned.
  #
  # Same shape as 3.2 deliberately: rebuilt from the raw extractions, so it
  # filters the SOURCE unit and never goes near 3.1's combined table. That is
  # the property which kept nominal USD correct while the combined table
  # silently carried a legacy vintage.
  #
  # Publishing it is a separate, still-held decision: no uploader in this repo
  # writes the type=combined per-unit keys (scripts/r3_publish_tiers.R ships
  # crop-livestock_all only), so this block refreshes the LOCAL artifact and
  # the S3 object stays stale until a publish route is authorised.

  file<-paste0(exposure_dir,"/vop_intld15-2021_adm_sum_spam20_glw420_",.zonal_res_tag,".parquet")

  if(!file.exists(file)|overwrite_glw|overwrite_spam){

      exposure_adm_sum_tab<-rbind(
        spam_extracted,
        glw_extracted
      )

      # Subset units. Abort rather than write an empty or wrong-vintage file:
      # a silent drop here is exactly how #30 stayed hidden for months.
      .unit_intld <- Sys.getenv("EXPOSURE_UNIT_INTLD", "intld15-2021")
      .present <- exposure_adm_sum_tab[, sort(unique(unit))]
      .log044(sprintf("section 3.3: unit kept = %s (present in extraction: %s)",
                      .unit_intld, paste(.present, collapse = ", ")))
      if (!.unit_intld %in% .present) {
        .msg <- sprintf(paste0("section 3.3: unit `%s` absent from the extraction (present: %s). ",
                               "Re-bake 0.4.0/0.4.1, or set EXPOSURE_UNIT_INTLD to the vintage ",
                               "actually on disk."),
                        .unit_intld, paste(.present, collapse = ", "))
        if (nzchar(Sys.getenv("EXPOSURE_UNITS_LENIENT"))) .log044(paste("WARNING -", .msg)) else stop(.msg)
      }
      exposure_adm_sum_tab<-exposure_adm_sum_tab[unit==.unit_intld]
      if (!nrow(exposure_adm_sum_tab)) {
        stop(sprintf("section 3.3: no rows left after filtering to unit `%s` - refusing to write an empty %s",
                     .unit_intld, basename(file)))
      }
      .log044(sprintf("section 3.3: %d rows, %d crops", nrow(exposure_adm_sum_tab),
                      exposure_adm_sum_tab[, uniqueN(crop)]))

      # Order to optimize parquet performance
      exposure_adm_sum_tab<-exposure_adm_sum_tab[order(iso3,admin0_name,admin1_name,admin2_name,exposure,unit,tech,crop)]

      filter_colnames<-c("crop","stat","exposure","unit","tech")
      filters <- lapply(filter_colnames, function(split_col) {
        unique(exposure_adm_sum_tab[[split_col]])
      })
      names(filters) <- filter_colnames

      attr_info <- list(
        source = list(input_raster1=atlas_data$mapspam_2020v1r2$name,
                      input_raster2="GLW4",
                      extraction_vect=atlas_data$boundaries$name),
        source_year = list(input_raster1=c(spam_year=2020,fao_gpv=2021),input_raster2=c(glw_year=2020,fao_gpv=2021)),
        date_created = Sys.time(),
        zonal_grid = list(resolution_deg = .zonal_res_deg, tag = .zonal_res_tag, base_raster = basename(zonal_base_path), note = "Admin zones rasterised at this resolution; inputs on a coarser grid were sum-resampled (area-weighted) to it in sections 1/2."),
        field_descriptions = field_descriptions,
        filters = filters,
        version = list(input_version1=version_spam,input_version2=version_glw),
        parent_script = "R/0.4.4_process_exposure.R - section 3.3",
        variable = exposure_adm_sum_tab[,unique(exposure)],
        unit = exposure_adm_sum_tab[,unique(unit)],
        technology = exposure_adm_sum_tab[,unique(tech)],
        stat = "sum",  # MapSPAM + GLW extractions both use FUN="sum" (see L343 note)
        notes = paste0("Value of production in CONSTANT 2015 INTERNATIONAL DOLLARS, 2021 vintage ",
                       "(FAOStat gross production value, const I$, distributed across MapSPAM crop ",
                       "production shares and GLW4 livestock head shares). The const-I$ twin of the ",
                       "nominal-USD table written by section 3.2. The vintage is carried in the unit ",
                       "string on every row and is not rewritten - see issue #30.")
      )

      attr_file<-paste0(file,".json")

      write_json(attr_info, attr_file, pretty = TRUE)

      exposure_adm_sum_tab[, unit_full := unit]   # back-compat copy, see section 3.1
      exposure_adm_sum_tab[,crop:=gsub("_| ","-",crop)]

      write_parquet_pushdown(
        exposure_adm_sum_tab, file,
        sort_by         = c("iso3", "admin0_name", "admin1_name", "admin2_name",
                            "exposure", "unit", "tech", "crop"),
        verify_stats_on = c("iso3", "exposure", "unit", "crop")
      )
    }

# 4) Population ######
.log044("section 4: Worldpop hpop harmonize + admin extract")
overwrite_pop<-T
  ## 4.1) Harmonize to atlas base raster ####
hpop_file<-paste0(hpop_int_dir,"/hpop_atlas_",.zonal_res_tag,".tif")
if(!file.exists(hpop_file)|overwrite_pop==T){
  local_files<-list.files(hpop_dir,".tif",full.names = T)
  hpop<-terra::rast(local_files)
  hpop<-terra::crop(hpop,rast(boundaries_zonal[[1]]))
  
  # v9: mass-conserving resample via method="sum". The prior
  # density / bilinear / density-back pattern leaked ~7.9% mass at country
  # totals for hpop (AGO probe; see R/checks/9_mass_conservation_check.R
  # and issue #9).
  .hpop_src_mass <- terra::global(hpop, "sum", na.rm = TRUE)[, 1]
  hpop <- terra::resample(hpop, rast(boundaries_zonal[[1]]), method = "sum")
  .hpop_dst_mass <- terra::global(hpop, "sum", na.rm = TRUE)[, 1]
  .hpop_ratio <- .hpop_dst_mass / .hpop_src_mass
  if (any(abs(.hpop_ratio - 1) > 0.005, na.rm = TRUE)) {
    warning(sprintf(
      "[0.4.4] mass not conserved on hpop resample: %s",
      paste(sprintf("%s=%.4f", names(hpop), .hpop_ratio), collapse = ", ")
    ))
  }
  
  terra::writeRaster(hpop,filename =hpop_file,overwrite=T)
}

  ## 4.2) Extraction ####
version_hpop<-1
file<-paste0(exposure_dir,"/hpop_adm_sum_",.zonal_res_tag,".parquet")

if(!file.exists(file)|overwrite_pop==T){  
  
  data<-rast(hpop_file)

  cat("Extracting hpop \n")
  file_base<-gsub(".parquet","",basename(file))
  var<-gsub(".parquet","",unlist(tstrsplit(basename(file),"_",keep=1)))
  unit<-"number"

  stat<-"sum"
  
  hpop_extracted<-admin_extract_wrap(data=data,
                             save_dir=dirname(file),
                             filename = paste0(var, "_", .zonal_res_tag),   # per-tif cache is resolution-specific
                             FUN=stat,
                             append_vals=c(exposure=var,unit=unit),
                             var_name="type",
                             round=1,
                             boundaries_zonal=boundaries_zonal,
                             boundaries_index=boundaries_index,
                             overwrite=overwrite_pop)
  
  filter_colnames<-c("stat","exposure","unit","type")
  filters <- lapply(filter_colnames, function(split_col) {
    unique(hpop_extracted[[split_col]])
  })
  names(filters) <- filter_colnames
  
  
  field_descriptions2<-copy(field_descriptions)
  field_descriptions2[!field_name %in% c("tech","crop")]
  field_descriptions2<-rbind(field_descriptions2,data.table(field_name = "type",
                                       type = "character",
                                       description = "Type of population measure, rural, urban or total."))
  
  # Order to optimize parquet performance
  hpop_extracted<-hpop_extracted[order(iso3,admin0_name,admin1_name,admin2_name)]

  write_parquet_pushdown(
    hpop_extracted, file,
    sort_by         = c("iso3", "admin0_name", "admin1_name", "admin2_name"),
    verify_stats_on = c("iso3")
  )
  
  attr_file<-paste0(file,".json")
  
  attr_info <- list(
    source = list(input_raster="Worldpop",
                  extraction_vect=atlas_data$boundaries$name),
    source_year = list(input_raster="2020"),
    date_created = Sys.Date(),
    field_descriptions = field_descriptions2,
    filters = filters,
    version = list(input_version=version_hpop),
    parent_script = "R/0.6_process_exposure.R",
    variable = exposure_adm_sum_tab[,unique(exposure)],
    unit = exposure_adm_sum_tab[,unique(unit)],
    type = exposure_adm_sum_tab[,unique(tech)],
    stat=stat,
    notes = paste0("Human population extracted by boundary vectors then summed")
  )
  
  write_json(attr_info, attr_file, pretty = TRUE)
}

cat("\n===== 0.4.4_process_exposure.R COMPLETE at ",
    format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"), " =====\n", sep = "")

