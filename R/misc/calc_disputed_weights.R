library(terra)

a0_rast <- rast(
  "/vsis3/digital-atlas/domain=boundaries/type=admin/source=gaul2024/region=africa/processing=rasterized/level=adm0/gaul24_a0_res-05.tif"
)

a0_rast <- as.factor(a0_rast)

disputed_ls <- list(
  # names are gaul0_code
  "100" = list(iso3 = c("SSD", "SDN"), gaul0_code = c("160", "161")), # Abyei
  "110" = list(iso3 = c("EGY", "SDN"), gaul0_code = c("120", "161")), # Bīr Ṭawīl
  "133" = list(iso3 = c("EGY", "SDN"), gaul0_code = c("120", "161")), # Hala'Ib Triangle
  "135" = list(iso3 = c("KEN", "SSD"), gaul0_code = c("137", "160")) # Ilemi Triangle
  # We can skip western sahara as it is not being aggregated to another country.
)

disputed_df <- data.frame(
  disputed_gaul0 = c(100, 100, 110, 110, 133, 133, 135, 135),
  disputed_name = c(
    "Abyei",
    "Abyei",
    "Bīr Ṭawīl",
    "Bīr Ṭawīl",
    "Hala'Ib",
    "Hala'Ib",
    "Ilemi",
    "Ilemi"
  ),
  claimant_iso3 = c("SSD", "SDN", "EGY", "SDN", "EGY", "SDN", "KEN", "SSD"),
  claimant_gaul0 = c(160, 161, 120, 161, 120, 161, 137, 160)
)


disputed_codes <- unique(c(
  disputed_df$disputed_gaul0,
  disputed_df$claimant_gaul0
))

disputed_mask <- a0_rast %in% disputed_codes
disputed_mask <- a0_rast
disputed_mask[!disputed_mask %in% disputed_codes] <- NA

cell_per_area <- as.data.frame(t(global(disputed_mask, table)))
cell_per_area$cell_count <- cell_per_area$gaul0_code
cell_per_area$gaul0_code <- as.numeric(gsub("X", "", row.names(cell_per_area)))
row.names(cell_per_area) <- NULL

# Merge in disputed_area
disputed_df$disputed_ncells <- cell_per_area$cell_count[match(
  disputed_df$disputed_gaul0,
  cell_per_area$gaul0_code
)]

disputed_df$claimant_ncells <- cell_per_area$cell_count[match(
  disputed_df$claimant_gaul0,
  cell_per_area$gaul0_code
)]

write.csv(disputed_df, "metadata/disputed_areas.csv")
