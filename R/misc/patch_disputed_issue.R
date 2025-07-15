library(data.table)
library(nanoparquet)

ds <- as.data.table(read_parquet(
  "~/Downloads/haz-freq-exp_vop_usd15_ENSEMBLEmean_solo_adm_severe.parquet"
))

# Use vectorized pattern matching once
conflict_area_mask <- grepl("^x|ESH", ds$admin1_name)

## -- Manage conflict areas errors -- ##
# The correct value is the mode as one of the 3 extractions created erroes
conflict_ds <- ds[conflict_area_mask]

conflict_ds[,
  group_id := .GRP,
  by = c(names(conflict_ds)[-5])
][, group_size := .N, by = group_id]

# The correct extraction has 3 values and the last is the correct one.
# Also drop any Admin 0 id as we will merge it back into both countries
conflict_ds_filter <- conflict_ds[group_size == 3]
conflict_ds_filter[, .SD[.N], by = group_id] # THIS SHOULD BE THE RESULT BRAYDEN!
conflict_ds_filter[, c("group_id", "group_size") := NULL]

# conflict_ds_filter[, c("iso3", "admin0_name", "group_size") := NULL]
#
# group_info <- conflict_ds_filter[, .SD[1], by = group_id]
#
# find_mode <- function(x) {
#   # Count frequencies including NaN which is often the actual value
#   freq_table <- table(x, useNA = "always")
#   as.numeric(names(freq_table)[which.max(freq_table)])
# }
#
# # Calculate mode for each group
# result <- conflict_ds_filter[, .(value = find_mode(value)), by = group_id]
#
# conflict_clean <- merge(group_info[, -"value"], result, by = "group_id")

## -- Next clean up the admin2 values which have been duplicated & one is an incorrect value -- ##
no_conflict_adm2_ds <- ds[!conflict_area_mask & !is.na(admin2_name)]

no_conflict_adm2_ds[,
  dup_group := .GRP,
  by = c(names(no_conflict_adm2_ds)[-5])
][, group_size := .N, by = dup_group]

no_duplicates <- subset(no_conflict_adm2_ds, group_size == 1)
no_duplicates[, c("dup_group", "group_size") := NULL]

dups <- subset(no_conflict_adm2_ds, group_size == 2)
# conviently, due to how the tables are merged, the second value is always the correct one...
correct_rows <- dups[, .SD[2], by = dup_group]
correct_rows[, c("dup_group", "group_size") := NULL]

# 7253 length for admin0, admin1, admin2
# total result length should be 16801344 for admin 2
# total result admin 2 no conflict: 16778016
# total result: 18820512

# Combine all
result_admin_dupes <- rbind(no_duplicates, correct_rows)
result_all <- rbind(result_admin_dupes, conflict_ds_filter)

agg_admin1 <- result_all[,
  .(value = sum(value, na.rm = TRUE)),
  by = setdiff(names(result_all), c("value", "admin2_name"))
]

agg_admin0 <- result_all[,
  .(value = sum(value, na.rm = TRUE)),
  by = setdiff(names(result_all), c("value", "admin1_name", "admin2_name"))
]

result_long <- rbind(
  result_all,
  rbind(agg_admin1, agg_admin0, fill = T),
  fill = T
)


sum(
  subset(agg_admin0, iso3 == "AGO" & crop == "maize", select = "value"),
  na.rm = T
) ==
  sum(
    subset(result_all, iso3 == "AGO" & crop == "maize", select = "value"),
    na.rm = T
  )
