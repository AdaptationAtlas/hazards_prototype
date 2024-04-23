
x<-crop_vop_tot_adm_sum
fwrite(x,file.path(exposure_dir,"crop_vop_tot_adm_sum.csv"),bom=T)

fwrite(exposure_adm_sum_tab,file.path(exposure_dir,"exposure_adm_sum_tab.csv"),bom=T)


# Read the CSV file containing the MapSPAM data for the given variable and technology.
variable<-"V"
technology<-"TA"
data <- fread(paste0(mapspam_dir, "/SSA_", variable, "_", technology, ".csv"))

# Prepare a list of crop names to filter from the MapSPAM data, based on the ms_codes lookup table.
crops <- tolower(ms_codes$Code)

colnames(data) <- gsub("_a$|_h$|_i$|_l$|_r$|_s$", "", colnames(data))
colnames(data) <- ms_codes[match(colnames(data), tolower(ms_codes$Code)), Fullname]


cols<-c("iso3",ms_codes$Fullname)

data<-data[,..cols]

crop_columns <- setdiff(names(data), "iso3")

# Calculating the sum for each crop within each iso3 group.
result <- data[, .(total_value = sum(unlist(.SD))), by = .(iso3), .SDcols = crop_columns][,total_value_M:=round(total_value/10^6,2)]

crop_vop_tot_adm_sum[is.na(admin1_name),sum(value),by=admin0_name]

# V1
livestock_vop<-terra::rast(ls_vop_files)
names(livestock_vop)<-unlist(tstrsplit(names(livestock_vop),"-",keep=3))
livestock_vop<-livestock_vop[[!grepl("total",names(livestock_vop))]]

ls2_ex<-data.table(terra::extract(livestock_vop,Geographies$admin0,fun="sum",na.rm=T))
ls2_ex$admin0<-Geographies$admin0$admin_name
ls2_ex[,total:=cattle+chicken+pig+sheep_goat]

# V2
livestock_vop<-terra::rast(ls_vop_files)

names(livestock_vop)<-unlist(tstrsplit(names(livestock_vop),"-",keep=3))

# Remove totals
livestock_vop<-livestock_vop[[!grepl("total",names(livestock_vop))]]

# resample to 0.05
livestock_density<-livestock_vop/terra::cellSize(livestock_vop,unit="ha")
livestock_density<-terra::resample(livestock_density,base_rast)
livestock_vop<-livestock_density*cellSize(livestock_density,unit="ha")

ls_ex<-data.table(terra::extract(livestock_vop,Geographies$admin0,fun="sum",na.rm=T))
ls_ex$admin0<-Geographies$admin0$admin_name
ls_ex[,total:=cattle+chicken+pig+sheep_goat]
setnames(ls_ex,c("cattle","chicken","pig","sheep_goat","total"),c("cattle2","chicken2","pig2","sheep_goat2","total2"))

ls_ex<-merge(ls2_ex,ls_ex)

cols<-sort(colnames(ls_ex))

ls_ex<-ls_ex[,..cols][,ID:=NULL]
ls_ex[,ratio:=total2/total]
ls_ex

# V3
livestock_vop<-terra::rast(ls_vop_files)

names(livestock_vop)<-unlist(tstrsplit(names(livestock_vop),"-",keep=3))

# Remove totals
livestock_vop<-livestock_vop[[!grepl("total",names(livestock_vop))]]

# resample to 0.05
livestock_density<-livestock_vop/terra::cellSize(livestock_vop,unit="ha")
livestock_density<-terra::resample(livestock_density,base_rast)
livestock_vop<-livestock_density*cellSize(livestock_density,unit="ha")
rm(livestock_density)

# Load prop files
sheep_prop<-terra::rast(shoat_prop_file)$sheep_prop
goat_prop<-terra::rast(shoat_prop_file)$goat_prop

# Split sheep goat vop using their populations
livestock_vop$sheep<-livestock_vop$sheep_goat*sheep_prop
livestock_vop$goats<-livestock_vop$sheep_goat*goat_prop
livestock_vop$sheep_goat<-NULL

# Split vop by highland vs lowland
livestock_vop<-split_livestock(data=livestock_vop,livestock_mask_high,livestock_mask_low)

ls_ex<-data.table(terra::extract(livestock_vop,Geographies$admin0,fun="sum",na.rm=T))
ls_ex$admin0<-Geographies$admin0$admin_name
ls_ex[,total2:=cattle_tropical+poultry_tropical+pigs_tropical +sheep_tropical+ goats_tropical+ 
        cattle_highland+ poultry_highland + pigs_highland+ sheep_highland+goats_highland
      ][,cattle2:=cattle_tropical+cattle_highland
        ][,chicken2:=poultry_highland+poultry_tropical
          ][,pig2:=pigs_highland+pigs_tropical
            ][,sheep_goat2:=sheep_highland+goats_highland+sheep_tropical+goats_tropical]


ls_ex<-merge(ls2_ex,ls_ex[,list(admin0,cattle2,chicken2,pig2,sheep_goat2,total2)])

cols<-sort(colnames(ls_ex))

ls_ex<-ls_ex[,..cols][,ID:=NULL]
ls_ex[,ratio:=total2/total]
ls_ex
