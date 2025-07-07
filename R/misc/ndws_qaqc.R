# Run ~/atlas/hazards_prototype/R/0_server_setup.R first
# I have not had time to integrate both sources (atlas_delta and nex-gddp into the script)
# You have to edit section 0.3) Set climate date source in ~/atlas/hazards_prototype/R/0_server_setup.R to choose the relevant data source
# This changes the indices_dir to the correct path for the selected dataset

# Load boundaries to get kenya border (or substitute your own boundary)
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


bbox<-Geographies$admin0[Geographies$admin0$iso3=="KEN",]

# Create maize mask
cmask<-rast(list.files(file.path(mapspam_pro_dir,"variable=prod_t"),"t_all.tif",full.names = T))

if(!grepl("nexg-ddp",indices_dir)){
  i_dirs<-list.dirs(indices_dir,recursive = F)
  i_dirs<-i_dirs[grepl("2081_2100|historic",i_dirs)]
  i_dirs<-i_dirs[!grepl("ipynb",i_dirs)]
  i_dirs<-file.path(i_dirs,"NDWS")
  
  # Resample the maize mask to nex-gddp 
  cmask<-resample(cmask,rast("/home/jovyan/common_data/nex-gddp-cmip6_indices/ssp126_ACCESS-CM2/NDWS/NDWS-2100-01.tif"))
  lab<-"atlas-delta"
}else{
  i_dirs<-list.dirs(indices_dir,recursive = F)
  i_dirs<-list.dirs(i_dirs,recursive = F)
  i_dirs<-grep("NDWS",i_dirs,value=T)
  lab<-"nex-gddp"
}

cmask<-mask(crop(cmask$maize,bbox),bbox)
cmask[cmask[]<100]<-NA

months<-c("01","02","03","04","05","06","07","08","09","10","11","12")

rdat_mean<-lapply(1:length(i_dirs),function(i){
  files<-list.files(i_dirs[i],"NDWS-.*tif",full.names = T)

  if(!grepl("nexg-ddp",indices_dir)){
    files<-grep(paste(2081:2100,collapse="|"),files,value=T)
  }
  
  if(length(files)>0){
  r_dat<-lapply(months,function(m){
    cat("folder",i,"/",length(i_dirs),"month",m,"       \n")
    files2<-grep(paste0(m,".tif"),files,value=T)
    if(length(files2)>0){
      rdat<-rast(files2)
      rdat<-mask(crop(rdat,cmask),cmask)
      rdat<-mean(rdat,na.rm=T)
      mean(values(rdat),na.rm=T)
      }
  })
  
  data.table(month=months,ndws_mean=unlist(r_dat),folder=basename(dirname(i_dirs[i])))
  }
})

result<-rbindlist(rdat_mean)
result[grepl("historical",folder),folder:=paste0(folder,"_",folder,"_",folder,"_",folder)]

if(!grepl("nexg-ddp",indices_dir)){
  
result[,c("scenario","model","year_start","year_end"):=tstrsplit(folder,"_")]
}else{
  result[,c("scenario","model"):=tstrsplit(folder,"_")]
}

# Build the ensemble rows
atlas_models<-c("ACCESS-ESM1-5","MPI-ESM1-2-HR","EC-Earth3","INM-CM5-0","MRI-ESM2-0")

ens <- result[ !grepl("historical",model) & model %in% atlas_models, .(ndws_mean = mean(ndws_mean,na.rm=T)),       # mean over *models*
               by = .(scenario, month)]                # keep scenario & month
ens[ , model := "Ensemble"]                            # tag as a pseudo-model

full <- rbind(result[,.(scenario,month,ndws_mean,model)], ens)

full[ , month_num := as.integer(month)]   # 1–12 for plotting
full[ , month_lab := month.abb[month_num]] # "Jan", "Feb", …
full[,unique(model)]


# base ggplot
#p <- ggplot(full[model %in% c("Ensemble","historical")], # un-hash if you just want the ensemble + historical baseline (note for nex-gddp there is no historical baseline calculated)
p <- ggplot(full[model %in% c("Ensemble",atlas_models)],
            aes(x = month_num, y = ndws_mean,
                colour = model, group = model)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2) +
  facet_wrap(~ scenario, ncol = 1) +           # one column of facets
  scale_x_continuous(breaks = 1:12, labels = month.abb) +
  labs(x        = "Month",
       y        = "NDWS mean",
       colour   = "GCM / model",
       title    = paste0("Monthly NDWS mean by model - ",lab),
       subtitle = "Faceted by SSP scenario") +
  theme_minimal(base_size = 12) +
  theme(panel.grid.minor.x = element_blank())


thresholds <- data.frame(                                   # ➊
  level      = c("Moderate", "Severe", "Extreme"),
  yintercept = c(15, 20, 25)
)

p_thr <- p +                                                # ➋ start from your p
  geom_hline(
    data     = thresholds,
    aes(yintercept = yintercept, linetype = level),  # map linetype ⇢ legend
    colour   = "grey30",
    linewidth = .6
  ) +
  scale_linetype_manual(                                    # ➌ nice legend labels
    name   = "Threshold",
    values = c(Moderate = "dotted",
               Severe   = "longdash",
               Extreme  = "solid")
  ) +
  guides(colour = guide_legend(order = 1),        # keep model legend first
         linetype = guide_legend(order = 2)) +    # threshold legend second
  theme(legend.position = "bottom")               # optional: put legends together

p_thr        # print the final plot


## ── ensure factors are ordered nicely ───────────────────────────────
# order models by their overall median (so the boxes read “best → worst”)
result[ , model := factor(model,
                          levels = result[ , stats::reorder(model, ndws_mean, median)])]

## ── box-plot
bp <- ggplot(result,
             aes(x = model, y = ndws_mean, fill = model)) +
  geom_boxplot(outlier.shape = 21, outlier.size = 2, linewidth = .4) +
  facet_wrap(~ scenario, ncol = 1, scales = "free_y") +
  labs(x        = "GCM (model)",
       y        = "NDWS mean (monthly distribution)",
       fill     = NULL,
       title    = "Distribution of monthly NDWS_mean by GCM",
       subtitle = "Boxes aggregate the 12 monthly points for each model") +
  scale_fill_brewer(palette = "Set2", guide = "none") +
  theme_minimal(base_size = 12) +
  theme(
    panel.grid.major.x = element_blank(),
    axis.text.x        = element_text(angle = 45, hjust = 1)
  )

bp


# Draw the box-plots + distinct styling for Ensemble
bp <- ggplot(full, aes(x = model, y = ndws_mean, fill = model)) +
  geom_boxplot(outlier.shape = 21, outlier.size = 2, linewidth = .4,
               aes(alpha = model == "Ensemble")) +       # subtler boxes
  scale_alpha_manual(values = c("TRUE" = 1, "FALSE" = .70), guide = "none") +
  scale_fill_brewer(palette = "Set2", guide = "none") +
  facet_wrap(~ scenario, ncol = 1, scales = "free_y") +
  labs(x = "GCM (model)",
       y = "NDWS mean (monthly distribution)",
       title = "Monthly NDWS_mean: individual GCMs vs Ensemble") +
  theme_minimal(base_size = 12) +
  theme(
    panel.grid.major.x = element_blank(),
    axis.text.x        = element_text(angle = 45, hjust = 1)
  )

bp
