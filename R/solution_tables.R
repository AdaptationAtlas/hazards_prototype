# This script is for internal use and merges/wrangles Todd's solution tables into a standard format
# You will need access to the files contained in the onedrive folder 2. Adaptation Atlas/Phase II_Shared/2. Product/2. MVP/Solutions
# Please run 0_server_setup.R before executing this script

# 0) Load packages ####
pacman::p_load(data.table,readxl)

# 1) Load data ####
# file.path(solution_tables_dir,"raw") is where the "raw" Solution tables are stored
list.files(file.path(solution_tables_dir,"raw"),full.names = T)

# 1.1) Load hazard response data #####
works_best<-data.table::fread(file.path(solution_tables_dir,"raw","WorksBest.csv"),blank.lines.skip=T)
works_best<-works_best[!is.na(Sol.ID),.(Sol.ID,Solution,Hazard,Stress.level,Exposure,Summary)]
works_best[,Solution:=gsub("fert","Fert",Solution)][,Solution:=gsub("insurance","Insurance",Solution)]
setnames(works_best,"Summary","hazard_mitigation")

# 1.2) Load impacts on ghgs, womens_time and profit #####
impacts<-data.table(read_excel(file.path(solution_tables_dir,"raw","MVP-Impacts-26aug22.xlsx"),sheet="Impacts"))
# There appear to be two GHG columns, using the first one
impacts<-impacts[,c(1:5)]
setnames(impacts,colnames(impacts),unlist(tstrsplit(colnames(impacts),"[.][.][.]",keep=1)))
impacts[,Sol.ID:=NULL]

# 1.3) Load impacts on general productivity #####
impact_yield<-data.table(read_excel(file.path(solution_tables_dir,"raw","MVP-Yield-6sept.xlsx"),sheet="Yield"))
setnames(impact_yield,c("Summary","Labels"),c("Productivity","Productivity_lab"))
impact_yield[,c("Stress level","Exposure","Hazard","Sol.ID"):=NULL]

impact_yield[,Solution:=gsub("fert","Fert",Solution)][,Solution:=gsub("insurance","Insurance",Solution)]

# 2) Merge datasets ####
# Merge impacts and impact_yield
impacts_m<-merge(impact_yield,impacts,by="Solution",all.x=T)

# Merge hazard response with other impacts
labs_w<-data.table(womens_time=c("nd",-1,0,1),womens_time_lab=c("nd","negative","uncertain","positive"))
labs_p<-data.table(profit=c("nd",-1,0,1,2,3),profit_lab=c("nd","negative","uncertain","positive","positive","positive"))
labs_g<-data.table(ghgs=c("nd",-1,-0.5,0,1,2),ghgs_lab=c("nd","negative","negative","uncertain","positive","positive"))

solutions_tab<-merge(works_best,impacts_m,all.x=T,by="Solution")
colnames(solutions_tab)<-gsub("'","",gsub("[.]| ","_",tolower(colnames(solutions_tab))))
setnames(solutions_tab,c("stress_level"),c("severity"))
solutions_tab<-solutions_tab[,hazard:=tolower(hazard)
                              ][,severity:=tolower(severity)
                                ][severity=="significant",severity:="severe"
                                  ][,exposure_crops:=F
                                    ][grep("Crop",exposure),exposure_crops:=T
                                      ][,exposure_livestock:=F
                                        ][grep("Livestock",exposure),exposure_livestock:=T
                                          ][,exposure:=NULL]

solutions_tab<-merge(solutions_tab,labs_w,all.x=T,by="womens_time")
solutions_tab<-merge(solutions_tab,labs_p,all.x=T,by="profit")
solutions_tab<-merge(solutions_tab,labs_g,all.x=T,by="ghgs")

solutions_tab[,.(sol_id,solution,productivity,productivity_lab,womens_time,womens_time_lab,profit,profit_lab,ghgs,ghgs_lab,
                 exposure_crops,exposure_livestock,hazard,severity,hazard_mitigation)]

# 2.1) Split 
solution_tab_others<-unique(solutions_tab[,.(sol_id,solution,productivity,productivity_lab,womens_time,womens_time_lab,profit,profit_lab,ghgs,ghgs_lab,
                                          exposure_crops,exposure_livestock)])
solution_tab_haz<-unique(solutions_tab[,.(sol_id,solution,exposure_crops,exposure_livestock,hazard,severity,hazard_mitigation)])

# 3) Save processed datasets
data.table::fwrite(solutions_tab,file.path(solution_tables_dir,"solutions_all_impacts.csv"))
data.table::fwrite(solution_tab_haz,file.path(solution_tables_dir,"solutions_haz_impacts.csv"))
data.table::fwrite(solution_tab_others,file.path(solution_tables_dir,"solutions_other_impacts.csv"))



