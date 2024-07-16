
pacman::p_load(data.table,readxl)


list.files(solution_tables_dir,full.names = T)

works_best<-data.table::fread(file.path(solution_tables_dir,"WorksBest.csv"),blank.lines.skip=T)
works_best<-works_best[!is.na(Sol.ID),.(Sol.ID,Solution,Hazard,Stress.level,Exposure,Summary)]
works_best[,Solution:=gsub("fert","Fert",Solution)][,Solution:=gsub("insurance","Insurance",Solution)]
setnames(works_best,"Summary","Hazard_response")

impacts<-data.table(read_excel(file.path(solution_tables_dir,"MVP-Impacts-26aug22.xlsx"),sheet="Impacts"))
# There appear to be two GHG columns, using the first one
impacts<-impacts[,c(1:5)]
setnames(impacts,colnames(impacts),unlist(tstrsplit(colnames(impacts),"[.][.][.]",keep=1)))
impacts[,Sol.ID:=NULL]

impact_yield<-data.table(read_excel(file.path(solution_tables_dir,"MVP-Yield-6sept.xlsx"),sheet="Yield"))
setnames(impact_yield,c("Summary","Labels"),c("Productivity","Productivity_yield_lab"))
impact_yield[,c("Stress level","Exposure","Hazard","Sol.ID"):=NULL]

impact_yield[,Solution:=gsub("fert","Fert",Solution)][,Solution:=gsub("insurance","Insurance",Solution)]

# merge impacts and impact_yield
impacts_m<-merge(impact_yield,impacts,by="Solution",all.x=T)


solutions_tab<-merge(works_best,impacts_m,all.x=T,by="Solution")
colnames(solutions_tab)<-gsub("'","",gsub("[.]| ","_",tolower(colnames(solutions_tab))))
solutions_tab[]