require(data.table)

fls <- system("aws s3 ls s3://nex-gddp-cmip6/ --no-sign-request --recursive", intern = TRUE)
nms <- strsplit(fls, split = '/') |> purrr::map(6) |> unlist()

# Split on "_"
split_nms <- strsplit(nms, "_")

# Convert to data frame
df <- rbindlist(lapply(split_nms, function(x) as.data.frame(t(x), stringsAsFactors = FALSE)),fill=TRUE)

# Optionally assign column names
colnames(df) <- c("variable", "frequency", "model", "scenario", "realization", "grid", "year", "version")

df<-data.table(df)
df<-df[realization=="r1i1p1f1"]

pr<-dcast(df[variable %in% c("pr"),.(model,scenario,variable)],model~variable+scenario)
tasmax<-dcast(df[variable %in% c("tasmax"),.(model,scenario,variable)],model~variable+scenario)
tasmin<-dcast(df[variable %in% c("tasmin"),.(model,scenario,variable)],model~variable+scenario)
hurs<-dcast(df[variable %in% c("hurs"),.(model,scenario,variable)],model~variable+scenario)
rsds<-dcast(df[variable %in% c("rsds"),.(model,scenario,variable)],model~variable+scenario)

model<-pr$model
pr<-rowSums(pr[,!"model"]>0)
tasmax<-rowSums(tasmax[,!"model"]>0)
tasmin<-rowSums(tasmin[,!"model"]>0)
hurs<-rowSums(hurs[,!"model"]>0)
rsds<-rowSums(rsds[,!"model"]>0)

available<-data.table(model=model,
           pr=pr,
           tasmax=tasmax,
           tasmin=tasmin,
           hurs=hurs,
           rsds=rsds,
           all=rowSums(cbind(pr,tasmax,tasmin,hurs,rsds)))

available[order(all,model,decreasing=T)]

pr<-dcast(df[variable %in% c("pr"),.(model,scenario,variable)],model~variable+scenario)
tasmax<-dcast(df[variable %in% c("tasmax"),.(model,scenario,variable)],model~variable+scenario)
tasmin<-dcast(df[variable %in% c("tasmin"),.(model,scenario,variable)],model~variable+scenario)
hurs<-dcast(df[variable %in% c("hurs"),.(model,scenario,variable)],model~variable+scenario)
rsds<-dcast(df[variable %in% c("rsds"),.(model,scenario,variable)],model~variable+scenario)

pr<-rowSums(pr[,!"model"])
tasmax<-rowSums(tasmax[,!"model"])
tasmin<-rowSums(tasmin[,!"model"])
hurs<-rowSums(hurs[,!"model"])
rsds<-rowSums(rsds[,!"model"])

available2<-data.table(model=model,
                      pr_n=pr,
                      tasmax_n=tasmax,
                      tasmin_n=tasmin,
                      hurs_n=hurs,
                      rsds_n=rsds,
                      all_n=rowSums(cbind(pr,tasmax,tasmin,hurs,rsds)))

available12<-merge(available,available2,by="model",all.x=T)
available12[order(all,model,decreasing=T)]

