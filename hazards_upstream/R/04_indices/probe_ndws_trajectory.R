# Definitive NDWS diagnosis: trace the daily water balance for ONE wet pixel,
# historic vs future, to see WHY historic ERATIO stays < 0.5 (saturation) when
# inputs + seed are clean and historic is actually COOLER (=> should be LESS
# stressed). Replicates peest2 (Priestley-Taylor PET) + eabyep_calc verbatim from
# fast_calc_NDWS.R, for a single cell across a year, from an EMPTY seed (AVAIL=0)
# AND a FIELD-CAPACITY seed (AVAIL=soilcp) so the empty-soil spin-up hypothesis is
# directly testable.
#
# Usage: COMMON_DATA=~/common_data GCM=ACCESS-CM2 HYR=1995 FYR=2050 FSSP=ssp245 \
#        Rscript 04_indices/probe_ndws_trajectory.R

local({
  cargs <- commandArgs(FALSE); fa <- grep("^--file=", cargs, value = TRUE)
  base <- if (length(fa)) dirname(normalizePath(sub("^--file=", "", fa[1]))) else getwd()
  cand <- c(file.path(base,"..","00_setup.R"), file.path(base,"00_setup.R"), "../00_setup.R","00_setup.R")
  hit <- cand[file.exists(cand)][1]; if (is.na(hit)) stop("00_setup.R not found"); source(normalizePath(hit), local=FALSE)
})
suppressMessages(library(terra))
root <- common_data_root()
GCM<-env_or("GCM","ACCESS-CM2"); HYR<-env_or("HYR","1995"); FYR<-env_or("FYR","2050"); FSSP<-env_or("FSSP","ssp245")

# peest2 for a single cell (vectors over days) - verbatim constants from fast_calc_NDWS.R
peest_cell <- function(srad,tmin,tmean,tmax){
  albedo<-0.2; rn<-(1-albedo)*srad
  eslope<-(611.2*17.67*243.5*exp(17.67*tmean/(tmean+243.5)))/((tmean+243.5)^2)
  vpd<-0.7*(0.6112*(exp((17.67*tmax)/(tmax+243.5))-exp((17.67*tmin)/(tmin+243.5))))
  pt_coef<-1+(1.26-1)*vpd/1; conv<-1e6*100/(2.26e6*997)*10
  pt_coef*rn*eslope/(eslope+62)*conv
}
# eabyep daily availability/eratio for one cell, given a starting avail
run_cell <- function(prc,etmax,soilcp,avail0){
  n<-length(prc); er<-numeric(n); av<-avail0
  for(i in 1:n){
    a<-min(av,soilcp); pw<-max(min(a/soilcp*100,100),1)
    e<-min(pw/(97-3.868*sqrt(soilcp)),1); er[i]<-e
    av<-max(min(soilcp, a+prc[i]-e*etmax[i]),0)
  }
  er
}
# pull one year of daily inputs for a cell, return per-day vectors
yr_cell <- function(ssp, yr, cellxy){
  vd<-function(v) file.path(root,"nex-gddp-cmip6",v,ssp,GCM)
  fls<-function(v) sort(list.files(vd(v), pattern=paste0("^",v,"_",yr,"-.*\\.tif$"), full.names=TRUE))
  ex<-function(v){ r<-terra::rast(fls(v)); terra::extract(r, cellxy)[1,-1] |> as.numeric() }
  list(pr=ex("pr"),rsds=ex("rsds"),tmax=ex("tasmax"),tmin=ex("tasmin"))
}

scp_r<-terra::rast(file.path(root,"atlas_hazards/soils/sscp_world.tif"))
# pick a WET cell: highest annual rain in the historic year (sample a mid tile)
prd<-file.path(root,"nex-gddp-cmip6","pr","historical",GCM)
prtot<-sum(terra::rast(sort(list.files(prd,pattern=paste0("^pr_",HYR,"-.*\\.tif$"),full.names=TRUE))))
wcell<-terra::xyFromCell(prtot, which.max(terra::values(prtot)))
cellxy<-data.frame(x=wcell[1],y=wcell[2])
soilcp<-as.numeric(terra::extract(scp_r,cellxy)[1,2])
cat(sprintf("Wettest cell: (%.2f, %.2f)  soilcp(TAW)=%.1f mm\n", wcell[1],wcell[2],soilcp))

for (cfg in list(c("HISTORIC","historical",HYR), c("FUTURE",FSSP,FYR))) {
  d<-yr_cell(cfg[2],cfg[3],cellxy)
  tmean<-(d$tmax+d$tmin)/2
  et<-peest_cell(d$rsds,d$tmin,tmean,d$tmax)
  er_empty<-run_cell(d$pr,et,soilcp,0)            # AVAIL=0 seed (legacy)
  er_fc   <-run_cell(d$pr,et,soilcp,soilcp)       # field-capacity seed
  cat(sprintf("\n%-8s %s: pr=%.2f ET=%.2f mm/day | NDWS(empty-seed)=%d  NDWS(FC-seed)=%d days/yr\n",
      cfg[1], cfg[3], mean(d$pr), mean(et), sum(er_empty<0.5), sum(er_fc<0.5)))
}
cat("\nRead-off: if HISTORIC NDWS(empty)>>NDWS(FC) but they converge under FC-seed,\n",
    "the saturation is the EMPTY-soil spin-up -> fix = seed at field capacity (as v2 does).\n",
    "If ET(historic)>>ET(future) despite cooler temps, peest2 is the culprit.\n", sep="")
