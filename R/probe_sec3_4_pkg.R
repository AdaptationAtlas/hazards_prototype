suppressMessages({library(data.table); library(arrow); library(future.apply)})
set.seed(1); dir<-tempfile("pk"); dir.create(dir)
fk<-c("srcid","admin1_name","scenario","model","hazard","season")
files<-vapply(1:4,function(s){g<-CJ(srcid=sprintf("s%d",s),admin1_name=sprintf("r%03d",1:300),scenario="ssp245",model=c("m1","m2"),hazard=c("PTOT","TMAX"),season=c("DJF","annual"),year=2021:2044)
 g[,gid:=.GRP,by=fk]; g[,value:=if(gid[1]%%2==0) as.numeric(arima.sim(list(ar=0.8),.N))+0.3*(year-2021) else 20+0.4*(year-2021)+rnorm(.N,0,2),by=fk]
 p<-file.path(dir,sprintf("s%d.parquet",s)); write_parquet(g,p); p},character(1))
# fit closure using the PACKAGE (no sourceCpp, no env)
make_fit<-function() function(year,value){n<-length(value); if(n<4L||!all(is.finite(value)))return(list(slope=NA_real_,p_value=NA_real_))
 t0<-trendkernel::mk_sen_cpp(value);s0<-t0$slope;i0<-median(value-s0*year);d<-value-(s0*year+i0);r<-trendkernel::lag1_ac_cpp(d)
 if(abs(r)<=0.1)return(list(slope=t0$slope,p_value=t0$p_value)); wr<-c(d[1],d[-1]-r*d[-n]);z<-wr+s0*year+i0;tz<-trendkernel::mk_sen_cpp(z);list(slope=tz$slope,p_value=tz$p_value)}
# reference (main process)
library(trendkernel); rf<-make_fit()
ref<-rbindlist(lapply(files,function(f) data.table(read_parquet(f))[,rf(year,value),by=fk]))
# worker: top-level-style FUN, loads the package itself (like the real script will)
worker<-function(f){ requireNamespace("trendkernel"); fit<-make_fit(); data.table(arrow::read_parquet(f))[,fit(year,value),by=fk] }
plan(multisession,workers=3)
new<-rbindlist(future_lapply(files,worker,future.seed=TRUE,future.packages="trendkernel"))
plan(sequential)
setkeyv(ref,fk);setkeyv(new,fk);m<-merge(ref,new,by=fk,suffixes=c(".r",".n"))
cat(sprintf("REF slopeNA=%.3f  NEW(multisession,pkg) slopeNA=%.3f  matched=%d/%d  max|slopeΔ|=%.2e\n",
  mean(is.na(ref$slope)),mean(is.na(new$slope)),nrow(m),nrow(ref),max(abs(m$slope.r-m$slope.n),na.rm=TRUE)))
ok<-mean(is.na(new$slope))<0.02 && nrow(m)==nrow(ref) && max(abs(m$slope.r-m$slope.n),na.rm=TRUE)<1e-9
cat(if(ok)"PASS: packaged kernel correct under future multisession\n" else "FAIL\n")
