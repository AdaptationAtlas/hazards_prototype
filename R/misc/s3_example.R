require(s3fs)
require(terra)

# 2.2) Atlas s3 bucket #####
bucket_name <- "http://digital-atlas.s3.amazonaws.com"
bucket_name_s3<-"s3://digital-atlas"
s3<-s3fs::S3FileSystem$new(anonymous = T)

folder<-"risk_prototype/data/hazard_timeseries_mean/annual"
head(files_s3<-s3$dir_ls(file.path(bucket_name_s3, folder)))

s3$file_size(files_s3[5])
file_path<-file.path(bucket_name,folder,basename(files_s3[5]))
dat<-terra::rast(file_path)

terra::plot(dat)
