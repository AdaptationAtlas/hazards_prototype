# Free up space deleting Nex-GDDP-CMIP6 raw files
# By: H. Achicanoy
# Alliance Bioversity-International & CIAT, 2025

# The purpose of this script is to free up space in our
# storage system so that we can process the baselines.
# However, it has one disadvantage: if any pre-processed
# file became corrupted, we must download the raw file
# to fix it again. BE CAREFUL WITH ITS USE.

vrs <- c('pr','tasmax','tasmin','hurs','rsds')

# Irreversible deletion. Default DRY_RUN=TRUE: only prints targets.
# Set DRY_RUN=FALSE in the environment to actually delete. (roadmap rank 2)
DRY_RUN <- as.logical(Sys.getenv('DRY_RUN', 'TRUE'))
stopifnot(!is.na(DRY_RUN))

for (vr in vrs) {

  pth <- file.path('~/common_data/nex-gddp-cmip6_raw',vr)
  drs <- list.dirs(path = pth, recursive = F)
  drs <- drs[grep('ssp', drs)]

  for (dr in drs) {
    if (!dir.exists(dr)) { cat('Skip (absent): ',dr,'\n'); next }
    if (DRY_RUN) {
      cat('[DRY_RUN] would delete: ',dr,'\n')
    } else {
      cat('Deleting: ',dr,'\n')
      unlink(dr, recursive = TRUE, force = TRUE)
    }
  }

}
