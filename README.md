# Hazard Risk Generation Scripts
This repository takes the monthly hazard data generated in the [hazards](https://github.com/AdaptationAtlas/hazards/tree/main) workflow and calculates the risk of a hazard occurring over historical and future climate scenarios.

## Where is the data?
While the Atlas is under development, the hazard time-series data used in the risk analysis is internally available to Atlas scientists in the EiA server under `~/common_data/atlas_hazards/cmip6`. The data are also available in the [AWS S3](s3://digital-atlas/).

Other dataset downloaded and used during the risk analysis include:
1. MapSPAM 2017V2r3 value of production available from s3://digital-atlas/MapSpam/raw/spam2017V2r3. *This is an updated version of the dataset that fixes several bugs and using country specific value of production values rather than global*
2. Livestock value of production available from s3://digital-atlas/livestock_vop and were provided the authors of [Herrero et al. (2013)](https://www.pnas.org/doi/full/10.1073/pnas.1308149110)
3. [Livestock numbers](https://data.apps.fao.org/catalog/organization/gridded-livestock-of-the-world-glw) from [Gilbert et al. (2018)](https://www.nature.com/articles/sdata2018227)
4. African highlands available from s3://digital-atlas/afr_highlands. *This dataset is used to split livestock data into highland vs tropical areas*
5. Human population available from s3://digital-atlas/population/ and was downloaded from worldpop 2020.
6. [GGCMI Phase 3 crop calendars](https://zenodo.org/records/5062513)
7. [Geoboundaries 6.0.0](https://github.com/wmgeolab/geoBoundaries) downloadable in processed form at s3://digital-atlas/boundaries.
8. [Ecocrop](https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/ecocrop.csv)

Note, until the s3 repository is made publically available you will need login credentials for s3://digital-atlas the bucket to run these scripts.

To recreate the Geoboundaries 6.0.0 data run the [download_and_combine_geoboundaries.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/download_and_combine_geoboundaries.R) then [process_geoboundaries](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/process_geoboundaries.R) scripts.

## What code is available?
This repository is divided into 4 main workflows, 0 to 3, available in the project [R](https://github.com/AdaptationAtlas/hazards_prototype/tree/main/R) folder. Other functions and miscellaneous data processing scripts are also contained with the folder.

The below provides an overview of each process, and some of the functions therein.

To get access to the code it is best to first clone the repository locally, as most of the scripts require small modifications (e.g., local folders). To clone the repository, use

```bash
git clone https://github.com/AdaptationAtlas/hazards_protoype.git
```
## 0) Summarize hazards within years or seasons
The script [0_make_timeseries.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/0_make_timeseries.R) takes the monthly hazard data generated from [hazards](https://github.com/AdaptationAtlas/hazards/tree/main) and summarizes the data either annually or seasonally using a crop calendar. The summary function (sum, mean, min or max) varies for each hazard and is specified in the [haz_metadata](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/metadata/haz_metadata.csv) table. For future scenarios each model is processed and then models are ensembled to give the ensemble mean and standard deviation.

## 1) Calculate hazard risk across a timeseries of years or seasons
The script [1_calculate_risks.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/1_calculate_risks.R) applies a series of steps to calculate the crop specific risk of hazards occurring for historical and future scenarios. Risk is calculated for different levels of severity.

Hazards are defined using thresholds and directions in the [haz_classes table](https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_metadata.csv) or specified using [Ecocrop](https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/ecocrop.csv) crop environmental requirements. When using ecocrop a moderate hazard is `hazard_value>optimum & hazard_value<=(optimum+absolute)/2`, a severe hazard is `hazard_value>(optimum+absolute)/2 & hazard_value<=absolute`, and a extreme hazard is `hazard_value>absolute`, in this example the directionality of the thresholding is greater than, but it can also be less than. Some hazards have both greater than and less than thresholds, for example precipitation `PTOT` can too much leading to waterlogging or too little leading to drought. Such hazards are renamed with a suffixes to indicate greater than, e.g. `PTOT_L`, or greater than, e.g. `PTOT_H`.

The major processes in the script are:
1. The summarized hazards timeseries calculated in step 0 are classified according to all the different thresholds required for each crop and severity combination. This process generates timeseries raster stacks of binary hazard presence/absence where each layer of the stack is a year or season, these are saved in the   `hazard_timeseries_class` directory.
2. Risk is calculated as the pixelwise timeseries average for each classified stack generate in step 1. For example, if pixel has values `0,0,1,0,1` the risk is 0.2. This process generates a directory `hazard_timeseries_risk` containing single layer rasters of risk (0-1).
3. The risks corresponding to crop x severity combination are assembled into a raster stack, e.g. `arabica coffee-moderate.tif`, saved in the `hazard_risk` directory.
4. The hazard mean and sd are calculated across the summarized hazard timeseries raster stacks calculated in step 0, the resulting single layer rasters are saved in the `haz_mean_dir` and `haz_sd_dir` directories. Change in mean values between historic and future scenarios can also be calculated in this step.
5. Risks for the interaction of dry, heat, and wet hazards are calculated, again this is crop and severity class specific. Note at present we are only considering hazard interactions within severity classes (moderate dry, moderate heat, and moderate wet) and not between classes. The risk of any hazard is calculated and then the risks for hazard combinations: a) dry only, b) heat only, c) wet only, d) dry+heat only, e) dry+wet only, f) heat+wet only and e) dry+heat+wet only. Each set of risk interaction is saved within a folder `NDWS-G15+NTx35-G7+NDWL0-G2` in the `hazard_timeseries_int` directory.


