# Hazard Risk Generation Scripts
This repository takes the monthly hazard data generated in the [hazards](https://github.com/AdaptationAtlas/hazards/tree/main) workflow and calculates the risk of a hazard occurring over historical and future climate scenarios.

## Where is the data?
While the Atlas is under development, the hazard time-series data used in the risk analysis is internally available to Atlas scientists in the EiA server under `~/common_data/atlas_hazards/cmip6`. The data are also available in the [AWS S3](s3://digital-atlas/).

Other dataset downloaded and used during the risk analysis include:
1. [MapSPAM](https://mapspam.info/) 2017V2r3 value of production available from s3://digital-atlas/MapSpam/raw/spam2017V2r3. *This is an updated version of the dataset that fixes several bugs and using country specific value of production values rather than global*
2. Livestock value of production available from s3://digital-atlas/livestock_vop and were provided the authors of [Herrero et al. (2013)](https://www.pnas.org/doi/full/10.1073/pnas.1308149110)
3. [Livestock numbers](https://data.apps.fao.org/catalog/organization/gridded-livestock-of-the-world-glw) from [Gilbert et al. (2018)](https://www.nature.com/articles/sdata2018227)
4. African highlands mask available from s3://digital-atlas/afr_highlands. *This dataset is used to split livestock data into highland vs tropical areas*
5. Total human population headcount density per km of land. available from s3://digital-atlas/population/. Human population was taken from [Worldpop](https://hub.worldpop.org/geodata/summary?id=24777) and separated into urban and rural classes using the [OECD Africapolis dataset](https://africapolis.org/).
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
## Process 0: Summarize hazards within years or seasons
The script [0_make_timeseries.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/0_make_timeseries.R) takes the monthly hazard data generated from [hazards](https://github.com/AdaptationAtlas/hazards/tree/main) and summarizes the data either annually or seasonally using a crop calendar. The summary function (sum, mean, min or max) varies for each hazard and is specified in the [haz_metadata](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/metadata/haz_metadata.csv) table. For future scenarios each model is processed and then models are ensembled to give the ensemble mean and standard deviation. Output data are saved to the `hazard_timeseries` folder.

## Process 1: Calculate hazard risk across a timeseries of years or seasons
The script [1_calculate_risks.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/1_calculate_risks.R) applies a series of steps to calculate the crop specific risk of hazards occurring for historical and future scenarios. Risk is calculated for different levels of severity.

Hazards are defined using thresholds and directions in the [haz_classes table](https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_metadata.csv) or specified using [Ecocrop](https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/ecocrop.csv) crop environmental requirements. When using ecocrop, depending on the directionality, we classify a moderate hazard as `>optimum` or `<optimum`, a severe hazard as `>(optimum+absolute)/2` or `<(optimum+absolute)/2`, and a extreme hazard as `>absolute` or `<absolute`. Some hazards have both greater than and less than thresholds, for example precipitation `PTOT` can too much leading to waterlogging or too little leading to drought. Such hazards are renamed with a suffixes to indicate greater than, e.g. `PTOT_L`, or greater than, e.g. `PTOT_H`.

The major processes in the script are:
1. The summarized hazards timeseries calculated in step 0 are classified according to all the different thresholds required for each crop and severity combination. This process generates timeseries raster stacks of binary hazard presence/absence where each layer of the stack is a year or season. Output stacks are saved in the   `hazard_timeseries_class` directory, e.g. as `hazard_timeseries_class/historical_PTOT_sum-G1875.tif`, where the suffix `-G1875` indicates the hazard is classified as present (i.e. 1) when the value of the hazard variable `PTOT_sum` is greater than 1875. A suffix `-L500` would indicate less than 500.
2. Risk is calculated as the pixelwise timeseries average for each classified stack generate in step 1. For example, if pixel has values `0,0,1,0,1` the risk is 0.2. This process generates a directory `hazard_timeseries_risk` containing single layer rasters of risk (0-1). Files are named as per step 1.
3. The risks corresponding to crop x severity combination are assembled into a raster stack, e.g. `arabica coffee-moderate.tif`, saved in the `hazard_risk` directory.
4. The hazard mean and sd are calculated across the summarized hazard timeseries raster stacks calculated in step 0, the resulting single layer rasters are saved in the `haz_mean_dir` and `haz_sd_dir` directories. Change in mean values between historic and future scenarios can also be calculated in this step.
5. Risks for the interaction of dry, heat, and wet hazards are calculated, again this is crop and severity class specific. Note at present we are only considering hazard interactions within severity classes (moderate dry, moderate heat, and moderate wet) and not between classes. The risk of any hazard is calculated and then the risks for hazard combinations: a) dry only, b) heat only, c) wet only, d) dry+heat only, e) dry+wet only, f) heat+wet only and e) dry+heat+wet only. The results of each risk interaction are saved within a folder `NDWS-G15+NTx35-G7+NDWL0-G2` in the `hazard_timeseries_int` directory. Within each folder are single layer rasters that contain the risk (0-1) of each combination occurring named by scenario and timeseries, e.g. `historic-historic-any.tif` or `ssp245-2021_2040-dry+heat.tif`. 
6. All the hazard interactions relevant to a specific crop and severity class are then compiled into a single raster stack, e.g. `arabica coffee-moderate-int.tif` which is also saved in the `hazard_risk` directory.

## Process 2: Intersect hazard risk with exposure
The script [2_risk_x_exposure.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/2_risk_x_exposure.R) downloads and prepares exposure data such as crop or animal value of production or human population and intersects it with the crop and severity risk stacks in the `hazard_risk` folder generated in steps 3 and 6 in workflow script 1 above. It also extracts exposure, risk and risk*exposure data by admin0, admin1 and admin2 geoboundaries to create vector objects saved as `geoparquet` files, for example. Finally the geoparquet data are reformatted into long tabular form and merged so that data can easily be subset on selections such as admin, scenario, timeframe, crop, severity, and/or hazard. Merged tables are saved in `parquet` format and these tables underlie the observable notebooks that power the Africa Adaptation Atlas Website.

The major processes in the script are:
0. Exposure variables are downloaded, processed and extracted by admin0, admin1 and admin2 geoboundaries. Exposure data is saved to the `exposure` directory which contains raster stacks, geoparquet vector objects and tabular parquet data for specific hazards:
   - **crop_ha** = harvested area (ha/pixel)
   - **crop_vop_usd17** = value of production in USD/pixel (to generate these data MapSPAM crop production was multiplied by 2017 [crop producer prices](https://fenixservices.fao.org/faostat/static/bulkdownloads/Prices_E_Africa.zip) obtained from FAOstat. These data are created by the [fao_producer_prices.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/fao_producer_prices.R) script.
   - **crop_vop** =  MapSPAM value of crop production in 2005 international dollars/pixel
   - **hpop** = rural, urban and total human population headcount/pixel (2020)
   - **livestock_no** = number of animals/pixel per livestock species
   - **livestock_vop_usd17** = value of production in USD/pixel, generated using a similar process to crop_vop_usd17 by the [fao_producer_prices_livestock.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/fao_producer_prices_livestock.R) script.
   - **livestock_vop** = value of livestock production in 2005 international dollars/pixel
  1. Hazard risk risk is extracted by admin vectors and saved to the `hazard_risk` directory. Geoparquets are named, for example, `haz_risk_int_adm0_extreme.parquet` or `haz_risk_solo_adm1_moderate.parquet` where a) `int` or `solo` indicate whether the file contains risk for hazards alone or as interactions of dry, heat, wet hazard, and b) `adm0` `adm1` `adm2` indicate the administrative level at which data was extracted. Files with `_adm` in their name, e.g. `haz_risk_solo_adm_severe.parquet` merge data across administrative levels into a long tabular form.
  2. Hazard means (process 1, step 4) are extracted by admin vectors and saved to the `haz_mean_dir` as geoparquets, e.g. `haz_means_adm0.parquet` and combined into long tabular form `haz_means_adm.parquet`.
  3. Hazard timeseries (process 0) are extracted by admin vectors and saved to the `hazard_timeseries` as geoparquets, e.g. `haz_timeseries_adm0.parquet` and combined into long tabular form `haz_timeseries_adm.parquet`.
  4. The crop or animal specfic risk raster stacks in the `hazard_risk` directory are multiplied by their corresponding exposure values then saved to a renamed directory, for example if multiple by value of production (vop) then the folder becomes `hazard_risk_vop` and the stacks are renamed to `arabica coffee-extreme-vop.tif`, for example. Risk x exposure is then extracted and saved as per step 1.

The script [2.1_risk_x_exposure_x_ac.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/2.1_risk_x_exposure_x_ac.R) takes the tabular risk x exposure parquet data produced in process 2:step 4, binds the three severity classes into a single table, and then merges this table with the vulnerability data contained in `s3://digital-atlas/vulnerability/vulnerability_adm_long.parquet`. The script also provides functions to subset the data into small chunks for easier loading into notebooks.

## Process 3: Calculation of return on investment data
*Description coming soon*

## Conventions
**Crops** are named according to [MapSPAM conventions](https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SpamCodes.csv). MapSPAM names and codes can be harmonized with FAO names using a the [SPAM2010_FAO_crops](https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SPAM2010_FAO_crops.csv) table.
**Hazards** codes or ackronyms are described in the [Hazards-definitions](https://github.com/AdaptationAtlas/hazards/wiki/Hazards-definitions) wiki.
Harmonization of file-naming according to atlas guidelines is in the pipeline, so expect scripts, folder, file and layer names to change with future versions of this R-project.

## Funding
This project, part of Phase II of the Africa Adaptation Atlas Project, is funded by the Bill & Melinda Gates Foundation.
