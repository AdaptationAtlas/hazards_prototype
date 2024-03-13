# Hazard Risk Generation Scripts
This repository takes the monthly hazard data generated in the [hazards](https://github.com/AdaptationAtlas/hazards/tree/main) workflow and calculates the risk of a hazard occurring over historical and future climate scenarios.

## Where is the data?
While the Atlas is under development, the hazard time-series data used in the risk analysis is internally available to Atlas scientists in the EiA server under `~/common_data/atlas_hazards/cmip6`. The data are also available in the [AWS S3](s3://digital-atlas/).

Other dataset downloaded and used during the risk analysis include:
1. MapSPAM 2017V2r3 value of production available from s3://digital-atlas/MapSpam/raw/spam2017V2r3. *This is an updated version of the dataset that fixes several bugs and using country specific value of production values rather than global*_
2. Livestock value of production available from s3://digital-atlas/livestock_vop and were provided the authors of [Herrero et al. (2013)](https://www.pnas.org/doi/full/10.1073/pnas.1308149110)
3. [Livestock numbers](https://data.apps.fao.org/catalog/organization/gridded-livestock-of-the-world-glw) from [Gilbert et al. (2018)](https://www.nature.com/articles/sdata2018227)
4. Human population
5. [GGCMI Phase 3 crop calendars](https://zenodo.org/records/5062513)
6. [Geoboundaries 6.0.0](https://github.com/wmgeolab/geoBoundaries) downloadable in processed form at s3://digital-atlas/boundaries. 

Note, until the s3 repository is made publically available you will need login credentials for s3://digital-atlas the bucket to run these scripts.

## What code is available?
This repository is divided into 4 main workflows, 0 to 3, available in the project [R](https://github.com/AdaptationAtlas/hazards_prototype/tree/main/R) folder. Other functions and miscellaneous data processing scripts are also contained with the folder.

The below provides an overview of each process, and some of the functions therein.

To get access to the code it is best to first clone the repository locally, as most of the scripts require small modifications (e.g., local folders). To clone the repository, use

```bash
git clone https://github.com/AdaptationAtlas/hazards_protoype.git
```
## Download and combine geoboundary data
To recreate the geoboundary data available at s3://digital-atlas/boundaries run the  [download_and_combine_geoboundaries.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/download_and_combine_geoboundaries.R) then [process_geoboundaries](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/process_geoboundaries.R) scripts.

## Calculate hazard timeseries
The script [0_make_timeseries.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/0_make_timeseries.R) takes the monthly hazard data generated from [hazards](https://github.com/AdaptationAtlas/hazards/tree/main) and summarizes the data either annually or seasonally using a crop calendar. The summary function (sum, mean, min or max) varies for each hazard and is specified in the [haz_metadata](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/metadata/haz_metadata.csv) table.

