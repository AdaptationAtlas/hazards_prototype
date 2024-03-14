# Hazard Risk Generation Scripts
This repository processes monthly hazard data from the [hazards](https://github.com/AdaptationAtlas/hazards/tree/main) workflow to calculate hazard risks over historical and future climate scenarios.

## Data Availability
Currently, as the Atlas is being developed, the hazard time-series data for risk analysis are accessible internally to Atlas scientists on the EiA server at `~/common_data/atlas_hazards/cmip6`. The data are also available on [AWS S3](s3://digital-atlas/).

Additional datasets utilized in the risk analysis include:
1. [MapSPAM](https://mapspam.info/) 2017V2r3 production value dataset from s3://digital-atlas/MapSpam/raw/spam2017V2r3, which provides updated, country-specific production values.
2. Livestock production values from s3://digital-atlas/livestock_vop, sourced from [Herrero et al. (2013)](https://www.pnas.org/doi/full/10.1073/pnas.1308149110).
3. [Livestock numbers](https://data.apps.fao.org/catalog/organization/gridded-livestock-of-the-world-glw) as detailed by [Gilbert et al. (2018)](https://www.nature.com/articles/sdata2018227).
4. The African highlands mask from s3://digital-atlas/afr_highlands for differentiating livestock data between highland and tropical areas.
5. Total human population density per km² from s3://digital-atlas/population/, with data from [Worldpop](https://hub.worldpop.org/geodata/summary?id=24777) and the [OECD Africapolis dataset](https://africapolis.org/) for urban and rural classification.
6. [GGCMI Phase 3 crop calendars](https://zenodo.org/records/5062513).
7. [GeoBoundaries 6.0.0](https://github.com/wmgeolab/geoBoundaries), accessible in processed form at s3://digital-atlas/boundaries.
8. The [Ecocrop dataset](https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/ecocrop.csv).

Access to the s3 repository requires login credentials until it is made publicly available.

To recreate the GeoBoundaries 6.0.0 data, run the [download_and_combine_geoboundaries.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/download_and_combine_geoboundaries.R) and [process_geoboundaries](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/process_geoboundaries.R) scripts.

## Available Code
The repository is organized into four main workflows (0 to 3) within the project's [R](https://github.com/AdaptationAtlas/hazards_prototype/tree/main/R) folder, alongside additional functions and data processing scripts.

### Cloning the Repository
To access the code, it is recommended to clone the repository locally, as most scripts require adjustments, such as specifying local directories. Use the following command to clone:

```bash
git clone https://github.com/AdaptationAtlas/hazards_protoype.git
```

### Process 0: Summarizing Hazards
The script [0_make_timeseries.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/0_make_timeseries.R) summarizes monthly hazard data from the [hazards](https://github.com/AdaptationAtlas/hazards/tree/main) workflow annually or seasonally. The summarization method (sum, mean, min, max) depends on the hazard type, as defined in the [haz_metadata](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/metadata/haz_metadata.csv). For future scenarios, data from each model are processed and then ensembled to provide ensemble mean and standard deviation. The output is saved in the `hazard_timeseries` folder.

### Process 1: Calculating Hazard Risk
[1_calculate_risks.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/1_calculate_risks.R) calculates crop-specific hazard risks for historical and future scenarios. Hazards are classified based on thresholds and directions specified in [haz_classes](https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/haz_metadata.csv) or using [Ecocrop](https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/ecocrop.csv) for crop environmental requirements. This process involves classifying hazard time series, calculating pixelwise time series average risks, and assembling these risks into raster stacks for each crop and severity combination. Additionally, hazard means, standard deviations, and interactions are analyzed to provide comprehensive risk assessments.

### Process 2: Intersecting Hazard Risk with Exposure
[2_risk_x_exposure.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/2_risk_x_exposure.R) intersects hazard risk data with exposure information, such as crop or animal production values or human population. It extracts exposure and risk data by administrative boundaries, reformats it into a tabular format, and merges it for easy analysis. This script prepares the data for use in the Africa Adaptation Atlas website by processing exposure variables, extracting hazard risks by administrative vectors, and multiplying risk raster stacks by exposure values. Script [2.1_risk_x_exposure_x_ac.R](https://github.com/AdaptationAtlas/hazards_prototype/blob/main/R/2.1_risk_x_exposure_x_ac.R), enhances the risk and exposure data by appending vulnerability information, providing a comprehensive view of risk across various scenarios.

## Process 3: Calculation of return on investment data
*Description coming soon*

## Conventions
Crops and hazards are named following [MapSPAM](https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/SpamCodes.csv) and [Hazards-definitions](https://github.com/AdaptationAtlas/hazards/wiki/Hazards-definitions) respectively. File naming is being harmonized to align with atlas guidelines, and changes to scripts and file names are expected in future versions.

## Funding
This project, part of Phase II of the Africa Adaptation Atlas Project, is funded by the Bill & Melinda Gates Foundation.
