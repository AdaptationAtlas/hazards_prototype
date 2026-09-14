# Issue #19 — exposure vs GYGA reproduction (Kenya maize)

Generated 2026-09-14 15:51 EAT by R/checks/19_exposure_vs_gyga_kenya.R

## V1 exposure — % of maize VoP exposed, severe (live notebook file)

| admin1_name | scenario | timeframe | vop_total_M | any | dry | dry+heat | dry+heat+wet | dry+wet | heat | heat+wet | no hazard | wet |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Bungoma | historic | historic | 21.6 | 2.1 | 0 | 0 | 0 | 0 | 0 | 0 | 97.9 | 2.1 |
| Bungoma | ssp585 | 2041_2060 | 21.6 | 3.3 | 0 | 0 | 0 | 0 | 0 | 0 | 96.7 | 3.3 |
| Meru | historic | historic | 21.8 | 92.5 | 91.4 | 1.1 | 0 | 0 | 0 | 0 | 7.5 | 0 |
| Meru | ssp585 | 2041_2060 | 21.8 | 92.4 | 87.5 | 4.9 | 0 | 0 | 0.1 | 0 | 7.6 | 0 |
| Nandi | historic | historic | 11.6 | 2.5 | 0.1 | 0 | 0 | 0 | 0 | 0 | 97.5 | 2.4 |
| Nandi | ssp585 | 2041_2060 | 11.6 | 2.9 | 0 | 0 | 0 | 0 | 0 | 0 | 97.1 | 2.8 |
| Narok | historic | historic | 22.6 | 37.9 | 37.9 | 0 | 0 | 0 | 0 | 0 | 62.1 | 0 |
| Narok | ssp585 | 2041_2060 | 22.6 | 35 | 35 | 0 | 0 | 0 | 0 | 0 | 65 | 0 |

## GYGA — maize, SSP585, cultivar current (live notebook table `aggregated`)

yw_rfd = rainfed water-limited POTENTIAL yield (t/ha), yw_irr = irrigated potential yield. horizon 2005 = 1995-2014 baseline, 2050 = 2041-2060.

| admin1_name | horizon | cycle | yw_rfd | rfd_abs_chg | rfd_pct_chg | yw_irr | irr_pct_chg |
|---|---|---|---|---|---|---|---|
| Bungoma | 2005 | 1 | 8.93 | NA | NA | 10.91 | NA |
| Bungoma | 2030 | 1 | 8.55 | -0.38 | -4.2 | 10.36 | -5 |
| Bungoma | 2050 | 1 | 8.18 | -0.74 | -8.3 | 9.78 | -10.3 |
| Bungoma | 2005 | both | 8.93 | NA | NA | 10.91 | NA |
| Bungoma | 2030 | both | 8.55 | -0.38 | -4.2 | 10.36 | -5 |
| Bungoma | 2050 | both | 8.18 | -0.74 | -8.3 | 9.78 | -10.3 |
| Meru | 2005 | 1 | 3.3 | NA | NA | 7.74 | NA |
| Meru | 2030 | 1 | 2.86 | -0.44 | -13.2 | 7.17 | -7.4 |
| Meru | 2050 | 1 | 2.41 | -0.89 | -26.9 | 6.49 | -16.1 |
| Meru | 2005 | 2 | 2.73 | NA | NA | 8.41 | NA |
| Meru | 2030 | 2 | 2.69 | -0.04 | -1.5 | 7.84 | -6.8 |
| Meru | 2050 | 2 | 2.52 | -0.21 | -7.8 | 7.1 | -15.6 |
| Meru | 2005 | both | 3.02 | NA | NA | 8.07 | NA |
| Meru | 2030 | both | 2.78 | -0.24 | -7.9 | 7.5 | -7.1 |
| Meru | 2050 | both | 2.46 | -0.55 | -18.3 | 6.8 | -15.8 |
| Nandi | 2005 | 1 | 10.08 | NA | NA | 12.63 | NA |
| Nandi | 2030 | 1 | 9.54 | -0.54 | -5.3 | 12.02 | -4.8 |
| Nandi | 2050 | 1 | 9.26 | -0.82 | -8.1 | 11.37 | -9.9 |
| Nandi | 2005 | both | 10.08 | NA | NA | 12.63 | NA |
| Nandi | 2030 | both | 9.54 | -0.54 | -5.3 | 12.02 | -4.8 |
| Nandi | 2050 | both | 9.26 | -0.82 | -8.1 | 11.37 | -9.9 |
| Narok | 2005 | 1 | 9.88 | NA | NA | 14.5 | NA |
| Narok | 2030 | 1 | 9.17 | -0.7 | -7.1 | 13.72 | -5.4 |
| Narok | 2050 | 1 | 8.94 | -0.93 | -9.5 | 12.95 | -10.7 |
| Narok | 2005 | both | 9.88 | NA | NA | 14.5 | NA |
| Narok | 2030 | both | 9.17 | -0.7 | -7.1 | 13.72 | -5.4 |
| Narok | 2050 | both | 8.94 | -0.93 | -9.5 | 12.95 | -10.7 |

## Kenya counties — exposure vs GYGA (sorted by exposure)

V1 counties without a GYGA row: Nyandarua, Nyeri, Mombasa, Lamu. Missing yields (NA): Nyandarua.

| admin1_name | vop_musd | exp_hist | exp_2050 | d_exp_pp | dry | heat | wet | yw_base | yw_2050 | yw_abs | yw_pct | irr_pct |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Nyamira | 9.5 | 0.6 | 0.8 | 0.3 | 0 | 0 | 0.8 | 9.01 | 8.12 | -0.89 | -9.9 | -10.8 |
| Busia | 8.1 | 0.9 | 1.6 | 0.7 | 0.4 | 0 | 1.2 | 8.77 | 7.68 | -1.08 | -12.3 | -11.7 |
| Kericho | 11.1 | 2.1 | 1.8 | -0.3 | 1.2 | 0 | 0.5 | 10.44 | 9.44 | -1 | -9.6 | -10.4 |
| Kisii | 12.6 | 1.2 | 2.4 | 1.2 | 0 | 0 | 2.4 | 6.46 | 6.78 | 0.32 | 4.9 | -10.6 |
| Trans Nzoia | 25.5 | 4.5 | 2.5 | -2 | 2.5 | 0 | 0 | 8.15 | 8.74 | 0.58 | 7.2 | -7 |
| Nandi | 11.6 | 2.5 | 2.9 | 0.4 | 0 | 0 | 2.8 | 10.08 | 9.26 | -0.82 | -8.1 | -9.9 |
| Bungoma | 21.6 | 2.1 | 3.3 | 1.2 | 0 | 0 | 3.3 | 8.93 | 8.18 | -0.74 | -8.3 | -10.3 |
| Kisumu | 6 | 9.4 | 5 | -4.4 | 4.9 | 0 | 0.1 | 9.01 | 8.12 | -0.89 | -9.9 | -10.8 |
| Uasin Gishu | 18.5 | 6.8 | 5.3 | -1.5 | 5.2 | 0 | 0.1 | 8.6 | 8.98 | 0.39 | 4.5 | -7.3 |
| Vihiga | 4.6 | 2.5 | 5.6 | 3.1 | 0 | 0 | 5.6 | 9.01 | 8.12 | -0.89 | -9.9 | -10.8 |
| Bomet | 12 | 10.8 | 7.9 | -2.9 | 7.5 | 0 | 0.4 | 12.3 | 11.15 | -1.15 | -9.3 | -9.9 |
| Kakamega | 20.6 | 5.9 | 8 | 2 | 0 | 0 | 8 | 8.83 | 8.25 | -0.58 | -6.5 | -9.8 |
| Siaya | 12.4 | 12.7 | 8.1 | -4.6 | 7.3 | 0 | 0.8 | 7.76 | 6.8 | -0.97 | -12.5 | -12 |
| Homa Bay | 15.5 | 15.4 | 8.1 | -7.3 | 7.6 | 0 | 0.5 | 5.9 | 5.58 | -0.32 | -5.5 | -12.1 |
| Migori | 13.6 | 19.5 | 9.8 | -9.7 | 9.7 | 0 | 0.1 | 5.54 | 5.36 | -0.18 | -3.3 | -12.1 |
| Elgeyo-Marakwet | 7.9 | 11.1 | 10.2 | -0.9 | 10 | 0 | 0.2 | 6.03 | 6.03 | 0.01 | 0.1 | -10.8 |
| Narok | 22.6 | 37.9 | 35 | -2.8 | 35 | 0 | 0 | 9.88 | 8.94 | -0.93 | -9.5 | -10.7 |
| West Pokot | 7 | 44.5 | 40.9 | -3.6 | 40.9 | 1.2 | 0 | 5.7 | 5.85 | 0.14 | 2.5 | -10.3 |
| Baringo | 6.9 | 49.9 | 50.9 | 1 | 50.9 | 0.5 | 0 | 5.19 | 5.3 | 0.11 | 2.1 | -12.1 |
| Nyandarua | 3.6 | 57.9 | 60.7 | 2.8 | 60.7 | 0 | 0 | NA | NA | NA | NA | NA |
| Nakuru | 15.5 | 60.8 | 63 | 2.2 | 63 | 0 | 0 | 6.72 | 7.18 | 0.46 | 6.8 | -11.3 |
| Laikipia | 8.8 | 75.4 | 77.3 | 1.9 | 77.3 | 0 | 0 | 5.02 | 5.75 | 0.73 | 14.5 | -11.3 |
| Nyeri | 6.7 | 75.6 | 78 | 2.4 | 78 | 0 | 0 | NA | NA | NA | NA | NA |
| Murang'a | 10.5 | 80 | 81.6 | 1.5 | 81.6 | 0 | 0 | 4.27 | 3.71 | -0.57 | -13.2 | -11.1 |
| Kiambu | 8.3 | 78.5 | 82.5 | 3.9 | 82.5 | 0 | 0 | 3.98 | 3.3 | -0.68 | -17.1 | -11.3 |
| Kirinyaga | 6.9 | 85.3 | 86.4 | 1.1 | 86.4 | 0 | 0 | 3.98 | 3.3 | -0.68 | -17.1 | -11.3 |
| Tharaka-Nithi | 8.6 | 87.4 | 87.7 | 0.3 | 87.2 | 23.9 | 0 | 2.76 | 2.27 | -0.49 | -17.8 | -16.5 |
| Embu | 11.9 | 87.4 | 88.1 | 0.6 | 87.7 | 13.1 | 0 | 3.75 | 3.1 | -0.65 | -17.4 | -12.4 |
| Samburu | 0.4 | 89.1 | 89.5 | 0.4 | 89.5 | 0.5 | 0 | 2.56 | 3.17 | 0.62 | 24.1 | -13.6 |
| Mombasa | 0.1 | 55.7 | 90.8 | 35.1 | 50.7 | 90.1 | 0 | NA | NA | NA | NA | NA |
| Kwale | 3.5 | 89.5 | 91.9 | 2.3 | 88.4 | 62.7 | 0 | 3.44 | 3.04 | -0.39 | -11.4 | -13 |
| Kilifi | 10.2 | 86.6 | 92 | 5.4 | 84.5 | 88.1 | 0 | 3.09 | 2.76 | -0.32 | -10.5 | -13.3 |
| Meru | 21.8 | 92.5 | 92.4 | -0 | 92.4 | 5 | 0 | 3.02 | 2.46 | -0.55 | -18.3 | -15.8 |
| Nairobi | 0.5 | 92.7 | 94.6 | 1.9 | 94.6 | 0 | 0 | 3.77 | 3.22 | -0.55 | -14.7 | -11.4 |
| Machakos | 36.3 | 97.4 | 96.9 | -0.5 | 96.9 | 0.1 | 0 | 3.88 | 3.26 | -0.62 | -15.9 | -11.4 |
| Marsabit | 2.4 | 98.2 | 97.1 | -1.1 | 97.1 | 7.6 | 0 | 2.17 | 1.96 | -0.21 | -9.9 | -14.5 |
| Kajiado | 9.3 | 97.5 | 97.4 | -0.1 | 97.4 | 0.7 | 0 | 3.46 | 2.99 | -0.47 | -13.6 | -12.3 |
| Taita Taveta | 6.4 | 98.7 | 97.6 | -1.1 | 97.6 | 0.3 | 0 | 3.81 | 3.73 | -0.08 | -2.1 | -13.4 |
| Kitui | 28.8 | 99.7 | 99.1 | -0.6 | 99 | 23.7 | 0 | 3.05 | 2.52 | -0.53 | -17.4 | -13.1 |
| Lamu | 1.6 | 98.8 | 99.2 | 0.4 | 98.6 | 95.2 | 0 | NA | NA | NA | NA | NA |
| Isiolo | 0.6 | 99.3 | 99.3 | 0 | 99.2 | 23.7 | 0 | 1.39 | 1.32 | -0.07 | -4.8 | -11 |
| Makueni | 23.5 | 99.6 | 99.6 | -0 | 99.6 | 6.4 | 0 | 3.18 | 2.64 | -0.54 | -17 | -12.1 |
| Turkana | 0.5 | 99.8 | 99.9 | 0.2 | 99.2 | 92.2 | 0 | 1.3 | 1.25 | -0.05 | -4.2 | -15.2 |
| Garissa | 0.1 | 100 | 100 | 0 | 99.5 | 91.5 | 0 | 1.84 | 1.83 | -0.01 | -0.4 | -13.9 |
| Tana River | 0.4 | 99.9 | 100 | 0.1 | 95.4 | 100 | 0 | 1.86 | 1.85 | -0.01 | -0.4 | -14 |
| Mandera | 0.6 | 100 | 100 | 0 | 100 | 47.8 | 0 | 1.46 | 1.45 | -0.01 | -0.8 | -9.8 |
| Wajir | 0.6 | 100 | 100 | 0 | 100 | 42.9 | 0 | 2.27 | 2.2 | -0.07 | -3 | -13.3 |

## Correlations across counties

| pair | spearman | pearson | n |
|---|---|---|---|
| exposure any% vs GYGA rainfed % change | -0.14 | -0.14 | 43 |
| exposure any% vs GYGA rainfed abs change (t/ha) | 0.34 | 0.24 | 43 |
| exposure dry% vs GYGA rainfed % change | -0.16 | -0.14 | 43 |
| d exposure (pp) vs GYGA rainfed % change | -0.13 | -0.08 | 43 |
| exposure any% vs GYGA BASELINE Yw | -0.91 | -0.9 | 43 |
| exposure any% vs GYGA IRRIGATED % change | -0.65 | -0.62 | 43 |

## GYGA change by exposure band

| exposure_band | n | mean_yw_base | mean_yw_abs | mean_yw_pct | mean_irr_pct |
|---|---|---|---|---|---|
| <10% | 15 | 8.59 | -0.55 | -5.9 | -10.4 |
| 10-50% | 3 | 7.2 | -0.26 | -2.3 | -10.6 |
| 50-90% | 9 | 4.25 | -0.13 | -3.9 | -12.3 |
| >90% | 16 | 2.69 | -0.28 | -9 | -13 |

## Quadrants: exposure < 10 % vs GYGA loss worse than −5 %

| low_exposure | big_loss | N |
|---|---|---|
| TRUE | TRUE | 11 |
| TRUE | FALSE | 4 |
| FALSE | TRUE | 15 |
| FALSE | FALSE | 13 |

## Kenya VoP-weighted maize exposure (any severe hazard)

| scenario | timeframe | vop_weighted_exposure_pct | counties |
|---|---|---|---|
| historic | historic | 49.2 | 47 |
| ssp585 | 2041_2060 | 48.6 | 47 |

## GYGA bit-identical county clusters

| yw_base | yw_pct | counties | n |
|---|---|---|---|
| 9.01 | -9.9 | Nyamira, Kisumu, Vihiga | 3 |
| 3.98 | -17.1 | Kiambu, Kirinyaga | 2 |

## 2025-07 bake — % of maize VoP exposed (ENSEMBLE mean, severe, annual, usd15; denominator = harmonized usd15 tech=all)

den_all_M < den_rfall_M for Nandi/Meru is the harmonized-file anomaly audited below; with rf-all as denominator the % are ~3x smaller for those two.

| admin1_name | scenario | timeframe | den_all_M | den_rfall_M | any | dry | dry+heat | dry+heat+wet | dry+wet | heat | heat+wet | wet |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Bungoma | historic | historic | 107.4 | 108.2 | 2.5 | 0 | 0 | 0 | 0 | 0 | 0 | 2.5 |
| Bungoma | ssp585 | 2041-2060 | 107.4 | 108.2 | 8.2 | 0.1 | 0 | 0 | 0 | 0 | 0 | 8.1 |
| Meru | historic | historic | 0.7 | 1.8 | 86 | 70.1 | 15.9 | 0 | 0 | 0 | 0 | 0 |
| Meru | ssp585 | 2041-2060 | 0.7 | 1.8 | 85.9 | 59.4 | 25.6 | 0 | 0 | 0.9 | 0 | 0 |
| Nandi | historic | historic | 9.3 | 30 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 2 |
| Nandi | ssp585 | 2041-2060 | 9.3 | 30 | 5.3 | 0.4 | 0 | 0 | 0 | 0 | 0 | 4.9 |
| Narok | historic | historic | 82.8 | 86.3 | 63.3 | 63.3 | 0 | 0 | 0 | 0 | 0 | 0 |
| Narok | ssp585 | 2041-2060 | 82.8 | 86.3 | 60.1 | 60.1 | 0 | 0 | 0 | 0 | 0 | 0 |

## Harmonized VoP audit — tech=all vs rf-all (+irr)

| file | level | rows | all_lt_rfall | pct_all_lt_rfall | pct_all_eq_rfall_plus_irr |
|---|---|---|---|---|---|
| vop_nominal-usd-2015.parquet | adm0 | 1815 | 743 | 40.9 | 28.7 |
| vop_nominal-usd-2015.parquet | adm1 | 23628 | 6381 | 27 | 32.3 |
| vop_nominal-usd-2015.parquet | adm2 | 213906 | 28884 | 13.5 | 49.1 |
| vop_nominal-usd-2015.parquet | ALL | 239349 | 36008 | 15 | 46.4 |
| vop_intld15-2021.parquet | adm0 | 1760 | 0 | 0 | 100 |
| vop_intld15-2021.parquet | adm1 | 22912 | 0 | 0 | 100 |
| vop_intld15-2021.parquet | adm2 | 207424 | 0 | 0 | 100 |
| vop_intld15-2021.parquet | ALL | 232096 | 0 | 0 | 100 |

## Session

R 4.6.0; arrow 24.0.0; data.table 1.18.4; duckdb CLI: v1.5.2 (Variegata) 8a5851971f

