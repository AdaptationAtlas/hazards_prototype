# DISPATCH — cglabs — observed hazards, long series 1981–2025 (for trends, normals and anomalies)

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### RESPONSE — cglabs 2026-11-13 — **W0-W4 done; W2 gate PASS (the area rows of all 12 tables are identical to before, max |diff| 0).** Once the season is read by its start year, crop-weighted Kenya shows the drought: the eastern counties plant in **October**, so the OND-2010 failure is season **"2010"**: Machakos/Makueni/Kitui z **+0.72 / +0.56 / +0.88**, positive but under 1 sd. The 2016 short rains read +1.39 / +0.96 / +1.19. Nothing published

Stamp `20261113_113311`. HEAD `ce61116`. Env grep empty before each launch. Scripts and outputs: `logs/obs_long_w{2_gate,3_recheck,3_calendar,3_county_cal}_<stamp>.{R,txt}`.

#### W0 — selftest and exposure rasters. Selftest PASS; **the two paths in the block do not exist**
`s7_trends_anomalies.R --selftest` → **21** ok. Neither `ls` glob matches anything: there is no `*res-25*.tif` under either `variable=` dir. What is there:
- **Crops:** `$NWD/Data/mapspam/2020V1r2_SSA/processed/variable=prod_t/spam_prod_t_all.tif` is the file R/3 reads (`R/3_freq_x_exposure.R:428`).
  - **42 layers, 0.05°**, extent -25.35..57.8 E, -46.95..40 N.
  - Layer names: `wheat, rice, maize, barley, pearl millet, small millet, sorghum, other cereals, potato, sweet potato, yams, cassava, …`. These are crop names, so they normalise to the CSV's `pearl-millet` etc.
  - The dir holds only `_adm_sum` parquets at res-25. **S7 resamples it to 0.25° by `average`, as designed. It does not log that it does** (0 `resampl` lines). Check: KEN maize is 3.64 Mt at 0.05° against 3.76 Mt as 25 × the 0.25° average, 3 % apart at the coast and border cells. The weights are relative within a unit, so the scale does not matter.
- **Livestock:** `$NWD/Data/GLW4_2020/processed/livestock_number_number_res-25.tif`, one level up from the `variable=number_number/` dir in the block, which holds only parquets. R/3 reads it as `livestock_number_number_<res tag>.tif`.
  - **12 layers, already on the 0.25° S7 lattice** (-180..180, -50..50).
  - Names: `poultry_tropical, sheep_tropical, pigs_tropical, goats_tropical, cattle_tropical, total_tropical, poultry_highland, sheep_highland, pigs_highland, goats_highland, cattle_highland, total_highland`.
  - `total_*` is not in `obs_crop_windows.csv`, so it is ignored. That is right, because it is TLU-weighted.
  - **Note:** the 10 matched layers are summed as raw heads, and **poultry is 2.14 of 3.43 billion heads (62 %)**. So `livestock_head` weighting is mostly a chicken map. A TLU-weighted sum is the usual choice for THI; your call.

#### W1 — recode the 10 old-code stacks. PASS
Ran as written, 2 min, 10 stacks at 0.2 min each. Each summary line is identical to its earlier one. Across **all 114** `_trend.tif` (both tokens), **0** still carry `n_years = 0` off land.

#### W2 — admin tables, 3 weightings, both tokens. **Gate PASS**
- **Setup:** the previous final tables were copied to `$OL/Data/obs_trends/_admin_tables_area_only_{main,rh15}/` (762 + 165 MB) for the gate. The old parts were moved to `_admin_parts_area_only_{main,rh15}/` (833 + 184 MB). All four are kept.
- **Run:** the loop under nohup took 11:36 → 12:13, **37 min** (main 30 min, `-rh15` 7 min), exit 0 for both.
  - `admin weights from cache` ×3 each, and `exposure weights: crop spam_prod_t_all.tif (42 layers), livestock livestock_number_number_res-25.tif (12 layers)`.
  - 0 errors. System memory peaked at **21.7 GB** (baseline 8.4).
- **`area weighting only`: 0 windows.** Every one of the 19 windows has at least one crop in SPAM.
- **Stack counts:**
  - Main: **195** = 95 area + 95 `crop_production` + 5 `livestock_head` (annual12).
  - `-rh15`: **39** = 19 + 19 + 1.

**Gate:** for each token × level × table, I merged the old table with the new `area` rows on (unit keys, window, index[, year]):
```
main admin0 trends   5,225 = 5,225   max|diff| 0, NA-mask equal | area 5,225      crop_production 3,055      livestock_head 260
main admin0 series 224,675 = 224,675 max|diff| 0, NA-mask equal | area 224,675    crop_production 131,365    livestock_head 11,180
main admin1 trends  68,305 = 68,305  max|diff| 0, NA-mask equal | area 68,305     crop_production 30,790     livestock_head 3,465
main admin1 series  2,937,115 equal  max|diff| 0, NA-mask equal | area 2,937,115  crop_production 1,323,970  livestock_head 148,995
main admin2 trends  633,650 equal    max|diff| 0, NA-mask equal | area 633,650    crop_production 204,050    livestock_head 33,105
main admin2 series 27,246,950 equal  max|diff| 0, NA-mask equal | area 27,246,950 crop_production 8,774,150  livestock_head 1,423,515
rh15 admin0 trends   1,045 equal     max|diff| 0, NA-mask equal | area 1,045      crop_production 611        livestock_head 52
rh15 admin0 series  44,935 equal     max|diff| 0, NA-mask equal | area 44,935     crop_production 26,273     livestock_head 2,236
rh15 admin1 trends  13,661 equal     max|diff| 0, NA-mask equal | area 13,661     crop_production 6,158      livestock_head 693
rh15 admin1 series 587,423 equal     max|diff| 0, NA-mask equal | area 587,423    crop_production 264,794    livestock_head 29,799
rh15 admin2 trends 126,730 equal     max|diff| 0, NA-mask equal | area 126,730    crop_production 40,810     livestock_head 6,621
rh15 admin2 series 5,449,390 equal   max|diff| 0, NA-mask equal | area 5,449,390  crop_production 1,754,830  livestock_head 284,703
```
There are fewer exposure rows than area rows because units with no production or heads of that window's crops or species drop out.

#### W3 — Kenya and Ethiopia, `ggcmi-mai` / `NDWS_mean`, main token
**The maize window in Kenya.** These are the GGCMI `mai_rf` months on the S7 grid, as S3 derives them, weighted by SPAM maize production:
- **53 % is planted in April**, maturing Nov-Jan (the western highlands, a long single season).
- **37 % is planted in October**, maturing in January (the short rains).
- 9 % is planted in March, maturing in June.

Per county:
```
Uasin Gishu / Trans Nzoia / Bungoma / Narok / Kakamega   plant Apr -> maturity Nov-Dec (Trans Nzoia also 28 % Mar -> Jun)
Machakos / Makueni / Kitui                               plant Oct -> maturity Jan (100 %)
```
**So in the east, season "2010" is Oct 2010 - Jan 2011, the failed OND-2010 short rains.** "2011" there is OND 2011, which was wet. In the west, "2011" is Apr-Dec 2011. Ethiopia is 82 % April-planted, maturing in November (kiremt).

**Admin0, normal 1991-2020 (days of water stress per season) and trend:**
```
KEN area            24.1 (sd 2.35)   slope -0.08/dec  p 0.68
KEN crop_production 15.6 (sd 2.74)   slope -0.47/dec  p 0.075
ETH area            18.9 (sd 1.11)   slope -0.19/dec  p 0.16
ETH crop_production 11.6 (sd 1.56)   slope -0.30/dec  p 0.098
```
Under `crop_production` the normal falls by about 9 days in Kenya and 7 in Ethiopia. The area mean was carrying the dry north and east, as you suspected. No trend is FDR-significant.

**Admin0 anomaly-z, 2008-2019:**
```
                     2008  2009  2010  2011  2012  2013  2014  2015  2016  2017  2018  2019
KEN area             0.74  0.28  0.60 -0.44 -0.13 -0.03  0.60 -0.69  0.96  0.46 -0.35 -2.22
KEN crop_production  0.57  0.50  0.12 -0.97 -1.20 -0.26  0.43 -1.21  1.57  0.29 -0.18 -2.25
ETH area             0.17  2.16 -0.16 -0.17 -0.29 -1.33 -0.27  0.08  0.43 -0.14 -0.83 -2.04
ETH crop_production -0.54  1.99  0.00 -0.08  0.22 -1.00 -0.86  0.60  0.23 -0.32 -0.42 -1.82
```
**KEN admin1.** The five largest maize counties by SPAM 2020 production (sum of 0.05° cells within GAUL admin1; national 3.54 Mt) are Uasin Gishu 383 kt, Trans Nzoia 380 kt, Bungoma 312 kt, Narok 242 kt and Kakamega 227 kt. The table adds the semi-arid east:
```
                              2009   2010   2011   2015   2016   2017   normal (crop_prod.)
Uasin Gishu  area             1.47  -0.56  -1.17  -0.12   0.41  -0.40
             crop_production  1.47  -0.61  -1.22  -0.11   0.36  -0.41   13.5 (sd 2.92)
Trans Nzoia  area             1.20  -1.59  -0.50  -0.81   0.56  -0.04
             crop_production  1.12  -1.59  -0.52  -0.93   0.52   0.00   12.4 (2.85)
Bungoma      area             0.71  -0.96  -0.97  -1.02   1.92  -0.05
             crop_production  0.63  -0.82  -1.10  -1.08   1.92  -0.07    8.2 (2.44)
Narok        area             0.66   0.15  -1.03  -2.56   0.31   0.26
             crop_production  0.32   0.17  -1.54  -2.40   0.60   0.12   20.6 (2.34)
Kakamega     area             0.22  -0.42  -0.98  -1.06   2.35   0.21
             crop_production  0.26  -0.48  -1.03  -1.03   2.21   0.18    9.9 (2.64)
Machakos     area            -0.27   0.72  -0.55  -0.93   1.31   0.40
             crop_production -0.28   0.72  -0.56  -0.88   1.39   0.44   20.5 (4.21)
Makueni      area            -0.15   0.54  -0.07  -0.33   0.87   0.39
             crop_production  0.02   0.56  -0.05  -0.43   0.96   0.35   21.1 (4.73)
Kitui        area            -0.05   0.81  -0.15  -0.67   0.93   0.83
             crop_production  0.01   0.88  -0.32  -0.81   1.19   0.80   21.9 (4.25)
```
**ETH admin1.** GAUL 2024 splits SNNP into `Snnp`, `Sidama` and `South West Ethiopia`, so all three are shown:
```
                                   2009   2010   2011   2015   2016   2017   normal (crop_prod.)
Amhara              area           2.11   0.36  -0.05   0.89  -0.30  -0.89
                    crop_production 2.01   0.57   0.01  -0.32  -0.31  -0.86   11.4 (1.75)
Oromia              area           1.57   0.07  -0.24   0.20   0.55  -0.17
                    crop_production 1.44  -0.08   0.16   0.93   0.11  -0.18   11.7 (1.62)
Snnp                area           2.04  -0.10  -1.17  -0.32   1.13  -0.27
                    crop_production 2.31  -0.38  -0.76   0.66   0.84   0.11   11.3 (2.23)
Sidama              area           2.25  -0.18  -0.55   0.51   1.04  -0.38
                    crop_production 2.30  -0.28  -0.74   0.76   1.02  -0.40    9.0 (2.51)
South West Ethiopia area           1.61   0.22  -1.06  -0.53   1.89  -0.48
                    crop_production 1.71   0.15  -0.04  -0.68   1.67  -0.37    6.1 (1.37)
```

#### W4 — findings (report only; nothing fixed)
1. **The national "2011 / 2015" expectation was a season-labelling issue more than a weighting one.**
   - The OND-2010 failure falls in season **"2010"** for the October-planted east. There it reads **dry, but weakly**: z +0.56 to +0.88 under `crop_production`, all under 1 sd.
   - "2011" in the east is OND 2011, which was wet, and reads z -0.05 to -0.56. That is consistent.
   - **So the eastern counties do show a dry 2010/11 signal, but it is small** in a 20-day-normal NDWS with sd 4.2-4.7. This is reported as a finding, as the block asks.
   - The same counties read **+0.96 to +1.39 for 2016** (the OND-2016 failure). The western long-season counties read **+0.6 to +1.5 for 2009** (the 2009 long-rains failure). Both fit the record.
2. **2015.** In Kenya it is negative almost everywhere: Narok -2.4, the west about -1. That fits a wet El Niño OND 2015, which also falls inside the April-December western window.
   - In Ethiopia, `crop_production` moves 2015 towards dry: Oromia +0.93, Sidama +0.76, Snnp +0.66, admin0 +0.60 (area +0.08). But the kiremt-2015 drought is not strong in these units: Amhara reads -0.32.
   - **2009 is the clearest Ethiopian dry year** (z +1.4 to +2.3 in every region). 2019 is the wettest in both countries (z about -2).
3. **Weighting moves the levels more than the anomalies.** The normals drop by 7-9 days at admin0. At admin1 the z-scores barely change, because units like the western counties are fairly uniform; the admin0 z-scores move more (KEN 2016 +0.96 → +1.57).
4. **To note before a publish:**
   - `livestock_head` is 62 % poultry (raw heads; see W0).
   - S7 does not log that it resampled the crop raster.
   - `metadata/obs_crop_windows.csv` is matched on SPAM's spaced names, which works (e.g. `pearl millet` → `pearl-millet`).

**Size:** `du -sh $OL/Data/obs_trends` = **6.3 GB**, up from 3.6 GB: the exposure rows, plus about 1.9 GB of kept area-only tables and parts. Nothing is running and nothing was published.

---

### MACBOOK 2026-11-13 — exposure-weighted admin tables (crop production, livestock head) + Kenya/Ethiopia recheck (W0–W4)

Excellent run, and the Kenya sanity check was exactly the right thing to report. The likely cause is **area weighting**:
Kenya's national mean is dominated by the arid north, where no maize grows and NDWS is near its ceiling every season.
S7 now also writes **exposure-weighted** admin tables (selftest 21):
- `crop_production`: each crop window weighted by the summed exposure of the crops that use it (`metadata/obs_crop_windows.csv`);
- `livestock_head`: the livestock window (`annual12`) weighted by head counts.

The tables gain a `weighting` column (`area` | `crop_production` | `livestock_head`). `--admin-only` rebuilds the admin
tables from the existing stacks without touching any raster.

#### W0 — pull, selftest, find the exposure rasters
```bash
cd $REPO && git pull --ff-only && Rscript R/obs/s7_trends_anomalies.R --selftest   # 21
ls $NWD/Data/mapspam/2020V1r2_SSA/processed/variable=prod_t/*res-25*.tif
ls $NWD/Data/GLW4_2020/processed/variable=number_number/*res-25*.tif
```
- **Crops:** pick the all-technologies production raster (one layer per crop, as R/3 reads it). **Livestock:** the head-count
  raster (one layer per species-system).
- Report the paths, `nlyr`, and the first ~10 layer names of each. S7 matches layer names to the crops in
  `obs_crop_windows.csv` after lower-casing and turning `_`/space into `-`, so the names must be crop or species names.
- If either raster is not on the 0.25° lattice, S7 resamples it by average and says so.

#### W1 — recode the 10 old-code stacks (n_years NA, as the new code writes) — ~2 min
```bash
nohup Rscript R/obs/s7_trends_anomalies.R --obs-root $OL --token chirps-chirts-agera5 --timeframe 1982-2025 --years 1982:2024 \
      --windows annual12,custom-mai_maturity-4 --cores 16 --overwrite > logs/obs_long_s7_w1_$STAMP.log 2>&1 &
```
(No `--boundaries-dir` here; W2 rebuilds every admin table.)

#### W2 — admin tables, all three weightings, both tokens (rasters untouched)
Move the old parts aside first; they predate the `weighting` tag. Keep them.
```bash
T=$OL/Data/obs_trends; mv $T/chirps-chirts-agera5_1982-2024/_admin_parts $T/_admin_parts_area_only_main
mv $T/chirps-chirts-agera5-rh15_1982-2024/_admin_parts $T/_admin_parts_area_only_rh15
for TOK in chirps-chirts-agera5 chirps-chirts-agera5-rh15; do
  Rscript R/obs/s7_trends_anomalies.R --obs-root $OL --token $TOK --timeframe 1982-2025 --years 1982:2024 --normal 1991:2020 \
     --boundaries-dir $NWD/Data/boundaries --weights-crop <W0 crop tif> --weights-livestock <W0 livestock tif> --admin-only \
     > logs/obs_long_s7_w2_${TOK}_$STAMP.log 2>&1
done   # run the loop itself under nohup
```
- **Gate:** for every (unit, window, index), the `area` rows must equal the previous area-only tables (max |diff| 0). Any
  difference is a stop.
- Report row counts per weighting, and the windows that logged `area weighting only` (no crop of that window found in
  the raster).

#### W3 — Kenya and Ethiopia, the recheck the area tables could not answer
From `admin_series_admin0/1.parquet`, `ggcmi-mai` / `NDWS_mean`, **`crop_production`** weighting, main token:
- **KEN and ETH admin0:** anomaly-z for 2008–2019, and the 1991–2020 normal.
- **KEN admin1:**
  - the five largest maize counties by production, likely in the west, which is long-rains maize;
  - Machakos, Makueni and Kitui, the semi-arid east where the short rains matter.

  Anomaly-z for 2009, 2010, 2011, 2015, 2016, 2017.
- **ETH admin1:** Oromia, Amhara, SNNP: the same years.
- For reference, the same rows with `area` weighting.

A season is named by its **start year** in the maize window. State the window's planting and harvest months in
Kenya (the modal months over KEN maize cells from the GGCMI calendar), so "2010" vs "2011" reads correctly against the
OND-2010 / MAM-2011 failure.

#### W4 — report
Prepend one `### RESPONSE`. Nothing published. If W3 still shows no dry signal in 2010/11 for the eastern counties
under `crop_production` weighting, report that as a finding with the numbers. Do not try to fix it.

### RESPONSE — cglabs 2026-11-12 (final) — **Z0-Z3 + Y4 done. The speed fix is exact: Z2 max |diff| = 0 on all 4 rasters, 12 s per stack against 83 min before. The full S7 ran both tokens in 37 + 8 min: 95 + 19 stacks, admin tables at 3 levels, peak system memory +9 GB.** Two sanity checks do not show the expected signal (report, not gates; see Y4). Nothing published

Stamp `20261112_132931`. HEAD `cc4fdcd` (fix `6471ecd`). Env grep empty before each launch.

#### Z0 — slow run stopped. PASS
I killed the master pid from the pid file, then its 16 `.workRSOCK` workers by PID. (`pkill -f workRSOCK` also matches the shell that runs it.) The `ps` check shows `none left`. By then the run had finished **10** stacks, two more than the interim's 8: `annual12/` ×5 and `custom-mai_maturity-4/` ×5, 40 COGs, all complete.

#### Z1 — selftest. PASS
`s7_trends_anomalies.R --selftest` → **19** ok, `ALL S7 SELFTEST ASSERTIONS PASSED`.

#### Z2 — equivalence on `annual12/NDWS_mean`. PASS
The old outputs are copied to `logs/s7_old/`, and the rerun with `--overwrite` took **0.2 min** (log line `…; 0.2 min`; the old code took 83 min).
```
NDWS_mean_anomaly_diff.tif      43/43 layers, names equal, NA mask equal,   max|diff| = 0
NDWS_mean_anomaly_z.tif         43/43 layers, names equal, NA mask equal,   max|diff| = 0
NDWS_mean_normal_1991-2020.tif   5/5  layers, names equal, NA mask equal,   max|diff| = 0
NDWS_mean_trend.tif              4/4  layers, names equal, NA mask DIFFERS, max|diff| = 0
```
**One coding change, which I did not treat as a stop:** in the trend raster's `n_years` layer, the **528,127** no-data cells changed from `0` (old) to `NA` (new). These are the off-land cells where `slope_per_decade`, `p_mk` and `sig_fdr` are NA in both versions. The other three layers have identical NA masks, and the values agree exactly everywhere. **Consequence:** the 9 stacks Z3 skipped, the old-code stacks other than this one, still carry `n_years = 0` on those cells; the 85 new stacks carry NA. Rerunning those 9 with `--overwrite --windows annual12,custom-mai_maturity-4` takes about 2 min, if you want the coding consistent.

#### Z3 — the full run, both tokens. PASS
| | main token | `-rh15` |
|---|---|---|
| wall-clock | 13:30:38 → 14:07:29, **37 min** | 14:07:54 → 14:16:09, **8 min** |
| stacks | 95 (**85 new**, 10 `skip (exists)`) | 19 (all THI_max) |
| per-stack S7 | 0.2 min each | 0.2 min each |
| admin weights | `admin weights from cache` ×3 | `admin weights from cache` ×3 |
| system used memory (`free -m`, 5 s) | baseline 8.3 GB → **peak 17.3 GB** | baseline 8.2 GB → peak 13.9 GB |
| errors / non-LIBTIFF warnings | 0 / 0 | 0 / 0 |

Most of the 37 min goes to the admin step, about 20 s per stack across 6,670 admin2 units. The per-cell statistics take 12 s per stack. The pid-tree RSS sampler read 28.7 GB, but it double-counts the pages shared by the forked workers, so the `free` figure is the one to use. The "50 or more warnings" are all the known `CPL_gdal_init` LIBTIFF notice: 0 lines in the warning list are anything else.

Outputs: the main token has 380 COGs (95 × 4) and 570 admin parts; `-rh15` has 76 COGs (19 × 4) and 114 parts. Admin log lines:
```
main  admin0: 95 stacks,   224,675 series rows,   5,225 trend rows; 38.6 % of units FDR-significant
main  admin1: 95 stacks, 2,937,115 series rows,  68,305 trend rows; 33.9 %
main  admin2: 95 stacks, 27,246,950 series rows, 633,650 trend rows; 29.9 %
rh15  admin0: 19 stacks,    44,935 series rows,   1,045 trend rows; 67.6 %
rh15  admin1: 19 stacks,   587,423 series rows,  13,661 trend rows; 63.5 %
rh15  admin2: 19 stacks, 5,449,390 series rows, 126,730 trend rows; 63.1 %
```
The tables hold 715 of 719 admin1 units and 6,666 of 6,670 admin2 units. The 4 + 4 missing are units with no cell centre or overlap on the 0.05° grid. The NA trend rows (247 / 2,432 / 4,142) are units with fewer than 30 valid seasons, mostly zero-variance or desert units.

#### Y4 — report
**Per-index summary lines (cells).** `annual12` (livestock, main token):
```
NDWL0_mean   trend on 47873 cells, 19.1 % p<0.05, 10.5 % FDR; median slope -0 /decade
NDWS_mean    trend on 47873 cells, 21.1 % p<0.05,  9.4 % FDR; median slope -0
NTx35_mean   trend on 47924 cells, 75.1 % p<0.05, 73.0 % FDR; median slope 0.404
PTOT_sum     trend on 48321 cells, 33.4 % p<0.05, 24.1 % FDR; median slope 0.513
THI_max_max  trend on 47885 cells, 60.9 % p<0.05, 54.6 % FDR; median slope 0.247
THI_max_max (-rh15)          56.5 % p<0.05, 50.5 % FDR; median slope 0.225
```
`ggcmi-mai` (maize):
```
NDWL0_mean   trend on 47763 cells, 15.6 % p<0.05,  6.6 % FDR; median slope -0
NDWS_mean    trend on 47763 cells, 18.3 % p<0.05,  6.6 % FDR; median slope -0
NTx35_mean   trend on 47814 cells, 55.5 % p<0.05, 49.8 % FDR; median slope 0.267
PTOT_sum     trend on 48211 cells, 31.6 % p<0.05, 21.4 % FDR; median slope 0.394
THI_max_max  trend on 47775 cells, 61.3 % p<0.05, 54.8 % FDR; median slope 0.246
```
(`-0` is the `%.3g` of a median that is exactly 0: on most cells the count indices have a zero Sen slope.)

**Admin trend summary per level** (main token, all 19 windows; % of units FDR-significant / median slope per decade):
```
            admin0 (55)        admin1 (715)       admin2 (6666)
NDWL0_mean  19.3 %  0          12.5 %  0           6.3 %  0
NDWS_mean   11.0 % -0.0027      9.1 % -0.0001      5.7 %  0
NTx35_mean  66.5 %  0.304      60.1 %  0.209      55.6 %  0.206
PTOT_sum    21.3 %  2.68       18.1 %  2.98       11.8 %  0.775
THI_max_max 74.5 %  0.257      69.6 %  0.276      70.0 %  0.332
```
**Sanity checks.** These are reports, not gates. They come from the admin0 parquets, and the script is `logs/obs_long_y4_sanity_<stamp>.R`.
- **1991-2020 normal of maize-window NDWS** (admin0, days):
  - **KEN 24.1** (sd 2.35), slope -0.08/decade, p 0.68, not significant.
  - **ETH 18.9** (sd 1.11), slope -0.19/decade, p 0.16, not significant.
- **KEN admin0 maize-window NDWS anomaly-z.** This **does not show the expected positive (dry) anomaly**: **2011 = -0.44, 2015 = -0.69**.
  - The neighbouring years are 2009 +0.28, 2010 +0.60, 2012 -0.13, 2014 +0.60, 2016 **+0.96**, 2017 +0.46.
  - ETH reads 2011 -0.17 and 2015 +0.08, with 2009 +2.16 as its driest year.
  - Possible reasons, not checked:
    - **Season labels.** Seasons are named by start year. If the KEN GGCMI maize window is the March-planted long rains, then "2011" is MAM 2011 and "2010" is MAM 2010; the failed OND 2010 short rains fall outside both.
    - **2015-16 El Niño.** It brought wet OND 2015 to East Africa; the drought hit Ethiopia's kiremt and southern Africa.
    - **National-mean dilution.** The KEN admin0 mean is area-weighted over the arid north, where maize is not grown.
  - A cropland-weighted mean or an admin1 view (e.g. Eastern/Rift Valley) would separate these. Not done, as it is not in the block.
- **THI_max national trend, `annual12`, per decade:** all positive and FDR-significant.
  - **ETH +0.31** (p 8e-18), **NGA +0.22** (p 2e-4), **KEN +0.18** (p 3e-4). Normals 84.4 / 89.3 / 87.9.
  - `-rh15`: ETH +0.25, NGA +0.15, KEN +0.17, all FDR-significant.

**`du -sh $OL/Data/obs_trends` = 3.6 GB:** main 2.9 GB, `-rh15` 721 MB, `_admin_weights` 1.2 MB.

**State:** nothing running and nothing published. The old outputs of `annual12/NDWS_mean` are kept in `logs/s7_old/` (4 files, untracked, not committed). `$OL` is kept.

---

### MACBOOK 2026-11-12 — S7 speed fix (matrix + forked workers, admin parts on disk); kill the slow run, one equivalence check, then Y3–Y4

Thank you for leaving it alive and measuring it properly. The workers sitting idle and the master pinned at 100 % was the clue.
**Cause:** S7 ran every stack through `terra::app(cores = 16)`. With PSOCK workers that pass is master-bound: it chops the
raster into blocks and serializes each one. On top of that, the admin tables were held in memory across stacks, so each stack
paid for everything before it.

**Fix (`R/obs/s7_trends_anomalies.R`, selftest 19):**
- Per-cell statistics run on the cell × season **matrix** with forked workers (`parallel::mclapply`). Nothing is
  serialized, and the workers see the script's functions.
- Each stack's admin tables are written to `obs_trends/…/_admin_parts/<level>/` as soon as the stack is done. The
  final `admin_*_<level>.parquet` files are assembled from **all** parts on disk at the end, so a resumed or split run
  gives complete tables.
- Macbook benchmark on a full Africa-window stack (81,760 cells × 44 seasons): **17 s on 1 core, 5 s on 4 cores**, against
  28–207 min per stack on the node. Expect the 95 stacks in minutes.

#### Z0 — stop the slow run (the 8 finished stacks are kept)
```bash
kill $(cat logs/obs_long_s7_20261111_111011.pid); sleep 5; pkill -f workRSOCK || true   # the 16 PSOCK workers
ps aux | grep -E 's7_trends|workRSOCK' | grep -v grep || echo "none left"
```
#### Z1 — pull, selftest
```bash
cd $REPO && git pull --ff-only && Rscript R/obs/s7_trends_anomalies.R --selftest   # 19
```
#### Z2 — equivalence: the new code reproduces a stack the old code wrote
```bash
D=$OL/Data/obs_trends/chirps-chirts-agera5_1982-2024/annual12; mkdir -p logs/s7_old && cp $D/NDWS_mean_* logs/s7_old/
nohup Rscript R/obs/s7_trends_anomalies.R --obs-root $OL --token chirps-chirts-agera5 --timeframe 1982-2025 --years 1982:2024 \
      --windows annual12 --indices NDWS_mean --cores 16 --overwrite > logs/obs_long_s7_z2_$STAMP.log 2>&1 &
```
When it is done, compare the 4 rasters old vs new. Report max |diff| per file; it must be 0 or float noise (< 1e-9).
**Anything larger is a stop.** Report the stack's minutes from its log line, which now ends with `…; N min`.

#### Z3 — the full run, both tokens (no `--overwrite`: the 8 stacks are skipped; admin parts are built for every stack)
```bash
nohup Rscript R/obs/s7_trends_anomalies.R --obs-root $OL --token chirps-chirts-agera5 --timeframe 1982-2025 --years 1982:2024 \
      --normal 1991:2020 --boundaries-dir $NWD/Data/boundaries --cores 16 > logs/obs_long_s7_full_$STAMP.log 2>&1 &
# when done:
nohup Rscript R/obs/s7_trends_anomalies.R --obs-root $OL --token chirps-chirts-agera5-rh15 --timeframe 1982-2025 --years 1982:2024 \
      --normal 1991:2020 --boundaries-dir $NWD/Data/boundaries --cores 16 > logs/obs_long_s7_rh15_$STAMP.log 2>&1 &
```
Peak memory: forked workers share pages until they write. Report the system's peak used memory (e.g. `free -g`
sampled), not only the master's RSS.

#### Y4 — report, as written in the 2026-11-06 block (summary lines, admin summaries, the sanity checks, `du -sh`).

### RESPONSE — cglabs 2026-11-12 (interim) — **Y0 PASS (selftest 18) and the multi-core fix works, but Y3 is far too slow: 8 of 95 main-token stacks done in 18.7 h, and each stack takes ~20 min longer than the one before.** At that rate the main token needs about two months. Run left alive; your call: kill and fix, or let it run

Stamp `20261111_111011`. HEAD `2a3e494`. Env grep empty. Launched 11:10:15 (`logs/obs_long_s7_<stamp>.log`, pid file beside it).

- **Selftest:** 18 ok, including `cores = 2 … gives the same trend raster as cores = 1`.
- **Admin weights:** `admin weights from cache` ×3 in 17 s, `admin units: admin0 55, admin1 719, admin2 6670`. The cache works.
- **No error so far.** Every finished stack logs a sane summary line:
```
annual12/NDWL0_mean:  43 seasons; trend on 47873 cells, 19.1 % p<0.05, 10.5 % FDR-significant; median slope -0 per decade
annual12/NDWS_mean:   43 seasons; trend on 47873 cells, 21.1 % p<0.05,  9.4 % FDR-significant; median slope -0 per decade
annual12/NTx35_mean:  43 seasons; trend on 47924 cells, 75.1 % p<0.05, 73.0 % FDR-significant; median slope 0.404 per decade
annual12/PTOT_sum:    43 seasons; trend on 48321 cells, 33.4 % p<0.05, 24.1 % FDR-significant; median slope 0.513 per decade
annual12/THI_max_max: 43 seasons; trend on 47885 cells, 60.9 % p<0.05, 54.6 % FDR-significant; median slope 0.247 per decade
custom-mai_maturity-4/NDWL0_mean: 43 seasons; trend on 47873 cells,  9.7 % p<0.05,  4.5 % FDR-significant; median slope -0 per decade
custom-mai_maturity-4/NDWS_mean:  43 seasons; trend on 47873 cells, 10.9 % p<0.05,  2.9 % FDR-significant; median slope -0 per decade
custom-mai_maturity-4/NTx35_mean: 43 seasons; trend on 47924 cells, 54.0 % p<0.05, 48.5 % FDR-significant; median slope 0.321 per decade
```
(The `-0` medians are the `%.3g` of a median of exactly 0. Most cells have a zero Sen slope on count indices.)

**The problem: stack time grows linearly.** Minutes from one summary line to the next: **28, 83, 104, 125, 144, 167, 187, 207**, about **+20 min per stack**, whatever the index. NDWL0 took 28 min as the first stack and 167 min as the sixth. Extrapolating linearly, the 95 main-token stacks need roughly 65 days, and `ggcmi-mai` (8th window) would finish in weeks. That rules out the Y4 report on this run.

What the node shows, 05:51 UTC, during stack 9 (`custom-mai_maturity-4/THI_max_max`):
- **The master is the bottleneck, not the workers.** The master R process is at 69-100 % of one core. Its 16 PSOCK workers sit at ~6 % CPU each, mostly sleeping. They were started 1 h 18 min earlier, when the current stack's `app()` began.
- **Each `app()` makes a fresh cluster.** The workers are re-parented to PID 1 and only one set of 16 exists, so the old ones are cleaned up.
- **Admin time is negligible.** The current cluster was born within seconds of the previous summary line, so `admin_stack()` takes well under a minute.
- **So the time and its growth are inside `do_stack()`'s `app(…, cores = 16)` calls.** Master RSS is flat at ~2.0 GB and `/tmp` holds 3 MB, so this is not a memory or tempfile leak I can see.

I have not found the cause and have not tried a fix. The suspect is per-block serialization on the master. Each `app()` block ships the closure *and its enclosing `do_stack` frame*, which holds the `SpatRaster` `r` and the other locals, to the workers. I can't explain the growth across stacks from what I see. A quick discriminator, if you want it: time one `do_stack()` on one stack in a fresh process, with `cores = 16` against `cores = 1`. If the fresh process is fast, the growth is state carried across stacks within one R session. Running the 95 stacks as separate invocations (`--windows`/`--indices`) would then avoid it.

**Note on peak memory:** the 5 s RSS sampler follows the pid tree. The PSOCK workers are re-parented to PID 1, so it misses them. Its peak of **3.1 GB** is the master only. The 16 workers add ~0.72 GB each (~11.5 GB) during `app()`. The 2026-11-11 (1) run never got this far, so its 1.76 GB figure stands.

**State:** `$OL/Data/obs_trends/chirps-chirts-agera5_1982-2024/` holds 8 complete stacks (`annual12/` ×5, `custom-mai_maturity-4/` ×3, 4 COGs each). No admin parquets yet, because they are written at the end. `-rh15` not launched. Nothing published. **The run is still going.** It is harmless to leave and the finished stacks are kept, but if a fix lands, kill pid file `logs/obs_long_s7_20261111_111011.pid` and its 16 `.workRSOCK` workers. The rerun skips stacks whose 4 outputs exist (no `--overwrite`). Note that the admin tables are built only from stacks processed in that invocation.

---

### MACBOOK 2026-11-11 (2) — Y3 multi-core fix on develop; resume Y3–Y4 (weights come from your cache)

Right again, and thanks for the precise diagnosis. Fixed by shipping local copies of `mk_sen` and `normal_stats` in the
closures `terra::app` hands to the PSOCK workers (both call sites). The selftest now runs `run()` with `cores = 2` and
checks the result equals the single-core result (18 assertions).

```bash
cd $REPO && git pull --ff-only && Rscript R/obs/s7_trends_anomalies.R --selftest   # 18
```
Then Y3 as written (main token, then `-rh15`), then Y4. Both runs should log `admin weights from cache`.

### RESPONSE — cglabs 2026-11-11 (1) — **STOP at Y3 again: the admin fix works (weights built and cached, `admin units: admin0 55, admin1 719, admin2 6670`), but the first stack fails under `--cores 16`: `could not find function "mk_sen"` on the PSOCK workers.** No stack output written; `-rh15` not launched. No fix improvised

Stamp `20261111_054848`. HEAD `fc6d730`. Env grep empty.

**Y0 rerun:** `s7_trends_anomalies.R --selftest` → **17** ok, `ALL S7 SELFTEST ASSERTIONS PASSED`.

**Y3, main token, command as written** (`logs/obs_long_s7_<stamp>.log`), 05:48:48 → 06:08:26 (**~20 min**), peak memory **1.76 GB** (R + children, 5 s samples).
```
[S7] src=…/obs_seasonal/chirps-chirts-agera5_1982-2025 windows=19 years=1982-2024 (43) normal=1991-2020 cores=16
[S7] admin weights admin0: 44881 unit-cell pairs in 0.2 min (cached)
[S7] admin weights admin1: 54862 unit-cell pairs in 2.0 min (cached)
[S7] admin weights admin2: 80408 unit-cell pairs in 17.0 min (cached)
[S7] admin units: admin0 55, admin1 719, admin2 6670
Error in checkForRemoteErrors(val) :
  1440 nodes produced errors; first error: could not find function "mk_sen"
Calls: run ... clusterApply -> staticClusterApply -> checkForRemoteErrors
Execution halted
```
(Plus the known `CPL_gdal_init` LIBTIFF notices; the "11 warnings" were not printed.) The weights took 19 min this time, not 36.

**Where:** `R/obs/s7_trends_anomalies.R:81`, in `do_stack()`:
```r
tr <- app(r, function(v) mk_sen(v, t, min_trend), cores = cores)
```
With `cores > 1`, terra's `app()` runs the function on a fresh PSOCK cluster. The anonymous function is shipped with its `do_stack` frame (`t`, `min_trend`), but `mk_sen` lives in the master's global env, which is not shipped, so every worker fails. Line 86 (`normal_stats`) has the same pattern and would fail next.
**Why the selftest passed:** every selftest `run()` and `do_stack()` call uses the default `cores = 1L`, where `app()` stays in-process. So the `cores > 1` path has no test.
**Suggested fix (your call):** pass the helper through `app()`'s `...`, which terra forwards to the workers. For example `app(r, function(v, f, t, m) f(v, t, m), f = mk_sen, t = t, m = min_trend, cores = cores)`, and the same for `normal_stats`. Then add one selftest `do_stack()` call with `cores = 2` that checks the result is identical to `cores = 1`.

**State:**
- `$OL/Data/obs_trends/` holds only `_admin_weights/admin{0,1,2}_<key>.rds` (184 KB, 358 KB, 679 KB). No window dir, stack or parquet was written. The weights are valid, so the rerun reads them from cache: no ~20 min rebuild for either token.
- The `-rh15` sensitivity run was not launched; Y4 not done. Nothing running. Nothing published.

---

### MACBOOK 2026-11-11 — Y3 fix on develop (nrow type + cached admin weights); resume Y3–Y4

Right stop, and a clean diagnosis. The bug is fixed in `R/obs/s7_trends_anomalies.R`: the unit count is now taken as an
integer. The selftest now runs the **full `run()` with admin units**, the path that crashed (17 assertions, was 15).

**Admin weights are now cached** in `$OL/Data/obs_trends/_admin_weights/<level>_<key>.rds`. The key covers the grid
and the unit geometry. The 36-minute build happens once; the second token and any rerun read it back (the log
says `admin weights from cache`).

```bash
cd $REPO && git pull --ff-only && Rscript R/obs/s7_trends_anomalies.R --selftest   # 17
```
Then Y3 as written: the main token first, then `--token chirps-chirts-agera5-rh15`; then Y4. Nothing in `$OL` needs
cleaning, because the crashed run wrote nothing.

### RESPONSE — cglabs 2026-11-06 (3) — **STOP at Y3: S7 halted on its admin-unit log line, after 36 min of building admin weights and before the first stack.** `vapply(units, nrow, 1L)`: `nrow()` of a terra `SpatVector` is a double. Nothing written under `obs_trends`; the `-rh15` run not started. Y0-Y2 stand. No fix improvised

**What ran:** the Y3 command as written (`--token chirps-chirts-agera5 … --boundaries-dir $NWD/Data/boundaries --cores 16`), `logs/obs_long_s7_<stamp>.log`,
12:08:18 → 12:44:30. Env grep empty.

```
[S7] src=…/obs_seasonal/chirps-chirts-agera5_1982-2025 windows=19 years=1982-2024 (43) normal=1991-2020 cores=16
Error in vapply(units, nrow, 1L) : values must be type 'integer',
 but FUN(X[[1]]) result is type 'double'
Calls: run ... sprintf -> paste0 -> sprintf -> paste -> sprintf -> vapply
Execution halted
```
(The eight `CPL_gdal_init` warnings with it are the known `/opt/conda/lib/gdalplugins` LIBTIFF notice, see the X4 final block.)

**Where:** `R/obs/s7_trends_anomalies.R:155`, in `run()`:
```r
lg(sprintf("admin units: %s", paste(sprintf("%s %d", names(units), vapply(units, nrow, 1L)), collapse = ", ")))
```
`read_units()` (l.95-102) returns `terra::vect(...)` objects, and terra's `nrow()` method returns a double, so `vapply(…, 1L)` refuses it.
The run had already spent **~36 min** in `unit_weights()` (l.103-108: `terra::extract(…, weights = TRUE)` over GAUL admin0/1/2, on one core)
when it reached this line, so the cost is in time only. Suggested one-token fix: `vapply(units, nrow, numeric(1))`, or `as.integer(nrow(x))`.
**Why the selftest passed:** its `run()` calls (l.201, l.214) never pass `boundaries_dir`, and the admin path is tested through
`admin_stack()` with a data.frame directly. So the `run()` → `read_units()` → `vect` path has no test. One assertion that calls `run()`
with a tiny boundaries dir of three `_a0_/_a1_/_a2_` parquets would cover it.

**State:**
- `$OL/Data/obs_trends` does not exist; nothing to clean up. The RH15 sensitivity run was not launched.
- Peak memory over the 36 min: **1.6 GB**, total RSS of the R process and its children sampled every 5 s
  (`logs/obs_long_s7_peakmem_<stamp>.txt`). `--cores 16` had not engaged yet, since `app()` is per stack.
- Y0-Y2 stand as reported below. Nothing running.

**Not done (waits on the fix):** the rest of Y3 and Y4. Already done for Y4: the Kenya PTOT sanity check (interim 1). **For the rerun, note** that
`unit_weights()` costs ~36 min **per S7 invocation**, so the two token runs together cost ~72 min before any stack. Caching the weights
(e.g. a `.rds` beside `dst`, keyed on the template grid) would halve that; your call.

---

### RESPONSE — cglabs 2026-11-06 (interim 2) — **Y2 PASS: 19 new `PTOT_sum.tif` stacks, 44 layers each; 0 existing stacks or bundles rewritten.** Y3 (S7, main token) launched 12:08:18

`logs/obs_long_s35_ptot_<stamp>.log`, 11:53:30 → 12:07:46, **14 min**, 0 WARN, 0 errors.
- The log reports `seasons kept=43 (1982..2024) timeframe=1982-2025` and the same window diagnostics as X4. It ends with `S3 <window>/PTOT: 44 seasons` for
  each window, every `S4/S5 … done` reusing its bundle, and **`done: 0 bundles`**.
- **The mtime gate**, over every file under `$OL/Data/obs_seasonal` and `$OL/Data/hazard_risk`: before **254** (95 stacks + 159 bundles), after
  **273**. **0 changed** (mtime or size), 0 gone, **19 new, all `PTOT_sum.tif`**: one per window of the main token, 44 layers
  (1981 … 2024) each.
- The `-rh15` token's 19 window dirs hold only its THI stacks, so they have no `PTOT_sum.tif`. That is expected, because the command runs on the
  main token only.
- Lists: `logs/obs_long_y2_mtimes_{before,after}_<stamp>.txt`.

---

### RESPONSE — cglabs 2026-11-06 (interim 1) — **Y0 + Y1 PASS.** Selftests 25 / 15; monthly PTOT 1981-2025 is 540 files, 0 WARN, 0 missing, 0 errors; the Kenya-box climatology shows both rains. Y2 (rainfall stacks) running

Stamp `20261106_111807`. HEAD `3ccda81`, branch `develop`. Env grep empty before each launch.

#### Y0 — selftests. PASS
`s3_s5_obs_hazards.R --selftest` **25** ok; `s7_trends_anomalies.R --selftest` **15** ok. `$NWD/Data/boundaries` holds exactly
one of each: `atlas_gaul24_a0_africa.parquet`, `…_a1_…`, `…_a2_…`.

#### Y1 — S2 monthly PTOT. PASS
`calc_PTOT.R` as written (`logs/obs_long_s2_ptot_<stamp>.log`), 11:18 → 11:53, **35 min**. **540** `PTOT-YYYY-MM.tif` (nothing else in
the dir), 0 WARN, 0 `daily inputs missing`, 0 errors.

Sanity check: the Kenya box (33.9..41.9 E, 4.7 S..5.0 N) land-mean monthly PTOT, averaged over 1981-2025, in mm:
```
Jan 36  Feb 32  Mar 78  Apr 139  May 83  Jun 32  Jul 30  Aug 29  Sep 26  Oct 60  Nov 103  Dec 68
```
Both rains are there: the long rains peak in April (139), the short rains in November (103), and June-September and January-February are dry.

---

### MACBOOK 2026-11-06 — long series accepted; Y0–Y4: rainfall stack + S7 trends, 1991–2020 normals, anomalies (nothing published)

**Accepted.** X0–X6 passed every gate, and the overlap result is exact: NDWS/NDWL0 are identical from 2006, so the
published product is free of its seed. Your note on 44 layers (43 seasons + the spin-up) is right; S7 selects seasons
by name and drops 1981.

**New on develop (pull):**
- `R/obs/s3_s5_obs_hazards.R --extra-indices PTOT` builds seasonal **rainfall-total** stacks (`PTOT_sum.tif`, sum
  over each window). These are stacks only: never classified, never in a bundle. Selftest 25.
- **`R/obs/s7_trends_anomalies.R`** (selftest 15). Per window × index stack it computes:
  - trend: Theil-Sen slope per decade; Mann-Kendall p with the Hamed-Rao autocorrelation correction; FDR
    (Benjamini-Hochberg) significance across cells;
  - the 1991–2020 normal: mean, sd, p10, p50, p90;
  - per-season anomalies: difference and z.

  With `--boundaries-dir` it also writes area-weighted admin0/1/2 series and trends, computed on each unit's own
  mean series. Outputs go under `$OL/Data/obs_trends/<token>_1982-2024/`.

#### Y0 — pull, selftests
```bash
cd $REPO && git pull --ff-only && git log -1 --format='%h %s'
Rscript R/obs/s3_s5_obs_hazards.R --selftest   # 25
Rscript R/obs/s7_trends_anomalies.R --selftest # 15
```
#### Y1 — S2 monthly PTOT, 1981–2025 (background)
```bash
cd $REPO/hazards_upstream/R/04_indices
COMMON_DATA=$OL SCENARIO=historical YRS=1981:2025 DAYS_CHECK=strict GCMS=chirps-chirts-agera5 \
  nohup Rscript calc_PTOT.R > $REPO/logs/obs_long_s2_ptot_$STAMP.log 2>&1 &   # + .pid
```
Expect 540 `PTOT-YYYY-MM.tif`, 0 WARN, 0 `daily inputs missing`. Sanity check: the Kenya-box land mean for each
month shows the March–May and October–December rains.

#### Y2 — S3 rainfall stacks only (no --overwrite: the existing stacks and bundles are reused)
```bash
nohup Rscript R/obs/s3_s5_obs_hazards.R --obs-root $OL --token chirps-chirts-agera5 --years 1981:2025 --spinup 1 \
      --ggcmi-dir $NWD/Data/ggcmi --extra-indices PTOT > logs/obs_long_s35_ptot_$STAMP.log 2>&1 &
```
Expect new `PTOT_sum.tif` in every window dir (44 layers), and the log reusing the existing stacks and bundles.
**Any other stack or bundle rewritten is a stop**: check mtimes before and after.

#### Y3 — S7 (background)
```bash
nohup Rscript R/obs/s7_trends_anomalies.R --obs-root $OL --token chirps-chirts-agera5 --timeframe 1982-2025 \
      --years 1982:2024 --normal 1991:2020 --boundaries-dir $NWD/Data/boundaries --cores 16 \
      > logs/obs_long_s7_$STAMP.log 2>&1 &
```
This covers all windows and all stacks (NDWS, NDWL0, NTx35, THI_max, PTOT). Run it again with
`--token chirps-chirts-agera5-rh15` for the RH15 THI stacks (livestock windows), as a sensitivity run. If the
boundaries dir does not hold exactly one `*_a0_*`, `*_a1_*` and `*_a2_*` parquet, S7 stops; say what is there.

#### Y4 — report
- Wall-clock and peak memory for Y3.
- Per index, the log's summary line: cells with a trend, % p < 0.05, % FDR-significant, median slope per decade.
  Report it for `ggcmi-mai` (maize) and `annual12` (livestock THI).
- The admin trend summary per level.
- **Sanity checks (report, not gates):**
  - the 1991–2020 normal of maize-window NDWS for KEN and ETH;
  - the 2011 and 2015 anomaly-z of maize-window NDWS for KEN admin0. The 2010–11 Horn of Africa and 2015–16 El Niño
    droughts should show as positive dry anomalies;
  - the national THI_max trend in Nigeria, Kenya and Ethiopia.
- `du -sh $OL/Data/obs_trends`.

Then push one `### RESPONSE`. **Nothing is published**: the keys and the publish are a later block with Pete's GO.

### RESPONSE — cglabs 2026-11-05 (final) — **X0-X6 complete, every gate PASS.** S3-S5 built 129 + 30 = 159 bundles with `seasons kept=43 (1982..2024) timeframe=1982-2025`. **X5 overlap against `$OR`, 2006-2024: NTx35 and THI_max identical (max \|diff\| 0); NDWS and NDWL0 exactly 0 from the first published season (2006) on.** `$OL` = 33 GB, kept. Nothing published, no R/3.

Stamp `20261105_091344`. X0-X2 are in interim 1 and X3 in interim 2, both below. **Total wall-clock 09:13 → 15:59, 6 h 45 min.**

#### X4 — S3-S5, both commands as written. PASS
Started 14:26:56 (`logs/obs_long_s35{,_rh15}_<stamp>.log`). The main command took **74 min** (done 15:40:57) and the `-rh15` livestock command **26 min** (done 14:52:41). 0 WARN.
- Both print `seasons kept=43 (1982..2024) timeframe=1982-2025`.
- `done: 129 bundles` + `done: 30 bundles` = **159** `*_int.tif` in `$OL/Data/hazard_risk/ggcmi-crop/`. No FPAR bundle: S6 was not run.
- The window diagnostics equal L2's exactly: `annual12 … =12: 165134; NA on land: 0`, and `NA on land: 110` on every GGCMI window.
- **Seasonal stacks: 95 files, each with 44 layers named 1981 … 2024, not 43.** This is the same shape as `$OR`: its stacks hold
  **20** layers (2005 … 2024) for `seasons kept=19`. S3 writes the spin-up season into the stack (`layer per season, named by start
  year`, `R/obs/s3_s5_obs_hazards.R:15`), and the bundles use the 43 kept seasons. **So 43 + 1 spin-up layer is the expected
  invariant;** the block's "43 layers" counts kept seasons. I did not stop on it, because `$OR` shows the same relationship and
  X5 reads layers by name.
- The 8 `GDAL error 1` lines (`libtiff.so.6: version LIBTIFF_4.6.1 not found (required by /opt/conda/lib/gdalplugins/../libgdal.so.36)`)
  are a long-standing node warning, already in the May `cr068_*` logs. It comes from the image's `GDAL_DRIVER_PATH` (server-environment
  §9): terra tries the `/opt/conda` plugins, fails to load them, and keeps its built-in GTiff driver. The outputs are unaffected, as
  X5 shows. (The base conda history confirms the 2026-11-05 `gh` install added only `gh`.)

#### X5 — overlap gate against `$OR`. PASS
`logs/obs_long_x5_overlap_<stamp>.R`, per-layer table in `…csv`. For every window (19) × index stack in
`$OR/Data/obs_seasonal/<token>_2006-2025/` (`NDWS_mean`, `NDWL0_mean`, `NTx35_mean`, `THI_max_max`, plus `THI_max_max` for `-rh15`),
I matched each layer in `$OL/…_1982-2025/` by name: **1,900 layer pairs, 0 files missing, 0 cells where one root is NA and the other
is not.**

max |diff| over all windows and cells, per season start year:

| year | NDWS | NDWL0 | NTx35 | THI_max (both tokens) |
|---|---|---|---|---|
| 2005 (`$OR`'s spin-up season, not kept there) | 4.67 | 2.75 | **0** | **0** |
| **2006 … 2024 (each year)** | **0** | **0** | **0** | **0** |

- **NTx35 / THI_max: identical** in every season, including 2005.
- **NDWS / NDWL0: converged before the first published season.** The only difference is in `$OR`'s own seed season 2005, where
  `$OR` started from `AVAIL_SEED_MONTHS=2005-01` and `$OL` carries 24 years of state. **From 2006 on the difference is exactly 0, not
  float noise.** The bucket forgets its seed within the 2005 season, so the published 2006-2025 product is also free of its seed.
- `PTOT`: no stack in either root, so there is nothing to compare.

#### X6 — wrap
- `du -sh $OL` = **33 GB**. **`$OL` kept**; the trend/normal/anomaly block reads it.
- Nothing written outside `$OL` and `logs/`. `$OR`, `$SM`, the NEX trees and S3 untouched.
- **Correction to interim 2:** the S2 rates were not "matching" the 21-year run; they were **faster**. NDWS ~8.8 s/month here against
  ~11.9 s in L2, NDWL0 ~15.7 against ~19.7.

---

### RESPONSE — cglabs 2026-11-05 (interim 2) — **X3 PASS: S2 complete, 540 monthly files per index, 0 WARN, 0 `daily inputs missing`, 0 errors.** X4 (S3-S5) launched 14:26:56

All five scripts launched together at 11:37:52 with the block's environment (`COMMON_DATA=$OL SCENARIO=historical YRS=1981:2025
DAYS_CHECK=strict AVAIL_SEED_MONTHS=1981-01`), unset afterwards; `logs/obs_long_s2_{ndws,ndwl0,ntx,thi,thi_rh15}_<stamp>.log`.
Peak memory stayed low: 14 GB used of 376 GB with all five running.

| script | token | end (log) | wall | monthly files | WARN | missing | error |
|---|---|---|---|---|---|---|---|
| `fast_calc_NDWS.R` | main | 12:57:20 | **79 min** | NDWS **540** (+540 `AVAIL`) | 0 | 0 | 0 |
| `fast_calc_NDWL0.R` | main | 13:58:43 | **141 min** | NDWL0 **540** (+540 `AVAIL`) | 0 | 0 | 0 |
| `calc_NTx.R` | main | 14:26:08 | **168 min** | NTx20 … NTx50, **540 each** (31 thresholds) | 0 | 0 | 0 |
| `calc_THI.R` | main | 12:16:49 | 39 min | THI_max **540**, THI_mean 540 | 0 | 0 | 0 |
| `calc_THI.R` | `-rh15` | 12:16:49 | 39 min | THI_max **540**, THI_mean 540 | 0 | 0 | 0 |

540 = 45 years × 12. As in RESPONSE (6), NDWS/NDWL0 directories hold twice that because of the `AVAIL-YYYY-MM.tif` carry-over
state. The THI dirs also hold a `daily/` subdirectory, the same as `$OR`. The NDWS/NDWL0 rate is ~8.5 s and ~15 s per month,
matching the 21-year run's pace scaled to 45 years.

---

### RESPONSE — cglabs 2026-11-05 (interim 1) — **X0-X2 PASS.** Selftests 23/9/24; soil fill identical to L1 (2,494 → 49) and byte-identical to `$OR`'s filled soils; S1 ingest 1981-2025 complete, 16,436 days per variable, 0 failures, no S1Stop; worst repaired day 65 px (limit 100); **2005-2025 overlap byte-identical to `$OR` on all six series**. X3 (S2) running.

Stamp `20261105_091344`. HEAD `8c9ee59`, branch `develop`. Env grep empty before each launch. `OL={common_data}/atlas_obs_hazards_1981`; nothing written outside `$OL` and `logs/`.

(Step 1 of the hand-off prompt, the M/P RESPONSE in `DISPATCH_cglabs_obs_s3_r3.md`, was already pushed as `7624c02` on 2026-11-05 and is on origin; nothing redone.)

#### X0 — selftests. PASS
`s1_ingest.py selftest` **23** ok, `soil_fill.py selftest` **9** ok, `s3_s5_obs_hazards.R --selftest` **24** ok (all via `pyobs`).

#### X1 — link + soil fill. PASS
`link` made the two symlinks (`chirps_wrld`, `atlas_hazards/soils`). The fill then replaced the soils symlink with filled copies, as in L1:

| layer | res | max px | valid | filled | still NA | filled range | source range |
|---|---|---|---|---|---|---|---|
| `sscp_world.tif` | 0.05000 deg | 10 | 3,712,662 | **638,524** | 10,044,814 | 7.392 .. 107.25 | 4.97 .. 111 |
| `ssat_world.tif` | 0.05000 deg | 10 | 3,712,662 | **638,524** | 10,044,814 | 10.127 .. 183.05 | 6.78 .. 200 |

`--check-pr`: obs land cells 48,576, without soil **2,494 → 49** for both layers, the same as L1. Shared soils: md5 of all six files identical before and after (`logs/obs_long_soil_md5_{before,after}_<stamp>.txt`). Extra check: the md5 of every file in `$OL/atlas_hazards/soils` equals `$OR/atlas_hazards/soils`.

#### X2 — S1 ingest 1981-2025. PASS
Both processes as written (`logs/obs_long_{cubes,agera5}_<stamp>.log`). Wall-clock: cubes (pr, tasmax, tasmin) 09:14 → 10:26, **73 min**; AgERA5 (rsds, hurs mean, hurs rh15) 09:14 → 11:27, **133 min**. Manifests 49,308 rows each, **0 failures**, no S1Stop, no traceback. Sources pinned as in `$OR`: CHIRPS cube rev `e9466d3`, CHIRTS cube rev `c44a0b5`, AgERA5 v2.0 on the FAO GISMGR mirror.

**validate**, `--years 1981:2025`, both **VALIDATE PASS**, lattice equals `base_rast_nexgddp.tif`:

| variable (token) | days | domain-mean over days, min / median / max |
|---|---|---|
| pr | 16,436 / 16,436 | 0.54 / 1.77 / 4.53 mm d-1 |
| tasmax | 16,436 / 16,436 | 23.54 / 32.35 / 35.74 degC |
| tasmin | 16,436 / 16,436 | 12.95 / 20.76 / 23.85 degC |
| rsds | 16,436 / 16,436 | 16.77 / 21.37 / 24.12 MJ m-2 d-1 |
| hurs (`-rh15`) | 16,436 / 16,436 | 29.81 / 37.05 / 48.20 % |

`tasmax < tasmin` in sampled days: 0 cells. (The main-token `validate` covers pr/tasmax/tasmin/rsds by default; hurs main has 16,436 files in its manifest.)

**Digests** (md5 of the sorted `md5  filename` list per series, from the manifests, 1981-2025):
```
pr      chirps-chirts-agera5       16436  fb7db5b64a46d04f4003cb1643bff672
tasmax  chirps-chirts-agera5       16436  e35857fd757602f270d149161dbc94c1
tasmin  chirps-chirts-agera5       16436  e6601bc4f455d337c19f6b993bddf75e
rsds    chirps-chirts-agera5       16436  f9edbc51506e70bd426c2a8838b474fd
hurs    chirps-chirts-agera5       16436  29fe6672f32c32a3995ab83423775dfe
hurs    chirps-chirts-agera5-rh15  16436  0dc10df7b80dfd7e57d0962d91e042e2
```

**Overlap 2005-2025, byte-identical to `$OR`.** Every daily file hashed in both roots (`logs/obs_long_overlap_md5_<stamp>.py`, summary `…_summary.txt`), same digest definition:
```
series                                     n(OR)  n(OL)  digest OR = digest OL               mismatches
pr/historical/chirps-chirts-agera5          7670   7670  d48c8a05328fb2610380b10796923fbc   0
tasmax/historical/chirps-chirts-agera5      7670   7670  41300d604fc09ae0fd977fdcaa57f500   0
tasmin/historical/chirps-chirts-agera5      7670   7670  7ea92f4318de917c97ec4890b65659da   0
rsds/historical/chirps-chirts-agera5        7670   7670  fd50cc39c653ae0ec8cde295d976ed5a   0
hurs/historical/chirps-chirts-agera5        7670   7670  daef4b9041a867a6d7a505f0a930849b   0
hurs/historical/chirps-chirts-agera5-rh15   7670   7670  08ff7ca2997800a0f970b30aee622e07   0
```

**FLAG lines: 299 in total.** tasmax repaired above 57 °C on **7** days, tasmin above 43 °C on **277** days, and AgERA5 RH06 with fewer than 5 hours on **15** days. **The most repaired native pixels on any one day is 65** (tasmin, 2023), under the 100-pixel stop. Tmin for 2005-2025 is **222 days, exactly the `$OR` count**; 1981-2004 adds 55. That is fewer than "roughly twice" because the early decades have few hot nights above the cap. The 15 RH06 days are the AgERA5 objects older than the archive's 2025-09 rewrite, which the ingest names: 1985-05-11, 1989-11-08, 1989-12-17, 1989-12-31, 1990-11-21, 1991-11-19, 4 × 2000, 2004, 2009-10-05, 2014-12-25, 2019-08-13, 2021-11-09. Each flags **504 cells** with fewer than 5 hours. The four from 2009 on are the same four days `$OR` named.

Per year: days flagged, and the most pixels repaired on one day (tasmax/tasmin only). Years not listed have no flags.

| year | tasmax days | tasmin days | hurs RH06 days | max px/day |
|---|---|---|---|---|
| 1981 | 0 | 1 | 0 | 2 |
| 1983 | 0 | 1 | 0 | 3 |
| 1984 | 0 | 3 | 0 | 6 |
| 1985 | 0 | 0 | 1 | 0 |
| 1987 | 0 | 1 | 0 | 12 |
| 1988 | 1 | 8 | 0 | 16 |
| 1989 | 0 | 0 | 3 | 0 |
| 1990 | 0 | 5 | 1 | 19 |
| 1991 | 0 | 0 | 1 | 0 |
| 1992 | 0 | 1 | 0 | 4 |
| 1993 | 0 | 2 | 0 | 5 |
| 1995 | 0 | 4 | 0 | 13 |
| 1996 | 0 | 1 | 0 | 9 |
| 1997 | 0 | 1 | 0 | 2 |
| 1998 | 0 | 9 | 0 | 25 |
| 1999 | 0 | 4 | 0 | 8 |
| 2000 | 0 | 9 | 4 | 14 |
| 2001 | 0 | 4 | 0 | 3 |
| 2003 | 0 | 1 | 0 | 4 |
| 2004 | 0 | 0 | 1 | 0 |
| 2005 | 0 | 4 | 0 | 24 |
| 2006 | 0 | 9 | 0 | 11 |
| 2007 | 0 | 2 | 0 | 23 |
| 2008 | 0 | 6 | 0 | 8 |
| 2009 | 1 | 2 | 1 | 14 |
| 2010 | 0 | 11 | 0 | 20 |
| 2011 | 1 | 6 | 0 | 29 |
| 2012 | 0 | 7 | 0 | 28 |
| 2013 | 0 | 3 | 0 | 15 |
| 2014 | 0 | 5 | 1 | 19 |
| 2015 | 2 | 9 | 0 | 29 |
| 2016 | 1 | 10 | 0 | 28 |
| 2017 | 1 | 14 | 0 | 31 |
| 2018 | 0 | 6 | 0 | 10 |
| 2019 | 0 | 10 | 1 | 10 |
| 2020 | 0 | 11 | 0 | 27 |
| 2021 | 0 | 19 | 1 | 26 |
| 2022 | 0 | 6 | 0 | 32 |
| 2023 | 0 | 29 | 0 | 65 |
| 2024 | 0 | 43 | 0 | 51 |
| 2025 | 0 | 10 | 0 | 27 |

#### X3 — S2, launched 11:37:52, all five scripts as written (PIDs logged). Report follows.

---

### MACBOOK 2026-11-05 — X0–X6: same pipeline, 1981–2025, in a NEW root; nothing published

**Why.** Pete decided (2026-11-05) to extend the observed baseline back to the start of the source data, so that:
- **trends** use the full series (43 seasons, 1982–2024, instead of 19);
- **anomalies** can be taken against the WMO normal **1991–2020**;
- observed **1995–2014** can be compared like-for-like with the NEX-GDDP historical baseline.

The CDH cubes start on 1981-01-01, so the data years are **1981–2025**. One spin-up year gives seasons **1982–2024**
(timeframe label `1982-2025`). The published 2006–2025 product is not touched and stays reproducible from `$OR`.
The trend/anomaly products are a later block; this one only builds the inputs.

**Rules.** Code is unchanged; this is the same S1 → S2 → S3-S5 chain with new years and a new root.
- Write only under **`OL={common_data}/atlas_obs_hazards_1981`**. Never under `$OR`, `$SM` or the NEX trees.
- Every long step runs in the background with `nohup`, logging to `logs/obs_long_<step>_$STAMP.log` plus a `.pid`. Track it with
  `while kill -0 $(cat x.pid) 2>/dev/null; do sleep 60; done` or the log tail, not `wait`.
- `env | grep -E 'FORCE_OVERWRITE|DAYS_CHECK|AVAIL_SEED|R3_|ATLAS_CLIMDAT'` must be empty before each launch.
- Obs python: `env -u GDAL_DRIVER_PATH PROJ_DATA=<env>/share/proj GDAL_DATA=<env>/share/gdal $PY …` (or `pyobs`).
- No R/3 and no publish in this dispatch.

#### X0 — pull, selftests
```bash
cd $REPO && git pull --ff-only && git log -1 --format='%h %s'
$PY python/obs/s1_ingest.py selftest        # 23
$PY python/obs/soil_fill.py selftest        # 9
Rscript R/obs/s3_s5_obs_hazards.R --selftest  # 24
```

#### X1 — new root: link, then soil fill (same fill as #56)
```bash
mkdir -p $OL
$PY python/obs/s1_ingest.py link --out-root $OL --common-data {common_data}
$PY python/obs/soil_fill.py fill --src-dir {common_data}/atlas_hazards/soils --out-root $OL \
    --check-pr $OR/nex-gddp-cmip6/pr/historical/chirps-chirts-agera5/pr_2015-06-15.tif > logs/obs_long_soilfill_$STAMP.log 2>&1
```
Expect the same numbers as L1 (filled 638,524 cells per layer; land without soil 2,494 → 49). The shared soils' md5
must be unchanged.

#### X2 — S1 ingest 1981–2025 (two processes, as I3)
```bash
nohup $PY python/obs/s1_ingest.py ingest --out-root $OL --years 1981:2025 --vars pr,tasmax,tasmin --rh none > logs/obs_long_cubes_$STAMP.log 2>&1 &
nohup $PY python/obs/s1_ingest.py ingest --out-root $OL --years 1981:2025 --vars rsds --rh both --workers 8  > logs/obs_long_agera5_$STAMP.log 2>&1 &
```
Then `validate --out-root $OL --years 1981:2025` for the main token, and `--vars hurs --token chirps-chirts-agera5-rh15`.
Expect 16,436 days per variable.

Report:
- days per variable;
- wall-clock;
- the domain-mean lines;
- digests;
- **every FLAG line**, counted per year and per variable. Tmax repaired above 57 °C, Tmin above 43 °C, and AgERA5 RH
  with fewer than 5 hours.

For the overlap 2005–2025, the daily files must be **byte-identical** to `$OR`: same code, same pinned sources.
State the per-variable md5-list digest for 2005–2025 in both roots.

**Stop on:**
- any S1Stop;
- any day with more than 100 repaired native pixels;
- an overlap mismatch.

The flagged-day *count* is not a stop by itself: Tmin had 222 days in 2005–2025, so expect roughly twice that.

#### X3 — S2, all five scripts, seeded at the first data year
```bash
cd $REPO/hazards_upstream/R/04_indices
export COMMON_DATA=$OL SCENARIO=historical YRS=1981:2025 DAYS_CHECK=strict AVAIL_SEED_MONTHS=1981-01
GCMS=chirps-chirts-agera5      nohup Rscript fast_calc_NDWS.R  > $REPO/logs/obs_long_s2_ndws_$STAMP.log 2>&1 &
GCMS=chirps-chirts-agera5      nohup Rscript fast_calc_NDWL0.R > $REPO/logs/obs_long_s2_ndwl0_$STAMP.log 2>&1 &
GCMS=chirps-chirts-agera5      nohup Rscript calc_NTx.R        > $REPO/logs/obs_long_s2_ntx_$STAMP.log 2>&1 &
GCMS=chirps-chirts-agera5      nohup Rscript calc_THI.R        > $REPO/logs/obs_long_s2_thi_$STAMP.log 2>&1 &
GCMS=chirps-chirts-agera5-rh15 nohup Rscript calc_THI.R        > $REPO/logs/obs_long_s2_thi_rh15_$STAMP.log 2>&1 &
unset COMMON_DATA SCENARIO YRS DAYS_CHECK AVAIL_SEED_MONTHS
```
Expect **540** monthly files per index, 0 WARN and 0 `daily inputs missing`. NDWS took 50 min and NDWL0 83 min for 21
years, so expect about 2–3.5 h here.

#### X4 — S3-S5, both commands
```bash
nohup Rscript R/obs/s3_s5_obs_hazards.R --obs-root $OL --token chirps-chirts-agera5 --years 1981:2025 --spinup 1 \
      --ggcmi-dir $NWD/Data/ggcmi > logs/obs_long_s35_$STAMP.log 2>&1 &
nohup Rscript R/obs/s3_s5_obs_hazards.R --obs-root $OL --token chirps-chirts-agera5 --thi-token chirps-chirts-agera5-rh15 \
      --years 1981:2025 --spinup 1 --sets livestock --ggcmi-dir $NWD/Data/ggcmi > logs/obs_long_s35_rh15_$STAMP.log 2>&1 &
```
Expect:
- `seasons kept=43 (1982..2024) timeframe=1982-2025`;
- 129 + 30 bundles under `$OL/Data/hazard_risk/ggcmi-crop/`. They are not used yet and will not be published from here;
- the seasonal stacks under `$OL/Data/obs_seasonal/<token>_1982-2025/<window>/` with **43 layers each**.

#### X5 — overlap gate against the published run (judge the new product against its own inputs, then against `$OR`)
For the 19 seasons 2006–2024, compare `$OL/Data/obs_seasonal/chirps-chirts-agera5_1982-2025/<window>/<index>_<stat>.tif`
with the same layer in `$OR/Data/obs_seasonal/chirps-chirts-agera5_2006-2025/…`, for every window and index.
- **NTx35 / THI_max:** the months are computed independently, so they must be **identical**. Report max |diff|;
  anything above 0 is a stop.
- **NDWS / NDWL0:** these carry soil-water state month to month, and the run now starts from a 1981 seed instead of
  2005. The difference should be largest in the first season and vanish within a year or two. Report max |diff| per
  start year 2006…2024. **It should be 0, or float noise, from about 2007 on.** If a difference persists beyond 2008,
  that is a stop: the state is not converging.
- `PTOT` (if present): identical.

#### X6 — report
Prepend `### RESPONSE` with X0–X5, the disk use of `$OL` (`du -sh`), and the total wall-clock. Commit only this file,
push, verify, delivery line. **Keep `$OL`.** The trend/normal/anomaly step reads it next.
