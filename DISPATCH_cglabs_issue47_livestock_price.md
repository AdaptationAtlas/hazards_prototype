# DISPATCH — cglabs — #47: livestock nominal-USD priced at the world median

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### RESPONSE 2026-10-21 — cglabs — B. Re-run complete. All gates PASS. The measurement is below.

**Outcome: A3.1 PASS, GATE 0 PASS, A3.3 invariants all hold.** The probe ran clean in **0.8 min**
and wrote its CSV. The two `from skipped section 2:` lines appeared exactly as block B predicted,
before `loading FAOSTAT data`:

```
[2026-10-21 12:52:12] [probe-041] from skipped section 2: line 100  dataset_name <- "glw4-2020"
[2026-10-21 12:52:12] [probe-041] from skipped section 2: line 99  glw_pro_dir <- glw2020_pro_dir
```

`glw_pro_dir` was indeed the next stop — thank you for catching it; my scan asked what the *kept
0.4.1 bands* need and never asked what the *probe body* needs.

Run: HEAD `c384a7d`, `EXPOSURE_RES=0.25`, STAMP `20261021_125145`, grid `base_rast_nexgddp.tif`
res 0.2500 tag `res-25`. Log `logs/probe_041_20261021_125145.log`. Nothing on the "What NOT to do"
list was touched: no 0.4.x / R/3 / publish run, no edits to 0.4.1, the probe, `R/price_fill.R` or
`metadata/haz_metadata.csv`, no S3 read or write.

#### GATE 0 — PASS

```
[2026-10-21 12:52:31] [probe-041] --- GATE 0: A vs glw4-2020_vop_nominal-usd-2021-prices-used.csv (mtime 2026-09-23 13:29)
[2026-10-21 12:52:31] [probe-041] GATE 0: 495 rows; live total 104.234 B, probe A total 104.234 B; 0 rows differ
[2026-10-21 12:52:31] [probe-041] GATE 0 PASS: probe A reproduces the published values
```

495 rows, 0 differing. The code still reproduces what is live, so everything below is a measurement
of the live product, not of a drifted re-derivation.

#### A3.3 invariants — all hold

- total A > 0: y2021 livestock nominal **A = 104.234 B USD**.
- B/A and C/A finite at every printed level: country×class, GLW class, product, year set — no `NaN`,
  no `Inf`, no division by a zero A anywhere in the tables below.
- every country × product row with production > 0 has a source other than `none` for both B and C.
  Checked directly on the CSV rather than only from the printed table:

```
rows: 1485   year_sets: y2015,y2020,y2021
rows with production>0: 1170
source_B missing/none: 0
source_C missing/none: 0
non-finite vop_A/B/C: 0 0 0
non-finite price_usd_final/price_C: 0 0
unique source_B: continent mean | milk: world override | neighbours mean | own (longer series) | own (window median) | region mean
unique source_C: continent median | milk: world override | neighbours median | own | region median
```

#### A4 — the log verbatim, from the FAO inputs to the end

```
[2026-10-21 12:52:27] [probe-041] --- FAO inputs
[2026-10-21 12:52:27] [probe-041] Value_of_Production_E_Africa.csv                 mtime 2026-05-15  size 15 MB
[2026-10-21 12:52:27] [probe-041] Prices_E_Africa_NOFLAG.csv                       mtime 2026-05-15  size 6 MB
[2026-10-21 12:52:27] [probe-041] Prices_E_All_Data_(Normalized).csv               mtime 2026-05-15  size 214 MB
[2026-10-21 12:52:27] [probe-041] Production_Crops_Livestock_E_Africa_NOFLAG.csv   mtime 2026-05-15  size 13 MB
[2026-10-21 12:52:31] [probe-041] --- GATE 0: A vs glw4-2020_vop_nominal-usd-2021-prices-used.csv (mtime 2026-09-23 13:29)
[2026-10-21 12:52:31] [probe-041] GATE 0: 495 rows; live total 104.234 B, probe A total 104.234 B; 0 rows differ
[2026-10-21 12:52:31] [probe-041] GATE 0 PASS: probe A reproduces the published values
[2026-10-21 12:52:31] [probe-041] --- y2021 totals (Africa, all countries)
[2026-10-21 12:52:31] [probe-041] livestock nominal: A 104.234 B | B 112.462 B (B/A 1.079) | C 109.070 B (C/A 1.046) | constant-I$ for scale 89.020 B
[2026-10-21 12:52:31] [probe-041] --- by GLW class (the published tier's species rows, before highland/tropical split)
   glw3_name      A_bn      B_bn     C_bn B_over_A C_over_A
      <char>     <num>     <num>    <num>    <num>    <num>
1:    cattle 49.275054 53.085518 50.07547    1.077    1.016
2:   poultry 25.171578 27.870441 28.96941    1.107    1.151
3:     sheep 13.224083 14.923290 14.06783    1.128    1.064
4:     goats 11.517177 11.029042 10.56570    0.958    0.917
5:      pigs  5.046395  5.553752  5.39156    1.101    1.068
[2026-10-21 12:52:31] [probe-041] --- by product
     atlas_name      A_bn      B_bn      C_bn n_ctry B_over_A C_over_A
         <char>     <num>     <num>     <num>  <int>    <num>    <num>
1:  cattle_meat 29.663552 28.560904 26.502575     50    0.963    0.893
2:  cattle_milk 19.611502 24.524614 23.572897     49    1.251    1.202
3: poultry_meat 17.775039 19.493790 20.557182     49    1.097    1.157
4:   sheep_meat 10.866246 12.565453 11.709995     50    1.156    1.078
5:    goat_meat  7.946346  7.458210  6.994867     50    0.939    0.880
6: poultry_eggs  7.396539  8.376652  8.412229     48    1.133    1.137
7:     pig_meat  5.046395  5.553752  5.391560     44    1.101    1.068
8:    goat_milk  3.570832  3.570832  3.570832     27    1.000    1.000
9:   sheep_milk  2.357837  2.357837  2.357837     23    1.000    1.000
[2026-10-21 12:52:31] [probe-041] --- per (iso3, GLW class) row factor, 243 rows; quantiles min/10/25/50/75/90/max
[2026-10-21 12:52:31] [probe-041] B/A: 0.55 / 0.75 / 0.93 / 1.06 / 1.24 / 1.38 / 2.20
[2026-10-21 12:52:31] [probe-041] C/A: 0.43 / 0.68 / 0.80 / 0.97 / 1.24 / 1.50 / 2.20
[2026-10-21 12:52:31] [probe-041] rows moving > 10% either way: B 156 (72% of A value), C 166 (69% of A value)
[2026-10-21 12:52:31] [probe-041] rows moving > 50% either way: B 21 (5% of A value), C 48 (8% of A value)
[2026-10-21 12:52:31] [probe-041] rows moving > 100% either way: B 1 (0% of A value), C 2 (0% of A value)
[2026-10-21 12:52:31] [probe-041] --- top 30 (iso3, GLW class) rows by |B - A| USD
      iso3 glw3_name  A_mn  B_mn  C_mn    rB    rC
    <char>    <char> <num> <num> <num> <num> <num>
 1:    ZAF   poultry  5669  3857  3857  0.68  0.68
 2:    ZAF    cattle  6314  4538  4538  0.72  0.72
 3:    EGY    cattle  3927  5461  5461  1.39  1.39
 4:    EGY   poultry  6124  7559  7559  1.23  1.23
 5:    TZA    cattle  3969  5321  4343  1.34  1.09
 6:    DZA     sheep  2413  3609  3609  1.50  1.50
 7:    SDN    cattle  3008  4180  4113  1.39  1.37
 8:    NGA   poultry  2012  3174  3497  1.58  1.74
 9:    MAR    cattle  2445  3383  3383  1.38  1.38
10:    ZWE    cattle  3240  2706  2631  0.83  0.81
11:    MAR     sheep  1015  1431  1431  1.41  1.41
12:    MOZ   poultry   358   686   686  1.92  1.92
13:    DZA    cattle  1773  2100  2100  1.18  1.18
14:    MAR   poultry  2095  1780  1780  0.85  0.85
15:    TZA   poultry   642   942   951  1.47  1.48
16:    DZA   poultry  1597  1304  1304  0.82  0.82
17:    MWI   poultry   304   576   460  1.89  1.51
18:    ZMB    cattle  1028  1275  1275  1.24  1.24
19:    ZAF      pigs   797   568   568  0.71  0.71
20:    SSD    cattle  1891  2113  2012  1.12  1.06
21:    KEN   poultry   389   607   607  1.56  1.56
22:    NGA    cattle  1658  1446  1315  0.87  0.79
23:    TUN    cattle   868   660   660  0.76  0.76
24:    MLI    cattle   397   596   596  1.50  1.50
25:    ETH     goats   786   588   621  0.75  0.79
26:    MOZ      pigs   326   510   510  1.57  1.57
27:    BFA   poultry   497   667   781  1.34  1.57
28:    MWI     goats   428   260   260  0.61  0.61
29:    SDN     goats  1533  1700  1700  1.11  1.11
30:    UGA      pigs   508   657   558  1.29  1.10
      iso3 glw3_name  A_mn  B_mn  C_mn    rB    rC
[2026-10-21 12:52:31] [probe-041] --- price sources, y2021 (country x product rows with production)
Key: <source>
                  source   B_n B_pct_A   C_n
                  <char> <int>   <num> <int>
 1:       continent mean    73    10.1    NA
 2:     continent median    NA      NA    41
 3: milk: world override    50     5.7    50
 4:      neighbours mean   107    23.7    NA
 5:    neighbours median    NA      NA   163
 6:                  own    NA      NA    94
 7:  own (longer series)    37     9.8    NA
 8:  own (window median)    57    37.6    NA
 9:          region mean    66    13.2    NA
10:        region median    NA      NA    42
[2026-10-21 12:52:31] [probe-041] --- C: own producer prices clipped (outside [1/5, 5] x world median for item-year), y2021 run
     iso3 atlas_name  year price_usd price_world ratio_world
   <char>     <char> <int>     <num>       <num>       <num>
1:    NER sheep_milk  2022     183.6       962.2      0.1910
2:    BWA  goat_milk  2020      30.8       754.9      0.0408
3:    BWA  goat_milk  2021      31.8       813.2      0.0391
[2026-10-21 12:52:31] [probe-041] --- top 30 B prices vs world (|log ratio|) -- what the one-token fix would publish
      iso3   atlas_name            source_B price_B price_world ratio_B price_C
    <char>       <char>              <char>   <num>       <num>   <num>   <num>
 1:    ZMB  cattle_milk own (window median)  1358.0       474.3   2.860  1358.0
 2:    MLI  cattle_milk own (longer series)  1214.0       474.3   2.560  1214.0
 3:    MWI  cattle_milk     neighbours mean  1116.0       474.3   2.350  1116.0
 4:    SLE poultry_eggs own (longer series)  4384.0      1869.0   2.350  4384.0
 5:    RWA   sheep_meat own (longer series)  2376.0      5479.0   0.434  2376.0
 6:    LSO  cattle_milk own (window median)  1085.0       474.3   2.290  1085.0
 7:    LSO poultry_eggs     neighbours mean   839.9      1869.0   0.449   839.9
 8:    ZAF poultry_eggs own (window median)   839.9      1869.0   0.449   839.9
 9:    GMB  cattle_milk own (window median)   998.8       474.3   2.110   998.8
10:    MWI    goat_meat own (longer series)  2567.0      5396.0   0.476  2567.0
11:    COD  cattle_milk     neighbours mean   981.0       474.3   2.070   604.3
12:    MLI poultry_meat own (longer series)  5056.0      2448.0   2.070  5056.0
13:    TZA  cattle_milk     neighbours mean   945.8       474.3   1.990   739.8
14:    BEN  cattle_milk     neighbours mean   240.3       474.3   0.507   240.3
15:    BFA  cattle_milk     neighbours mean   240.3       474.3   0.507   726.9
16:    NER  cattle_milk own (window median)   240.3       474.3   0.507   240.3
17:    NGA  cattle_milk     neighbours mean   240.3       474.3   0.507   240.3
18:    TCD  cattle_milk     neighbours mean   240.3       474.3   0.507   240.3
19:    CIV  cattle_milk     neighbours mean   935.1       474.3   1.970  1074.0
20:    GIN  cattle_milk own (window median)   935.1       474.3   1.970   935.1
21:    GNB  cattle_milk     neighbours mean   935.1       474.3   1.970   800.6
22:    LBR  cattle_milk     neighbours mean   935.1       474.3   1.970   935.1
23:    SLE  cattle_milk     neighbours mean   935.1       474.3   1.970   935.1
24:    MOZ poultry_meat own (window median)  4790.0      2448.0   1.960  4790.0
25:    MWI poultry_meat     neighbours mean  4790.0      2448.0   1.960  3693.0
26:    BEN poultry_eggs     neighbours mean  3475.0      1869.0   1.860  3475.0
27:    BFA poultry_eggs     neighbours mean  3475.0      1869.0   1.860  3001.0
28:    NER poultry_eggs own (window median)  3475.0      1869.0   1.860  3475.0
29:    NGA poultry_eggs     neighbours mean  3475.0      1869.0   1.860  3475.0
30:    TCD poultry_eggs     neighbours mean  3475.0      1869.0   1.860  3475.0
      iso3   atlas_name            source_B price_B price_world ratio_B price_C
    ratio_C production_t
      <num>        <num>
 1:   2.860       461900
 2:   2.560       293500
 3:   2.350        64050
 4:   2.350        11580
 5:   0.434         3285
 6:   2.290       180000
 7:   0.449         1396
 8:   0.449       562000
 9:   2.110        49690
10:   0.476        59510
11:   1.270         8832
12:   2.070        49550
13:   1.560      3552000
14:   0.507       114600
15:   1.530       220700
16:   0.507       840000
17:   0.507       523300
18:   0.507       243700
19:   2.270        35760
20:   1.970       220700
21:   1.690        17960
22:   1.970         8299
23:   1.970       146900
24:   1.960       135700
25:   1.510       106200
26:   1.860        15320
27:   1.610        68110
28:   1.860        10590
29:   1.860       640000
30:   1.860         4050
    ratio_C production_t
[2026-10-21 12:52:31] [probe-041] --- basis: rows whose nominal/constant-I$ factor is > 3x or < 1/3 the product median (B 3, C 3)
Key: <atlas_name>
     iso3   atlas_name        source_B fB_rel fC_rel vop_A_mn vop_B_mn
   <char>       <char>          <char>  <num>  <num>    <num>    <num>
1:    LBY  cattle_meat neighbours mean  39400  48300       27       36
2:    SLE  cattle_meat neighbours mean  36400  41800       48       33
3:    LSO poultry_meat neighbours mean    346    305        1        1
[2026-10-21 12:52:31] [probe-041] y2015: A 83.417 B | B 96.594 B (1.158) | C 95.475 B (1.145)
[2026-10-21 12:52:31] [probe-041] y2020: A 92.113 B | B 101.437 B (1.101) | C 101.538 B (1.102)
[2026-10-21 12:52:31] [probe-041] wrote /home/jovyan/common_data/nex-gddp-cimp6_hazards/checks/issue47_livestock_price_20261021_125145.csv (1485 rows)
[2026-10-21 12:52:31] [probe-041] done in 0.8 min
```

**CSV:** `/home/jovyan/common_data/nex-gddp-cimp6_hazards/checks/issue47_livestock_price_20261021_125145.csv`
— 1485 rows, 183 KB, under the 1 MB bar, FAOSTAT-derived values only, so it is **committed with this
block** at `R/checks/041_outputs/issue47_livestock_price_20261021_125145.csv`, following the
existing `R/checks/<n>_outputs/` convention rather than making a new top-level `checks/`. Columns: `year_set, iso3, atlas_name,
glw3_name, production_t, price_usd_global, price_usd_final, source_B, price_C, source_C, vop_A, vop_B,
vop_C, vop_intd15`.

#### My read: B is not the September crop case, but it is not clean either

**The headline.** Pricing Africa's livestock at the world median costs about **7.9 %** of the
nominal-USD total against the one-token fix (104.234 B → 112.462 B) and **4.6 %** against the crop
method (→ 109.070 B). At continental scale that is modest. **The country-level effect is not**:
156 of 243 country×class rows move more than 10 %, and those rows carry **72 % of the A value**.
So almost three quarters of the published livestock nominal-USD value sits on a price that moves
materially once the country's own price is used. That is the size of the effect you asked for.

**The direction is not uniform**, which is worth saying because "world median" sounds like it would
only ever under-price poor countries. B/A quantiles run 0.55 / 0.93 / 1.06 / 1.24 / 2.20. South
Africa's poultry falls 32 % and its cattle 28 %; Egypt's cattle rises 39 %, Nigeria's poultry 58 %,
Mozambique's poultry 92 %. The world median is simply the wrong statistic in both directions.

**On the specific question — does B carry the September pattern?** Partly. Taking the two halves
separately:

- **Outliers beyond 5× world: no.** The top-30 B-vs-world table maxes out at **2.86×** (ZMB
  cattle_milk) and bottoms at **0.434×** (RWA sheep_meat). Every B price is inside the [1/5, 5] band
  that C has to clip to. C's clip list confirms it from the other side: it clips only **three**
  prices in the whole y2021 run (NER sheep_milk 0.19×, BWA goat_milk 0.041× / 0.039×), and those are
  own-price rows, not fills. So the clipping that mattered so much on the crop side buys almost
  nothing here.
- **Region-wide constants: yes, and this is the real finding.** Five West African countries
  (BEN, BFA, NGA, TCD + NER) share `cattle_milk` at exactly **240.3**, and the same five share
  `poultry_eggs` at exactly **3475.0**. Five more (CIV, GNB, LBR, SLE + GIN) share `cattle_milk` at
  **935.1**. Look at which row in each group is `own` rather than `neighbours mean`: NER holds the
  own price in the first two groups, GIN in the third. **The neighbour mean in these groups is one
  country's own price propagated to four others** — Niger prices Nigeria's 523,300 t of cattle milk
  and 640,000 t of eggs. That is a single-source dependency dressed as a regional average, and it is
  the same shape as the crop-side problem even though the ratios are tame.

**Where B and C actually differ.** Almost nowhere that matters at the top: the largest movers (ZAF
poultry/cattle, EGY cattle/poultry, DZA sheep, MAR cattle) have **rB == rC to two decimals**, because
they are own-price rows and both methods use the own price. The divergence is confined to fill-dependent
rows — TZA cattle 1.34 vs 1.09, NGA poultry 1.58 vs 1.74, MWI poultry 1.89 vs 1.51, BFA poultry 1.34
vs 1.57. So the B-vs-C choice is **not** clipping, it is mean-vs-median on the fills, and it changes
the Africa total by only 3.4 B (3 %).

**One thing I would not publish without a second look**, flagged by the probe's own basis table:
LBY and SLE `cattle_meat` carry a nominal/constant-I$ factor roughly **39,000× and 36,000×** the
product median under B (48,300× / 41,800× under C). The amounts are small (LBY 27 → 36 mn, SLE
48 → 33 mn) so they will not move a tier, but a factor that far out usually means a currency or a
unit problem in the underlying FAO series rather than a price-fill artefact, and both candidate
fixes inherit it. A's world-median pricing happens to mask it.

**So: is B publishable as it stands?** My read is **yes on the arithmetic, not yet on the provenance.**
It is strictly better than A — it removes a known-wrong global constant, it introduces no ratio
outside [1/5, 5], and it leaves the goat/sheep-milk world override untouched (those two products are
identical under all three methods, 1.000). But I would not publish a `neighbours mean` that is really
one country's price without either saying so in the metadata or switching those fills to C's median,
which is cheap: C already computes it, and the two differ by 3 % continent-wide. If you want a single
recommendation, it is **C's fill statistic with B's lack of clipping fuss** — but that is a
composition, not one of the three measured options, so it is your call, not a gate I can settle here.

The other year sets move the same way and by a little more, which is consistent rather than alarming:

```
y2015: A 83.417 B | B 96.594 B (1.158) | C 95.475 B (1.145)
y2020: A 92.113 B | B 101.437 B (1.101) | C 101.538 B (1.102)
```

Note y2020 is the one year set where C comes out *above* B.

#### Housekeeping

The failed block-A run left an empty `checks/` directory; it now holds this run's CSV only. The
block-A log `logs/probe_041_20261021_120942.log` is still on the node alongside the new one; both are
git-ignored. No parked or backup directory was touched.

---

### MACBOOK 2026-10-21 — B. Probe fixed probe-side; re-run block A from A2.

**Your stop was right and your diagnosis is right.** `dataset_name` lives in section 2.0.1 (line 100),
inside the band the probe skips. Fixed **probe-side**: 0.4.1 is not touched for a probe's benefit,
and it will be edited deliberately when the #47 fix is chosen.

**Your scan had one blind spot, and it would have been the next stop.** It checked what the *kept
0.4.1 bands* reference. The probe body itself also uses `glw_pro_dir` (line 99, same band) to build
GATE 0's CSV path, after `run_lines`. A scan of the skipped bands against the probe body finds exactly
these two names and nothing else:

```
skipped-band objects referenced in probe body: glw_pro_dir dataset_name
```

**The fix:** after the first slice, the probe evaluates only the two top-level assignment lines
`dataset_name <- ` and `glw_pro_dir <- ` from lines 94-168, and logs each one. It stops if either
is not found exactly once. Nothing else from the GLW sections runs.

**A1 note taken:** `glw_pro_dir` is relative after the setup `setwd()`. That is harmless here,
because the probe runs with the same working directory, but GATE 0's log line will print the
relative path.

#### B1. Re-run

`git pull`, confirm HEAD carries this block, then **A2 exactly as before**, with a new STAMP. The
existing `checks/` directory is fine. Gates **A3.1-A3.3 unchanged**, report **A4 unchanged**, and
the same "What NOT to do" list. If it stops again, stop and describe it as you did; do not patch.

The probe should now log two `from skipped section 2: line …` lines before
`loading FAOSTAT data`. If it doesn't, treat that as an A3.1 stop.

---

### RESPONSE 2026-10-21 — cglabs — A. STOPPED at gate A3.1. Probe does not evaluate.

**Outcome: A1 PASS, A2 launched, A3 gate 1 FAILED.** The probe died 29 s in, inside the 0.4.1
slices, before any pricing was computed. Per A3.1 I stopped and did not edit 0.4.1 or the probe.
No GATE 0, no totals, no tables — none of the A4 items exist yet. Nothing was written: the probe's
`fwrite` guard never fired because it never reached the write, and the output CSV does not exist.

#### A1. Pre-flight — PASS

```
HEAD: 71dcf4b peetmate docs: empty _parked_b2_rebake parent removed; r22 pre-#13 snapshot kept as the B3 diff baseline
CSV:  /home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/GLW4_2020/processed/fao_vop/glw4-2020_vop_nominal-usd-2021-prices-used.csv
      exists: TRUE   mtime: 2026-09-23 13:29:29
```

mtime is 2026-09-23, i.e. on or before 2026-10-08, so the CSV is the run that fed the reference
republish. The stop condition in A1 did not trigger.

(One note for the A1 command as written: it prints `glw2020_pro_dir` unresolved, which after
`0_server_setup.R`'s `setwd()` comes out as the relative `Data/GLW4_2020/processed/fao_vop/...`.
The absolute path above is that path through `normalizePath()`.)

#### A2. Run — launched as specified

```
STAMP=20261021_120942
OUT=/home/jovyan/common_data/nex-gddp-cimp6_hazards/checks/issue47_livestock_price_20261021_120942.csv
EXPOSURE_RES=0.25 nohup Rscript R/checks/probe_041_livestock_price.R --top 30 --out "$OUT" \
  > logs/probe_041_20261021_120942.log 2>&1 &
```

`nex-gddp-cimp6_hazards/checks/` did not exist; `mkdir -p` created it. It is a reasonable home for a
node-side check CSV and it is under `common_data`, so I used it as the dispatch specified. It is
still empty — the run produced no CSV.

#### A3 gate 1 — FAIL. The probe errors inside the 0.4.1 slices.

The full log is 21 lines, so here it is entire rather than the last 20 before the error:

```
[2026-10-21 12:09:43] [probe-041] sourcing R/0_server_setup.R
Climate data source =  nexgddp
Data acquisition is on demand - nothing was downloaded.
  atlas_require("<dataset-id>")  fetch one dataset
  atlas_require_stage("<stage>")  fetch what a stage consumes
  ATLAS_PREFETCH=all               restore the old eager behaviour
  Rscript R/checks/73_catalogue.R --status   what this host already has
0_server_setup.R has completed successfully.
CGLabs =  TRUE
[2026-10-21 12:09:52] [probe-041] evaluating 0.4.1_create_livestock_exposure.R lines 1-93, 169-182, 265-504 (GLW raster sections skipped)
[12:09:56] [0.4.1] script start (FORCE_OVERWRITE=<unset> -> overwrite=FALSE)
[12:09:56] [0.4.1] loading base raster
[12:09:56] [0.4.1] exposure grid = base_rast_nexgddp.tif | res 0.2500 | tag res-25
[12:09:56] [0.4.1] loading geoboundaries (admin0)
[12:10:12] [0.4.1] loading FAOSTAT data (VoP, prices, production)
[12:10:21] [0.4.1] merging FAOSTAT datasets (VoP / Price / Production / PPP)
[12:10:21] [0.4.1] inferring missing VoP values (production x price gap-fill)
Error in eval(e, envir = globalenv()) : object 'dataset_name' not found
Calls: run_lines -> eval -> eval -> paste0
In addition: There were 50 or more warnings (use warnings() to see the first 50)
Execution halted
```

Total runtime to the error: 29 s (12:09:52 start of evaluation → 12:10:21). The exposure grid
resolved to `res-25` as intended, geoboundaries and all three FAOSTAT tables loaded, the merge and
the VoP gap-fill ran. It got as far as section 4.2 and fell over on the first line that names the
dataset.

**Diagnosis — a slice-boundary gap in the probe, not a data problem.** It is the second kind of
A3.1 failure the dispatch anticipated: an object from a skipped section. The section anchors
resolve on this checkout to

```
 94: # 2) Load GLW4 data ####
169: ## 2.3) map GLW values to atlas #####
183: # 2.4) Load high/low mask
265: # 3) Load FAOstat data ####
505: ## 4.3) Not Run: Converting iusd2015 to nominal usd ...
```

so the probe evaluates 1-93, 169-182, 265-504 and skips 94-168 and 183-264. But:

```
R/0.4.1_create_livestock_exposure.R:100   dataset_name <- "glw4-2020"
R/0.4.1_create_livestock_exposure.R:488   names(nominal_usd) <- paste0(dataset_name, "_vop_nominal-usd-", ...)
R/0.4.1_create_livestock_exposure.R:503   names(intd_2015)   <- paste0(dataset_name, "_vop_intld15-",    ...)
```

Line 100 is a plain scalar assignment sitting in the middle of section 2.0.1, four lines after the
`# 2) Load GLW4 data` anchor and so inside the first skipped band — it is co-located with the GLW
raster directory handles (`glw_dir`, `glw_int_dir`, `glw_pro_dir`) rather than with the script
header. Lines 488 and 503 are in the `265-504` slice, which the probe does evaluate, and the
`stopifnot(exists("nominal_usd"), exists("intd_2015"), ...)` right after `run_lines` needs both to
have been named.

I scanned the two skipped bands for every other top-level assignment that the kept bands reference,
to see whether this is the only hole or the first of several. It is the only one:

```
objects assigned ONLY in skipped bands but referenced in kept bands:
  dataset_name           def L100  used L488,503
```

(Caveat on that scan: it matches top-level `name <- ...` / `name = ...` at the start of a line, so
it would not catch an object created by `assign()`, by `list2env()`, or as a data.table `:=` target.
It is a strong signal that `dataset_name` is the only gap, not a proof.)

**What I did not do.** I did not add `dataset_name <- "glw4-2020"` to the probe, move line 100 above
the section-2 anchor, or widen the first slice to `c2 + k`. Any of those would be the one-line fix,
but A3 says stop at the gate and not improvise, and the choice between patching the probe and
moving the assignment in 0.4.1 is a macbook call — moving line 100 edits a pipeline script for a
probe's benefit, which is the kind of change that wants to be deliberate.

Nothing else in the dispatch's "what NOT to do" list was touched: no 0.4.x, R/3 or publish run, no
edits to 0.4.1 / the probe / `R/price_fill.R` / `metadata/haz_metadata.csv`, no S3 reads or writes.

#### A4 — not reachable

None of the A4 items can be reported: the probe never priced anything, so there are no FAO input
lines beyond the loader messages above, no GATE 0 lines, no y2021 totals, no GLW-class or product
tables, no row-factor quantiles, no top-30 tables, no price-source or clipped-price tables, no basis
table and no other-year-set lines. No CSV to commit. **GATE 0 remains unrun**, so the question of
whether the code still reproduces the live prices-used CSV is still open.

On the closing question — whether B looks publishable, or carries the September mean-fill/outlier
pattern — I have no evidence either way and will not guess from the source. The probe needs to run
first.

#### What I suggest comes back in block B

One line, whichever side you prefer it on:

- probe-side: define `dataset_name <- "glw4-2020"` in the probe before `run_lines`, or extend the
  first slice to cover line 100; the probe already owns the assumption that the GLW sections are
  skippable, so patching it keeps the pipeline script untouched; or
- script-side: move `dataset_name <- "glw4-2020"` from line 100 up into the header block above
  `# 2) Load GLW4 data`, where it arguably belongs anyway — it is a naming constant, not a GLW raster
  handle. This is a real edit to 0.4.1 and changes nothing at runtime, but it is a pipeline file.

Re-running after either is a few minutes; the run reached section 4.2 in 29 s, so the FAO path is
healthy and the remaining cost is the three pricings.

Log on the node: `logs/probe_041_20261021_120942.log` (git-ignored). Stamp file:
`logs/probe041_stamp.txt`.

---

### MACBOOK 2026-10-21 — A. Measure only. One read-only probe; no 0.4.x run, no publish.

**What this is about.** `R/0.4.1_create_livestock_exposure.R:480-483` builds a gap-filled national
price `price_usd_final`, then computes `vop_usd_nominal := production_t * price_usd_global`, so every
country's livestock nominal-USD value is priced at the world median producer price. This has been in
the code since `a8ca8ee` (2025-07-10) and has never been measured. It affects only the nominal-USD
livestock rows: the reference's `nominal-usd-2021` livestock rows and the `vop_nominal-usd21` tier.
The intld tier uses FAOSTAT constant-I$ GPV and is untouched.

Pete wants the **size of the effect** before any fix is chosen. This block measures; it changes nothing.

**The probe** is `R/checks/probe_041_livestock_price.R` (header explains it). It evaluates only the
FAO sections of 0.4.1 (a-b, 1, 2.3's name map, 3-4.2; the GLW raster sections are skipped) and
refuses to evaluate any expression containing `fwrite` / `writeRaster` / `ensure_dir`. It prices
each (country, product) three ways:

- **A** — as published: `production_t × price_usd_global`.
- **B** — the one-token fix: `production_t × price_usd_final` (add_nearby **mean** fills, unclipped prices).
- **C** — the crop method (`R/price_fill.R`): own prices clipped to [1/5, 5] × world per item-year,
  then **median** neighbour / region / continent / world fills; same goat/sheep-milk world override.

It has been parse-checked and reviewed on the macbook. **It has not run on real data**; GATE 0 is its
first real-data check.

#### A1. Pre-flight

```bash
cd /home/jovyan/atlas/hazards_prototype && git pull --ff-only && git log -1 --format='%h %an %s'
Rscript -e 'suppressMessages(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")); f <- file.path(glw2020_pro_dir, "fao_vop", "glw4-2020_vop_nominal-usd-2021-prices-used.csv"); cat(f, "\n exists:", file.exists(f), " mtime:", format(file.mtime(f)), "\n")'
```

Report the HEAD hash and the CSV's path and mtime. The CSV was written by the last 0.4.1 run, the
run whose rasters fed the 2026-10-08 reference republish; its mtime should be on or before that date.
If it is **later**, stop and say so — the live reference would then not be the run the CSV records.

#### A2. Run (background; a few minutes)

```bash
STAMP=$(date +%Y%m%d_%H%M%S)
OUT=/home/jovyan/common_data/nex-gddp-cimp6_hazards/checks/issue47_livestock_price_${STAMP}.csv
mkdir -p "$(dirname "$OUT")" logs
EXPOSURE_RES=0.25 nohup Rscript R/checks/probe_041_livestock_price.R --top 30 --out "$OUT" \
  > logs/probe_041_${STAMP}.log 2>&1 &
echo $! > logs/probe_041_${STAMP}.pid
```

(If `nex-gddp-cimp6_hazards/checks/` is the wrong home for a node-side check CSV, put it wherever
node check outputs normally live — anywhere under `common_data`, never the container `/` — and say where.)

#### A3. Gates — stop at each

1. **The probe evaluates without error.** If it dies inside the 0.4.1 slices (an object from a
   skipped section, a missing FAO file), stop and paste the error and the 20 log lines before it.
   Do not edit 0.4.1 or the probe to get past it.
2. **GATE 0 PASS.** `A` must reproduce the CSV's `value` per (iso3, atlas_name), with 0 rows differing.
   If it fails, the probe stops itself; paste its table. That means the code no longer reproduces
   what is live (an FAO file refresh, say). This is a finding in its own right; **do not** re-run
   0.4.1 to make them agree.
3. **Invariants, not figures:** total A > 0; B/A and C/A are finite at every level printed; every
   country × product row with production > 0 has a source for B and for C other than `none`. If one
   does not hold, report which. Do not fix it.

No expected ratio is stated, on purpose: the ratio is what we are measuring.

#### A4. Report

Paste, verbatim from the log: the FAO input lines; the GATE 0 lines; the y2021 totals line; the GLW-class
and product tables; the row-factor quantiles and the `> 10/50/100 %` lines; the top-30 row table;
the price-source table; the clipped-price table; the top-30 B-vs-world table; the basis table; the
other-year-set lines; and total runtime. Give the CSV path. Commit the CSV only if it is under 1 MB
(it holds public FAOSTAT values only); otherwise leave it on the node.

Your read, briefly: does B look publishable as it stands, or does it carry the mean-fill/outlier
pattern the crop side had in September (a few region-wide constants, ratios beyond 5× world)?

#### What NOT to do

- **Do not run 0.4.1, 0.4.4, R/3, or any publish script.** No fix is chosen yet.
- **Do not edit `R/0.4.1_create_livestock_exposure.R`, the probe, or `R/price_fill.R`.**
- **Do not change `metadata/haz_metadata.csv`** (the HSH description waits for #14; #49).
- **Do not improvise at any gate.** Stop and describe what you see.

#### Reporting

Prepend a `### RESPONSE <date>` block above this one. Then verify it landed: `git fetch`;
`git log origin/develop..HEAD` empty; `git show origin/develop:DISPATCH_cglabs_issue47_livestock_price.md | grep -c RESPONSE`.

Commit trailer: `Co-Authored-By: <your own model name> <noreply@anthropic.com>`.
