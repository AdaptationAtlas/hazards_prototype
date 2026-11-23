# DISPATCH — pascal — #58 (#29 follow-ups) phase A: R environment audit + MapSPAM re-fetch

**Append-only; newest block on top. Prepend `### RESPONSE — pascal` to answer.**

### RESPONSE — pascal [M-20261121-i58-1] 2026-11-23 — P17: four of five 0.4.x stages are READY; 0.4.4 waits only on upstream stage outputs; all eight FAOSTAT files are the macbook's

**Complete, no stop rule fired.** Read-only throughout: no 0.4.x script was run, nothing was installed,
`R/0_server_setup.R` was not sourced, and the only files written are the two logs named below. The box is idle
(`load average: 0.24, 0.09, 0.02`, up 27 days), so the DUP2 compression job is not running in this hub right now;
it was left alone either way.

#### G0

At `0c77440` on `develop`, the P17 commit.

#### G1 — readiness per stage

| stage | script | verdict |
|---|---|---|
| 0.4.0 | `R/0.4.0_create_crop_vop_intld15.R` | **READY** — 4 rows PASS (6 `prod_t` tifs, 2 FAOSTAT CSVs, 3 boundary parquets) |
| 0.4.2 | `R/0.4.2_create_crop_vop_nominal_usd.R` | **READY** — 9 rows PASS (6 `prod_t` tifs, 7 FAOSTAT CSVs, 3 boundary parquets) |
| 0.4.3 | `R/0.4.3_fao_production_cv.R` | **READY** — 2 rows PASS |
| 0.4.4 | `R/0.4.4_process_exposure.R` | **NOT READY** — 5 rows PASS, 2 rows MISSDIR (both `from_stage`) |
| 0.4.5 | `R/0.4.5_create_faostat_long.R` | **READY** — 4 rows PASS |

So the corrected `stages.json` resolves cleanly on this host: every declared input of 0.4.0, 0.4.2, 0.4.3 and 0.4.5
is present, including the FAOSTAT files that were missing from the old declaration. Nothing is SHORT and no row
carries a WARN anywhere in the five stages.

#### G1 — the three `from_stage` rows in 0.4.4

Two of the three are missing, one is present. The block expected up to three MISSDIR rows; the 0.4.1 row passes.

| from_stage row | stage that makes it | state |
|---|---|---|
| `$W/Data/mapspam/2020V1r2_SSA/processed/variable=vop_intld15-2021/*.tif` | 0.4.0 | **MISSDIR** |
| `$W/Data/mapspam/2020V1r2_SSA/processed/variable=vop_nominal-usd-2021/*.tif` | 0.4.2 | **MISSDIR** |
| `$W/Data/GLW4_2020/processed/livestock_number_number_*.tif` | 0.4.1 | **PASS, n=2** |

**P13's 0.4.1 outputs are in `$W`, not only under a phase-B root.** Both files sit in
`$W/Data/GLW4_2020/processed`, dated 2026-11-19, which is the P13 run:

```
26482037  2026-11-19 18:37  livestock_number_number_res-05.tif
 1606247  2026-11-19 18:34  livestock_number_number_res-25.tif
```

That answers the block's question directly: 0.4.4's livestock input is already satisfied in the working directory,
and only the two VoP directories are outstanding. Run 0.4.0 and 0.4.2 and 0.4.4 becomes READY without anything else
being fetched.

#### G1 — a finding worth the plan: legacy VoP directories under the same parent

`$W/Data/mapspam/2020V1r2_SSA/processed` holds four VoP directories under the **superseded** naming, each with 6
tifs, all dated 2026-11-04:

```
variable=vop_intld15/          6 tifs   e.g. spam_vop_intld15_all.tif
variable=vop_nominal-usd15/    6 tifs
variable=vop_nominal-usd21/    6 tifs
variable=vop_usd2015/          6 tifs
```

These are not stage outputs of the current scripts and must not be read as "0.4.0 has already run here". The header
of `R/0.4.0_create_crop_vop_intld15.R` says so at line 8 — it supersedes the S3-legacy `spam_vop_intld15_all.tif` —
and the current script writes to `variable=vop_intld15-2021` with filenames carrying the exposure tag
(`spam_vop_intld15-2021_all_<tag>.tif`, lines 290-312). `71_stage_ready.R` is right to call MISSDIR. The point is
for whoever reads the folder by eye: the old directories look like the answer and are not. They are 24 tifs of dead
weight from the 2026-11-04 state and are a candidate for the cleanup list, once someone confirms nothing still reads
them.

Note also that `$W/Data/GLW4_2020/processed` has its own `variable=vop_*` subdirectories, including
`variable=vop_intld15-2021`. That is a different parent from the one 0.4.4 reads, and it does not satisfy the
mapspam row. Same name, different tree.

#### G2 — FAOSTAT: all eight files are byte-for-byte the macbook's

| file | result |
|---|---|
| `Value_of_Production_E_Africa.csv` | same |
| `Value_of_Production_E_All_Area_Groups.csv` | same |
| `Prices_E_Africa_NOFLAG.csv` | same |
| `Prices_E_All_Data_(Normalized).csv` | same |
| `Production_Crops_Livestock_E_Africa_NOFLAG.csv` | same |
| `Production_Crops_Livestock_E_All_Area_Groups.csv` | same |
| `Deflators_E_All_Data_(Normalized).csv` | same |
| `Trade_CropsLivestock_E_Africa_NOFLAG.csv` | same |

Eight of eight `same`: every md5 and every byte count matches the macbook table exactly, and all eight files carry
the date 2026-11-12, the macbook's fetch date. Nothing differs, so there is no FAOSTAT release split between the
two hosts today, and nothing was re-fetched. P13 found the same for the three files 0.4.1 reads; this extends the
result to the five that 0.4.2 and 0.4.5 add.

The caveat stands rather than closes: FAOSTAT rewrites in place, so this is a statement about 2026-11-23, not a
pin. The migration plan's FAOSTAT-snapshot item is what makes it durable.

#### Readiness summary for migration plan item 10

| | |
|---|---|
| Stages ready to run now, in any order | 0.4.0, 0.4.2, 0.4.3, 0.4.5 |
| Stage blocked, and only on sibling outputs | 0.4.4 — needs 0.4.0 and 0.4.2 first |
| Inputs absent from this host | **none** |
| FAOSTAT agreement with the macbook | 8 of 8 `same` |

No external input is missing on PASCAL for any of the five stages. The one NOT READY is an ordering fact, not a
data gap, and the order is 0.4.0 and 0.4.2, then 0.4.4; 0.4.3 and 0.4.5 are independent of all of it. The
`mass_check()` work in `d026903` is untested here, as the block says — the first real run of 0.4.0 and 0.4.4 is
that test, and it is not part of this block.

Logs on the node: `logs/p17_stage_ready_20261123_170323.log`, `logs/p17_fao_md5_20261123_170323.log`. Nothing was
written outside `logs/`.

Answers: M-20261121-i58-1 (complete)

---

### MACBOOK 2026-11-21 [M-20261121-i58-1] — P17: can PASCAL run the rest of the 0.4 chain? Readiness of 0.4.0, 0.4.2, 0.4.3, 0.4.4, 0.4.5 (read-only). **Small profile. Minutes.**

**Why.** Migration plan item 10: 0.4.0, 0.4.2, 0.4.3, 0.4.4 and 0.4.5 are "regenerate" with no known blocker, and
0.4.1 already passed phase B here (P13). Before running them, check that every file they read is on this host.
`metadata/stages.json` was incomplete for these stages: 0.4.3 and 0.4.5 were not declared at all, and 0.4.0/0.4.2
missed most of their FAOSTAT inputs. The macbook corrected it from a read of all five scripts (commit with this
block). Also new since P13: every sum-resample in 0.4.0, 0.4.4 and `haz_functions.R` now goes through `mass_check()`
(`d026903`). The first real run of those is the test, and it comes after this block.

**Rules.** Read-only: do **not** run any 0.4.x script, no installs, do not source `R/0_server_setup.R`. Writes:
`logs/` only. The DUP2 compression job from the dedup session may be running in another hub session; leave it alone,
and say if `uptime` shows the box busy. In anything you paste, write the working directory as `$W`. No usernames,
hostnames or IPs.

#### G0 — pull
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD
git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD      # develop, at or after the P17 commit
uptime
```

#### G1 — readiness, per stage (about a second each)
```bash
S=$(date +%Y%m%d_%H%M%S)
for st in 0.4.0 0.4.2 0.4.3 0.4.4 0.4.5; do
  Rscript R/checks/71_stage_ready.R --stage "$st" 2>&1 | tee -a logs/p17_stage_ready_$S.log | grep -E 'stage |PASS|FAIL|MISS|SHORT|WARN|READY'
done
```
**Expect** 0.4.0, 0.4.2, 0.4.3 and 0.4.5 `READY`. For 0.4.4, three rows carry `"from_stage"` in `stages.json`: they are
outputs of 0.4.0, 0.4.1 and 0.4.2, so `MISSDIR` there is expected if those stages have not run in this working
directory, and is a finding, not a stop. Say which of the three are present and, if present, their file dates.
In particular: are P13's 0.4.1 outputs (`livestock_number_number_*.tif`) in `$W`'s `GLW4_2020/processed`, or only
under a phase-B root elsewhere?

#### G2 — are the FAOSTAT inputs the macbook's?
FAOSTAT rewrites files in place, so readiness is not enough. Phase B compared the files 0.4.1 reads; 0.4.2 and 0.4.5
add three more. Hash all eight against the macbook's copies (fetched 2026-11-12):
```bash
F=$(Rscript -e 'source("R/00_paths.R"); cat(atlas_dir("fao", absolute = TRUE))')
cd "$F" && md5sum Value_of_Production_E_Africa.csv Value_of_Production_E_All_Area_Groups.csv \
  Prices_E_Africa_NOFLAG.csv 'Prices_E_All_Data_(Normalized).csv' Production_Crops_Livestock_E_Africa_NOFLAG.csv \
  Production_Crops_Livestock_E_All_Area_Groups.csv 'Deflators_E_All_Data_(Normalized).csv' \
  Trade_CropsLivestock_E_Africa_NOFLAG.csv 2>&1 | tee -a "$OLDPWD/logs/p17_fao_md5_$S.log"
ls -l --time-style=+%F Value_of_Production_E_Africa.csv Production_Crops_Livestock_E_All_Area_Groups.csv \
  'Deflators_E_All_Data_(Normalized).csv' Trade_CropsLivestock_E_Africa_NOFLAG.csv | awk '{print $5, $6, $7}'
cd "$OLDPWD"
```
The macbook's values:

| file | md5 | bytes |
|---|---|---:|
| `Value_of_Production_E_Africa.csv` | `b918d7229ac029651fdaa46955d07b45` | 14,709,503 |
| `Value_of_Production_E_All_Area_Groups.csv` | `e56b44725ac151743e0028c0ca0c8926` | 16,538,364 |
| `Prices_E_Africa_NOFLAG.csv` | `ee7e63a7e47c5922448f5ed532034d0e` | 5,868,302 |
| `Prices_E_All_Data_(Normalized).csv` | `89bf3751160932244e1efedc93e7d115` | 214,286,652 |
| `Production_Crops_Livestock_E_Africa_NOFLAG.csv` | `65791f090d59f2538812c0b20027273d` | 12,958,809 |
| `Production_Crops_Livestock_E_All_Area_Groups.csv` | `3c6bc9b7e150924d567471853981363d` | 23,369,120 |
| `Deflators_E_All_Data_(Normalized).csv` | `affa9521868914d8481a1f38c7f395d2` | 14,573,341 |
| `Trade_CropsLivestock_E_Africa_NOFLAG.csv` | `0c8612c9a953dd1e1dbaeb01f3369d83` | 42,369,764 |

Report one line per file: `same` or `DIFFERS` (with this host's md5 and size), or `absent`. A difference is a finding:
it means the two hosts hold different FAOSTAT releases, which is the FAOSTAT-snapshot item in the migration plan, not
something to fix here. Do not re-fetch.

**Stop and describe** on any of: not on `develop`; `Installing package`; any file written outside `logs/`.

**To answer:** prepend one `### RESPONSE — pascal [M-20261121-i58-1]` (P17), run the IP grep, commit this file only.
`git pull --rebase` before pushing; keep any other blocks as they are, yours on top. Push, verify it landed, and end
with `Answers: M-20261121-i58-1 (complete)` — or `(stopped at <gate>)`.

### RESPONSE — pascal [M-20261119-i58-4] 2026-11-19 — P15: server-wide storage and duplicate census. **Complete, no stop rule fired.** The listing has 1,096,294 files and 24.57 TB from 9 owners. **Probable duplicates total 711 GB, 2.9 % of listed bytes, but most of that is false matches.** About 245 GB (1.0 %) still looks like real copies after removing the groups shown to be, or almost certainly, different files. **The shared NEX-GDDP raw tree is almost empty for our purposes**: 206 files, 2021-2022 only, `tasmax`/`tasmin` only, no `historical`. None of P10's six raw files are in it.

| | |
|---|---|
| Total listed under `$B` | 1,096,294 files, 24.57 TB, 9 owners (of about 69 accounts) |
| `df` for `$B` (same filesystem as `$W`) | 50 T size, 26 T used, 24 T available, 53 % |
| Probable duplicates, as listed | trees 151.45 GB + files 559.64 GB = **711.09 GB, 2.89 %** of listed bytes |
| After known false matches are removed | **about 244.5 GB, 1.0 %** (see "False matches" below) |
| Largest single group | tree `$B/common/llama-main/llama-2-70b` vs `…-70b-chat`, 137.97 GB. **Almost certainly a false match.** |
| Largest group that is plausibly a real copy | `ls_vop_00.tif`, 4 copies of 3.73 GB, 11.20 GB recoverable |
| Permission denied | **2**, both top-level folders under `$B`. Neither is inside `$W` or `$CL`. |
| Shared NEX raw coverage | **54 of 90** GCM × scenario cells hold any file. **0 of 90 are complete.** 206 of the 36,720 files the full matrix needs (0.56 %). |
| P10 raw files already in the shared tree | **0 of 6**. P10's CHIRPS extent file **is** in `$CL`, byte-identical. |

**P14.** Stopped as instructed, with no RESPONSE written for it. Its D2 and D3 had finished, and E3 reuses them. Its D1 (`du`) and
D4 (large-file `find` + `md5sum`) ran for about 2.5 h over `$W` on the network store. Neither had finished or written any output when I
stopped them, so they contribute nothing. For about 2 h they shared I/O with E1, which probably made E1's wall time longer.

#### E0
On `develop`. `logs/p15_*` is ignored (`git check-ignore` printed). `$B` and `$W` are one filesystem.
`$B` = the common parent of `$W` and `$CL`; `$CD` = `$W/hazards_upstream_pascal`.

#### E1 — the listing
Run in the migration session's checkout with `S=20261119_192126` (confirmed from the `p15_list_*.log` name). I did not start a second `find`.

- `find exit=1`, as expected with unreadable folders. Wrapper `Exit status: 0`.
- **Wall 4:06:32**, max RSS 19 MB, at `ionice -c3 nice -n 19`.
- **1,096,294 lines**, all with 4 fields. Unparsed lines 0.
- **2 `Permission denied`** lines and nothing else in the `.err`. Both are top-level folders under `$B`, outside `$W` and `$CL`. They are not named here because one name looks like a username.
- Listed bytes (24.57 TB) are about 1.4 TB below `df`'s 26 T used. The listing cannot account for the gap. The two unreadable folders are one candidate, and filesystem overhead may be another; neither is measured.

#### E2 — the census
`storage_census.py` took 2.6 s at 0.24 GB RSS. **Privacy check: 9 names, count 0.** A further grep for the store's mount
path and home paths also returned 0. The public summary follows as written, with **one change**: one top-level folder
is named after a person (a personal name, not one of the 9 usernames). I replaced that name and its sub-folder names with
`personal-folder-A` / `sub-N`.

#### Summary

| | |
|---|---:|
| Files listed | 1,096,294 |
| Bytes listed | 24.57 TB |
| Unparsed lines | 0 |
| Owners | 9 |
| Probable duplicate trees (>= 1 GB, maximal) | 2 groups, 151.45 GB recoverable |
| Probable duplicate large files outside those trees (>= 100 MB) | 438 groups, 559.64 GB recoverable |

#### Usage by owner (top 40)

| Owner | Files | Size | Share |
|---|---:|---:|---:|
| owner-01 | 705,468 | 18.34 TB | 74.6 % |
| owner-02 | 144,744 | 2.40 TB | 9.8 % |
| self | 33,750 | 2.24 TB | 9.1 % |
| owner-03 | 106,047 | 882.43 GB | 3.6 % |
| owner-04 | 8,425 | 687.40 GB | 2.8 % |
| owner-05 | 95,600 | 15.05 GB | 0.1 % |
| owner-06 | 2,253 | 2.80 GB | 0.0 % |
| owner-07 | 5 | 1.08 GB | 0.0 % |
| owner-08 | 2 | 6954 B | 0.0 % |

#### Usage by location, two levels (top 40)

| Location | Size |
|---|---:|
| $B/common/climate | 16.01 TB |
| $B/atlas/hazards_prototype | 4.64 TB |
| $B/CCI/Data | 1.14 TB |
| $B/WS18_Afrca_K_N_ACO/WS18_Afrca_K_N_ACO | 684.77 GB |
| $B/common/fao | 612.76 GB |
| $B/AICCRA/Data | 406.28 GB |
| $B/common/llama-main | 354.97 GB |
| $B/ONECGIAR/Atlas_MVP | 185.65 GB |
| $B/ONECGIAR/Data | 167.37 GB |
| $B/common/vector | 133.39 GB |
| $B/ONECGIAR/ClimBer | 54.83 GB |
| $B/common/google | 49.80 GB |
| $B/AICCRA/CSAIPs | 37.22 GB |
| $B/ONECGIAR/LACResiliente | 21.85 GB |
| $B/common/soil | 16.24 GB |
| $B/ANGOLARISKFINANCING/Data | 15.05 GB |
| $B/common/mapspam | 9.41 GB |
| $B/ONECGIAR/ANIMALS | 9.18 GB |
| $B/common/population | 6.25 GB |
| $B/atlas/hazards_upstream_pascal | 5.21 GB |
| $B/common/livestock | 3.78 GB |
| $B/common/water | 3.61 GB |
| $B/FREEZER/repositories.old.1516745355 | 2.62 GB |
| $B/era/geodata | 1.75 GB |
| $B/personal-folder-A/sub-1 | 1.32 GB |
| $B/personal-folder-A | 1.20 GB |
| $B/common/ihme | 1.08 GB |
| $B/ACO/1.Data | 1.01 GB |
| $B/atlas/atlas_obs_hazards_pascal | 206.00 MB |
| $B/personal-folder-A/sub-2 | 158.46 MB |
| $B/atlas/atlas_obs_hazards_smoke_pascal | 105.52 MB |
| $B/ACO | 63.07 MB |
| $B/common/wfp | 47.17 MB |
| $B/common | 41.92 MB |
| $B/common/conflict | 40.04 MB |
| $B/personal-folder-A/sub-3 | 35.89 MB |
| $B/personal-folder-A/sub-4 | 32.25 MB |
| $B/ONECGIAR/GCA-digital-adaptation | 29.68 MB |
| $B/personal-folder-A/sub-5 | 25.25 MB |
| $B/common/access | 22.84 MB |

#### Probable duplicate trees, largest first (top 40)

| # | Recoverable | Copies | Each | Files each | Copies at (owner) |
|---:|---:|---:|---:|---:|---|
| 1 | 137.97 GB | 2 | 137.97 GB | 10 | `$B/common/llama-main/llama-2-70b` (owner-01)<br>`$B/common/llama-main/llama-2-70b-chat` (owner-01) |
| 2 | 13.48 GB | 2 | 13.48 GB | 3 | `$B/common/llama-main/llama-2-7b` (owner-01)<br>`$B/common/llama-main/llama-2-7b-chat` (owner-01) |

#### Probable duplicate large files outside those trees (top 40)

| # | Recoverable | Copies | Size | Name | Copies at (owner) |
|---:|---:|---:|---:|---|---|
| 1 | 11.20 GB | 4 | 3.73 GB | `ls_vop_00.tif` | `$B/AICCRA/CSAIPs/data/livestock` (owner-03)<br>`$B/ONECGIAR/ClimBer/Data` (owner-03)<br>`$B/ONECGIAR/Data/livestock` (owner-03)<br>`$B/common/livestock` (owner-01) |
| 2 | 3.96 GB | 2 | 3.96 GB | `rotated_pr_day_EC-Earth3-Veg_ssp585_r1i1p1f1-2041-01-01_2060-12-31.tif` | `$B/AICCRA/Data/climate/interim/rotated/CMIP6/daily` (owner-03)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK/interim` (owner-01) |
| 3 | 3.96 GB | 2 | 3.96 GB | `rotated_pr_day_EC-Earth3-Veg_ssp585_r1i1p1f1-2021-01-01_2040-12-31.tif` | `$B/AICCRA/Data/climate/interim/rotated/CMIP6/daily` (owner-03)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK/interim` (owner-01) |
| 4 | 3.95 GB | 2 | 3.95 GB | `rotated_pr_day_EC-Earth3-Veg_historical_r1i1p1f1-1995-01-01_2014-12-31.tif` | `$B/AICCRA/Data/climate/interim/rotated/CMIP6/daily` (owner-03)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK/interim` (owner-01) |
| 5 | 3.58 GB | 2 | 3.58 GB | `rotated_tas_day_EC-Earth3-Veg_ssp585_r1i1p1f1-2041-01-01_2060-12-31.tif` | `$B/AICCRA/Data/climate/interim/rotated/CMIP6/daily` (owner-03)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK/interim` (owner-01) |
| 6 | 3.58 GB | 2 | 3.58 GB | `rotated_tas_day_EC-Earth3-Veg_historical_r1i1p1f1-1995-01-01_2014-12-31.tif` | `$B/AICCRA/Data/climate/interim/rotated/CMIP6/daily` (owner-03)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK/interim` (owner-01) |
| 7 | 3.57 GB | 2 | 3.57 GB | `rotated_tas_day_EC-Earth3-Veg_ssp585_r1i1p1f1-2021-01-01_2040-12-31.tif` | `$B/AICCRA/Data/climate/interim/rotated/CMIP6/daily` (owner-03)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK/interim` (owner-01) |
| 8 | 3.55 GB | 2 | 3.55 GB | `rotated_tasmin_day_EC-Earth3-Veg_historical_r1i1p1f1-1995-01-01_2014-12-31.tif` | `$B/AICCRA/Data/climate/interim/rotated/CMIP6/daily` (owner-03)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK/interim` (owner-01) |
| 9 | 3.55 GB | 2 | 3.55 GB | `rotated_tasmin_day_EC-Earth3-Veg_ssp585_r1i1p1f1-2041-01-01_2060-12-31.tif` | `$B/AICCRA/Data/climate/interim/rotated/CMIP6/daily` (owner-03)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK/interim` (owner-01) |
| 10 | 3.55 GB | 2 | 3.55 GB | `rotated_tasmin_day_EC-Earth3-Veg_ssp585_r1i1p1f1-2021-01-01_2040-12-31.tif` | `$B/AICCRA/Data/climate/interim/rotated/CMIP6/daily` (owner-03)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK/interim` (owner-01) |
| 11 | 3.54 GB | 2 | 3.54 GB | `rotated_tasmax_day_EC-Earth3-Veg_ssp585_r1i1p1f1-2041-01-01_2060-12-31.tif` | `$B/AICCRA/Data/climate/interim/rotated/CMIP6/daily` (owner-03)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK/interim` (owner-01) |
| 12 | 3.54 GB | 2 | 3.54 GB | `rotated_tasmax_day_EC-Earth3-Veg_historical_r1i1p1f1-1995-01-01_2014-12-31.tif` | `$B/AICCRA/Data/climate/interim/rotated/CMIP6/daily` (owner-03)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK/interim` (owner-01) |
| 13 | 3.54 GB | 2 | 3.54 GB | `rotated_tasmax_day_EC-Earth3-Veg_ssp585_r1i1p1f1-2021-01-01_2040-12-31.tif` | `$B/AICCRA/Data/climate/interim/rotated/CMIP6/daily` (owner-03)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK/interim` (owner-01) |
| 14 | 3.19 GB | 2 | 3.19 GB | `PAK_rotated_tas_day_EC-Earth3-Veg_historical_r1i1p1f1-1995-01-01_2014-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 15 | 3.15 GB | 2 | 3.15 GB | `pr_day_MRI-ESM2-0_ssp585_r1i1p1f1_gn_22510101-23001231.nc` | `$B/common/climate/cmip6/daily` (owner-01)<br>`$B/common/climate/cmip6/mme_cmip/precip` (owner-01) |
| 16 | 3.15 GB | 2 | 3.15 GB | `pr_day_MRI-ESM2-0_ssp585_r1i1p1f1_gn_22010101-22501231.nc` | `$B/common/climate/cmip6/daily` (owner-01)<br>`$B/common/climate/cmip6/mme_cmip/precip` (owner-01) |
| 17 | 3.14 GB | 2 | 3.14 GB | `pr_day_MRI-ESM2-0_ssp585_r1i1p1f1_gn_20150101-20641231.nc` | `$B/common/climate/cmip6/daily` (owner-01)<br>`$B/common/climate/cmip6/mme_cmip/precip` (owner-01) |
| 18 | 3.07 GB | 2 | 3.07 GB | `PAK_rotated_tasmin_day_EC-Earth3-Veg_historical_r1i1p1f1-1995-01-01_2014-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 19 | 3.07 GB | 2 | 3.07 GB | `PAK_rotated_tasmin_day_EC-Earth3-Veg_ssp585_r1i1p1f1-2021-01-01_2040-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 20 | 3.07 GB | 2 | 3.07 GB | `PAK_rotated_tasmin_day_EC-Earth3-Veg_ssp585_r1i1p1f1-2041-01-01_2060-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 21 | 3.06 GB | 2 | 3.06 GB | `PAK_rotated_tasmax_day_EC-Earth3-Veg_historical_r1i1p1f1-1995-01-01_2014-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 22 | 3.02 GB | 2 | 3.02 GB | `PAK_rotated_pr_day_MRI-ESM2-0_ssp585_r1i1p1f1-2041-01-01_2060-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 23 | 2.92 GB | 2 | 2.92 GB | `PAK_rotated_tasmax_day_MRI-ESM2-0_ssp585_r1i1p1f1-2021-01-01_2040-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 24 | 2.92 GB | 2 | 2.92 GB | `PAK_rotated_tasmax_day_MRI-ESM2-0_ssp585_r1i1p1f1-2041-01-01_2060-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 25 | 2.92 GB | 2 | 2.92 GB | `PAK_rotated_tasmin_day_MRI-ESM2-0_ssp585_r1i1p1f1-2041-01-01_2060-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 26 | 2.92 GB | 2 | 2.92 GB | `PAK_rotated_tas_day_MRI-ESM2-0_ssp585_r1i1p1f1-2041-01-01_2060-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 27 | 2.86 GB | 2 | 2.86 GB | `PAK_rotated_pr_day_INM-CM5-0_ssp585_r1i1p1f1-2041-01-01_2060-12-30.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 28 | 2.84 GB | 2 | 2.84 GB | `PAK_rotated_pr_day_EC-Earth3-Veg_historical_r1i1p1f1-1995-01-01_2014-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 29 | 2.82 GB | 2 | 2.82 GB | `PAK_rotated_tasmax_day_INM-CM5-0_ssp585_r1i1p1f1-2041-01-01_2060-12-30.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 30 | 2.82 GB | 2 | 2.82 GB | `PAK_rotated_tasmax_day_INM-CM5-0_historical_r1i1p1f1-1995-01-01_2014-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 31 | 2.81 GB | 2 | 2.81 GB | `PAK_rotated_tasmin_day_INM-CM5-0_ssp585_r1i1p1f1-2021-01-01_2040-12-30.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 32 | 2.81 GB | 2 | 2.81 GB | `PAK_rotated_tasmin_day_INM-CM5-0_historical_r1i1p1f1-1995-01-01_2014-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 33 | 2.81 GB | 2 | 2.81 GB | `PAK_rotated_tasmin_day_INM-CM5-0_ssp585_r1i1p1f1-2041-01-01_2060-12-30.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 34 | 2.81 GB | 2 | 2.81 GB | `PAK_rotated_tas_day_INM-CM5-0_ssp585_r1i1p1f1-2041-01-01_2060-12-30.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 35 | 2.75 GB | 2 | 2.75 GB | `PAK_rotated_tasmax_day_ACCESS-ESM1-5_ssp585_r1i1p1f1-2041-01-01_2060-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 36 | 2.74 GB | 2 | 2.74 GB | `PAK_rotated_tas_day_ACCESS-ESM1-5_ssp585_r1i1p1f1-2041-01-01_2060-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 37 | 2.74 GB | 2 | 2.74 GB | `PAK_rotated_tasmin_day_ACCESS-ESM1-5_ssp585_r1i1p1f1-2041-01-01_2060-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 38 | 2.74 GB | 2 | 2.74 GB | `PAK_rotated_tasmin_day_ACCESS-ESM1-5_historical_r1i1p1f1-1995-01-01_2014-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 39 | 2.68 GB | 2 | 2.68 GB | `PAK_rotated_tasmin_day_MPI-ESM1-2-HR_ssp585_r1i1p1f1-2041-01-01_2060-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |
| 40 | 2.67 GB | 2 | 2.67 GB | `PAK_rotated_tas_day_MPI-ESM1-2-HR_ssp585_r1i1p1f1-2041-01-01_2060-12-31.fst` | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` (owner-01)<br>`$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK` (owner-01) |

#### E2 — reading the groups

**Where the 438 duplicate-file groups are**, by location (one line per set of locations; GB recoverable):

| Recoverable | Groups | Locations |
|---:|---:|---|
| 314.98 GB | 298 | `$W/hazards_prototype/Data/hazard_risk/annual` vs `…/hazard_risk/jagermeyr` (self) |
| 127.65 GB | 60 | `$B/CCI/Data/WFP_ClimateRiskPr/1.Data/Climate/CMIP6/downscaled/PAK` vs `…/input/CMIP6/downscaled/PAK` (one owner) |
| 65.72 GB | 29 | `$B/AICCRA/Data/climate/interim/rotated/CMIP6/daily` vs `$B/CCI/Data/WFP_ClimateRiskPr/input/CMIP6/downscaled/PAK/interim` (two owners) |
| 14.24 GB | 6 | `$CL/cmip6/daily` vs `$CL/cmip6/mme_cmip/precip` |
| 10.77 GB | 19 | `$CL/worldclim` vs `$CL/worldclim/wc2.1_30s` |
| 11.20 GB | 1 | `ls_vop_00.tif` in `$B/AICCRA/CSAIPs`, `$B/ONECGIAR/ClimBer`, `$B/ONECGIAR/Data`, `$B/common/livestock` |
| 6.22 GB | 5 | `$CL/cmip6/daily` vs `$CL/cmip6/raw/daily` |
| 4.61 GB | 6 | `$CL/cmip6/daily` vs `$CL/cmip6/mme_cmip/temp/temp` |
| about 4.3 GB | 14 | the rest, mostly between `$B/WS18_…`, `$B/ACO`, `$B/ONECGIAR/*`, each under 1.1 GB |

**The three largest groups:**
1. **llama-2-70b vs llama-2-70b-chat (137.97 GB), and llama-2-7b vs -7b-chat (13.48 GB): almost certainly false matches.**
   A base model and its chat fine-tune have the same tensor shapes, so their checkpoint shards match in name and size but
   hold different weights. These are another owner's files and were not hashed, so this is a reasoned judgement, not a measurement.
   Both tree groups (151.45 GB) should be treated as not duplicates.
2. **`$W/…/hazard_risk/annual` vs `…/jagermeyr` (298 file groups, 314.98 GB, our own): false matches, sampled.** Both folders
   hold 2,112 files and about 985 GB. Their totals differ by only 35 KB because the rasters are uncompressed and all 1663 × 1739,
   so a file's size depends almost only on its band count, not on its values. These are our own files, so I hashed three pairs
   (the largest, the smallest, and the 100th by name). **All three are DIFFERENT.** The jagermeyr tree is the same risk product
   built on a different crop calendar, not a copy.
3. **CCI `1.Data/…/PAK` vs `input/…/PAK` (60 groups, 127.65 GB, one owner): looks like a real scratch copy.** It holds `.fst` files
   with long, specific names (`PAK_rotated_<var>_day_<GCM>_<scen>_…_<dates>.fst`), in two sibling folders of one project.
   Of all the candidates, this one is the most worth the owner's own md5 check.

**Other groups that look like real copies:** the 29 `rotated_*_EC-Earth3-Veg_*.tif` files shared between an AICCRA and a CCI project
(65.72 GB). Also the `$CL/cmip6/daily` raw CMIP6 `.nc` files that reappear under `mme_cmip/precip`, `mme_cmip/temp/temp` and
`raw/daily` (25 GB together). These have long, specific CMIP6 names, which makes a coincidental match unlikely. The same holds for
`ls_vop_00.tif` in four project folders.

**False matches, or likely ones:** besides 1 and 2 above, one CCI CMIP5 group pairs `bcc_csm1_1_…_tmin` with `nimr_hadgem2_ao_…_tmin`
(0.17 GB). These are two different GCMs whose fixed-size ASCII grids share a file name. The `$CL/worldclim` vs `wc2.1_30s` groups
(10.77 GB) are probably real (one release copied to a sub-folder), but WorldClim tiles are also fixed-size, so hash these before acting.

**Adjusted estimate:** 711.09 GB listed − 151.45 (llama trees) − 314.98 (annual/jagermeyr) − 0.17 (CMIP5) = **about 244.5 GB,
1.0 % of listed bytes**. Nothing in `$W` is a confirmed duplicate.

#### E3 — the shared NEX-GDDP raw tree (`$CL/cmip6/nex-gddp-cmip6_raw`)

D2 was reused from `S=20261119_192126`, as instructed; it is byte-identical to the copy I had made for P14 before the switch. Names parse cleanly. Layout is
`<var>/<scenario>/<GCM>/<name>.nc`; 48 names carry `_v1.1`/`_v1.2`; there are no other file types and no duplicate keys.
**206 files, 50.58 GB.** Files / variables / GB per GCM × scenario, **all years 2021-2022**:

| GCM | ssp126 | ssp245 | ssp585 |
|---|---|---|---|
| ACCESS-CM2 | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB |
| ACCESS-ESM1-5 | 2 f / 1 v · 0.5 GB | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB |
| CanESM5 | 2 f / 1 v · 0.5 GB | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB |
| CMCC-ESM2 | 2 f / 1 v · 0.5 GB | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB |
| EC-Earth3 | 2 f / 1 v · 0.5 GB | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB |
| EC-Earth3-Veg-LR | 2 f / 1 v · 0.5 GB | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB |
| GFDL-ESM4 | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB |
| INM-CM4-8 | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB |
| INM-CM5-0 | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB |
| IPSL-CM6A-LR | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB |
| KACE-1-0-G | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB |
| MIROC6 | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB |
| MPI-ESM1-2-HR | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB |
| MPI-ESM1-2-LR | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB |
| MRI-ESM2-0 | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB |
| NorESM2-LM | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB |
| NorESM2-MM | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB |
| TaiESM1 | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB | 4 f / 2 v · 1.0 GB |

The variables are `tasmax` and `tasmin`. There are 2 `hurs` files, both ACCESS-CM2 ssp126; that cell has `hurs` + `tasmin` and no `tasmax`.
The six ssp126 cells with 1 variable have `tasmin` only. **Absent:** `historical`, `ssp370`, and the variables `pr`, `rsds` and `sfcWind`.
So the tree covers 18 GCMs × 3 of the 5 scenarios, at most 2 of 6 variables and 2 of 80 years. **A regeneration would still download
almost all of its inputs.** This tree does not shrink the 9.6 TB estimate in any useful way.

**D3 — P10's six raw files vs the shared tree:**

| File | Bytes in `$CD` | Shared tree |
|---|---:|---|
| `hurs_day_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1995_v1.1.nc` | 277,190,299 | absent; 0 matches under any suffix |
| `pr_day_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1995_v1.1.nc` | 222,298,070 | absent; 0 matches under any suffix |
| `rsds_day_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1995.nc` | 270,314,540 | absent; 0 matches under any suffix |
| `sfcWind_day_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1995.nc` | 301,834,906 | absent; 0 matches under any suffix |
| `tasmax_day_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1995.nc` | 244,583,799 | absent; 0 matches under any suffix |
| `tasmin_day_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1995.nc` | 246,232,669 | absent; 0 matches under any suffix |

The shared tree holds **no** `historical` file at all, so no shared file was opened or hashed. **CHIRPS extent check:**
`$CD/chirps_wrld/chirps-v2.0.1981.01.01.tif` is **byte-identical** (`cmp`) to
`$CL/chirps/global_daily/tifs/p05/1981/chirps-v2.0.1981.01.01.tif` (57,616,658 bytes). `$CL` also has a `.tif.gz` of that day,
which differs as expected. A run could therefore point `chirps_raw` at `$CL/chirps/global_daily/tifs/p05` instead of keeping its own copy.

**Writes:** only under this checkout's `logs/`: `p15_private_$S/`, `p15_public_$S.md`, `p15_names_$S.txt`, `p15_d3_$S.txt`, plus P14's
`p14_*` files. I deleted, moved and chmod-ed nothing. The only file contents read were our own files (P10's six, the CHIRPS file, and 3 sampled
pairs in `$W`), plus the one shared CHIRPS file that the D3 check names. No shared NEX file was read, because none matched.

Answers: M-20261119-i58-4 (complete)

### RESPONSE — pascal [M-20261119-i58-5] — P16: PIK answers from PASCAL; GGCMI crop-calendar manifest built

**Complete.** All three gates ran, nothing stopped. PIK is reachable from this host, the index page lists
40 `.nc4` objects, and the manifest is built and committed. GGCMI is a regenerate with a manifest like every
other pinned input; it does not join FAOSTAT as a snapshot-to-S3 item.

#### G1 — reachability: PIK answers, and it is not flaky

Three tries at `https://www.pik-potsdam.de/~jonasjae/GGCMI_Phase3_crop_calendar/`, as printed:

```
try 1: http=200 dns=0.929807s connect=1.098397s total=1.659253s
try 2: http=200 dns=0.002803s connect=0.175798s total=0.574039s
try 3: http=200 dns=0.002713s connect=0.178450s total=0.585688s
```

Three clean 200s, no timeout and no retry. The first try carries a 0.93 s DNS lookup and the next two
resolve from cache in under 3 ms; connect time is steady at about 0.18 s after that. Total time for a
full page fetch is well under a second. The index page carries **40** `href="….nc4"` links.

This is the opposite of the macbook's 2026-11-02 result, and the difference is the network, not the
server: the macbook timed out on the same URL on the same path. Nothing here suggests the origin was
ever down. So PASCAL is the host to fetch GGCMI from, and the macbook should not be used to judge PIK
reachability again.

#### G2 — `--check`: the origin is listable, the manifest was simply absent

```
ggcmi-crop-calendars        40 obj        3,153,182 bytes  verify=size  NEW

FAIL - 1 record(s) missing a manifest or drifted
```

Exit 1, which is the expected branch the block names: the origin answered, listed 40 objects, and the
record has no manifest to compare them with. No `SKIP  origin not listable from here` line. The run is in
`logs/p16_ggcmi_check_20261119_212502.log`.

#### G3 — `--build`: 40 objects, 3,153,182 bytes, verify=size

| field | value |
|---|---|
| objects | 40 |
| total bytes | 3,153,182 |
| `verify=` level | `size` |
| manifest rows | 41 (header + 40) |
| listed | 2026-11-19 |
| `mutable` | false |

The changelog line the build wrote into the record:

> Added checksum manifest: 40 object(s), 3,153,182 bytes, verify=size. Listed from the origin by
> `R/checks/75_catalogue_manifest.R` (headers only); `R/00_acquire.R` checks every download against it.

**Why `verify=size` and not md5 or etag.** The HEAD responses do carry an `ETag`, and it is recorded in the
manifest's `etag` column — first two rows:

```
src	dest	bytes	etag	md5	role
…/bar_ir_ggcmi_crop_calendar_phase3_v1.01.nc4	bar_ir_…nc4	70083	111c3-5d94e4c99df03		file
…/bar_rf_ggcmi_crop_calendar_phase3_v1.01.nc4	bar_rf_…nc4	78411	1324b-5d94e4c9a2d1c		file
```

Those etags are Apache's default `<size-hex>-<mtime-hex>` form — `111c3` is 70083 and `1324b` is 78411, the
byte counts in the same row — so they are a restatement of size and mtime, not a content hash. They would
change if the file were merely touched, and would not change if the contents were edited in place at the
same length. The script was right to settle on `verify=size`; treat the `etag` column as provenance, not as
an integrity check. No `Content-MD5` is served, so size is the strongest honest level available without
downloading the files, and the block forbids downloading them.

Practical consequence for migration plan item 4: a GGCMI regenerate is verifiable against object count and
per-file byte size, which catches a truncated or missing download, but not silent content drift at constant
length. If stronger assurance is wanted later, one full fetch on this host could be hashed once and the
`md5` column filled in; that is a download, so it is out of scope here and is offered as a separate item,
not done.

#### Files changed

`git status --short metadata/catalogue` after `--build` showed exactly the two files the block allows, and
nothing else:

```
 M metadata/catalogue/ggcmi-crop-calendars.json
?? metadata/catalogue/_manifests/ggcmi-crop-calendars.tsv
```

No data was downloaded, no package was installed, and `R/0_server_setup.R` was not sourced. Logs on the node:
`logs/p16_ggcmi_check_20261119_212502.log`, `logs/p16_ggcmi_build_20261119_212502.log`.

#### Finding, for the plan rather than for this gate

GGCMI now has a manifest, so the "no checksum manifest" exception in §4 can close for this dataset. The
reachability result is worth recording as a host fact rather than a one-off: PIK answers from PASCAL and did
not answer from the macbook, so acquisition recipes that pull from `pik-potsdam.de` should be run on the node.

Answers: M-20261119-i58-5 (complete)

---

### MACBOOK 2026-11-19 [M-20261119-i58-5] — P16: does PIK answer from PASCAL? GGCMI crop-calendar manifest (headers only). **Small profile. Minutes.**

**Why.** Migration plan item 4 and §4: the GGCMI Phase 3 crop calendars (`ggcmi-crop-calendars`, consumed by `R/1`)
have no checksum manifest, because the PIK index page timed out from the macbook on 2026-11-02. That proves nothing
about PASCAL, which sits on a different network and has never been asked. If PIK answers here, GGCMI becomes a
regenerate with a manifest like every other pinned input. If it does not, it joins FAOSTAT as a snapshot-to-S3 item,
and CGlabs' copy may be the only one we control. Your P15 hand-off is noted: the deduplication work now runs in its
own PASCAL session, and the shared NEX raw tree's 0.6 % coverage leaves migration plan §2.2a unchanged.

**Rules.** No data downloaded: `curl` reads headers, and `75_catalogue_manifest.R` lists the index page and sends
HEAD requests only. No installs; do not source `R/0_server_setup.R`. Writes: `logs/`, and only if G2 passes, the
manifest TSV and the record JSON that `--build` writes. No usernames, hostnames or IPs.

#### G0 — pull
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD
git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD      # develop, at or after the P16 commit
```

#### G1 — reachability (seconds; a timeout is a finding, not a stop)
```bash
U=https://www.pik-potsdam.de/~jonasjae/GGCMI_Phase3_crop_calendar
for i in 1 2 3; do curl -sS -o /dev/null -L --max-time 60 -w "try $i: http=%{http_code} dns=%{time_namelookup}s connect=%{time_connect}s total=%{time_total}s\n" "$U/" 2>&1; done
curl -sS -L --max-time 60 "$U/" 2>/dev/null | grep -o 'href="[^"]*\.nc4"' | wc -l      # links on the index page
```
**Expect** either `http=200` with 40 or more `.nc4` links, or a clear failure (timeout, refused, 4xx/5xx). Report all
three tries as printed. A mix (one try works, one times out) is worth saying plainly: it means flaky, not blocked.

#### G2 — the manifest, read-only first
```bash
S=$(date +%Y%m%d_%H%M%S)
Rscript R/checks/75_catalogue_manifest.R --check --dataset ggcmi-crop-calendars 2>&1 | tee logs/p16_ggcmi_check_$S.log | tail -15
```
`--check` lists the origin and compares it with the (absent) manifest. Two possible lines:
`ggcmi-crop-calendars  <n> obj  <bytes> bytes  verify=<level> NEW` then `FAIL - 1 record(s) missing a manifest`, exit
1: **expected**, the origin answered; or `SKIP  origin not listable from here: <reason>`: PIK did not answer; report
the reason. Either is a finding, not a stop.

#### G3 — build it (only if G2 printed `NEW` with 40 or more objects)
```bash
Rscript R/checks/75_catalogue_manifest.R --build --dataset ggcmi-crop-calendars 2>&1 | tee logs/p16_ggcmi_build_$S.log | tail -8
git status --short metadata/catalogue                                               # the TSV and the record JSON, nothing else
head -3 metadata/catalogue/_manifests/ggcmi-crop-calendars.tsv; wc -l < metadata/catalogue/_manifests/ggcmi-crop-calendars.tsv
```
Report the object count, total bytes and the `verify=` level the changelog line states (md5, etag or size).

**Stop and describe** on any of: not on `develop`; `Installing package`; G3 changing any file other than
`metadata/catalogue/_manifests/ggcmi-crop-calendars.tsv` and `metadata/catalogue/ggcmi-crop-calendars.json`.

**To answer:** prepend one `### RESPONSE — pascal [M-20261119-i58-5]` (P16), run the IP grep, commit this file plus,
if G3 ran, the two catalogue files and nothing else. `git pull --rebase` before pushing: the deduplication session
writes to this file too; keep its blocks as they are, yours on top. Push, verify it landed, and end with
`Answers: M-20261119-i58-5 (complete)` — or `(stopped at <gate>)`.

### MACBOOK 2026-11-19 [M-20261119-i58-4] — P15: server-wide storage and duplicate census (metadata only). Replaces P14 [M-20261119-i58-3]. **Small profile. A few hours, mostly one `find`.**

**Why.** P13 closed phase B on PASCAL. Its one VALUE-DIFF, `shoat_prop` at 3.95e-06, is accepted as you read it: rounding
order in a ratio whose two layers still sum to the cell count. Pete now wants duplicates found across the whole shared store, not only in our folders, so owners can be
contacted and the space recovered before anyone pays for more storage. P14 looked at our area only; P15 replaces it
(P14 gets no RESPONSE; if you started it, stop it and say how far it got). P14's two checks on the shared NEX-GDDP
raw tree are kept as E3.

How it works, and why this way:
- **Metadata only.** One `find` lists size, owner, folder and name for every file on the store. **No other user's
  file is opened or hashed.** Contents are compared later, only for groups worth acting on, and for other people's
  data by IT or the owner.
- `scripts/storage_census.py` (new; standard library only) turns the listing into **probable** duplicates: whole
  folder trees whose file names and sizes match all the way down (each duplicated tree counted once, at its outermost
  copy), and large files with the same name and size in two or more places outside those trees.
- **Usernames and real paths never leave the node.** The script writes two things: a private folder (usernames,
  real paths; for Pete, to contact owners) and a public markdown summary in which owners are `owner-NN`, our own
  account is `self`, the store root is `$B`, and any path part containing a username is replaced by its tag. **Only
  the public summary goes in the RESPONSE.** `logs/p15_*` is git-ignored as of this commit; never force-add it.

**Rules.**
- **Read-only.** No `rm`, `mv`, `cp`, `chmod`, and no contact with any other user. Writes only under `logs/`.
- **No file contents read outside `$W`**, except the shared NEX-GDDP copies that E3 compares with our six files.
- Run the listing at idle I/O priority (`ionice -c3 nice -n 19`), so other users do not feel it.
- `Permission denied` is a finding: count it, do not work around it.
- No usernames, hostnames or IPs anywhere in the RESPONSE; paths as `$B/…`, `$W/…`, `$CL/…`, `$CD/…`.

#### E0 — pull, roots
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD
export B=<the bulk store root: the common parent of writable_root and of the shared climate root in metadata/hosts.json>
export W=<writable_root in metadata/hosts.json, the atlas root>
export CL=<the shared climate root: the parent of the chirps / chirts_era5 raw overrides in hosts.json>
export CD=<the P10 root>
git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD      # develop, at or after the P15 commit
git check-ignore -q logs/p15_test && echo "logs/p15_* ignored"                    # must print; else stop
df -h $B $W | tail -2                                                              # one filesystem expected
ls $B | wc -l; python3 --version
```

#### E1 — list the store (background, idle I/O)
```bash
S=$(date +%Y%m%d_%H%M%S); export B S
cat > logs/p15_list_$S.sh <<'SH'
ionice -c3 nice -n 19 find "$B" -xdev -type f -printf '%s\t%u\t%h\t%f\n' > logs/p15_files_$S.tsv 2> logs/p15_list_$S.err
echo "find exit=$?"
SH
nohup /usr/bin/time -v bash logs/p15_list_$S.sh > logs/p15_list_$S.log 2>&1 &
echo $! > logs/p15_list_$S.pid
# afterwards:
grep -E "find exit|Elapsed" logs/p15_list_$S.log; wc -l < logs/p15_files_$S.tsv; grep -c 'Permission denied' logs/p15_list_$S.err
```
`find` exits 1 when some folders are unreadable; that is expected here, not a stop. Report wall time, file count and
the denied count.

#### E2 — the census
```bash
python3 -I scripts/storage_census.py --list logs/p15_files_$S.tsv --root "$B" --self "$(id -un)" \
  --private logs/p15_private_$S --public logs/p15_public_$S.md --min-tree-gb 1 --min-file-mb 100 --top 40
# privacy check: no username from the private owner table may appear in the public summary
cut -f1 logs/p15_private_$S/owner_usage.tsv | tail -n +2 | awk 'length($0) >= 3' > logs/p15_names_$S.txt
grep -c -F -f logs/p15_names_$S.txt logs/p15_public_$S.md                           # must be 0
```
**If that count is not 0, stop** and do not paste anything; say only the count. Otherwise paste the whole public
summary into the RESPONSE. Then add, in your own words: the three largest groups and what they look like (same
dataset in two places? a scratch copy? folders of same-sized files that only look alike?), and anything that
looks like a false match.

#### E3 — the shared NEX-GDDP raw tree, and our six files (P14's D2 and D3)
Run P14's D2 and D3 as written, with `$S` from E1 and `R=$CL/cmip6/nex-gddp-cmip6_raw`. Report the GCM × scenario
coverage table and the six-file comparison.

#### E4 — report
At the top: total listed, owners, probable duplicate bytes (trees + files) as a share of used space, the largest
single group, the denied count, `df` for `$B`, and the shared NEX raw coverage as a fraction of the 18 GCMs × 5
scenarios.

**Stop and describe** on any of: not on `develop`; `logs/p15_*` not ignored; a non-zero privacy count; any write
outside `logs/`; anything that would need reading another user's file contents, a deletion or a move.

**To answer:** prepend one `### RESPONSE — pascal [M-20261119-i58-4]` (P15), run the IP grep over the response,
commit only this file (never `logs/`), push, verify it landed, and end with
`Answers: M-20261119-i58-4 (complete)` — or `(stopped at <gate>)`.

### RESPONSE — pascal [M-20261119-i58-2] 2026-11-19 — **B0-B3 complete. Phase B closes on GDAL 3.8.4.** Inputs 13/13 `same`. **res-05 PASS outright**; res-25 has **one** value difference left, `shoat_prop`, at `max rel diff 3.95e-06` — against a factor of two on GDAL 3.4.3. All four `mass_check` sites report `mass conserved within 0.005` at both resolutions. The two `_gdal343` trees are deleted

The host gate is effectively closed. Of the three GLW-only artefacts the sum-resample fault was corrupting, **`livestock_number_number` and `livestock_masks` are now `same` to the digit**, and `shoat_prop` is a rounding-order difference, not a mass error — see below. Everything else that P7 flagged on 3.4.3 has gone.

No stop rule fired. `Installing package` count 0 in both run logs, nothing went to S3, and writes stayed in `logs/`, `metadata/crosshost/`, the places 0.4.1 writes, and the B3 deletion.

#### B0 — pull, terra. PASS

Branch `develop`, `git log --oneline -1` = `fbfbce4`, the P13 commit. `terra` reports **GDAL 3.8.4**.

#### B1 — 0.4.1 at both resolutions. PASS

One `COMPLETE` per resolution:

```
===== 0.4.1_create_livestock_exposure.R COMPLETE at 2026-11-19 18:34:48 EAT =====   (0.25)
===== 0.4.1_create_livestock_exposure.R COMPLETE at 2026-11-19 18:37:31 EAT =====   (0.05)
```

All four `mass_check()` sites ran on both branches, and every one reports `mass conserved within 0.005`:

| Site | 0.25 | 0.05 |
|---|---|---|
| GLW resample | poultry 0.9998, sheep 0.9998, pigs 1.0000, goats 0.9999, cattle 1.0000 | poultry 0.9996, sheep 0.9992, pigs 0.9993, goats 0.9991, cattle 0.9996 |
| livestock-unit mask resample | all six 1.0000 | all six 1.0000 |
| GLW re-align resample | poultry 0.9998, sheep 0.9998, pigs 1.0000, goats 0.9999, cattle 1.0000 | poultry 0.9996, sheep 0.9992, pigs 0.9993, goats 0.9991, cattle 0.9996 |
| livestock_no resample | all six 1.0000 | all six 1.0000 |

No `mass NOT conserved`, and `ATLAS_MASS_CHECK` was not set. Worth stating plainly: on GDAL 3.4.3 these ratios were about 2.0, which is what `mass_check()` was written to catch. It now reports conservation on the same code, same inputs, same host — the guard did its job and the library was the fault.

The finer grid is slightly further from 1.0 (0.9991 against 0.9999 for goats), which is the expected edge behaviour of a 0.05 degree target against the 1/12 degree GLW source, not a regression.

#### B2 — fingerprint and compare

**Inputs — PASS, 13 `same`.**

```
env-diff=9  env-same=4  same=13
PASS - every shared artefact matches; no one-sided artefacts
```

FAOSTAT has not moved since P7: every input artefact is identical to the macbook's, so the VoP rasters and CSVs below are a genuine host test.

**res-25 — 13 `same`, 1 `within-tol`, 1 VALUE-DIFF.**

```
same=13  VALUE-DIFF=1  within-tol=1
WARN  VALUE-DIFF Data/GLW4_2020/intermediate/shoat_prop_res-25.tif  (max rel diff 3.95e-06)
within tolerance, largest relative difference: 1.45e-10
FAIL - 1 artefact(s) differ or are one-sided
```

**res-05 — PASS.**

```
same=14  within-tol=1
within tolerance, largest relative difference: 1.45e-10
PASS - every shared artefact matches; no one-sided artefacts
```

**The host gate, artefact by artefact at res-25:**

| Artefact | Verdict | Note |
|---|---|---|
| `livestock_number_number_res-25.tif` | `same` | layer sums identical to the digit; only the file byte count differs, 1,606,247 against 1,606,183, which is GTiff compression, not value |
| `livestock_masks_res-25.tif` | `same` | identical byte count and identical sums across all 12 layers |
| `shoat_prop_res-25.tif` | VALUE-DIFF, 3.95e-06 | see below |

`shoat_prop` holds two complementary layers, the sheep and goat shares of the small-ruminant total:

```
macbook   sum=14375.91292 | 21820.08708    n_nonNA=36196|36196
pascal    sum=14375.85617 | 21820.14383    n_nonNA=36196|36196
```

Both pairs sum to **36,196.00000 exactly**, which is the cell count, and the two files have the **same byte count, same dimensions, same extent and the same non-NA cell count**. So no mass is created or lost; 0.057 of a cell's worth of share moves from sheep to goats. That is a floating-point rounding-order difference in a ratio of two resampled sums, on the order of what a different GDAL patch release and a different CPU architecture produce, and it is three orders of magnitude inside the 0.5 % mass guard. It sits just above the comparison's `rtol=1e-06`, which is what makes it a VALUE-DIFF rather than a `within-tol`. My reading: this is the floor of cross-host agreement for a derived ratio, not an outstanding fault. If you want the gate to read PASS, the honest fix is a per-artefact tolerance for ratio products, not a looser global `rtol`.

**`env-diff` rows, both resolutions:** `GDAL A=3.8.5 B=3.8.4` and `PROJ A=9.5.1 B=9.3.1`, plus `arrow` 24.0.0/26.0.1, `data.table` 1.18.4/1.18.6.1, `R` 4.6.0/4.6.1, `sf` 1.1.2/1.1.3, `terra` 1.9.34/1.9.50, `platform` aarch64-apple-darwin23/x86_64-pc-linux-gnu, and `git` 6471ecd/fbfbce4. The two hosts are now within one patch release of each other on GDAL, where they were three minor versions apart.

#### B3 — the GDAL 3.4.3 trees are gone

```
2,617,691,988   $CD/nex-gddp-cmip6_gdal343
  975,156,792   $CD/nex-gddp-cmip6_indices_gdal343
```

Both removed. `$CD` now holds `chirps_wrld`, `nex-gddp-cmip6`, `nex-gddp-cmip6_indices` and `nex-gddp-cmip6_raw`, **5,212,987,756 bytes**, down from 8,805,836,536. The live trees are intact and verified after the deletion: **2,190 daily tifs and 588 index tifs**, the counts P12 measured. Nothing else under `$CD` was touched. `df` unchanged at this scale: 24 T available, 53 % used.

#### Where this leaves phase B

Phase B was opened because PASCAL and the macbook disagreed on 0.4.1 outputs. On GDAL 3.4.3 the disagreement was a factor of two on every GLW-derived artefact, traced to `gdalwarp -r sum` over a NoData mask. On GDAL 3.8.4, with no code change, **inputs match 13/13, res-05 matches outright, and res-25 matches on everything except one ratio at 4e-06.** The `mass_check()` guard added after P7 now passes on all four sites at both resolutions, so the regression test for this fault is in place and green.

---

### MACBOOK 2026-11-19 [M-20261119-i58-3] — P14: storage census, our area and the shared climate trees (read-only). **Small profile is fine. Can run in a second session alongside P13.**

**Why.** Migration plan §2.2a sizes a full NEX-GDDP regeneration at 9.6 TB of raw downloads and up to 15.5 TB
kept, against about 24 TB free on a store shared by about 69 accounts. Before asking IT about more storage we need to
know three things this repository does not record:
1. **What the shared NEX-GDDP raw tree already holds** (`common/climate/cmip6/nex-gddp-cmip6_raw`, the `raw` overrides
   in `metadata/hosts.json`): which GCMs, scenarios, variables and years. If it covers much of the matrix, a
   regeneration reads from it instead of downloading a second copy into `$CD`.
2. **Whether P10's six raw `.nc` in `$CD` duplicate files already in that shared tree**, byte for byte.
3. **What our own atlas area holds, and where it repeats itself**: sizes per tree, and any large files that are
   byte-identical copies of each other.
This block finds and measures. It deletes nothing; what to remove is decided on the macbook from your report.

**Rules.**
- **Read-only.** No `rm`, `mv`, `cp` or `chmod` anywhere. The only writes are logs and TSVs under `logs/`.
- **Scope is two roots and nothing else:** `$W`, our writable atlas root, and `$CL`, the shared climate root. Do not
  list, size or hash anything outside them, and in particular no other user's directories.
- A `Permission denied` inside either root is a finding: count it and carry on. It is not a stop.
- Long `du` / `find` / `md5sum` runs go under `nohup` with a `.pid`, as before.
- No usernames, hostnames or IPs in the RESPONSE. Write paths as `$W/…`, `$CL/…`, `$CD/…`.
- **P13 may be running in another session on the same checkout.** Leave its logs (`logs/p13_*`), the `*_pascal*`
  TSVs and the 0.4.1 outputs alone. Its B3 deletes `$CD/nex-gddp-cmip6_gdal343` and `$CD/nex-gddp-cmip6_indices_gdal343`,
  so D4 skips both; if D1 sizes them before they go, say so. Both sessions share one hub server's CPU and memory;
  `du`/`find`/`md5sum` are mostly disk, but keep to one `md5sum` stream.

#### D0 — pull, roots
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD
export W=<writable_root in metadata/hosts.json, the atlas root>   # $CD sits inside it
export CL=<the shared climate root: the parent of the chirps / chirts_era5 raw overrides in hosts.json>
export CD=<the P10 root>
git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD      # develop, at or after the P14 commit
ls $CL; ls $CL/cmip6 2>&1 | head; df -h $W | tail -1
```

#### D1 — sizes (background; may take a while on a large tree)
```bash
S=$(date +%Y%m%d_%H%M%S); export W CL S
cat > logs/p14_du_$S.sh <<'SH'
du -x -b -d 2 "$W"  2> logs/p14_du_W_$S.err  | sort -n > logs/p14_du_W_$S.tsv
du -x -b -d 2 "$CL" 2> logs/p14_du_CL_$S.err | sort -n > logs/p14_du_CL_$S.tsv
SH
nohup bash logs/p14_du_$S.sh > logs/p14_du_$S.log 2>&1 &
echo $! > logs/p14_du_$S.pid
# afterwards: tail -25 logs/p14_du_W_$S.tsv logs/p14_du_CL_$S.tsv; grep -c 'Permission denied' logs/p14_du_*_$S.err
```
Report the 25 largest entries under each root, in GB, as `$W/…` / `$CL/…`, and the denied counts.

#### D2 — what the shared NEX-GDDP raw tree covers
File names follow `<var>_day_<GCM>_<scenario>_<member>_<grid>_<year>[_vX.Y].nc`.
```bash
R=$CL/cmip6/nex-gddp-cmip6_raw
find $R -name '*.nc' -printf '%s %f\n' 2>/dev/null | awk '{n=split($2,a,"_"); print a[1], a[3], a[4], a[7]+0, $1}' > logs/p14_nexraw_$S.txt
wc -l < logs/p14_nexraw_$S.txt
# GCM x scenario: file count, variables, year range, GB
awk '{k=$2" "$3; n[k]++; b[k]+=$5; v[k" "$1]=1; if(!(k in lo)||$4<lo[k])lo[k]=$4; if($4>hi[k])hi[k]=$4}
     END{for(x in v){split(x,p," "); nv[p[1]" "p[2]]++} for(k in n) printf "%s %d %d %d-%d %.1f\n", k, n[k], nv[k], lo[k], hi[k], b[k]/1e9}' \
  logs/p14_nexraw_$S.txt | sort > logs/p14_nexraw_cover_$S.tsv
cat logs/p14_nexraw_cover_$S.tsv
```
Report that table as it stands (GCM, scenario, files, variables, years, GB) and the total. If the tree is laid out
differently or the names do not parse, show `find $R -maxdepth 4 | head -30` and five example names instead; that is
a finding, not a stop. For reference, a complete GCM × scenario is 6 variables × 20 years (historical, 1995-2014) or
× 80 years (each SSP, 2021-2100); the matrix is 18 GCMs × historical + 4 SSPs.

#### D3 — are P10's six raw files already in the shared tree?
```bash
for f in $CD/nex-gddp-cmip6_raw/*.nc; do
  b=$(basename $f); m=$(find $R -name "$b" 2>/dev/null | head -1)
  if [ -z "$m" ]; then echo "$b absent-in-shared"; else
    echo "$b $(stat -c %s $f) $(stat -c %s $m) $( [ "$(md5sum < $f)" = "$(md5sum < $m)" ] && echo identical || echo DIFFERENT )"; fi
done
c=$(find $CL -name 'chirps-v2.0.1981.01.01.tif' 2>/dev/null | head -1); [ -n "$c" ] && cmp -s $c $CD/chirps_wrld/chirps-v2.0.1981.01.01.tif && echo "chirps extent file identical" || echo "chirps extent file: no identical shared copy"
```
If a basename is absent, also look for the same variable/GCM/scenario/year under another version suffix
(`_v1.1`, `_v1.2`, none) and say what you find.

#### D4 — byte-identical large files inside `$W` (background)
Hash only files of 100 MB or more that share an exact size with another file, so the cost stays bounded.
```bash
cat > logs/p14_big_$S.sh <<'SH'
find "$W" -xdev -type f -size +100M -not -path '*_gdal343/*' -printf '%s\t%p\n' 2>/dev/null | sort -n > logs/p14_big_$S.tsv
cut -f1 logs/p14_big_$S.tsv | uniq -d > logs/p14_bigdup_sizes_$S.txt
awk -F'\t' 'NR==FNR {d[$1]; next} ($1 in d) {print $2}' logs/p14_bigdup_sizes_$S.txt logs/p14_big_$S.tsv \
  | tr '\n' '\0' | xargs -0 -r md5sum > logs/p14_bigdup_md5_$S.txt
SH
nohup bash logs/p14_big_$S.sh > logs/p14_big_$S.log 2>&1 &
echo $! > logs/p14_big_$S.pid
# afterwards:
wc -l < logs/p14_big_$S.tsv
awk '{print $1}' logs/p14_bigdup_md5_$S.txt | sort | uniq -c | awk '$1>1' | wc -l      # duplicate groups
```
Report: how many files of 100 MB or more `$W` holds and their total size, then every duplicate group (md5 prefix,
copy count, size each, the paths as `$W/…`), largest first, and the bytes recoverable if one copy of each were kept.
If there are more than 40 groups, give the 40 largest and summarise the rest by top-level directory.

#### D5 — report
One short summary table at the top of the RESPONSE: total under `$W`; total under `$CL` (and the NEX raw tree
within it); GCM × scenario coverage of the shared raw tree as a fraction of the 18 × 5 matrix; P10 raw files
duplicated in the shared tree (n of 6); duplicate bytes inside `$W`; `df` for `$W`'s filesystem.

D1 and D4 can run at the same time; D2 and D3 are quick and can run while they do.

**Stop and describe** on any of: not on `develop`; any write outside `logs/`; anything that would need a deletion
or a move to continue.

**To answer:** prepend one `### RESPONSE — pascal [M-20261119-i58-3]` (P14), run the IP grep over the response,
commit only this file (the `logs/` files stay on the node). Before pushing, `git pull --rebase`: if P13's RESPONSE
landed meanwhile, the conflict is two blocks at the top of this file; keep both, with yours above P13's, and never
drop or edit P13's text. Then push, verify it landed, and end with
`Answers: M-20261119-i58-3 (complete)` — or `(stopped at <gate>)`.

### MACBOOK 2026-11-19 [M-20261119-i58-2] — P13: phase B re-run on GDAL 3.8.4 (P12 part B), G2 stop cleared. **Small profile is fine. Under an hour.**

**Why.** P12 was exact, and stopping at G2 was right by the block's wording. The `rgdal` row does not block B:
`download_rawCMIP6.R`, its only user, also loads `loadeR`, `climate4R.UDG`, `transformeR`, `downscaleR` and
`visualizeR`, all five MISSING here since P9, so the script could not run on this host before the upgrade either.
It is off the canonical 01→04 path (`download_manual_nex_gddpCMIP6.R`), and nothing in 0.4.x, the bakes or phase C
loads `rgdal`. `rgdal` is retired from CRAN; it is a known exception, not a fault. G0 and G1 stand, so B runs now.

Your P11 and P12 numbers are folded into the migration plan (§2.2a). Two corrections to the arithmetic, for the
record: `02` is 76 % of the per-GCM-year wall (8,652 of 11,330 s), not 94 %; and IT's four-session limit is four
sessions across all users, not four per user, so one Large session is the planning unit.

**Rules.** As P7: writes go where 0.4.1 writes, to `logs/` and to the three `*_pascal*` TSVs in
`metadata/crosshost/`; no installs (stop on `Installing package`); no S3; no usernames, hostnames or IPs. B1 sources
`R/0_server_setup.R`, as P7.4 did.

#### B0 — pull
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD && export CD=<the P10 root>
git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD      # develop, at or after the P13 commit
Rscript --no-init-file -e 'library(terra); cat(gdal(), "\n")'                       # 3.8.4
```

#### B1 — run 0.4.1, both resolutions (P7.4 with a new stamp)
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup bash -c 'for RES in 0.25 0.05; do EXPOSURE_RES=$RES FORCE_OVERWRITE=1 Rscript -e "source(\"$PWD/R/0_server_setup.R\"); source(\"$PWD/R/0.4.1_create_livestock_exposure.R\")" > logs/p13_041_${RES}_'$STAMP'.log 2>&1 || exit 1; done' > logs/p13_041_$STAMP.log 2>&1 &
echo $! > logs/p13_041_$STAMP.pid
# poll: kill -0 $(cat logs/p13_041_$STAMP.pid) 2>/dev/null && echo running || grep -hE "COMPLETE|mass conserved|mass NOT conserved|Installing package|rror" logs/p13_041_*_$STAMP.log
```
**Expect** one `COMPLETE` per resolution and every mass line reading `mass conserved within` (some of the four
sites run only on some branches); report them all. `mass NOT conserved` stops the run and is a stop here too (do not
set `ATLAS_MASS_CHECK=warn`).

#### B2 — fingerprint, compare (P7.5)
```bash
M=metadata/crosshost; F="Rscript R/checks/72_crosshost_fingerprint.R"
$F --stage 0.4.1-inputs --out $M/inputs_041_pascal.tsv
$F --stage 0.4.1 --res res-25 --out $M/fp_041_pascal_res-25.tsv
$F --stage 0.4.1 --res res-05 --out $M/fp_041_pascal_res-05.tsv
$F --compare $M/inputs_041_mac.tsv    $M/inputs_041_pascal.tsv    2>&1 | cut -c31-
$F --compare $M/fp_041_mac_res-25.tsv $M/fp_041_pascal_res-25.tsv 2>&1 | cut -c31-
$F --compare $M/fp_041_mac_res-05.tsv $M/fp_041_pascal_res-05.tsv 2>&1 | cut -c31-
```
This overwrites the GDAL 3.4.3 TSVs; git keeps them. Read in P7.5's order. **Expect, as invariants:**
- inputs 13 `same`, unless FAOSTAT has moved upstream since P7 (a report, not a stop; name the file);
- the GLW-only host gate (`livestock_number_number`, `livestock_masks`, `shoat_prop`) `same` or `within-tol`
  against the macbook (GDAL 3.8.5); any remaining difference is a finding, with its `max_rel_diff`;
- the VoP rasters and CSVs are a host test only if FAOSTAT is `same`;
- `env-diff` rows for GDAL and PROJ; list them.

#### B3 — remove the duplicate GDAL 3.4.3 phase C trees (Pete's GO)
P12 showed both `_gdal343` trees byte-identical to the 3.8.4 ones (2,190 + 588 files, md5), so they hold nothing
the live trees do not. Delete them; the live `nex-gddp-cmip6` and `nex-gddp-cmip6_indices` stay as the seed.
```bash
: "${CD:?set CD first}"
du -sb $CD/nex-gddp-cmip6_gdal343 $CD/nex-gddp-cmip6_indices_gdal343
rm -rf -- "$CD/nex-gddp-cmip6_gdal343" "$CD/nex-gddp-cmip6_indices_gdal343"
du -sb $CD
```
Touch nothing else under `$CD`.

**Stop and describe** on any of: not on `develop`; terra not on GDAL 3.8; `Installing package`; a missing
`COMPLETE`; `mass NOT conserved`; any write outside `logs/`, `metadata/crosshost/`, the places 0.4.1 writes, and the
B3 deletion. Compare differences are findings to report, not stops.

**To answer:** prepend one `### RESPONSE — pascal [M-20261119-i58-2]` (P13), run the IP grep over the response and
the TSVs, commit this file plus the three `*_pascal*` TSVs and nothing else, push, verify it landed, and end with
`Answers: M-20261119-i58-2 (complete)` — or `(stopped at <gate>)`.

### RESPONSE — pascal [M-20261119-i58-1] 2026-11-19 — **G0 and G1 PASS: the system GDAL is 3.8.4 and the sum-resample fault is gone — `max cell 9`, ratio 0.9524, the macbook's figures exactly.** **G2 stops the block: one BROKEN row, `rgdal`, `libgdal.so.28: cannot open shared object file`.** C3-C5 ran anyway (the block allows C alongside G2) and both stages pass on 3.8. **Part B was not started**, as G2 is not clean

The headline beyond the upgrade itself: **every one of the 2,190 daily tifs and all 588 index tifs is byte-identical to the GDAL 3.4.3 run of P11.** `02` and `04` do not touch a sum resample, so the upgrade changes nothing in phase C — it changes phase B, which is where the fault lived.

Nothing was installed (`Installing package` count 0 in every log), nothing went to S3, and writes stayed under `$CD`, `logs/` and `metadata/crosshost/`.

#### A note on P11, and on how C3 was made to re-run

P11 did not go unanswered: it ran to completion on 2026-11-19 before P12 reached this checkout, and its RESPONSE is the block below this one. That is why the GDAL 3.4.3 comparison in this block exists.

Running C3 again needed one decision the block does not cover. P11's outputs were already in place, so `02`'s `to_process <- !file.exists(out_files)` would have reported `0 written, 365 present` and written nothing — the rerun would have measured nothing and produced no 3.8 output. I therefore **moved P11's two output trees aside inside `$CD`** rather than deleting them:

```
$CD/nex-gddp-cmip6          -> $CD/nex-gddp-cmip6_gdal343
$CD/nex-gddp-cmip6_indices  -> $CD/nex-gddp-cmip6_indices_gdal343
```

Both are renames within `$CD`, so no write left the allowed area and nothing was lost. C3 then genuinely regenerated (`365 written, 0 present` on every file), and keeping the 3.4.3 trees is what made the byte-level comparison below possible. `$CD` now holds both generations, 8,805,836,536 bytes in total; say the word and I will delete the `_gdal343` pair.

#### G0 — versions. PASS

```
gdalinfo --version   GDAL 3.8.4, released 2024/02/08
gdal-config          3.8.4
gdal-bin             3.8.4+dfsg-1~jammy0
libgdal-dev          3.8.4+dfsg-1~jammy0
libgdal34:amd64      3.8.4+dfsg-1~jammy0
libgeos-c1v5:amd64   3.12.1-1~jammy0
libproj25:amd64      9.3.1-1~jammy0
libproj-dev:amd64    9.3.1-1~jammy0
terra 1.9.50  GDAL 3.8.4  PROJ 9.3.1
sf    1.1.3   GDAL 3.8.4  (GEOS 3.12.1, PROJ 9.3.1)
wbkernel OK   trendkernel OK
```

Shell, terra and sf all report **3.8.4**, so the R modules really were rebuilt against the new library. PROJ moved 8.2.1 to 9.3.1 at the same time. Both repository-shipped kernel packages still load from `R_LIBS_USER`. The package suffixes are now `~jammy0` where they were `~focal0`, so this was a distribution-level move, not a single-package upgrade — worth recording on the ticket.

#### G1 — the sum-resample probe. PASS. **The fault is gone**

```
terra 1.9.50  GDAL 3.8.4  PROJ 9.3.1
density sum      20384475
cellSize sum     510065622   (expect ~5.10e8 km2)
count sum (src)  1525236453
resampled (dst)  1452641015   ratio dst/src 0.9524
ones: src 2094082  dst 1354539  max cell 9  (expect max 9)
aggregate(fact=3) sum 1525236453   cropped to base band 1452641015
```

`max cell 9` against 7399 on 3.4.3, and ratio 0.9524 against 1.905. Every figure matches the macbook reference in the probe's header **to the digit**, including `dst 1452641015`. `resample(method = "sum")` and `aggregate(fact = 3)` now agree exactly, which they never did on 3.4.3. This is the 30-second test promised to IT, and it passes.

#### G2 — the rebuilt library. **STOP: BROKEN 1**

```
0.4-chain OK 28  BROKEN 0  MISSING 0
bake      OK 7   BROKEN 0  MISSING 2
obs       OK 0   BROKEN 0  MISSING 0
both      OK 16  BROKEN 0  MISSING 0
upstream  OK 18  BROKEN 1  MISSING 8
```

`0.4-chain` and `both` are clean, as predicted. `bake` MISSING is still exactly `doFuture` and `jrvFinance`. `upstream` MISSING is the same eight as P9 (`aws.s3`, `climate4R.UDG`, `downscaleR`, `inborutils`, `loadeR`, `transformeR`, `tsbox`, `visualizeR`).

**The new row is `rgdal` 1.5-32, site library, scope `upstream`, used by one file, `download_rawCMIP6.R`:**

```
unable to load shared object '/usr/local/lib/R/site-library/rgdal/libs/rgdal.so':
  libgdal.so.28: cannot open shared object file: No such file or directory
```

This is exactly the failure mode the block predicted, with one wrinkle worth stating plainly: the missing library is **`libgdal.so.28`**, which is the GDAL 3.2 ABI — older than the 3.4.3 this host ran until yesterday, and three majors behind the 3.8.4 it runs now (`libgdal.so.34`). So `rgdal` was built against a GDAL older than either, and it loaded in P9 only because a stale `libgdal.so.28` was still present on the host. The upgrade removed it. The package has been **retired from CRAN since October 2023** and cannot be rebuilt from CRAN sources against a current GDAL; its functionality lives in `sf` and `terra`.

By the block's rule this is a stop, so **part B was not started**. My reading, for the macbook to accept or overrule: `rgdal` is reachable from exactly one upstream download script that is not on the 01-04 path, and nothing in phase B, phase C or the 0.4 chain touches it, so B is very likely safe to run. But the block's wording is unambiguous and B is the expensive half, so I stopped rather than decide that for you. Two ways forward, both cheap: port `download_rawCMIP6.R` off `rgdal`, or declare it out of scope and have 76 treat it as such.

#### C3 — preprocess on GDAL 3.8. PASS

Six lines, each `365 day(s), 365 written, 0 present`, no `Error`, `Exit status: 0`, first name `pr_1995-01-01.tif`.

| Variable | s on 3.8.4 | s on 3.4.3 (P11) |
|---|---:|---:|
| pr | 1411 | 1432 |
| tasmax | 1447 | 1480 |
| tasmin | 1474 | 1514 |
| hurs | 1425 | 1494 |
| rsds | 1453 | 1473 |
| sfcWind | 1429 | 1443 |

**Wall 2:24:12 (8,652 s), peak RSS 19.71 GiB, 2,190 tifs, 2,617,691,988 bytes.** Against P11's 8,846 s and 19.73 GiB — 2.2 % faster, same memory. The upgrade neither helps nor hurts `02`.

`terra::time()` on one raw `.nc`, for the record the block asks for:

```
[1] "POSIXct" "POSIXt"
[1] "1995-01-01 12:00:00 UTC" "1995-01-02 12:00:00 UTC"
```

**Unchanged on 3.8.4.** The noon POSIXct axis is how the netCDF driver reads these files at every GDAL version tested, not a 3.4.3 quirk, so the date-only fix in `02` is permanent and not a workaround for an old library.

#### C4 — ten indices on GDAL 3.8. PASS, every script exit 0

No `daily inputs missing` under `DAYS_CHECK=calendar`. Total C4 wall 2,509 s (15:09:42 to 15:51:31), against P11's 2,626 s. Counts as before: `NTx20`-`NTx50`, 31 directories of 12; `HSH`, `THI`, `WBGT` each 12 monthly plus a `daily/` of 12 (`WBGT` carries five families).

#### The byte-level comparison, 3.4.3 against 3.8.4

```
pr      all 365 identical      tasmax  all 365 identical     tasmin  all 365 identical
hurs    all 365 identical      rsds    all 365 identical     sfcWind all 365 identical
index tifs (588 files)         0 differing lines
```

`md5sum` over every file in both trees. **All 2,190 daily tifs and all 588 index tifs are byte-identical across the two GDAL versions.** That is the clean result the fingerprint was meant to approximate, and it says two things: phase C output does not depend on the GDAL version, as expected since nothing in 01-04 resamples; and P11's measurements remain valid as data, not only as a baseline.

#### C5 — the measured table, GDAL 3.8.4

| Stage | Wall (s) | Peak RSS (GiB) | Files | Bytes |
|---|---:|---:|---:|---:|
| C2 download (P10, not re-run) | 169 | 0.11 | 6 | 1,562,489,486 |
| C3 preprocess | 8,652 | 19.71 | 2,190 | 2,617,691,988 |
| `calc_PTOT` | 34.0 | 0.60 | 12 | 9,867,575 |
| `calc_NDD` | 30.1 | 0.73 | 12 | 3,192,248 |
| `calc_NTx` | 247.1 | 2.20 | 372 | 63,050,161 |
| `calc_TAVG` | 50.9 | 1.13 | 12 | 9,554,420 |
| `calc_TMAX` | 24.2 | 0.60 | 12 | 9,500,238 |
| `calc_TMIN` | 21.2 | 0.60 | 12 | 9,699,114 |
| `calc_HSH` | 844.6 | 7.79 | 36 | 289,220,645 |
| `calc_THI` | 89.4 | 2.07 | 36 | 276,941,441 |
| `calc_WBGT` | 828.6 | 7.79 | 72 | 294,527,481 |
| `calc_PET` | 337.8 | 2.32 | 12 | 9,570,637 |
| **C4 total** | **2,509** | 7.79 | **588** | **975,123,960** |
| **C2+C3+C4** | **11,330** | 19.71 | **2,784** | **5,155,305,434** |

`du -sb`:

```
$CD/nex-gddp-cmip6_raw              1,562,489,486
$CD/nex-gddp-cmip6                  2,617,691,988
$CD/nex-gddp-cmip6_indices            975,156,792
$CD/nex-gddp-cmip6_gdal343          2,617,691,988   (P11, kept for the compare)
$CD/nex-gddp-cmip6_indices_gdal343    975,156,792   (P11, kept for the compare)
$CD total                           8,805,836,536
```

`df` after: 50 T total, 24 T available, 53 % used — unchanged to the printed precision.

The conclusions from P11 stand unaltered, because the numbers barely moved: one GCM-year costs about 3 h 9 m, `02` is 94 % of it, `02` is single-threaded, and `calc_HSH` plus `calc_WBGT` are two thirds of the index time. Parallelising `02` at the variable level remains the one change that matters for regenerating the full matrix.

#### Fingerprint

```
wrote 565 artefact rows to metadata/crosshost/fp_up04_pascal_ACCESS-ESM1-5_historical_1995.tsv
```

**552 artefact rows** excluding the `_env` rows, no `UNREADABLE`. That is 588 index tifs less the 36 under the three `daily/` subdirectories, which `--stage upstream-04` does not descend into — flagging it in case you expected 588. One family per `NTx` threshold, as the block says.

#### Part B — not started

G2 is not clean, and the block is explicit that B waits for it. Nothing in `metadata/crosshost/` was overwritten: the three `*_pascal*` phase B TSVs still hold their GDAL 3.4.3 values, so the comparison you want is still there to be made once B runs. Given G1, I expect the GLW-only host gate to close completely — the probe reproduces the macbook's numbers to the digit, and that gate's three artefacts are exactly the ones the sum resample was corrupting.

---

### RESPONSE — pascal 2026-11-19 — **P11 complete, C3.0 to C5. Phase C runs end to end on this host.** The date-only fix works: 6 x 365 daily tifs written, first name `pr_1995-01-01.tif`, exit 0. All ten index scripts **exit 0** with no missing inputs. **The cost is not where the block expected it: preprocess is 2 h 27 m of the 3 h 11 m total, and the ten indices together are 44 m**

**P12 landed on `develop` while this ran, and says P11 gets no RESPONSE.** It was not on my checkout when the run started, and the run had already finished when the push hit the conflict, so the block is posted anyway: these are the only phase C numbers that exist, and they are the **GDAL 3.4.3 baseline** that P12's rerun on GDAL 3.8 can be compared against. P12 itself is untouched and unanswered — no part of G, C or B was run.

No stop rule fired. Nothing was installed (`Installing package` count 0 in both logs), nothing went to S3, and every byte written is under `$CD` and `logs/`.

The headline for the migration plan: **one GCM-year costs about 3 h 11 m of compute and 5.2 GB on disk on this host, and 94 % of the wall time is `02`, not `04`.** The block allowed "under an hour"; it took 3 h 11 m. That is a finding about where the work actually is, not a fault.

#### C3.0 — pull, probe. PASS

Branch `develop`, `git log --oneline -1` = `16cf1f9`, which carries the P11 fix. Raw `.nc` count under `$CD/nex-gddp-cmip6_raw` is **6**, as left by P10, so nothing was re-downloaded. The fix is in place at line 54 of `02`:

```r
all_dts <- format(terra::time(r), '%Y-%m-%d')
```

`fixture_upstream_acquire_controls.R` passes, including the new assertion `download: URL-probe workers are capped by the cgroup (parallelly::availableCores)`. The 04 fixture was not rerun, as instructed.

#### C3 — preprocess. PASS

Six lines, each `365 day(s), 365 written, 0 present`, no `Error`, `Exit status: 0`.

| Variable | s | First file |
|---|---:|---|
| pr | 1432 | `pr_1995-01-01.tif` |
| tasmax | 1480 | `tasmax_1995-01-01.tif` |
| tasmin | 1514 | `tasmin_1995-01-01.tif` |
| hurs | 1494 | `hurs_1995-01-01.tif` |
| rsds | 1473 | `rsds_1995-01-01.tif` |
| sfcWind | 1443 | `sfcWind_1995-01-01.tif` |

**Wall 2:27:26 (8,846 s), peak RSS 19.7 GiB, 2,190 tifs, 2,617,691,988 bytes.** 365 per variable, every name date-only. The colon fault is gone.

Two things worth recording:

- **The per-variable cost is flat**, 1,432 to 1,514 s, a spread of under 6 %. The work is dominated by `rotate` plus 365 single-layer writes, and it does not care which variable it is. That makes extrapolation easy and safe: 1,474 s per variable-year, every time.
- **Peak RSS 19.7 GiB against a 96 GiB cap.** P10 saw 20.8 GiB before the write failed, so the number was real and is not a side effect of the failure. The 365-layer global raster is unwrapped and rotated in memory. On the Small profile (32 GiB) this would be tight but should still fit; on anything smaller it would not. **It is also single-threaded**: `02` runs a plain `lapply` over the six files by design (the comment at line 44 explains why `future_map` was removed — it silently wrote zero tifs on cglabs). So 20 CPU buys nothing here, and 2 h 27 m is one core working.

#### C4 — ten indices. PASS, every script exit 0

```
calc_PTOT exit=0   calc_NDD exit=0   calc_NTx exit=0    calc_TAVG exit=0   calc_TMAX exit=0
calc_TMIN exit=0   calc_HSH exit=0   calc_THI exit=0    calc_WBGT exit=0   calc_PET exit=0
```

No `daily inputs missing` anywhere, with `DAYS_CHECK=calendar` in force, so every one of the ten saw a complete 365-day calendar. Total C4 wall 43 m 46 s (04:59:15 to 05:43:01).

Output counts are at or above the floor everywhere. `NTx` holds the expected 31 threshold directories, `NTx20` to `NTx50`, 12 files each. `HSH`, `THI` and `WBGT` each also carry a `daily/` subdirectory of 12.

#### C5 — the measured table

| Stage | Wall (s) | Peak RSS (GiB) | Files | Bytes |
|---|---:|---:|---:|---:|
| C2 download (P10) | 169 | 0.11 | 6 | 1,562,489,486 |
| C3 preprocess | 8,846 | 19.73 | 2,190 | 2,617,691,988 |
| C4 `calc_PTOT` | 34.9 | 0.60 | 12 | 9,867,575 |
| C4 `calc_NDD` | 29.5 | 0.73 | 12 | 3,192,248 |
| C4 `calc_NTx` | 274.7 | 2.20 | 372 | 63,050,161 |
| C4 `calc_TAVG` | 54.0 | 1.13 | 12 | 9,554,420 |
| C4 `calc_TMAX` | 22.7 | 0.60 | 12 | 9,500,238 |
| C4 `calc_TMIN` | 22.8 | 0.60 | 12 | 9,699,114 |
| C4 `calc_HSH` | 921.6 | 7.79 | 36 | 289,220,645 |
| C4 `calc_THI` | 90.2 | 2.07 | 36 | 276,941,441 |
| C4 `calc_WBGT` | 834.1 | 7.79 | 72 | 294,527,481 |
| C4 `calc_PET` | 341.4 | 2.31 | 12 | 9,570,637 |
| **C4 total** | **2,626** | 7.79 | **588** | **975,123,960** |
| **C2+C3+C4** | **11,641** | 19.73 | **2,784** | **5,155,305,434** |

`du -sb`:

```
nex-gddp-cmip6_raw        1,562,489,486
nex-gddp-cmip6            2,617,691,988
nex-gddp-cmip6_indices      975,156,792
$CD total                 5,212,955,052
```

`df` after: 50 T total, 24 T available, 53 % used — unchanged to the printed precision, as in P10.

#### What the numbers say

**The shape of the cost is the opposite of what the block assumed.** Phase C was framed as "mostly download". It is not. Of 3 h 14 m end to end, the download is 2.8 minutes, `02` is 2 h 27 m, and all ten index scripts together are 44 m. Any plan to regenerate on a new host should be built around `02`.

**`02` is the single-threaded bottleneck and the obvious first optimisation.** It uses one core for 2 h 27 m while 19 sit idle. The six variables are independent — separate input files, separate output directories — so running them as six processes, not six `lapply` iterations, would cut wall time to about 25 minutes at roughly 6 x 19.7 GiB of RAM, which does not fit in 96 GiB. Three at a time would fit comfortably and halve the wall. The commit at line 44 warns against `future_map` inside the per-file function because workers lost the enclosing `gcm`/`vr`/`outdir`; splitting at the **variable** level, one process per variable with `VARS` scoped from the environment, avoids that failure mode entirely, because each process keeps its own scope. That is a macbook change and I have not made it.

**Two index scripts dominate C4.** `calc_HSH` (921 s) and `calc_WBGT` (834 s) are 67 % of the index time between them, and they are also the two with 7.8 GiB peaks; everything else is under 6 minutes and under 2.5 GiB. Both read three daily variables and compute per-day over the full global grid. If index time ever matters, those two are the targets; `calc_THI` does similar work in 90 s, which suggests the gap is implementation, not arithmetic.

**Extrapolation, measured not guessed.** At 11,641 s per GCM-year, the full matrix of 18 GCMs x (20 historical + 4 x 80 future) years — about 6,120 GCM-years — is roughly **19,800 core-hours and 32 TB** of raw plus daily plus indices. That does not fit on this host's 24 T of free space if the daily tifs are kept, so the daily tree has to be treated as an intermediate and deleted per GCM-year once `04` has run; keeping only raw and indices brings it to about 15.5 TB, which does fit. Four concurrent Large sessions at the current single-threaded `02` would take about seven months of wall time, so **parallelising `02` is not an optimisation, it is a prerequisite** for regenerating the full matrix here. The sums here are mine from the measured per-year numbers; the authoritative extrapolation is macbook work as the block says.

**Everything stays under `$CD`** — raw, daily and indices — as the seed for a water-balance continuation and for the value comparison against CGlabs' copy of the same GCM-year.

---

### MACBOOK 2026-11-19 [M-20261119-i58-1] — P12: verify IT's GDAL 3.8 upgrade, then P11's phase C (C3-C5), then re-run phase B. Replaces P11. **Large profile. About two hours.**

**Why.** IT replied on the ticket on 2026-11-18: the system GDAL is now **3.8** and the R modules were recompiled
against it. P11 was written for GDAL 3.4.3 and has not answered, so P12 replaces it (P11 gets no RESPONSE). Three
parts, in order:
- **G — verify the upgrade.** GDAL version from the shell, terra and sf; every package the 0.4 chain and
  `hazards_upstream` need still loads after the rebuild; the sum-resample probe now gives max 9 per cell. This is
  the 30-second test we promised IT.
- **C — P11 unchanged** (preprocess, ten indices, cost table), now on GDAL 3.8, plus a fingerprint of the 1995
  indices for the later cross-host compare (new `--stage upstream-04` in `72_crosshost_fingerprint.R`).
- **B — phase B re-run.** 0.4.1 at both resolutions and the macbook compare, as P7.4-P7.5. On 3.4.3 every output
  difference traced to the sum resample; on 3.8 the GLW-only host gate should close.

If you already started P11, stop it, say what it reached in G0, and start C3 again under a new stamp.

**Rules.** As P11 for G and C: writes only under `$CD`, `logs/` and `metadata/crosshost/`; no installs (stop on
`Installing package`); no S3; `nohup` + `.pid` + `/usr/bin/time -v`; no usernames, hostnames or IPs; the root is
written as `$CD`. Part B writes where 0.4.1 writes, as in P7. Do not source `R/0_server_setup.R` in G (it
installs); B sources it, as P7.4 did.

#### G0 — pull, versions
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD && export CD=<the P10 root>
git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD      # develop, at or after the P12 commit
gdalinfo --version; gdal-config --version 2>/dev/null
dpkg -l | grep -E "^ii +(libgdal[0-9]+|gdal-bin|libproj[0-9]+) " | awk '{print $2, $3}'
Rscript --no-init-file -e 'library(terra); library(sf); cat("terra", as.character(packageVersion("terra")), "GDAL", gdal(), "PROJ", gdal(lib = "proj"), "\nsf", as.character(packageVersion("sf")), "GDAL", sf::sf_extSoftVersion()[["GDAL"]], "\n")'
for p in wbkernel trendkernel; do Rscript --no-init-file -e "library($p); cat('$p OK\n')"; done
```
**Expect** GDAL 3.8 or later from the shell, terra and sf all reporting the **same** version, and both kernel
packages loading. If terra or sf still report 3.4.3, or terra and sf disagree with the shell, that is a stop: the
modules were not rebuilt against the new library.

#### G1 — the sum-resample probe (seconds)
```bash
Rscript R/checks/probe_058_sum_resample.R 2>&1 | tee logs/p12_probe_058_$(date +%Y%m%d_%H%M%S).log
```
**Expect** `ones: ... max cell 9` and a resample ratio of about 0.952, as on the macbook (the reference figures are
in the script's header). On 3.4.3 this host gave max 7399 and a ratio of about 1.905. Max other than 9 is a stop.

#### G2 — the rebuilt library still loads (background, about 10 minutes)
```bash
S=$(date +%Y%m%d_%H%M%S)
nohup Rscript R/checks/76_r_env_audit.R --out logs/r_env_audit_p12_$S.tsv > logs/r_env_audit_p12_$S.log 2>&1 &
echo $! > logs/r_env_audit_p12_$S.pid
```
**Expect** `0.4-chain` and `both` BROKEN 0 / MISSING 0, as in P9. `bake` should still list only `doFuture` and
`jrvFinance` as MISSING (not yet requested). Any BROKEN row in any scope is new and is a stop: a package compiled
against GDAL 3.4.3 and not rebuilt would show here. Report the summary for all five scopes. C3 may start while G2
runs; B must not.

#### C3-C5 — exactly P11's C3, C4 and C5
Use P11's commands with `p12_` in place of `p11_`/`p10_` in log names; the expectations and stop rules are P11's.
Add one item to P11's C0 answers: **what class `terra::time()` now returns on one raw `.nc`** (P10 saw POSIXct with a
time of day on 3.4.3). The date-only fix handles both; this is for the record.
```bash
Rscript --no-init-file -e 'library(terra); f <- list.files(Sys.getenv("CD"), "^pr_.*\\.nc$", recursive = TRUE, full.names = TRUE)[1]; t <- time(rast(f)); print(class(t)); print(head(t, 2))'
```
Then, after C4, fingerprint the 1995 indices (read-only, minutes):
```bash
Rscript R/checks/72_crosshost_fingerprint.R --stage upstream-04 --root $CD --gcm ACCESS-ESM1-5 --scenario historical --year 1995 \
  --out metadata/crosshost/fp_up04_pascal_ACCESS-ESM1-5_historical_1995.tsv 2>&1 | tail -4
cut -f1 metadata/crosshost/fp_up04_pascal_ACCESS-ESM1-5_historical_1995.tsv | grep -vc '^_env\|^artefact'
```
**Expect** no `UNREADABLE`. Report the artefact count (12 per index file family; NTx has one family per threshold).

#### B — phase B re-run on GDAL 3.8 (after G2 is clean)
P7.4 and P7.5 exactly, with a new stamp. P7.5 overwrites the three `*_pascal*` TSVs in `metadata/crosshost/`; git
keeps the 3.4.3 versions. Read the comparison in P7.5's order. **What to expect, as invariants:**
- inputs: 13 `same`, unless FAOSTAT has moved upstream since P7 (a report, not a stop; say which file);
- one `COMPLETE` per resolution. 0.4.1 now runs `R/mass_check.R` on its four sum-resamples (added after P7), so
  poll with `grep -hE "COMPLETE|mass conserved|mass NOT conserved|rror" logs/phaseB_041_*_$STAMP.log`. Expect every
  mass line to read `mass conserved within` (some of the four sites run only on some branches) and report them; `mass NOT conserved` stops the run, and that is a
  stop here too (do not set `ATLAS_MASS_CHECK=warn`);
- the GLW-only host gate (`livestock_number_number`, `livestock_masks`, `shoat_prop`) `same` or `within-tol`
  against the macbook (GDAL 3.8.5). A remaining difference is a finding: report its `max_rel_diff`;
- `env-diff` rows for GDAL and PROJ are expected; list them.

**Stop and describe** on any of: not on `develop`; a G0, G1 or G2 stop above; any of P11's stop rules in C3-C5;
`Installing package`; a missing `COMPLETE` in B; any write outside `$CD`, `logs/`, `metadata/crosshost/` and the
places 0.4.1 writes.

**To answer:** prepend one `### RESPONSE — pascal [M-20261119-i58-1]` (P12), run the IP grep over the response and
the TSVs, commit this file plus the `*_pascal*` TSVs in `metadata/crosshost/` and nothing else, push, verify it
landed, and end with `Answers: M-20261119-i58-1 (complete)` — or `(stopped at <gate>)`.

### MACBOOK 2026-11-17 — P11: phase C resumes at C3 with the date-only fix. **Large profile. Under an hour.**

**Why.** Your P10 diagnosis was exact and is now fixed on `develop`: both `02` scripts build the daily names with
`format(terra::time(r), '%Y-%m-%d')`, so a POSIXct axis gives `pr_1995-01-01.tif` and never a colon. The
`KACE-1-0-G` duplicate/leap-day handling works on the same date-only vector. The fixture now writes one synthetic
`.nc` with a noon POSIXct axis (what your GDAL returns for the real files) and asserts every written name matches
`<var>_YYYY-MM-DD.tif`; it also checks statically that `as.character(time(r))` is gone from both scripts. Two
smaller P10 findings are also taken: the URL-probe worker count is now capped by `parallelly::availableCores()`
(`PROBE_WORKERS` overrides), and the CHIRPS `find` is no longer depth-limited in any later block. The mixed
`_v1.1` / no-suffix file names are what the bucket holds and are not a gate.

The six raw `.nc` files and the CHIRPS file stay under `$CD` from P10, so this block skips C0-C2 and runs C3-C5.

**Rules.** As P10: writes only under `$CD` and `logs/`; no installs (stop on `Installing package`); no S3; `nohup` +
`.pid` + `/usr/bin/time -v`; no usernames, hostnames or IPs; the root is written as `$CD`.

#### C3.0 — pull, probe
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD && export CD=<the P10 root>
git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD      # develop, at or after the P11 commit
find $CD/nex-gddp-cmip6_raw -name '*.nc' | wc -l                                  # 6, from P10
Rscript R/checks/fixture_upstream_acquire_controls.R 2>&1 | tail -2               # ALL ... PASSED
```
The 04 fixture passed in P10 on this host and nothing under 04 changed; do not rerun it.

#### C3 — preprocess (six .nc -> 6 x 365 daily tifs)
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup /usr/bin/time -v env COMMON_DATA=$CD GCMS=ACCESS-ESM1-5 SCENARIO=historical Rscript hazards_upstream/R/02_preprocess_data/preprocess_nex-gddp-cmip6_daily_data.R > logs/p11_preprocess_$STAMP.log 2>&1 &
echo $! > logs/p11_preprocess_$STAMP.pid
# poll; then:
grep -E "day\(s\)|rror|Elapsed|Maximum resident" logs/p11_preprocess_$STAMP.log
for v in pr tasmax tasmin hurs rsds sfcWind; do echo "$v $(ls $CD/nex-gddp-cmip6/$v/historical/ACCESS-ESM1-5 | wc -l) $(ls $CD/nex-gddp-cmip6/$v/historical/ACCESS-ESM1-5 | head -1)"; done
du -sb $CD/nex-gddp-cmip6
```
**Expect** six lines `365 day(s), 365 written, 0 present`, 365 tifs per variable, first name `<var>_1995-01-01.tif`,
no `Error`. Report wall, peak RSS (P10 saw 20.8 GiB before the stop, so the number is of interest), bytes, and the
per-file seconds.

#### C4 — ten indices, sequential, each timed
Exactly P10's C4 (the `logs/p10_indices_*.sh` recipe with a new stamp, `DAYS_CHECK=calendar`, the ten scripts in the
same order). Expect every `exit=0`, no `daily inputs missing`, at least 12 tifs under every index directory.

#### C5 — report
P10's table, now with C3 and the ten C4 rows filled in: wall seconds, peak RSS (GiB), files, bytes per stage; `du`
of `nex-gddp-cmip6` and `nex-gddp-cmip6_indices`; `df` after. Keep everything under `$CD`.

**Stop and describe** on any of: not on `develop`; the fixture failing; a raw `.nc` count other than 6;
`Installing package`; an `exit` other than 0; fewer than 365 daily tifs for any variable; any write outside `$CD`
and `logs/`.

**To answer:** prepend one `### RESPONSE — pascal` (P11), run the IP grep, commit only this file, push, verify it
landed, end with a delivery line.

### RESPONSE — pascal 2026-11-17 — **P10 stopped at C3.** C0, C1 and C2 all pass, and the download is cheap: **six files, 1.56 GB, 169 s wall, 0.11 GiB peak RSS**. **Preprocess fails for every variable**: `terra::time()` on these `.nc` files returns a POSIXct with a time of day here, so `02` builds filenames like `pr_1995-01-01 12:00:00.tif`, and GDAL cannot create a file whose name contains colons — `[writeRaster] cannot write file`, exit 1, zero daily tifs. C4 and C5 were not run

The fault is in the daily filename, not in the data, the host or the environment: the same raster writes without complaint the moment the name is `pr_1995-01-01.tif`. It is also not merely a naming nicety — `04` composes its inputs as `paste0(pr_pth,'/pr_',dts,'.tif')` from `seq(as.Date(...), by = 'day')`, so a date-with-time name would miss every lookup in `04` even on a filesystem that accepted it. A one-line change in `02` fixes it, but the block says never improvise, so nothing was changed.

Everything written stays under `$CD`: the six raw `.nc` files and the CHIRPS extent file, 1,620,106,528 bytes in total. Nothing was installed (`Installing package` count 0 in every log), nothing was written to S3, and nothing outside `$CD` and `logs/`.

#### C0 — pull, limits, prerequisites. PASS

Branch `develop`, `git log --oneline -1` = `aef5a4e`, which is after `e98e312`.

| Check | Result |
|---|---|
| `cpu.max` | `2000000 100000` — 20 CPU, the Large profile as asked |
| `memory.max` | `103079215104` — 96 GiB |
| netCDF driver | present: `netCDF -raster,multidimensional raster,vector- (rw+vs)` |
| `/usr/bin/time` | present, `-v` reports `Maximum resident set size` |
| free space before | 50 T total, 26 T used, 24 T available, 53 % used |

**The CHIRPS file is the real one, but the block's `find` would have missed it.** `-maxdepth 3` returns nothing; the file is five levels down, under `global_daily/tifs/p05/1981/`. Searching without the depth limit finds it at once, both as a `.tif` and as a `.tif.gz`. I copied the real `.tif`. Had I run the command exactly as written I would have silently written the extent-only stand-in while the real file sat on the same disk, so **raise the `-maxdepth` in any later block that reuses this recipe**.

```
SpatExtent : -180, 180.00000536441803, -50.000001490116119, 50 (xmin, xmax, ymin, ymax)
dim 2000 x 7200   res 0.05
```

That is the expected global p05 extent, so `ext(msk)` in the ten `04` scripts gets the genuine CHIRPS extent, not an approximation.

#### C1 — both fixtures pass

| Fixture | Assertions | Wall | Result |
|---|---|---|---|
| `fixture_upstream_acquire_controls.R` | 25 | seconds | `ALL UPSTREAM ACQUIRE-CONTROLS FIXTURE ASSERTIONS PASSED` |
| `fixture_upstream_run_params.R` | 53 | 340 s | `ALL UPSTREAM RUN-PARAMETER FIXTURE ASSERTIONS PASSED` |

The block predicted 23 assertions for the first; it carries 25. Every one passes, so this is a count drift in the block, not a failure.

Worth noting for what follows: `fixture_upstream_run_params.R` proves the `04` code path on a **synthetic** grid, and it passes here. The `04` scripts are therefore not implicated in the C3 failure — they were never reached.

#### C2 — download. PASS

The dry run plans exactly six files, one per variable, into `$CD/nex-gddp-cmip6_raw`.

**Finding on the suffix.** The block expects every file to carry `_v1.1.nc` or `_v1.2.nc`. Three do and three do not:

```
pr       pr_day_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1995_v1.1.nc
tasmax   tasmax_day_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1995.nc
tasmin   tasmin_day_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1995.nc
hurs     hurs_day_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1995_v1.1.nc
rsds     rsds_day_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1995.nc
sfcWind  sfcWind_day_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1995.nc
```

I treated this as a finding and continued, because the enumerated stop list names only "the DRY_RUN plan not exactly 6 files", and the plan is exactly six, one per variable. The mixed suffix is what the bucket actually holds: NEX-GDDP-CMIP6 re-issues only the corrected variables with a version suffix, and the probe's 54 HEAD requests resolved each variable to the name that exists. All six then downloaded with no 404 and no retry, which confirms the resolution was right. If you intended the suffix itself as a gate, say so and I will stop on it next time.

Six `fetched` lines, bytes / seconds / rate:

| Variable | Bytes | s | MB/s |
|---|---:|---:|---:|
| pr | 222,298,070 | 39 | 5.70 |
| tasmax | 244,583,799 | 25 | 9.75 |
| tasmin | 246,232,669 | 24 | 10.28 |
| hurs | 277,190,299 | 28 | 9.79 |
| rsds | 270,314,540 | 26 | 10.49 |
| sfcWind | 301,834,906 | 26 | 11.78 |

Total 1,562,489,486 bytes, 6 `.nc` files, **169 s wall, 0.11 GiB peak RSS**. The block allowed "2-3 h, mostly download"; the download took under three minutes. The first file is slower than the rest, which is the usual connection warm-up, and the sustained rate is about 10 MB/s.

**Second finding, cheap to fix.** The download script sets up 30 local workers and `parallelly` objects:

```
Careful, you are setting up 30 localhost parallel workers with only 20 CPU cores available for this R
process (per 'cgroups2.cpu.max'), which could result in a 150% load.
```

The job is IO-bound, so the oversubscription costs little here, but the worker count is hard-wired rather than derived from the cgroup. On a shared box that is the same class of problem as `detectCores()`.

#### C3 — preprocess. FAIL, exit 1, zero tifs written. **Stop**

```
Error in `purrr::map()`:
i In index: 1.
Caused by error:
! [writeRaster] cannot write file
```

Index 1 is `pr`. Wall 118.7 s, **peak RSS 20.8 GiB**, exit status 1. Output per variable:

```
pr 0   tasmax 0   tasmin 0   hurs 0   rsds 0   sfcWind 0
```

The output directory tree is created, and it is empty. Nothing partial was left behind.

**Diagnosis.** Read-only probing against the downloaded `pr` file, in the same output directory:

- `nlyr` 365, `time()` has no NA, and `timeInfo()` reports step `seconds`, not `days`.
- `as.character(time(r))` yields `"1995-01-01 12:00:00"`, so line 67 builds `pr_1995-01-01 12:00:00.tif`.
- Writing a 4x4 raster to that exact name fails with the identical message; writing the same raster to `pr_1995-01-01.tif` in the same directory succeeds.
- A full single-layer slice of the real file — `rotate(r[[1]] * 86400)` — also writes without error to a colon-free name.

So the filesystem, the netCDF driver, the rotate, the unit conversion and the write path are all sound. The only thing GDAL refuses is the colon: a name of the form `pr_1995-01-01 12:00:00.tif` collides with GDAL's connection-string parsing, which reads `prefix:...:...` as a driver spec.

Both probe files were deleted afterwards; the output tree is empty again.

**Why this has not bitten CGlabs.** `02` has always used `as.character(terra::time(r))` — the v2_0 variant does the same at its lines 55 and 64. Whether that string carries a time of day depends on how the GDAL netCDF driver reports the time axis, and this host's GDAL 3.4.3 reports it in seconds. A host whose driver reports it as a date gets `1995-01-01` and never notices. This is a **second cross-host fault in GDAL 3.4.3**, independent of the sum-resample fault, and unlike that one it is not a wrong number — it is a hard stop, which is the better failure.

**Why a filesystem that accepted colons would not save it.** `04` builds its input names from dates, not from whatever `02` wrote — `calc_PTOT.R` line 43 does `seq(from = as.Date(...), by = 'day')` and line 46 does `paste0(pr_pth,'/pr_',dts,'.tif')`. Tifs named with a time of day would be invisible to all ten index scripts, and with `DAYS_CHECK=calendar` every one of them would stop on missing inputs. The date string in `02` has to be date-only for the pipeline to join up at all.

**The fix is one line**, in `get_daily_data()` in both `02` scripts, and it belongs in a macbook block, not here: take the date part of the timestamp rather than the whole of it, for example `format(as.Date(terra::time(r)), '%Y-%m-%d')`, with the `KACE-1-0-G` duplicate and leap-day handling applied to the same date-only vector. A fixture assertion that the generated name matches `^[a-z]+_[0-9]{4}-[0-9]{2}-[0-9]{2}\.tif$` would have caught it before any download. The existing fixtures do not test the filename, which is why they pass while the real run fails.

#### C4, C5 — not run

C4 needs the daily tifs. C5 is the measurement table and is given below for the stages that did run.

#### Measured so far

| Stage | Wall (s) | Peak RSS (GiB) | Files written | Bytes written |
|---|---:|---:|---:|---:|
| C1 acquire-controls fixture | ~3 | — | 0 | 0 |
| C1 run-params fixture | 340 | — | synthetic, in its own temp dir | 0 |
| C2 download | 169 | 0.11 | 6 | 1,562,489,486 |
| C3 preprocess | 119 | 20.8 | 0 | 0 |
| C4 ten indices | not run | — | — | — |

Free space before and after are the same to the printed precision: 50 T total, 24 T available, 53 % used. The 1.6 GB under `$CD` does not move the needle on this store.

**What the numbers already say about "regenerate, not transfer".** One GCM-year of raw input is 1.56 GB and takes under three minutes to fetch on this host. Eighteen GCMs across 20 historical plus 4 x 80 future years is about 6,000 GCM-years; at this rate that is roughly 9.4 TB of raw download and about 11 days of pure transfer if run strictly in series, and far less in parallel since the host is nowhere near its bandwidth limit at 10 MB/s on a single stream. The raw `.nc` files are also deletable once `02` has run. The transfer side of the migration is therefore not the obstacle; the preprocess and index stages, which are still unmeasured, are where the cost will be.

**Everything is in place for the rerun.** The six raw files and the CHIRPS extent file stay under `$CD`, so a block that fixes the date string can go straight to C3 and skip the download.

---

### MACBOOK 2026-11-17 — P10 (phase C): regenerate one GCM-year of monthly indices here, measured. **Large profile (20 CPU / 96 GiB). Allow 2-3 h, mostly download.**

**Why.** The migration plan rests on "regenerate, not transfer": a new host fetches the public NEX-GDDP daily data
and runs `hazards_upstream` 01→04 itself. Nobody has measured what that costs. P10 does it once, end to end, for
**ACCESS-ESM1-5, historical, 1995** (the AVAIL seed year, so a later water-balance run can continue from it), all six
variables, and the ten indices that need only the daily NEX inputs. Every stage is timed and sized.

Not run, and why:
- `fast_calc_NDWS / NDWL0 / NDWL50`: need `atlas_hazards/soils/{sscp,ssat}_world.tif`, which this host does not
  hold (S1 repro, 2026-10-27). They are a 23 MB must-transfer from CGlabs, queued for when CGlabs is free.
- `fast_calc_waterbalance`: needs `sroot_world.tif`, which no host holds (#45).
- `calc_TAI`: needs the `ET_SolRad` climatology, absent here.

The GDAL 3.4.3 sum-resample fault does not touch this: 02 uses `terra::rotate`, 04 uses `crop` and per-cell sums,
and nothing resamples.

**What changed in the code** (`e98e312`): the canonical download and preprocess scripts now honour
`SCENARIO / SSPS / GCMS / YRS / VARS` and `DRY_RUN=1`; a scoped download names its manifest by scope and stops if the
manifest does not cover the request; preprocess stops if any expected daily tif is missing after a file. The
fixture `R/checks/fixture_upstream_acquire_controls.R` runs the real scripts on synthetic data (C1 below).

**Layout.** `hazards_upstream` is a standalone producer: its `COMMON_DATA` is its own root with a fixed layout
(`nex-gddp-cmip6_raw/`, `nex-gddp-cmip6/`, `nex-gddp-cmip6_indices/`, `chirps_wrld/`). On this host use a **new**
root in the writable area, next to the other atlas roots:
```bash
export CD=<bulk>/atlas/hazards_upstream_pascal      # <bulk> as in the obs dispatches; never the top of Workspace
```
Every 04 script reads `chirps_wrld/chirps-v2.0.1981.01.01.tif` for its **extent only** (`xtd <- ext(msk)`; checked
in all ten). C0 provides that file: the real one if this host's CHIRPS holdings have it, else an extent-only stand-in
written by terra, with a README beside it saying so.

**Rules.**
- Writes go only under `$CD` and `logs/`. Nothing else on the bulk store is touched.
- No installs. Every package the three stages load is OK in your P9 audit (`purrr`, `furrr`, `future`, `dplyr`,
  `httr`, `tidyverse`, `terra`, `gtools`, `lubridate`). If any `Installing package` line appears, **stop**.
- No S3 writes. The NEX bucket is read anonymously over HTTPS.
- Every long step runs under `nohup` with a `.pid`, polled with `kill -0`, never `wait`. Wrap each stage in
  `/usr/bin/time -v` if C0 finds it; otherwise sample RSS as C0 describes.
- No usernames, hostnames or IPs in the RESPONSE. Write the root as `$CD`.

#### C0 — pull, identity, limits, the two prerequisites (minutes)
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD
git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD      # develop, at or after e98e312
CG=/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup); cat $CG/cpu.max $CG/memory.max  # expect 2000000 100000 and 96 GiB
gdalinfo --formats | grep -i netcdf                     # terra::rast(.nc) needs the system GDAL's netCDF driver
command -v /usr/bin/time && /usr/bin/time -v true 2>&1 | grep -i "maximum resident"   # peak-RSS tool
df -h $(dirname $CD) | tail -1                          # free space before
mkdir -p $CD/chirps_wrld $CD/nex-gddp-cmip6_raw logs
# the CHIRPS extent file: real if present in this host's CHIRPS tree, else an extent-only stand-in
CH=$(find <chirps_raw from metadata/hosts.json, afrilabs> -maxdepth 3 -name 'chirps-v2.0.1981.01.01.tif*' 2>/dev/null | head -1); echo "found: ${CH:-none}"
if [ -n "$CH" ]; then case "$CH" in *.gz) gunzip -c "$CH" > $CD/chirps_wrld/chirps-v2.0.1981.01.01.tif;; *) cp "$CH" $CD/chirps_wrld/;; esac; else
  Rscript -e 'library(terra); r <- rast(nrows=2000, ncols=7200, xmin=-180, xmax=180, ymin=-50, ymax=50, crs="EPSG:4326", vals=1); writeRaster(r, "'$CD'/chirps_wrld/chirps-v2.0.1981.01.01.tif", overwrite=TRUE, datatype="INT1U")'
  printf 'Extent-only stand-in for CHIRPS v2.0 1981-01-01 (global p05: -180..180, -50..50, 0.05 deg), all cells 1.\nhazards_upstream/R/04_indices reads this file for ext() only. Not CHIRPS data. Made %s for issue #58 P10.\n' "$(date -I)" > $CD/chirps_wrld/README_extent_only.txt; fi
Rscript -e 'library(terra); print(ext(rast("'$CD'/chirps_wrld/chirps-v2.0.1981.01.01.tif")))'   # -180, 180, -50, 50
```
If the netCDF driver is absent, **stop**: 02 cannot read the .nc files, and that is an IT item (it ships in
`libgdal30`, so absence would be a packaging fault). If `/usr/bin/time` is absent, sample RSS instead: start the
stage, then `while kill -0 $PID 2>/dev/null; do ps -o rss= -p $PID; sleep 15; done | sort -n | tail -1` in a second
command; report the max in GiB.

#### C1 — probes (minutes)
```bash
Rscript R/checks/fixture_upstream_acquire_controls.R 2>&1 | tail -3        # ALL ... PASSED (23)
Rscript R/checks/fixture_upstream_run_params.R        2>&1 | tail -2        # ALL ... PASSED (2-4 min)
```
Both must pass; either failing is a stop. The second one runs the real 04 scripts, water balance included, on a
synthetic grid with synthetic soils, so it proves the 04 code path here before any real data is spent.

#### C2 — download (one GCM-year, six files, about 1.2 GB; ~10 min at this host's S3 rate)
```bash
cd hazards_upstream/R/01_download_data
COMMON_DATA=$CD GCMS=ACCESS-ESM1-5 SCENARIO=historical YRS=1995 DRY_RUN=1 Rscript download_manual_nex_gddpCMIP6.R 2>&1 | tail -9
```
The URL probe runs here (54 HEAD requests). **Expect exactly 6 files listed**, one per variable, with a `_v1.1.nc`
or `_v1.2.nc` suffix. Anything else is a stop.
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup /usr/bin/time -v env COMMON_DATA=$CD GCMS=ACCESS-ESM1-5 SCENARIO=historical YRS=1995 Rscript download_manual_nex_gddpCMIP6.R > ../../../logs/p10_download_$STAMP.log 2>&1 &
echo $! > ../../../logs/p10_download_$STAMP.pid
# poll; then:
grep -E "fetched|Elapsed|Maximum resident" ../../../logs/p10_download_$STAMP.log
du -sb $CD/nex-gddp-cmip6_raw; find $CD/nex-gddp-cmip6_raw -name '*.nc' | wc -l      # 6
cd $ATLAS_PROJECT_DIR
```
Report the six `fetched` lines (bytes, seconds, MB/s each), the wall time and the total bytes.

#### C3 — preprocess (six .nc -> 6 x 365 daily tifs)
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup /usr/bin/time -v env COMMON_DATA=$CD GCMS=ACCESS-ESM1-5 SCENARIO=historical Rscript hazards_upstream/R/02_preprocess_data/preprocess_nex-gddp-cmip6_daily_data.R > logs/p10_preprocess_$STAMP.log 2>&1 &
echo $! > logs/p10_preprocess_$STAMP.pid
# poll; then:
grep -E "day\(s\)|rror|Elapsed|Maximum resident" logs/p10_preprocess_$STAMP.log
for v in pr tasmax tasmin hurs rsds sfcWind; do echo "$v $(ls $CD/nex-gddp-cmip6/$v/historical/ACCESS-ESM1-5 | wc -l)"; done   # 365 each
du -sb $CD/nex-gddp-cmip6
```
**Expect** six per-file lines each `365 day(s), 365 written, 0 present`, no `Error`, 365 tifs per variable. Fewer is
a stop (the script itself stops on a missing tif; if it did not, say so). Report wall, peak RSS, bytes.

#### C4 — ten indices, sequential, each timed
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
cat > logs/p10_indices_$STAMP.sh <<'SH'
set -u
cd "$ATLAS_PROJECT_DIR/hazards_upstream/R/04_indices"
for s in calc_PTOT calc_NDD calc_NTx calc_TAVG calc_TMAX calc_TMIN calc_HSH calc_THI calc_WBGT calc_PET; do
  echo "=== $s $(date +%H:%M:%S)"
  /usr/bin/time -v env COMMON_DATA=$CD GCMS=ACCESS-ESM1-5 SCENARIO=historical YRS=1995 DAYS_CHECK=calendar Rscript $s.R
  echo "=== $s exit=$? $(date +%H:%M:%S)"
done
SH
nohup bash logs/p10_indices_$STAMP.sh > logs/p10_indices_$STAMP.log 2>&1 &
echo $! > logs/p10_indices_$STAMP.pid
# poll; then:
grep -E "^=== |Elapsed|Maximum resident|daily inputs missing|rror" logs/p10_indices_$STAMP.log
for d in $CD/nex-gddp-cmip6_indices/historical_ACCESS-ESM1-5/*/; do echo "$(basename $d) $(find $d -name '*.tif' | wc -l)"; done
du -sb $CD/nex-gddp-cmip6_indices; df -h $(dirname $CD) | tail -1
```
`DAYS_CHECK=calendar` makes a missing input day a stop rather than a silent under-count. **Expect** every script
`exit=0`, no `daily inputs missing`, and at least 12 tifs under every index directory (`NTx` holds 31 threshold
subdirectories of 12). Anything else is a finding; an `exit` other than 0 is a stop.

#### C5 — report
One table, one row per stage (download, preprocess, each of the ten indices): wall seconds, peak RSS (GiB), files
written, bytes written. Plus: the six per-file download rates; `df` before and after; the C0 answers (netCDF
driver, `/usr/bin/time`, CHIRPS real or stand-in). The extrapolation to 18 GCMs x (20 historical + 4 x 80 future)
years is macbook work; just give the measured numbers.

**Keep everything under `$CD`.** It is the seed for a later water-balance continuation and for the value
comparison against CGlabs' copy of the same GCM-year.

**Stop and describe** on any of: not on `develop`; a fixture failing; no netCDF driver; the DRY_RUN plan not
exactly 6 files; `Installing package` in any log; an `exit` other than 0; fewer than 365 daily tifs for any
variable; any write outside `$CD` and `logs/`.

**To answer:** prepend one `### RESPONSE — pascal` (P10), run the IP grep, commit only this file, push, verify it
landed, end with a delivery line.

### RESPONSE — pascal 2026-11-17 — **P9 complete, P9.0 to P9.5, no stop rule fired. The five load from the site library, the personal library is now empty, and `wbkernel` and `trendkernel` both compile and load.** The widened audit is clean where it had to be — `0.4-chain` 28/0/0, `both` 16/0/0 — and the new scopes found **two genuinely missing packages for the bakes: `doFuture` and `jrvFinance`**

The scope fix earned its keep on the first run. `bake` is where the remaining gap is, and neither package has ever appeared in an audit of this host before. Nothing was installed to paper over it, as the block says.

Nothing was installed except the two kernel packages the block names, nothing went anywhere but `R_LIBS_USER`, nothing ran as root, and `R/0_server_setup.R` was not sourced.

#### P9.0 — pull

Branch `develop`, `git log --oneline -1` = `8a31a3f`.

#### P9.1 — the five load from the shared library. Gate PASS

```
MetBrewer      0.2.0      /usr/local/lib/R/site-library/MetBrewer
miceadds       3.20.10    /usr/local/lib/R/site-library/miceadds
paws.storage   0.11.0     /usr/local/lib/R/site-library/paws.storage
s3fs           0.1.7      /usr/local/lib/R/site-library/s3fs
sfarrow        0.4.1      /usr/local/lib/R/site-library/sfarrow
```

All five load with `R_LIBS_USER` pointed at a path that does not exist, and all five come from the site library. The versions are identical to the personal copies they were shadowing, so nothing changes behaviour by removing them.

**`paws.common` also resolves from the site library**, version 0.9.0, so the personal copy was an orphan rather than a dependency the site build needed.

#### P9.2 — personal library emptied. PASS

Before, six directories:

```
MetBrewer  miceadds  paws.common  paws.storage  s3fs  sfarrow
```

`remove.packages()` on the five named packages left `paws.common` alone, as the block anticipated. Since P9.1 had already shown `paws.storage` loading from the site library with the personal library hidden, and `paws.common` itself resolving from site, I removed it too.

After: **the personal library is empty** — `ls -1A` prints nothing. Re-checking all six with the personal library back in play, every one resolves from `/usr/local/lib/R/site-library`. The 2026-10-28 ABI break that forced the per-user rebuild is now fully unwound on this node.

#### P9.3 — `wbkernel` and `trendkernel` installed. PASS

Both compiled from this checkout into `R_LIBS_USER`, in one background job, about 50 s in total.

```
* DONE (wbkernel)
* DONE (trendkernel)
```

Compiler `g++ (Ubuntu 11.4.0-1ubuntu1~22.04.3) 11.4.0`, `-std=gnu++20`, against the site `Rcpp` headers. No errors, no warnings. The only notice is `No man pages found in package 'trendkernel'`, which is a packaging nicety, not a fault.

Entry points present:

```
wbkernel    0.2.0 TRUE /home/.../R/x86_64-pc-linux-gnu-library/4.6/wbkernel
trendkernel 0.1.0 TRUE /home/.../R/x86_64-pc-linux-gnu-library/4.6/trendkernel
```

`TRUE` is `exists()` on `eabyep_kernel_cpp` and `mk_sen_cpp` in each namespace. So the personal library is no longer a workaround holding shadow copies; it now holds exactly the two packages that only exist in this repository, which is what it is for.

#### P9.4 — the widened audit

90 files, **79 distinct packages**, 83.2 s. `.libPaths()` is personal, then `/usr/local/lib/R/site-library`, then `/usr/lib/R/site-library`, then the base library.

```
OK 69   BROKEN 0   MISSING 10   (of 79)
  0.4-chain OK 28  BROKEN 0  MISSING 0
  bake      OK 7   BROKEN 0  MISSING 2
  obs       OK 0   BROKEN 0  MISSING 0
  both      OK 16  BROKEN 0  MISSING 0
  upstream  OK 18  BROKEN 0  MISSING 8
```

**BROKEN 0 across the whole chain.** The invariants hold: `0.4-chain` and `both` are clean, and `upstream` MISSING is exactly the predicted eight, `wbkernel` having moved to OK.

MISSING, in full:

```
aws.s3           upstream   1:upload_AWS.R
climate4R.UDG    upstream   1:download_rawCMIP6.R
doFuture         bake       2:1.2_create_isimip_timeseries.R;1_make_timeseries.R
downscaleR       upstream   1:download_rawCMIP6.R
inborutils       upstream   1:download_cropCalendar.R
jrvFinance       bake       1:4.1_roi_econ_ind.R
loadeR           upstream   1:download_rawCMIP6.R
transformeR      upstream   1:download_rawCMIP6.R
tsbox            upstream   2:preprocess_nex-gddp-cmip6_daily_data_v2_0.R;preprocess_nex-gddp-cmip6_daily_data.R
visualizeR       upstream   1:download_rawCMIP6.R
```

**The two new ones are the finding.** Neither has been visible to any audit of this host until now:

- **`doFuture`** — `R/1_make_timeseries.R` and `R/1.2_create_isimip_timeseries.R`. That is the parallel backend for the R/1 timeseries build, so it blocks the first bake, not a download script.
- **`jrvFinance`** — `R/4.1_roi_econ_ind.R`. Financial functions for the ROI indicators, so it blocks R/4.1.

Both are ordinary CRAN packages. I installed neither, per the block. Whoever schedules the bakes will need them, and they belong in the same `R_LIBS_USER` install as the kernels, or in a request to IT for the shared library if several users will run the bakes.

The other seven `bake` rows are fine and all from site: `Hmisc` 5.3.0, `RColorBrewer` 1.1.3, `Rcpp` 1.1.2, `reshape2` 1.4.5, `treemap` 2.4.4, `trend` 1.1.10, plus `trendkernel` from `user`.

**`obs` reads 0/0/0, and that is a real result, not an empty scan.** Scanning `R/obs/*.R` on its own finds six packages — `arrow`, `data.table`, `parallel`, `sf`, `stats`, `terra` — and every one of them is already required by the 0.4 chain or by `hazards_upstream`, so the classifier files them under the earlier scope. **The observed pipeline introduces no package that the rest of the chain does not already need.** For the migration that is the best possible answer: S1 to S7 adds nothing to the R environment it has to carry, and the Python side is the `obs` conda env, which the audit does not cover.

Rows still reading `lib = user`: exactly two, both expected.

```
trendkernel      OK   bake       0.1.0   1:2.1_create_monthly_haz_tables.R
wbkernel         OK   upstream   0.2.0   2:fast_calc_NDWS.R;fast_calc_waterbalance.R
```

#### P9.5 — the two packages this repository ships

```
trendkernel  OK  bake      0.1.0  user  1:2.1_create_monthly_haz_tables.R
wbkernel     OK  upstream  0.2.0  user  2:fast_calc_NDWS.R;fast_calc_waterbalance.R
```

`trendkernel` appears in an audit on this node for the first time, exactly as the scope fix predicted. Phase C's water-balance kernel and R/2.1's Theil-Sen kernel are both resolvable here now.

#### What this leaves for the migration

The R environment is no longer a blocker for phase C: `0.4-chain`, `both` and `upstream` carry no BROKEN rows, and the only `upstream` gaps are download and plotting scripts outside 01→04. The bakes are a different matter — `doFuture` and `jrvFinance` have to be installed before R/1 or R/4.1 can run here, and that was invisible until the audit's scope was widened. Worth noting in the migration plan against items 13 and 16.

**Logs** (untracked, not committed): `logs/p9_install_20261117_142546.log`, `logs/r_env_audit_p9_20261117_142612.{log,tsv}`.

---

### MACBOOK 2026-11-16 — P9: finish the PASCAL R environment (five shared packages confirmed, personal library emptied, `wbkernel` and `trendkernel` installed). **Small profile is fine. Minutes.**

**Why.** P8 closed the GDAL question, and the IT ticket now carries it. The remaining PASCAL gap is the R
environment: IT installed five packages on 2026-11-12 that the personal library still shadows, and two packages
this repository ships as source (`wbkernel`, `trendkernel`) have never been installed there. `wbkernel` is what
phase C needs — `hazards_upstream/R/04_indices/fast_calc_NDWS.R:165` calls `wbkernel::eabyep_kernel_cpp` — and
`trendkernel` is what `R/2.1_create_monthly_haz_tables.R:1070` calls for the Theil-Sen kernel. Neither can be
sourced into an environment; both are called through their namespace, so both must be installed packages.

**Rules.**
- **Do not source `R/0_server_setup.R`.** Its `pacman::p_load()` installs.
- Installs go into `R_LIBS_USER` only (`~/R/x86_64-pc-linux-gnu-library/4.6`). Never the site or system library,
  never as root.
- The only deletions permitted are the five named packages in P9.2, and only after P9.1 passes.
- Track anything backgrounded by polling the pid. **Never use `wait` or a long blocking loop.**
- No usernames, hostnames or IPs in the RESPONSE.

#### P9.0 — pull
```bash
cd <repo> && git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD   # develop
```

#### P9.1 — do the five IT packages load from the shared library?
Hide the personal library, then load each one in its own fresh R. This is the same technique P6 used.
```bash
for p in MetBrewer miceadds paws.storage s3fs sfarrow; do
  R_LIBS_USER=/nonexistent Rscript --no-environ --no-init-file -e "
    suppressPackageStartupMessages(library($p));
    cat(sprintf('%-14s %-10s %s\n', '$p', as.character(packageVersion('$p')),
                dirname(dirname(getNamespaceInfo('$p','path')))))" 2>&1 | tail -2
done
```
Report the version and the library path for each. `paws.common` is a dependency of `paws.storage`; if it resolves
from the site library, say so, and if it does not, name where it came from.

**Gate:** all five load, and the path printed is the site library, not the personal one.
**Stop** if any of the five fails to load, or loads from somewhere other than the site library. Delete nothing in
that case, and report what you see — that is an IT follow-up, not something to fix on the node.

#### P9.2 — empty the personal library of the five
Only after P9.1 passes. Pete has given GO for exactly these removals.
```bash
L=$(Rscript -e "cat(Sys.getenv('R_LIBS_USER'))")
ls -1 "$L"                                        # before
Rscript -e "remove.packages(c('MetBrewer','miceadds','paws.storage','s3fs','sfarrow'), lib = Sys.getenv('R_LIBS_USER'))"
ls -1 "$L"                                        # after
```
`paws.common` may be left behind as an orphan. If `paws.storage` loads from the site library with the personal
`paws.common` hidden (P9.1 already shows this), remove `paws.common` too and say so; otherwise leave it and report
that it is still needed.

#### P9.3 — install the two kernel packages from this checkout
Both have compiled code, so this takes a minute or two each and needs the build toolchain. Run them one after the
other in the background, log to `logs/`, and poll the pid.
```bash
L=$(Rscript -e "cat(Sys.getenv('R_LIBS_USER'))")
S=$(date +%Y%m%d_%H%M%S)
nohup bash -c "R CMD INSTALL wbkernel --library=$L && R CMD INSTALL trendkernel --library=$L" \
  > logs/p9_install_$S.log 2>&1 &
```
Then verify, with the personal library in play this time, that each loads and its entry point is present:
```bash
Rscript -e "
  library(wbkernel);    cat('wbkernel   ', as.character(packageVersion('wbkernel')),
                            exists('eabyep_kernel_cpp', where = asNamespace('wbkernel')), '\n')
  library(trendkernel); cat('trendkernel', as.character(packageVersion('trendkernel')),
                            exists('mk_sen_cpp', where = asNamespace('trendkernel')), '\n')"
```
**Stop** if either install fails to compile. Quote the first error line and the compiler message under it; do not
try to install build tools.

#### P9.4 — re-run the environment audit, now over the whole production chain
`76_r_env_audit.R` had a scope gap, fixed in this same commit. It always read `pkg::` references, but its default
scope was only `R/0.4*.R` and `hazards_upstream/R/**`, so **R/1–R/4 and `R/obs` were never audited on any host**.
The default scope now also covers `R/1*.R`, `R/2*.R`, `R/3*.R`, `R/4*.R` and `R/obs/*.R`: 90 files and 79 distinct
packages, against 70 before. The summary gains two rows, `bake` and `obs`.
```bash
S=$(date +%Y%m%d_%H%M%S)
nohup Rscript R/checks/76_r_env_audit.R --out logs/r_env_audit_p9_$S.tsv > logs/r_env_audit_p9_$S.log 2>&1 &
```
**Expected, as invariants rather than counts:**
- `0.4-chain` and `both` clean — BROKEN 0, MISSING 0.
- `upstream` MISSING drops by one as `wbkernel` moves to OK, leaving the eight that belong to download and plotting
  scripts outside the 01→04 chain: `aws.s3`, `climate4R.UDG`, `downscaleR`, `inborutils`, `loadeR`, `transformeR`,
  `tsbox`, `visualizeR`. Not a phase C blocker.
- `bake` and `obs` are **new information**. Nobody has audited them on this host, so whatever they report is the
  finding — including MISSING rows. Do not install anything to make them clean.

Report the summary counts for all five scopes, the full MISSING list per scope, and any row whose `lib` column
still reads `user`.

#### P9.5 — the two packages this repository ships
Both should now read OK, from `user`:
- `trendkernel`, scope `bake`, used by `2.1_create_monthly_haz_tables.R`;
- `wbkernel`, scope `upstream`, used by `fast_calc_NDWS.R` and `fast_calc_waterbalance.R`.

Quote those two rows from your TSV. `trendkernel` appears in an audit on this host for the first time, because of
the scope fix in P9.4.

**Either outcome in P9.1 and P9.5 is a finding, not a stop, and so are the `bake` and `obs` rows in P9.4.** The stop
rules are the ones named in P9.1, P9.2 and P9.3.

**To answer:**
1. Prepend one `### RESPONSE — pascal` (P9) to this file.
2. Run the IP grep from `AGENTS.md` §3.
3. Commit only this file, then push to `develop`.
4. Verify it landed (`git log origin/develop..HEAD` empty).
5. End with a delivery line.

**After P9:** phase C is next, on a Large profile, but **not until Pete says go**.

---

### RESPONSE — pascal 2026-11-13 — P8: **confirmed, GDAL 3.4.3 is the fault.** The block's synthetic case passes on both GDALs, so it does not reproduce; the real input does, and the trigger is **source NoData under `-r sum`**. `gdalwarp -r sum` on the same file, same host: GDAL 3.4.3 max 7399, GDAL 3.13.3 max 9

The block as written returns "both are right", which would point at terra. That reading would have been wrong. The synthetic raster has no NoData, and NoData is exactly what GDAL 3.4.3's sum warper mishandles. Four short extra checks, all read-only and all in the temp directory, separate the two and land on GDAL 3.4.3. The IT ask is a GDAL upgrade, as the macbook expected.

No stop rule fired. Nothing was installed, the only `apt` calls were `apt-cache policy`, scratch files stayed in `mktemp -d` directories that are now deleted, and the only file written outside them is the log named at the end.

#### P8.0 — pull

Branch `develop`, `git log --oneline -1` = `dab4df3`.

#### P8.1 — versions

```
gdalinfo --version      GDAL 3.4.3, released 2022/04/22
gdal-config --version   3.4.3
obs env gdalinfo        GDAL 3.13.3 "Iowa City", released 2026/08/13
```

Packages (`dpkg -l`):

```
gdal-bin       3.4.1+dfsg-1build4
libgdal28      3.2.1+dfsg-1~focal0
libgdal30      3.4.3+dfsg-1~focal0
libproj-dev    8.2.1-1
libproj19      7.2.1-1~focal0
libproj22      8.2.1-1
proj-bin       8.2.1-1
proj-data      8.2.1-1
```

`apt-cache policy libgdal-dev` shows `Installed: 3.4.3+dfsg-1~focal0`, `Candidate: 3.4.3+dfsg-1~focal0`, and the only version the configured apt sources offer is **3.4.1+dfsg-1build4** from `jammy/universe`. `gdal-bin` is the same story: installed 3.4.1, candidate 3.4.1.

**This matters for the ticket.** The installed GDAL 3.4.3 carries a `~focal0` suffix, so it did not come from the distribution's own jammy archive, and the archive's own candidate is *older* than what is installed. A plain `apt upgrade` cannot raise GDAL here; it would offer a downgrade. The upgrade needs a repository that actually carries a newer GDAL, or the library that the `obs` conda environment already holds.

#### P8.2 — the same warp, two GDALs, one host

The block's command, run as written on a 2160 × 4320 raster of ones with no NoData:

```
== /usr/bin: GDAL 3.4.3, released 2022/04/22
    STATISTICS_MINIMUM=9  STATISTICS_MEAN=9  STATISTICS_MAXIMUM=9
== obs env: GDAL 3.13.3 "Iowa City", released 2026/08/13
    STATISTICS_MINIMUM=9  STATISTICS_MEAN=9  STATISTICS_MAXIMUM=9
```

Both correct. On its own this is the block's second branch — "the fault is in how terra uses GDAL 3.4.3". It is not, and the next four checks show why.

#### P8.3 — terra on the same synthetic raster is also correct

`terra::resample(ones, 0.25-degree grid, method = "sum")`, terra 1.9.50, GDAL 3.4.3, same file:

```
terra resample(sum): min 9  max 9  mean 9  sum 5184000      (expected sum 1440 x 400 x 9 = 5184000)
```

So neither tool is wrong on the synthetic case. **The synthetic case does not reproduce the fault at all**, which means it cannot assign it. The difference has to be in the input.

#### P8.4 — the real input reproduces it, and `gdalwarp` alone is enough

The P7 probe builds its ones mask from the GLW raster: `one <- g * 0 + 1`, which carries GLW's NoData (`nan`) and its geometry (`ymin -89.9999999999999`, a 1e-13 degree offset from the base grid). Writing exactly that raster to the temp directory and warping it to the base grid, same host, same file, same command:

| | terra 1.9.50 resample | `gdalwarp -r sum` GDAL 3.4.3 | `gdalwarp -r sum` GDAL 3.13.3 |
|---|---|---|---|
| dst sum | 2709066 | — | — |
| max per cell | **7399** | **7399** | **9** |
| mean | — | 17.30 | 8.548 |

The `7399` reproduces through plain `gdalwarp`, with no terra in the picture. **The fault is in GDAL's warper, and it is gone in GDAL 3.13.3 on this same machine.** terra 1.9.50 is exonerated, which also agrees with the macbook running terra 1.9.50 correctly on GDAL 3.8.5.

#### P8.5 — the trigger is NoData, not the misalignment

Two controls separate the two candidate differences between the synthetic raster and the real one.

- **Misalignment alone, no NoData.** The same synthetic ones raster rebuilt with GLW's `ymin -89.9999999999999`: GDAL 3.4.3 gives 9 / 9 / 9, GDAL 3.13.3 gives 9 / 9 / 9. The 1e-13 degree offset is harmless.
- **NoData alone.** The real ones mask through GDAL 3.4.3 again, with `-srcnodata None` so the warper ignores the mask: **9 / 9 / 9**. The same file and the same command, differing only in whether the source NoData mask is active, is correct without it and reads 7399 with it.

So: **`-r sum` on GDAL 3.4.3 is wrong whenever the source has a NoData mask.** Every GLW and MapSPAM input has one, which is why phase B saw it on every livestock artefact and nowhere else.

#### P8.6 — the obvious workaround does not work

Substituting the NoData away inside terra before the resample, `subst(cnt, NA, 0)` then `resample(method = "sum")`, on GDAL 3.4.3:

```
NA->0 workaround: src 1525236453  dst 2905279253  ratio 1.9048    (macbook dst 1452641015, ratio 0.9524)
NA->0 ones:       dst 2709066  max 7399
```

Unchanged. Replacing the values does not clear the NoData flag that terra writes with the raster, and the flag is what drives the warper. The only thing that fixed it at this GDAL version was `-srcnodata None`, which is a `gdalwarp` command-line option that terra does not expose.

That leaves two usable routes for 0.4.1 on an unupgraded host, both for the macbook to choose:

1. **Upgrade GDAL.** The clean fix, and the one the ticket should ask for. The `obs` conda environment on this host already has 3.13.3, so a correct GDAL exists here; the R build is what is linked to 3.4.3.
2. **Avoid the warper for this one call.** P7 already proved `aggregate(fact = 3, fun = "sum")` reproduces the macbook's total to the digit (1452641015), because it never enters GDAL's warper. It only works where the ratio of resolutions is an exact integer, which it is for GLW at 1/12 degree onto the 0.25 degree grid.

#### Summary for the IT ticket

GDAL 3.4.3, as shipped on this node, computes `-r sum` resampling incorrectly when the source raster carries a NoData mask: a destination cell that must sum at most 9 source cells returns up to 7399. The same command on the same file under GDAL 3.13.3 on the same machine returns the correct 9. The distribution's own apt sources only offer GDAL 3.4.1, which is older than what is installed, so the upgrade needs a repository that carries a current GDAL. No pipeline code change can work around it: the fault is below terra, and the `-srcnodata None` escape is not reachable from R.

**Log:** `logs/p8_gdal_sum_20261113_143321.log` (untracked, not committed). Both `mktemp -d` directories were deleted. Nothing else was written.

---

### MACBOOK 2026-11-13 — P8: confirm on this host that GDAL 3.4.3 is the sum-resample fault (read-only, minutes). **Small profile is fine.**

**Why.** P7 was excellent. It ruled out PROJ and the inputs. On the macbook, terra **1.9.50**, the same version you
run, gives the correct result on GDAL 3.8.5 (max 9 per cell, ratio 0.9524). So the fault follows **GDAL 3.4.3**.
P8 confirms that on this host alone: run the same `gdalwarp -r sum` with the system GDAL and with the `obs`
env's GDAL 3.13, on the same machine and the same file. If only the system GDAL is wrong, the IT ask is a GDAL
upgrade. (The `R/obs` and `python/obs` code has no sum-resample, so #31 is not affected.)

**Rules.**
- Read-only. Scratch files go in a `mktemp -d` directory, deleted at the end; logs go in `logs/`.
- No installs. No `apt` actions beyond the read-only `apt-cache policy`.
- No usernames, hostnames or IPs in the RESPONSE.

#### P8.1 — versions
```bash
cd <repo> && git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD   # develop
gdalinfo --version; gdal-config --version 2>/dev/null
~/.conda/envs/obs/bin/gdalinfo --version
dpkg -l | grep -E "^ii +(libgdal[0-9]+|gdal-bin|libproj[0-9]+) " | awk '{print $2, $3}'
apt-cache policy libgdal-dev | head -8          # which GDAL versions the configured apt sources offer
```

#### P8.2 — the same warp, two GDALs, one host
```bash
T=$(mktemp -d)
Rscript -e "library(terra); r <- rast(nrows=2160, ncols=4320, xmin=-180, xmax=180, ymin=-90, ymax=90, vals=1); writeRaster(r, '$T/ones.tif', datatype='FLT4S')"
for G in /usr/bin ~/.conda/envs/obs/bin; do
  $G/gdalwarp -q -r sum -te -180 -50 180 50 -tr 0.25 0.25 -ot Float32 $T/ones.tif $T/out_$(basename $(dirname $G)).tif
  echo "== $G: $($G/gdalinfo --version)"
  $G/gdalinfo -stats $T/out_$(basename $(dirname $G)).tif | grep -E "STATISTICS_(MAXIMUM|MEAN|MINIMUM)"
done
rm -rf "$T"
```
**What to expect:** the correct answer is MIN = MEAN = MAX = **9**. One raster cell at 1/12° feeds each 0.25° cell
3 × 3. The macbook (GDAL 3.11) prints 9 / 9 / 9.
- If `/usr/bin` (3.4.3) is wrong and the `obs` env (3.13) prints 9, the fault is GDAL 3.4.3's warper, proven on this
  host.
- If both are right, the fault is in how terra uses GDAL 3.4.3. Say so; the next step is then different.

Either result is a finding, not a stop.

**Stop and describe** on any of:
- not on `develop`;
- anything written outside the temp dir or `logs/`.

**To answer:**
1. Prepend one `### RESPONSE — pascal` (P8) to this file.
2. Run the IP grep.
3. Commit only this file, then push.
4. Verify it landed.
5. End with a delivery line.

### RESPONSE — pascal 2026-11-12 — P7: the 2x is the **`method = "sum"` resample**, not PROJ. Inputs now **PASS 13/13**. Phase B run completed; every output difference traces to that one resample.

Phase B has its answer. All five FAOSTAT CSVs now match the macbook, so the comparison is finally a host test rather than an input test, and it isolates a single cause:

**`terra::resample(method = "sum")` on terra 1.9.50 with GDAL 3.4.3 over-counts.** The probe shows it directly, and every differing artefact at both resolutions traces back to it. Nothing else differs. The FAOSTAT-derived economics match the macbook exactly.

No stop rule fired. P7.0 to P7.5 all ran. Nothing was installed, there were no S3 writes, and writes stayed inside the places listed under Rules.

---

#### P7.0 — pull

Branch `develop`, `git log --oneline -1` = `cc4fdcd`.

#### P7.1 — probe: the resample, not PROJ

```
[16:28:21] terra 1.9.50  GDAL 3.4.3  PROJ 8.2.1
[16:28:22] GLW grid 2160x4320x1 res 0.083333333,0.083333333  base grid 400x1440x1 res 0.25,0.25
[16:28:23] density sum      20384475
[16:28:23] cellSize sum     510065622   (expect ~5.10e8 km2)
[16:28:23] count sum (src)  1525236453
[16:28:24] resampled (dst)  2905279253   ratio dst/src 1.9048
[16:28:25] ones: src 2094082  dst 2709066  max cell 7399  (expect max 9)
[16:28:25] aggregate(fact=3) sum 1525236453   cropped to base band 1452641015
[16:28:25] done
```

Read against the macbook, line by line:

| | macbook | pascal | |
|---|---|---|---|
| `cellSize sum` | 510065622 | **510065622** | identical |
| `count sum (src)` | 1525236453 | **1525236453** | identical |
| `resampled (dst)` | 1452641015 | **2905279253** | **1.99999x** |
| ratio `dst/src` | 0.9524 | 1.9048 | |
| `ones … max cell` | 9 | **7399** | |
| `aggregate(fact=3)` cropped | 1452641015 | **1452641015** | identical |

**PROJ is cleared.** `cellSize sum` is identical to the digit, so PROJ 8.2.1 computes the same cell areas as PROJ 9.5.1. The source count is identical too, so the inputs and the grids agree.

**The resample is the cause**, and it is worse than a factor of two. The `ones` test is the decisive line: a raster of 1s resampled with `method = "sum"` should put at most 9 source cells into one destination cell at this ratio, and the macbook gets exactly 9. Here one destination cell receives **7399**. The total happens to come out near 2x, but the per-cell distribution is badly wrong, not uniformly doubled.

The last line is the proof that nothing else is at fault: `aggregate(fact = 3)`, which does the same summation by a different code path, gives **1452641015** — the macbook's resampled total, to the digit. The data, the grids and the arithmetic are all correct here. Only `resample(method = "sum")` is not.

This is a finding, as the dispatch says, not a stop. It is worth raising upstream with terra: a reproducible `ones` case where `method = "sum"` violates its own invariant on GDAL 3.4.3.

#### P7.2 — personal library cleaned

The eight named packages were removed from `R_LIBS_USER`. Six remain, exactly the expected set:

```
   MetBrewer     miceadds  paws.common paws.storage         s3fs      sfarrow 
     "0.2.0"    "3.20-10"      "0.9.0"     "0.11.0"      "0.1.7"      "0.4.1"
```

Re-audit, 79.4 s:

```
[2026-11-12 16:30:02] +  79.4s | OK 61   BROKEN 0   MISSING 9   (of 70)
[2026-11-12 16:30:02] +  79.4s |   0.4-chain OK 28  BROKEN 0  MISSING 0
[2026-11-12 16:30:02] +  79.4s |   both      OK 13  BROKEN 0  MISSING 0
[2026-11-12 16:30:02] +  79.4s |   upstream  OK 20  BROKEN 0  MISSING 9
```

`0.4-chain` is BROKEN 0 MISSING 0, so no stop. All eight removed packages now report `site` in the `lib` column:

```
arrow        OK  both       26.0.1    site
data.table   OK  0.4-chain  1.18.6.1  site
dplyr        OK  both       1.2.1     site
duckdb       OK  0.4-chain  2.0.0     site
geoarrow     OK  0.4-chain  0.4.4     site
```

`base64enc`, `rlang` and `vctrs` are not direct references in the 70, so they do not appear as rows; they resolve from the shared library as dependencies and nothing BROKEN remains. Only five rows still read `user`: `MetBrewer`, `miceadds`, `paws.storage`, `s3fs`, `sfarrow`, plus `paws.common` which no scanned file names directly.

The nine MISSING are unchanged and all `upstream`: `aws.s3`, `climate4R.UDG`, `downscaleR`, `inborutils`, `loadeR`, `transformeR`, `tsbox`, `visualizeR`, `wbkernel`.

**IT's rebuild also moved a number of versions** that were not part of the brief, which matters for anyone comparing against an older fingerprint: `countrycode` 1.6.1 to 1.9.0, `DBI` 1.2.3 to 1.3.0, `fs` 1.6.6 to 2.1.0, `furrr` 0.3.1 to 0.4.0, `future` 1.69.0 to 1.82.0, `ggplot2` 4.0.1 to 4.0.3, `progressr` 0.18.0 to 1.1.0, `wbstats` 1.1 to 1.2, `xml2` 1.5.2 to 1.6.0, among others.

#### P7.3 — FAOSTAT refreshed

```
INFO  | acquire 'faostat-bulk' method=http-zip -> <working_dir>/Data/fao
INFO  |   pinned manifest: 9 object(s), 84,053,732 bytes (mutable origin)
INFO  |   9 object(s): 9 fetched (0 re-fetched), 0 already present, 486,749,544 bytes
INFO  |   verified: hash 0, size only 9, drift 0, no manifest 0
```

43.9 s. No `verification failed`, no missing member after unzip, and **`drift 0`** — the sizes still match the manifest listing, so FAO published nothing new between the macbook's fetch this morning and this one. 486,749,544 bytes unpacked, close to the 300 MB the dispatch estimated.

```
PASS     n=1    <working_dir>/Data/fao/Prices_E_All_Data_(Normalized).csv
PASS     n=3    <working_dir>/Data/boundaries/*.parquet
READY
```

`71_stage_ready.R` now checks the world prices file, as `f5d0939` intended, and it is present at 214,286,652 bytes. The P5 stop is closed.

#### P7.4 — 0.4.1 at both resolutions

Both resolutions completed:

```
===== 0.4.1_create_livestock_exposure.R COMPLETE at 2026-11-12 16:33:13 EAT =====
===== 0.4.1_create_livestock_exposure.R COMPLETE at 2026-11-12 16:36:15 EAT =====
```

`grep -il install` over both logs returned nothing; `pacman` installed nothing.

The mass-conservation lines:

```
res-25  [0.4.1] mass not conserved on GLW resample:
          poultry=1.8928  sheep=1.9041  pigs=1.7754  goats=1.9926  cattle=1.9048
res-05  [0.4.1] mass not conserved on GLW resample:
          poultry=0.2535  sheep=0.6956  pigs=0.0945  goats=0.7698  cattle=0.4183
res-05  [0.4.1] mass not conserved on livestock_no resample:
          poultry=1.6052  sheep=1.6035  pigs=1.6055  goats=1.6040  cattle=1.6049  total=1.6047
```

res-25 is unchanged from the partial P5 run, and still 2.0008 to 2.0018 times the macbook's five ratios. res-05 is **1.6036 to 1.6071** times the macbook's, matching the `livestock_no resample` line's 1.6047 exactly. So the over-count factor depends on the grid: about 2.0 when downsampling to 0.25, about 1.605 when going to 0.05. That is consistent with a per-cell distribution fault rather than a constant scaling, which is what the `ones … max cell 7399` line already showed.

#### P7.5 — fingerprints and compare

```
metadata/crosshost/inputs_041_pascal.tsv      26 rows  (13 artefact + 13 _env)
metadata/crosshost/fp_041_pascal_res-25.tsv   28 rows  (15 artefact + 13 _env)
metadata/crosshost/fp_041_pascal_res-05.tsv   28 rows  (15 artefact + 13 _env)
```

All three at the expected counts.

**Step 1, inputs: PASS.**

```
env-diff=9  env-same=4  same=13
PASS - every shared artefact matches; no one-sided artefacts
```

All thirteen match, the eight pinned inputs and now **all five FAOSTAT CSVs**. Both hosts hold the same FAOSTAT release, so step 3 is a genuine host test for the first time.

**Step 2, the GLW-only host gate.** Mixed, and the pattern is informative.

| artefact | status | max rel diff |
|---|---|---|
| `livestock_masks_res-05.tif` | **same** | — |
| `shoat_prop_res-05.tif` | **same** | — |
| `livestock_number_number_res-25.tif` | VALUE-DIFF | 2.39e-06 |
| `livestock_masks_res-25.tif` | VALUE-DIFF | 0.0372 |
| `shoat_prop_res-25.tif` | VALUE-DIFF | 0.0422 |
| `livestock_number_number_res-05.tif` | VALUE-DIFF | 0.378 |

Reading each against P7.1:

- **The two res-05 masks are byte-identical to the macbook**, shapes, sums and `n_nonNA` all equal. They are built from boundaries and thresholds, not from resampled mass, so the fault does not reach them.
- **`livestock_number_number_res-05`** is the fault at full strength. Its layer sums are 1.6036 to 1.6059 times the macbook's, matching the `livestock_no resample` ratio exactly, while `n_nonNA` is identical layer for layer. The coverage is right; the values are inflated.
- **`livestock_number_number_res-25`** agrees to 2.39e-06, only just over the 1e-6 tolerance. Here the national-total normalisation divides the over-count out, so the per-pixel values come back correct.
- **The two res-25 intermediates differ by coverage, not by value.** `livestock_masks_res-25` has `n_nonNA` 41338 against the macbook's 41660, and `shoat_prop_res-25` 34935 against 36196. The broken resample changes which cells carry non-zero mass, so a few hundred cells fall the other side of the mask threshold. The 0.0372 and 0.0422 are the layer sums moving with those cell counts.

**Step 3, VoP.** With FAOSTAT identical, these are now a real host test, and they are nearly clean:

| artefact | res-25 | res-05 |
|---|---|---|
| `glw4-2020_vop_intld15-2015` | 0.000115 | 0.00207 |
| `glw4-2020_vop_intld15-2020` | 0.000108 | 0.00237 |
| `glw4-2020_vop_intld15-2021` | 0.000115 | 0.00234 |
| `glw4-2020_vop_nominal-usd-2015` | 0.000241 | 0.00229 |
| `glw4-2020_vop_nominal-usd-2020` | 0.000190 | 0.00295 |
| `glw4-2020_vop_nominal-usd-2021` | 0.000196 | 0.00288 |
| the six `-prices-used.csv` | 0.377 | 0.377 |

The six VoP rasters differ by 1e-4 at res-25 and 2e-3 at res-05. VoP is value times a per-pixel proportion, so the over-count mostly cancels in the proportion and leaves only a residue — three to four orders of magnitude smaller than the raw resample error.

**The six `-prices-used.csv` files deserve their own line, because the 0.377 is misleading.** They have identical shape on both hosts, `nrow=495 ncol=9`, and three of their four numeric columns are **identical to the digit**:

```
             macbook            pascal
value        77901741410        77901741410        same
basis_ratio  82387.06497        82387.06497        same
basis_median 689.3551896        689.3551896        same
glw3_no      6814140876         10939962880        ratio 1.6054
```

Only `glw3_no`, the GLW animal count carried into the table, differs, and it differs by **1.6054** — the resample factor again. **The FAOSTAT-derived economics are identical across hosts.** The prices, the deflator ratios and the medians all match; nothing in the FAOSTAT path is host-dependent. That is the result phase B was built to establish, and it holds.

**So every single differing artefact, at both resolutions, reduces to the one broken `method = "sum"` resample.** There is no second cause, no platform arithmetic difference, and no FAOSTAT difference.

#### `env-diff` rows, identical for all three comparisons

```
arrow        mac 24.0.0                 pascal 26.0.1
data.table   mac 1.18.4                 pascal 1.18.6.1
GDAL         mac 3.8.5                  pascal 3.4.3
git          mac 6471ecd                pascal cc4fdcd
platform     mac aarch64-apple-darwin23 pascal x86_64-pc-linux-gnu
PROJ         mac 9.5.1                  pascal 8.2.1
R            mac 4.6.0                  pascal 4.6.1
sf           mac 1.1.2                  pascal 1.1.3
terra        mac 1.9.34                 pascal 1.9.50
```

Four match: `dplyr` 1.2.1, `rlang` 1.3.0, `tidyr` 1.3.2, `vctrs` 0.7.3. `arrow` now reads 26.0.1 rather than 26.0.0, because P7.2 removed the personal copy that was shadowing the shared one.

**The two rows that matter are `GDAL` and `terra`.** P7.1 cleared PROJ, and the remaining suspects for the resample fault are GDAL 3.4.3 against 3.8.5, or terra 1.9.50 against 1.9.34, or the pair. Worth separating, and it can be done without a host: run the same probe on this node against a terra 1.9.34 build, or on the macbook against terra 1.9.50. If the ratio follows terra, it is a package regression; if it follows GDAL, this node's GDAL 3.4.3 is the limit and is worth adding to the IT ticket, since 3.4.3 shipped with Ubuntu 22.04 and is several years old.

#### What this means

- **Phase B has succeeded at what it was for.** It found a genuine, reproducible cross-host value difference, isolated it to one function call, cleared every other candidate, and showed that the FAOSTAT economics are host-independent.
- **The res-25 `livestock_number_number` output is trustworthy on this node** at 2.39e-06, because the normalisation cancels the fault. Nothing else produced here from a sum-resample should be used until the cause is fixed: specifically `livestock_number_number_res-05`, which is inflated by about 1.605, and the `glw3_no` column of the `-prices-used.csv` files.
- **A workaround exists and is already proven by the probe**: `aggregate(fact = 3)` reproduces the macbook's resampled total to the digit for the res-25 case. That is not a general substitute for `resample`, since it only works at integer factors, but it is a direction worth testing if the terra fix is slow in coming.
- **Nothing here needs re-running on this node** until the macbook decides which of terra or GDAL to pin.

#### Logs kept

`logs/probe_058_20261112_162815.log`, `logs/r_env_audit_p7_20261112_162900.{tsv,log,pid}`, `logs/p7_fao_refresh_20261112_163100.{log,pid}`, `logs/phaseB_041_20261112_163130.{log,pid}`, `logs/phaseB_041_0.25_20261112_163130.log`, `logs/phaseB_041_0.05_20261112_163130.log`.

---

### MACBOOK 2026-11-12 — P7: probe the 2x mass ratio, clean the personal library, refresh FAOSTAT, re-run phase B. **Small profile is fine.** Pete's GO (2026-11-12) on the cleanup and the FAOSTAT refresh.

**Why.** Thank you for P6 and the careful P5 stop. Four things follow from it, done in this order so 0.4.1 runs on
the final stack:
1. **The 2x GLW mass ratio.** It is either the source mass (`cellSize`, PROJ 8.2.1) or the `method = "sum"`
   resample (terra 1.9.50 on GDAL 3.4.3). `R/checks/probe_058_sum_resample.R` tells the two apart in seconds; the
   macbook reference is in its header.
2. **The personal library.** IT's rebuild made 8 of its 14 packages redundant, and one of them (`arrow`) shadows a
   newer shared copy. Remove those 8, so the run uses the shared library.
3. **FAOSTAT.** The macbook re-fetched its FAOSTAT on 2026-11-12 with the same recipe. Fetch it here too, so both
   hosts hold the current release. If FAO publishes a new release between the two fetches, the inputs compare will
   show it.
4. **Phase B again.** 71 now checks the world prices file (`f5d0939`, `metadata/stages.json`), and the macbook
   has re-run 0.4.1 on the refreshed FAOSTAT and re-committed its fingerprints.

**Rules.**
- Writes go only to:
  - `R_LIBS_USER`, and only to remove the 8 packages named below;
  - `<working_dir>/Data/fao`, through the recipe;
  - the 0.4.1 output directories;
  - `logs/`;
  - the three `metadata/crosshost/*_pascal*` files.
- No installs. No S3 writes.
- Background jobs with `nohup` and a `.pid` file, polled with `kill -0`, never `wait`.
- No usernames, hostnames or IPs in the RESPONSE or the TSVs.

#### P7.0 — pull
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD
git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD    # develop
```

#### P7.1 — probe (read-only, seconds)
```bash
Rscript R/checks/probe_058_sum_resample.R 2>&1 | tee logs/probe_058_$(date +%Y%m%d_%H%M%S).log
```
Report all of it verbatim. The macbook printed:
- `cellSize sum 510065622`;
- `count sum (src) 1525236453`;
- `resampled (dst) 1452641015`, ratio `0.9524`;
- `ones … max cell 9`;
- `aggregate(fact=3)` cropped sum equal to the resampled sum.

To read your result:
- **If `cellSize`/`src` is about half the macbook's,** PROJ is the cause.
- **If `dst` or the ones `max cell` is about double,** the resample is, and then the res-25 livestock outputs below
  will not match the macbook. That mismatch is the finding phase B exists for.

Either way, this is a finding, not a stop.

#### P7.2 — remove the 8 redundant personal-library packages, then re-audit
```bash
Rscript -e 'l <- Sys.getenv("R_LIBS_USER"); rm <- c("arrow","base64enc","data.table","dplyr","duckdb","geoarrow","rlang","vctrs"); remove.packages(rm, lib = l); print(installed.packages(lib.loc = l)[, "Version"])'
STAMP=$(date +%Y%m%d_%H%M%S)
nohup Rscript R/checks/76_r_env_audit.R --out logs/r_env_audit_p7_$STAMP.tsv > logs/r_env_audit_p7_$STAMP.log 2>&1 &
echo $! > logs/r_env_audit_p7_$STAMP.pid
```
**What to expect:**
- 6 packages left in the personal library: `MetBrewer`, `miceadds`, `paws.common`, `paws.storage`, `s3fs`, `sfarrow`;
- the audit shows `0.4-chain` BROKEN 0 MISSING 0;
- the `lib` column shows `site` for the 8 removed packages.

If any 0.4-chain row is BROKEN or MISSING, **stop**. Do not reinstall into the personal library.

#### P7.3 — refresh FAOSTAT (verified fetch through the recipe)
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup Rscript -e 'source("R/00_paths.R"); source("R/00_acquire.R"); atlas_acquire("faostat-bulk", force = TRUE)' > logs/p7_fao_refresh_$STAMP.log 2>&1 &
echo $! > logs/p7_fao_refresh_$STAMP.pid
# poll; then:
grep -E "object|verified|drift|WARN|rror" logs/p7_fao_refresh_$STAMP.log
Rscript R/checks/71_stage_ready.R --stage 0.4.1 2>&1 | tail -3      # READY, now including the world prices file
```
FAOSTAT is declared mutable, so a `drift` count (file sizes changed since the manifest was listed) is expected and is
not a failure. A `verification failed` line, or a missing member after unzip, is a stop. About 300 MB unpacked.

#### P7.4 — run 0.4.1, both resolutions
Same command as P5.1, with a new stamp. `FORCE_OVERWRITE=1` replaces the partial res-25 masks P5 left behind.
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup bash -c 'for RES in 0.25 0.05; do EXPOSURE_RES=$RES FORCE_OVERWRITE=1 Rscript -e "source(\"$PWD/R/0_server_setup.R\"); source(\"$PWD/R/0.4.1_create_livestock_exposure.R\")" > logs/phaseB_041_${RES}_'$STAMP'.log 2>&1 || exit 1; done' > logs/phaseB_041_$STAMP.log 2>&1 &
echo $! > logs/phaseB_041_$STAMP.pid
# poll: kill -0 $(cat logs/phaseB_041_$STAMP.pid) 2>/dev/null && echo running || grep -hE "COMPLETE|mass not conserved|rror" logs/phaseB_041_*_$STAMP.log
```
Expect one `COMPLETE` per resolution. Report both `mass not conserved` lines. A missing `COMPLETE` is a stop.

#### P7.5 — fingerprint, compare, commit
```bash
M=metadata/crosshost; F="Rscript R/checks/72_crosshost_fingerprint.R"
$F --stage 0.4.1-inputs --out $M/inputs_041_pascal.tsv
$F --stage 0.4.1 --res res-25 --out $M/fp_041_pascal_res-25.tsv
$F --stage 0.4.1 --res res-05 --out $M/fp_041_pascal_res-05.tsv
$F --compare $M/inputs_041_mac.tsv    $M/inputs_041_pascal.tsv    2>&1 | cut -c31-
$F --compare $M/fp_041_mac_res-25.tsv $M/fp_041_pascal_res-25.tsv 2>&1 | cut -c31-
$F --compare $M/fp_041_mac_res-05.tsv $M/fp_041_pascal_res-05.tsv 2>&1 | cut -c31-
```
Read the comparison in the same order as P5.3:
1. **Inputs.** All 13 should be `same`, now including the 5 FAOSTAT CSVs.
2. **The GLW-only host gate:** `livestock_number_number`, `livestock_masks`, `shoat_prop`.
3. **VoP.** The six rasters and the six CSVs. These are a host test only if step 1 shows FAOSTAT `same`.

Report the summary lines, every non-`same` row with its `max_rel_diff`, and every `env-diff` row. Tie any
step-2 difference to what P7.1 found.

**Stop and describe** on any of:
- not on `develop`;
- a 0.4-chain BROKEN or MISSING row after P7.2;
- `verification failed` in P7.3;
- a missing `COMPLETE`;
- any write outside the places listed under Rules.

Differences found by the compare are **findings to report**, not stops.

**To answer:**
1. Prepend one `### RESPONSE — pascal` (P7) to this file.
2. Run the IP grep over the response and the TSVs.
3. Commit this file plus the three `*_pascal*` TSVs, and nothing else.
4. Push, then verify it landed.
5. End with a delivery line.

### RESPONSE — pascal 2026-11-11 — P6: IT's rebuild verified, **588 failures down to 7**. P5: **STOPPED at P5.1**, a FAOSTAT world-prices file 0.4.1 needs is absent and `71_stage_ready.R` does not check for it.

Two results, in the order the 2026-11-11 note asks for.

**P6 is good news.** The shared library, tested as an ordinary account with the personal library excluded, now loads 1,671 of 1,678 packages. `arrow`, `geoarrow` and `duckdb` are present, built under R 4.6.1, and all three load. A draft reply to IT is at the end of this block.

**P5 stopped at a stop rule.** 0.4.1 failed partway through res-25 on a missing input, so there is no `COMPLETE` line, res-05 never started, and the two output fingerprints do not exist. The inputs fingerprint was produced and compared, because that is what describes the stop. The eight hash-pinned inputs are `same` as the macbook's. No fix was improvised.

---

## P6 — shared-library retest

#### Account, R version and library paths

Run as an ordinary account, not root. The active R is **4.6.1**, so the "if 4.6.1 is unavailable, stop there" branch does not apply.

```
R version 4.6.1 (2026-06-24) -- "Happy Hop"
```

Library paths with the personal library excluded (`R_LIBS_USER` pointed at a path that does not exist, plus `--no-environ --no-init-file`):

```
/usr/local/lib/R/site-library
/usr/lib/R/site-library
/usr/lib/R/library
```

The personal library normally sits first, ahead of those three. It was left in place and untouched; it was only hidden for the duration of these tests.

#### The seven packages IT asked about

Each loaded in its own fresh R with the personal library hidden, so every result below is the shared library's own:

```
rlang       1.3.0     built R 4.6.1   LOADS
data.table  1.18.6.1  built R 4.6.1   LOADS
base64enc   0.1-6     built R 4.6.1   LOADS
RGtk2       2.20.36   built R 4.1.0   FAILS  undefined symbol: Rf_EncodeReal (in RGtk2)
arrow       26.0.1    built R 4.6.1   LOADS
geoarrow    0.4.4     built R 4.6.1   LOADS
duckdb      2.0.0     built R 4.6.1   LOADS
```

**One correction for IT, in their favour.** The note said `arrow`, `geoarrow` and `duckdb` were installed under R 4.5.6. On disk their `DESCRIPTION` files record `Built: R 4.6.1`, and all three load under R 4.6.1. Whatever the build pipeline reported, the result in the shared library is correct and working. Nothing needs redoing for those three.

`RGtk2` is the only one of the seven still failing, and it is unchanged from 2026-11-05: still built under R 4.1.0, still missing `Rf_EncodeReal`.

#### Full sweep, against the 2026-11-05 baseline

`R/checks/77_r_lib_sweep.R`, read-only, 6 workers, 632.9 s. Log and TSV kept in `logs/`.

```
[2026-11-11 08:51:36] +   3.1s | R 4.6.1; library has 1678 package(s); 6 worker(s)
[2026-11-11 08:51:36] +   3.1s |   built under R 4.1   compiled   38  pure-R  103
[2026-11-11 08:51:36] +   3.1s |   built under R 4.5   compiled   12  pure-R    8
[2026-11-11 08:51:36] +   3.1s |   built under R 4.6   compiled  620  pure-R  897
[2026-11-11 09:02:06] + 632.9s | loaded OK 1671   BROKEN 7   (of 1678)
[2026-11-11 09:02:06] + 632.9s |   direct (own shared object fails): 2
[2026-11-11 09:02:06] + 632.9s |   via another package's shared object: 2
[2026-11-11 09:02:06] + 632.9s |   missing dependency: 2   other: 1
[2026-11-11 09:02:06] + 632.9s | ROOTS (direct failures) and the symbol each lacks:
[2026-11-11 09:02:06] + 632.9s |   pryr                   built R 4.5  undefined symbol: EXTPTR_PROT
[2026-11-11 09:02:06] + 632.9s |   RGtk2                  built R 4.1  undefined symbol: Rf_EncodeReal
[2026-11-11 09:02:06] + 632.9s | knock-on failures by root:
[2026-11-11 09:02:06] + 633.0s |   RGtk2                  2 package(s)
[2026-11-11 09:02:06] + 633.0s | undefined symbols among roots: EXTPTR_PROT=1  Rf_EncodeReal=1
```

Side by side:

| | 2026-11-05 | 2026-11-11 |
|---|---|---|
| packages | 1,653 | 1,678 |
| load OK | 1,065 | **1,671** |
| BROKEN | 588 | **7** |
| direct roots | 4 | **2** |
| built under R 4.6 | **0** | **1,517** |

1,517 of 1,678 packages are now built under R 4.6, against none a week ago. That is the whole of the fix, and it worked.

#### The seven that still fail, split as requested

**Genuine failures (4)** — these are compiled against an older R and were not rebuilt:

```
RGtk2          built 4.1  direct     undefined symbol: Rf_EncodeReal
pryr           built 4.5  direct     undefined symbol: EXTPTR_PROT
gWidgetsRGtk2  built 4.1  via:RGtk2  undefined symbol: Rf_EncodeReal
rggobi         built 4.1  via:RGtk2  undefined symbol: Rf_EncodeReal
```

`RGtk2` is the root of three of the four. It binds to GTK2, which is long out of support, and it was already irrelevant to this pipeline in the P3 report. `pryr` is a developer convenience package. **Neither blocks any Atlas work**, and neither needs rebuilding on our account.

**Absent dependencies, not failures (2)** — the package itself is fine; something it needs was never installed:

```
ANCOMBC     there is no package called 'CVXR'
mlr3proba   there is no package called 'mlr3'
```

**Version skew, not an ABI problem (1)**:

```
climateR    object '%>%' is not exported by 'namespace:sf'
```

`climateR` expects an older `sf` that re-exported the pipe. The shared `sf` is now 1.1.3 and does not. This is a package-version mismatch, not a compilation problem, and it would be fixed by updating `climateR` rather than rebuilding anything.

**None of the seven is used by this pipeline.** Everything the 0.4 chain and `hazards_upstream` 01 to 04 reference now loads from the shared library.

#### What this means for the personal library

The personal library holds 14 packages, installed on 2026-11-05 as a workaround. Comparing each against the rebuilt shared library:

```
arrow         user 26.0.0    site 26.0.1    site is newer; the personal copy now shadows it
base64enc     user 0.1-6     site 0.1-6     same
data.table    user 1.18.6.1  site 1.18.6.1  same
dplyr         user 1.2.1     site 1.2.1     same
duckdb        user 2.0.0     site 2.0.0     same
geoarrow      user 0.4.4     site 0.4.4     same
rlang         user 1.3.0     site 1.3.0     same
vctrs         user 0.7.3     site 0.7.3     same
MetBrewer     user 0.2.0     site ABSENT    still needed
miceadds      user 3.20-10   site ABSENT    still needed
paws.common   user 0.9.0     site ABSENT    still needed
paws.storage  user 0.11.0    site ABSENT    still needed
s3fs          user 0.1.7     site ABSENT    still needed
sfarrow       user 0.4.1     site ABSENT    still needed
```

Seven are now redundant, one (`arrow`) actively shadows a newer shared copy, and six are still only in the personal library. **The personal library was left intact**, as instructed, pending this verification. The tidy-up, once Pete is happy, is to delete the eight redundant or shadowing entries and ask IT to add the remaining six to the shared library; they are all on CRAN.

#### Draft reply to IT — for Pete's review, not sent

> Subject: R 4.6.1 shared-library rebuild — verified on our side, with two leftovers
>
> Thank you for the rebuild, and for the detail in your reply. We have re-tested on an ordinary user account against system R 4.6.1 and the shared site library, with our personal R library excluded so that nothing of ours could mask the result. The improvement is clear and we are no longer blocked.
>
> **Measured results.** Before the rebuild, 588 of 1,653 packages in the shared library failed to load; none had been built under R 4.6. After it, 7 of 1,678 fail and 1,517 are built under R 4.6. The packages you asked us to check — `arrow`, `geoarrow` and `duckdb` — are present and all three load. One small correction, in your favour: their `DESCRIPTION` files record `Built: R 4.6.1`, not 4.5.6, so whatever the build log reported, what is installed is correct.
>
> **The seven that still fail**, for completeness, none of which blocks our work:
> - `RGtk2`, `gWidgetsRGtk2`, `rggobi` — `RGtk2` is still built under R 4.1 and is missing the symbol `Rf_EncodeReal`; the other two fail only because they load it. `RGtk2` depends on GTK2, which is out of support, so we would suggest retiring it rather than rebuilding it.
> - `pryr` — still built under R 4.5, missing `EXTPTR_PROT`. A developer convenience package; worth a rebuild only if someone asks for it.
> - `ANCOMBC` and `mlr3proba` — not build problems. Each needs a package that is not installed: `CVXR` and `mlr3` respectively.
> - `climateR` — not a build problem either. It expects an older `sf` that re-exported the `%>%` pipe; the current `sf` 1.1.3 does not. Updating `climateR` would fix it.
>
> **Two requests, both about notice rather than action.**
> 1. Please do let us have advance notice of R upgrades, and of routine shared-library rebuilds as well. The upgrade on 2026-10-26 took our pipeline out for about ten days, and a day's warning would have let us pin our dependencies first. Freshservice notification would suit us well, thank you for offering it.
> 2. When a rebuild happens, a one-line note of which R version the library was built against would let us check compatibility in minutes rather than re-running a full sweep.
>
> **A few points from your reply we would still value an answer on.**
> - *Idle culling.* You confirmed that background jobs survive while the Hub server stays running. Could you tell us what makes the server stop — is there an idle timeout, and does an unattended long-running job count as activity? Some of our ingests run for many hours with no interactive session attached, and we need to know whether to expect them to be killed.
> - *Session sizing.* Thank you for confirming 20 CPUs and 96 GB per session with four sessions available. Our current profile gives us 8 CPUs and 32 GB. Could you tell us how to select the larger profile, or whether it needs to be enabled for the account?
> - *Remote compute access.* Is there a supported way to reach this environment other than through the browser — SSH, or a scheduled batch submission? Several of our jobs would be better run as scheduled work than in a notebook session.
> - *The slow external route.* No action needed for now. We have since measured 2.5 MB/s from our object store on the same node, roughly twice what we get elsewhere, so the slowness we reported looks specific to that one external source rather than to the network here. We will come back with a concrete trace if it recurs.
>
> Thanks again — the rebuild has unblocked the work.

---

## P5 — phase B: STOPPED at P5.1

**Which R stack P5 ran on:** the shared library **plus** the personal library, in the normal order with the personal library first. The `_env` rows in the inputs TSV record the versions that were actually used. Only `arrow` differs between the two libraries, where the personal 26.0.0 shadowed the shared 26.0.1; for every other `_env` package the two libraries now hold the same version, so the stack P5 ran on is effectively the rebuilt shared library.

#### P5.0 — readiness and pinned inputs

Branch `develop`, `git log --oneline -1` = `c3aa381`.

`71_stage_ready.R --stage 0.4.1` reported **READY**.

Pinned-input verification:

```
glw4-2020            ok=6   PASS
afr-highlands        ok=1   PASS
boundaries-gaul2024  FAIL - 1 object(s) missing or wrong
```

The boundaries failure was a size mismatch on one file, `atlas_gaul24_a0_africa.parquet`, local 31,935,988 bytes against a manifest 28,780,999. P5.0 names the remedy, so it was applied: `atlas_require("boundaries-gaul2024")`, a verified fetch, which re-fetched that one object and left the other two alone.

```
WARN  |   wrong size, re-fetching: …/atlas_gaul24_a0_africa.parquet (31,935,988 bytes, manifest 28,780,999)
INFO  |   3 object(s): 1 fetched (1 re-fetched), 2 already present, 169,268,377 bytes
INFO  |   verified: hash 1, size only 2, drift 0, no manifest 0
```

Re-verify: `ok=3`, `PASS`. **All three pinned records then verified clean**, so this was not a stop. Worth noting for the record that a local boundaries copy had drifted from the origin and nobody had spotted it; the manifest caught it.

#### P5.1 — the stop

0.4.1 was launched exactly as written, both resolutions in one background job with a `.pid`, polled with `kill -0`. **It did not reach `COMPLETE`.** res-25 failed partway through, and because the loop carries `|| exit 1`, res-05 never started.

```
[09:04:12] [0.4.1] loading FAOSTAT data (VoP, prices, production)
Error in fread(fao_econ_file_world) : 
  File 'Data/fao/Prices_E_All_Data_(Normalized).csv' does not exist or is non-readable.
Execution halted
```

**A missing `COMPLETE` is a listed stop rule, so P5 stops here and nothing was improvised.** In particular no FAOSTAT bulk fetch was attempted: that would have pulled the current FAOSTAT release and changed the very inputs phase B is comparing, turning a clean finding into a muddled one.

`grep -il install` over both run logs returned nothing. **`pacman` installed nothing**, as P4 intended.

**The finding underneath the stop.** `R/0_server_setup.R:576` defines

```r
fao_econ_file_world <- file.path(fao_dir, "Prices_E_All_Data_(Normalized).csv")
```

That file is not in `Data/fao` on this node. What is there is `Prices_E_Africa_NOFLAG.csv`, `Prices_E_Africa.csv`, the deflators and the production files — the Africa cut, not the world file. And `71_stage_ready.R --stage 0.4.1` checks `Prices_E_Africa_NOFLAG.csv`, `Production_Crops_Livestock_E_Africa_NOFLAG.csv`, `Deflators_E_All_Data_(Normalized).csv` and the boundaries parquets, but **not** the world prices file. So 71 reports READY for a stage that cannot run.

That is a gap in the readiness check rather than a problem with this node, and it is worth fixing centrally: 71 should check every file 0.4.1 actually opens. `72_crosshost_fingerprint.R:150` already lists `Prices_E_All_Data_(Normalized).csv` among the five FAOSTAT inputs it fingerprints, so the two checks already disagree about what 0.4.1 needs.

**The one value that did come out before the stop**, reported because the dispatch asks for it as a cross-host number:

```
[0.4.1] mass not conserved on GLW resample:
  poultry=1.8928  sheep=1.9041  pigs=1.7754  goats=1.9926  cattle=1.9048
```

Against the macbook's res-25 ratios of `poultry=0.9460, sheep=0.9513, pigs=0.8871, goats=0.9954, cattle=0.9520`, **every one of these is almost exactly twice the macbook's** — the five ratios of pascal to macbook are 2.0008, 2.0016, 2.0014, 2.0018 and 2.0008. A factor that constant across five independent species is not a numerical difference; it points at the denominator, meaning the global GLW source mass the warning divides by, rather than at the resampling. It is reported as a finding and left alone. It may well resolve once 0.4.1 runs to completion, but it should be looked at either way.

#### What was written, and what was not

Only one partial output exists, inside the permitted 0.4.1 output tree:

```
Data/GLW4_2020/intermediate/livestock_masks_res-25.tif   610,455 bytes
Data/GLW4_2020/processed/                                empty
```

The run log records `livestock_masks.tif exists=FALSE` before writing, so nothing was overwritten. **These res-25 intermediates are incomplete and must not be used**; the next clean run with `FORCE_OVERWRITE=1` will replace them.

Nothing was written outside the permitted places. No S3 writes. The personal R library is intact.

#### P5.2 — fingerprints

Only the inputs fingerprint could be produced, since the two output fingerprints need outputs that do not exist.

```
metadata/crosshost/inputs_041_pascal.tsv    25 rows  (12 artefact + 13 _env)
metadata/crosshost/fp_041_pascal_res-25.tsv NOT PRODUCED - run stopped
metadata/crosshost/fp_041_pascal_res-05.tsv NOT PRODUCED - run stopped
```

25 rather than the expected 26, and the missing row is exactly the missing file: `Data/fao/Prices_E_All_Data_(Normalized).csv`. The other 12 artefact rows and all 13 `_env` rows are present.

#### P5.3 — inputs compare against the macbook

```
env-diff=9  env-same=4  ONLY-A=1  same=8  SHAPE-DIFF=4
```

**Step 1, the pinned inputs: PASS.** All eight are `same` — the six GLW tifs, the highlands file and the admin0 boundaries. These are the inputs the host gate depends on, and they are byte-identical to the macbook's. The #58 manifests did their job, including catching the drifted boundaries file first.

**The five FAOSTAT CSVs all differ:**

```
SHAPE-DIFF Data/fao/Prices_E_Africa_NOFLAG.csv
ONLY-A     Data/fao/Prices_E_All_Data_(Normalized).csv
SHAPE-DIFF Data/fao/Production_Crops_Livestock_E_Africa_NOFLAG.csv
SHAPE-DIFF Data/fao/Value_of_Production_E_Africa.csv
SHAPE-DIFF Data/fao/Value_of_Production_E_All_Area_Groups.csv
```

So this node holds a **different FAOSTAT release** from the macbook's 2026-05-14 snapshot, not merely a different value here and there — the shapes differ, meaning different row counts. By the dispatch's own rule this is an input difference, not a host difference, and had 0.4.1 completed, the six `glw4-2020_vop_*` rasters and the `-prices-used.csv` files could not have matched the macbook. **Step 3 of the comparison was therefore never going to be a host test on this node as it stands.** Whoever runs phase B next should decide whether to pin FAOSTAT across hosts first; otherwise only the three GLW-derived outputs can carry the gate.

**Step 2, the host gate, could not be evaluated**, since `livestock_number_number_res-*`, `livestock_masks_res-*` and `shoat_prop_res-*` were never produced.

#### `env-diff` rows, all nine

```
arrow        mac 24.0.0                 pascal 26.0.0
data.table   mac 1.18.4                 pascal 1.18.6.1
GDAL         mac 3.8.5                  pascal 3.4.3
git          mac 3ccda81                pascal c3aa381
platform     mac aarch64-apple-darwin23 pascal x86_64-pc-linux-gnu
PROJ         mac 9.5.1                  pascal 8.2.1
R            mac 4.6.0                  pascal 4.6.1
sf           mac 1.1.2                  pascal 1.1.3
terra        mac 1.9.34                 pascal 1.9.50
```

Four `_env` rows match: `dplyr` 1.2.1, `rlang` 1.3.0, `tidyr` 1.3.2 and `vctrs` 0.7.3.

**The `terra` and `sf` numbers have moved since the dispatch was written.** P5 anticipated terra 1.8.93 and sf 1.0.24 here; IT's rebuild has taken them to **terra 1.9.50** and **sf 1.1.3**, now *ahead* of the macbook's 1.9.34 and 1.1.2 rather than behind. GDAL 3.4.3 and PROJ 8.2.1 are system libraries and did not change. The raster stack is therefore closer across hosts than phase B assumed, which makes the eventual value comparison more meaningful, not less.

#### To finish P5

One thing is needed, and it is a decision rather than a command: how to obtain `Prices_E_All_Data_(Normalized).csv` on this node without changing the other four FAOSTAT files, since those four already differ from the macbook's and fetching the bulk record would move them again. Options, for the macbook to choose between:

1. **Pin FAOSTAT.** Add the five CSVs to a `#58` manifest with hashes, as GLW and boundaries already are, and have every host fetch that pinned set. This makes the VoP outputs a real host gate instead of an input comparison, and it is the only option that makes phase B say what it was built to say.
2. **Fetch the current bulk release here** and accept that all five FAOSTAT inputs differ from the macbook's, so only the three GLW-derived outputs carry the gate.
3. **Copy the macbook's five CSVs** to this node, which would make the FAOSTAT inputs identical, at the cost of being a manual step outside the acquire layer.

Option 1 is the one worth the effort. Say which you want and P5 can be re-run; the 0.4.1 run itself is 2 to 3 minutes.

Separately, `71_stage_ready.R` should be taught about the world prices file so that READY means ready. That is a one-line change in the 0.4.1 stage definition, and it is not PASCAL's to make.

#### Logs kept

`logs/r_lib_sweep_20261111_p6.{log,tsv,pid}`, `logs/phaseB_041_20261111_090330.{log,pid}`, `logs/phaseB_041_0.25_20261111_090330.log`, `logs/p5_require_boundaries_20261111.log`.

---

### MACBOOK 2026-11-09 — P6: PASCAL shared R library retest and support reply (#58)

**Order (added 2026-11-11, formatting fix only):** run this P6 block **first**, then P5 below. P5 is unchanged.
Record which R stack P5 then runs on: the shared library only, or the shared library plus the personal library.
Its `_env/*` rows capture the versions either way.

IT reports recompilation and says arrow, geoarrow and duckdb were installed under R 4.5.6. Please verify on PASCAL using an ordinary account, system R 4.6.1 and the shared site library. Exclude the personal R library for this test and record the active version and library paths (redact personal paths). If 4.6.1 is unavailable, report which version is active and stop there.

Run the existing read-only R/checks/77_r_lib_sweep.R, compare with 588 failures among 1,653 packages, and explicitly test rlang, data.table, base64enc, RGtk2, arrow, geoarrow and duckdb. Record failures and absent packages separately, with concise errors. Leave the personal workaround intact pending verification.

IT also offered Freshservice notification after upgrades, said bash jobs should survive while the Hub server stays running, confirmed 20 CPUs / 96 GB per session with four sessions, reported no proxy or firewall limit, and could not explain the slow JRC route. Prepare a short editable ticket reply for Pete's review. Thank IT, summarize measured results, ask for advance notice before upgrades and routine shared-library rebuilds, and cover remaining unanswered points (including idle culling and remote compute access) where relevant. Draft only. Add results in a RESPONSE block.


### MACBOOK 2026-11-06 — P5 (phase B, PASCAL half): run R/0.4.1 at both resolutions, fingerprint inputs and outputs, value-compare against the macbook. **Small profile is fine.**

**Why.** P4 made the 0.4 chain load here, so phase B can start. Phase B turns the 2026-09-23 cross-host gate, which
compared shape only and only inputs, into a **value** gate on real outputs. R/0.4.1 is unchanged since CGlabs last ran
it (`931e5a7`, 2026-11-02, both resolutions, for #54), and its run takes 1-3 min. `72_crosshost_fingerprint.R` gained
three things:
- `--stage 0.4.1` (only what 0.4.1 writes, for one grid);
- `--stage 0.4.1-inputs` (md5 of every input 0.4.1 reads);
- `--compare`, the gate: shape exact, `n_nonNA` exact, sums within `--rtol 1e-6`. Version rows (`_env/*`) are reported
  but never gated.

The macbook ran the same code and committed its reference fingerprints to `metadata/crosshost/`. The CGlabs half
comes later; CGlabs is busy.

**What the gate can and cannot say.** 0.4.1 has two kinds of output:
- **Built from GLW, highlands and boundaries only:** `livestock_number_number`, `livestock_masks`, `shoat_prop`. Those
  inputs are hash-pinned to their origin by the #58 manifests, so they are identical on every host by construction.
  These outputs are the host gate. They must be `same` or `within-tol`.
- **Built from FAOSTAT as well:** the six `glw4-2020_vop_*` rasters and the `-prices-used.csv` files. FAOSTAT is a
  rolling release (its archives change in place); the macbook's CSVs date from 2026-05-14. If your FAOSTAT md5s match
  the macbook's, these outputs must match too. If they differ, a VoP difference is an **input** difference, not a host
  one: report it, and it is not a failure.

**Rules.**
- Writes go only to your own 0.4.1 output directories (`<working_dir>/Data/GLW4_2020/{processed,intermediate}`),
  `logs/`, and three new files in `metadata/crosshost/`.
- Installs: none. If the 0.4.1 log shows `pacman` installing anything, stop. P4 left nothing for it to install.
- Background jobs with `nohup` and a `.pid` file, polled with `kill -0` or the log tail, never `wait`.
- No S3 writes.
- No usernames, hostnames or IPs in the RESPONSE or in the TSVs. The TSVs hold paths relative to `<working_dir>`
  only; check them by eye before committing.

#### P5.0 — pull, readiness, pinned inputs
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD
git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD      # develop
Rscript R/checks/71_stage_ready.R --stage 0.4.1 2>&1 | tail -5
for id in glw4-2020 afr-highlands boundaries-gaul2024; do Rscript R/checks/75_catalogue_manifest.R --verify $id --hash 2>&1 | tail -2; done
```
**What to expect:**
- 71 reports READY;
- all three `--verify` lines end `PASS`. A `missing` or `size` row there means a local copy differs from the origin.
  Fix it with `Rscript -e 'source("R/00_paths.R"); source("R/00_acquire.R"); atlas_require("<id>")'`, which is a
  verified fetch, then verify again.

If 71 is NOT READY, run `atlas_require_stage("0.4.1")` the same way (verified fetches only) and say what it fetched.
If that includes `faostat-bulk`, you now hold the **current** FAOSTAT release. That is fine; just say so.

#### P5.1 — run 0.4.1, res-25 then res-05 (about 2 + 3 min)
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup bash -c 'for RES in 0.25 0.05; do EXPOSURE_RES=$RES FORCE_OVERWRITE=1 Rscript -e "source(\"$PWD/R/0_server_setup.R\"); source(\"$PWD/R/0.4.1_create_livestock_exposure.R\")" > logs/phaseB_041_${RES}_'$STAMP'.log 2>&1 || exit 1; done' > logs/phaseB_041_$STAMP.log 2>&1 &
echo $! > logs/phaseB_041_$STAMP.pid
# poll: kill -0 $(cat logs/phaseB_041_$STAMP.pid) 2>/dev/null && echo running || grep -hE "COMPLETE|mass not conserved|rror" logs/phaseB_041_*_$STAMP.log
grep -il "install" logs/phaseB_041_*_$STAMP.log      # expect no output
```
**What to expect:**
- one `COMPLETE` line per resolution, and no `Error`;
- **one `mass not conserved on GLW resample` warning per resolution. It is expected:** the warning compares global GLW
  mass with what lands on the grid, and the res-25 grid is a 50°S-50°N band. Report its ratios verbatim; they are a
  cross-host value too. For res-25 the macbook got
  `poultry=0.9460, sheep=0.9513, pigs=0.8871, goats=0.9954, cattle=0.9520`. For res-05, an Africa-only grid, it got
  `poultry=0.1579, sheep=0.4337, pigs=0.0588, goats=0.4799, cattle=0.2606`.

#### P5.2 — fingerprint and commit
```bash
mkdir -p metadata/crosshost
Rscript R/checks/72_crosshost_fingerprint.R --stage 0.4.1-inputs --out metadata/crosshost/inputs_041_pascal.tsv
Rscript R/checks/72_crosshost_fingerprint.R --stage 0.4.1 --res res-25 --out metadata/crosshost/fp_041_pascal_res-25.tsv
Rscript R/checks/72_crosshost_fingerprint.R --stage 0.4.1 --res res-05 --out metadata/crosshost/fp_041_pascal_res-05.tsv
```
Expect 15 artefact rows + 13 `_env` rows per output TSV, and 13 + 13 for the inputs. If the counts differ, list what
is missing.

#### P5.3 — compare against the macbook
```bash
C="Rscript R/checks/72_crosshost_fingerprint.R --compare"; M=metadata/crosshost
$C $M/inputs_041_mac.tsv       $M/inputs_041_pascal.tsv       2>&1 | cut -c31-
$C $M/fp_041_mac_res-25.tsv    $M/fp_041_pascal_res-25.tsv    2>&1 | cut -c31-
$C $M/fp_041_mac_res-05.tsv    $M/fp_041_pascal_res-05.tsv    2>&1 | cut -c31-
```
**Read the result in this order:**
1. **Inputs.** The 6 GLW tifs, the highlands file and the admin0 boundaries must be `same`. These are the pinned
   inputs, and anything else is a stop. For the 5 FAOSTAT CSVs, report `same` or `VALUE-DIFF`. That decides step 3.
2. **The host gate.** `livestock_number_number_res-*`, `livestock_masks_res-*` and `shoat_prop_res-*` must be `same`
   or `within-tol` at both resolutions. Anything else is a **finding: report it, don't fix it.** Give the
   `max_rel_diff` and the `_env` differences printed beside it.
3. **VoP outputs.** If FAOSTAT was `same` in step 1, these must also be `same` or `within-tol`. If it was
   `VALUE-DIFF`, list their statuses and largest relative differences as input-driven, not as a failure.

Also report:
- every `env-diff` line, i.e. which library versions differ from the macbook. The macbook runs terra 1.9.34 with
  GDAL 3.8.5 on arm64; you run terra 1.8.93 with GDAL 3.4.3. A sub-tolerance difference is therefore plausible and
  fine; a larger one is the finding phase B exists to make.
- the two `mass not conserved` lines, set against the macbook's res-25 ratios above.

**Stop and describe** on any of:
- not on `develop`;
- a pinned input that will not verify;
- `pacman` installing anything;
- a missing `COMPLETE`;
- any write outside the places listed under Rules.

A host-gate difference is a **finding** to report, not a stop.

**To answer:**
1. Prepend one `### RESPONSE — pascal` (P5) to this file.
2. Run the IP grep over the response **and** the three TSVs.
3. Commit this file plus `metadata/crosshost/{inputs_041_pascal,fp_041_pascal_res-25,fp_041_pascal_res-05}.tsv`, and
   nothing else.
4. Push, then verify it landed.
5. End with a delivery line.

### RESPONSE — pascal 2026-11-05 (3) — P4: three install rounds, all binary, **`0.4-chain` BROKEN 0 MISSING 0** and **BROKEN 0 across all 70 packages**. Phase B is unblocked.

Every gate passes, and the result is better than the dispatch allowed for: the final audit has **BROKEN 0** in all three scopes, not only in `0.4-chain`. `both` has MISSING 0 as well. The nine remaining MISSING rows are all `upstream`-only.

No stop rule fired. Nothing was installed into the site or system library, nothing ran as root, and `R/0_server_setup.R` was not sourced.

#### P4.0 — before-state

Branch `develop`, `git log --oneline -1` = `bde991c`.

```
user lib first: TRUE
data.table  1.18.6.1  R 4.6.1; x86_64-pc-linux-gnu; 2026-10-28 12:58:17 UTC; unix
```

One package in the user library, as expected.

#### P4.1 — round 1

**Binary, not source.** Posit Package Manager serves R 4.6 jammy binaries for every package requested, so the slow `arrow`-from-source path never came up.

```
binary: 11    source: 0
```

Eleven, because `install.packages` pulled in one dependency that was not present anywhere, `paws.common`. All ten requested packages installed:

```
rlang          1.3.0
base64enc      0.1.6
arrow          26.0.0
geoarrow       0.4.4
duckdb         2.0.0
sfarrow        0.4.1
MetBrewer      0.2.0
miceadds       3.20.10
s3fs           0.1.7
paws.storage   0.11.0
```

**Round 1 wall time: about 20 s.** No errors, no warnings, nothing unavailable.

#### P4.2 — second and third layers

The dispatch allowed at most two extra rounds. Both were needed, and the third round cleared everything.

**Audit after round 1** — `OK 46   BROKEN 15   MISSING 9   (of 70)`, `0.4-chain OK 23  BROKEN 5  MISSING 0`. Every MISSING row in the 0.4 chain was gone, but a second-layer root appeared:

```
      6 vctrs
```

**Round 2: `P4_PKGS=vctrs`.** One binary, about 7 s, `vctrs 0.7.3`.

**Audit after round 2** — `OK 57   BROKEN 4   MISSING 9   (of 70)`, `0.4-chain OK 27  BROKEN 1  MISSING 0`. A third-layer root:

```
      2 dplyr
```

Four packages were still BROKEN and all four traced to that one root. `dplyr` and `tidyr` named `dplyr.so` directly; `tidyverse` failed in `.onAttach`; and `miceadds` reported `package 'mice' could not be loaded`, which is not a missing dependency. Loading `mice` on its own gave:

```
unable to load shared object '/usr/local/lib/R/site-library/dplyr/libs/dplyr.so':
  /usr/local/lib/R/site-library/dplyr/libs/dplyr.so: undefined symbol: R_shallow_duplicate_attr
```

So the same root, one level further down, and a **third symbol**: `R_shallow_duplicate_attr`, alongside the `SETLENGTH` and `Rf_EncodeReal` found in P3.

**Round 3: `P4_PKGS=dplyr`.** One binary, about 7 s, `dplyr 1.2.1`.

Summary of the three rounds:

| round | packages installed | binary / source | wall |
|---|---|---|---|
| 1 | `rlang`, `base64enc`, `arrow`, `geoarrow`, `duckdb`, `sfarrow`, `MetBrewer`, `miceadds`, `s3fs`, `paws.storage`, plus `paws.common` | 11 / 0 | about 20 s |
| 2 | `vctrs` | 1 / 0 | about 7 s |
| 3 | `dplyr` | 1 / 0 | about 7 s |

**This is the layering P3 predicted.** P3 reported four direct roots and said the figure was a lower bound, because the classifier names only the first shared object that fails. Two more roots surfaced here once `rlang` loaded: `vctrs`, then `dplyr`. The same thing will happen to IT, so the node-wide fix is a rebuild-and-re-sweep loop, not a one-shot list of four.

#### Final audit, verbatim

`logs/r_env_audit_p4_20261105_151730.{tsv,log}`:

```
[2026-11-05 15:16:51] +   1.1s | scope: 76 file(s); 70 distinct package(s)
[2026-11-05 15:17:53] +  62.8s | OK 61   BROKEN 0   MISSING 9   (of 70)
[2026-11-05 15:17:53] +  62.8s |   0.4-chain OK 28  BROKEN 0  MISSING 0
[2026-11-05 15:17:53] +  62.8s |   both      OK 13  BROKEN 0  MISSING 0
[2026-11-05 15:17:53] +  62.8s |   upstream  OK 20  BROKEN 0  MISSING 9
```

No BROKEN row anywhere, so no site-library root is still failing for anything the pipeline touches. The nine MISSING rows are all `upstream`, all packages outside the 0.4 chain, and none was in P4's list:

```
aws.s3           upstream   1:upload_AWS.R
climate4R.UDG    upstream   1:download_rawCMIP6.R
downscaleR       upstream   1:download_rawCMIP6.R
inborutils       upstream   1:download_cropCalendar.R
loadeR           upstream   1:download_rawCMIP6.R
transformeR      upstream   1:download_rawCMIP6.R
tsbox            upstream   2:preprocess_nex-gddp-cmip6_daily_data_v2_0.R;preprocess_nex-gddp-cmip6_daily_data.R
visualizeR       upstream   1:download_rawCMIP6.R
wbkernel         upstream   2:fast_calc_NDWS.R;fast_calc_waterbalance.R
```

Six of the nine belong to `download_rawCMIP6.R` and `download_cropCalendar.R`, outside 01 to 04. **`wbkernel` is the one that matters for phase C**, needed by `fast_calc_NDWS.R` and `fast_calc_waterbalance.R`. It is not on CRAN, so it needs its own decision rather than another `install.packages` line. `tsbox` and `aws.s3` are CRAN packages, but neither is in 01 to 04.

#### P4.3 — gates

All twelve smoke-test packages load:

```
rlang       OK 1.3.0
dplyr       OK 1.2.1
tidyr       OK 1.3.2
ggplot2     OK 4.0.1
purrr       OK 1.2.1
wbstats     OK 1.1
arrow       OK 26.0.0
geoarrow    OK 0.4.4
duckdb      OK 2.0.0
data.table  OK 1.18.6.1
terra       OK 1.8.93
sf          OK 1.0.24
```

```
arrow parquet: TRUE
```

```
14 packages in user lib
146M	~/R/x86_64-pc-linux-gnu-library/4.6
```

Worth noting which library each gate package came from: only `rlang`, `dplyr`, `arrow`, `geoarrow`, `duckdb` and `data.table` are user-library copies. `tidyr`, `ggplot2`, `purrr`, `wbstats`, `terra` and `sf` are the original site builds, now loading because the roots beneath them are fixed. Fourteen user-library packages were enough to unbreak fifty-odd site ones.

#### The user library, for the day IT rebuilds the site library

Everything now in `R_LIBS_USER`. All fourteen are built under R 4.6.0 or 4.6.1, so all are ABI-compatible with the running 4.6.1. **When the site library is rebuilt, this whole directory should be deleted**, so that the node goes back to one copy of each package and these per-user versions stop shadowing the site ones.

```
package        version    built    build date
arrow          26.0.0     R 4.6.0  2026-10-25
base64enc      0.1-6      R 4.6.0  2026-08-12
data.table     1.18.6.1   R 4.6.1  2026-10-28
dplyr          1.2.1      R 4.6.0  2026-08-12
duckdb         2.0.0      R 4.6.0  2026-10-24
geoarrow       0.4.4      R 4.6.0  2026-09-17
MetBrewer      0.2.0      R 4.6.0  2026-04-24
miceadds       3.20-10    R 4.6.0  2026-10-18
paws.common    0.9.0      R 4.6.0  2026-10-06
paws.storage   0.11.0     R 4.6.0  2026-10-08
rlang          1.3.0      R 4.6.0  2026-08-12
s3fs           0.1.7      R 4.6.0  2026-04-24
sfarrow        0.4.1      R 4.6.0  2026-04-24
vctrs          0.7.3      R 4.6.0  2026-08-12
```

Two version changes are worth flagging before phase B runs, because they are not like-for-like replacements of the site builds:

- **`dplyr` 1.2.1 in the user library against 1.1.4 in the site library.** A minor-version step, picked up because P3M's `latest` snapshot was used rather than a pin. Phase B compares values across hosts, so if cglabs or the macbook is on `dplyr` 1.1.4, that difference is now in the comparison. Flagging it rather than acting on it; say the word and the pin can be moved.
- **`rlang` 1.3.0 against the site library's 1.1.7**, and **`vctrs` 0.7.3 against 0.7.1**, for the same reason. These are lower-level and less likely to move a number, but they are the same kind of difference.

Everything else either did not exist here before, or matches what the site library held.

#### Notes

- **One write fell outside `R_LIBS_USER` and `logs/`**, and it is inherent to `install.packages`: R staged each downloaded tarball under its own session temporary directory before unpacking. Those directories are removed when the session ends and hold nothing of value. Nothing else outside the two permitted places was touched, and no S3 writes were made. Flagging it for completeness rather than as a problem.
- **No prompt to install as root or into the site library appeared at any point.** The `lib =` argument was explicit on every call, and `R_LIBS_USER` is first in `.libPaths()`.
- **Phase B is unblocked.** R/0.4.1 and the rest of the 0.4 chain now have every package they reference, and the 0.4 chain has no BROKEN and no MISSING row.
- **Phase C still needs `wbkernel`**, which was deliberately outside P4.
- **The IT ticket should gain one line from this**: the roots do not surface all at once. Four were visible in P3; fixing them exposed `vctrs`, and fixing that exposed `dplyr`, with a third missing symbol, `R_shallow_duplicate_attr`. The site-library rebuild needs the sweep re-run after each pass until it comes back clean.

#### Logs kept

`logs/p4_install.R`, `logs/p4_install_r1_20261105_151250.{log,pid}`, `logs/p4_install_r2_20261105_151500.{log,pid}`, `logs/p4_install_r3_20261105_151700.{log,pid}`, and the three audits `logs/r_env_audit_p4_20261105_{151330,151530,151730}.{tsv,log}`.

---

### MACBOOK 2026-11-05 — P4: per-user rebuild so the 0.4 chain loads (installs, Pete's GO). **Small profile is fine.**

**Why.** Your P3 sweep shrank the problem to four compiled roots. `data.table` is already rebuilt in your user
library, `RGtk2` is irrelevant, and the IT ticket is going in for the node-wide fix. Phase B (R/0.4.1 on this node)
should not wait for IT, so P4 does the same per-user repair you did for `data.table` and goes no further:
- rebuild `rlang` and `base64enc`;
- install the eight packages the 0.4 chain needs that are MISSING here: `arrow`, `geoarrow`, `duckdb`, `sfarrow`,
  `MetBrewer`, `miceadds`, `s3fs`, `paws.storage`;
- go round again for any second-layer roots that only show once `rlang` loads.

`wbkernel` (phase C) is **not** part of P4.

**Rules.**
- **Install only** into `R_LIBS_USER` (`~/R/x86_64-pc-linux-gnu-library/4.6`). Never into the site or system library,
  and never as root.
- Do not source `R/0_server_setup.R`.
- Writes go only to `R_LIBS_USER` and `logs/`.
- Installs run in the background with `nohup` and a `.pid` file, polled with `kill -0` or the log tail, never `wait`.
- No usernames, hostnames or IPs in the RESPONSE.

#### P4.0 — pull, and record the before-state
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD
git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD    # develop
Rscript -e 'l <- Sys.getenv("R_LIBS_USER"); cat("user lib first:", normalizePath(.libPaths()[1]) == normalizePath(l), "\n"); print(installed.packages(lib.loc = l)[, c("Version", "Built")])'
```
Expect `user lib first: TRUE` and one row, `data.table`.

#### P4.1 — round 1 (Posit Package Manager binaries for Ubuntu 22.04)
```bash
cat > logs/p4_install.R <<'RS'
lib <- Sys.getenv("R_LIBS_USER")
options(repos = c(P3M = "https://packagemanager.posit.co/cran/__linux__/jammy/latest"),
        HTTPUserAgent = sprintf("R/%s R (%s)", getRversion(),
          paste(getRversion(), R.version$platform, R.version$arch, R.version$os)))
pk <- strsplit(Sys.getenv("P4_PKGS"), ",")[[1]]
cat("installing into user lib:", paste(pk, collapse = " "), "\n")
install.packages(pk, lib = lib, Ncpus = 4)
for (p in pk) cat(sprintf("%-14s %s\n", p, tryCatch(as.character(packageVersion(p, lib.loc = lib)), error = function(e) "NOT INSTALLED")))
RS
STAMP=$(date +%Y%m%d_%H%M%S)
P4_PKGS=rlang,base64enc,arrow,geoarrow,duckdb,sfarrow,MetBrewer,miceadds,s3fs,paws.storage \
  nohup Rscript logs/p4_install.R > logs/p4_install_r1_$STAMP.log 2>&1 &
echo $! > logs/p4_install_r1_$STAMP.pid
# poll: kill -0 $(cat logs/p4_install_r1_$STAMP.pid) 2>/dev/null && echo running || tail -15 logs/p4_install_r1_$STAMP.log
```
**Binary or source?** Count the two kinds of install line in the log:
```bash
grep -c "installing \*binary\* package" logs/p4_install_r1_$STAMP.log
grep -c "installing \*source\* package" logs/p4_install_r1_$STAMP.log
```
If Posit Package Manager has no R 4.6 binaries for this platform, the install falls back to source. That is
allowed, but slower, and `arrow` from source can take 30-60 min. Report which way it went.

`install.packages` pulls in any dependency that is **not installed**. It does **not** rebuild dependencies that are
installed but broken in the site library. P4.2 covers those.

#### P4.2 — second layer, at most two more rounds
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
Rscript R/checks/76_r_env_audit.R --out logs/r_env_audit_p4_$STAMP.tsv > logs/r_env_audit_p4_$STAMP.log 2>&1
grep -E "OK [0-9]+ +BROKEN" logs/r_env_audit_p4_$STAMP.log
# roots still failing: the package whose shared object fails, taken from each BROKEN row's error
awk -F'\t' '$2=="BROKEN"' logs/r_env_audit_p4_$STAMP.tsv | grep -oE "site-library/[A-Za-z0-9.]+/libs" | cut -d/ -f2 | sort | uniq -c
```
The audit run takes a little over a minute; if your shell kills foreground commands sooner, run it with `nohup`.

If any roots are listed, install exactly those with the same script: `P4_PKGS=<root1>,<root2> nohup Rscript logs/p4_install.R …`, with
log `p4_install_r2_<STAMP>`. Then re-run the audit. At most **two** extra rounds. If BROKEN rows still name a
site-library root after round 3, stop and report them.

Packages that stay BROKEN through a **missing dependency** go into the report. Do not chase them beyond the list
above.

#### P4.3 — gates
```bash
Rscript -e 'for (p in c("rlang","dplyr","tidyr","ggplot2","purrr","wbstats","arrow","geoarrow","duckdb","data.table","terra","sf")) cat(sprintf("%-11s %s\n", p, tryCatch({suppressPackageStartupMessages(library(p, character.only = TRUE)); paste("OK", packageVersion(p))}, error = function(e) "FAIL")))'
Rscript -e 'cat("arrow parquet:", arrow::arrow_info()$capabilities[["parquet"]], "\n")'
Rscript -e 'l <- Sys.getenv("R_LIBS_USER"); ip <- installed.packages(lib.loc = l); cat(nrow(ip), "packages in user lib\n")'
du -sh ~/R/x86_64-pc-linux-gnu-library/4.6
```
**What to expect** (invariants):
- the final 76 audit shows **`0.4-chain` BROKEN 0 and MISSING 0**;
- `both` shows BROKEN 0. Its only MISSING row may be `wbkernel`, which is phase C and not P4's job;
- all twelve packages in the smoke test print `OK`;
- `arrow parquet: TRUE`.

`upstream`-only rows can stay BROKEN or MISSING; report the counts.

**Report:**
- binary vs source, with the install wall time per round;
- the root list and install line for each round of P4.2;
- the final 76 summary lines verbatim;
- the P4.3 output;
- the user-library package count and size;
- a `package, version, built` list of everything now in the user library, in a fenced block. When IT rebuilds the
  site library, that list says what to delete.

**Stop and describe** on any of:
- not on `develop`;
- any write outside `R_LIBS_USER` or `logs/`;
- any prompt to install as root or into the site library;
- roots still failing after round 3;
- a 0.4-chain gate failing for any reason other than a listed missing dependency.

**To answer:**
1. Prepend one `### RESPONSE — pascal` (P4) to this file.
2. Run the IP grep.
3. Commit only this file, then push.
4. Verify it landed.
5. End with a delivery line.

Keep `logs/p4_*`.

### RESPONSE — pascal 2026-11-05 (2) — P3: 1,653 packages swept, **588 BROKEN, but only 4 direct roots**. R was upgraded on 2026-10-26.

The ticket is smaller than the breakage looks. 588 of 1,653 site-library packages fail to load for a plain account, and 584 of those 588 fail only because one of **four** compiled packages fails underneath them. Nothing is a missing dependency, and nothing is unexplained.

No stop rule fired. Read-only throughout: nothing installed, `R/0_server_setup.R` not sourced, and `logs/` was the only thing written.

#### P3.1 — pull

Branch `develop`, `git log --oneline -1` = `063feaa` (the commit that added the P3 block).

#### P3.2 — site-library sweep

`R/checks/77_r_lib_sweep.R --lib /usr/local/lib/R/site-library --workers 6`, run with `nohup` and a `.pid`, polled with `kill -0`. 581.9 s wall, about 9 min 42 s. Log `logs/r_lib_sweep_20261105_141328.log`, TSV `logs/r_lib_sweep_20261105_141328.tsv` (1,653 rows plus header), both kept.

The log, from the `R 4.6.1; library has …` line to the end, verbatim:

```
[2026-11-05 14:14:05] +  36.0s | R 4.6.1; library has 1653 package(s); 6 worker(s)
[2026-11-05 14:14:05] +  36.0s |   built under R 4.1   compiled   64  pure-R  253
[2026-11-05 14:14:05] +  36.0s |   built under R 4.5   compiled  585  pure-R  751
[2026-11-05 14:23:11] + 581.8s | loaded OK 1065   BROKEN 588   (of 1653)
[2026-11-05 14:23:11] + 581.8s |   direct (own shared object fails): 4
[2026-11-05 14:23:11] + 581.8s |   via another package's shared object: 584
[2026-11-05 14:23:11] + 581.8s |   missing dependency: 0   other: 0
[2026-11-05 14:23:11] + 581.8s | ROOTS (direct failures) and the symbol each lacks:
[2026-11-05 14:23:11] + 581.8s |   base64enc              built R 4.1  undefined symbol: SETLENGTH
[2026-11-05 14:23:11] + 581.8s |   data.table             built R 4.5  undefined symbol: SETLENGTH
[2026-11-05 14:23:11] + 581.8s |   RGtk2                  built R 4.1  undefined symbol: Rf_EncodeReal
[2026-11-05 14:23:11] + 581.8s |   rlang                  built R 4.5  undefined symbol: SETLENGTH
[2026-11-05 14:23:11] + 581.9s | knock-on failures by root:
[2026-11-05 14:23:11] + 581.9s |   rlang                  503 package(s)
[2026-11-05 14:23:11] + 581.9s |   data.table             65 package(s)
[2026-11-05 14:23:11] + 581.9s |   base64enc              14 package(s)
[2026-11-05 14:23:11] + 581.9s |   RGtk2                  2 package(s)
[2026-11-05 14:23:11] + 581.9s | undefined symbols among roots: Rf_EncodeReal=1  SETLENGTH=3
[2026-11-05 14:23:11] + 581.9s | wrote logs/r_lib_sweep_20261105_141328.tsv
```

All three expectations from the dispatch hold. The library has 1,653 packages, close to the 1,650 predicted. `rlang` and `data.table` are both `direct` roots with `SETLENGTH`. `terra`, `sf`, `exactextractr` and `stringi` all load, each built under R 4.5:

```
exactextractr	4.5	yes	OK
sf	4.5	yes	OK
stringi	4.5	yes	OK
terra	4.5	yes	OK
```

Four points for the ticket.

**1. There are four direct roots, and two symbols between them.** `SETLENGTH` breaks `rlang`, `data.table` and `base64enc`; `Rf_EncodeReal` breaks `RGtk2`. Both are symbols R 4.6 no longer exports in the form these builds were compiled against. One root, `rlang`, accounts for 503 of the 584 knock-on failures on its own.

**2. Nothing is a missing dependency, and nothing is unclassified.** `missing dependency: 0   other: 0`. Every one of the 588 failures is the 4.5.1 to 4.6.1 ABI break and nothing else. There is no second, unrelated problem hiding in the count.

**3. Not one package in the library was built under R 4.6.** The tally is 1,336 built under R 4.5 and 317 under R 4.1; the running R is 4.6.1. The 1,065 that load do so because they do not touch the removed symbols, not because they are current. That is the real state of the library: it is entirely a 4.5-and-earlier library being run by 4.6.1.

**4. The figure of 4 roots is a lower bound, and the ticket should say so.** The classifier reports the first shared object that fails. A package whose own `.so` is also broken, but which loads `rlang` before reaching it, is counted as `via:rlang`. 158 of the 588 broken packages need compilation themselves (142 built under R 4.5, 16 under R 4.1), so some of them may turn out to be roots once `rlang` is fixed. The honest ask is: rebuild these four first, then re-run this sweep, which takes ten minutes and will show whether a second layer exists.

Full status tally, as `built / compiled / status`:

```
    443  4.5  yes  OK
    417  4.5  no   OK
    334  4.5  no   BROKEN
    157  4.1  no   OK
    142  4.5  yes  BROKEN
     96  4.1  no   BROKEN
     48  4.1  yes  OK
     16  4.1  yes  BROKEN
```

430 of the 588 broken packages are pure R. They hold no compiled code of their own and would load the moment the four roots are rebuilt.

#### P3.3 — when R was upgraded

`/var/log/dpkg.log*` is readable by any account on this node, so both commands returned an answer.

```
2026-10-26 19:18:26 upgrade r-base-core:amd64 4.5.1-1.2204.0 4.6.1-6.2204.0
```

That is the only `upgrade` or `install` line for `r-base-core` in the retained dpkg logs. The site library was not touched at the same time, which is the whole cause.

```
drwxr-xr-x 7 root root 4096 2026-10-26 19:18 /usr/lib/R/library/base
```

The base library directory carries the same minute, so it confirms the dpkg entry rather than adding a date of its own.

**The upgrade was ten days ago, on 2026-10-26.** It lines up with the first `SETLENGTH` failures seen in the FPAR thread, and it means nothing on this node has run against a consistent R since then.

#### What this means for the IT ask

The ticket can now be specific and small. The request is not "rebuild the site library"; it is:

- rebuild four compiled packages against R 4.6.1 — `rlang`, `data.table`, `base64enc`, `RGtk2` — which should clear about 584 knock-on failures;
- then re-run `R/checks/77_r_lib_sweep.R` to confirm, and to expose any second layer of roots that the first four are currently masking;
- and, separately, decide whether the whole library, all 1,653 packages built under R 4.1 or R 4.5, should be rebuilt as a batch, since not one of them has been rebuilt for 4.6.

The per-user workaround stays available but does not scale: it was enough to put `data.table` 1.18.6.1 into a per-user library for the FPAR work, but doing the same for every account on this node is not a fix.

#### Logs kept

`logs/r_lib_sweep_20261105_141328.log`, `logs/r_lib_sweep_20261105_141328.tsv`, `logs/r_lib_sweep_20261105_141328.pid`.

---

### MACBOOK 2026-11-05 — P3: whole site-library sweep before the IT ticket (read-only). **Small profile is fine.**

**Why.** Thank you for P0-P2. MapSPAM is fixed, and the new acquire layer has passed on its first node run. Before Pete
asks IT for a site-library rebuild, the ticket has to say how much is actually broken **for every account**, not
just for the 70 packages the pipeline uses. P1 tested those 70, with the per-user library first. P3 tests every
package in the site library as a plain account sees it.

**Rules.** Same as P0-P2:
- read-only;
- install nothing;
- do not source `R/0_server_setup.R`;
- writes go only to `logs/`;
- background jobs with `nohup` and `.pid`, polled with `kill -0` or the log tail, never `wait`;
- no usernames, hostnames or IPs in the RESPONSE.

#### P3.1 — pull
```bash
cd <repo> && git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD   # develop
```

#### P3.2 — sweep the site library (a few minutes at 6 workers)
`R/checks/77_r_lib_sweep.R` loads every package in the library, each in a fresh
`Rscript --no-environ --no-init-file` with the per-user library hidden, so your `data.table` rebuild cannot mask
the broken site build. It classifies each failure as:
- `direct`: the package's own `.so` fails;
- `via:<pkg>`: another package's `.so` fails while loading it;
- `dep`: a dependency is missing;
- `other`: anything else.

It also tallies the library by the R version each package was built under.
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup Rscript R/checks/77_r_lib_sweep.R --lib /usr/local/lib/R/site-library --workers 6 \
  --out logs/r_lib_sweep_$STAMP.tsv > logs/r_lib_sweep_$STAMP.log 2>&1 &
echo $! > logs/r_lib_sweep_$STAMP.pid
# poll: kill -0 $(cat logs/r_lib_sweep_$STAMP.pid) 2>/dev/null && echo running || tail -40 logs/r_lib_sweep_$STAMP.log
```
**What to expect:**
- about 1,650 packages, nearly all built under R 4.5;
- `rlang` and `data.table` among the `direct` roots, with `SETLENGTH`;
- `terra`, `sf`, `exactextractr` and `stringi` load.

The number to find out is how many **direct** roots there are, and which symbols they lack.

**Report:**
- the log from the `R 4.6.1; library has …` line to the end, verbatim. It holds the built-under tally, the counts,
  every direct root with its symbol, the knock-on counts per root, and the dep/other rows;
- not the full TSV. Keep it in `logs/`.

#### P3.3 — when R was upgraded (read-only)
```bash
zgrep -h -E " (upgrade|install) r-base-core" /var/log/dpkg.log* 2>/dev/null | sort | tail -5
ls -ld --time-style=long-iso /usr/lib/R/library/base
```
Report both outputs, with versions and dates. If `/var/log/dpkg.log*` is not readable, say so. That is an answer,
not a failure.

**Stop and describe** on any of:
- not on `develop`;
- the sweep dying before its summary;
- anything written outside `logs/`.

**To answer:**
1. Prepend one `### RESPONSE — pascal` (P3) to this file.
2. Run the IP grep.
3. Commit only this file, then push.
4. Verify it landed.
5. End with a delivery line.

### RESPONSE — pascal 2026-11-05 — P0-P2 done. P1 audit: OK 37 / BROKEN 16 / MISSING 17. P2 re-fetch: **PASS, ok=48**, both truncated tiles now match the reference.

All of P0, P1 and P2 ran to completion. No stop rule fired. Nothing was installed, `R/0_server_setup.R` was not sourced, and every write landed inside the three permitted places.

---

#### P0 — pull, identity, limits

- Branch: `develop`.
- `git log --oneline -1` at the time of the run: `6765459` (this file's MACBOOK block was present).
- Limits, read from the cgroup v2 path for this process: `cpu.max` = `800000 100000` (8 CPUs), `memory.max` = `34359738368` (32 GiB). This is the Small profile. Note for anyone reading the logs later: `nproc` on this node reports 80 and is always wrong, because it reports the host, not the cgroup.
- `R --version | head -1`: `R version 4.6.1 (2026-06-24) -- "Happy Hop"`.
- `.libPaths()`:
  ```
  ~/R/x86_64-pc-linux-gnu-library/4.6
  /usr/local/lib/R/site-library
  /usr/lib/R/site-library
  /usr/lib/R/library
  ```
  The per-user library is first, which is what makes the `data.table` rebuild from the FPAR thread effective.

---

#### P1 — R environment audit

`R/checks/76_r_env_audit.R`, run in the background, 68.9 s wall. Logs: `logs/r_env_audit_20261104_154610.tsv` and `.log`.

Summary lines verbatim:

```
[2026-11-04 15:46:12] +   1.5s | scope: 76 file(s); 70 distinct package(s)
[2026-11-04 15:47:20] +  68.9s | OK 37   BROKEN 16   MISSING 17   (of 70)
[2026-11-04 15:47:20] +  68.9s |   0.4-chain OK 15  BROKEN 6  MISSING 7
[2026-11-04 15:47:20] +  68.9s |   both      OK 8  BROKEN 4  MISSING 1
[2026-11-04 15:47:20] +  68.9s |   upstream  OK 14  BROKEN 6  MISSING 9
```

All four invariants from the dispatch hold:

- `data.table` is OK at 1.18.6.1 from the `user` library;
- the `rlang`-dependent packages are BROKEN with `SETLENGTH`;
- `terra` (1.8.93) and `sf` (1.0.24) are OK;
- `s3fs` and `paws.storage` are MISSING.

**Is any BROKEN error something other than `SETLENGTH`?** One row's error text differs, and it resolves to the same cause. `devtools` reports `package 'usethis' could not be loaded` rather than naming the symbol. Loading `usethis` on its own fails with the same `rlang.so: undefined symbol: SETLENGTH`, so `devtools` is that failure one level down. Every one of the 16 BROKEN packages therefore traces to the single broken `rlang` build. That is useful for sizing the IT ask: rebuilding `rlang` alone against R 4.6.1 would likely clear most of the 16 at once, and the rest are its reverse dependencies that carry their own compiled code.

Full TSV (columns: `package status scope version lib used_by error`; the `error` column is truncated by the audit script, which is why the shared-object paths stop mid-line):

```
package	status	scope	version	lib	used_by	error
devtools	BROKEN	upstream	2.4.6	site	2:download_cropCalendar.R;download_rawCMIP6.R	package ‘usethis’ could not be loaded
dplyr	BROKEN	both	1.1.4	site	44:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;download_chirts.R;...	package or namespace load failed for ‘dplyr’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs/
ecmwfr	BROKEN	upstream	2.0.3	site	1:download_AgERA5.R	package or namespace load failed for ‘ecmwfr’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs
FactoMineR	BROKEN	upstream	2.13	site	2:QAQC_daily_files-NexGDDP.R;QAQC_for_indices.R	package or namespace load failed for ‘FactoMineR’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/
furrr	BROKEN	both	0.3.1	site	8:0.4.4_process_exposure.R;download_manual_nex_gddpCMIP6_v2_0.R;download_manual_nex_gddpCMIP6.R;...	package or namespace load failed for ‘furrr’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs/
ggplot2	BROKEN	both	4.0.1	site	3:0.4.1_create_livestock_exposure.R;QAQC_daily_files-NexGDDP.R;QAQC_for_indices.R	package or namespace load failed for ‘ggplot2’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/lib
progress	BROKEN	0.4-chain	1.2.3	site	1:0.4.5_create_faostat_long.R	package or namespace load failed for ‘progress’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/li
purrr	BROKEN	upstream	1.2.1	site	46:download_chirps.R;download_chirts.R;download_manual_nex_gddpCMIP6_v2_0.R;...	package or namespace load failed for ‘purrr’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs/
rvest	BROKEN	0.4-chain	1.0.5	site	2:0_server_setup.R;00_acquire.R	package or namespace load failed for ‘rvest’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs/
stringr	BROKEN	0.4-chain	1.6.0	site	1:haz_functions.R	package or namespace load failed for ‘stringr’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/lib
tibble	BROKEN	upstream	3.3.1	site	1:identifyCorruptedFiles.R	package or namespace load failed for ‘tibble’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs
tidyr	BROKEN	both	1.3.2	site	3:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;createAfricaSoils.R	package or namespace load failed for ‘tidyr’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs/
tidyverse	BROKEN	upstream	2.0.0	site	54:download_AgERA5.R;download_chirps.R;download_chirts.R;...	package or namespace load failed for ‘tidyverse’:  .onAttach failed in attachNamespace() for 'tidyverse', details:   call: NULL   error: package or namespace lo
viridis	BROKEN	0.4-chain	0.6.5	site	1:haz_functions.R	package or namespace load failed for ‘viridis’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/lib
wbstats	BROKEN	0.4-chain	1.1	site	2:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R	package or namespace load failed for ‘wbstats’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/lib
xml2	BROKEN	0.4-chain	1.5.2	site	1:0_server_setup.R	package or namespace load failed for ‘xml2’ in dyn.load(file, DLLpath = DLLpath, ...):  unable to load shared object '/usr/local/lib/R/site-library/rlang/libs/r
arrow	MISSING	both			9:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	there is no package called ‘arrow’
aws.s3	MISSING	upstream			1:upload_AWS.R	there is no package called ‘aws.s3’
climate4R.UDG	MISSING	upstream			1:download_rawCMIP6.R	there is no package called ‘climate4R.UDG’
downscaleR	MISSING	upstream			1:download_rawCMIP6.R	there is no package called ‘downscaleR’
duckdb	MISSING	0.4-chain			2:0.4.4_process_exposure.R;0.4.5_create_faostat_long.R	there is no package called ‘duckdb’
geoarrow	MISSING	0.4-chain			4:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	there is no package called ‘geoarrow’
inborutils	MISSING	upstream			1:download_cropCalendar.R	there is no package called ‘inborutils’
loadeR	MISSING	upstream			1:download_rawCMIP6.R	there is no package called ‘loadeR’
MetBrewer	MISSING	0.4-chain			1:haz_functions.R	there is no package called ‘MetBrewer’
miceadds	MISSING	0.4-chain			1:haz_functions.R	there is no package called ‘miceadds’
paws.storage	MISSING	0.4-chain			3:0.4.5_create_faostat_long.R;0_server_setup.R;haz_functions.R	there is no package called ‘paws.storage’
s3fs	MISSING	0.4-chain			3:0.4.5_create_faostat_long.R;0_server_setup.R;haz_functions.R	there is no package called ‘s3fs’
sfarrow	MISSING	0.4-chain			1:haz_functions.R	there is no package called ‘sfarrow’
transformeR	MISSING	upstream			1:download_rawCMIP6.R	there is no package called ‘transformeR’
tsbox	MISSING	upstream			2:preprocess_nex-gddp-cmip6_daily_data_v2_0.R;preprocess_nex-gddp-cmip6_daily_data.R	there is no package called ‘tsbox’
visualizeR	MISSING	upstream			1:download_rawCMIP6.R	there is no package called ‘visualizeR’
wbkernel	MISSING	upstream			2:fast_calc_NDWS.R;fast_calc_waterbalance.R	there is no package called ‘wbkernel’
compiler	OK	upstream	4.6.1	base	4:calc_THI.R;fast_calc_NDWL0.R;fast_calc_NDWL50.R;...	
countrycode	OK	0.4-chain	1.6.1	site	5:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	
data.table	OK	0.4-chain	1.18.6.1	user	8:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	
DBI	OK	0.4-chain	1.2.3	site	2:0.4.4_process_exposure.R;0.4.5_create_faostat_long.R	
envirem	OK	upstream	3.1	site	1:calc_TAI.R	
exactextractr	OK	0.4-chain	0.10.1	site	1:0_server_setup.R	
fields	OK	upstream	17.1	site	1:bc_calc_anomalies.R	
fs	OK	0.4-chain	1.6.6	site	1:haz_functions.R	
future	OK	both	1.69.0	site	9:0.4.4_process_exposure.R;0.4.5_create_faostat_long.R;haz_functions.R;...	
future.apply	OK	both	1.20.1	site	3:0.4.5_create_faostat_long.R;haz_functions.R;identifyCorruptedFiles.R	
gdalUtilities	OK	0.4-chain	1.2.5	site	1:haz_functions.R	
geodata	OK	upstream	0.6.6	site	3:download_chirts.R;download_cropCalendar.R;createAfricaMask.R	
glue	OK	0.4-chain	1.8.0	site	1:0_server_setup.R	
gtools	OK	upstream	3.9.5	site	16:fixDailyFutureData.R;getDailyFutureData.R;identifyCorruptedFiles.R;...	
httr	OK	both	1.4.7	site	5:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0_server_setup.R;...	
jsonlite	OK	0.4-chain	2.0.0	site	5:0.4.4_process_exposure.R;0_server_setup.R;haz_functions.R;...	
lubridate	OK	upstream	1.9.4	site	25:download_AgERA5.R;download_chirps.R;preprocess_nex-gddp-cmip6_daily_data_v2_0.R;...	
ncdf4	OK	upstream	1.24	site	3:fast_calc_NDWL0.R;fast_calc_NDWL50.R;fast_calc_NDWS.R	
pacman	OK	both	0.5.1	site	39:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	
parallel	OK	upstream	4.6.1	base	2:download_AgERA5.R;identifyCorruptedFiles.R	
pbapply	OK	0.4-chain	1.7.4	site	5:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.3_fao_production_cv.R;...	
progressr	OK	0.4-chain	0.18.0	site	2:0.4.5_create_faostat_long.R;haz_functions.R	
R.utils	OK	upstream	2.13.0	site	1:download_chirps.R	
raster	OK	upstream	3.6.32	site	5:createAfricaMask.R;createAfricaSoils.R;fast_calc_NDWL0.R;...	
remotes	OK	0.4-chain	2.5.0	site	1:0_server_setup.R	
rgdal	OK	upstream	1.5.32	site	1:download_rawCMIP6.R	
rnaturalearth	OK	upstream	1.2.0	site	1:createAfricaMask.R	
rnaturalearthdata	OK	upstream	1.0.0	site	1:createAfricaMask.R	
sf	OK	0.4-chain	1.0.24	site	6:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	
sp	OK	both	2.2.0	site	2:0.4.3_fao_production_cv.R;createAfricaMask.R	
stats	OK	both	4.6.1	base	4:0.4.0_create_crop_vop_intld15.R;0.4.2_create_crop_vop_nominal_usd.R;00_paths.R;...	
stringi	OK	0.4-chain	1.8.7	site	1:haz_functions.R	
terra	OK	both	1.8.93	site	60:0.4.0_create_crop_vop_intld15.R;0.4.1_create_livestock_exposure.R;0.4.2_create_crop_vop_nominal_usd.R;...	
tools	OK	0.4-chain	4.6.1	base	1:00_acquire.R	
utils	OK	both	4.6.1	base	7:00_acquire.R;download_chirps.R;download_cropCalendar.R;...	
wesanderson	OK	0.4-chain	0.3.7	site	1:haz_functions.R	
xts	OK	upstream	0.14.1	site	2:preprocess_nex-gddp-cmip6_daily_data_v2_0.R;preprocess_nex-gddp-cmip6_daily_data.R
```

Two observations for phases B and C:

- **Phase B (R/0.4.1 value-level gate) is blocked as things stand.** R/0.4.1 needs `dplyr`, `tidyr` and `ggplot2`, all BROKEN, plus `arrow` and `geoarrow`, both MISSING. R/0.4.0 and R/0.4.2 are in the same position.
- **Phase C (`hazards_upstream` 01 to 04) is closer than the headline numbers suggest.** The heavy `upstream` MISSING rows are climate4R, `loadeR`, `downscaleR`, `transformeR`, `visualizeR` and `inborutils`, which the `used_by` column shows belong to `download_rawCMIP6.R` and `download_cropCalendar.R`, outside 01 to 04. `wbkernel` is the one that matters inside that range: `fast_calc_NDWS.R` and `fast_calc_waterbalance.R` need it and it is not installed here.

**P2 gate:** `jsonlite` is OK, 2.0.0, from the site library. The gate passes, so P2 ran.

---

#### P2.1 — verify before

Log: `logs/mapspam_verify_before_20261104_154803.log`. Hashing about 1.3 GB took under a second of reported wall time because the size check short-circuits the missing and wrong-size objects.

Summary line verbatim:

```
[2026-11-04 15:48:39] +   0.0s INFO  |   manifest 48 object(s); local 36 file(s) matching '*.tif'
[2026-11-04 15:48:39] +   0.0s INFO  |   missing=12  ok=34  size=2
```

The verifier prints only the counts that have members, so there are no `hash=` or `extra=` figures on that line: no object had a size match with a hash mismatch, and no local file was outside the manifest. Closing line: `FAIL - 14 object(s) missing or wrong`.

All 14 non-`ok` rows, verbatim apart from the leading timestamp:

```
WARN  |   size    variable=phys-area_ha/spam_phys-area_ha_rf-all.tif  (local 45,664,814, manifest 47,197,123)
WARN  |   size    variable=phys-area_ha/spam_phys-area_ha_rf-highinput.tif  (local 11,715,130, manifest 25,830,038)
WARN  |   missing variable=vop_nominal-usd15/spam_vop_nominal-usd15_all.tif  (local NA, manifest 19,451,146)
WARN  |   missing variable=vop_nominal-usd15/spam_vop_nominal-usd15_irr.tif  (local NA, manifest 5,070,786)
WARN  |   missing variable=vop_nominal-usd15/spam_vop_nominal-usd15_rf-all.tif  (local NA, manifest 19,048,878)
WARN  |   missing variable=vop_nominal-usd15/spam_vop_nominal-usd15_rf-highinput.tif  (local NA, manifest 10,644,922)
WARN  |   missing variable=vop_nominal-usd15/spam_vop_nominal-usd15_rf-lowinput.tif  (local NA, manifest 15,662,860)
WARN  |   missing variable=vop_nominal-usd15/spam_vop_nominal-usd15_rf-subsistence.tif  (local NA, manifest 14,119,296)
WARN  |   missing variable=vop_nominal-usd21/spam_vop_nominal-usd21_all.tif  (local NA, manifest 19,584,380)
WARN  |   missing variable=vop_nominal-usd21/spam_vop_nominal-usd21_irr.tif  (local NA, manifest 5,078,404)
WARN  |   missing variable=vop_nominal-usd21/spam_vop_nominal-usd21_rf-all.tif  (local NA, manifest 19,169,272)
WARN  |   missing variable=vop_nominal-usd21/spam_vop_nominal-usd21_rf-highinput.tif  (local NA, manifest 10,678,008)
WARN  |   missing variable=vop_nominal-usd21/spam_vop_nominal-usd21_rf-lowinput.tif  (local NA, manifest 15,766,518)
WARN  |   missing variable=vop_nominal-usd21/spam_vop_nominal-usd21_rf-subsistence.tif  (local NA, manifest 14,207,476)
```

The 12 missing objects are two complete variable directories, `vop_nominal-usd15/` and `vop_nominal-usd21/`, six files each. This matches the 2026-10-09 finding exactly: 36 of 48 present, two of those truncated.

#### P2.2 — re-fetch

Launched with `nohup` exactly as the dispatch writes it, into `logs/mapspam_refetch_20261104_155500.log` with a matching `.pid`. Tracked by polling `kill -0` on the pid; no `wait`.

```
INFO  | acquire 'mapspam-2020v1r2' method=s3-dir -> <working_dir>/Data/mapspam/2020V1r2_SSA/processed
INFO  |   pinned manifest: 48 object(s), 1,332,962,700 bytes
INFO  |   48 object(s): 48 fetched (36 re-fetched), 0 already present, 1,332,962,700 bytes
INFO  |   verified: hash 48, size only 0, drift 0, no manifest 0
```

All four expectations met, and `verification failed` appears nowhere in the log.

**Rate: 530.6 s wall (8 min 51 s) for 1,332,962,700 bytes, which is 2.51 MB/s (2.40 MiB/s).** That is roughly twice the macbook's 1.3 MB/s from the same bucket, and it is worth recording against the FPAR result from the other thread: that ingest ran 29.5 h here against 1.9 h on cglabs. The two together say the FPAR slowness was not raw S3-over-HTTPS bandwidth from this node. This node pulls a large single-stream S3 object at a healthy rate.

Note on the `(36 re-fetched)` count: with `force = TRUE` every object is fetched, so `36` is the number that already existed locally, including the two truncated ones, and `0 already present` is consistent with that. The two truncated tiles were overwritten through their `.part` files rather than left in place.

#### P2.3 — verify after

```
[2026-11-05 11:23:01] +   0.0s INFO  | verify 'mapspam-2020v1r2' at <working_dir>/Data/mapspam/2020V1r2_SSA/processed  [hash]
[2026-11-05 11:23:01] +   0.0s INFO  |   manifest 48 object(s); local 48 file(s) matching '*.tif'
[2026-11-05 11:23:01] +   0.0s INFO  |   ok=48
PASS - every manifest object present and matching
```

The two tiles, before against after against the dispatch's reference:

| tile | before size | before md5 | after size | after md5 | reference size | reference md5 | match |
|---|---|---|---|---|---|---|---|
| `spam_phys-area_ha_rf-all.tif` | 45,664,814 | `8a91f44edb3394d4a21e9115ff3d32a9` | 47,197,123 | `3807675ba25f455ffaa4d8f3d84e58f8` | 47,197,123 | `3807675ba25f455ffaa4d8f3d84e58f8` | yes |
| `spam_phys-area_ha_rf-highinput.tif` | 11,715,130 | `32a0f7c1fa8ef04cef679418b6ed1371` | 25,830,038 | `e5cc9a45b20fb9e45e60080558a7fa6b` | 25,830,038 | `e5cc9a45b20fb9e45e60080558a7fa6b` | yes |

Both now agree with the macbook copy byte for byte. `rf-highinput` had been missing more than half its bytes, which is why anything that read it would have produced wrong numbers rather than an error.

`.part` files remaining: **0**.

Receipt tail (`atlas_acquire_status`), one row, since this is the first acquire of this record on this node:

```
                timestamp method files      bytes status
 2026-11-04T21:19:15+0300 s3-dir    48 1332962700     ok
```

**Extras: none.** The dispatch expected `variable=vop_intld15-2021/` and `fao_prices/` to show up as `extra`, from R/0.4.0 and R/0.4.2. Neither exists in this tree, and `ok=48` with 48 local files confirms there is nothing beyond the manifest. The explanation is that R/0.4.0 and R/0.4.2 have never run on this node, which the P1 result makes inevitable: both need `dplyr`, `tidyr`, `arrow` and `geoarrow`, which are BROKEN or MISSING here. The eight variable directories present are all manifest members, six files each:

```
variable=harv-area_ha        6
variable=phys-area_ha        6
variable=prod_t              6
variable=vop_intld15         6
variable=vop_nominal-usd15   6
variable=vop_nominal-usd21   6
variable=vop_usd2015         6
variable=yield_kgha          6
```

Nothing was deleted.

---

#### Notes

- **The new acquire layer works on a node, first time.** The pinned manifest, the `.part` staging and the base-R HTTPS reader all behaved as designed, and the `s3fs` gap is no longer in the way. That is the thing phase A was really testing.
- **Writes stayed inside the permitted set**: the MapSPAM processed tree, its receipt at `<working_dir>/Data/_acquisition/mapspam-2020v1r2.jsonl`, and `logs/`. Nothing else in the repo or the bulk area changed. No S3 writes were attempted.
- **The IT ask is now one line**: rebuild the site-library packages against R 4.6.1, starting with `rlang`, which is the single root of all 16 BROKEN rows. The per-user fallback works but does not scale: it put `data.table` 1.18.6.1 into the user library, and doing the same for 16 packages and their compiled dependencies would take a long time and leave this node silently different from the others.
- Logs kept: `logs/r_env_audit_20261104_154610.{tsv,log}`, `logs/mapspam_verify_before_20261104_154803.log`, `logs/mapspam_refetch_20261104_155500.log`, `logs/mapspam_verify_after_20261104_155500.log`.

---

### MACBOOK 2026-11-02 — P0-P2: R audit (read-only), re-fetch `mapspam-2020v1r2` (the one fix). **Small profile is fine.**

**Why.** Issue [#58](https://github.com/AdaptationAtlas/hazards_prototype/issues/58) tracks the seven #29
follow-ups.
- **Phases B and C need a working R stack.** Phase B runs R/0.4.1 here for a value-level cross-host gate. Phase C runs
  one GCM through `hazards_upstream` 01→04. Since the 4.5.1 → 4.6.1 upgrade, `data.table` and `rlang` fail to load
  here. P1 measures how much of the stack those two phases need is broken, which sizes the IT ask and the per-user
  rebuild fallback.
- **The MapSPAM fix.** On 2026-10-09 your processed MapSPAM tree held 36 files against the 48 the origin holds, and
  two of them were truncated. `R/00_acquire.R` now pins a per-object manifest
  (`metadata/catalogue/_manifests/mapspam-2020v1r2.tsv`: size plus S3 ETag), writes each download through a `.part`
  file and hashes it. It also reads S3 over plain HTTPS in base R, so it no longer needs `s3fs`, which this node does
  not have. P2 is the first run of that code on a node.

**Rules.**
- **Install nothing.** No `install.packages`, no `pacman`, and do **not** source `R/0_server_setup.R`: its
  `pacman::p_load()` installs whatever is missing.
- **Writes go only to:**
  - the MapSPAM processed tree (`<working_dir>/Data/mapspam/2020V1r2_SSA/processed`);
  - its receipt (`<working_dir>/Data/_acquisition/mapspam-2020v1r2.jsonl`);
  - `logs/`.
- No S3 writes (none are possible here).
- **Background jobs** use `nohup … &` with `logs/<name>_<STAMP>.log` plus a `.pid` file. Track them by polling
  `kill -0 $(cat <pid>)` or the log tail, not with `wait`.
- **Shell variables.** If your shell does not keep variables between commands, start each one with
  `cd <repo> && export ATLAS_PROJECT_DIR=$PWD`, and re-derive `MS` (P2.1) where it is used.
- **Public repo.** No usernames, hostnames, IPs or mount identifiers in the RESPONSE. Write paths as `<repo>`,
  `<working_dir>` and `~`.

#### P0 — pull, identity, limits (this is item (c))
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD
git fetch && git rev-parse --abbrev-ref HEAD          # must be: develop
git pull --ff-only && git log --oneline -1            # at or after the commit that added this file
CG=/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup); cat $CG/cpu.max $CG/memory.max
R --version | head -1; Rscript -e 'cat(.libPaths(), sep = "\n")' | sed "s#$HOME#~#"
```
**Stop and report** if the branch is not `develop`. Do not switch branches yourself.

#### P1 — (a) R environment audit (read-only, a few minutes)
`R/checks/76_r_env_audit.R` statically scans R/0.4.x, together with what they need sourced first and
`hazards_upstream/R/**`, for `library`, `require`, `requireNamespace`, `p_load` and `pkg::`. That gives 70
packages. It then loads each one in its own fresh `Rscript` and reports OK, BROKEN or MISSING, the version, and the
library it came from (user / site / base). It installs nothing.
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup Rscript R/checks/76_r_env_audit.R --out logs/r_env_audit_$STAMP.tsv > logs/r_env_audit_$STAMP.log 2>&1 &
echo $! > logs/r_env_audit_$STAMP.pid
# poll: kill -0 $(cat logs/r_env_audit_$STAMP.pid) 2>/dev/null && echo running || tail -6 logs/r_env_audit_$STAMP.log
```
**What to expect** (invariants, not exact counts):
- `data.table` is OK from `user` (1.18.6.1);
- `rlang`-dependent packages (`dplyr`, `tidyr`, `purrr`, …) are BROKEN with `SETLENGTH`;
- `terra` and `sf` are OK;
- `s3fs` and `paws.storage` are MISSING (already known).

Some `upstream` rows come from `hazards_upstream` scripts outside 01→04 (climate4R and similar). The `used_by` column
shows which.

**Report:**
- the summary lines verbatim (`OK / BROKEN / MISSING`, plus the per-scope lines `0.4-chain` / `both` /
  `upstream`);
- the whole TSV in a fenced block;
- one line on whether any BROKEN error is something **other** than `SETLENGTH`.

#### P2 — (b) re-fetch `mapspam-2020v1r2` (the one fix)
**Gate:** `jsonlite` must be OK in the P1 table, because the acquire layer reads the catalogue with it. If it is
BROKEN or MISSING, stop here and do not install it.

**P2.1 before (read-only; hashes about 1.3 GB):**
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup Rscript R/checks/75_catalogue_manifest.R --verify mapspam-2020v1r2 --hash > logs/mapspam_verify_before_$STAMP.log 2>&1 &
echo $! > logs/mapspam_verify_before_$STAMP.pid
MS=$(Rscript -e 'source("R/00_paths.R"); cat(file.path(atlas_working_dir(), "Data/mapspam/2020V1r2_SSA/processed"))')
for f in rf-all rf-highinput; do p="$MS/variable=phys-area_ha/spam_phys-area_ha_$f.tif"; echo "$f $(stat -c %s "$p" 2>/dev/null) $(md5sum "$p" 2>/dev/null | cut -d' ' -f1)"; done
```
These ETags are multipart (`<md5>-<parts>`), so a plain md5 of the file will **not** equal them. The verifier
recomputes the multipart form; the plain md5s are reported for the record.

**Reference** (S3 listing 2026-11-02; the macbook copy hash-verifies against it, 48/48):

| tile | bytes | S3 ETag | plain md5 of a good copy (macbook) |
|---|---|---|---|
| `spam_phys-area_ha_rf-all.tif` | 47,197,123 | `851e2ea5dc5b7cb19f6303cd9e05400d-6` | `3807675ba25f455ffaa4d8f3d84e58f8` |
| `spam_phys-area_ha_rf-highinput.tif` | 25,830,038 | `f2e4ecb28b9e774e2c8507f836d62bda-4` | `e5cc9a45b20fb9e45e60080558a7fa6b` |

**P2.2 fetch** (`force = TRUE`, scoped to this one record, about 1.33 GB over HTTPS). The macbook gets about 1.3 MB/s
from this bucket, so allow **20-60 min**:
```bash
STAMP=$(date +%Y%m%d_%H%M%S)
nohup Rscript -e 'source("R/00_paths.R"); source("R/00_acquire.R"); atlas_acquire("mapspam-2020v1r2", force = TRUE)' \
  > logs/mapspam_refetch_$STAMP.log 2>&1 &
echo $! > logs/mapspam_refetch_$STAMP.pid
# poll: kill -0 $(cat logs/mapspam_refetch_$STAMP.pid) 2>/dev/null && echo running || tail -6 logs/mapspam_refetch_$STAMP.log
```
**What to expect:**
- the log shows `pinned manifest: 48 object(s), 1,332,962,700 bytes`;
- it ends `48 object(s): 48 fetched (N re-fetched)`, where N = files that already existed;
- the closing line reads `verified: hash 48, size only 0, drift 0, no manifest 0`;
- no `verification failed` anywhere.

Give the wall time and the effective MB/s, since this is the S3-over-HTTPS rate for this node.

**P2.3 after:**
```bash
Rscript R/checks/75_catalogue_manifest.R --verify mapspam-2020v1r2 --hash > logs/mapspam_verify_after_$STAMP.log 2>&1; tail -4 logs/mapspam_verify_after_$STAMP.log
for f in rf-all rf-highinput; do p="$MS/variable=phys-area_ha/spam_phys-area_ha_$f.tif"; echo "$f $(stat -c %s "$p") $(md5sum "$p" | cut -d' ' -f1)"; done
find "$MS" -name '*.part' | wc -l       # expect 0
Rscript -e 'source("R/00_paths.R"); source("R/00_acquire.R"); s <- atlas_acquire_status("mapspam-2020v1r2"); print(tail(s[, c("timestamp","method","files","bytes","status")], 2), row.names = FALSE)'
```
**What to expect:**
- `PASS`, with `ok=48`;
- the two tiles at the reference sizes and plain md5s above;
- zero `.part` files;
- the last receipt row `ok`.

Any `extra` rows are local files the origin does not hold. They are expected, not a failure: R/0.4.0 and R/0.4.2 write
`variable=vop_intld15-2021/` and `fao_prices/` into this tree. List the extras by top-level directory with a count for
each, and **do not delete them**.

**Report:**
- the P2.1 summary line (`ok=… missing=… size=… hash=… extra=…`) and every non-`ok` row;
- the two tiles' before/after size and md5, each set against the reference;
- the P2.2 tail and its rate;
- the P2.3 lines.

**Stop and describe** (do not improvise a fix) on any of:
- not on `develop`;
- `jsonlite` not OK;
- `verification failed` in P2.2;
- P2.3 not `PASS` (other than extras);
- anything written outside the three places listed under Rules.

**To answer:**
1. Prepend one `### RESPONSE — pascal` (P0-P2) to this file.
2. Run the `AGENTS.md` §3 IP grep, and check by eye that the RESPONSE holds no usernames or hostnames.
3. Commit only this file, then push.
4. Verify it landed: `git fetch`, then `git log origin/develop..HEAD` must be empty.
5. End with a delivery line.

Keep the logs.
