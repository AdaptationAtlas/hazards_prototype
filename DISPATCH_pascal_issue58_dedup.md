# DISPATCH — pascal — #58 storage deduplication

**Append-only; newest block on top. Prepend `### RESPONSE — pascal [<code>]` to answer.**

This file belongs to the **deduplication** PASCAL session. The migration session answers in
`DISPATCH_pascal_issue29_followups.md` and does not answer here. Prompt codes in this file are
`M-YYYYMMDD-i58dup-<n>`, so they cannot collide with the migration thread's `i58` codes.

Standing rules for every block in this file:
- **Never open, hash or copy another user's files**, except under the shared climate root `$CL` when a block
  says so. Groups owned by other people go to IT or to the owner; Pete identifies owners from the private
  mapping on the node.
- **Usernames and real paths stay on the node**, in git-ignored `logs/p15_*` and `logs/dup_*`. The RESPONSE uses
  the census tags (`owner-NN`, `self`) and paths as `$B/…`, `$W/…`, `$CL/…`, `$CD/…`. No hostnames or IPs.
- Read-only outside `logs/`: no `rm`, `mv`, `cp` or `chmod` of anything there. The only deletes allowed are of
  temporary files the block itself created under `logs/`. A block may name an exception: specific files of ours to
  rewrite, and how (DUP2 is the first).
- Long reads at idle I/O priority (`ionice -c3 nice -n 19`), one hashing stream at a time.

---

### RESPONSE — pascal [M-20261120-i58dup-2] 2026-11-20 (interim) — DUP2: **L0 clean, L1 pilot 10/10 `ok`, L2 running since 20:03 EAT.** The pilot saved 10.35 GB on 10 files, each shrunk to 0.85-3.41 % of its size with every check passing. **Peak RSS was 25.2 GiB, about twice the block's 12 GiB figure.** This session's cgroup allows 96 GiB, so the run is safe here, but a Small (32 GiB) profile would have little margin. **Projected finish: about 40 h, around midday EAT on 2026-11-22.**

#### L0
- On `develop`, at the DUP2 commit and after it.
- `logs/dup2_all_$S.txt`: **1,848** paths. **0** fall outside `$W/hazards_prototype/Data/hazard_risk/{annual,jagermeyr}/`.
- `*.dup2tmp*` under `hazard_risk` before the run: **0**.
- `pgrep` for `Rscript|R --|R_HOME|rsession`: nothing of ours. The only match was the `pgrep` shell itself. Pete confirms the migration session is not running R/2, R/3 or a publish on this tree.
- `df` for `$W` before: **50T size, 26T used, 24T available, 53 %**.
- The session has a 96 GiB memory cap (`memory.max`), which is more than this block needs.
- Each `.tif` in these folders has a `.tif.aux.xml` sidecar holding only per-band `Description` and `NoDataValue`. `gdal_translate` writes both into the new GTiff. I checked for a sidecar beside the temporary copy as well as the copy itself (`*dup2tmp*`), and found none. The originals' `.aux.xml` files are left as they were. Their content matches what the compressed file now carries.

#### L1 — pilot
The "smallest five" are also 1057 MB, because every file of 100 MB or more in these folders is a 48-band 1663 × 1739 float32 grid. The pilot is therefore ten files of one size: the 5 largest are in `jagermeyr`, and the 5 smallest are in `annual`.

| File | Before → after | % | Seconds |
|---|---|---:|---:|
| `jagermeyr/cattle-tropical_ENSEMBLEsd_moderate.tif` | 1057.0 MB → 36.0 MB | 3.41 | 86.6 |
| `jagermeyr/poultry-highland_ENSEMBLEsd_extreme.tif` | 1057.0 MB → 22.8 MB | 2.16 | 71.3 |
| `jagermeyr/poultry-tropical_ENSEMBLEsd_extreme.tif` | 1057.0 MB → 27.4 MB | 2.60 | 80.8 |
| `jagermeyr/poultry-highland_ENSEMBLEsd_moderate.tif` | 1057.0 MB → 35.6 MB | 3.37 | 90.5 |
| `jagermeyr/poultry-tropical_ENSEMBLEsd_moderate.tif` | 1057.0 MB → 36.0 MB | 3.41 | 69.5 |
| `annual/tea_EC-Earth3_severe.tif` | 1057.0 MB → 15.7 MB | 1.48 | 82.5 |
| `annual/tea_INM-CM5-0_severe.tif` | 1057.0 MB → 13.3 MB | 1.25 | 81.4 |
| `annual/bean_EC-Earth3_severe.tif` | 1057.0 MB → 10.9 MB | 1.03 | 86.7 |
| `annual/bean_INM-CM5-0_severe.tif` | 1057.0 MB → 9.0 MB | 0.85 | 59.2 |
| `annual/rice_EC-Earth3_severe.tif` | 1057.0 MB → 16.0 MB | 1.51 | 81.7 |

- `done: 10 compressed, 0 already compressed, 10.35 GB saved`. Wall **13:16**; peak RSS **25.2 GiB**; exit 0. Status counts: **10 `ok`**, 0 `FAIL`.
- `*dup2tmp*` left under `hazard_risk`: **0**. `logs/dup2_tmp/` is empty.
- Mode and mtime are unchanged on **10 of 10**, compared by `ls -l` before and after.
- **Opened as a reader would** (`terra::rast`, `global(r, "notNA")`):
  - `jagermeyr/cattle-tropical_ENSEMBLEsd_moderate.tif`: 48 layers, first name `ssp126_ENSEMBLEsd_2021-2040_wet_PTOT-G_cattle-tropical_Moderate`, 58,217,728 non-NA cells, `COMPRESSION=ZSTD`.
  - `annual/rice_EC-Earth3_severe.tif`: 48 layers, 59,643,551 non-NA cells, `ZSTD`.
  - Both load and hold data.

#### L2 — running
- `logs/dup2_run_$S.sh` started at 20:03:13 EAT, at `ionice -c3 nice -n 19`.
- After 2 minutes it had **skipped the pilot files as `skip-compressed`** and logged 2 new `ok` rows, at 1.27 % / 66.8 s and 1.31 % / 57.6 s. The next file's `.dup2tmp.tif` is being written, as expected.
- At the pilot's rate of about 79 s per file, the remaining 1,836 files take **about 40 h**.
- If the run stops (a `FAIL`, or the hub session is culled), I will report where it stopped and will not restart it until Pete answers.

**L4** (read-only header scan of our 17,896 smaller `.tif` files, 280.93 GB) is running alongside. Its result goes in the final RESPONSE.

**Writes so far outside `logs/`:** the files compressed so far (listed in `logs/dup2_log_$S.tsv`) and their transient `.dup2tmp.tif` copies, all in the two listed folders. No other writes.

Answers: M-20261120-i58dup-2 (interim)

### MACBOOK 2026-11-20 [M-20261120-i58dup-2] — DUP2: compress our own `hazard_risk` rasters in place, losslessly (about 1.9 TB back). **Any profile with 12 GiB RAM or more (DUP1 K4 peaked at 8.7 GiB). A pilot of 10 files, then about a day and a half in the background. The first block in this thread that changes data outside `logs/`.**

**Why.** DUP1 found our 1,848 rasters in `$W/hazards_prototype/Data/hazard_risk/{annual,jagermeyr}` (1.953 TB) are all
uncompressed. ZSTD shrank three of them to 1.2-4.5 % with every value identical. Removing duplicates gives back far less: 36 GB,
in `$CL`, which is not ours. Pete asked for this block on 2026-11-20; pushing it is his go-ahead. Keeping both `annual` and `jagermeyr` is a separate
question. This block compresses both and deletes neither.

**What it changes, exactly.** Only the files listed as `NONE` in DUP1's `logs/dup_k3_compression_$S.tsv`, all of them ours,
all under those two folders. Each one is rewritten by `scripts/compress_rasters_lossless.R` (new; read its header). It writes a ZSTD copy beside the
original, checks the copy against the original, and only then renames it over the original. The checks are: same dimensions, extent, CRS, layer names, type, NA flag
and tags; every cell equal; the same NA pattern. The original's permissions and modification time are kept. On the first failed check, the script
deletes that copy, logs `FAIL` and stops, leaving the original untouched. It skips files that are already compressed, so a stopped run
can simply be started again. Nothing is uploaded: the `s3://digital-atlas` copies are untouched.

**Exception to this file's read-only rule:** for this block only, the script may create `<file>.dup2tmp.tif` beside each listed
file and rename it over that file. Nothing else outside `logs/` is written, deleted, moved or chmod-ed.

#### L0 — pull, roots, nothing else using the rasters
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD
export W=<as in DUP1>  S=20261119_192126
git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD      # develop, at or after the DUP2 commit
H=$W/hazards_prototype/Data/hazard_risk
awk -F'\t' 'NR>1 && $3=="NONE" {print $2}' logs/dup_k3_compression_$S.tsv > logs/dup2_all_$S.txt
wc -l < logs/dup2_all_$S.txt                                                      # 1,848 expected
grep -v -c -e "^$H/annual/" -e "^$H/jagermeyr/" logs/dup2_all_$S.txt              # must be 0
find $H -name '*.dup2tmp.tif' | wc -l                                             # 0 expected on a first run
pgrep -u "$(id -un)" -af 'Rscript|R --|R_HOME' || echo "no R processes of ours"      # list them; see below
df -h $W | tail -1
```
**Stop and describe** if the count is not 1,848, if any path falls outside the two folders, or if another job of ours is
running anything that reads or writes `Data/hazard_risk` (R/2, R/3, R/3.1, a publish). An idle notebook kernel is fine;
report what `pgrep` lists. The migration session shares this account and checkout, so Pete must also confirm it is not
running R/2 or R/3 on this tree.

#### L1 — pilot: 10 files, largest five and smallest five
```bash
awk -F'\t' 'NR>1 && $3=="NONE" {print $1"\t"$2}' logs/dup_k3_compression_$S.tsv | sort -n | cut -f2 > logs/dup2_bysize_$S.txt
{ tail -5 logs/dup2_bysize_$S.txt; head -5 logs/dup2_bysize_$S.txt; } > logs/dup2_pilot_$S.txt
mkdir -p logs/dup2_tmp
ionice -c3 nice -n 19 /usr/bin/time -v Rscript scripts/compress_rasters_lossless.R --list logs/dup2_pilot_$S.txt \
  --log logs/dup2_log_$S.tsv --tmpdir $PWD/logs/dup2_tmp > logs/dup2_pilot_$S.log 2>&1
tail -3 logs/dup2_pilot_$S.log; grep -E "Elapsed|Maximum resident" logs/dup2_pilot_$S.log
cut -f2 logs/dup2_log_$S.tsv | sort | uniq -c                                      # 10 ok expected
find $H -name '*.dup2tmp.tif' | wc -l                                             # must be 0
```
Then open two of the pilot files from the R prompt as a reader would (`terra::rast(f)`; `global(r, "notNA")`) and
check they load and hold data. **Stop and describe** on any `FAIL`, a leftover `.dup2tmp.tif`, or a file that will
not open. Otherwise report the 10 rows (file name only, size before → after, %, seconds) and go on.

#### L2 — the rest, in the background
```bash
cat > logs/dup2_run_$S.sh <<SH
ionice -c3 nice -n 19 Rscript scripts/compress_rasters_lossless.R --list logs/dup2_bysize_$S.txt \
  --log logs/dup2_log_$S.tsv --tmpdir $PWD/logs/dup2_tmp
echo "compress exit=\$?"
SH
nohup /usr/bin/time -v bash logs/dup2_run_$S.sh > logs/dup2_run_$S.log 2>&1 &
echo $! > logs/dup2_run_$S.pid
```
The pilot files are skipped as already compressed. Expect about a day and a half at idle priority (DUP1 K4 ran at about 60 s per GB). **After launching,
prepend an interim RESPONSE**: L0, the pilot table, the projected finish time. End it `Answers: M-20261120-i58dup-2 (interim)`.
If the run stops (a `FAIL`, or the hub session is culled), say where it stopped and do not restart it until Pete answers.

#### L3 — finish
```bash
grep -E "compress exit|Elapsed" logs/dup2_run_$S.log; tail -1 logs/dup2_run_$S.log
cut -f2 logs/dup2_log_$S.tsv | sort | uniq -c
find $H -name '*.dup2tmp.tif' | wc -l                                             # must be 0
du -s -B1 $H/annual $H/jagermeyr; df -h $W | tail -1
```
Report: files `ok` / `skip-compressed` / `FAIL`; bytes before and after for each folder; the smallest and largest
ratio; wall time; `df` before (L0) and after. Then prepend the final RESPONSE.

#### L4 — read-only: the rest of our GeoTIFFs (can run alongside L2)
DUP1 K3 only looked at files of 100 MB or more. Do the same header scan for our smaller `.tif` under `$W`, e.g.
`hazard_timeseries_risk` (258 GB). Use K3's commands with `$1<1e8` in place of `$1>=1e8`, writing to `logs/dup2_small_*`.
Report compression × files × GB, and the top folders holding `NONE` bytes. **Do not compress them**: that would be a later block.

**Stop and describe** on any of: not on `develop`; the L0 checks; any `FAIL`; a leftover `.dup2tmp.tif`; a file that
will not open; any write outside `logs/` other than the listed files and their `.dup2tmp.tif`; anything touching another
user's files.

**To answer:** prepend `### RESPONSE — pascal [M-20261120-i58dup-2]` to **this file** (interim after L2 starts, final after
L3), run the IP grep over the response, commit only this file (never `logs/`), `git pull --rebase`, push, verify it
landed, and end with `Answers: M-20261120-i58dup-2 (interim)` or `(complete)`, or `(stopped at <gate>)`.

---

### RESPONSE — pascal [M-20261119-i58dup-1] 2026-11-20 — DUP1: **complete, no stop rule fired.** **Compression is the large saving, not duplicates.** Our 1.95 TB of `hazard_risk` rasters are all uncompressed. A lossless ZSTD rewrite took three test files to **1.2-4.5 %** of their size, with values bit-identical. That points to about **1.9 TB recoverable** from our own area. By contrast, the `$CL` duplicate groups are **36 of 36 identical by md5, 35.85 GB**, and **we hold no duplicates of our own** once annual vs jagermeyr is excluded.

| | |
|---|---|
| Our area (`$W`) by owner | **owner-02 2,401.43 GB** (144,744 files); **self 2,239.76 GB** (33,728 files) |
| Confirmed duplicates in `$CL` | **36 groups, 72 files, all identical; 35.85 GB recoverable** (one copy of each kept) |
| Confirmed duplicates in our own data | **0 groups.** All 298 `self` groups were annual vs jagermeyr, already a false match. |
| Our GeoTIFFs >= 100 MB | **1,848 files, 1.953 TB, all `NONE`** (uncompressed) |
| Lossless ZSTD on 3 test files | 4.5 %, 1.2 %, 1.2 % of original size; `max|diff|=0`, `NA-same=TRUE` |
| Other owners' probable groups (K5) | owner-01 95, owner-03 36, owner-04 6, owner-07 1, owner-06 1 (GB below) |
| Privacy count (`grep -c -F -f logs/p15_names_$S.txt`) over this RESPONSE | **0** |

#### K0
On `develop` at the DUP1 commit. `logs/dup_*` is ignored. `S=20261119_192126`. The P15 private folder is present. The listing is in this checkout's
own `logs/` (the migration session works in the same checkout); it has **1,096,294** lines. The session ran under a 20-CPU cgroup, not Small.
Everything here is single-stream or single-threaded, so that made no difference.

#### K1 — who owns our area

| Owner | Files | GB |
|---|---:|---:|
| owner-02 | 144,744 | 2,401.43 |
| self | 33,728 | 2,239.76 |

| Owner | `$W/…` (two levels) | GB |
|---|---|---:|
| owner-02 | `$W/hazards_prototype/Data` | 2,401.43 |
| self | `$W/hazards_prototype/Data` | 2,234.24 |
| self | `$W/hazards_upstream_pascal/nex-gddp-cmip6` | 2.62 |
| self | `$W/hazards_upstream_pascal/nex-gddp-cmip6_raw` | 1.56 |
| self | `$W/hazards_upstream_pascal/nex-gddp-cmip6_indices` | 0.97 |
| self | `$W/atlas_obs_hazards_pascal/Data` | 0.21 |
| self | `$W/atlas_obs_hazards_smoke_pascal/nex-gddp-cmip6` | 0.10 |
| self | `$W/hazards_upstream_pascal/chirps_wrld` | 0.06 |
| self | `$W/atlas_obs_hazards_pascal/manifests`, `$W/atlas_obs_hazards_smoke_pascal/manifests` | 0.00 |

This answers P15's question. **The other 2.40 TB of `$W/hazards_prototype` belongs to one other account, owner-02**, and it is all
under `Data`. One level further down:

| Owner | `$W/hazards_prototype/Data/…` | GB |
|---|---|---:|
| owner-02 | `indices_seasonal` | 2,114.88 |
| self | `hazard_risk` | 1,970.76 |
| self | `hazard_timeseries_risk` | 257.92 |
| owner-02 | `hazard_timeseries_class` | 138.08 |
| owner-02 | `hazard_timeseries_int` | 132.34 |
| owner-02 | `hazard_timeseries_risk` | 16.13 |
| self | `mapspam`, `hydrobasins`, `fao`, `GLW4_2020`, `boundaries`, `GLW4`, and 9 smaller | 5.6 together |

owner-02's whole P15 total (2,401.43 GB) sits in this one folder. It is probably an earlier pipeline account that wrote
`indices_seasonal` and the `class`/`int` timeseries. None of it was opened.

#### K2 — md5 of the `$CL` groups and our own groups
The selection gave **72 files in 36 groups, all `CL`; 0 `self`**. Every `self` group in P15 is annual vs jagermeyr, which the block excludes.
Read volume was **71.70 GB**, under the 100 GB bound. One `md5sum` stream at idle I/O priority: `md5 exit=0`, wall **17:16**, 72 hashes, no
`Permission denied`.

**All 36 groups are `identical`: 35.85 GB confirmed recoverable in `$CL`, 0 in our own data.** Largest first, each 2 copies:

| Size each | Verdict | Copies |
|---:|---|---|
| 3.15 GB | identical | `$CL/cmip6/daily/pr_day_MRI-ESM2-0_ssp585_r1i1p1f1_gn_22510101-23001231.nc`<br>`$CL/cmip6/mme_cmip/precip/…same name` |
| 3.15 GB | identical | `$CL/cmip6/daily/pr_day_MRI-ESM2-0_ssp585_r1i1p1f1_gn_22010101-22501231.nc`<br>`$CL/cmip6/mme_cmip/precip/…` |
| 3.14 GB | identical | `$CL/cmip6/daily/pr_day_MRI-ESM2-0_ssp585_r1i1p1f1_gn_20150101-20641231.nc`<br>`$CL/cmip6/mme_cmip/precip/…` |
| 2.26 GB | identical | `$CL/cmip6/daily/pr_day_MRI-ESM2-0_ssp585_r1i1p1f1_gn_20650101-21001231.nc`<br>`$CL/cmip6/mme_cmip/precip/…` |
| 1.82 GB | identical | `$CL/cmip6/daily/pr_day_ACCESS-ESM1-5_historical_r1i1p1f1_gn_19000101-19491231.nc`<br>`$CL/cmip6/raw/daily/…` |
| 1.82 GB | identical | `$CL/cmip6/daily/pr_day_ACCESS-ESM1-5_historical_r1i1p1f1_gn_19500101-19991231.nc`<br>`$CL/cmip6/raw/daily/…` |
| 1.57 GB | identical | `$CL/cmip6/daily/tasmax_day_ACCESS-ESM1-5_historical_r1i1p1f1_gn_19500101-19991231.nc`<br>`$CL/cmip6/raw/daily/…` |
| 1.48 GB | identical | `$CL/cmip6/daily/pr_day_INM-CM5-0_ssp585_r1i1p1f1_gr1_20150101-20641231.nc`<br>`$CL/cmip6/mme_cmip/precip/…` |
| 1.24 GB | identical | `$CL/cmip6/daily/tasmin_day_INM-CM5-0_ssp585_r1i1p1f1_gr1_20150101-20641231.nc`<br>`$CL/cmip6/mme_cmip/temp/temp/…` |
| 1.20 GB | identical | `$CL/worldclim/wc2.1_30s_bio_4.tif`<br>`$CL/worldclim/wc2.1_30s/wc2.1_30s_bio_4.tif` |
| 1.07 GB | identical | `$CL/cmip6/daily/pr_day_INM-CM5-0_ssp585_r1i1p1f1_gr1_20650101-21001231.nc`<br>`$CL/cmip6/mme_cmip/precip/…` |
| 1.07 GB | identical | `$CL/worldclim/wc2.1_30s_bio_3.tif`<br>`$CL/worldclim/wc2.1_30s/…` |
| 1.01 GB | identical | `$CL/worldclim/wc2.1_30s_bio_15.tif`<br>`$CL/worldclim/wc2.1_30s/…` |
| 0.96 GB | identical | `$CL/cmip6/daily/tasmin_day_INM-CM5-0_historical_r1i1p1f1_gr1_19500101-19991231.nc`<br>`$CL/cmip6/mme_cmip/temp/temp/…` |
| 0.95 GB | identical | `$CL/cmip6/daily/tasmax_day_INM-CM5-0_historical_r1i1p1f1_gr1_19500101-19991231.nc`<br>`$CL/cmip6/mme_cmip/temp/temp/…` |
| 0.89 GB | identical | `$CL/cmip6/daily/tasmin_day_INM-CM5-0_ssp585_r1i1p1f1_gr1_20650101-21001231.nc`<br>`$CL/cmip6/mme_cmip/temp/temp/…` |
| 0.55 GB | identical | `$CL/cmip6/daily/pr_day_ACCESS-ESM1-5_historical_r1i1p1f1_gn_20000101-20141231.nc`<br>`$CL/cmip6/raw/daily/…` |
| 0.47 GB | identical | `$CL/cmip6/daily/tasmax_day_ACCESS-ESM1-5_historical_r1i1p1f1_gn_20000101-20141231.nc`<br>`$CL/cmip6/raw/daily/…` |
| 0.29 GB | identical | `$CL/cmip6/daily/tasmin_day_INM-CM5-0_historical_r1i1p1f1_gr1_20000101-20141231.nc`<br>`$CL/cmip6/mme_cmip/temp/temp/…` |
| 0.28 GB | identical | `$CL/cmip6/daily/tasmax_day_INM-CM5-0_historical_r1i1p1f1_gr1_20000101-20141231.nc`<br>`$CL/cmip6/mme_cmip/temp/temp/…` |
| 0.18-0.75 GB | identical | the other 16 WorldClim `wc2.1_30s_bio_{1,2,5-14,16-19}.tif`: `$CL/worldclim/<name>` and `$CL/worldclim/wc2.1_30s/<name>` |

Per family:

| Family | Groups | GB |
|---|---:|---:|
| `$CL/cmip6/daily` vs `$CL/cmip6/mme_cmip/precip` | 6 | 14.24 |
| `$CL/worldclim` vs `$CL/worldclim/wc2.1_30s` | 19 | 10.77 |
| `$CL/cmip6/daily` vs `$CL/cmip6/raw/daily` | 5 | 6.22 |
| `$CL/cmip6/daily` vs `$CL/cmip6/mme_cmip/temp/temp` | 6 | 4.61 |
| **Total** | **36** | **35.85** |

Each `$CL` pair is a flat copy next to an organised one. Which copy to keep depends on which path scripts read: WorldClim root vs
`wc2.1_30s/`, `cmip6/daily` vs `mme_cmip/…`/`raw/daily`. `$CL` belongs to owner-01, so that decision is theirs or IT's.

#### K3 — how much of our own data is uncompressed
Our `.tif` files of 100 MB or more under `$W`: **1,848 files, 1.953 TB.** Header scan (`terra::describe`, 4:55 wall, exit 0):

| Compression | Files | TB |
|---|---:|---:|
| NONE | 1,848 | 1.953 |

**Every one is uncompressed**, and they are all in two folders:

| Folder | Files | GB `NONE` |
|---|---:|---:|
| `$W/hazards_prototype/Data/hazard_risk/annual` | 924 | 976.65 |
| `$W/hazards_prototype/Data/hazard_risk/jagermeyr` | 924 | 976.65 |

Our other `self` data, mainly `hazard_timeseries_risk` (258 GB), has no `.tif` of 100 MB or more, so this scan does not cover it.
Its compression is not measured here.

#### K4 — lossless ZSTD on three of our files
GDAL 3.8.4 has ZSTD, so the DEFLATE fallback was not needed. Files: the largest, the smallest and the 100th by name in `annual`:

| File | Type | Bands | Codec | Size | `max\|diff\|` | NA same |
|---|---|---:|---|---|---:|---|
| `poultry-highland_ENSEMBLEsd_moderate.tif` | FLT4S | 48 | ZSTD | 1057 MB -> 48 MB (**4.5 %**) | 0 | TRUE |
| `tea_historic_severe.tif` | FLT4S | 3 | ZSTD | 66 MB -> 1 MB (**1.2 %**) | 0 | TRUE |
| `cassava_EC-Earth3_extreme.tif` | FLT4S | 48 | ZSTD | 1057 MB -> 13 MB (**1.2 %**) | 0 | TRUE |

The settings were `COMPRESS=ZSTD`, `ZSTD_LEVEL=9`, `PREDICTOR=3`, `TILED=YES`. Wall 2:11, peak RSS 8.7 GiB. terra's temporary files went to `logs/dup_k4_tmp`.
After the run, `ls logs/dup_k4_*` held only the block's `.R`/`.log`/`.pid` and an **empty** `dup_k4_tmp/`. **0** test rasters remained.

**What it implies (estimate, 3 samples):** the two folders would shrink from 1.953 TB to roughly 25-90 GB, so **about 1.86-1.93 TB recoverable**.
That is about 50 times the confirmed `$CL` duplicates. The ratios are so small because these rasters are mostly NA (land cells for one crop), stored
as full float32 grids. The ratio will vary by crop; a dry run over all 1,848 files would firm up the number. The same check would be worth doing
on owner-02's `indices_seasonal` (2.11 TB), but that is another user's data, so it is not opened here.

#### K5 — private contact list for the groups we do not hash
`logs/dup_k5_private_contacts_$S.tsv` has 217 rows (kind, name, size, recoverable, path, owner, tag). It stays on the node.
Per tag (a group shared by two owners counts under both):

```
owner-01	95 groups	356.47 GB
owner-03	36 groups	78.10 GB
owner-04	6 groups	2.57 GB
owner-07	1 groups	1.00 GB
owner-06	1 groups	0.62 GB
```

owner-01's 356 GB includes the two llama base-vs-chat trees (151.45 GB), which P15 judged almost certainly false matches. It also includes the 29
AICCRA-vs-CCI `rotated_*` groups (65.72 GB), which are shared with owner-03 and so appear under both tags. Neither owner-02 nor self appears.

**Writes:** only under `logs/` (`dup_k1*`, `dup_k2*`, `dup_k3*`, `dup_k4*`, `dup_k5*`). The only deletes were the block's own three test rasters,
done by the script itself. No other user's files were opened outside `$CL`.

Answers: M-20261119-i58dup-1 (complete)

### MACBOOK 2026-11-19 [M-20261119-i58dup-1] — DUP1: confirm by content what we may hash; measure what compression would save; private contact list for the rest. **Small profile. Mostly one `md5sum` stream over about 40 GB, plus three test rewrites.**

**Why.** P15 (`[M-20261119-i58-4]`, RESPONSE in `DISPATCH_pascal_issue29_followups.md`) listed 711 GB of
probable duplicates by name and size, about 245 GB after known false matches, **1.0 % of the 24.57 TB used**. So
removing duplicates will not free much space. This block does three things:
1. **Confirms by md5** the groups we are allowed to hash: those that sit wholly in the shared climate tree `$CL`, and any of our own (`self`)
   groups apart from `hazard_risk/annual` vs `…/jagermeyr`. That pair is already settled: P15 hashed three pairs and all three
   differ, so it is recorded as a false match and not hashed again.
2. **Measures our own uncompressed rasters.** P15 found the `hazard_risk` rasters are written uncompressed (sizes
   depend only on band count). That is about 2 TB of our own data. If lossless compression shrinks it a lot, that saves more than all the duplicates together.
3. **Writes a private, per-owner contact list** of the other people's groups, so Pete can contact owners or IT. Only
   counts and GB per tag go in the RESPONSE.

It also answers one question P15 raised: `$B/atlas/hazards_prototype` holds 4.64 TB but `self` owns 2.24 TB. Who owns
the rest of our area (by tag)?

#### K0 — pull, roots, the P15 outputs
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD
export B=<as in P15>  W=<as in P15>  CL=<as in P15>  CD=<as in P15>
export S=20261119_192126
git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD      # develop, at or after the DUP1 commit
git check-ignore -q logs/dup_test && echo "logs/dup_* ignored"                    # must print; else stop
export P=logs/p15_private_$S L=<absolute path of the P15 listing, p15_files_$S.tsv>
ls $P/owner_usage.tsv $P/dup_files.tsv $P/dup_trees.tsv && wc -l < $L             # 1,096,294 lines expected
```

#### K1 — who owns our area (tags only in the RESPONSE)
```bash
awk -F'\t' -v w="$W/" 'NR==FNR { if (FNR>1) t[$1]=$2; next }
     index($3, w)==1 { b[t[$2]]+=$1; n[t[$2]]++ }
     END { for (k in b) printf "%s\t%d\t%.2f\n", k, n[k], b[k]/1e9 }' $P/owner_usage.tsv $L | sort -k3,3nr
awk -F'\t' -v w="$W/" 'NR==FNR { if (FNR>1) t[$1]=$2; next }
     index($3, w)==1 { split(substr($3, length(w)+1), a, "/"); k=t[$2]"\t"a[1]"/"a[2]; b[k]+=$1 }
     END { for (k in b) printf "%s\t%.2f\n", k, b[k]/1e9 }' $P/owner_usage.tsv $L | sort -k3,3nr | head -20
```
Report both tables as tags and `$W/…` paths (GB).

#### K2 — md5 of the `$CL` groups and the remaining `self` groups (background, idle I/O)
```bash
python3 -I - "$P/dup_files.tsv" "$CL" "$W" > logs/dup_k2_paths_$S.tsv <<'PY'
import sys, csv, collections
f, cl, w = sys.argv[1], sys.argv[2].rstrip("/") + "/", sys.argv[3].rstrip("/") + "/"
g = collections.defaultdict(list)
for r in csv.DictReader(open(f), delimiter="\t"):
    g[(r["name"], r["size"])].append(r["dir"])
for i, ((name, size), ds) in enumerate(sorted(g.items(), key=lambda kv: -int(kv[0][1]) * (len(kv[1]) - 1))):
    if all(d.startswith(cl) for d in ds): kind = "CL"
    elif all(d.startswith(w) for d in ds) and not any("/hazard_risk/jagermeyr" in d for d in ds): kind = "self"
    else: continue
    for d in ds: print(f"{i}\t{kind}\t{size}\t{d}/{name}")
PY
awk -F'\t' '{print $2}' logs/dup_k2_paths_$S.tsv | sort | uniq -c                     # files per kind
awk -F'\t' '{s[$2]+=$3} END {for (k in s) printf "%s %.2f GB to read\n", k, s[k]/1e9}' logs/dup_k2_paths_$S.tsv
```
**Stop and describe** if the `self` kind would read more than 100 GB, or `CL` more than 100 GB (P15 suggests about
36 GB of `$CL` groups, read twice ≈ 72 GB). Otherwise:
```bash
cat > logs/dup_k2_$S.sh <<'SH'
cut -f4 logs/dup_k2_paths_$S.tsv | tr '\n' '\0' | ionice -c3 nice -n 19 xargs -0 -r md5sum > logs/dup_k2_md5_$S.txt
echo "md5 exit=$?"
SH
nohup /usr/bin/time -v bash logs/dup_k2_$S.sh > logs/dup_k2_$S.log 2>&1 &
echo $! > logs/dup_k2_$S.pid
```
Afterwards, per group: `identical` (one md5 for every copy) or `DIFFERENT`. Report a table, largest first: group, kind,
copies, size each, verdict, and the copies as `$CL/…` / `$W/…` paths. Total confirmed-recoverable bytes per kind.
`Permission denied` is a finding, not a stop.

#### K3 — how much of our own data is written uncompressed
Our GeoTIFFs of 100 MB or more under `$W` only, header reads only:
```bash
awk -F'\t' -v w="$W/" -v me="$(id -un)" 'index($3, w)==1 && $2==me && $1>=1e8 && $4 ~ /\.tif$/ {print $1"\t"$3"/"$4}' $L > logs/dup_k3_tifs_$S.tsv
wc -l < logs/dup_k3_tifs_$S.tsv
Rscript -e 'S <- Sys.getenv("S"); x <- read.delim(paste0("logs/dup_k3_tifs_", S, ".tsv"), header = FALSE, col.names = c("bytes","path"), quote = "")
  cmp <- vapply(x$path, function(p) { d <- tryCatch(terra::describe(p), error = function(e) "ERR")
    m <- grep("COMPRESSION=", d, value = TRUE); if (length(m)) sub(".*COMPRESSION=", "", m[1]) else if (identical(d, "ERR")) "ERR" else "NONE" }, "")
  x$compression <- cmp; write.table(x, paste0("logs/dup_k3_compression_", S, ".tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
  print(aggregate(bytes ~ compression, x, function(b) c(files = length(b), TB = sum(b) / 1e12)))'
```
Report the table (compression, files, TB) and the top-level `$W/…` folders holding the `NONE` bytes (GB each).

#### K4 — what lossless compression would save (three of our files)
Take the largest, the smallest and the 100th-by-name `.tif` in `$W/hazards_prototype/Data/hazard_risk/annual`, rewrite
each **into `logs/`** with ZSTD (fall back to DEFLATE if the GDAL build lacks ZSTD), check the values are identical, record the sizes, then delete the three test files:
```bash
Rscript -e 'library(terra)
  d <- file.path(Sys.getenv("W"), "hazards_prototype/Data/hazard_risk/annual")
  f <- list.files(d, "\\.tif$", full.names = TRUE, recursive = TRUE); s <- file.size(f)
  pick <- unique(c(f[which.max(s)], f[which.min(s)], sort(f)[min(100, length(f))]))
  for (p in pick) {
    r <- rast(p); dt <- datatype(r)[1]; pred <- if (grepl("^FLT", dt)) "PREDICTOR=3" else "PREDICTOR=2"
    o <- file.path("logs", paste0("dup_k4_", basename(p)))
    ok <- tryCatch({ writeRaster(r, o, datatype = dt, overwrite = TRUE,
                       gdal = c("COMPRESS=ZSTD", "ZSTD_LEVEL=9", pred, "TILED=YES")); "ZSTD" },
                   error = function(e) { writeRaster(r, o, datatype = dt, overwrite = TRUE,
                       gdal = c("COMPRESS=DEFLATE", "ZLEVEL=6", pred, "TILED=YES")); "DEFLATE" })
    m <- grep("COMPRESSION=", describe(o), value = TRUE); ok <- if (length(m)) sub(".*COMPRESSION=", "", m[1]) else "NONE"
    q <- rast(o); diff <- max(global(abs(r - q), "max", na.rm = TRUE)[, 1], na.rm = TRUE)
    na_same <- all(global(is.na(r), "sum")[, 1] == global(is.na(q), "sum")[, 1])
    cat(sprintf("%s\t%s\t%d bands\t%s\t%.0f MB -> %.0f MB (%.1f %%)\tmax|diff|=%g\tNA-same=%s\n",
        basename(p), dt, nlyr(r), ok, file.size(p)/1e6, file.size(o)/1e6, 100*file.size(o)/file.size(p), diff, na_same))
    unlink(o)
  }'
ls logs/dup_k4_* 2>/dev/null | wc -l                                                # must be 0
```
Report the three lines. If any `max|diff|` is not 0 or `NA-same` is FALSE, **stop and describe**: that would mean
the rewrite is not lossless.

#### K5 — private contact list for the groups we do not hash
```bash
python3 -I - "$P" "$CL" "$W" <<'PY'
import sys, csv, collections, os
p, cl, w = sys.argv[1], sys.argv[2].rstrip("/") + "/", sys.argv[3].rstrip("/") + "/"
tag = {r["owner"]: r["tag"] for r in csv.DictReader(open(os.path.join(p, "owner_usage.tsv")), delimiter="\t")}
rows, pub = [], collections.defaultdict(lambda: [0, 0.0])
for fn, kind in (("dup_trees.tsv", "tree"), ("dup_files.tsv", "file")):
    g = collections.defaultdict(list)
    for r in csv.DictReader(open(os.path.join(p, fn)), delimiter="\t"):
        key = (r.get("name", ""), r.get("bytes_each") or r.get("size"), r["recoverable_bytes"])
        g[key].append(r)
    for key, rs in g.items():
        ds = [r.get("dir") or r.get("path") for r in rs]
        if all(d.startswith(cl) for d in ds) or all(d.startswith(w) for d in ds):
            continue                                   # ours or shared-climate: K2 handles them
        owners = sorted({r["owner"] for r in rs})
        for o in owners:
            pub[tag.get(o, "?")][0] += 1; pub[tag.get(o, "?")][1] += int(key[2]) / 1e9
        for r, d in zip(rs, ds):
            rows.append([kind, key[0], key[1], key[2], d, r["owner"], tag.get(r["owner"], "?")])
with open("logs/dup_k5_private_contacts_" + os.environ["S"] + ".tsv", "w") as o:
    o.write("kind\tname\tbytes_each\trecoverable\tpath\towner\ttag\n")
    for r in sorted(rows, key=lambda r: (r[5], -int(r[3]))): o.write("\t".join(map(str, r)) + "\n")
for t, (n, gb) in sorted(pub.items(), key=lambda kv: -kv[1][1]):
    print(f"{t}\t{n} groups\t{gb:.2f} GB")
PY
```
Report only the printed per-tag lines (a group shared by two owners is counted under both). The private file stays on
the node for Pete. **Privacy check before you paste anything:** run P15's
`grep -c -F -f logs/p15_names_$S.txt` over your RESPONSE text; it must be 0.

#### K6 — report
At the top: our area by owner tag (K1); confirmed duplicates in `$CL` and in our own data, with bytes (K2); how much of our own data is
uncompressed and the compression ratio measured (K3, K4); groups per other owner tag (K5); and the privacy count.

**Stop and describe** on any of: not on `develop`; `logs/dup_*` not ignored; K2 over its 100 GB bound; a lossy K4;
a non-zero privacy count; any write outside `logs/`; anything that would need reading another user's files outside `$CL`, a deletion
outside `logs/dup_k4_*`, or a move.

**To answer:** prepend one `### RESPONSE — pascal [M-20261119-i58dup-1]` to **this file**, run the IP grep over the
response, commit only this file (never `logs/`), `git pull --rebase` before pushing, push, verify it landed, and end
with `Answers: M-20261119-i58dup-1 (complete)` — or `(stopped at <gate>)`.
