# Dispatch: one 0.4.x correction pass — #38 millet split, #39 coverage guard, #40 touches, implied-price method (item 2 of HANDOVER_2026-10-01.md)

## cglabs response — Block A clean except the FAO-vintage gate; STOPPED before Block B (2026-10-03, 16473c5)

Repo at `16473c5`, develop, clean tree. Block A read-only, done. Everything passes **except** the FAO
file-vintage expectation — stopped there rather than run B against example numbers conditioned on a
different vintage (AGENTS.md §41; the block's own "any other vintage: STOP").

**Clean:**
- No real R running. (The only match is a months-old inert `r21_rerun.sh` bash wrapper — bash+tail, no
  R child, R21 log 2026-09-18 — same zombie noted before; not writing anything. Left it; prior kill was
  classifier-denied.) No `0.4.x` env set.
- `fixture_price_fill.R` → `ALL PRICE-FILL FIXTURE ASSERTIONS PASSED`; `fixture_vop_allocate.R` →
  `ALL VOP-ALLOCATE FIXTURE ASSERTIONS PASSED`.
- Cross-basis BEFORE state, **GATE PASS both resolutions**, reading
  `fao_prices/crop_price_nominal-usd-2021-t_fill-sources_<tag>.csv`:
  - res-25: 1360 pairs / 1007 material / 48 one-sided; residual 28 material pairs (CAF,ETH,KEN,MOZ,MWI,
    NER,NGA,RWA,SDN,TGO,UGA,ZWE).
  - res-05: 1374 / 1008 / 44 one-sided; residual 26 material pairs (BDI,ETH,KEN,MWI,NGA,RWA,SDN,TGO,
    UGA,ZWE). SDN + NGA + pearl-millet countries present — the state this pass removes.
- Inventory (what C parks): `variable=vop_intld15-2021` 24 files/74 MB · `vop_nominal-usd-2021`
  36/105.5 · `vop_nominal-usd-2015` 36/105.4 · `vop_nominal-usd-2020` 36/105.2 · `fao_prices` 12/86.6;
  **6 §3 tables + 6 sidecars** (exposure_adm_sum + vop_nominal-usd-2021 adm_sum mtime 2026-09-28 12:40-43;
  vop_intld15-2021 adm_sum mtime 2026-09-24 19:50-55); 42 per-tif `_adm_sum.parquet` caches in the two
  vop dirs.
- common_data: NFS mount, 192 T, **123 T free (37 % used)**. Ample.

**DEVIATION — FAO file vintage (the stop):**
```
Value_of_Production_E_Africa.csv                 2026-05-15  15 MB
Value_of_Production_E_All_Data.csv               2025-08-21  71 MB   <-- ~10 months older + size
Value_of_Production_E_All_Area_Groups.csv        2026-05-15  17 MB
Prices_E_Africa_NOFLAG.csv                       2026-05-15   6 MB
Prices_E_All_Data_(Normalized).csv               2026-05-15 214 MB
Production_Crops_Livestock_E_Africa_NOFLAG.csv   2026-05-15  13 MB
Production_Crops_Livestock_E_All_Area_Groups.csv 2026-05-15  23 MB
```
Two issues vs the block's expectation ("every other FAO file present with the 2026-05-14 mtime; any
other vintage: STOP"):
1. **Every file is 2026-05-15, not 2026-05-14** — one day off. Almost certainly download/copy skew of
   the same FAO release, but it is not the stated figure, so flagging rather than assuming.
2. **`Value_of_Production_E_All_Data.csv` is 2025-08-21, 71 MB** — present (so 0.4.0 will not auto-download),
   but ~10 months older than the rest, and the block warns 0.4.0 "downloads it otherwise — 1 GB". 71 MB
   vs ~1 GB suggests the node's copy may not be what the macbook measured on. Since the implied-price
   method is GPV ÷ production, the VoP vintage is exactly what sets Block B's stale-price / basis-fallback
   numbers.

**Not running Block B, not planning C's download, until this is resolved** — the Block B example numbers
(and the "row for row if same vintage" claim) are conditioned on the FAO set. Questions for macbook/Pete:
- Is the node's 2026-05-15 set the same FAO release you measured as 2026-05-14 (then B's invariant ranges
  apply and I proceed)?
- Is `Value_of_Production_E_All_Data.csv` 2025-08-21 (71 MB) the file you ran 0.4.0/0.4.2 against, or do
  you want a fresh ~1 GB pull first (planned, not a surprise mid-background-job)?

**STOP.** Node idle, nothing written, nothing published. Separately still open from the prior thread:
the S3 backup-retention flag (delete 2026-11-01 / on clean-confirmation) in the archived r3_res25 dispatch.

---

**Status:** code on `develop` — `715057f` (0.4.2 price chain), `95b2159` (0.4.0 allocation groups +
coverage guard + mapping table), `94a98fa` (`touches = TRUE`), the 2026-10-02 stale-price test + evidence
pins commit, plus the probes and this file. Blocks A
and B are read-only and runnable now. **Block C is value-changing and GO-gated; Block D writes to S3
and is GO-gated separately.** Do not start either without the GO line in this file.

**Append your response at the top of this file** as `## cglabs response — <summary> (<date>, <sha>)`,
newest block first; commit, push, and verify it landed (`git log origin/develop..HEAD` empty).

**Why.** The published exposure reference (`crop-livestock_all_res-05` / `_res-25` and the per-unit
family `vop_nominal-usd-2021` / `vop_intld15-2021`) carries three intld-side defects the cross-basis
gate exposed on 2026-09-28 — #38 (all FAOSTAT Millet value on pearl-millet), #39 (national value
over a negligible SPAM footprint: Sudan on every crop, Nigeria banana), #40 (Seychelles NaN on the
0.25° grid) — and Pete decided on 2026-10-01 to move the nominal price source to FAOSTAT's implied
price (GPV current US$ ÷ production) with a within-item basis guard. All four land in one 0.4.0 +
0.4.2 → 0.4.4 pass at both resolutions, then one republish of reference + family. Evidence and
decisions: `HANDOVER_2026-10-01_exposure-intld-fixes.md`.

**What changed in code (read before running):**
- `R/0.4.2_create_crop_vop_nominal_usd.R` §1.6.3 + §3, `R/price_fill.R`: own price = FAO GPV current
  US$ ÷ production, 2019-23 median, clipped 5× against World GPV ÷ World production per item-year;
  producer price (clipped vs the world producer-price median) as first fallback; longer series of each;
  then the neighbour → region → continent → world MEDIAN chain; then a **basis guard**: an own price
  whose nominal ÷ constant-I$ ratio sits beyond 4× the item's cross-country median is replaced by the
  item-median factor × the country's constant-I$ value (`price_source = "basis fallback"`). The tea
  floor hack is gone (the guard covers it). `price_usd_global` in the audit CSV is now the **world
  implied price** (the cross-basis gate reads it as its independent reference — the matching one for
  a GPV-derived nominal side). Measured on the macbook with the same 2026-05-14 FAO files: own
  prices cover 83 % of FAO production (was 56 %); 37 basis fallbacks at y2021; all-crops nominal
  total 0.82× the previous method.
- **Added 2026-10-02 (Pete GO after the evidence review):** (i) a **stale-local-price test** before the
  clip — FAO's GPV in current local currency per tonne over the window against the country's GDP
  deflator (FAO Deflators bulk); a pair whose real local unit value fell by more than half is a frozen
  imputation (Sudan millet = the 2013 price for 2019-21) and its implied prices are rejected for every
  year (27 of 564 pairs on the macbook: SDN 8, AGO 7, GHA 4, SLE 4, EGY/ETH/KEN/LSO 1); (ii) **evidence
  pins**, `metadata/price_pins.csv`, cited per row, applied last — one row today, AGO banana 300 USD/t.
  Basis fallbacks drop to 26-27; all-crops nominal total 0.83× (was 0.82× before these two). Methods
  text for the records and notebooks: `docs/methods/nominal_price_method.md`.
- `R/0.4.0_create_crop_vop_intld15.R` + new `R/vop_allocate.R`: GPV is distributed per **allocation
  group** — the SPAM layers sharing a FAO item (Millet → pearl + small millet; Coffee → arabica +
  robusta), items a SPAM crop spans (rapeseed = rape or colza seed + mustard seed), and the pooled
  banana family (`Bananas` + `Plantains and cooking bananas` → banana + plantain by SPAM share; FAO
  reports Nigeria's 7.4 Mt as Bananas with no Plantains item, SPAM codes it as plantain). **Coverage
  guard**: SPAM national t ÷ FAO production t per (country, group) below `VOP_COVERAGE_MIN` (0.10)
  → NA, logged, in `fao_prices/crop_vop_intld15-2021_allocation_<tag>.csv`. Found on the way:
  `terra::classify()` leaves unmatched admin IDs as values, so every country × crop with no GPV
  row carried its admin ID (thousand I$) as a spurious value — `others = NA` now. In-script zonal
  check: every allocated pair conserved to 1e-6, every guarded one empty, or the script stops.
- `metadata/SPAM2010_FAO_crops.csv`: `name_fao_val` placeholders for acof/rcof/smil replaced by the
  real items; `Rapeseed` → `Rape or colza seed`. Other readers (qaqc, align gate, 0.4.3) match on
  code or take the first code per item — totals unchanged.
- `touches = TRUE` on the 0.4.0 admin zone raster and the 0.4.2 price rasters (0.4.4 already had it).
  Beyond SYC this also returns value to **coastal cells whose centre is offshore** on res-25, on both
  bases — a small positive move for coastal countries, not a defect.
- Fixtures (no data, seconds): `R/checks/fixture_price_fill.R`, `R/checks/fixture_vop_allocate.R`.
  Probes (read-only): `R/checks/probe_042_price_fill.R` (now skips the audit-CSV write),
  `R/checks/probe_040_allocation.R` (new). Report: `R/checks/exposure_pair_drift.R` (new).

**Expectations are invariants, not figures.** Where a block says "PASS", "≥", "within", that is the
gate; a number quoted from the macbook run is an example of what the same files produced there, not
a target. **If anything deviates from a stated expectation, stop at that step and describe what you
see — do not improvise a fix.**

**Settled 2026-10-02 (Pete): Sudan is IN scope** — its intld rows come out NA by the coverage guard
(a SPAM 2020 SSA data gap, 0.05 Mt against ~15 Mt), and the CDH record will say so, not "out of scope".
**Still to settle (ask when you reach it, do not decide node-side):** (b) the basis
guard at 4× also catches low-side exchange-rate regimes (Angola on 8 crops, Guinea 5, Sudan 3) and
two Nigeria pairs (oil palm 45.8 → 190 USD/t, groundnut 151 → 719) — Block B prints the full list for
Pete to read before the GO.

---

## Block A — read-only audit (< 10 min, paste everything)

```bash
cd <hazards_prototype> && git fetch origin && git checkout develop && git pull --ff-only && git log -1 --oneline
pgrep -af Rscript || echo "no Rscript running"
env | grep -E '^(FORCE_OVERWRITE|EXPOSURE_|VOP_COVERAGE_MIN|PRICE_BAND|BASIS_)' || echo "no 0.4.x env set (good)"
Rscript R/checks/fixture_price_fill.R 2>&1 | tail -2
Rscript R/checks/fixture_vop_allocate.R 2>&1 | tail -2
# the BEFORE state of the gate, both resolutions (expect PASS with the intld residual named)
Rscript R/checks/vop_cross_basis_gate.R --res 0.25 2>&1 | grep -E 'world price|pairs:|residual|FAIL|GATE'
Rscript R/checks/vop_cross_basis_gate.R --res 0.05 2>&1 | grep -E 'world price|pairs:|residual|FAIL|GATE'
# inventory of everything Block C parks (record it: this is what "left behind = 0" is checked against)
Rscript -e '
  suppressMessages(suppressWarnings(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")))
  v <- file.path(mapspam_pro_dir, c("variable=vop_intld15-2021", "variable=vop_nominal-usd-2021", "variable=vop_nominal-usd-2015", "variable=vop_nominal-usd-2020", "fao_prices"))
  for (d in v) { f <- list.files(d, recursive = TRUE, full.names = TRUE); cat(sprintf("%-60s %3d files %7.1f MB\n", d, length(f), sum(file.size(f)) / 1e6)) }
  e <- list.files(exposure_dir, "^(exposure_adm_sum_spam20-20_glw420-20|vop_nominal-usd-2021_adm_sum_spam20_glw420|vop_intld15-2021_adm_sum_spam20_glw420)_res-(05|25)\\.parquet(\\.json)?$", full.names = TRUE)
  cat("0.4.4 section 3 tables + sidecars:\n"); print(data.frame(file = basename(e), mtime = format(file.mtime(e), "%Y-%m-%d %H:%M"), MB = round(file.size(e) / 1e6, 1)))
  cat("per-tif 0.4.4 caches inside the vop dirs:", length(list.files(file.path(mapspam_pro_dir, c("variable=vop_intld15-2021", "variable=vop_nominal-usd-2021")), "_adm_sum\\.parquet", recursive = TRUE)), "\n")
  cat("FAO files:\n"); for (f in c("Value_of_Production_E_Africa.csv", "Value_of_Production_E_All_Data.csv", "Value_of_Production_E_All_Area_Groups.csv", "Prices_E_Africa_NOFLAG.csv", "Prices_E_All_Data_(Normalized).csv", "Production_Crops_Livestock_E_Africa_NOFLAG.csv", "Production_Crops_Livestock_E_All_Area_Groups.csv")) { p <- file.path(fao_dir, f); cat(sprintf("  %-55s %s %s\n", f, if (file.exists(p)) format(file.mtime(p), "%Y-%m-%d") else "MISSING", if (file.exists(p)) sprintf("%.0f MB", file.size(p) / 1e6) else "")) }
'
df -h <common_data mount>
```

**Expect:**
- both fixtures end `ALL ... FIXTURE ASSERTIONS PASSED`; no Rscript running; no 0.4.x env set.
- cross-basis gate **PASS at both resolutions**, reading `world price reference: .../fao_prices/
  crop_price_nominal-usd-2021-t_fill-sources_<tag>.csv`, with the intld residual present (countries
  include SDN and NGA; pearl-millet among the pairs). That is the state this pass removes.
- `Value_of_Production_E_All_Data.csv` **present** (0.4.0 downloads it otherwise — 1 GB; if it is
  missing, say so before Block C so the download is planned, not a surprise in a background job).
  Every other FAO file present with the 2026-05-14 mtime (any other vintage: STOP, the macbook
  numbers below no longer apply as examples).
- the vop dirs hold tagged rasters for both resolutions plus per-tif caches; six §3 tables + sidecars
  (3 tables × 2 res). Paste the table.

**STOP.** Paste. Block B may follow immediately if A is clean.

---

## Block B — probes on the current code and the current tables (read-only, ~15-25 min)

```bash
cd <hazards_prototype>; STAMP=$(date +%Y%m%d_%H%M%S); echo $STAMP > logs/intld_fixes_stamp.txt
nohup bash -c "EXPOSURE_RES=0.25 Rscript R/checks/probe_042_price_fill.R --crops coff,plnt,cowp,cass,teas,toba --top 25 > logs/probe042_$STAMP.log 2>&1; EXPOSURE_RES=0.25 Rscript R/checks/probe_040_allocation.R > logs/probe040_$STAMP.log 2>&1" > /dev/null 2>&1 &
# when both logs end with 'done in': paste
grep -E 'stale local price test|evidence pins|price clip|basis guard|fill sources|own \(incl|skipped|WARN|Error|done in' logs/probe042_$STAMP.log
grep -A30 'stale local price test' logs/probe042_$STAMP.log
grep -A45 'basis fallbacks' logs/probe042_$STAMP.log
grep -A30 'top 25 prices' logs/probe042_$STAMP.log
grep -E 'allocation groups|allocation table|guarded countries|not judged|coverage distribution|^   n=|Error|done in' logs/probe040_$STAMP.log
grep -A80 'every guarded pair' logs/probe040_$STAMP.log | head -120
grep -A25 'named pairs' logs/probe040_$STAMP.log
grep -A40 'SPAM national tonnage inside' logs/probe040_$STAMP.log
```

**Expect (probe_042, the nominal side):**
- `skipped (read-only): fwrite(...)` lines present — the live audit CSV is untouched (the gate's
  reference stays what Block A read).
- `stale local price test`: **between 15 and 40 pairs** rejected, **SDN and AGO the two largest
  groups** (macbook: 27 of 564 — SDN 8, AGO 7, GHA 4, SLE 4). `evidence pins loaded: 1 (AGO:bana=300)`
  and `evidence pins applied: 1` per year set.
- `fill sources`: `fao gpv implied` is the largest own class; `own (incl. basis fallback) prices
  cover ... ≥ 75 % of FAO production` (macbook: 81 %); basis fallbacks **between 15 and 45 rows**, and
  the list contains **KEN coff, BDI toba, NGA grou, NGA oilp, GIN on several crops**, and **no SDN or
  AGO row** (those are now stale-rejected upstream; macbook: 27 rows). Clip line: implied dropped
  **≤ 15 %** of own observations (macbook: 842 of 7,994), producer price unchanged from the
  2026-09-27 run (99 of 3,295).
- the top-25 vs world table: no own price beyond 5× by construction; what sits beyond is a basis
  fallback or a fill (plantain in Sahelian countries at ~970 USD/t from the old neighbour chain is
  the known one). **No `WARN ...: rows with no price at all`.**
- Paste the basis-fallback table whole. Pete reviewed the macbook version on 2026-10-02 against
  independent price evidence (handover, "Evidence" section): 7 of the 8 material rows sit inside the
  supportable farm-gate range; the eighth (AGO banana) is pinned. The node's table should match the
  macbook's row for row if the FAO files are the same vintage.

**Expect (probe_040, the intld side):**
- groups table shows **acof+rcof, pmil+smil, banpl, rape** (plus the many-item ocer / rest / vege);
  no `WARN: SPAM layers with no FAO item`.
- `allocation table: ... guarded N ...` with the **guarded share of continental GPV below 5 %**; the
  guarded list contains **SDN on every material crop** (sorg, grou, sesa, whea, pmil+smil, sugc, ...)
  and nothing for **KEN / ETH / UGA / TZA pmil+smil**, nothing for **NGA banpl** (pooled coverage
  ≈ 1: SPAM 6.5 Mt vs FAO 6.4 Mt), nothing for SYC cnut.
- coverage distribution over allocated pairs: **median within [0.7, 1.4]** (SPAM 2020 is calibrated
  to FAOSTAT ~2020), and a short tail below 0.5 — paste the n and quantiles.
- the compound-group tonnage table: KEN and ETH small millet ≫ pearl millet (finger millet); NGA
  plantain ≫ banana.
- Anything else guarded that is NOT Sudan: list it by name — it is a candidate "SPAM has it under a
  different crop" case like Nigeria banana and Pete should see it before C.

**STOP.** Paste both. **Pete reads the basis-fallback and guarded lists and gives the GO for C.**

---

## Block C — rebuild 0.4.0 + 0.4.2 at both resolutions, 0.4.4 at both, gates (GO-GATED; writes node Data/, nothing to S3; ~1.5-3 h)

**GO line (Pete fills in):** `GO C: ______ (date)`

### C0 — park (minutes). `mv`, never `rm`. Park OUTSIDE `mapspam_pro_dir`.

```bash
cd <hazards_prototype>; STAMP=$(cat logs/intld_fixes_stamp.txt); pgrep -af Rscript || echo "no Rscript running"
Rscript -e '
  suppressMessages(suppressWarnings(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")))
  stamp <- readLines("/home/jovyan/atlas/hazards_prototype/logs/intld_fixes_stamp.txt")
  park <- file.path("Data", paste0("_parked_intld_fixes_", stamp)); dir.create(park, recursive = TRUE)
  vdirs <- file.path(mapspam_pro_dir, c("variable=vop_intld15-2021", "variable=vop_nominal-usd-2021", "variable=vop_nominal-usd-2015", "variable=vop_nominal-usd-2020", "fao_prices"))
  src <- c(list.files(vdirs, recursive = TRUE, full.names = TRUE),
           list.files(exposure_dir, "^(exposure_adm_sum_spam20-20_glw420-20|vop_nominal-usd-2021_adm_sum_spam20_glw420|vop_intld15-2021_adm_sum_spam20_glw420)_res-(05|25)\\.parquet(\\.json)?$", full.names = TRUE))
  for (f in src) { d <- file.path(park, dirname(f)); dir.create(d, recursive = TRUE, showWarnings = FALSE); stopifnot(file.rename(f, file.path(d, basename(f)))) }
  cat("parked", length(src), "files under", normalizePath(park), "\n")
  left <- c(list.files(vdirs, recursive = TRUE), list.files(exposure_dir, "^(exposure_adm_sum_spam20-20_glw420-20|vop_nominal-usd-2021_adm_sum|vop_intld15-2021_adm_sum).*_res-(05|25)\\.parquet"))
  cat("left behind:", length(left), "\n"); if (length(left)) print(left)
'
```
**Expect:** `parked <n>` equals Block A's inventory (vop dirs + fao_prices + 6 tables + their sidecars);
**`left behind: 0`**. Otherwise STOP. Untagged native inputs (`variable=prod_t`, `harv-area`, …) and their
caches are not touched — they are reused.

### C1 — 0.4.0 and 0.4.2, both resolutions (background, one chain; ~20-60 min)

`FORCE_OVERWRITE` stays **unset**: 0.4.0 writes because its outputs are parked, 0.4.2 always writes.
`VOP_COVERAGE_MIN` stays unset (0.10). Scripts need setup sourced first (AGENTS.md §2).

```bash
cd <hazards_prototype>; STAMP=$(cat logs/intld_fixes_stamp.txt)
cat > logs/c1_$STAMP.sh <<'SH'
set -e
S=/home/jovyan/atlas/hazards_prototype/R
for RES in 0.25 0.05; do
  for SCR in 0.4.0_create_crop_vop_intld15.R 0.4.2_create_crop_vop_nominal_usd.R; do
    echo "===== $(date '+%F %T') START $SCR EXPOSURE_RES=$RES"
    EXPOSURE_RES=$RES Rscript -e "source('$S/0_server_setup.R'); source('$S/$SCR')"
    echo "===== $(date '+%F %T') END $SCR EXPOSURE_RES=$RES"
  done
done
SH
nohup bash logs/c1_$STAMP.sh > logs/c1_$STAMP.log 2>&1 &
echo $! > logs/c1_$STAMP.pid
```
When the log shows the fourth `END`, paste:
```bash
grep -E '=====|exposure grid|allocation groups|allocation table|guarded countries|allocation check|writing|price clip|basis guard|fill sources|own \(incl|WARN|Error|COMPLETE' logs/c1_$STAMP.log
Rscript -e 'suppressMessages(suppressWarnings(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"))); for (d in file.path(mapspam_pro_dir, c("variable=vop_intld15-2021", "variable=vop_nominal-usd-2021", "fao_prices"))) { f <- list.files(d, full.names = TRUE); cat(d, "\n"); print(data.frame(file = basename(f), MB = round(file.size(f) / 1e6, 1), mtime = format(file.mtime(f), "%H:%M"))) }'
```
**Expect:** four START/END pairs, no `Error`; 0.4.0 at each res logs `allocation check: ... pairs
conserved to 1e-6, ... guarded pairs empty` and `writing .../spam_vop_intld15-2021_all_<tag>.tif`;
0.4.2 at each res logs the clip line, three `basis guard` lines (one per year set) and three `fill
sources` lines with the same counts as Block B (prices do not depend on the grid), and no `WARN`.
On disk: `vop_intld15-2021/` holds all / irr / rf-all × 2 tags (6 tifs); `vop_nominal-usd-2021/`
holds 4 techs × 2 tags (8 tifs; 2015 and 2020 likewise); `fao_prices/` holds
`crop_price_<set>-t_<tag>.tif` (6), `crop_price_<set>-t_fill-sources_<tag>.csv` (6) and
`crop_vop_intld15-2021_allocation_<tag>.csv` (2). The guarded share and the guarded country list are
identical at both resolutions (the guard compares national totals). **If the two resolutions'
allocation CSVs differ in their guarded rows, STOP.**

### C2 — 0.4.4 at both resolutions (background; ~20-40 min each)

No `FORCE_OVERWRITE`: skip-if-exists rebuilds the parked per-tif caches and the three §3 tables, reuses
everything else (livestock, population, prod_t / harv-area caches).

```bash
cd <hazards_prototype>; STAMP=$(cat logs/intld_fixes_stamp.txt)
cat > logs/c2_$STAMP.sh <<'SH'
set -e
S=/home/jovyan/atlas/hazards_prototype/R
for RES in 0.25 0.05; do
  echo "===== $(date '+%F %T') START 0.4.4 EXPOSURE_RES=$RES"
  EXPOSURE_RES=$RES Rscript -e "source('$S/0_server_setup.R'); source('$S/0.4.4_process_exposure.R')"
  echo "===== $(date '+%F %T') END 0.4.4 EXPOSURE_RES=$RES"
done
SH
nohup bash logs/c2_$STAMP.sh > logs/c2_$STAMP.log 2>&1 &
```
Then:
```bash
grep -E '=====|section 1:|section 3|units|twin|Error|WARN' logs/c2_$STAMP.log | head -60
Rscript -e 'suppressMessages(suppressWarnings(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"))); e <- list.files(exposure_dir, "_res-(05|25)\\.parquet$", full.names = TRUE); print(data.frame(file = basename(e), MB = round(file.size(e) / 1e6, 1), mtime = format(file.mtime(e), "%m-%d %H:%M")))'
```
**Expect:** two START/END, no `Error`, no `untagged legacy twin` stop; `section 3.1: units present in
extraction = ... intld15-2021 ... nominal-usd-2021 ...` and nothing in `units DROPPED` except the 2015
/ 2020 nominal sets; the six tables rewritten with today's mtime.

### C3 — gates and reports (minutes each; paste everything)

```bash
cd <hazards_prototype>; STAMP=$(cat logs/intld_fixes_stamp.txt); P=Data/_parked_intld_fixes_$STAMP   # relative to working_dir; use the path C0 printed
# 1. the arbiter: cross-basis per pair, intld side now gated
Rscript R/checks/vop_cross_basis_gate.R --res 0.25 --fail-on-intld-side 2>&1 | tee logs/gate_cb25_$STAMP.log | grep -vE '^\s+[0-9]+:' ; grep -E 'GATE|FAIL|residual|world price' logs/gate_cb25_$STAMP.log
Rscript R/checks/vop_cross_basis_gate.R --res 0.05 --fail-on-intld-side 2>&1 | tee logs/gate_cb05_$STAMP.log | grep -E 'pairs:|material ratio|GATE|FAIL|residual|world price|one-sided'
# 2. national totals still reconcile to FAOSTAT (intld): ratio ~1 except where the guard blanked value
Rscript R/qaqc_vop_vs_faostat.R 2>&1 | tee logs/qaqc_$STAMP.log | tail -40
# 3. what moved, per pair, old (parked) vs new, both resolutions
Rscript R/checks/exposure_pair_drift.R --old <path C0 printed>/<exposure_dir>/exposure_adm_sum_spam20-20_glw420-20_res-25.parquet --new <exposure_dir>/exposure_adm_sum_spam20-20_glw420-20_res-25.parquet --out logs/pair_drift_res25_$STAMP.csv 2>&1 | tee logs/pair_drift_res25_$STAMP.log
Rscript R/checks/exposure_pair_drift.R --old <path C0 printed>/<exposure_dir>/exposure_adm_sum_spam20-20_glw420-20_res-05.parquet --new <exposure_dir>/exposure_adm_sum_spam20-20_glw420-20_res-05.parquet --out logs/pair_drift_res05_$STAMP.csv 2>&1 | tee logs/pair_drift_res05_$STAMP.log
# 4. publisher dry-runs, reference + family, both resolutions (no S3 write)
for R in 0.25 0.05; do Rscript scripts/r3_publish_tiers.R --reference-only --res $R --dry-run 2>&1 | grep -E 'reference|rows|columns|distinct|unit|GATE|PASS|FAIL|would'; done
for R in 0.25 0.05; do Rscript scripts/r3_publish_tiers.R --family-only --res $R --dry-run 2>&1 | grep -E 'family|rows|columns|distinct|unit|GATE|PASS|FAIL|would'; done
```

**Expect — stop at the first that does not hold:**
1. **Cross-basis `--fail-on-intld-side`: `GATE PASS` at both resolutions.** `world price reference`
   line present (reads the NEW audit CSV, whose `price_usd_global` is now the world implied price).
   Nominal-side list empty; intld-side list **empty** (the target state: SDN, NGA banana and
   pearl-millet gone). Per-crop medians within [1/5, 5]. `one-sided` pairs: small-millet should **no
   longer** be among them; SDN crops **will** be (nominal only — that is the guard, by design) and
   that is the only expected country-wide one-sided set. If the gate FAILs on a pair the briefing
   did not name (it mentions MOZ tea and NER maize at the band edge on the old tables), STOP and paste
   the table — do not loosen the band.
2. **qaqc_vop_vs_faostat**: livestock ratios unchanged from the last run; crop ALL-CROPS ratio ≈ 1
   (within a few %) for every country **except the guarded ones, where it equals 1 − the guarded
   share Block B printed for that country** (Sudan → ≈ 0). Anything else off by more than a few %
   → STOP.
3. **Pair drift, both resolutions** (a report; the named movers are expected, anything else large is
   not):
   - livestock control: `max |ratio − 1|` = 0 (res-05) — on res-25 identical too (livestock rasters
     are not touched by this pass).
   - production rows (`prod`): ratio 1 everywhere on res-05; on **res-25** small positive moves for
     coastal countries and SYC appearing (touches) — nothing else.
   - **intld15-2021**: `appears` = small-millet rows (KEN, ETH, UGA, TZA, …), SYC rows (res-25), NGA
     plantain; `disappears` = every SDN crop, NGA banana (→ ~0, may show as "to zero"), plus a set of
     tiny pairs (old value ≲ 60,000 I$ — the classify ID leak; name them); material movers beyond 2×:
     pearl-millet DOWN where finger millet dominates, rapeseed UP slightly where mustard seed exists,
     the rest within 2× except coastal effects on res-25.
   - **nominal-usd-2021**: this is the price-method move and it is large by design: continental
     all-crops total ratio ≈ **0.8** at both resolutions (macbook on the FAO tables: 0.82 before
     coastal gain), per-crop continental lowest cowpea ≈ 0.3, coffee ≈ 0.4, plantain ≈ 0.5, coconut
     ≈ 0.5; highest sugar beet / sugarcane ≈ 1.2; material movers include NGA cass / cowp / cnut,
     CMR+GHA+CIV plnt, ETH+KEN coff down, GIN mill / bana, ZMB+AGO+COG sugc up. **The same per-country
     factors must appear at both resolutions** (prices are per country; only the coastal term
     differs). SYC appears on res-25. AGO banana nominal = 300 USD/t × SPAM tonnage (the pin).
   - If `appears` / `disappears` on intld lists a country other than SDN with several crops, STOP:
     that is a guarded country Block B should have shown.
4. **Publisher dry-runs**: columns and `distinct(exposure, unit, stat)` identical to live at both
   resolutions; row counts within the gate's 25 % (intld small-millet rows arrive, SDN intld rows
   leave); family tables likewise; each prints what it *would* upload and nothing more. **Do not pass
   `--allow-unit-vintage-change`, `--allow-res-change` or `--allow-schema-drift`.** If a dry-run
   fails a gate, STOP with the line.

**STOP.** Paste C3 entire. **Pete gives the GO for D.**

---

## Block D — publish reference + family, both resolutions (LIVE WRITE to S3 — GO-GATED)

**GO line (Pete fills in):** `GO D: ______ (date)`

Background every publish (the interactive shell kills foreground work after ~2 min). The bucket is
versioned (noncurrent versions kept ≥ 270 days), so the overwritten objects stay recoverable; no
`sandbox/backup/` copy is needed for this pass unless Pete asks.

```bash
cd <hazards_prototype>; STAMP=$(cat logs/intld_fixes_stamp.txt)
cat > logs/d_$STAMP.sh <<'SH'
set -e
cd /home/jovyan/atlas/hazards_prototype
for R in 0.25 0.05; do
  echo "===== $(date '+%F %T') reference res $R"; Rscript scripts/r3_publish_tiers.R --reference-only --res $R
  echo "===== $(date '+%F %T') family res $R";    Rscript scripts/r3_publish_tiers.R --family-only --res $R
done
echo "===== $(date '+%F %T') DONE"
SH
nohup bash logs/d_$STAMP.sh > logs/d_$STAMP.log 2>&1 &
```
Then verify **from S3 by re-download**, not from the local files (keys as `scripts/r3_publish_tiers.R`
builds them: `REF_KEY_LEGACY`, `res_key()`, `family_key_legacy()`):
```bash
Rscript -e '
  suppressMessages(suppressWarnings(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"))); suppressPackageStartupMessages({library(s3fs); library(arrow)})
  pre <- "s3://digital-atlas/domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/"
  map <- c("variable=crop-livestock_all_res-25.parquet"  = "exposure_adm_sum_spam20-20_glw420-20_res-25.parquet",
           "variable=crop-livestock_all_res-05.parquet"  = "exposure_adm_sum_spam20-20_glw420-20_res-05.parquet",
           "variable=crop-livestock_all.parquet"         = "exposure_adm_sum_spam20-20_glw420-20_res-05.parquet",   # deprecated alias of res-05
           "variable=vop_nominal-usd-2021_res-25.parquet" = "vop_nominal-usd-2021_adm_sum_spam20_glw420_res-25.parquet",
           "variable=vop_nominal-usd-2021_res-05.parquet" = "vop_nominal-usd-2021_adm_sum_spam20_glw420_res-05.parquet",
           "variable=vop_nominal-usd-2021.parquet"        = "vop_nominal-usd-2021_adm_sum_spam20_glw420_res-05.parquet",
           "variable=vop_intld15-2021_res-25.parquet"     = "vop_intld15-2021_adm_sum_spam20_glw420_res-25.parquet",
           "variable=vop_intld15-2021_res-05.parquet"     = "vop_intld15-2021_adm_sum_spam20_glw420_res-05.parquet",
           "variable=vop_intld15-2021.parquet"            = "vop_intld15-2021_adm_sum_spam20_glw420_res-05.parquet")
  tmp <- file.path(tempdir(), "s3check"); dir.create(tmp, showWarnings = FALSE)
  for (k in names(map)) {
    s3fs::s3_file_download(paste0(pre, k), file.path(tmp, k), overwrite = TRUE)
    loc <- file.path(exposure_dir, map[[k]])
    cat(sprintf("%-48s rows %7d | md5 S3 %s | local md5 %s | identical %s\n", k, nrow(arrow::read_parquet(file.path(tmp, k))),
                substr(tools::md5sum(file.path(tmp, k)), 1, 8), substr(tools::md5sum(loc), 1, 8), unname(tools::md5sum(file.path(tmp, k)) == tools::md5sum(loc))))
  }
  cat("downloaded to", tmp, "\n")
'
# the arbiter, on the RE-DOWNLOADED reference (--file), with the new world-price reference
Rscript R/checks/vop_cross_basis_gate.R --file <tmp>/variable=crop-livestock_all_res-25.parquet --fail-on-intld-side --world-prices <mapspam_pro_dir>/fao_prices/crop_price_nominal-usd-2021-t_fill-sources_res-25.csv 2>&1 | grep -E 'GATE|FAIL|residual|world price'
Rscript R/checks/vop_cross_basis_gate.R --file <tmp>/variable=crop-livestock_all_res-05.parquet --fail-on-intld-side --world-prices <mapspam_pro_dir>/fao_prices/crop_price_nominal-usd-2021-t_fill-sources_res-05.csv 2>&1 | grep -E 'GATE|FAIL|residual|world price'
```
**Expect:** `DONE` in the log, every publish's gates PASS (the same lines as the C3 dry-runs); nine
keys re-downloaded, each **byte-identical (md5) to its local table** (the unsuffixed alias to the
res-05 table); cross-basis `GATE PASS` on both re-downloaded references. Then **STOP** and paste.
Nothing else is published in this pass: the hazard tiers (usd, intld, ha) ride the #13 full R/2 +
R/3 re-bake on this exposure (`R/NEXT_FULL_REBAKE.md`), and `_parked_intld_fixes_<STAMP>` stays
until Pete releases it.

---

## Block E — records and issues (macbook side; nothing to run on the node)

After D's response: the three CDH records (`metadata/cdh/africa-exposure-combined-res25.yaml`, `-res05`,
`africa-hazard-exposure-nexgddp.yaml`) lose their #38 / #39 / #40 paragraphs, gain a "changed
against the previous publication" paragraph quoting C3's pair-drift numbers and cite
`docs/methods/nominal_price_method.md` for the price method; `metadata/cdh/README.md` row updated;
`README.md` VoP section points at the methods doc; the KE-ENSO notebook repo's
`data/economicReturns/text/methods.en.md` (ours) is rewritten from the methods doc (it still describes
the producer-price mean method); `atlas_notebooks` notified (relay only); #38, #39, #40 closed with the
C3 gate lines; this file archived; the handover updated. The consumer list lives at the end of the
methods doc.
