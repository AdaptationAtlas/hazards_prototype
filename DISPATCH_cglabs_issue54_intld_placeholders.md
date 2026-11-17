# DISPATCH — cglabs — #54 + #52: constant-I$ placeholders and the TLU slot

Append-only, newest block on top. Read `AGENTS.md` first, then `HANDOVER_2026-10-23.md` §3 items 2 and 5.

---

### RESPONSE — cglabs 2026-11-17 (interim) — **C-re0 + C1 done, every gate PASS; the denominator is live (18/18 keys verified). C2 (R/3 intld) is running.** C3/C4 to follow in the final RESPONSE.

HEAD `d098557` (`Pete Steward`, `--tlu-slot` + exit 1 on ABORT). Stamp `20261116_222408`.

**C-re0 — fixture.**
- `Rscript R/checks/fixture_publish_tier_gates.R`: exit 0, **59 `ok`**, `ALL PUBLISH-GATE FIXTURE ASSERTIONS PASSED`.
- That includes the `--tlu-slot` stat case and both #30 regression assertions.
- Log: `logs/issue54_Cre0_fixture_20261116_222408.log`.

**C1 — dry-runs** (`logs/issue54_C1r_dry_20261116_222408.log`):
```
CONTROL --reference-only --res 0.25 --dry-run (no flag): FAIL distinct(exposure) + FAIL distinct(unit), ABORT,
        "one or more gates FAILED - exiting 1 so a calling loop stops here"   -> exit 1   (exit-code fix confirmed on the real artifact)
--reference-only --res 0.05 --tlu-slot: 'exposure' gains 'tlu' ALLOWED (--tlu-slot, the #52 total-TLU relabel), 5 -> 6; same for 'unit'; rows 8065336 = 8065336 -> exit 0
--reference-only --res 0.25 --tlu-slot: same two ALLOWED lines; rows 5667864 = 5667864 -> exit 0
--family-only --res 0.05 --tlu-slot:    usd 1508624 = 1508624, intld 986408 = 986408, all ok -> exit 0
--family-only --res 0.25 --tlu-slot:    usd 1060176 = 1060176, intld 693192 = 693192, all ok -> exit 0
```
- The dry-run upload plan lists **18 keys, every one under `domain=exposure/type=combined/`**.
- No other flag was passed.

**C1 — live** (`logs/issue54_C1_publish_20261116_222408.log`): the #47 loop with `--tlu-slot`. All four runs exit 0, in 0.4 / 0.3 / 0.5 / 0.3 min.

The publisher backed up each live key first. Backup prefixes:
- `sandbox/backup/issue9_20261117_163638/` — reference res-05 + alias
- `sandbox/backup/issue9_20261117_163702/` — reference res-25
- `sandbox/backup/issue9_20261117_163719/` — family res-05 + aliases
- `sandbox/backup/issue9_20261117_163749/` — family res-25

The pre-publish S3 state is in `logs/issue54_C1_s3_before_20261116_222408.txt`.

**Gate C1 — PASS on every invariant.**
- **Only `domain=exposure/type=combined/...` keys were touched:** 18 of 18.
- **Size and content match the local files on all 18.** No multipart ETags, so every ETag equals the local MD5.
- **Anonymous HTTPS:** 18 × 200.
- **Read-back from S3.**
  - **What I ran:** downloaded the six suffixed parquets back from S3 into a scratch dir under their local filenames. Ran the B3 gate `R/checks/gate_054_reference_diff.R --new-dir <that dir>` against the old side `$PARK/exposure` and the 11-02 probe CSV. Log: `logs/issue54_C1_readback_gate*_20261116_222408.log`.
  - **Gate result:** `GATE 054-REF PASS (res-25): 3/3` and `GATE 054-REF PASS (res-05): 3/3`.
  - **#52 TLU slot:** 10194 → 10194 tlu rows (res-25) and 14506 → 14506 (res-05), values identical.
  - **Rows outside the #54 pairs:** 0 differ.
  - **The four #54 pairs:** within 2 % of the probe's NEW values.
  - **Livestock intld total on S3:** **89.1075 B (res-25) / 89.1110 B (res-05)**, which is exactly the expected figure.
  - **Nominal twin:** `#54 pairs present but unchanged: ok`.
  - **Nominal parquets are byte-identical to #47's:** `d6f14d3e…` and `502ef758…`. Only their `.json` sidecars changed.
  - **One harness slip, not a gate result:** my first res-05 pass was killed at 50 s by my own `timeout`. I re-ran it unbounded and it is the PASS above.

**Denominator pin table** (prefix `domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/processing=atlas-harmonized/variable=`):

| key | size | ETag | LastModified | VersionId |
|---|---|---|---|---|
| `crop-livestock_all.parquet` | 9172420 | `8bd43fa0ce9c6baa1141a0288d86bb37` | 2026-11-17T16:37:00+00:00 | `W7GT0Yhb1cfIEG555TzVbnpA76FVfcP.` |
| `crop-livestock_all.parquet.json` | 4253 | `97eb6d48f1cfc92b60ac9a8a1a997067` | 2026-11-17T16:37:02+00:00 | `iZx7W2OCDIpHxBU7.ZxErNtt9CyJhDSh` |
| `crop-livestock_all_res-05.parquet` | 9172420 | `8bd43fa0ce9c6baa1141a0288d86bb37` | 2026-11-17T16:36:53+00:00 | `zSvMOFkise.EJpsuKQwJEoCUPO5FI3f1` |
| `crop-livestock_all_res-05.parquet.json` | 4253 | `97eb6d48f1cfc92b60ac9a8a1a997067` | 2026-11-17T16:36:54+00:00 | `09EunkehfagEFTp7uWSLIIo8fbOAZdb7` |
| `crop-livestock_all_res-25.parquet` | 7295873 | `87af763d3a32b73b50a7664a58235c4f` | 2026-11-17T16:37:17+00:00 | `Ws14cqe_Omkn7U2kX.IYBkdrypHxcalw` |
| `crop-livestock_all_res-25.parquet.json` | 4259 | `4920a3aec0410f5f2f99b9e718b502a9` | 2026-11-17T16:37:18+00:00 | `ndFuR4TBwtKlwv2J6_8yHwpbxYmZUdrB` |
| `vop_intld15-2021.parquet` | 1397844 | `24d738a4e71407f584b93c08ab689ea0` | 2026-11-17T16:37:47+00:00 | `GxDZ6ShQ_xad7k9.Hmh.hBcbYD.bL3qh` |
| `vop_intld15-2021.parquet.json` | 4155 | `1c5f128ac9360df660f010e3e629ae84` | 2026-11-17T16:37:48+00:00 | `5DadCsJ2U6B1CTUCxDKvT5THJ1IvV9_k` |
| `vop_intld15-2021_res-05.parquet` | 1397844 | `24d738a4e71407f584b93c08ab689ea0` | 2026-11-17T16:37:42+00:00 | `su4Kf.VMOopMEjWs5XfafDeHeQxM4Zix` |
| `vop_intld15-2021_res-05.parquet.json` | 4155 | `1c5f128ac9360df660f010e3e629ae84` | 2026-11-17T16:37:43+00:00 | `6M5OnbYJVscjENMCs238GkIsxLH_SyNl` |
| `vop_intld15-2021_res-25.parquet` | 1121126 | `03d95946d7433d55c4df111980c474cb` | 2026-11-17T16:38:07+00:00 | `8RTICM87jZtFOAa1G26rbuv1BeupyT6c` |
| `vop_intld15-2021_res-25.parquet.json` | 4161 | `c5d65b130a609980fe078fb46a66add5` | 2026-11-17T16:38:09+00:00 | `kl2L43TxhX58X9FQ8CTS.beOJyq7uWOI` |
| `vop_nominal-usd-2021.parquet` | 1735243 | `d6f14d3ef7da421c767b193a9eb4d302` | 2026-11-17T16:37:37+00:00 | `bvlSephxxuJT_c.14gffBDEQcAlRonz0` |
| `vop_nominal-usd-2021.parquet.json` | 3850 | `01760cff46c75c4fd91454b774dc74d9` | 2026-11-17T16:37:38+00:00 | `pQMycScxfNhMqXSRhikCJZ7ho9oSzmjz` |
| `vop_nominal-usd-2021_res-05.parquet` | 1735243 | `d6f14d3ef7da421c767b193a9eb4d302` | 2026-11-17T16:37:31+00:00 | `Rkp4eF8LHlv5tO1JnmwkkG92GWJ2inHV` |
| `vop_nominal-usd-2021_res-05.parquet.json` | 3850 | `01760cff46c75c4fd91454b774dc74d9` | 2026-11-17T16:37:33+00:00 | `CkEvaOKrqelhd25fboGG9DHCyEfR8bZi` |
| `vop_nominal-usd-2021_res-25.parquet` | 1407996 | `502ef758713afe7717dd0ae081c68111` | 2026-11-17T16:38:01+00:00 | `gemXpf_wyKRjfyi5E5YNii5y79iIiFX2` |
| `vop_nominal-usd-2021_res-25.parquet.json` | 3856 | `cd56acd184a0ee91048d04a6ed12bbfa` | 2026-11-17T16:38:03+00:00 | `_jRcqSBjmGim1buapRHtvaE90efCEArW` |

**C2 — running.**
- **Parked by move:**
  - Destination: `Data/_parked_issue54_20261116_222408/r3_vop_intld/{annual,jagermeyr}`.
  - Source: `atlas_dirs$data_dir$hazard_risk_vop`.
  - 1002 of 1002 files moved per timeframe, **0 left behind**.
  - The file lists are in `logs/issue54_C2_parked_{annual,jagermeyr}_20261116_222408.txt`.
- **Launched** 16:42, pid in `logs/issue54_r3_20261116_222408.pid`, `since` = `2026-11-17 16:42`. Settings:
  - `R3_TIMEFRAMES=annual,jagermeyr`
  - `R3_CHECK_WORKERS=4`
  - `FORCE_OVERWRITE`, `R3_ALLOW_41_FAILURES` and `SKIP_R3_4_1` unset
  - 368 GiB available at launch
- **Header as expected:**
  - `FORCE_OVERWRITE=<unset> -> overwrite=FALSE`, `overwrite4= FALSE`
  - `R3_TIMEFRAMES active - running only: annual, jagermeyr`
  - crop intld `spam_vop_intld15-2021_all_res-25.tif`
  - livestock intld `glw4-2020_vop_intld15-2021_res-25.tif`, mtime 2026-11-02 19:03 (the B1 rebuild)
- §4.1 intld started at 16:43. Expect several hours, mostly §4.2 at `worker_n4.2 <- 1`.

---


### MACBOOK 2026-11-17 — **C1 re-issue: fix landed, option 1.** `--tlu-slot` declares the #52 addition; a gate ABORT now exits 1

Your STOP was right, and both findings are fixed on `develop` (pull first; the fixture is the proof):

1. **`--tlu-slot`** (option 1, as recommended). The distinct(exposure/unit/stat) gate is now
   `R/checks/r3_tier_drift_vs_live.R::ref_distinct_gate`, called by `publish_table`. With
   `--tlu-slot`, `exposure` and `unit` may each gain **exactly `tlu`** — any other added value, any
   lost value, and `stat` still fail. `--allow-unit-vintage-change` is unchanged (1:1 renames only).
   One-shot: once the live reference carries `tlu`, the flag is a no-op.
2. **Exit codes.** Every ABORT (tier loop and `publish_table`) now records `PUB_FAILED`, and the
   script ends `quit(status = 1)` if any fired — your #47-style loop now stops at a refused publish,
   dry-run included.
3. `R/checks/fixture_publish_tier_gates.R` gained paired assertions for all of this (undeclared tlu
   fails, declared passes, stowaway/replacement/stat still fail, #30 vintage rename unaffected).
   It passes on the macbook; run it first as your C-re0.

**Re-issue of C1, amended:** run the fixture, then the C1 dry-runs again with `--tlu-slot` appended
to the four **reference** invocations (`--reference-only` / `--family-only`, both `--res`). The
family twins are single-unit and need no flag, but it is harmless if your loop passes it everywhere.
Expected dry-run lines: `'exposure' gains 'tlu' ALLOWED (--tlu-slot, …)` and the same for `unit`, at
both resolutions; family unchanged. Gate C1 as in the GO block below, with one addition: **a
deliberately flag-less reference dry-run must exit non-zero** (the exit-code fix, checked once on
the real artifact). Then C2-C4 exactly as the GO block states.

---

### RESPONSE — cglabs 2026-11-16 — **STOPPED at C1. Nothing published, R/3 not run.** C0 PASS. The reference dry-runs abort on the post-#52 `tlu` exposure/unit: `FAIL distinct(exposure)` and `FAIL distinct(unit)` at both resolutions. No allow-flag can clear this gate, and none was passed. The family dry-runs are clean, but I held them back so the denominator does not go out half-published.

Stamp `20261116_222408` (`logs/issue54c_stamp.txt`). HEAD `e6111c7` (`Pete Steward`, block C GO). Log: `logs/issue54_C1_dry_20261116_222408.log`.

**C0 — PASS.**
- **Node quiet:** no R process running. The newest log was the R/2.2 publish, which finished at 19:34. Memory: 368 GiB available.
- **The six rebuilt 0.4.4 parquets are untouched since B3** (mtimes 2026-11-02):

  | table | res-25 | res-05 |
  |---|---|---|
  | `exposure_adm_sum_spam20-20_glw420-20` | 19:10:10 | 19:14:19 |
  | `vop_nominal-usd-2021_adm_sum_spam20_glw420` | 19:10:15 | 19:14:27 |
  | `vop_intld15-2021_adm_sum_spam20_glw420` | 19:10:20 | 19:14:32 |

  Nothing newer. The W0-W4 obs work did not rewrite them.
- **`$PARK` intact:** `Data/_parked_issue54_20261102_185857` holds `exposure/` (the six pre-#54 parquets, 2026-10-21 vintage) and `glw_processed/` (`fao_vop` plus `variable=vop_intld15-{2015,2020,2021}`).

**C1 — dry-runs. All four exit 0, but both reference runs ABORT.**

```
--reference-only --res 0.05:
  ok: 14 columns identical
  FAIL distinct(exposure): live=[harv-area,number,phys-area,prod,vop] local=[harv-area,number,phys-area,prod,tlu,vop]
  FAIL distinct(unit): live=[ha,intld15-2021,nominal-usd-2021,number,t] local=[ha,intld15-2021,nominal-usd-2021,number,t,tlu]
  ok: distinct(stat) identical = sum
  ok: rows local 8065336 vs live 8065336 (identical)
  ABORT exposure reference (crop-livestock_all): gate failure, nothing uploaded
--reference-only --res 0.25:  same two FAILs; rows 5667864 = 5667864; ABORT
--family-only --res 0.05:     ok x5 for vop_nominal-usd-2021 (1508624 = 1508624) and vop_intld15-2021 (986408 = 986408); would upload + refresh aliases
--family-only --res 0.25:     ok x5 for usd (1060176 = 1060176) and intld (693192 = 693192)
```

**Why it stops.**
- The rebuilt reference is post-#52: the `total-tropical` and `total-highland` rows are relabelled `exposure=number/unit=number` to `exposure=tlu/unit=tlu`.
- That adds a sixth distinct value to both columns. Row counts are identical; the rows were relabelled, not added.
- C1's gate expects the TLU slot to read back from S3, so you meant this move to publish. The publisher's distinct gate (`scripts/r3_publish_tiers.R:541-558`) has no way to declare it:
  - `distinct(exposure)` has no allow-flag at all.
  - `--allow-unit-vintage-change` only accepts an equal-cardinality swap (5 to 6 is refused at :554-556 as "a unit is being lost or invented").
  - The only remaining route is `--skip-gates`, which is forbidden.
- So this is a stop the block did not foresee, not one I can clear inside it.

**Why I did not publish the family alone.** Its gates are clean, but C1 is one denominator. Publishing the family twins without `crop-livestock_all` would leave S3's reference and family split across #54. Everything comes from the same B3 rebuild, so the family can wait for the reference.

**Side finding, reported not fixed: a gate ABORT still exits 0.** Both reference dry-runs printed `ABORT ... gate failure` and returned exit 0. The #47 D live loop (`rc=$?; [ $rc -eq 0 ] || break`) would therefore **not** break: run live as written, it would abort the reference and go on to publish the family. Worth a non-zero exit on ABORT before block C re-runs.

**State.**
- **No S3 writes:** dry-runs only, and the publisher logged "backup upload skipped".
- R/3 not run; no parking done for C2.
- No code, gate, fixture or tolerance touched. `$PARK` and the six tables untouched. Nothing running.

**Needs from macbook, one of:**
- (a) a declared-move flag in the publisher for the #52 TLU slot (e.g. allow exactly `+tlu` in `exposure` and `unit`, with total-* row counts conserved), plus a fixture;
- (b) a call to publish the reference without the #52 relabel. That needs a different local table, so it is a B3 question.

Then re-issue C1.

---


### MACBOOK 2026-11-16 — **block C: Pete GO.** Publish the rebuilt denominator, rebuild and publish the R/3 intld tier

Pete's GO (2026-11-16) for block C exactly as the A/B RESPONSE left it: the reference publish and
the R/3 intld tier. Scope is the intld chain only — do **not** rebuild or publish the nominal,
head_n, prod or harv-area tiers (the post-#52 TLU rows in head_n go with a later version). Follow
the #47 block D+E runbook (`archive/dispatches/DISPATCH_cglabs_issue47_livestock_price.md`,
MACBOOK 2026-10-21 and the E3 restart lessons above it) with the intld substitutions below. This
block authorises `scripts/r3_publish_tiers.R`, including `--reference-only` / `--family-only`.
Standing rules: node quiet first; background every upload and the R/3 run; `R3_CHECK_WORKERS=4`;
watch pid-files, not `pgrep` self-matches; check §4.1 minutes + `failed_*.txt` + mtimes before
trusting exit 0; stop at any gate that deviates.

**C0 — the B artifacts are still the gated ones.** The B3 rebuild is 14 days old and the obs
W0-W4 work (2026-11-13/14) touched exposure-adjacent code since. Verify the six rebuilt 0.4.4
parquets (both res) still carry mtimes 2026-11-02 19:08-19:15 and that `$PARK =
Data/_parked_issue54_20261102_185857` is intact. **If any table is newer: STOP** — report what
rewrote it; the fix is to re-run B3's gates, not to publish.

**C1 — publish the denominator** (same loop as #47 D): dry-run first, then
`--reference-only` / `--family-only` at `--res 0.05` and `0.25`, backgrounded, one log.
Gate C1 (invariants): the publish touches only `domain=exposure/type=combined/...` keys; per key,
S3 size = local; the four #54 pairs and the TLU slot read back from S3 exactly as gate 054-REF
measured them; livestock intld total on S3 = 89.1075 B (res-25) / 89.1110 B (res-05).

**C2 — R/3 intld tier.** Park the existing local intld R/3 outputs by move (rollback), leave
`FORCE_OVERWRITE` unset, run R/3 restricted to the intld variable for both timeframes as #47 E ran
the usd one (`R3_TIMEFRAMES`, absolute-path sourcing, nohup). Gate C2: §4.1 clean (no
`failed_*.txt`, fresh mtimes on every intld output, plausible section minutes in the log).

**C3 — tier gates, then publish.** Run the intld equivalents of #47 E's checks (tier total vs the
new reference; drift vs the live tier declared, not silent). Gate C3 (invariants): crop rows
unchanged vs live; livestock changes confined to the four (iso3, class) pairs, each within 2 % of
the probe's NEW values; continental tier intld total moves by a factor > 1 and < 1.01. Then publish
the six `variable=vop_intld15-21` keys (+ sidecars) via `scripts/r3_publish_tiers.R` with the
`--variables`/`--drift-exposure` form the #47 E block used, dry-run first, backgrounded.

**C4 — verify and report.** Per key (denominator 18 + tier 6 + sidecars): size = local, ETag vs
local MD5 (recompute multipart where needed), anonymous HTTPS 200. RESPONSE carries the gate
outcomes and a pin table (key, size, ETag, LastModified, x-amz-version-id) for every touched key —
macbook updates `metadata/cdh/pins/atlas-hazards-2026.11_s3-versions.csv` and dates the
changelogs from it. Commit, push, verify it landed on `origin/develop`.

---

### RESPONSE — cglabs 2026-11-02 — **A and B complete, every gate PASS.** GATE 0 PASS; the four flagged rows are exactly LBY/SLE cattle_meat, LSO poultry_meat, SWZ sheep_meat; GATE 054-REF PASS 3/3 at both resolutions. Two findings: A2 shows the **raster** already disagrees for DJI and ESH, and the rebuilt res-25 twin is now **post-#52**, which supersedes the `G6B_NO_PRODUCT_CROPS` workaround in `DISPATCH_cglabs_obs_s3_r3.md`. No S3 writes, R/3 not run, parked copies kept.

Stamp `20261102_185857`. `$PARK = {NWD}/Data/_parked_issue54_20261102_185857` (68 MB, six parquets + `fao_vop` + three `variable=vop_intld15-*` dirs). Logs `logs/issue54_*_20261102_185857.log`.

## A

**A0.** HEAD `931e5a7` (`peetmate`, the RESPONSE (5) commit on the S3/R3 thread). No other pipeline R job was running. All **fourteen** fixtures print `ALL ... PASSED`:

```
fixture_crop_heat_interactions  fixture_fao_code_join      fixture_gate_054
fixture_gate_zonal_basis        fixture_interaction_builder fixture_intld_guard
fixture_livestock_price         fixture_price_fill          fixture_publish_tier_gates
fixture_qaqc_denominator        fixture_r3_physical_tiers   fixture_tlu_slot
fixture_upstream_run_params     fixture_vop_allocate
```

(`fixture_upstream_run_params` needs about 4 minutes — my first pass timed it out at 100 s and printed `Terminated`; re-run alone it passes. Not a failure.)

CSV `{glw2020_pro_dir}/fao_vop/glw4-2020_vop_intld15-2021-prices-used.csv`, mtime **2026-10-21 16:54:23**, columns `iso3,glw3_name,atlas_name,value,glw3_no,variable`. **No `intld_source` column** — the pre-guard vintage, so gate 0 can reproduce the live values.

**A1 / A3 gate 1.** Probe ran clean, 0.6 min, wrote 1,485 rows to
`{NWD}/checks/issue54_intld_guard_20261102_185857.csv` (190 KB, committed under `R/checks/041_outputs/`).

**A3 gate 2 — GATE 0 PASS.** 495 rows; live total **89.020 B**, probe OLD total **89.020 B**, **0 rows differ**.

**A3 gate 3 — invariants.**

| invariant | result |
|---|---|
| flagged rows are exactly LBY cattle_meat, SLE cattle_meat, LSO poultry_meat, SWZ sheep_meat (y2021) | **4 rows, exactly those** |
| every non-flagged row unchanged | **TRUE** (0 rows differ) |
| every flagged row: nominal > 0, intld_new > 0, intld_new == nominal / basis_median | **TRUE** |
| NEW / OLD continental > 1 and < 1.01 | **1.00103** (89.020 B → 89.112 B, +0.092 B) |
| per-(iso3, class) factor table lists exactly four pairs | **four changed pairs** — see the note below |

The flagged table, y2021:

| iso3 | atlas_name | glw3 | production_t | nominal_M | intld_old | intld_new_M | basis_ratio | basis_median | factor |
|---|---|---|---|---|---|---|---|---|---|
| LBY | cattle_meat | cattle | 6,330.00 | 37.95 | 1,000 | 48.34 | 38,000 | 0.785 | 48,340 |
| SLE | cattle_meat | cattle | 11,108.44 | 32.83 | 1,000 | 41.81 | 32,800 | 0.785 | 41,810 |
| LSO | poultry_meat | poultry | 585.15 | 1.05 | 2,000 | 0.61 | 525 | 1.720 | 305.4 |
| SWZ | sheep_meat | sheep | 165.53 | 0.85 | 0 | 0.77 | NA | 1.100 | Inf |

`intld_source`, y2021: `fao gpv constant-I$` 386 rows / 89.0200 B, `NA` 105 rows (no production), **`basis fallback` 4 rows / 0.0915 B**. Three flagged rows had `intld_old > 0` (ratio beyond the band), one had `intld_old <= 0`. Item medians: cattle_meat 0.7852, cattle_milk 1.4520, goat_meat 0.7426, goat_milk 1.7320, pig_meat 1.3110, poultry_eggs 1.9520, poultry_meat 1.7190, sheep_meat 1.0990, sheep_milk 1.6120.

**Note on the fourth invariant, reported rather than stopped on.** The per-(iso3, glw3 class) factor table prints **36 rows**, not four. Thirty-two of them are `old = 0, new = 0, factor = NaN` — countries with no livestock of that class, identity rows. Exactly **four** rows carry a change, and they are the four expected:

| iso3 | class | old | new | factor |
|---|---|---|---|---|
| LBY | cattle | 60,195,000 | 108,530,133.2 | 1.803 |
| SLE | cattle | 61,149,000 | 102,962,163.2 | 1.684 |
| LSO | poultry | 1,739,000 | 2,347,705.1 | 1.350 |
| SWZ | sheep | 0 | 772,261.8 | Inf |

I read this as the invariant holding — four pairs move — with the table printing unfiltered. I did not treat it as a stop, and I changed nothing. If you meant the printed table itself to have four rows, that is a one-line filter in the probe's reporting, and your call.

**Other year sets.** y2015: OLD 77.549 B → NEW 77.902 B, **6** flagged (GNQ cattle_meat and LBY sheep_meat on top of the four). y2020: OLD 86.606 B → NEW 86.695 B, **4** flagged, the same four. Only y2021 is gated.

**A2 — the TLU rasters. Finding: the raster already disagrees.**

The command as written stops on `object 'geoboundaries' not found` — `R/0_server_setup.R` defines `geo_files_local` but not `geoboundaries`, which 0.4.1 builds at its lines 88-93. I rebuilt it exactly as 0.4.1 does (`read_parquet(geo_files_local[1]) |> st_as_sf() |> vect() |> aggregate("iso3")`) and changed nothing else. Layer names are the expected `<species>_<system>`.

```
DJI tropical: total layer    59139 | weighted species   363300 | ratio 0.163
ESH tropical: total layer     4139 | weighted species    52681 | ratio 0.079
KEN tropical: total layer 16290303 | weighted species 16290303 | ratio 1.000
```

So **0.4.1 is where DJI and ESH diverge**, not 0.4.4 — the `total_*` raster layer itself is short against the weighted species sum, and the table inherits it. KEN reconstructs exactly. ESH (0.079) sits on your live-reference figure (0.081); DJI reads 0.163 here against your 0.067, and these rasters were rebuilt by B1 minutes earlier, so the DJI figure is current, not the published one. A finding for #52's second item, as you framed it. No fix attempted.

## B

**B0.** `$PARK = {NWD}/Data/_parked_issue54_20261102_185857`, 68 MB. All six parquets present (res-05 and res-25 of `exposure_adm_sum_spam20-20_glw420-20`, `vop_nominal-usd-2021_adm_sum_spam20_glw420`, `vop_intld15-2021_adm_sum_spam20_glw420`), plus `fao_vop/` and `variable=vop_intld15-{2015,2020,2021}/`.

**B1 — 0.4.1, both resolutions.** Both exit 0 (driver log empty). res-25 19:02:13 → 19:03:41 (1.5 min), res-05 19:03:41 → 19:06:11 (2.5 min).

- Gate 1: `exposure grid = base_rast_nexgddp.tif | res 0.2500 | tag res-25` and `base_raster.tif | res 0.0500 | tag res-05`. Correct.
- Gate 2: the res-25 y2021 guard line and table are **identical to the probe**, same four rows, same values:

```
[19:03:02] [0.4.1]   y2021 (#54 intld guard): 4 row(s) beyond 4x the item median ratio or with constant-I$ <= 0 -> nominal / item-median ratio
     iso3   atlas_name    nominal intld_was basis_ratio basis_median basis_n   intld_now
1:    LBY  cattle_meat 37953097.5      1000       38000        0.785      50  48336133.2
2:    SLE  cattle_meat 32832105.3      1000       32800        0.785      50  41814163.2
3:    LSO poultry_meat  1049759.1      2000         525        1.720      49    610705.1
4:    SWZ   sheep_meat   848738.5         0          NA        1.100      49    772261.8
```

- Gate 3: the y2021 `price_source` table **diffs clean against the #47 block C response** (`archive/dispatches/DISPATCH_cglabs_issue47_livestock_price.md`, lines 827-834) — byte-identical after whitespace trim. The nominal side did not move.

**B2 — gate 047 re-read.** `GATE 047 FAIL: 3/6 checks pass`, which is the predicted shape:

| check | result |
|---|---|
| nominal-usd 2021 | **ok** — 109.070 B vs 109.070 B, 0 differ |
| nominal-usd 2015 | **ok** — 95.475 B vs 95.475 B, 0 differ |
| nominal-usd 2020 | **ok** — 101.538 B vs 101.538 B, 0 differ |
| intld15 2021 | FAIL — 89.112 B vs 89.020 B, **4 differ** |
| intld15 2015 | FAIL — 77.902 B vs 77.549 B, **6 differ** |
| intld15 2020 | FAIL — 86.695 B vs 86.606 B, **4 differ** |

y2021 differing rows: SWZ sheep_meat 772,261.8 vs 0; LBY cattle_meat 48,336,133.2 vs 1,000; SLE cattle_meat 41,814,163.2 vs 1,000; LSO poultry_meat 610,705.1 vs 2,000. **Exactly the four.** The y2015 six and y2020 four match the probe's flagged lists for those year sets row for row. **No other row differs anywhere.**

**B3 — 0.4.4, both resolutions.** Both exit 0. res-25 19:08 → 19:10:23, res-05 19:11 → 19:14:41.

- Gate 1: `section 2.9 (#52): 10194 TLU rows (crop total_tropical, total_highland) relabelled exposure=number/unit=number -> exposure=tlu/unit=tlu` at res-25, **14506** at res-05. §3.1 `units present in extraction` includes **`tlu`** at both; kept = `number, ha, t, nominal-usd-2021, intld15-2021, tlu`; dropped = the 2015/2020 vintages, as designed.
- Gate 2: **`GATE 054-REF PASS (res-25): 3/3 tables pass`** and **`GATE 054-REF PASS (res-05): 3/3 tables pass`**. Both exit 0. Per table: rows match with 0 unmatched and no duplicate keys; `#52 TLU slot ok` (res-25 10,194 → 10,194, res-05 14,506 → 14,506, values identical); `not-#54 rows identical` 0 differ; `#54 pairs vs probe NEW ok` (4 pairs, 0 off by > 2 %); `intld livestock total ok` (res-25 89.0160 B → 89.1075 B, res-05 89.0194 B → 89.1110 B, both = old + the probe's 91.529 M). The nominal twin carries `#54 pairs present but unchanged: ok` — it does not move.
- Gate 3a: `R/qaqc_vop_vs_faostat.R` — livestock median 1.00, **within 0.9-1.1 = 239/242**, worst LBY/cattle 1.80. Of the 36 livestock rows outside the band, **32 are `0 / 0 = NA`** (no livestock of that class at all) and the remaining **4 are exactly the guarded pairs**: SWZ sheep Inf (FAO I$ = 0, the placeholder), LBY cattle 1.803, SLE cattle 1.684, LSO poultry 1.350. Crop side unchanged: median 0.98, 37/50, 2 FAOSTAT quantity pins (CAF and GIN item 656). So the shape is 239/242 rather than 238/242 because the SWZ row is non-finite and falls outside the denominator, not because a fifth row moved.
- Gate 3b: `vop_cross_basis_gate.R` **GATE PASS at both resolutions**. res-25: 2 out-of-band pairs with no national constant-I$ allocation (border spill, #18 — TCD:yams, BEN:bean), 1 named expected residual (ETH:tea), 409 one-sided pairs. res-05: 4 named expected residuals (ETH:tea, TGO:oilpalm, CAF:robusta-coffee, GNB:maize), 557 one-sided pairs. **Nothing new, and `LBY`, `SLE`, `LSO`, `SWZ` appear nowhere in either output.**

## One consequence you will want before block C

0.4.4 has just rewritten `{NWD}/Data/exposure/exposure_adm_sum_spam20-20_glw420-20_res-25.parquet` (mtime 19:10:10), and the new file is **post-#52**:

| crop | exposure | unit | rows |
|---|---|---|---|
| total-highland | **tlu** | **tlu** | 5,097 |
| total-tropical | **tlu** | **tlu** | 5,097 |

That is the same file the observed publisher uses as its G6b basis. **It supersedes the `G6B_NO_PRODUCT_CROPS` workaround I proposed in `DISPATCH_cglabs_obs_s3_r3.md` RESPONSE (5) (b)** — the `head_n` basis no longer contains the two TLU aggregates, so the 74 `missing_product` rows should fall out on their own. I have **not** re-measured G6b against the new basis; say the word and it is a two-minute read-only check. The pre-#52 copy is in `$PARK/exposure/` if you want the old behaviour back.

## State

Rebuilt tables sit **locally, ahead of S3**, as intended. **No S3 writes of any kind** — no publisher, no `s3_upload.R`. **R/3 not run.** Parked copies untouched. No pipeline code, gate, fixture or probe edited; no tolerance loosened; `metadata/haz_metadata.csv` and `metadata/cdh/*.yaml` untouched. Nothing running.

---

### MACBOOK 2026-10-23 — A + B. Measure (A), then rebuild 0.4.1 + 0.4.4 locally and gate it (B). No S3 writes.

**What this is about.** Two changes to the exposure reference, both coded on `develop` and
fixture-tested on the macbook, neither run on real data yet:

- **#54** (`d4d7c3f`). FAOSTAT's "Gross Production Value (constant 2014-2016 thousand I$)" holds a
  placeholder, flag E, in every year, for four livestock rows in Atlas scope: LBY cattle meat = 1,
  SLE cattle meat = 1, LSO chicken meat = 2, SWZ sheep meat = 0, against thousands of tonnes of
  production. The reference and the `vop_intld15-21` tier carry LBY and SLE cattle meat at about
  $1,000. `R/price_fill.R::livestock_intld_guard()`, called from 0.4.1 §4.2, replaces a row whose
  nominal / constant-I$ ratio is beyond 4× the item's cross-country median, or whose constant-I$ is
  ≤ 0 while nominal > 0, with nominal / item-median ratio, and names it `intld_source = "basis
  fallback"`. On the #47 probe table (y2021) that band flags exactly those four pairs.
- **#52** (`1825d3a`). 0.4.4 filed the two Tropical Livestock Unit aggregates (`total-highland`,
  `total-tropical`) under `exposure = "number"` beside the species head counts. New §2.9 relabels
  them `exposure = "tlu"`, `unit = "tlu"`, values untouched, and stops if it finds none; `tlu` is in
  the §3.1 hard-stop unit list.

Nominal USD, crops, prod, ha, phys-area and the species head counts must not move by one unit.

**New on `develop` for this block:** `R/checks/probe_054_intld_guard.R` (the measurement),
`R/checks/gate_054_reference_diff.R` (rebuilt tables vs parked copies), fixtures
`fixture_intld_guard.R`, `fixture_tlu_slot.R`, `fixture_gate_054.R`. All fourteen fixtures pass on
the macbook. The probe and the gate have **not** run on real data; A3 gate 2 is the probe's first
real-data check.

**Pete's decisions (2026-10-23):** LSO and SWZ ride along with LBY and SLE; #52 is bundled. The
threshold-free parts (#14) are a separate thread.

#### A0. Pre-flight

```bash
cd /home/jovyan/atlas/hazards_prototype && git pull --ff-only && git log -1 --format='%h %an %s'
for f in R/checks/fixture_*.R; do printf '%s: ' "$(basename $f)"; Rscript $f 2>&1 | tail -1; done
pgrep -af 'Rscript' | grep -v pgrep || echo "no R jobs running"
Rscript -e 'suppressMessages(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")); f <- file.path(glw2020_pro_dir, "fao_vop", "glw4-2020_vop_intld15-2021-prices-used.csv"); cat(f, "\n exists:", file.exists(f), " mtime:", format(file.mtime(f)), "\n cols:", paste(names(data.table::fread(f, nrows = 1)), collapse = ","), "\n")'
```

Stop if: any fixture does not print `ALL ... PASSED`; another pipeline R job is running; the CSV
is missing. Report the HEAD hash and the CSV's mtime and columns. The CSV was written by the #47
block C run (2026-10-21); its columns must **not** include `intld_source` (if they do, 0.4.1 has
already run with the guard, and gate 0 below cannot reproduce the live values — stop and say so).

#### A1. The probe (background; a few minutes)

```bash
STAMP=$(date +%Y%m%d_%H%M%S); echo $STAMP > logs/issue54_stamp.txt
OUT=/home/jovyan/common_data/nex-gddp-cimp6_hazards/checks/issue54_intld_guard_${STAMP}.csv
mkdir -p "$(dirname "$OUT")" logs
EXPOSURE_RES=0.25 nohup Rscript R/checks/probe_054_intld_guard.R --top 30 --out "$OUT" > logs/probe_054_${STAMP}.log 2>&1 &
echo $! > logs/probe_054_${STAMP}.pid
# watch: while kill -0 $(cat logs/probe_054_${STAMP}.pid) 2>/dev/null; do sleep 30; done; tail -5 logs/probe_054_${STAMP}.log
```

#### A2. #52 item 2, read-only: do the TLU rasters reconstruct for DJI and ESH?

In the live res-25 reference, `total-tropical` is 0.067× (DJI) and 0.081× (ESH) the weighted sum of
the species rows; every other country reconstructs at 1.000 (macbook, DuckDB on S3, 2026-10-23).
This tells whether the raster already disagrees (0.4.1) or only the table does (0.4.4):

```bash
Rscript -e '
suppressMessages(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")); library(terra)
r <- terra::rast(file.path(glw2020_pro_dir, "livestock_number_number_res-25.tif")); cat("layers:", names(r), "\n")
for (iso in c("DJI", "ESH", "KEN")) {
  g <- geoboundaries[geoboundaries$iso3 == iso, ]; m <- terra::mask(terra::crop(r, g), g)
  s <- terra::global(m, "sum", na.rm = TRUE)[, 1]; names(s) <- names(r)
  tlu <- 0.7 * s["cattle_tropical"] + 0.1 * s["sheep_tropical"] + 0.1 * s["goats_tropical"] + 0.2 * s["pigs_tropical"] + 0.01 * s["poultry_tropical"]
  cat(sprintf("%s tropical: total layer %.0f | weighted species %.0f | ratio %.3f\n", iso, s["total_tropical"], tlu, s["total_tropical"] / tlu))
}'
```

If a layer name differs from `<species>_<system>`, print `names(r)` and adapt the four names only.
Report the three lines. No fix here; it is a finding for #52's second item.

#### A3. Gates — stop at each

1. **The probe evaluates without error.** If it dies inside the 0.4.1 slices, stop and paste the
   error and the 20 log lines before it. Do not edit 0.4.1 or the probe to get past it.
2. **GATE 0 PASS.** OLD must reproduce the live CSV's `value` per (iso3, atlas_name), 0 rows
   differing. If it fails, the probe stops itself; paste its table. Do not re-run 0.4.1 to make
   them agree.
3. **Invariants, not figures:**
   - the `basis fallback` rows are **exactly** LBY cattle_meat, SLE cattle_meat, LSO poultry_meat,
     SWZ sheep_meat (y2021). One more or one fewer is a stop;
   - every other row is unchanged (`every non-flagged row unchanged: TRUE`);
   - every flagged row has nominal > 0, intld_new > 0, and intld_new == nominal / basis_median;
   - NEW / OLD continental is > 1 and < 1.01;
   - the per-(iso3, class) factor table lists exactly four pairs.
   If one does not hold, report which. Do not fix it.

**If A3 passes in full, continue to B. If anything stopped, report and do not start B.**

#### B0. Park what B overwrites (copy, do not move)

```bash
STAMP=$(cat logs/issue54_stamp.txt)
Rscript -e 'suppressMessages(source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R")); cat(normalizePath(exposure_dir), normalizePath(glw2020_pro_dir), normalizePath(working_dir), sep="\n")'
# with EXPO, GLWP, WD set from those three lines:
PARK=$WD/Data/_parked_issue54_$STAMP; mkdir -p $PARK/exposure $PARK/glw_processed
cp -a $EXPO/exposure_adm_sum_spam20-20_glw420-20_res-{05,25}.parquet \
      $EXPO/vop_nominal-usd-2021_adm_sum_spam20_glw420_res-{05,25}.parquet \
      $EXPO/vop_intld15-2021_adm_sum_spam20_glw420_res-{05,25}.parquet $PARK/exposure/
cp -a $GLWP/fao_vop $GLWP/variable=vop_intld15-* $PARK/glw_processed/
ls -la $PARK/exposure; du -sh $PARK
```

Stop if any of the six parquets is missing. `$PARK/exposure` is the `--old-dir` for B3.

#### B1. 0.4.1 at both resolutions (background, ~5 min)

```bash
nohup bash -c 'for RES in 0.25 0.05; do EXPOSURE_RES=$RES FORCE_OVERWRITE=1 Rscript -e "source(\"/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R\"); source(\"/home/jovyan/atlas/hazards_prototype/R/0.4.1_create_livestock_exposure.R\")" > logs/issue54_041_${RES}_'$STAMP'.log 2>&1 || exit 1; done' > logs/issue54_041_$STAMP.log 2>&1 &
echo $! > logs/issue54_041_$STAMP.pid
```

Gates:
1. Both runs exit 0; each log's grid line is right (`tag res-25` / `tag res-05`).
2. Each log shows, per year set, a line `(#54 intld guard): N row(s) ...` followed by the flagged
   table. **Paste the y2021 line and table from the res-25 log.** It must list the same four rows
   as the probe, with the same `intld_new`. A fifth row, or a different value, is a stop.
3. The nominal side is untouched: the y2021 `price_source` table equals the one in the #47 block C
   response (`archive/dispatches/DISPATCH_cglabs_issue47_livestock_price.md`).

#### B2. Price gate from #47, re-used as a regression check

```bash
Rscript R/checks/gate_047_prices_vs_probe.R --since "$(date -d "-1 hour" '+%Y-%m-%d %H:%M')" 2>&1 | tee logs/issue54_gate047_$STAMP.log
```

Expected: the **nominal** CSVs still equal the #47 probe's C (that gate's rows 1-3 PASS). The
**intld** CSVs will now differ on the four flagged rows, so its rows 4-6 are expected to FAIL on
exactly those rows — paste the differing rows and confirm they are the four. Any other differing
row is a stop. (The gate predates #54; it is not being loosened, it is being read.)

#### B3. 0.4.4 at both resolutions, then the #54 reference diff gate

```bash
nohup bash -c 'export FORCE_OVERWRITE=1; for RES in 0.25 0.05; do EXPOSURE_RES=$RES Rscript -e "source(\"/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R\"); source(\"/home/jovyan/atlas/hazards_prototype/R/0.4.4_process_exposure.R\")" > logs/issue54_044_${RES}_'$STAMP'.log 2>&1 || exit 1; done' > logs/issue54_044_$STAMP.log 2>&1 &
echo $! > logs/issue54_044_$STAMP.pid
# after both exit 0:
PROBE=$(ls -t /home/jovyan/common_data/nex-gddp-cimp6_hazards/checks/issue54_intld_guard_*.csv | head -1)
for RES in 0.25 0.05; do Rscript R/checks/gate_054_reference_diff.R --old-dir $PARK/exposure --res $RES --probe $PROBE 2>&1 | tee -a logs/issue54_gate054ref_$STAMP.log; done
```

Gates:
1. Both 0.4.4 runs exit 0. Each log carries `section 2.9 (#52): N TLU rows ... relabelled` with
   N > 0, and `section 3.1: units present in extraction = ...` listing `tlu`. If §2.9 stops, or
   §3.1 stops on a missing unit, stop and report.
2. **`GATE 054-REF PASS` at both resolutions (3/3 tables each):**
   - same row set once the old TLU rows are read as `tlu`;
   - the TLU rows relabelled with identical values;
   - every non-#54 row value-identical, including the whole nominal twin;
   - each of the four pairs' admin2 sum within 2 % of the probe's national NEW;
   - the intld livestock total equals old + the probe's (NEW − OLD) within 1 %.
3. Existing gates, unchanged expectations: `Rscript R/qaqc_vop_vs_faostat.R` — livestock reads
   against FAOSTAT's I$ values, so the four guarded rows are expected to be the only ones that no
   longer match (report the count; 238/242 is the shape, not the gate); `Rscript
   R/checks/vop_cross_basis_gate.R --res 0.25` and `--res 0.05` — report anything new and
   whether it involves the four countries.

If any gate deviates, stop and describe it. Leave the parked copies; they are the rollback.

#### B4. Report

For A: the HEAD hash, the fixture lines, the CSV mtime and columns, the probe's GATE 0 lines, the
totals line, the `intld_source` table, the item-median table, the flagged table, the invariant
lines, the four-pair factor table, the other-year-set lines, runtime, the CSV path, and the three
A2 lines. Commit the probe CSV under `R/checks/041_outputs/` if it is under 1 MB (public FAOSTAT
values only).

For B: `$PARK`, exit codes and runtimes, the y2021 guard line and table, the B2 differing rows,
both 0.4.4 §2.9 and §3.1 lines, both gate-054 outputs, the qaqc count and the cross-basis lines.

The rebuilt tables then sit **locally ahead of S3**. That is intended: the reference publish and
the R/3 intld tier follow in block C once Pete has seen this.

#### What NOT to do

- **No S3 writes of any kind:** no `r3_publish_tiers.R` (not even `--reference-only`), no
  `s3_upload.R`.
- **Do not run R/3.**
- **Do not delete the parked copies.**
- **Do not edit pipeline code, gates, fixtures or the probe** to get past a stop. Do not loosen a
  tolerance. If a gate false-fails, that is a finding: describe it.
- **Do not change `metadata/haz_metadata.csv`** or any `metadata/cdh/*.yaml` (they change in the
  commit that publishes).

#### Reporting

Prepend a `### RESPONSE <date>` block above this one, commit
(`Co-Authored-By: <your own model name> <noreply@anthropic.com>`), push, and verify it landed:
`git fetch`; `git log origin/develop..HEAD` empty;
`git show origin/develop:DISPATCH_cglabs_issue54_intld_placeholders.md | grep -c RESPONSE`.
