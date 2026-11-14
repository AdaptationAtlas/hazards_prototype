# DISPATCH — publish the rebuilt R/2.2 hazard-change tables (into v2026.11)

Append-only. Newest block on top. Respond by prepending a `### RESPONSE` block.

### DISPATCH 2026-11-14 (macbook) — verify the 2026-10-15 R/2.2 rebuild, gate it, publish the 10 keys

**Context.** The live `processing=hazard-change` keys (10 parquet, `timeframe=annual`) are the
2026-06-24 CR-093 build. R/2.2 was re-run on the node during #13 B3 (2026-10-15, RESPONSE in
`archive/dispatches/DISPATCH_cglabs_issue13_rebake.md`): exit 0, CR-093 validator 10/10 PASS,
outputs de-duplicated and on the 18-GCM / post-#25 inputs — but never published. Pete's GO
(2026-11-13): publish this rebuild **before** the `v2026.11` freeze. The CDH record
(`metadata/cdh/africa-hazard-change-nexgddp.yaml`, commit `7c09d51`) and its changelog describe the
rebuilt files; the changelog's "pending" row gets its date from this publish.

**Out of scope.** Do not fix or touch anything else: not the disputed-claimant adm0 rows
(issue #11), not the `.preFix*.bak` clutter (clean-up is #59), no other prefix, no re-run of
R/2.2 or any upstream stage. If a gate fails, stop and describe — do not improvise.

**Rules that bite here:** `git pull` first; absolute-path sourcing in `Rscript -e`; uploads in the
background (`nohup … &`, log to `logs/`) because the shell kills foreground jobs at ~2 min; keep
`Data/_parked_r22_pre13_20261015_213020/` and every `_parked_*`/`sandbox/backup/` dir; after
committing the RESPONSE, verify it landed on `origin/develop`.

---

**Step 0 — node quiet?** Nothing that writes under `Data/` may be in flight.
Check `ps aux | grep -i [R]script`, live `logs/*.pid`, and recent log mtimes.
**Gate G0 (invariant):** no running pipeline job. If anything is running or ambiguous: STOP,
report what is running, publish nothing.

**Step 1 — the rebuild is intact and is the 2026-10-15 artifact.** The 10 files (paths exactly as
`R/publish_cr093_r22.R` lists them, `R22_TIMEFRAME=annual`):
`ptot_change_{by_model,ensemble}`, `ptot_diff_{by_model,ensemble}` (under `ptot_perc/`),
`thi_perc_area_{by_model,ensemble}`, `ntx_perc_area_{by_model,ensemble}`,
`stats/haz_freq{,_ensemble}`.
**Gate G1 (invariants):** all 10 exist; every mtime is 2026-10-15 (the B3 run) and none is newer;
the five byte-sizes the B3 RESPONSE recorded still hold —
`ptot_change_by_model` 2,931,264 · `ptot_diff_by_model` 1,465,632 ·
`thi_perc_area_by_model` 3,114,468 · `ntx_perc_area_by_model` 6,228,936 · `haz_freq` 12,457,872.
A newer mtime means something re-ran R/2.2 since: STOP.

**Step 2 — settle the adm0 `thi_perc` shift with an independent reference.** B3 measured: THI %
area differs from the parked 06-24 product at adm0 only (19 countries), worst case
`SDN / GFDL-ESM4 / ssp126 / 2061-2080 / extreme`: 100.0 → 11.5. Unexplained ≠ publishable.
Recompute that one value independently with `terra` straight from the THI mean raster R/2.2 SEC3
reads (same raster, same threshold, zonal over the SDN adm0 polygon — mirror SEC3's logic for this
single combination, nothing more).
**Gate G2 (invariant):** the independent recompute agrees with the NEW value (within a couple of
percentage points, i.e. clearly ~11.5, clearly not ~100). If it agrees with the OLD 100.0, or with
neither: STOP and show the three numbers. Spot-check one more country from the B3 list (KEN or ERI)
the same way if the first passes cheaply.

**Step 3 — validator.** `Rscript R/validate_cr093_real.R` from the repo root.
**Gate G3 (invariant):** 10 PASS / 0 FAIL, as in B3.

**Step 4 — dry-run.** `Rscript R/publish_cr093_r22.R` (no CONFIRM).
**Gate G4 (invariants):** plan lists exactly the 10 canonical `timeframe=annual` keys; every
pre-flight check (iso3 present, 0% NA, non-null row-group stats, ≥1 row, ≥2 iso3) passes.

**Step 5 — publish.** Background it:
`nohup env CONFIRM=1 Rscript R/publish_cr093_r22.R > logs/r22_publish_$(date +%Y%m%d_%H%M%S).log 2>&1 &`
The script backs up each live key to `.preFix-<stamp>.bak` itself. Watch the log to completion.

**Step 6 — verify on S3.** For each of the 10 keys: size equals the local file; the uploaded-iso3
check in the log passed; anonymous read works
(`curl -sI 'https://digital-atlas.s3.amazonaws.com/<key>'` → 200). Then collect, per key:
**size, ETag, LastModified, x-amz-version-id** (`aws s3api head-object`) and put the 10-row table
in the RESPONSE — macbook updates `metadata/cdh/pins/atlas-hazards-2026.11_s3-versions.csv` and the
changelog date from it.

**Step 7 — RESPONSE.** Gates G0–G4 outcomes, the G2 three-number comparison, the step-6 table,
log paths. Commit, push, verify it landed on `origin/develop`.
