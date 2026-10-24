# DISPATCH — cglabs + pascal — observed pipeline S0: where should ingest run?

**Append-only; newest block on top. Each node prepends its own `### RESPONSE — <node>` block.**

### PASCAL → MACBOOK 2026-10-24 — five decisions the probe forces, before S1 is written

The probe block below answers A/B/C. This block is the part that needs a macbook decision: each item
changes what S1 ingests, or where it runs, and none of them can be settled on the node.

**1. CHIRTS vintage — the one that blocks S1's design.**
The node's 2005-2020 CHIRTS-ERA5 is a mirror of the CHC experimental tree taken October 2023
(`TIFFTAG_DATETIME=2023:10:02`; the 2015-06-15 file is byte-identical to the copy downloaded today, so
that part of the tree has not been regenerated since). 2021-2025 would be fetched now, from a product
CHC labels "experimental" and does not version. A 19-season series built from a 2023 snapshot for
2005-2020 and a 2026 pull for 2021-2025 is two vintages in one series, with no version string that
would ever reveal it.

Three ways out, in increasing cost:
- accept the mix, and record the split in the manifest (snapshot date per file, as `PLAN` §1 already
  plans for CHIRPS) — cheapest, but the series carries a seam at 2020/2021;
- re-pull 2005-2025 entirely, so one vintage covers the series — ~384 GiB, of which 292 GiB is
  re-downloading what is already on disk, from a host that currently serves this client at
  8-10 kB/s (item 2);
- ask CHC whether the 2005-2020 tifs have changed since October 2023, and re-pull only if they have.
  A HEAD request per file would answer it for 11k files cheaply, if the throttle allows HEADs.

Recommend the third, falling back to the first. **Macbook's call.**

**2. The CHC route, before any ingest is scheduled.**
`data.chc.ucsb.edu` throttled this client to 8-10 kB/s after roughly 100 MB and had not recovered
after 15 minutes idle. Eight concurrent streams aggregated 0.066 MB/s — worse than one. The node is
not the constraint: eight parallel streams to the FAO/GCS mirror aggregate 17.7 MB/s on the same
session. At the throttled rate, 92 GiB (2021-2025 Tmax+Tmin) is about 120 days; unthrottled at the
first-pull rate of 3.9 MB/s it is about 7 hours.

So the question is not "which node has the bandwidth" — it is "how do we get CHIRTS out of CHC at
all". Options to choose between: a politeness-budgeted crawl (1-2 streams, exponential backoff,
resumable, run over days); the **yearly netCDFs** CHC also publishes, which may be served from a
different path or rate class and would be 5 files rather than ~3,650; or writing to CHC for bulk
access or a DOI, which `PLAN` §1 already flags as needed for the licence anyway. Worth testing the
netCDF route before committing — one file would settle it.

**3. Where ingest runs — the probe's own question.**
On the evidence, the two nodes are not meaningfully different for this work, because the bottleneck
is the remote host, not local egress. What does differ: PASCAL holds 292 GiB of the CHIRTS series
already, and CGlabs would have to download all of it. If the 2023 vintage is accepted (item 1), that
makes PASCAL the cheaper node by ~290 GiB of transfer. If a full re-pull is chosen, the two nodes are
equivalent and the choice should fall out of CPU and free space instead.

Note the caveat on PASCAL's figures: this session ran inside a JupyterHub container capped at 8 CPUs
and 32 GiB (`cpu.max` = `800000 100000`). RStudio Server and plain SSH are not capped and reach the
full 80 cores / 397 GiB, per `server-environment.md` §3 — but the agent session had no route to
either, so the compute numbers in the RESPONSE block are a floor, not the machine's capability.

**4. CHIRPS v3 is not on the node, and v2.0 is not a substitute.**
The tree is CHIRPS **v2.0** daily `p05` — global, 1981-01-01 to 2026-01-31, no missing days, 928 GiB.
There is no v3 and no `p25`. `PLAN`'s grid decision rests on v3 `p25` aligning with the NEX-GDDP
0.25° grid, so v2.0 `p05` is a different product on a different grid: usable as a cross-check,
not as the input. The v3 `p25` download stands in full, from the same CHC host as item 2 — which
means item 2's answer governs CHIRPS as well as CHIRTS. Worth measuring the `products/CHIRPS/v3.0/`
path separately: it may not sit in the same rate class as `experimental/`.

**5. Two corrections to `PLAN_obs_hazard_pipeline.md` §1.**
- AgERA5 SRF measured **10.4 MB/day** (10,443,448 B for 2015-06-15), not the 12.8 MB in the table.
  Over the ~7,670 days of 2005-2025 that is ~75 GiB for SRF alone, against ~92 GiB at the table's
  figure. Neither matches the "25-30 GB of AgERA5" in this dispatch's preamble, and RH15 would
  roughly double whichever is right — so it would help to know which basis the 25-30 GB uses
  (post-crop? SRF only? fewer years?), since the other two preamble figures read as raw download
  sizes.
- CHIRTS-ERA5 daily files are striped, not tiled (`Block=7200x1`, LZW). A `/vsicurl` windowed read
  therefore pulls whole 7200-pixel strips, so the "stream and crop" design in S1 saves far less on
  CHIRTS than it does on AgERA5, which is a genuine 256-tiled COG. Cropping to Africa still saves
  disk; it does not save much transfer.

**One more thing worth knowing:** `CHIRTS-ERA5.daily_Tmax.2008.11.24.tif` is missing from the node's
tree (Tmin is complete, 10,958 files; Tmax has 10,957). One day, inside the baseline window. And
`chirts_era5/tavg/` is **not** a global CHIRTS variable — it is a 109 × 115 East-African crop whose
band `Description` reads `CHIRTS-ERA5.daily_Tmax.<date>`, i.e. a derived product, mislabelled, by an
unknown author. Nothing should read it as Tavg until that is traced.

**Not blocking, but unanswered:** there is still no cglabs RESPONSE block on this dispatch, so the
two-node comparison the probe was written to settle is half-complete.

---

### RESPONSE — pascal 2026-10-24

**Session.** JupyterHub, **Small profile**: `cpu.max` = `800000 100000` → **8 usable CPUs**;
`memory.max` = 32 GiB. `nproc` still reports 80 and affinity still shows 0-79, as
`server-environment.md` §3 warns. No SSH or RStudio session was available to this agent, so every
figure below is from inside the capped container. Read-only throughout; no pipeline run, no S3 write.
Test downloads went to a temporary directory on the bulk data root and were deleted (see *Cleanup*).

**Headline.** The node already holds **CHIRTS-ERA5 daily Tmax and Tmin for 1991-2020**, not 2005-2025
— 2021-2025 are absent. And the CHIRPS tree is **v2.0 daily at 0.05°**, not v3, with no `p25`.
Separately, **`data.chc.ucsb.edu` throttles this client hard**: the first pull ran at 3.9 MB/s, and
after roughly 100 MB every CHC stream collapsed to **8-10 kB/s** and was still there 15 minutes later. The node's own
egress is fine — 8 parallel streams from the FAO/GCS mirror aggregate **17.7 MB/s**. So A4 is a
statement about CHC, not about PASCAL.

#### A — bandwidth

| test | run 1 | run 2 | notes |
|---|---|---|---|
| **A1** CHIRTS-ERA5 daily Tmax, 2015.06.15 | 26,321,804 B in 6.815 s → **3.86 MB/s** | 26,321,804 B in 7.044 s → **3.74 MB/s** | HTTP 200 both. Byte-identical to the copy already on the node. |
| **A2** AgERA5 v2.0 SRF, 2015-06-15 | 10,443,448 B in 2.700 s → **3.87 MB/s** | 10,443,448 B in 2.445 s → **4.27 MB/s** | HTTP 200. **10.4 MB, not the 12.8 MB in the dispatch** — worth re-checking the per-day figure in `PLAN` §1. |
| **A3** ASAP FPAR, `fpar_20150701.tif` (nearest to mid-2015) | 315,692,952 B in 179.9 s → **1.75 MB/s** | 315,692,952 B in 178.0 s → **1.77 MB/s** | HTTP 200. 301 MiB, matches the ~305 MB estimate. |
| **A4** 8 concurrent CHIRTS days (2015-06-01…08) | **0.066 MB/s aggregate** | — | Measured over a 120 s steady-state window; the run never completed and was killed at 75 MB of 210 MB. See below. |
| **A5** `gdal_translate -projwin -18 38 52 -35` over `/vsicurl/<A2>` | **26.1 s** | **25.5 s** | HTTP 200, 2,046,574 B out. `gdal-config --version` → **3.4.3**. |

**A4 in context.** CHC rate-limits, and the limit is sticky:

| probe | throughput |
|---|---|
| first CHIRTS stream of the session (A1 run 1) | 3.86 MB/s |
| 9th single stream opened *while* the 8-way run was active | 9.97 kB/s |
| single stream *after* the 8-way run was killed, nothing else running | 8.46 kB/s (timed out at 240 s, HTTP 200, 2.0 MB) |
| AgERA5/GCS single stream at the same moment | 4.01 MB/s |
| ASAP/JRC ranged request at the same moment | 1.42 MB/s |
| **8 parallel AgERA5/GCS streams** | **17.73 MB/s aggregate** |
| CHIRTS stream after 5 min with the network idle | 9.83 kB/s |
| CHIRTS stream after a further 10 min idle | 9.97 kB/s |

So the 390 GiB CHIRTS figure in `PLAN` §1 is not bandwidth-bound by this node — it is bound by what
CHC will serve. Parallelism against CHC made it worse, not better, which is the opposite of the D-list
assumption. Any CHIRTS ingest needs a politeness budget (1-2 streams, backoff) and should be planned
in days, not hours, unless a bulk route (the yearly netCDFs, or asking CHC) is used instead.

Two caveats on A5. The AgERA5 file is a 256-tiled COG, so the window really is a partial read. The
CHIRTS files are **not** tiled — `Block=7200x1`, LZW, striped — so a `-projwin` over `/vsicurl` still
pulls whole strips. I tried the same window against the A1 URL for comparison; under the throttle it
had not finished after 18 minutes and I killed it. That is the throttle, not a `/vsicurl` failure:
every URL in this dispatch returned **200**, and `/vsicurl` worked against A2.

#### B — resources

| item | value |
|---|---|
| `nproc` | 80 (**not usable** — see cgroup line) |
| usable CPUs (`cpu.max`) | **8** (`800000 100000`); cgroup `memory.max` 32 GiB |
| `free -g` (host-wide) | total 397, used 5, free 391, available 390; swap 3 |
| `uptime` | up 198 days; load average 1.12 / 0.44 / 0.15 at start of probe — the box was idle |
| bulk data root | 50 TB network filesystem, 26 TB used, **24 TB free** (53 %) |
| R | 4.5.1; `terra` **1.8.93** |
| GDAL via terra | **3.4.3**; PROJ 8.2.1; GEOS 3.10.2 |
| GDAL on `PATH` | `gdal-config` 3.4.3, `gdalinfo` GDAL 3.4.3 (released 2022/04/22) |

#### C — the raw climate trees

**`chirts_raw` → `…/common/climate/chirts_era5`**

| | tmax | tmin | tavg |
|---|---|---|---|
| product | CHIRTS-ERA5 | CHIRTS-ERA5 | derived, see below |
| cadence | **daily** | **daily** | daily |
| naming | `CHIRTS-ERA5.daily_Tmax.YYYY.MM.DD.tif` | `…daily_Tmin…` | `…daily_Tavg…` |
| first / last | 1991-01-01 / **2020-12-31** | 1991-01-01 / **2020-12-31** | 1991-01-01 / 2020-12-31 |
| files | 10,957 | 10,958 | 10,958 |
| size | **273 GiB** | **274 GiB** | 298 MiB |
| extent / size | −180…180, −60…70; 7200 × 2600 | same | **29.55…35.00 E, −1.50…4.25 N**; 109 × 115 |
| resolution | 0.05°, EPSG:4326, Float32, LZW, `Block=7200x1` | same | 0.05° |

- **Variables present: Tmax, Tmin only.** No RH, no radiation, no precipitation in this tree.
- **`tavg` is not a global CHIRTS variable.** It is a ~5.5° × 5.75° East-African subset at 0.05°, and
  each file's band `Description` reads `CHIRTS-ERA5.daily_Tmax.<date>` — a derived crop, mislabelled.
  Do not treat it as Tavg without tracing who made it.
- **One gap:** `CHIRTS-ERA5.daily_Tmax.2008.11.24.tif` is missing. Tmin is complete.
- No readme anywhere in the tree. Provenance comes from the tif tags:
  `TIFFTAG_DOCUMENTNAME=/home/chc-data-out/experimental/CHIRTS-ERA5/tmax/tifs/daily/2015/…`,
  `TIFFTAG_SOFTWARE=IDL 8.9.0`, `TIFFTAG_DATETIME=2023:10:02`. The 2015-06-15 file is byte-identical
  to the one A1 just downloaded, so this is a straight mirror of the CHC experimental tree, as of
  October 2023.

**`chirps_raw` → `…/common/climate/chirps`**

- `global_daily/tifs/p05/` — **CHIRPS v2.0, daily, global**, `chirps-v2.0.YYYY.MM.DD.tif`,
  **1981-01-01 → 2026-01-31**, 0.05°, −180…180 / −50…50, 7200 × 2000, Float32.
  **No missing days** across the whole span. 1981-2020 carry both `.tif` and `.tif.gz` (730/732 files
  per year, ~21 GiB); 2021 is part-converted (365 tif + 151 gz); 2022 onward are `.tif` only
  (~20 GiB/yr). `global_daily` totals **928 GiB**.
- `global_daily/tifs/ndvi/` — **empty**.
- `forecast_daily/test/` — **empty**.
- `chirps_gefs/` — GEFS forecast ensemble tifs (`chirps_gefs_X01__lag_*.tif`), 2,924 files at the top
  level. Not relevant to S1.
- **No v3 anywhere, and no `p25` tree.**

#### The question that matters

| | answer |
|---|---|
| **CHIRTS-ERA5 daily Tmax/Tmin present for 2005-2025?** | **Partly. 2005-2020 yes** — complete bar one Tmax day (2008-11-24) — **2021-2025 no.** The 2005-2020 slice is 292 GiB already on disk. |
| **CHIRPS v3 daily present?** | **No.** CHIRPS **v2.0** daily `p05` is present and complete 1981→2026-01-31, but `PLAN` D-list asks for v3 `p25`. |

What this changes for S1, for the macbook to weigh:

1. The CHIRTS download shrinks from ~390 GiB to **5 years × 2 variables ≈ 92 GiB** (2021-2025), plus
   one missing day — *if* the 2023 mirror is acceptable as the 2005-2020 source. CHIRTS-ERA5 is
   flagged "experimental" and unversioned, so 2005-2020 from an October-2023 snapshot and 2021-2025
   fetched now may not be the same vintage. That is a provenance decision, not a technical one.
2. At CHC's observed throttled rate those 92 GiB are not a one-afternoon download. Reading the
   2005-2020 slice off local disk is free; only 2021-2025 needs the network.
3. CHIRPS v3 `p25` has to be downloaded whatever happens. v2.0 `p05` on disk is a different product
   and a different grid, so it is a fallback or a cross-check, not a substitute.

**Cleanup.** The temporary directory under the bulk root was removed; `logs/obs_s0_probe_*` kept
(gitignored). No other writes.

---

### MACBOOK 2026-10-23 — READ-ONLY probe on BOTH nodes. Measure network and resources; on PASCAL, also inventory the CHIRTS/CHIRPS trees.

**Why.** `PLAN_obs_hazard_pipeline.md` (v2, decisions D1-D5) needs about 390 GiB of CHIRTS-ERA5 daily
data, 25-30 GB of AgERA5 and about 50 GB of ASAP FPAR, streamed and reduced to 0.25° Africa. Compute
is small (S2-S5 under ~1 h). **Download bandwidth decides the node.** PASCAL's `chirts_raw` points at a
`chirts_era5` tree; if that tree already holds CHIRTS-ERA5 *daily*, the largest download disappears.

**Scope.**
- Read-only. Test downloads go to a temporary directory **under your bulk data root** (not `/`, not
  `/home`), and are deleted at the end.
- No pipeline runs, no S3 writes.
- On PASCAL, use an SSH or RStudio session, or say which JupyterHub profile you are in and read the
  `cpu.max` value (`server-environment.md` §3).
- Background anything over about a minute; logs go in `logs/obs_s0_probe_<STAMP>.log`.
- Public repo: no IPs, hostnames or mount identifiers in the RESPONSE (`AGENTS.md` §3 grep before
  committing).

#### A — bandwidth (both nodes)

Time each download with `curl -s -o "$TMP/<name>" -w '%{size_download} %{time_total} %{speed_download}\n' <url>`.
Run each twice and report both.

- **A1. CHIRTS-ERA5 daily Tmax**, about 26 MiB:
  `https://data.chc.ucsb.edu/experimental/CHIRTS-ERA5/tmax/tifs/daily/2015/CHIRTS-ERA5.daily_Tmax.2015.06.15.tif`
- **A2. AgERA5 v2.0 SRF**, about 12.8 MB:
  `https://storage.googleapis.com/fao-gismgr-c3s-data/DATA/C3S/MAPSET/AGERA5-SRF/C3S.AGERA5-SRF.2015-06-15.tif`
- **A3. ASAP FPAR, one dekad**, about 305 MB. Pick the file nearest mid-2015 from
  `https://agricultural-production-hotspots.ec.europa.eu/data/indicators_fpar/fpar/` and report the
  name.
- **A4. Parallel throughput:** 8 concurrent A1-type downloads (8 different days in 2015-06),
  `xargs -P 8`. Report aggregate MB/s.
- **A5. Windowed read:** time
  `gdal_translate -projwin -18 38 52 -35 /vsicurl/<A2 url> $TMP/af.tif`
  and report `gdal-config --version`.

#### B — resources (both nodes)

`nproc`; usable CPUs (on PASCAL from `cpu.max`); `free -g`; `uptime` (the load now); free space on the
bulk data root (`df -h`); R `packageVersion("terra")` and the GDAL that terra reports
(`terra::gdal()`).

#### C — PASCAL only: what is in the raw climate trees?

The `chirts_raw` and `chirps_raw` roots are in `metadata/hosts.json` (pascal profile). For each, report:
- the product and version, from file names, any readme and the tif metadata;
- **daily or monthly**;
- which variables (Tmax, Tmin, RH, other);
- the first and last date;
- file count and approximate size;
- the extent and resolution of one file (`gdalinfo`).

**The question that matters: is CHIRTS-ERA5 daily Tmax/Tmin present for 2005-2025, and is CHIRPS v3
daily present?**

#### Then

1. Delete `$TMP`.
2. Prepend `### RESPONSE — <node> 2026-10-xx` with the A/B (and C) tables.
3. Commit and push only this file.
4. Verify on `origin/develop` (`git log origin/develop..HEAD` empty).

**Stop and report** if any URL returns non-200, or `/vsicurl` fails, rather than working around it.
