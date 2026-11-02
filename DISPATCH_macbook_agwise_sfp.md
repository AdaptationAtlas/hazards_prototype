# DISPATCH — macbook — AgWise outputs for the SFP data hub

Append-only, newest block on top. Read `AGENTS.md` first, then
`HANDOVER_2026-11-02_agwise-sfp-inventory.md` (the full first-pass inventory this thread opens on).

---

### CGLABS → MACBOOK 2026-11-02 — first pass done, read-only. No AgWise tree on this node; two empty `agwise` stubs; three things need a decision off-node

**What was asked.** Identify AgWise (`github.com/CGIAR-AgWise`) outputs on CGlabs storage that
could feed the SFP data hub. First pass, high-level folder identification only, stop for folder
selection before drilling deeper.

**What was done.** Immediate children of candidate roots, bounded output, short timeouts. No
storage-wide recursive search, no recursive listing, no `du`, no file counts, no content scans.
Nothing downloaded, modified or published. The one name search that was needed was **reused, not
repeated** — the `-maxdepth 8` sweep in `DISPATCH_obs_find_local_copies.md` §F1 (2026-10-26)
already covered all four mount paths of the bulk share plus the home volume, rc 0 on all 95
top-level directories.

**Result, in one line.** AgWise has **no working tree on this node**. What exists is two
AgWise-authored download scripts at the `{common_data}` root whose target landing tree has never
existed here, two empty `agwise` directory stubs on the home volume, and a set of
AgWise-*adjacent* datasets owned by other projects. Full tables in the briefing; the three items
below are the ones that cannot be settled from this node.

#### Item 1 — find the real landing tree

`{common_data}/download_chirps_v3_rnl.R` and `download_chirts_era5_temperature.R` (2026-05-28/29,
headers both `Project : AgWise Data Sourcing / Agroclimate DataOps`, Alliance of Bioversity
International and CIAT) write to a **relative** path:

```
agwise-datasourcing/dataops/datasourcing/Data/Global_GeoData/Landing/
    Rainfall/chirps_v3/<year>.nc
    TemperatureMax/CHIRTS-ERA5/<year>.nc
```

No `chirps_v3` and no `CHIRTS-ERA5` directory exists under any searched root. Two empty, root-owned
stubs sit at `~/AgWise` (mtime 2022-11-09) and `~/agwise` (2023-01-10), on the home volume rather
than the bulk share, never written to since creation.

**Ask AgWise where that tree is mounted, naming those two stub paths.** Cheaper and more reliable
than any further search from here. The script author's name and CGIAR address are in the header on
the node — not quoted in the repo, which is public. If the answer is "it only ever ran on a
laptop", that closes the thread; if it is another CGIAR host, the hub needs a route to it, not a
copy here.

Worth flagging to them: both paths are relative, so a run from the wrong working directory would
scatter the output rather than fail. Nothing matching was found either way.

#### Item 2 — licence decisions before anything is listed

Two of the three best candidates are third-party redistributions, and neither can go in a hub
listing on this node's say-so:

- `{common_data}/isda/raw/` — iSDA Africa soil, 30 m, v0.13, 2001-2017, ~346 GB, eleven properties
  across three depth slices, plus `.vrt` mosaics in `intermediate/`. Gridded property rasters, **not**
  DSSAT profiles: no `.SOL`, no profile-keyed table. The ingredients an AgWise DSSAT workflow would
  build from, not its output.
- `{common_data}/era/` — ERA (Evidence for Resilient Agriculture), versioned parquet/RData releases
  (V1.0 2023-11-22, V1.1 2023-06-27) with a `MASTER CODES` sheet and site-joined covariates under
  `geodata/`. Its own terms, separate from AgWise's.

This is a provenance-and-licensing call, which is macbook-side work —
`provenance-licensing-reviewer` territory, not a node run.

#### Item 3 — settle what the hub actually wants

`era/` is ERA proper. **No `carob` directory exists at any inspected level**, so "ERA–Carob" on this
node resolves to the ERA half only. Decide whether the hub wants:

- the ERA holdings themselves (owner: ERA), or
- AgWise's *use* of ERA (owner: AgWise, and the data is wherever item 1 lands).

Different answers, different owners, different next step. Nothing further should be inspected until
this is picked.

#### What this node did NOT do, deliberately

No recursive or storage-wide search; no `du`; no file counts; no content scans; no downloads, no
modifications, no publication; no drilling below the first level of any candidate root. The climate
NetCDF holdings were **not** re-measured — `DISPATCH_obs_find_local_copies.md` §F2 already carries
products, versions, date ranges and completeness.

#### The one narrow inspection queued behind this

On a go-ahead: `{common_data}/era/` at depth 1 with `ls -l` for sizes and mtimes, plus **parquet
schemas only** — column names and types, no row scans — separating `data/data_entry/` and
`errors/` from the versioned releases. Bounded, read-only, minutes. Say the word and it runs.

#### Also open on this node, unrelated to AgWise

`DISPATCH_cglabs_issue54_intld_placeholders.md` still has **no RESPONSE** — the macbook block of
2026-10-23 (#54 + #52, parts A and B) has not been run here. Flagging it because it is the only
unanswered inbound dispatch at the repo root.
