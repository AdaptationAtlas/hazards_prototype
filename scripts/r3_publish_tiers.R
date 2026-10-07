#!/usr/bin/env Rscript
# scripts/r3_publish_tiers.R
# ==========================
# Publish the notebook-facing hazard_exposure parquets (all three severity tiers)
# and, optionally, the exposure reference parquet the notebook divides by.
# Generalises scripts/r3_publish_moderate_extreme.R (CR-091) to include `severe`
# and adds hard gates so a half-baked product cannot reach the live Atlas.
#
# WHAT THE NOTEBOOK READS (verified 2026-09-10 against atlas_notebooks):
#   s3://digital-atlas/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/
#     processing=hazard-risk-exposure/variable=vop_nominal-usd21/period=jagermeyr/
#     model=ENSEMBLEmean/severity={severe,moderate,extreme}/int=multi-hazard.parquet
#   s3://digital-atlas/domain=exposure/type=combined/source=glw4-2020_spam2020AA/
#     region=ssa/processing=atlas-harmonized/variable=crop-livestock_all.parquet
#   NOTE: R/s3_upload.R publishes a DIFFERENT product (source=atlas_cmip6/vop_intld15).
#   That legacy route is superseded by --variables vop_intld here and should be retired once the
#   new key is live (handover 2026-10-07 §2 A2).
#
# TIER VARIABLES (--variables, 2026-10-07). Until then this script could publish only
# vop_nominal-usd21; the intld, harvested-area, production-tonnes and head-count tiers were built
# on the node with no publish route at all. See VAR_SPECS below for the key scheme, each tier's
# local directory and each tier's G6/G6b basis. The default is still `vop_usd` alone, so every
# existing invocation and archived dispatch line keeps meaning what it meant.
#
# LOCAL SOURCES (working_dir-relative after 0_server_setup.R):
#   <VAR_SPECS[[v]]$dir_key>/<timeframe>/haz-freq-exp_<file_var>_<model>_int_adm_<tier>.parquet
#   <exposure_dir>/exposure_adm_sum_spam20-20_glw420-20_res-05.parquet     (--reference --res 0.05)
#
# GATES (per tier variable x severity; any FAIL aborts that tier, nothing uploaded):
#   G1 local file exists and is > 10 MB
#   G2 every hazard_vars has hazard='none' rows and n(none) == n(any)   (issue #9 ask)
#   G3 scenarios = historic + ssp126/245/370/585 (historic rows live INSIDE this file)
#   G4 severity column == tier
#   G5 column names identical to the live object (schema drift breaks notebook SQL)
#   G6 VALUE drift vs the live object (R/checks/r3_tier_drift_vs_live.R): admin0 historic
#      any+none totals per (iso3, crop), in populations - continental per-crop total,
#      material pairs (+ median), small/border-heavy countries in their own table,
#      livestock as a "must not move" control, zero flips, zero unmatched, row/scenario
#      parity. G1-G5 never look at a value; G6 is what notices a shifted product.
#      --allow-value-drift demotes G6 to WARN (like --allow-schema-drift for G5).
#   G6b PRODUCT vs THE EXPOSURE IT WAS BUILT FROM (R/checks/r3_tier_drift_vs_live.R::
#      tier_vs_exposure). G6 compares against whatever is live at the key, so a key published for
#      the FIRST time has no G6 at all - and this bake publishes four such keys. Because
#      freq_any + freq_none = 1 per pixel, a tier's admin0 historic any+none total per (iso3, crop)
#      IS the exposure total it was multiplied by, so the 0.4.4 table on the same zonal grid gates
#      it with no live object in sight. Populations split material / immaterial / unmatched, and a
#      product pair carrying value where the exposure has no row is a FAIL. REQUIRED by default:
#      no --drift-exposure basis for a tier aborts it. --no-exposure-gate opts out,
#      --allow-exposure-mismatch demotes to WARN, --exposure-tol sets the bound (default 0.02).
# Reference (--reference --res 0.05|0.25): columns + distinct(exposure/unit/stat) identical
#   to live, rows within 25 %. Published at both resolutions 2026-09-25 (#30); the unsuffixed
#   key is the deprecated alias of res-05.
#
# Convention: back up the live object to sandbox/backup/issue9_<STAMP>/... then
# upload with ACL="public-read" (download+upload, NEVER s3_file_copy - strips ACL).
#
# Usage:
#   Rscript scripts/r3_publish_tiers.R --dry-run                 # gates G1-G6b + backup plan, no writes
#   Rscript scripts/r3_publish_tiers.R                           # vop_usd, severe+moderate+extreme
#   Rscript scripts/r3_publish_tiers.R --tiers severe            # subset
#
# The no-regrets publish order of handover 2026-10-07 §2D, one step per command, each independently
# gated so a failure stops one key and not the sequence. Give every step its own --drift-exposure
# basis: the vop tiers take the 0.4.4 §3.2 / §3.3 single-unit twin, the physical tiers the
# multi-unit combined reference, both on the res-25 grid the bake ran on.
#   1. --variables vop_usd   --drift-exposure vop_usd=<vop_nominal-usd-2021_adm_sum_..._res-25>
#   2. --variables vop_intld --drift-exposure vop_intld=<vop_intld15-2021_adm_sum_..._res-25>
#   3. --variables ha        --drift-exposure ha=<exposure_adm_sum_..._res-25>
#   4. --variables prod_t    --drift-exposure prod_t=<exposure_adm_sum_..._res-25>
#      --variables head_n    --drift-exposure head_n=<exposure_adm_sum_..._res-25>
#   5. repeat 1-4 with --timeframe annual
#   6. --model ENSEMBLE (carries value_sd) and --model <GCM>, last, after Brayden confirms the
#      notebook read contract - this is the only step that widens it.
#   Rscript scripts/r3_publish_tiers.R --reference               # tiers + exposure reference
#   Rscript scripts/r3_publish_tiers.R --reference-only
#   Rscript scripts/r3_publish_tiers.R --sidecar-only            # ship .json only, parquet untouched
#   Rscript scripts/r3_publish_tiers.R --reference-only --res 0.05 --allow-unit-vintage-change   # -> ..._res-05.parquet + legacy alias
#   Rscript scripts/r3_publish_tiers.R --reference-only --res 0.25 --allow-unit-vintage-change --allow-res-change   # -> ..._res-25.parquet (first publish gates vs legacy)
#                                                                # intld15 -> intld15-2021 migration (#30)
#   Rscript scripts/r3_publish_tiers.R --family-only --res 0.05 --allow-unit-vintage-change      # vop_nominal-usd-2021 + vop_intld15-2021 -> _res-05 + alias
#   Rscript scripts/r3_publish_tiers.R --family-only --res 0.25 --allow-unit-vintage-change --allow-res-change   # -> _res-25 (first publish gates vs legacy)
#   flags: --timeframe jagermeyr (default) | --allow-schema-drift (G5 -> warn) | --skip-gates
#          --variables vop_usd,vop_intld,ha,prod_t,head_n | --model ENSEMBLEmean|ENSEMBLE|<GCM>
#          --no-exposure-gate | --allow-exposure-mismatch | --exposure-tol 0.02
#          --allow-value-drift (G6 -> warn) | --drift-tol-pair 0.25 | --drift-tol-median 0.03
#          --drift-tol-total 0.05 | --drift-tol-small 0.5 | --drift-tol-control 0.02
#          --drift-min-live 1e6 | --drift-small-iso3 GMB,SWZ,...   (see the check's header for why)

t0 <- Sys.time()
.ts  <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.log <- function(fmt, ...) cat(sprintf("[%s] [publish-tiers] %s\n", .ts(), sprintf(fmt, ...)))
.elapsed <- function(from) sprintf("%.1f min", as.numeric(difftime(Sys.time(), from, units = "mins")))

args <- commandArgs(trailingOnly = TRUE)
flag <- function(x) x %in% args
opt  <- function(x, default) { i <- match(x, args); if (is.na(i) || i == length(args)) default else args[i + 1] }
DRY_RUN     <- flag("--dry-run")
SKIP_GATES  <- flag("--skip-gates")
ALLOW_DRIFT <- flag("--allow-schema-drift")
# G6 (p.steward 2026-09-26, #30 follow-up): the crop nominal-USD raster moved from
# 0.05 deg (aligned in-flight) to native 0.25 deg, so the usd product shifts by
# border-cell price assignment. That shift is expected and small; a wrong raster,
# a stale tif that survived parking, or livestock moving is not. Bound it
# explicitly rather than reach for --skip-gates. Defaults: continental per-crop
# within 5 %, material pairs within 25 % with median within 3 %, small countries
# in their own table (50 %), livestock control within 2 %.
ALLOW_VALUE_DRIFT <- flag("--allow-value-drift")
DRIFT <- list(tol_pair    = as.numeric(opt("--drift-tol-pair", "0.25")),
              tol_median  = as.numeric(opt("--drift-tol-median", "0.03")),
              tol_total   = as.numeric(opt("--drift-tol-total", "0.05")),
              tol_small   = as.numeric(opt("--drift-tol-small", "0.5")),
              tol_control = as.numeric(opt("--drift-tol-control", "0.02")),
              min_live    = as.numeric(opt("--drift-min-live", "1e6")),
              small_iso3  = opt("--drift-small-iso3", ""),
              # csv iso3,crop,ratio_expected: how much each pair's EXPOSURE input moved between
              # what live was built from and what this bake used (R/3 is a linear multiply, so
              # the product must move by the same factor). Made on the node by
              # R/checks/r3_expected_drift_from_exposure.R. Absent = every pair expects 1.
              expected    = opt("--drift-expected", ""),
              # PREFERRED over --drift-expected: the 0.4.4 §3.2 twin this bake multiplied by, on the
              # same zonal grid (vop_nominal-usd-2021_adm_sum_spam20_glw420_res-25.parquet). Because
              # freq_any + freq_none = 1 per pixel, product any+none must equal it pair by pair.
              exposure    = opt("--drift-exposure", ""),
              # "ISO3:crop,..." live -> ~0 flips that are explained (e.g. an island with no 0.25 deg
              # cell centre); reported, not FAIL.
              allow_flips = opt("--drift-allow-flips", ""))
# Ship only the `.json` sidecar for each tier, leaving the live parquet untouched.
# The #26 membership stamp can be applied to sidecars on disk in seconds, so it
# should not cost three ~190 MB re-uploads of byte-identical parquets, nor put a
# known-good live object through an overwrite for no change.
SIDECAR_ONLY <- flag("--sidecar-only")
# Issue #30 / p.steward 2026-09-18: the reference's `unit` values move from the
# vintage-less labels (`intld15`, `usd`) to the vintages the producers actually
# emit (`intld15-2021`, `nominal-usd-2021`). That is an INTENDED change and the
# distinct(unit) gate below will rightly fail on it, so it needs its own flag
# rather than --skip-gates, which would wave through the schema and row checks
# too. The flag still refuses to lose or invent a unit: cardinality must match,
# because a vanishing unit is exactly how #30 stayed hidden.
ALLOW_UNIT_VINTAGE <- flag("--allow-unit-vintage-change")
DO_REF      <- flag("--reference") || flag("--reference-only")
# p.steward 2026-09-23 (issue #30): the reference exists at two zonal resolutions,
# res-05 (Atlas exposure grid, what the live object was built on) and res-25
# (NEX-GDDP hazard grid, what R/3 is on). --res is REQUIRED with --reference and
# selects both the local file and the S3 key: each resolution publishes to its
# own key, variable=crop-livestock_all_<tag>.parquet. The legacy unsuffixed key
# is kept as a deprecated alias of res-05 and is rewritten only on a res-05
# publish. The first publish of a suffixed key has no live twin to gate against,
# so the gates fall back to the legacy key as baseline; when that baseline is a
# different resolution the row-count gate cannot be meaningful and needs
# --allow-res-change to become informational. Columns and distinct() still gate.
REF_RES     <- opt("--res", "")
ALLOW_RES_CHANGE <- flag("--allow-res-change")
# --family (p.steward 2026-09-26, #30 follow-up): the two single-unit twins 0.4.4
# §3.2 / §3.3 write next to the reference, vop_nominal-usd-2021 and
# vop_intld15-2021. Their S3 keys have been live since 2025-11-03 with no producer
# in this repo (unit still `usd` / `intld15`), and the KE-ENSO ROI notebook reads
# vop_nominal-usd-2021 directly. They go through the SAME code path and gates as
# the reference: res-tagged key per resolution, unsuffixed key = deprecated alias
# of res-05, --allow-unit-vintage-change for the label move, --allow-res-change on
# the first res-25 publish. The third family key, vop_nominal-usd-2015, has no
# producer at all and is retired by hand (dispatch), not by this script.
DO_FAMILY   <- flag("--family") || flag("--family-only")
FAMILY      <- c("vop_nominal-usd-2021", "vop_intld15-2021")
if ((DO_REF || DO_FAMILY) && !REF_RES %in% c("0.05", "0.25")) stop("--reference / --family need --res 0.05 | 0.25 (the table and its S3 key are named for the zonal grid)")
REF_RES_TAG <- if (nzchar(REF_RES)) sprintf("res-%02d", round(as.numeric(REF_RES) * 100)) else ""
DO_TIERS    <- !(flag("--reference-only") || flag("--family-only"))
TF          <- opt("--timeframe", "jagermeyr")
TIERS       <- strsplit(opt("--tiers", "severe,moderate,extreme"), ",")[[1]]
# --model: which R/3 §4.2 aggregate to publish. The local filename carries it too, so one option
# selects both sides. ENSEMBLEmean is what has always been published; ENSEMBLE is the merged object
# that still has `value_sd` (handover §2D item E), and a bare GCM name publishes that member.
MODEL       <- opt("--model", "ENSEMBLEmean")
STAMP       <- format(Sys.time(), "%Y%m%d_%H%M%S")

# ---------------------------------------------------------------- variables --
# One row per hazard-exposure tier R/3 §4.2 produces. Until 2026-10-07 this script could publish
# only vop_nominal-usd21; the other tiers were built on the node and had no route at all, and the
# constant-dollar one had only the legacy `source=atlas_cmip6/vop_intld15` tree that R/s3_upload.R
# writes (2025-06/07, pre-currency-fix). Fields:
#   dir_key    atlas_dirs$data_dir key holding <timeframe>/ for this tier
#   file_var   R/3's `variable`, which is also the token in the local parquet's name
#   s3_var     the `variable=` segment of the key (see the key scheme above)
#   twin       which 0.4.4 table is G6's basis: "family" = the single-unit twin written by §3.2/§3.3,
#              "reference" = the multi-unit combined reference
#   twin_expo  the `exposure` value to filter that basis on. 0.4.4 derives it as field 2 of the
#              raster's name, so spam_prod_t_all -> "prod", spam_harv-area_ha_all -> "harv-area",
#              livestock_number_number -> "number", and both vop rasters -> "vop" (the vop tiers are
#              told apart by the twin FILE, which is already per-unit).
VAR_SPECS <- list(
  vop_usd   = list(dir_key = "hazard_risk_vop_usd", file_var = "vop_nominal-usd-2021", s3_var = "vop_nominal-usd21", twin = "family",    twin_file = "vop_nominal-usd-2021", twin_expo = "vop"),
  vop_intld = list(dir_key = "hazard_risk_vop",     file_var = "vop_intld15-2021",     s3_var = "vop_intld15-21",    twin = "family",    twin_file = "vop_intld15-2021",     twin_expo = "vop"),
  ha        = list(dir_key = "hazard_risk_ha",      file_var = "harv-area_ha",         s3_var = "harv-area_ha",      twin = "reference", twin_file = NA,                     twin_expo = "harv-area"),
  prod_t    = list(dir_key = "hazard_risk_prod",    file_var = "prod_t",               s3_var = "prod_t",            twin = "reference", twin_file = NA,                     twin_expo = "prod"),
  head_n    = list(dir_key = "hazard_risk_n",       file_var = "head_n",               s3_var = "head_n",            twin = "reference", twin_file = NA,                     twin_expo = "number")
)
# Default stays the single tier this script has always published, so every existing invocation and
# every archived dispatch line keeps meaning what it meant. New tiers are named explicitly - which
# is also what the no-regrets publish order of handover §2D asks for, one tier at a time.
VARIABLES <- strsplit(opt("--variables", "vop_usd"), ",")[[1]]
if (DO_TIERS) {
  .unknown <- setdiff(VARIABLES, names(VAR_SPECS))
  if (length(.unknown)) stop("--variables: unknown tier(s) [", paste(.unknown, collapse = ","), "]; known: ", paste(names(VAR_SPECS), collapse = ", "))
}

setup <- if (file.exists("R/0_server_setup.R")) "R/0_server_setup.R" else
  file.path(Sys.getenv("project_dir"), "R", "0_server_setup.R")
if (nzchar(Sys.getenv("ATLAS_SETUP_SKIP"))) {
  .log("ATLAS_SETUP_SKIP set - not sourcing setup; expecting atlas_dirs (+ exposure_dir) in the calling env")
  stopifnot(exists("atlas_dirs"))
} else {
  .log("sourcing %s", setup)
  suppressMessages(suppressWarnings(source(setup)))
}
suppressPackageStartupMessages({ pacman::p_load(s3fs, arrow, dplyr, data.table, jsonlite) })
# G6 lives in its own file so it can be run standalone on the node (new vs parked
# tier, e.g. the unpublished intld / ha tiers) and unit-tested off-node on a fixture.
.drift_src <- if (exists("project_dir")) file.path(project_dir, "R", "checks", "r3_tier_drift_vs_live.R") else "R/checks/r3_tier_drift_vs_live.R"
if (!file.exists(.drift_src)) stop("G6 needs ", .drift_src)
source(.drift_src)
if (!nzchar(DRIFT$small_iso3)) DRIFT$small_iso3 <- DRIFT_SMALL_ISO3_DEFAULT else DRIFT$small_iso3 <- strsplit(DRIFT$small_iso3, ",")[[1]]
DRIFT_EXPECTED <- NULL
if (nzchar(DRIFT$expected)) {
  if (!file.exists(DRIFT$expected)) stop("--drift-expected file not found: ", DRIFT$expected)
  DRIFT_EXPECTED <- data.table::fread(DRIFT$expected)
  stopifnot(all(c("iso3", "crop", "ratio_expected") %in% names(DRIFT_EXPECTED)))
  .log("G6 expected input moves: %s (%d pairs, %d with ratio != 1)", DRIFT$expected, nrow(DRIFT_EXPECTED), sum(abs(DRIFT_EXPECTED$ratio_expected - 1) > 1e-9))
} else .log("G6 expected input moves: none supplied (every pair expects 1)")
`%||%` <- function(a, b) if (is.null(a) || (length(a) == 1 && is.na(a))) b else a
# --drift-exposure takes either one path (when a single tier is being published) or `tier=path`
# pairs, because each tier has its OWN 0.4.4 basis: the vop tiers use the single-unit §3.2 / §3.3
# twins, the physical tiers the multi-unit combined reference. One path shared across tiers would
# judge prod_t against a vop table.
DRIFT_EXPOSURE_BY_VAR <- list()
if (nzchar(DRIFT$exposure)) {
  .parts <- strsplit(DRIFT$exposure, ",")[[1]]
  if (!any(grepl("=", .parts, fixed = TRUE))) {
    if (length(VARIABLES) != 1L) stop("--drift-exposure was given one bare path but ", length(VARIABLES),
                                      " tiers are selected. Each tier has its own 0.4.4 basis, so name them: --drift-exposure ",
                                      paste(sprintf("%s=<path>", VARIABLES), collapse = ","))
    DRIFT_EXPOSURE_BY_VAR[[VARIABLES[1]]] <- DRIFT$exposure
  } else for (.p in .parts) {
    .kv <- strsplit(.p, "=", fixed = TRUE)[[1]]
    if (length(.kv) != 2L) stop("--drift-exposure: expected tier=path, got '", .p, "'")
    DRIFT_EXPOSURE_BY_VAR[[trimws(.kv[1])]] <- trimws(.kv[2])
  }
  for (.k in names(DRIFT_EXPOSURE_BY_VAR)) {
    if (!file.exists(DRIFT_EXPOSURE_BY_VAR[[.k]])) stop("--drift-exposure file not found for ", .k, ": ", DRIFT_EXPOSURE_BY_VAR[[.k]])
    .log("G6/G6b basis for %s: %s (preferred; overrides --drift-expected)", .k, DRIFT_EXPOSURE_BY_VAR[[.k]])
  }
}
DRIFT_FLIPS <- if (nzchar(DRIFT$allow_flips)) strsplit(DRIFT$allow_flips, ",")[[1]] else character(0)
if (length(DRIFT_FLIPS)) .log("G6 flips allowed by name: %s", paste(DRIFT_FLIPS, collapse = ","))
# G6b bounds. Tighter than G6's, because this is an identity and not a drift: any + none = the
# exposure total, exactly, up to R/3's rounding and the admin0-vs-admin2 zonal split of #18.
G6B_TOL      <- as.numeric(opt("--exposure-tol", "0.02"))
ALLOW_G6B    <- flag("--allow-exposure-mismatch")
REQUIRE_G6B  <- !flag("--no-exposure-gate")

BUCKET  <- "digital-atlas"
# ---------------------------------------------------------------- key scheme --
# Settled 2026-10-07 (handover §2D) BEFORE the first new key was written, because the layout has to
# carry every tier this bake produces and the bucket is versioned but `s3fs` deletes are permanent,
# so a layout changed halfway leaves stale keys that are awkward to retire:
#
#   domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/
#     variable=<variable>/period=<period>/model=<model>/severity=<tier>/int=multi-hazard.parquet
#
#   variable  vop_nominal-usd21 | vop_intld15-21 | harv-area_ha | prod_t | head_n
#   period    jagermeyr | annual                (--timeframe; `annual` has never been published)
#   model     ENSEMBLEmean | ENSEMBLE | <GCM>   (--model; value_sd exists only in the merged
#                                                ENSEMBLE, so publishing that IS handover §2D item E)
#
# `vop_nominal-usd21` is the one irregular spelling - it has been live and read by the notebooks
# since 2026-09, so it stays as it is rather than being regularised into a second key for the same
# product. Every other variable is spelled exactly as R/3 names it.
S3_KEY_BASE <- "domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure"
s3_base_for <- function(spec, period, model) sprintf("%s/variable=%s/period=%s/model=%s", S3_KEY_BASE, spec$s3_var, period, model)
REF_KEY_LEGACY <- paste0("domain=exposure/type=combined/source=glw4-2020_spam2020AA/region=ssa/",
                         "processing=atlas-harmonized/variable=crop-livestock_all.parquet")   # deprecated alias of res-05
# variable=<name>.parquet -> variable=<name>_<res-tag>.parquet (no tag: the legacy key itself)
res_key <- function(key_legacy) if (nzchar(REF_RES_TAG)) sub("\\.parquet$", paste0("_", REF_RES_TAG, ".parquet"), key_legacy) else key_legacy
REF_KEY <- res_key(REF_KEY_LEGACY)
local_ref_dir  <- if (exists("exposure_dir")) exposure_dir else atlas_dirs$data_dir$exposure
local_ref      <- file.path(local_ref_dir, sprintf("exposure_adm_sum_spam20-20_glw420-20%s.parquet", if (nzchar(REF_RES_TAG)) paste0("_", REF_RES_TAG) else ""))
family_local   <- function(name) file.path(local_ref_dir, sprintf("%s_adm_sum_spam20_glw420_%s.parquet", name, REF_RES_TAG))   # 0.4.4 §3.2 / §3.3
family_key_legacy <- function(name) sub("crop-livestock_all", name, REF_KEY_LEGACY, fixed = TRUE)

cat(sprintf("\n=== publish: variables=%s | tiers=%s | period=%s | model=%s | reference=%s | family=%s | %s ===\n",
            if (DO_TIERS) paste(VARIABLES, collapse = ",") else "-",
            paste(TIERS, collapse = ","), TF, MODEL, DO_REF, DO_FAMILY, if (DRY_RUN) "[DRY RUN]" else "LIVE WRITE"))
if (DO_TIERS) for (.v in VARIABLES) .log("local tier dir %s = %s", .v, file.path(atlas_dirs$data_dir[[VAR_SPECS[[.v]]$dir_key]], TF))
.log("local reference = %s", local_ref)
if (DO_FAMILY) for (f in FAMILY) .log("local family %s = %s", f, family_local(f))

s3_exists <- function(url) !is.null(tryCatch(suppressWarnings(s3fs::s3_file_info(url)), error = function(e) NULL))
download_live <- function(url) {
  tmp <- tempfile(fileext = ".parquet"); t1 <- Sys.time()
  s3fs::s3_file_download(url, tmp); .log("  downloaded live copy (%.0f MB) in %s", file.size(tmp) / 1e6, .elapsed(t1)); tmp
}
backup_then_upload <- function(local_f, s3_key, label) {
  s3_url  <- sprintf("s3://%s/%s", BUCKET, s3_key)
  bak_url <- sprintf("s3://%s/sandbox/backup/issue9_%s/%s", BUCKET, STAMP, s3_key)
  live_tmp <- NULL
  if (s3_exists(s3_url)) {
    .log("  live object exists -> backup %s", bak_url)
    live_tmp <- download_live(s3_url)
    if (!DRY_RUN) { s3fs::s3_file_upload(live_tmp, bak_url, ACL = "public-read", overwrite = TRUE); .log("  backup written") }
    else .log("  [dry run] backup upload skipped")
  } else .log("  no live object at %s (first publish)", s3_url)
  list(s3_url = s3_url, live_tmp = live_tmp)
}
finish_upload <- function(local_f, s3_url) {
  if (DRY_RUN) { .log("  [dry run] would upload %s (%.0f MB) -> %s", basename(local_f), file.size(local_f) / 1e6, s3_url); return(invisible(TRUE)) }
  t1 <- Sys.time()
  s3fs::s3_file_upload(local_f, s3_url, ACL = "public-read", overwrite = TRUE)
  info <- s3fs::s3_file_info(s3_url)
  ok <- isTRUE(as.numeric(info$size) == file.size(local_f))
  .log("  uploaded in %s: remote %s bytes vs local %s bytes -> %s", .elapsed(t1), info$size, file.size(local_f), if (ok) "SIZE MATCH" else "SIZE MISMATCH")
  https <- sub("^s3://([^/]+)/", "https://\\1.s3.amazonaws.com/", s3_url)
  rc <- tryCatch(system2("curl", c("-s", "-o", "/dev/null", "-w", "%{http_code}", "-H", "'Range: bytes=0-0'", shQuote(https)), stdout = TRUE), error = function(e) NA)
  .log("  HTTPS range probe %s -> HTTP %s (expect 206)", https, paste(rc, collapse = ""))
  if (!ok) stop("size mismatch after upload: ", s3_url)
  invisible(ok)
}
schema_of <- function(f) sort(names(arrow::open_dataset(f)$schema))

## ---------------------------------------------------------------- tiers ----
if (DO_TIERS) for (vkey in VARIABLES) {
 spec <- VAR_SPECS[[vkey]]
 local_tier_dir <- file.path(atlas_dirs$data_dir[[spec$dir_key]], TF)
 S3_BASE <- s3_base_for(spec, TF, MODEL)
 cat(sprintf("\n=== TIER VARIABLE %s | local %s | key variable=%s period=%s model=%s ===\n",
             vkey, local_tier_dir, spec$s3_var, TF, MODEL))
 .log("G6 basis for %s: %s", vkey, if (nzchar(DRIFT_EXPOSURE_BY_VAR[[vkey]] %||% "")) DRIFT_EXPOSURE_BY_VAR[[vkey]] else "none supplied — G6 falls back to the live product, and G6b cannot run")
 for (tier in TIERS) {
  t_tier <- Sys.time()
  cat(sprintf("\n--- %s / %s ---\n", vkey, toupper(tier)))
  local_f <- file.path(local_tier_dir, sprintf("haz-freq-exp_%s_%s_int_adm_%s.parquet", spec$file_var, MODEL, tier))
  s3_key  <- sprintf("%s/severity=%s/int=multi-hazard.parquet", S3_BASE, tier)

  if (SIDECAR_ONLY) {
    sc_local <- paste0(local_f, ".json")
    if (!file.exists(sc_local)) { .log("  SIDECAR-ONLY FAIL: missing %s (run scripts/stamp_ensemble_membership.R first)", sc_local); next }
    em <- tryCatch(jsonlite::read_json(sc_local, simplifyVector = TRUE)$ensemble, error = function(e) NULL)
    if (is.null(em)) { .log("  SIDECAR-ONLY FAIL: %s has no `ensemble` block - nothing worth publishing", basename(sc_local)); next }
    .log("  sidecar: ensemble = %d GCMs [%s]", em$n_members, paste(em$members, collapse = ","))
    if (!identical(as.integer(em$n_members), 18L)) {
      .log("  SIDECAR-ONLY FAIL: %d members, not 18 - refusing to publish a partial-ensemble claim (#26)", em$n_members); next
    }
    finish_upload(sc_local, sprintf("s3://%s/%s.json", BUCKET, s3_key))
    .log("  %s / %s sidecar done in %s", vkey, tier, .elapsed(t_tier))
    next
  }

  # G1
  if (!file.exists(local_f)) { .log("  G1 FAIL: missing %s (has R/3 §4.2 run for %s?)", local_f, TF); next }
  if (file.size(local_f) < 10e6) { .log("  G1 FAIL: %s is only %.1f MB", basename(local_f), file.size(local_f) / 1e6); next }
  .log("  G1 ok: %s (%.0f MB, mtime %s)", basename(local_f), file.size(local_f) / 1e6, format(file.mtime(local_f), "%Y-%m-%d %H:%M"))
  ds <- arrow::open_dataset(local_f)

  if (!SKIP_GATES) {
    # G2 none coverage
    cnt <- ds |> dplyr::count(hazard_vars, hazard) |> dplyr::collect() |> as.data.table()
    wide <- dcast(cnt, hazard_vars ~ hazard, value.var = "n", fill = 0L)
    print(wide, nrows = 50)
    g2 <- all(c("none", "any") %in% names(wide)) && all(wide$none > 0) && all(wide$none == wide$any)
    .log("  G2 %s: every hazard_vars has none rows and n(none)==n(any)", if (g2) "ok" else "FAIL")
    # G3 scenarios
    sc <- ds |> dplyr::count(scenario) |> dplyr::collect() |> as.data.table()
    g3 <- setequal(sc$scenario, c("historic", "ssp126", "ssp245", "ssp370", "ssp585"))
    .log("  G3 %s: scenarios = %s", if (g3) "ok" else "FAIL", paste(sort(sc$scenario), collapse = ","))
    # G4 severity
    sv <- ds |> dplyr::distinct(severity) |> dplyr::collect()
    g4 <- identical(sort(sv$severity), tier)
    .log("  G4 %s: severity column = %s", if (g4) "ok" else "FAIL", paste(sv$severity, collapse = ","))
    if (!(g2 && g3 && g4)) { .log("  ABORT %s/%s: gate failure, nothing uploaded", vkey, tier); next }

    # G6b product vs the exposure input it was built from. Runs BEFORE the upload and without
    # needing a live object, which is the whole point: G6 compares against whatever is at the key,
    # so a key published for the first time - intld, ha, prod_t, head_n in this bake - gets no
    # value gate from G6 at all. Skipping it needs --no-exposure-gate, said out loud.
    .expo <- DRIFT_EXPOSURE_BY_VAR[[vkey]] %||% ""
    if (nzchar(.expo)) {
      t6b <- Sys.time()
      g6b <- tier_vs_exposure(local_f, .expo, spec$twin_expo, tol_pair = G6B_TOL)
      print_tier_vs_exposure(g6b, label = sprintf("%s/%s", vkey, tier), log = function(fmt, ...) .log(paste0("  ", fmt), ...))
      .log("  G6b %s in %s", if (g6b$pass) "ok" else if (ALLOW_G6B) "WARN (allowed)" else "FAIL", .elapsed(t6b))
      if (!g6b$pass && !ALLOW_G6B) { .log("  ABORT %s/%s: the product does not reproduce the exposure it was multiplied by (--allow-exposure-mismatch only once the cause is understood)", vkey, tier); next }
    } else if (REQUIRE_G6B) {
      .log("  G6b FAIL: no --drift-exposure basis for %s, so the product cannot be checked against its own input. Pass --drift-exposure %s=<0.4.4 %s table on this grid>, or --no-exposure-gate to publish without it.",
           vkey, vkey, if (identical(spec$twin, "family")) sprintf("%s twin", spec$twin_file) else "combined reference")
      .log("  ABORT %s/%s: no exposure basis", vkey, tier); next
    } else {
      .log("  G6b SKIPPED by --no-exposure-gate for %s", vkey)
    }
  }

  bu <- backup_then_upload(local_f, s3_key, tier)
  if (!SKIP_GATES && !is.null(bu$live_tmp)) {
    # G5 schema vs live
    live_cols <- schema_of(bu$live_tmp); loc_cols <- schema_of(local_f)
    if (identical(live_cols, loc_cols)) .log("  G5 ok: %d columns identical to live", length(loc_cols))
    else {
      .log("  G5 %s: local-only = [%s] live-only = [%s]", if (ALLOW_DRIFT) "WARN (allowed)" else "FAIL",
           paste(setdiff(loc_cols, live_cols), collapse = ","), paste(setdiff(live_cols, loc_cols), collapse = ","))
      if (!ALLOW_DRIFT) { .log("  ABORT %s/%s: schema drift (use --allow-schema-drift after checking the notebook SQL)", vkey, tier); next }
    }
    live_n <- arrow::open_dataset(bu$live_tmp) |> dplyr::count(hazard_vars, hazard) |> dplyr::collect()
    .log("  info: live has none rows? %s | live hazard_vars = %s", "none" %in% live_n$hazard,
         paste(sort(unique(live_n$hazard_vars)), collapse = ", "))
    # G6 value drift vs live (populations + bounds; see R/checks/r3_tier_drift_vs_live.R)
    t6 <- Sys.time()
    g6 <- tier_drift(local_f, bu$live_tmp,
                     tol_pair = DRIFT$tol_pair, tol_median = DRIFT$tol_median, tol_total = DRIFT$tol_total,
                     tol_small = DRIFT$tol_small, tol_control = DRIFT$tol_control,
                     min_live = DRIFT$min_live, small_iso3 = DRIFT$small_iso3, expected = DRIFT_EXPECTED,
                     exposure_new = if (nzchar(DRIFT_EXPOSURE_BY_VAR[[vkey]] %||% "")) DRIFT_EXPOSURE_BY_VAR[[vkey]] else NULL,
                     flip_allow = DRIFT_FLIPS, exposure_var = spec$twin_expo)
    print_drift(g6, label = sprintf("%s/%s", vkey, tier), log = function(fmt, ...) .log(paste0("  ", fmt), ...))
    .log("  G6 %s in %s", if (g6$pass) "ok" else if (ALLOW_VALUE_DRIFT) "WARN (allowed)" else "FAIL", .elapsed(t6))
    if (!g6$pass && !ALLOW_VALUE_DRIFT) { .log("  ABORT %s/%s: value drift outside the stated bounds (read the populations above; --allow-value-drift only after the cause is understood)", vkey, tier); next }
  }
  finish_upload(local_f, bu$s3_url)

  # Issue #26 ask 4: ensemble membership exists nowhere in the parquet's columns,
  # so for this product the sidecar is not an extra - it is the only place a
  # consumer can see whether they are reading an 18-member or a 5-member
  # ensemble. Ship it with the tier. Missing or unstamped is reported loudly but
  # does not abort: the parquet itself is unaffected and already verified above.
  sc_local <- paste0(local_f, ".json")
  if (file.exists(sc_local)) {
    em <- tryCatch(jsonlite::read_json(sc_local, simplifyVector = TRUE)$ensemble,
                   error = function(e) NULL)
    if (is.null(em)) {
      .log("  sidecar WARN: %s has no `ensemble` block - R/3 predates the #26 stamp, membership unrecorded",
           basename(sc_local))
    } else {
      .log("  sidecar: ensemble = %d GCMs [%s]", em$n_members, paste(em$members, collapse = ","))
      if (!identical(as.integer(em$n_members), 18L)) {
        .log("  sidecar WARN: %d members, not 18 - published product is a partial ensemble (#26)", em$n_members)
      }
      # Membership must not change silently between publishes either.
      live_sc <- paste0(bu$s3_url, ".json")
      if (s3_exists(live_sc)) {
        tmp_sc <- tempfile(fileext = ".json"); s3fs::s3_file_download(live_sc, tmp_sc)
        em_live <- tryCatch(jsonlite::read_json(tmp_sc, simplifyVector = TRUE)$ensemble, error = function(e) NULL)
        if (is.null(em_live)) .log("  sidecar info: live sidecar has no `ensemble` block")
        else if (setequal(em_live$members, em$members)) .log("  sidecar ok: membership identical to live (%d GCMs)", length(em$members))
        else .log("  sidecar WARN: membership differs from live - gone [%s] new [%s]",
                  paste(setdiff(em_live$members, em$members), collapse = ","), paste(setdiff(em$members, em_live$members), collapse = ","))
      }
    }
    finish_upload(sc_local, paste0(bu$s3_url, ".json"))
  } else {
    .log("  sidecar MISSING (%s) - published parquet records no ensemble membership", basename(sc_local))
  }
  .log("  %s / %s done in %s", vkey, tier, .elapsed(t_tier))
 }
}

## ------------------------------------------------ reference + family ----
# One code path for every table under the type=combined prefix: the canonical
# multi-unit reference (crop-livestock_all) and the --family single-unit twins.
# Gates: columns identical to live; distinct(exposure/unit/stat) identical (unit
# may change vintage 1:1 with --allow-unit-vintage-change); rows within 25 % of
# live, or informational with --allow-res-change when the only baseline is the
# legacy 0.05 deg key. A res-05 publish also rewrites the unsuffixed alias.
publish_table <- function(local_f, key_legacy, label) {
  key <- res_key(key_legacy)
  cat(sprintf("\n--- %s (%s) ---\n", toupper(label), REF_RES_TAG))
  .log("  target key = %s", key)
  if (!file.exists(local_f)) { .log("  FAIL: missing %s (has 0.4.4 run with EXPOSURE_RES=%s?)", local_f, REF_RES); return(invisible(FALSE)) }
  .log("  local %s (%.0f MB, mtime %s)", basename(local_f), file.size(local_f) / 1e6, format(file.mtime(local_f), "%Y-%m-%d %H:%M"))
  bu <- backup_then_upload(local_f, key, label)
  baseline_is_other_res <- FALSE
  if (is.null(bu$live_tmp) && s3_exists(sprintf("s3://%s/%s", BUCKET, key_legacy))) {
    .log("  first publish of %s: gating against the legacy unsuffixed key as baseline", REF_RES_TAG)
    bu$live_tmp <- download_live(sprintf("s3://%s/%s", BUCKET, key_legacy))
    baseline_is_other_res <- REF_RES != "0.05"   # every legacy object under this prefix is a 0.05 deg table
  }
  ok <- TRUE
  if (!SKIP_GATES) {
    if (is.null(bu$live_tmp)) { .log("  FAIL: no live %s to compare against - refusing to publish blind", label); ok <- FALSE }
    else {
      lc <- schema_of(bu$live_tmp); oc <- schema_of(local_f)
      if (!identical(lc, oc)) { .log("  FAIL columns: local-only=[%s] live-only=[%s]", paste(setdiff(oc, lc), collapse = ","), paste(setdiff(lc, oc), collapse = ",")); ok <- FALSE }
      else .log("  ok: %d columns identical", length(oc))
      dl <- arrow::open_dataset(bu$live_tmp); dr <- arrow::open_dataset(local_f)
      for (col in c("exposure", "unit", "stat")) if (col %in% oc) {
        a <- sort((dl |> dplyr::distinct(!!rlang::sym(col)) |> dplyr::collect())[[col]])
        b <- sort((dr |> dplyr::distinct(!!rlang::sym(col)) |> dplyr::collect())[[col]])
        if (setequal(a, b)) .log("  ok: distinct(%s) identical = %s", col, paste(a, collapse = ","))
        else if (col == "unit" && ALLOW_UNIT_VINTAGE && length(a) == length(b)) {
          .log("  unit vintage change ALLOWED (--allow-unit-vintage-change), %d -> %d units:", length(a), length(b))
          .log("    live  = [%s]", paste(a, collapse = ","))
          .log("    local = [%s]", paste(b, collapse = ","))
          .log("    gone  = [%s]  new = [%s]", paste(setdiff(a, b), collapse = ","), paste(setdiff(b, a), collapse = ","))
        }
        else {
          .log("  FAIL distinct(%s): live=[%s] local=[%s]", col, paste(a, collapse = ","), paste(b, collapse = ","))
          if (col == "unit" && ALLOW_UNIT_VINTAGE) {
            .log("    --allow-unit-vintage-change given but cardinality differs (%d live vs %d local) - a unit is being lost or invented, which is the #30 failure mode. Refusing.",
                 length(a), length(b))
          }
          ok <- FALSE
        }
      }
      nl <- nrow(dl); nr <- nrow(dr)
      if (abs(nr - nl) / nl <= 0.25) .log("  ok: rows local %d vs live %d%s", nr, nl, if (nr == nl) " (identical)" else "")
      else if (baseline_is_other_res && ALLOW_RES_CHANGE) {
        .log("  rows local %d vs baseline %d (%.2fx) - INFORMATIONAL: baseline is the 0.05 deg legacy object and this is %s (--allow-res-change)", nr, nl, nr / nl, REF_RES_TAG)
      }
      else {
        .log("  FAIL rows: local %d vs live %d (>25%% apart)", nr, nl)
        if (baseline_is_other_res) .log("    baseline is a different resolution; pass --allow-res-change if that is the intended reason")
        ok <- FALSE
      }
    }
  }
  if (!ok) { .log("  ABORT %s: gate failure, nothing uploaded", label); return(invisible(FALSE)) }
  finish_upload(local_f, bu$s3_url)
  sidecar <- paste0(local_f, ".json")
  if (file.exists(sidecar)) finish_upload(sidecar, paste0(bu$s3_url, ".json"))
  else .log("  sidecar MISSING (%s) - zonal_grid unrecorded on S3", basename(sidecar))
  if (REF_RES == "0.05") {
    .log("  res-05 also refreshes the deprecated unsuffixed alias %s", key_legacy)
    bu2 <- backup_then_upload(local_f, key_legacy, paste(label, "alias"))
    finish_upload(local_f, bu2$s3_url)
    if (file.exists(sidecar)) finish_upload(sidecar, paste0(bu2$s3_url, ".json"))
  }
  invisible(TRUE)
}

if (DO_REF) publish_table(local_ref, REF_KEY_LEGACY, "exposure reference (crop-livestock_all)")
if (DO_FAMILY) for (f in FAMILY) publish_table(family_local(f), family_key_legacy(f), paste("family", f))

.log("complete in %s%s", .elapsed(t0), if (DRY_RUN) " [DRY RUN - nothing written]" else "")
