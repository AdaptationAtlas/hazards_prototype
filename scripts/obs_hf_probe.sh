#!/usr/bin/env bash
# scripts/obs_hf_probe.sh
# =======================
# Read-only bandwidth probe for the Hugging Face Zarr cubes of CHIRTS-ERA5 daily and CHIRPS v3 daily
# (CDH redistributions, 2026-10-08), the alternative to data.chc.ucsb.edu that the S0 probe found
# throttled to 8-10 kB/s (DISPATCH_obs_s0_probe.md, 2026-10-24).
#
# Stores (Zarr v3, int16, Zstd-5, chunks 128 days x 300 lat x 300 lon, chunk keys c/<t>/<lat>/<lon>):
#   https://huggingface.co/datasets/aaguilar90/chirts-era5-daily/resolve/main/chirts-era5-daily.zarr  (Tmax, Tmin; 70N..60S)
#   https://huggingface.co/datasets/aaguilar90/chirps-v3-daily-rnl/resolve/main/chirps-v3-daily-rnl.zarr (precip; 60N..60S)
# Africa window -18..52 E, 38 N..35 S on the 0.05 grid:
#   CHIRTS lat rows (70-lat)/0.05 -> 640..2100 -> lat chunks 2..7; lon cols (lon+180)/0.05 -> 3240..4640 -> lon chunks 10..15
#   CHIRPS lat rows (60-lat)/0.05 -> 440..1900 -> lat chunks 1..6; same lon chunks
#   => 36 chunks per variable per 128-day slab. Time index = days since 1981-01-01; slab = index // 128.
#   2015-06-15 -> 12583 -> slab 98;  2025-06-15 -> 16237 -> slab 126.
# `resolve/main/` answers 302 to a CDN host; curl needs -L. Range requests are honoured (206).
#
# 2026-10-24 fixes after the node runs: output names sanitised to [A-Za-z0-9._-] (PASCAL's filesystem refused ':'), a slab
# line counts a non-zero curl exit as an error and sums bytes over complete files only, and H8 sets the georeferencing by hand
# (GDAL's ZARR driver drops lat/lon) and no longer rescales (terra applies scoff).
# Chunks that are all fill value (ocean) are not written, so they answer 404; inside the Africa window
# that is 11 of 36 CHIRTS chunks and 4 of 36 CHIRPS chunks. The slab lines count them as absent_ocean_404,
# separately from errors. Measured from the macbook 2026-10-24: single stream 1.4-1.9 MB/s, 8 streams
# 4-5.4 MB/s aggregate, range requests 206.
# Tests (each prints one CSV line: test,label,files,bytes,seconds,MB_per_s,note):
#   H1  one CHIRTS Tmax chunk, twice (single-stream rate; compare with S0 A1 at ~1 MB/s and the 8-10 kB/s throttle)
#   H2  Africa slab, CHIRTS Tmax, slab 98, 36 chunks, 8 parallel streams (what an ingest would do)
#   H3  same slab, 1 stream (does parallelism help or hurt here, as it hurt on CHC)
#   H4  Africa slab, CHIRTS Tmax, slab 126 (recent years; detects any per-period difference)
#   H5  Africa slab, CHIRPS precip, slab 98, 8 parallel
#   H6  range request: first 1 KiB of one chunk -> expects HTTP 206 (what GDAL /vsicurl and zarr-python do)
#   H7  optional: python3 zarr>=3 + xarray end-to-end read of the Africa window for 2015-06 (if importable)
#   H8  optional: R terra open of the store via GDAL ZARR driver over /vsicurl (needs terra::gdal() >= 3.11)
# Nothing is kept: downloads go to $TMP (under the bulk root you pass) and are deleted on exit.
#
# Usage (node, repo root; TMP root MUST be under the bulk data root, never / or /home):
#   OBS_TMP_ROOT=/path/under/bulk/root bash scripts/obs_hf_probe.sh 2>&1 | tee logs/obs_hf_probe_$(date +%Y%m%d_%H%M%S).log
# Env: OBS_HF_PAR (default 8) parallel streams; OBS_HF_SKIP_OPTIONAL=1 skips H7/H8.
set -uo pipefail
: "${OBS_TMP_ROOT:?set OBS_TMP_ROOT to a directory under the bulk data root}"
PAR="${OBS_HF_PAR:-8}"
TMP="$(mktemp -d "$OBS_TMP_ROOT/obs_hf_probe.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT
ts() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(ts)] [obs-hf] $*"; }
CHIRTS=https://huggingface.co/datasets/aaguilar90/chirts-era5-daily/resolve/main/chirts-era5-daily.zarr
CHIRPS=https://huggingface.co/datasets/aaguilar90/chirps-v3-daily-rnl/resolve/main/chirps-v3-daily-rnl.zarr
log "curl $(curl --version | head -1 | cut -d' ' -f2)  PAR=$PAR  (no hostname printed: public repo)"
echo "test,label,files,bytes,seconds,MB_per_s,note"

# one URL, timed; prints bytes seconds http_code
fetch() { curl -sL --max-time 600 -o "$2" -w '%{size_download} %{time_total} %{http_code}\n' "$1"; }

# H1: single chunk twice
for i in 1 2; do
  r=$(fetch "$CHIRTS/Tmax/c/98/5/13" "$TMP/h1_$i"); set -- $r
  echo "H1,single_chunk_run$i,1,$1,$2,$(awk -v b=$1 -v t=$2 'BEGIN{printf "%.2f", b/t/1e6}'),http=$3"
done

# slab downloader: store var slab lat0 lat1 par label
slab() {
  local store="$1" var="$2" t="$3" la0="$4" la1="$5" par="$6" label="$7"
  local list="$TMP/list_$label" out="$TMP/$label"
  mkdir -p "$out"; : > "$list"
  for la in $(seq $la0 $la1); do for lo in $(seq 10 15); do echo "$store/$var/c/$t/$la/$lo" >> "$list"; done; done
  local n; n=$(wc -l < "$list" | tr -d ' ')
  local t0 t1; t0=$(date +%s.%N)
  # per-file lines: bytes seconds http curl_rc  (a helper script, not xargs -I: BSD xargs caps -I replacement at 255 bytes)
  # PASCAL 2026-10-24: the output name must carry no ':' (its bulk filesystem refuses the colon with EIO, curl then exits 23
  # "failed writing body" after one buffer while http_code still reads 200), and the line must fail on curl's exit status,
  # not on the HTTP code alone. Name = URL with every char outside [A-Za-z0-9._-] replaced; rc appended as a 4th field.
  # The result line is written with ONE printf (under PIPE_BUF) so parallel workers cannot interleave their fields:
  # a two-write version merged lines under -P 8 on the macbook and showed up as phantom errors.
  printf '%s\n' '#!/usr/bin/env bash' 'out="$1"; url="$2"; f="$out/$(printf "%s" "$url" | tr -c "A-Za-z0-9._-" "_")"; r=$(curl -sL --max-time 600 -o "$f" -w "%{size_download} %{time_total} %{http_code}" "$url"); rc=$?; printf "%s %s\n" "$r" "$rc"' > "$TMP/get.sh"
  xargs -P "$par" -n 1 bash "$TMP/get.sh" "$out" < "$list" > "$TMP/res_$label"
  t1=$(date +%s.%N)
  local bytes wall absent errors
  bytes=$(awk '{s+=$1} END{print s+0}' "$TMP/res_$label"); wall=$(awk -v a=$t0 -v b=$t1 'BEGIN{printf "%.2f", b-a}')
  # 404 = chunk not written = every cell is the fill value (ocean); expected inside the Africa window. Any other HTTP code,
  # or a non-zero curl exit (truncated write, timeout, TLS), is an error. Bytes are summed over complete files only.
  absent=$(awk '$3==404 && $4==0{c++} END{print c+0}' "$TMP/res_$label"); errors=$(awk '!($3==200 && $4==0) && !($3==404 && $4==0){c++} END{print c+0}' "$TMP/res_$label")
  bytes=$(awk '$3==200 && $4==0{s+=$1} END{print s+0}' "$TMP/res_$label")
  echo "$label,$var slab $t lat $la0-$la1 x$par,$n,$bytes,$wall,$(awk -v b=$bytes -v t=$wall 'BEGIN{printf "%.2f", b/t/1e6}'),absent_ocean_404=$absent errors=$errors complete=$(awk '$3==200 && $4==0{c++} END{print c+0}' "$TMP/res_$label") per_stream_min_max=$(awk '$3==200 && $4==0{r=$1/$2; if(mn==""||r<mn)mn=r; if(r>mx)mx=r} END{printf "%.2f/%.2f MB/s", mn/1e6, mx/1e6}' "$TMP/res_$label")"
  rm -rf "$out"
}
slab "$CHIRTS" Tmax 98  2 7 "$PAR" H2
slab "$CHIRTS" Tmax 98  2 7 1      H3
slab "$CHIRTS" Tmax 126 2 7 "$PAR" H4
slab "$CHIRPS" precip 98 1 6 "$PAR" H5

# H6: range request
code=$(curl -sL -r 0-1023 -o /dev/null -w '%{http_code}' "$CHIRTS/Tmax/c/98/5/13")
echo "H6,range_request_1KiB,1,1024,,,http=$code (expect 206)"

if [ -z "${OBS_HF_SKIP_OPTIONAL:-}" ]; then
  # H7: python zarr + xarray end to end
  if ! python3 -c 'import zarr, xarray; assert int(zarr.__version__.split(".")[0])>=3' 2>/dev/null; then
    echo "H7,python_zarr_xarray,,,,,skipped: python3 with zarr>=3 and xarray not importable"
  else
    python3 - "$CHIRTS" <<'PY'
import sys, time, xarray as xr
url = sys.argv[1]; t0 = time.time()
ds = xr.open_zarr(url, consolidated=True, chunks={})
t1 = time.time()
sub = ds["Tmax"].sel(time=slice("2015-06-01", "2015-06-30"), lat=slice(38, -35), lon=slice(-18, 52)).load()
t2 = time.time()
v = sub.values; nbytes = v.nbytes
print(f"H7,python_xarray_africa_2015-06,1,{nbytes},{t2-t0:.2f},{nbytes/(t2-t0)/1e6:.2f},open={t1-t0:.2f}s load={t2-t1:.2f}s shape={tuple(sub.shape)} dtype={v.dtype} mean_scaled={float(sub.mean())*0.1 if v.dtype.kind=='i' else float(sub.mean()):.2f} zarr={__import__('zarr').__version__} xarray={xr.__version__}")
PY
  fi
  # H8: R terra via GDAL ZARR driver over /vsicurl. cglabs 2026-10-24: GDAL 3.11.3 opens the store but drops the
  # lat/lon coordinate arrays (extent 0..7200 x 0..2600, no CRS), so the extent and CRS are set here by hand from
  # the store's grid (0.05 deg, lon -180..180, lat 70N..60S, row 0 = north). terra applies the stored scale
  # (scoff 0.1) on read, so values are already degC: no extra x0.1 (the first version applied it twice).
  if command -v Rscript >/dev/null 2>&1; then
    Rscript -e '
suppressMessages(library(terra)); g <- as.character(terra::gdal())
u <- commandArgs(TRUE)[1]; t0 <- Sys.time()
r <- tryCatch(suppressWarnings(terra::rast(sprintf("ZARR:\"/vsicurl/%s\":/Tmax", u))), error = function(e) e)
if (inherits(r, "error")) { cat(sprintf("H8,r_terra_gdal_zarr,,,,,gdal=%s open FAILED: %s\n", g, gsub(",", ";", conditionMessage(r)))) } else {
  georef_missing <- isTRUE(all.equal(as.vector(terra::ext(r)), c(0, 7200, 0, 2600))) || !nzchar(terra::crs(r))
  if (georef_missing) { terra::ext(r) <- terra::ext(-180, 180, -60, 70); terra::crs(r) <- "EPSG:4326" }
  so <- terra::scoff(r); so_txt <- if (is.null(so)) "none" else sprintf("scale=%g offset=%g", so[1, 1], so[1, 2])
  e <- terra::ext(-18, 52, -35, 38); t1 <- Sys.time()
  x <- tryCatch({ s <- r[[12583 + 1]]; terra::crop(s, e) }, error = function(e) e)   # 2015-06-15 = index 12583 (0-based)
  t2 <- Sys.time()
  if (inherits(x, "error")) cat(sprintf("H8,r_terra_gdal_zarr,,,,,gdal=%s open ok (%d layers) but crop FAILED: %s\n", g, terra::nlyr(r), gsub(",", ";", conditionMessage(x)))) else {
    NAflag(x) <- -32768
    cat(sprintf("H8,r_terra_gdal_zarr_africa_1day,1,%d,%.2f,,gdal=%s layers=%d georef_set_by_hand=%s scoff=%s dims=%dx%d mean_degC=%.2f open=%.2fs crop=%.2fs\n",
        terra::ncell(x) * 2L, as.numeric(difftime(t2, t0, units = "secs")), g, terra::nlyr(r), georef_missing, so_txt, nrow(x), ncol(x),
        terra::global(x, "mean", na.rm = TRUE)[1, 1], as.numeric(difftime(t1, t0, units = "secs")), as.numeric(difftime(t2, t1, units = "secs"))))
  }
}' "$CHIRTS" 2>&1 | grep '^H8' || echo "H8,r_terra_gdal_zarr,,,,,Rscript present but no H8 line (see stderr above)"
  else echo "H8,r_terra_gdal_zarr,,,,,skipped: no Rscript"; fi
fi
log "done; TMP removed on exit"
