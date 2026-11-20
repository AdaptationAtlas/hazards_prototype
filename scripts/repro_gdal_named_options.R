# Reproducer: terra::writeRaster(gdal = <named vector>) silently drops GDAL options.
# terra passes only the values ("NONE", "ZSTD"), which GDAL ignores (no KEY=), so a COG
# falls back to its defaults: LZW compression and overviews built. Run: Rscript scripts/repro_gdal_named_options.R
library(terra)
r <- rast(nrows = 2048, ncols = 2048, xmin = 0, xmax = 20, ymin = 0, ymax = 20)
values(r) <- round(outer(1:2048, 1:2048, function(i, j) sin(i / 200) + cos(j / 300)), 2)
opts <- list(named_before = c("OVERVIEWS" = "NONE", COMPRESS = "ZSTD"),
             keyvalue_after = c("OVERVIEWS=NONE", "COMPRESS=ZSTD"))
cat(sprintf("terra %s, GDAL %s\n", packageVersion("terra"), gdal()))
for (nm in names(opts)) {
  f <- tempfile(fileext = ".tif")
  writeRaster(r, f, filetype = "COG", gdal = opts[[nm]], overwrite = TRUE)
  info <- describe(f)
  comp <- sub(".*COMPRESSION=", "", grep("COMPRESSION=", info, value = TRUE))
  n_ovr <- length(strsplit(sub(".*Overviews: ", "", grep("Overviews:", info, value = TRUE)[1]), ",")[[1]])
  if (!any(grepl("Overviews:", info))) n_ovr <- 0L
  cat(sprintf("%-15s compression=%-5s overviews=%d size=%7.1f KB values_equal=%s\n", nm, comp,
              n_ovr, file.size(f) / 1024, isTRUE(all.equal(values(rast(f)), values(r), tolerance = 1e-6))))
}
