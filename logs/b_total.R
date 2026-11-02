suppressMessages({library(arrow); library(data.table)})
f <- "/home/jovyan/common_data/nex-gddp-cimp6_hazards/Data/exposure/exposure_adm_sum_spam20-20_glw420-20_res-25.parquet"
d <- as.data.table(read_parquet(f))
cat("cols:", paste(names(d), collapse=", "), "\n")
cat("nrow:", nrow(d), "\n\n")
cat("== exposure values present ==\n"); print(d[, .N, by=exposure][order(-N)])
cat("\n== total-* rows, by crop x exposure ==\n")
print(d[grepl("^total-", crop), .N, by=.(crop, exposure)][order(crop, exposure)])
cat("\n== tlu present at all? ==\n"); print(d[exposure=="tlu", .N, by=crop])
if ("unit" %in% names(d)) { cat("\n== units for total-* ==\n"); print(unique(d[grepl("^total-",crop), .(crop, exposure, unit)])) }
cat("\n== file mtime ==\n"); print(file.info(f)$mtime)
