-- CC4A briefing: interaction-class shares of exposure, observed (CHIRPS v3 / CHIRTS-ERA5 / AgERA5, 2006-2025)
-- vs NEX-GDDP-CMIP6 historical ensemble (1995-2014), same hazard set NDWS+NTx35+NDWL0.
-- share(class) = sum over admin2 of exposure x frequency(class) / sum of (any + none); NaN rows skipped.
-- Run: duckdb -csv < class_shares.sql > class_shares.csv
INSTALL httpfs; LOAD httpfs;
CREATE MACRO obs_url(v, sev) AS 'https://digital-atlas.s3.amazonaws.com/domain=hazard_exposure/source=chirps-chirts-agera5/region=ssa/processing=hazard-risk-exposure/variable=' || v || '/period=ggcmi-crop/model=chirps-chirts-agera5/severity=' || sev || '/int=multi-hazard.parquet';
CREATE MACRO nex_url(v, sev) AS 'https://digital-atlas.s3.amazonaws.com/domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/variable=' || v || '/period=jagermeyr/model=ENSEMBLEmean/severity=' || sev || '/int=multi-hazard.parquet';
CREATE TEMP TABLE raw AS
  SELECT 'observed 2006-2025' AS source, 'moderate' AS severity, 'prod_t' AS var, iso3, crop, hazard, value FROM read_parquet(obs_url('prod_t','moderate'), hive_partitioning=false) WHERE hazard_vars='NDWS+NTx35+NDWL0'
  UNION ALL SELECT 'NEX historic 1995-2014','moderate','prod_t', iso3, crop, hazard, value FROM read_parquet(nex_url('prod_t','moderate'), hive_partitioning=false) WHERE hazard_vars='NDWS+NTx35+NDWL0' AND scenario='historic'
  UNION ALL SELECT 'observed 2006-2025','moderate','head_n', iso3, crop, hazard, value FROM read_parquet(obs_url('head_n','moderate'), hive_partitioning=false) WHERE hazard_vars='NDWS+THI-max+NDWL0'
  UNION ALL SELECT 'NEX historic 1995-2014','moderate','head_n', iso3, crop, hazard, value FROM read_parquet(nex_url('head_n','moderate'), hive_partitioning=false) WHERE scenario='historic' AND hazard_vars='NDWS+THI-max+NDWL0';
CREATE TEMP TABLE agg AS
  SELECT source, severity, var, iso3, crop, hazard, sum(value) FILTER (WHERE NOT isnan(value)) AS v FROM raw GROUP BY ALL;
-- continental rows too
INSERT INTO agg SELECT source, severity, var, 'SSA' AS iso3, crop, hazard, sum(v) FROM agg GROUP BY source, severity, var, crop, hazard;
SELECT source, var, iso3, crop,
  round(100*sum(v) FILTER (WHERE hazard='none') / tot, 1) AS none_pct,
  round(100*sum(v) FILTER (WHERE hazard='dry') / tot, 1) AS dry,
  round(100*sum(v) FILTER (WHERE hazard='heat') / tot, 1) AS heat,
  round(100*sum(v) FILTER (WHERE hazard='wet') / tot, 1) AS wet,
  round(100*sum(v) FILTER (WHERE hazard='dry+heat') / tot, 1) AS dry_heat,
  round(100*sum(v) FILTER (WHERE hazard='dry+wet') / tot, 1) AS dry_wet,
  round(100*sum(v) FILTER (WHERE hazard='heat+wet') / tot, 1) AS heat_wet,
  round(100*sum(v) FILTER (WHERE hazard='dry+heat+wet') / tot, 1) AS dry_heat_wet,
  round(tot) AS exposure_total
FROM (SELECT *, sum(v) FILTER (WHERE hazard IN ('any','none')) OVER (PARTITION BY source, var, iso3, crop) AS tot FROM agg)
WHERE iso3 IN ('SSA','KEN','ETH','NGA','TZA') AND crop IN ('maize','sorghum','cassava','bean','cattle-tropical','goats-tropical')
GROUP BY source, var, iso3, crop, tot ORDER BY var, crop, iso3, source DESC;
