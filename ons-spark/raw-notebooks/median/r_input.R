options(warn = -1, knitr.table.format = 'simple')
library(sparklyr)
library(dplyr)
library(knitr) # For pretty printing of tables

sc <- sparklyr::spark_connect(
  master = "local",
  app_name = "ons-spark",
  config = sparklyr::spark_config(),
  )

config <- yaml::yaml.load_file("config.yaml")

pop_df <- sparklyr::spark_read_parquet(sc, path = config$population_path)
   
sparklyr::sdf_schema(pop_df)

sdf_quantile(pop_df, "population", probabilities = 0.5, relative.error = 0)

sdf_quantile(pop_df, "population", probabilities = c(0.25, 0.5, 0.75), relative.error = 0.01)

exact = sdf_quantile(pop_df, "population", probabilities = c(0.5), relative.error = 0)[[1]]
estimate = sdf_quantile(pop_df, "population", probabilities = c(0.5), relative.error = 0.01)[[1]]

print(round((exact-estimate)/exact, 3))

borough_df <- sparklyr::spark_read_parquet(sc, path = config$rescue_clean_path)

borough_df <- borough_df %>%
    select(borough, postcode_district = postcodedistrict) %>%
    mutate(postcode_district = upper(postcode_district))
    
pop_borough_df <- borough_df %>%
    left_join(pop_df, by = "postcode_district") %>%
    filter(!is.na(borough))
    
knitr::kable(collect(pop_borough_df, n=10))

pop_borough_grouped_df <- pop_borough_df %>%
    group_by(borough) %>%
    summarise(median_postcode_population = percentile_approx(population, c(0.5), as.integer(100000)))

knitr::kable(collect(pop_borough_grouped_df, n=10))

sparklyr::spark_disconnect(sc)
