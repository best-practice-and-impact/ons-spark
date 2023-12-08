options(warn = -1)
library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
  master = "local[2]",
  app_name = "ons-spark",
  config = sparklyr::spark_config(),
  )

config <- yaml::yaml.load_file("ons-spark/config.yaml")

pop_df <- sparklyr::spark_read_parquet(sc, path = config$population_path)
                                     
sparklyr::sdf_schema(pop_df)

sdf_quantile(pop_df, "population", probabilities = c(0.5), relative.error = 0)

sdf_quantile(pop_df, "population", probabilities = c(0.25, 0.5, 0.75), relative.error = 0.2)

borough_df <- sparklyr::spark_read_parquet(sc, path = config$rescue_clean_path)

borough_df <- borough_df %>%
    select(borough, postcode_district = postcodedistrict) %>%
    mutate(postcode_district = upper(postcode_district))
    
pop_borough_df <- borough_df %>%
    left_join(pop_df, by = "postcode_district") %>%
    filter(!is.na(borough))
    
print(pop_borough_df)

pop_borough_df %>%
    group_by(borough) %>%
    summarise(median_postcode_population = percentile_approx(population, 0.5, 100000)) %>%
    print()

sparklyr::spark_disconnect(sc)
