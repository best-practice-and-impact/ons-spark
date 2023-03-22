options(warn = -1)
library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "cache",
    config = sparklyr::spark_config())

config <- yaml::yaml.load_file("ons-spark/config.yaml")

population <- sparklyr::spark_read_parquet(
    sc,
    path=config$population_path,
    name="population")

sparklyr::tbl_cache(sc, "population", force=TRUE)

sparklyr::tbl_uncache(sc, "population")

sparklyr::sdf_persist(population, storage.level = "DISK_ONLY", name = "population") %>%
    sparklyr::sdf_nrow()

sparklyr::tbl_uncache(sc, "population")

rescue <- sparklyr::spark_read_csv(sc, config$rescue_path_csv, header=TRUE, infer_schema=TRUE) 
