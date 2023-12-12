options(warn = -1)
library(sparklyr)
library(dplyr)

joins_config <- sparklyr::spark_config()
# Disable broadcast join by default
joins_config$spark.sql.autoBroadcastJoinThreshold <- -1

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "joins",
    config = joins_config)

config <- yaml::yaml.load_file("ons-spark/config.yaml")

rescue <- sparklyr::spark_read_parquet(sc, config$rescue_path) %>%
    sparklyr::select(incident_number,
                     cal_year,
                     animal_group,
                     postcode_district,
                     origin_of_call)

population <- sparklyr::spark_read_parquet(sc, config$population_path)          

rescue %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()
    
population %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

rescue_with_pop <- rescue %>%
    sparklyr::left_join(population, by="postcode_district")

rescue_with_pop %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

rescue_with_pop %>% dplyr::explain()

rescue_with_pop_broadcast <- rescue %>%
    sparklyr::left_join(sparklyr::sdf_broadcast(population), by="postcode_district")

rescue_with_pop_broadcast %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

rescue_with_pop_broadcast %>% dplyr::explain()

sparklyr::spark_disconnect(sc)

joins_config <- sparklyr::spark_config()
# Automatically broadcast DataFrames less than 10MB
joins_config$spark.sql.autoBroadcastJoinThreshold <- 10 * 1024**2

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "joins",
    config = joins_config)

rescue <- sparklyr::spark_read_parquet(sc, config$rescue_path) %>%
    sparklyr::select(incident_number,
                     cal_year,
                     animal_group,
                     postcode_district,
                     origin_of_call)


population <- sparklyr::spark_read_parquet(sc, config$population_path) 

rescue_with_pop_auto_broadcast<- rescue %>%
    sparklyr::left_join(population, by="postcode_district")

rescue_with_pop_auto_broadcast %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

rescue_with_pop_auto_broadcast %>% dplyr::explain()

rescue %>% 
    sparklyr::select(origin_of_call) %>%
    sparklyr::sdf_distinct() %>%
    sparklyr::collect() %>%
    print()

call_origin <- sparklyr::sdf_copy_to(sc, data.frame(
    "origin_of_call" = c("Coastguard", "Police", "Ambulance", "Other FRS",
                         "Person (mobile)", "Person (land line)", "Person (running call)",
                         "Not known"),
    "origin_type" = c(rep(c("Emergency Services"), 4),
                      rep(c("Member of Public"), 3),
                      NA)))

call_origin %>%
    sparklyr::collect() %>%
    print()

rescue_with_origin <- rescue %>%
    sparklyr::left_join(sparklyr::sdf_broadcast(call_origin), by="origin_of_call")

rescue_with_origin %>%
    dplyr::arrange(incident_number) %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

rescue_with_origin_when <- rescue %>%
    sparklyr::mutate(origin_type = case_when(
        substr(origin_of_call, 1, 6) == "Person" ~ "Member of Public",
        origin_of_call %in% c("Coastguard", "Police", "Ambulance", "Other FRS") ~ "Emergency Services",
        origin_of_call == "Not known" ~ NA,
        TRUE ~ NA))

rescue_with_origin_when %>%
    dplyr::arrange(incident_number) %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

rescue_with_origin_when %>% dplyr::explain()
