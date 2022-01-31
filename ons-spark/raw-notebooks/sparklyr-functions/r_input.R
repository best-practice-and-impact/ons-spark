
library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "sparklyr-functions",
    config = sparklyr::spark_config())
        
config <- yaml::yaml.load_file("ons-spark/config.yaml")

rescue <- sparklyr::spark_read_parquet(sc, config$rescue_path) %>%
    sparklyr::select(date_time_of_call, animal_group, property_category)
    
pillar::glimpse(rescue)

rescue <- rescue %>% 
    sparklyr::mutate(date_of_call = to_date(date_time_of_call, "dd/MM/yyyy"))

rescue %>%
    sparklyr::select(date_time_of_call, date_of_call) %>%
    head(5) %>%
    sparklyr::collect()

cats <- rescue %>% sparklyr::filter(initcap(animal_group) == "Cat")

cats %>%
    dplyr::group_by(animal_group) %>%
    dplyr::summarise(n())

rescue <- rescue %>% sparklyr::mutate(animal_property = concat_ws(": ", animal_group, property_category))

rescue %>%
    sparklyr::select(animal_group, property_category, animal_property) %>%
    head(5) %>%
    sparklyr::collect()
