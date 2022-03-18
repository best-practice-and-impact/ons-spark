options(warn = -1)
library(sparklyr)
library(dplyr)

default_config <- sparklyr::spark_config()

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "sampling",
    config = default_config)

config <- yaml::yaml.load_file("ons-spark/config.yaml")

rescue <- sparklyr::spark_read_parquet(sc, config$rescue_path) %>%
    dplyr::group_by(animal_group, cal_year) %>%
    dplyr::summarise(cost = sum(total_cost)) %>%
    dplyr::ungroup()

rescue %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

rescue %>%
    dplyr::filter(animal_group == "Hamster" & cal_year == 2012) %>%
    sparklyr::collect() %>%
    print()


animals <- rescue %>%
    sparklyr::select(animal_group) %>%
    sparklyr::sdf_distinct() %>%
    dplyr::arrange(animal_group)
    
cal_years <- rescue %>%
    sparklyr::select(cal_year) %>%
    sparklyr::sdf_distinct() %>%
    dplyr::arrange(cal_year)

print(paste("Distinct animal count:",  sparklyr::sdf_nrow(animals), sep=" "))
print(paste("Distinct year count:",  sparklyr::sdf_nrow(cal_years), sep=" "))

sparklyr::spark_set_checkpoint_dir(sc, config$checkpoint_path)

animals <- sparklyr::sdf_checkpoint(animals)
cal_years <- sparklyr::sdf_checkpoint(cal_years)

result <- animals %>%
    sparklyr::full_join(cal_years, by=character()) %>%
    dplyr::arrange(animal_group, cal_year)
    
sparklyr::sdf_nrow(result)

result %>%
    head(15) %>%
    sparklyr::collect() %>%
    print()

result <- result %>%
    sparklyr::left_join(rescue, by = c("animal_group", "cal_year"))

result %>%
    head(10) %>%
    sparklyr::collect() %>%
    print()

result <- result %>%
    sparklyr::mutate(cost = ifnull(cost, 0)) %>%
    dplyr::arrange(animal_group, cal_year)
    
result %>%
    sparklyr::filter(animal_group == "Hamster") %>%
    sparklyr::collect() %>%
    print()

system(paste0("hdfs dfs -rm -r -skipTrash ", config$checkpoint_path))
