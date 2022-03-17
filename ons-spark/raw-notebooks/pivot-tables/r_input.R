
library(sparklyr)
library(dplyr)
options(pillar.print_max = Inf, pillar.width=Inf)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "sampling",
    config = sparklyr::spark_config())

config <- yaml::yaml.load_file("ons-spark/config.yaml")

rescue <- sparklyr::spark_read_parquet(sc, config$rescue_path) %>%
    sparklyr::select(incident_number, animal_group, cal_year, total_cost, origin_of_call) %>%
    sparklyr::filter(animal_group %in% c("Cat", "Dog", "Hamster", "Sheep"))

rescue_pivot <- rescue %>%
    sparklyr::sdf_pivot(animal_group ~ cal_year)

rescue_pivot %>%
    sparklyr::collect() %>%
    print()

rescue_grouped <- rescue %>%
    dplyr::group_by(animal_group, cal_year) %>%
    dplyr::summarise(n()) %>%
    sparklyr::sdf_sort(c("animal_group", "cal_year"))

rescue_grouped %>%
    sparklyr::collect() %>%
    print()

rescue_pivot <- rescue %>%
    sparklyr::filter(cal_year %in% c("2009", "2010", "2011")) %>%
    sparklyr::sdf_pivot(
        animal_group ~ cal_year,
        fun.aggregate = list(total_cost = "sum"))

rescue_pivot %>%
    sparklyr::collect() %>%
    print()

rescue_pivot <- rescue %>%
    sparklyr::filter(cal_year %in% c("2009", "2010", "2011")) %>%
    sparklyr::mutate(total_cost_copy = total_cost) %>%
    sparklyr::sdf_pivot(
        animal_group + origin_of_call ~ cal_year,
        fun.aggregate = list(
            total_cost_copy = "sum",
            total_cost = "max"
        )) %>%
    dplyr::rename_with(~substr(., 1, 8), contains(c("_max", "_sum"))) %>%
    sparklyr::sdf_sort(c("animal_group", "origin_of_call")) %>%
    sparklyr::na.replace(0)

rescue_pivot %>%
    sparklyr::collect() %>%
    print()

rescue_tibble <- rescue %>%
    sparklyr::filter(cal_year %in% c("2009", "2010", "2011")) %>%
    sparklyr::collect()

# Check that this is a tibble
class(rescue_tibble)

tibble_pivot <- rescue_tibble %>%
    sparklyr::select(animal_group, origin_of_call, cal_year, total_cost) %>%
    tidyr::pivot_wider(
        names_from = cal_year,
        values_from = total_cost,
        values_fn = list(total_cost = sum)) %>%
    dplyr::arrange(animal_group, origin_of_call)
    
tibble_pivot %>%
    print()
