
library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "window-functions",
    config = sparklyr::spark_config())

config <- yaml::yaml.load_file("ons-spark/config.yaml")

rescue_agg <- sparklyr::spark_read_parquet(sc, config$rescue_path) %>%
    sparklyr::mutate(animal_group = initcap(animal_group)) %>%
    dplyr::group_by(animal_group, cal_year) %>%
    dplyr::summarise(animal_count = n()) %>%
    dplyr::ungroup()
    
rescue_agg %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

rescue_annual <- rescue_agg %>%
    dplyr::group_by(cal_year) %>%
    sparklyr::mutate(annual_count = sum(animal_count)) %>%
    dplyr::ungroup()

rescue_annual %>%
    sparklyr::filter(
        (animal_group %in% c("Cat", "Dog", "Hamster")) &
        (cal_year %in% 2012:2014)) %>%
    sparklyr::sdf_sort(c("cal_year", "animal_group")) %>%
    sparklyr::collect() %>%
    print()

rescue_annual <- rescue_annual %>%
    sparklyr::mutate(animal_pct = round((animal_count / annual_count) * 100, 2))

rescue_annual %>%
    sparklyr::filter(animal_group == "Dog") %>%
    sparklyr::sdf_sort("animal_group") %>%
    sparklyr::collect() %>%
    print()

rescue_annual = rescue_annual %>%
    dplyr::group_by(cal_year) %>%
    sparklyr::mutate(
        avg_count = mean(animal_count),
        max_count = max(animal_count)) %>%
    dplyr::ungroup()

rescue_annual %>%
    sparklyr::filter(animal_group == "Snake") %>%
    head(10) %>%
    sparklyr::collect() %>%
    print()

rescue_counts <- rescue_agg %>%
    dplyr::group_by(cal_year) %>%
    dplyr::summarise(annual_count = sum(animal_count))

rescue_annual_alternative <- rescue_agg %>%
    sparklyr::left_join(rescue_counts, by="cal_year")

rescue_annual_alternative %>%
    sparklyr::filter(animal_group == "Dog") %>%
    sparklyr::collect() %>%
    print()

rescue_rank = rescue_agg %>%
    dplyr::group_by(cal_year) %>%
    sparklyr::mutate(rank = rank(desc(animal_count))) %>%
    dplyr::ungroup()

rescue_rank %>%
    sparklyr::filter(rank <= 3) %>%
    sparklyr::sdf_sort(c("cal_year", "rank")) %>%
    head(12) %>%
    sparklyr::collect() %>%
    print()

rescue_rank %>%
    sparklyr::filter(rank == 1) %>%
    sparklyr::sdf_sort("cal_year") %>%
    sparklyr::collect() %>%
    print()

rank_comparison <- rescue_agg %>%
    dplyr::group_by(cal_year) %>%
    sparklyr::mutate(
        rank = rank(desc(animal_count)),
        dense_rank = dense_rank(desc(animal_count)),
        row_number = row_number(desc(animal_count))) %>%
    dplyr::ungroup()

rank_comparison %>%
    sparklyr::filter(cal_year == 2012) %>%
    sparklyr::sdf_sort(c("cal_year", "row_number")) %>%
    sparklyr::collect() %>%
    print()

rescue_agg %>%
    sparklyr::mutate(row_number = row_number(cal_year)) %>%
    head(10) %>%
    sparklyr::collect() %>%
    print()

rescue_agg %>%
    dplyr::group_by(animal_group) %>%
    sparklyr::mutate(previous_count = lag(animal_count, order_by = cal_year)) %>%
    dplyr::ungroup() %>%
    head(10) %>%
    sparklyr::collect() %>%
    print()

# Create a DF of all combinations of animal_group and cal_year
all_animals_years <- rescue_agg %>%
    sparklyr::select(animal_group) %>%
    sparklyr::sdf_distinct() %>%
    sparklyr::full_join(
        rescue_agg %>% sparklyr::select(cal_year) %>% sparklyr::sdf_distinct(),
        by=character()) %>%
    sparklyr::sdf_sort(c("animal_group", "cal_year"))

# Use this DF as a base to join the rescue_agg DF to
rescue_agg_prev <- all_animals_years %>%
    sparklyr::left_join(rescue_agg, by=c("animal_group", "cal_year")) %>%
    # Replace null with 0
    sparklyr::mutate(animal_count = ifnull(animal_count, 0)) %>%
    dplyr::group_by(animal_group) %>%
    # lag will then reference previous year, even if 0
    sparklyr::mutate(previous_count = lag(animal_count, order_by = cal_year)) %>%
    dplyr::ungroup()

rescue_agg_prev %>%
    sparklyr::sdf_sort(c("animal_group", "cal_year")) %>%
    head(20) %>%
    sparklyr::collect() %>%
    print()

sparklyr::sdf_register(rescue_agg, "rescue_agg")

sql_window <- dplyr::tbl(sc, dplyr::sql(
    "
    SELECT
        cal_year,
        animal_group,
        animal_count,
        SUM(animal_count) OVER(PARTITION BY cal_year) AS annual_count
    FROM rescue_agg
    "))

sql_window %>%
    sparklyr::filter(animal_group == "Snake") %>%
    sparklyr::collect() %>%
    print()
