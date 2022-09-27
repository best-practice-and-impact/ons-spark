
library(sparklyr)
library(dplyr)

config <- yaml::yaml.load_file("ons-spark/config.yaml")

small_config <- sparklyr::spark_config()
small_config$spark.sql.shuffle.partitions <- 12
small_config$spark.sql.shuffle.partitions.local <- 12

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "df-order",
    config = small_config)

winners <- sparklyr::sdf_copy_to(sc,
    data.frame(
        "year" = 2022:2000,
            "nation" = c(
                "France",
                "Wales",
                "England",
                "Wales",
                "Ireland",
                rep("England", 2),
                rep("Ireland", 2),
                rep("Wales", 2),
                "England",
                "France",
                "Ireland",
                "Wales",
                rep("France", 2),
                "Wales",
                "France",
                "England",
                "France",
                rep("England", 2))),
    repartition=2)

winners_ordered <- winners %>%
    sparklyr::sdf_sort(c("nation")) %>%
    sparklyr::mutate(partition_id = spark_partition_id())

for(show_no in 1:3){
    winners_ordered %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()
}

rescue <- sparklyr::spark_read_csv(sc,
                                   config$rescue_path_csv,
                                   header=TRUE,
                                   infer_schema=TRUE) %>%
    dplyr:::rename(
        incident_number = IncidentNumber,
        animal_group = AnimalGroupParent) %>%
    sparklyr::select(incident_number, animal_group) %>%
    sparklyr::sdf_sort(c("animal_group"))

animal_counts <- rescue %>%
    dplyr::group_by(animal_group) %>%
    dplyr::summarise(count = n()) %>%
    sparklyr::mutate(partition_id = spark_partition_id()) %>%
    sparklyr::filter(animal_group %in% c("Fox", "Goat", "Hamster"))

for(show_no in 1:3){
    animal_counts %>%
    sparklyr::collect() %>%
    print()
}
