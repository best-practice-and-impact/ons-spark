options(warn = -1)
library(sparklyr)
library(dplyr)

shuffle_config <- sparklyr::spark_config()
# Set number of partitions after a shuffle to 2
shuffle_config$spark.sql.shuffle.partitions <- 2
# Set number of local partitions to 2
shuffle_config$spark.sql.local.partitions <- 2

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "shuffling",
    config = shuffle_config)

seed_no <- 999L

example_1 <- sparklyr::sdf_register(sparklyr::sdf_seq(sc, 0, 19, repartition=2), "example_1")
sparklyr::tbl_cache(sc, "example_1")
example_1 %>%
    sparklyr::collect() %>%
    print()

example_1 <- example_1 %>%
    sparklyr::mutate(partition_id = spark_partition_id())
sparklyr::tbl_cache(sc, "example_1")
example_1 %>%
    sparklyr::collect() %>%
    print()

example_1 <- example_1 %>%
    sparklyr::mutate(
        rand1 = ceil(rand(seed_no) * 10),
        partition_id = spark_partition_id())

sparklyr::tbl_cache(sc, "example_1")
example_1 %>%
    sparklyr::collect() %>%
    print()

example_1 <- example_1 %>%
    sparklyr::sdf_sort("rand1") %>%
    sparklyr::mutate(partition_id_new = spark_partition_id())

example_1 %>%
    sparklyr::collect() %>%
    print()

spark_ui_url <- paste0(
    "http://",
    "spark-",
    Sys.getenv("CDSW_ENGINE_ID"),
    ".",
    Sys.getenv("CDSW_DOMAIN"))

spark_ui_url

sparklyr::spark_disconnect(sc)

shuffle_config <- sparklyr::spark_config()
# Set number of partitions after a shuffle to 100
shuffle_config$spark.sql.shuffle.partitions <- 100
# Set number of local partitions to 100
shuffle_config$spark.sql.local.partitions <- 100
# Disable broadcast join by default
shuffle_config$spark.sql.autoBroadcastJoinThreshold <- -1

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "shuffling",
    config = shuffle_config)

example_2A <- sparklyr::sdf_seq(sc, 0, 10**5 - 1) %>%
    sparklyr::mutate(rand1 = ceil(rand(seed_no) * 10))
           
example_2B <- sparklyr::sdf_seq(sc, 0, 10**5 - 1) %>%
    sparklyr::mutate(rand2 = ceil(rand(seed_no + 1L) * 10))

joined_df <- example_2A %>%
    sparklyr::left_join(example_2B, by="id") %>%
    dplyr::group_by(rand1, rand2) %>%
    dplyr::summarise(row_count = n()) %>%
    sparklyr::sdf_sort(c("rand1", "rand2"))

collected_df <- joined_df %>%
    sparklyr::collect()

print("Row count: ")
print(nrow(collected_df))
print("Top 5 rows:")
print(head(collected_df, 5))
print("Bottom 5 rows:")
print(tail(collected_df, 5))

spark_ui_url

unsorted_df <- sparklyr::sdf_seq(sc, 0, 10**5 - 1) %>%
    sparklyr::mutate(rand1 = ceil(rand(seed_no) * 10),
                     # Create temporary column for sorting descending
                     rand2 = rand1 * -1)

sorted_df <- unsorted_df %>% sparklyr::sdf_sort("rand1")
sorted_df <- sorted_df %>% sparklyr::sdf_sort("rand2")
sorted_df <- sorted_df %>% sparklyr::sdf_sort("rand1")
sorted_df <- sorted_df %>% sparklyr::sdf_sort("rand2") %>%
    # Remove temporary column used to sort descending
    sparklyr::select(-rand2)

sorted_collected_df <- sorted_df %>%
    sparklyr::collect()

print("Row count: ")
print(nrow(sorted_collected_df))
print("Top 5 rows:")
print(head(sorted_collected_df, 5))
print("Bottom 5 rows:")
print(tail(sorted_collected_df, 5))

spark_ui_url

sorted_df %>%
    sparklyr::spark_dataframe() %>%
    sparklyr::invoke("queryExecution")
