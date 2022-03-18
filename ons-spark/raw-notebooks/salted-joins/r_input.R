options(warn = -1)
library(sparklyr)
library(dplyr)

small_config <- sparklyr::spark_config()

small_config$spark.executor.memory <- "1500m"
small_config$spark.executor.cores <- 2
small_config$spark.dynamicAllocation.enabled <- "true"
small_config$spark.dynamicAllocation.maxExecutors <- 4
# Disable Broadcast join by default
small_config$spark.sql.autoBroadcastJoinThreshold <- -1

sc <- sparklyr::spark_connect(
  master = "yarn-client",
  app_name = "joins",
  config = small_config)


row_ct <- 10 ** 7
skewed_df <- sparklyr::sdf_seq(sc, 0, row_ct - 1) %>%
    sparklyr::mutate(join_col = dplyr::case_when(
        id < 100 ~ "A",
        id < 1000 ~ "B",
        id < 10000 ~ "C",
        id < 100000 ~ "D",
        TRUE ~ "E"))

skewed_df %>%
    dplyr::group_by(join_col) %>%
    dplyr::summarise(count = n()) %>%
    sparklyr::mutate(pct_of_data = round((count / row_ct) * 100, 3)) %>%
    sparklyr::collect() %>%
    print()

small_df <- sparklyr::sdf_copy_to(sc, data.frame(
    join_col = LETTERS[1:5],
    number_col = 1:5))

joined_df <- skewed_df %>%
    sparklyr::left_join(small_df, by="join_col")

sc %>%
    sparklyr::spark_context() %>%
    sparklyr::invoke("setJobDescription", "Row Count")

sparklyr::sdf_nrow(joined_df)

sc %>%
    sparklyr::spark_context() %>%
    sparklyr::invoke("setJobDescription", "Show Grouped Data")

joined_df %>%
    dplyr::group_by(join_col, number_col) %>%
    dplyr::summarise(count = n()) %>%
    sparklyr::collect() %>%
    print()

spark_ui_url <- paste0(
    "http://",
    "spark-",
    Sys.getenv("CDSW_ENGINE_ID"),
    ".",
    Sys.getenv("CDSW_DOMAIN"))

print(spark_ui_url)

sc %>%
    sparklyr::spark_context() %>%
    sparklyr::invoke("setJobDescription", "Salted Join")

salt_count <- 10
seed_no <- 123L
skewed_df <- skewed_df %>%
    # Salted column will be in the format A-0, A-1...E-9
    sparklyr::mutate(salted_col = concat_ws("-", join_col, floor(rand(seed_no) * salt_count)))

skewed_df %>%
    head(20) %>%
    sparklyr::collect() %>%
    print()

salt_df <- sparklyr::sdf_seq(sc, 0, salt_count - 1)

small_df_salted <- small_df %>%
    sparklyr::full_join(salt_df, by=character()) %>%
    sparklyr::mutate(salted_col = concat_ws("-", join_col, id)) %>%
    sparklyr::select(-id, -join_col)

small_df_salted %>%
    head(20) %>%
    sparklyr::collect() %>%
    print()

salted_join_df <- skewed_df %>%
    sparklyr::left_join(small_df_salted, by="salted_col")

sc %>%
    sparklyr::spark_context() %>%
    sparklyr::invoke("setJobDescription", "Salted Join Row Count")

sparklyr::sdf_nrow(salted_join_df)

sc %>%
    sparklyr::spark_context() %>%
    sparklyr::invoke("setJobDescription", "Salted Join Show Grouped Data")

joined_df %>%
    dplyr::group_by(join_col, number_col) %>%
    dplyr::summarise(count = n()) %>%
    sparklyr::collect() %>%
    print()

spark_ui_url
