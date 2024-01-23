options(warn = -1)
library(sparklyr)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "partitions",
    config = sparklyr::spark_config())

row_ct <- 5000
seed_no <- 42L #this is used to create the pseudo-random numbers

rand_df <- sparklyr::sdf_seq(sc, 0, row_ct - 1) %>%
    sparklyr::mutate(rand_val = ceiling(rand(seed_no)*10))

rand_df %>% head(10) %>% sparklyr::collect()

print(paste0('Number of partitions: ', rand_df %>% sparklyr::sdf_num_partitions()))

rows_in_part <- sparklyr::spark_apply(rand_df, function(x) nrow(x)) %>% sparklyr::collect()
print(paste0('Number of rows per partition: ', rows_in_part)) 

small_rand <- sparklyr::sdf_seq(sc, 0, 9, repartition=1) #just 10 rows, so we'll put them into one partition
small_rand <- small_rand %>% 
    sparklyr::mutate(rand_val = ceiling(rand(seed_no)*10))
    
small_rand %>% sparklyr::collect()
small_rand %>% sparklyr::sdf_num_partitions()

config <- yaml::yaml.load_file("ons-spark/config.yaml")

rescue <- sparklyr::spark_read_csv(sc, config$rescue_path_csv, header=TRUE, inferSchema=TRUE) 

rescue %>% sparklyr::sdf_num_partitions()

rescue_rows <- rescue %>% sparklyr::sdf_nrow()
rescue_columns <- rescue %>% sparklyr::sdf_ncol()
    
rand_df_columns <- rand_df %>% sparklyr::sdf_ncol()
 
print(paste0('Number of rows in rescue DataFrame: ', rescue_rows))
print(paste0('Number of columns in rescue DataFrame:', rescue_columns))
print(paste0('Total number of cells in rescue DataFrame:', rescue_rows*rescue_columns))
print(paste0('Number of rows in random_df DataFrame:', row_ct))
print(paste0('Number of columns in random_df DataFrame:', rand_df_columns))
print(paste0('Total number of cells in random_df DataFrame:', rand_df_columns*row_ct))

filtered_rescue <- rescue %>% sparklyr::filter(AnimalGroupParent == "Cat")
filtered_rescue %>% sparklyr::select(AnimalGroupParent, FinalDescription) %>% 
    head(3) %>% 
    sparklyr::collect()

print(paste0('Number of partitions in filtered_rescue DataFrame: ',filtered_rescue %>% sparklyr::sdf_num_partitions()))

count_by_area <- rescue %>% dplyr::group_by(PostcodeDistrict) %>%
    dplyr::summarise(count=n())
    
count_by_area %>% head(5) %>% sparklyr::collect()
print(paste0('Number of partitions in count_by_area DataFrame:', count_by_area %>% sparklyr::sdf_num_partitions()))

# Note that this code takes a long time to run in sparklyr
rows_in_part <- sparklyr::spark_apply(count_by_area, function(x) nrow(x)) %>% sparklyr::collect()
print(paste0('Number of rows per partition: ', rows_in_part)) 

rand_df %>% sparklyr::sdf_num_partitions()

rand_df <- rand_df %>% sparklyr::sdf_coalesce(1)
rand_df %>% sparklyr::sdf_num_partitions()

rand_df <- rand_df %>% sparklyr::sdf_coalesce(10)
rand_df %>% sparklyr::sdf_num_partitions()

rand_df <- rand_df %>% sparklyr::sdf_repartition(1)
rand_df %>% sparklyr::sdf_num_partitions()

rand_df <- rand_df %>% sparklyr::sdf_repartition(10)
rand_df %>% sparklyr::sdf_num_partitions()

row_ct <- 10**7
seed_no <- 42L

skewed_df <- sparklyr::sdf_seq(sc, 0, row_ct - 1, repartition=2) %>%
    sparklyr::mutate(
        skew_col = dplyr::case_when(
            id < 100 ~ "A",
            id < 1000 ~ "B",
            id < 10000 ~ "C",
            id < 100000 ~ "D",
            TRUE ~ "E"),
        rand_val = ceiling(rand(seed_no)*10))

skewed_df %>% head(5) %>% sparklyr::collect()

print_partitioning_info <- function(sdf) {
    sdf %>% 
        sparklyr::mutate(
            part_id = spark_partition_id()) %>%
        dplyr::group_by(part_id) %>%
        dplyr::summarise(count=n()) %>%
        sparklyr::collect() %>%
        print()

    print(paste0("Number of partitions: ", sparklyr::sdf_num_partitions(sdf)))
}

print_partitioning_info(skewed_df)

small_df <- sparklyr::sdf_copy_to(sc, data.frame(
    "skew_col" = LETTERS[1:5], 
    "number_col" = 1:5)) 

write_delete <- function(sdf) {
    path <- paste0(config$checkpoint_path, "/temp.parquet")
    sdf %>% sparklyr::spark_write_parquet(path=path, mode="overwrite")
    cmd <- paste0("hdfs dfs -rm -r -skipTrash ", path)
    system(cmd)
}

sc %>%
    sparklyr::spark_context() %>%
    sparklyr::invoke("setJobDescription", "join")

joined_df <- skewed_df %>% sparklyr::left_join(small_df, by="skew_col")
write_delete(joined_df)

sc %>%
    sparklyr::spark_context() %>%
    sparklyr::invoke("setJobDescription", "groupby")
    
grouped_df <- skewed_df %>% 
    dplyr::group_by(skew_col) %>% 
    dplyr::summarise(sum_rand = sum(rand_val))

write_delete(grouped_df)

sc %>%
    sparklyr::spark_context() %>%
    sparklyr::invoke("setJobDescription", "window")

window_df <- skewed_df %>%
    dplyr::group_by(skew_col) %>%
    dplyr::mutate(skew_window_sum = sum(rand_val)) %>%
    dplyr::ungroup()

write_delete(window_df)

print_partitioning_info(joined_df)

print_partitioning_info(grouped_df)

print_partitioning_info(window_df)


# Stop previous spark session and create new spark session limiting number of partitions
spark_disconnect(sc)

small_config <- sparklyr::spark_config()
small_config$spark.sql.shuffle.partitions <- 12

sc <- sparklyr::spark_connect(
  master = "local",
  app_name = "partitions",
  config = small_config)

sparklyr::spark_connection_is_open(sc)

# Set the data path
config <- yaml::yaml.load_file("ons-spark/config.yaml")
# Read in and shuffle data
rescue <- sparklyr::spark_read_csv(sc, config$rescue_path_csv, header=TRUE, infer_schema=TRUE)

rescue <- rescue %>%
    dplyr::rename(
        incident_number = IncidentNumber,
        animal_group = AnimalGroupParent,
        cal_year = CalYear) %>%
        dplyr::group_by(animal_group,cal_year) %>%
        dplyr::count(animal_count = count())

rescue %>% head(5)

print(paste0("Rescue partitions: ", sparklyr::sdf_num_partitions(rescue)))

dogs <- rescue %>% sparklyr::filter(animal_group == 'Dog')
cats <- rescue %>% sparklyr::filter(animal_group == 'Cat')
hamsters <- rescue %>% sparklyr::filter(animal_group == 'Hamster')

dogs %>% head(5)

print(paste0("Dogs partitions: ", sparklyr::sdf_num_partitions(dogs)))
print(paste0("Cats partitions: ", sparklyr::sdf_num_partitions(cats)))
print(paste0("Hamsters partitions: ", sparklyr::sdf_num_partitions(hamsters)))

dogs_and_cats = sparklyr::sdf_bind_rows(dogs,cats)
print(paste0("Dogs and Cats union partitions: ", sparklyr::sdf_num_partitions(dogs_and_cats)))

dogs_cats_and_hamsters = sparklyr::sdf_bind_rows(dogs_and_cats,hamsters)
print(paste0("Dogs, Cats and Hamsters union partitions: ", sdf_num_partitions(dogs_cats_and_hamsters)))

sparklyr::sdf_num_partitions(dogs_cats_and_hamsters %>% sparklyr::sdf_sort(c('animal_group','cal_year')))

sparklyr::sdf_num_partitions(dogs_cats_and_hamsters %>% sparklyr::sdf_repartition(20))
