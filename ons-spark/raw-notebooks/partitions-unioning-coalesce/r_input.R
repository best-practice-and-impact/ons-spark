options(warn = -1)
library(sparklyr)
library(dplyr)

# Create spark session
small_config <- sparklyr::spark_config()
small_config$spark.sql.shuffle.partitions <- 12

sc <- sparklyr::spark_connect(
  master = "local",
  app_name = "Partitioning-Unioning",
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
