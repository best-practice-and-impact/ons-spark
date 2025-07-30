

library(sparklyr)
library(dplyr)
library(rlang)

default_config <- sparklyr::spark_config()
default_config$spark.local.dir <- "D:/tmp"

sc <- sparklyr::spark_connect(
  master = "local",
  app_name = "working_with_duplicates",
  config = default_config)

connection_is_open(sc)


#read in the MOT dataset and select columns we want to work with
#mot_path = "s3a://onscdp-dev-data01-5320d6ca/bat/dapcats/mot_test_results.csv"
#mot_path = "C:/Users/hallj/repos/data/mot_test_results.csv"
mot_path = "D:/dapcats_guidance/working-with-duplicates/mot_test_results.csv"

mot <- sparklyr::spark_read_csv(sc,
    mot_path, 
    source = "csv", 
    header = TRUE, 
    infer_schema = TRUE) %>% 
    sparklyr::select(
      test_id,
      vehicle_id, 
      test_date, 
      test_mileage, 
      postcode_area, 
      make, 
      colour,
      test_result)

#change the date format to yyyy-MM-dd
mot <- mot %>%
    dplyr::mutate(test_date = dplyr::sql("CAST(test_date AS DATE)"))

#remove rows with nulls
mot <- na.omit(mot)

#check schema and preview data
pillar::glimpse(mot)


#check the size of the data we are working with
mot %>%
    sparklyr::sdf_nrow() %>%
    print()




#Filter to vehicles that have passed their MOT and groupby vehicle_id
duplicate_count <- mot %>%
  dplyr::filter(test_result == "P") %>%
  dplyr::group_by(vehicle_id) %>%
  dplyr::summarise(count = dplyr::n(), .groups = 'drop') %>%
  dplyr::arrange(dplyr::desc(count)) %>%
  dplyr::ungroup()

#Show top 10 rows
duplicate_count %>%
  head(10) %>%
  collect() %>%
  print(width = Inf)



#sample_path = "s3a://onscdp-dev-data01-5320d6ca/bat/dapcats/mot_duplicate_sample.parquet"
#sample_path = "C:/Users/hallj/repos/ons-spark/ons-spark/ons-spark/data/mot_duplicate_sample.parquet"
sample_path = "D:/dapcats_guidance/working-with-duplicates/mot_duplicate_sample.parquet"

#Read in a 10% sample of vehicle id 223981155 for demo purposes
sample <- sparklyr::spark_read_parquet(sc,
                                   sample_path,
                                   source = "parquet")

#Check count of this vehicle per postcode area
sample %>%
  dplyr::group_by(postcode_area) %>%
  dplyr::summarise(count = dplyr::n(), .groups = 'drop') %>%
  dplyr::arrange(dplyr::desc(count)) %>%
  collect() %>%
  print(width = Inf) 



#Drop duplicates based on all columns
sample %>% 
  sparklyr::sdf_drop_duplicates() %>%
  dplyr::arrange(postcode_area, desc(test_mileage)) %>%   
  collect() %>%
  print(width = Inf)



#Check current partition of the data
sparklyr::sdf_num_partitions(sample) %>% print()



#Drop duplicates based on postcode_area
sample %>%
  sparklyr::sdf_drop_duplicates(cols = "postcode_area") %>%
  collect() %>%
  print(width = Inf)



#Repartition the data
sample <- sparklyr::sdf_repartition(sample, partitions = 10)
sparklyr::sdf_num_partitions(sample) %>% print()



#Rerun drop duplicates
sample %>%
  sparklyr::sdf_drop_duplicates(cols = "postcode_area") %>%
  collect() %>%
  print(width = Inf)
  


#create our window based on postcode_area, ordered by test_mileage.
#rank the rows within each postcode_area based on mileage.
windowed_duplicates <- sample %>%
  dplyr::group_by(postcode_area) %>%
  dplyr::arrange(desc(test_mileage), .by_group = TRUE) %>%
  dplyr::mutate(row_n = dplyr::row_number()) %>%
  dplyr::ungroup()

windowed_duplicates %>%
  collect() %>%
  print(width = Inf)



#retain the first entry (highest mileage) in each postcode_area
highest_mileage_duplicates <- windowed_duplicates %>%
  dplyr::filter(row_n == 1) %>%
  dplyr::select(-row_n) %>%
  dplyr::arrange(postcode_area)

highest_mileage_duplicates %>%
  collect() %>%
  print(width = Inf)


#Close Spark session
spark_disconnect(sc)
