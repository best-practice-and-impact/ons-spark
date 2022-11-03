options(warn = -1)
library(sparklyr)
library(dplyr)
library(magrittr)

config <- yaml::yaml.load_file("ons-spark/config.yaml")

sc <- sparklyr::spark_connect(
  master = "local[2]",
  app_name = "sparklyr-intro",
  config = sparklyr::spark_config())
  
sparklyr::spark_connection_is_open(sc)

rescue_path = config$rescue_path_csv

rescue <- sparklyr::spark_read_csv(
    sc,
    path=rescue_path, 
    header=TRUE,
    infer_schema=TRUE)

class(rescue)

pillar::glimpse(rescue)

rescue

print(rescue, n=5)

rescue_tibble <- rescue %>%
    head(3) %>%
    sparklyr::collect()
    
rescue_tibble %>%
    print()

class(rescue_tibble)

rescue %>%
    sparklyr::select(IncidentNumber, DateTimeOfCall, FinalDescription) %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

rescue <- rescue %>%
    sparklyr::select(
        -WardCode,
        -BoroughCode,
        -Easting_m,
        -Northing_m,
        -Easting_rounded,
        -Northing_rounded)
        
pillar::glimpse(rescue)

rescue %>%
    sparklyr::sdf_nrow()

rescue <- rescue %>%
    dplyr::rename(
        incident_number = IncidentNumber,
        date_time_of_call = DateTimeOfCall,
        animal_group = AnimalGroupParent,
        cal_year = CalYear,
        total_cost = IncidentNotionalCostGBP,
        job_hours = PumpHoursTotal,
        engine_count = PumpCount)

pillar::glimpse(rescue)

# 1a: Use rename. Remember to put the name of the new column first.
rescue <- rescue %>%
    dplyr::rename(
        description = FinalDescription,
        postcode_district = PostcodeDistrict)

# 1b: Select the nine columns and assign to rescue
rescue <- rescue %>%
    sparklyr::select(
        incident_number,
        date_time_of_call,
        animal_group,
        cal_year,
        total_cost,
        job_hours,
        engine_count,
        description,
        postcode_district)

# 1c Preview with glimpse()
# As we have nine columns it is easier to view the transposed output
rescue %>%
    pillar::glimpse()

hamsters <- rescue %>%
    sparklyr::filter(animal_group == "Hamster")

hamsters %>%
    sparklyr::select(
        incident_number,
        animal_group,
        cal_year,
        total_cost)

expensive_olympic_dogs <- rescue %>%
    sparklyr::filter(
        animal_group == "Dog" &
        total_cost >= 750 &
        cal_year == "2012")

expensive_olympic_dogs %>%
    sparklyr::select(
        incident_number,
        animal_group,
        cal_year,
        total_cost)

# Use filter(), ensuring that a new DataFrame is created
foxes <- rescue %>%
    sparklyr::filter(animal_group == "Fox")

# Preview with head(10)
foxes %>%
    sparklyr::select(
        incident_number,
        animal_group,
        cal_year,
        total_cost) %>%
    head(10)

rescue <- rescue %>%
    sparklyr::mutate(
        incident_duration = job_hours / engine_count)

rescue %>%
    sparklyr::select(
        incident_number,
        animal_group,
        incident_duration,
        job_hours,
        engine_count) %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

rescue <- rescue %>%
    sparklyr::mutate(date_time_of_call = to_date(date_time_of_call, "dd/MM/yyyy"))

rescue %>%
    sparklyr::select(
        incident_number,
        date_time_of_call) %>%
    pillar::glimpse()

?to_date

rescue %>%
    sparklyr::select(
        incident_number,
        animal_group,
        total_cost) %>%
    dplyr::arrange(desc(total_cost)) %>%
    head(10)

# To get the top 10, sort the DF descending
top10 <- rescue %>%
    sparklyr::select(
        incident_number,
        animal_group,
        total_cost,
        incident_duration) %>%
    dplyr::arrange(desc(incident_duration)) %>%
    head(10)

top10

# The bottom 10 can just be sorted ascending, so in this example we have used sdf_sort
# Note that .tail() does not exist in Spark 2.4
bottom10 <- rescue %>%
    sparklyr::select(
        incident_number,
        animal_group,
        total_cost,
        incident_duration) %>%
    sparklyr::sdf_sort(c("incident_duration")) %>%
    head(10)

# When previewing the results, the incident_duration are all null
bottom10

cost_by_animal <- rescue %>%
    dplyr::group_by(animal_group) %>%
    dplyr::summarise(average_cost = mean(total_cost))

cost_by_animal %>%
    head(5)

cost_by_animal %>%
    dplyr::arrange(desc(average_cost)) %>%
       head(10)

goats <- rescue %>%
    sparklyr::filter(animal_group == "Goat")
    
goats %>%
    sparklyr::sdf_nrow()

goat <- goats %>%
    sparklyr::select(incident_number, animal_group, description) %>%
    sparklyr::collect()

goat

population_path <- config$population_path
population <- sparklyr::spark_read_parquet(sc, population_path)

pillar::glimpse(population)

rescue_with_pop <- rescue %>%
    sparklyr::left_join(y=population, by="postcode_district")

rescue_with_pop <- rescue_with_pop %>%
    sparklyr::sdf_sort(c("incident_number")) %>%
    sparklyr::select(incident_number, animal_group, postcode_district, population)

rescue_with_pop %>%
    head(5)

output_path_parquet <- config$rescue_with_pop_path_parquet

rescue_with_pop %>%
    sparklyr::spark_write_parquet(output_path_parquet)

output_path_csv <- config$rescue_with_pop_path_csv

rescue_with_pop %>%
    sparklyr::spark_write_csv(output_path_csv, header=TRUE)

rescue_with_pop %>%
    sparklyr::sdf_coalesce(1) %>%
    sparklyr::spark_write_csv(output_path_csv, header=TRUE, mode="overwrite")

delete_file <- function(file_path){
    cmd <- paste0("hdfs dfs -rm -r -skipTrash ",
                  file_path)
    
    system(cmd,
           ignore.stdout=TRUE,
           ignore.stderr=TRUE)
}

delete_file(output_path_parquet)
delete_file(output_path_csv)
