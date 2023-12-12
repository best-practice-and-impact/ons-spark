options(warn = -1)
library(sparklyr)
library(dplyr)

default_config <- sparklyr::spark_config()

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "cramer-v",
    config = default_config)

config <- yaml::yaml.load_file("ons-spark/config.yaml")
rescue <- sparklyr::spark_read_parquet(sc, config$rescue_clean_path, header=TRUE, infer_schema=TRUE)
rescue <- rescue %>% dplyr::rename(
                    animal_type = animal_group,
                    postcode_district = postcodedistrict)


freq_spark <- sdf_crosstab(rescue, 'cal_year', 'animal_type') 
glimpse(freq_spark)

freq_r <- freq_spark %>% collect() 
freq_r <- subset(freq_r, select = -cal_year_animal_type)


get_cramer_v <- function(freq_r){
  # Chi-squared test statistic, sample size, and minimum of rows and columns
  X2 <- chisq.test(freq_r, correct = FALSE)$statistic
  X2 <- as.double(X2)
  n <- sum(freq_r)
  minDim <- min(dim(freq_r)) - 1

  # calculate Cramer's V 
  V <- sqrt((X2/n) / minDim)
  return(V)
}

get_cramer_v(freq_r)

freq_spark <- sdf_crosstab(rescue, 'postcode_district', 'animal_type') 
freq_r <- freq_spark %>% collect() 
freq_r <- subset(freq_r, select = -postcode_district_animal_type)
get_cramer_v(freq_r)


rescue_raw <- sparklyr::spark_read_csv(sc, config$rescue_path_csv, header=TRUE, infer_schema=TRUE)
rescue_raw <- rescue_raw %>%
    dplyr::rename(
        incident_number = IncidentNumber,
        animal_type = AnimalGroupParent,
        cal_year = CalYear)

tryCatch(
  {
    sdf_crosstab(rescue_raw, 'cal_year', 'animal_type')
  },
  error = function(e){
    message('Error message from Spark')
    print(e)
  }
)

rescue_raw %>% sparklyr::select(animal_type) %>% distinct() %>% print(n=27)
