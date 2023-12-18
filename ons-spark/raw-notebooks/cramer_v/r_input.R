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


freq_spark <- sparklyr::sdf_crosstab(rescue, 'cal_year', 'animal_type') 
glimpse(freq_spark)

freq_r <- freq_spark %>% collect() 
freq_r <- subset(freq_r, select = -cal_year_animal_type)


get_cramer_v <- function(freq_r){
  # Get cramer V statistic
  # Returns the Cramer's V Statistic for the given
  # pair-wise frequency table
  # 
  # Param: 
  # freq_r: dataframe. pair-wise frequency / contingency table not containing strings
  #
  # Return:
  # cramer_v_statistic: numeric. The Cramer's V statistic 
  # 

  # Chi-squared test statistic, sample size, and minimum of rows and columns
  chi_sqrd <- chisq.test(freq_r, correct = FALSE)$statistic
  chi_sqrd <- as.double(chi_sqrd)
  n <- sum(freq_r)
  min_dim <- min(dim(freq_r)) - 1

  # calculate Cramer's V 
  cramer_v_statistic <- sqrt((chi_sqrd/n) / min_dim)
  return(cramer_v_statistic)
}

get_cramer_v(freq_r)

freq_spark <- sparklyr::sdf_crosstab(rescue, 'postcode_district', 'animal_type') 
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
    sparklyr::sdf_crosstab(rescue_raw, 'cal_year', 'animal_type')
  },
  error = function(e){
    message('Error message from Spark')
    print(e)
  }
)

rescue_raw %>% sparklyr::select(animal_type) %>% distinct() %>% dplyr::arrange(animal_type) %>% print(n=27)
