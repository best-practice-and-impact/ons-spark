options(warn = -1)
library(sparklyr)
library(dplyr)

options(pillar.max_dec_width = 14)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "data-types",
    config = sparklyr::spark_config())

config <- yaml::yaml.load_file("ons-spark/config.yaml")

numeric_df <- sparklyr::sdf_seq(sc, 0, 4) %>%
    sparklyr::mutate(
        really_big_number_double = id * 10**9,
        small_number_double = id / 10)

pillar::glimpse(numeric_df)
print(numeric_df)

dates <- sparklyr::sdf_copy_to(sc, data.frame(
        "month_name" = c("March", "April", "May"),
        "example_date" = lubridate::ymd(
            c("2022-03-01", "2022-04-01", "2022-05-01")))) %>%
    sparklyr::mutate(example_timestamp = to_timestamp(example_date))

pillar::glimpse(dates)
print(dates)

rescue_from_parquet <- sparklyr::spark_read_parquet(sc, config$rescue_path) %>%
    sparklyr::select(incident_number, date_time_of_call, cal_year, fin_year)

pillar::glimpse(rescue_from_parquet)

rescue_from_csv <- sparklyr::spark_read_csv(sc, config$rescue_path_csv, header=TRUE, infer_schema=TRUE) %>%
    sparklyr::select(IncidentNumber, DateTimeOfCall, CalYear, FinYear)
    
pillar::glimpse(rescue_from_csv)

rescue_schema <- list(
    incident_number = "character",
    date_time_of_call = "character",
    cal_year = "integer",
    fin_year = "character"
)

rescue_from_csv_schema <- sparklyr::spark_read_csv(sc,
                                                   config$rescue_path_csv,
                                                   columns=rescue_schema,
                                                   infer_schema=FALSE)

pillar::glimpse(rescue_from_csv_schema)

casted_df <- sparklyr::sdf_seq(sc, 0, 4) %>%
    sparklyr::mutate(id_double = double(id))

pillar::glimpse(casted_df)
print(casted_df)
