options(warn = -1)
library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
  master = "local[2]",
  app_name = "ons-spark",
  config = sparklyr::spark_config(),
  )

config <- yaml::yaml.load_file("ons-spark/config.yaml")

rescue_df <- sparklyr::spark_read_csv(sc,
                                     path = config$rescue_path_csv,
                                     header = TRUE,
                                     infer_schema = TRUE)

sparklyr::sdf_schema(rescue_df)

custom_schema = list(IncidentNumber = "character",
                     DateTimeOfCall = "date",
                     CalYear = "integer",
                     TypeOfIncident = "character")

rescue_df <- sparklyr::spark_read_csv(sc,
                                     path = config$rescue_path_csv,
                                     header = TRUE,
                                     infer_schema = FALSE,
                                     columns = custom_schema)

sparklyr::sdf_schema(rescue_df)

sparklyr::spark_write_csv(rescue_df,
                         paste0(config$temp_outputs, "animal_rescue_r.csv"),
                         header = TRUE,
                         mode = 'overwrite')


rescue_df <- sparklyr::spark_read_parquet(sc, path = config$rescue_path)

sparklyr::spark_write_parquet(rescue_df, paste0(config$temp_outputs, "animal_rescue_r.parquet"), mode = 'overwrite')

rescue_df <- sparklyr::spark_read_orc(sc, path = config$rescue_path_orc)

sparklyr::spark_write_orc(rescue_df, paste0(config$temp_outputs, "animal_rescue_r.orc"), mode = 'overwrite')

library(sparkavro)

animal_rescue = sparkavro::spark_read_avro(sc, "animal_rescue", config$rescue_path_avro)

sparkavro::spark_write_avro(animal_rescue, paste0(config$temp_outputs, "animal_rescue.avro"), mode = "overwrite")

rescue_df <- sparklyr::sdf_sql(sc, "SELECT * FROM train_tmp.animal_rescue")

sparklyr::spark_write_table(rescue_df, name = "train_tmp.animal_rescue_temp", mode = 'overwrite')

repartition_path <- paste0(config$checkpoint_path, "/rescue_by_year.parquet")
sparklyr::spark_write_parquet(rescue_df, 
                              repartition_path,
                              mode='overwrite',
                              partition_by='cal_year')

cmd <- paste0("hdfs dfs -ls -C ", repartition_path)
system(cmd)

cmd <- paste0("hdfs dfs -rm -r -skipTrash ", repartition_path)
system(cmd)
