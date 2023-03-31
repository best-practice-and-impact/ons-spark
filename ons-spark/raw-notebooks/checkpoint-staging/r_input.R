options(warn = -1) 
library(sparklyr)
library(dplyr)
library(DBI)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "checkpoint",
    config = sparklyr::spark_config())


set.seed(42)
new_cols <- 12
num_rows <- 10^3

start_time <- Sys.time()

df = sparklyr::sdf_seq(sc, 1, num_rows) %>%
    sparklyr::mutate(col_0 = ceiling(rand()*new_cols))

for (i in 1: new_cols)
{
  column_name = paste0('col_', i)
  prev_column = paste0('col_', i-1)
  df <- df %>%
    mutate(
      !!column_name := case_when(
        !!as.symbol(prev_column) > i ~ !!as.symbol(prev_column)))
  
}

df %>%
    head(10)%>%
    collect()%>%
    print()

end_time <- Sys.time()
time_taken = end_time - start_time

cat("Time taken to create DataFrame", time_taken)

 

spark_disconnect(sc)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "checkpoint",
    config = sparklyr::spark_config())


config <- yaml::yaml.load_file("/home/cdsw/ons-spark/config.yaml")

spark_set_checkpoint_dir(sc, config$checkpoint_path)

 
start_time <- Sys.time()

df1 = sparklyr::sdf_seq(sc, 1, num_rows) %>%
    sparklyr::mutate(col_0 = ceiling(rand()*new_cols))

for (i in 1: new_cols)
{
  column_name = paste0('col_', i)
  prev_column = paste0('col_', i-1)
  df1 <- df1 %>%
    mutate(
    !!column_name := case_when(
        !!as.symbol(prev_column) > i ~ !!as.symbol(prev_column) ))
  
  
  if (i %% 3 == 0) 
  {
    sdf_checkpoint(df1, eager= TRUE)
  }
}

df1 %>%
    head(10)%>%
    collect()%>%
    print()

end_time <- Sys.time()
time_taken = end_time - start_time


cat("Time taken to create DataFrame: ", time_taken)
 
cmd <- paste0("hdfs dfs -rm -r -skipTrash ", config$checkpoint_path)
p <- system(command = cmd)

spark_disconnect(sc)
 
library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "staging_tables",
    config = sparklyr::spark_config())
 
animal_rescue_csv = config$rescue_path_csv

df = sparklyr::spark_read_csv(sc,
                              path=animal_rescue_csv,
                              header=TRUE,
                              infer_schema=TRUE)

df %>%
    sparklyr::select( 
             -("WardCode"), 
             -("BoroughCode"), 
             -("Easting_m"), 
             -("Northing_m"), 
             -("Easting_rounded"), 
             -("Northing_rounded"), 
            "EngineCount" = "PumpCount",
            "Description" = "FinalDescription",
            "HourlyCost" = "HourlyNotionalCostGBP",
            "TotalCost" = "IncidentNotionalCostGBP",
            "OriginOfCall" = "OriginofCall",
            "JobHours" = "PumpHoursTotal",
            "AnimalGroup" = "AnimalGroupParent") %>%

    sparklyr::mutate(DateTimeOfCall = to_date(DateTimeOfCall, "dd/MM/yyyy")) %>%
    dplyr::arrange(desc(IncidentNumber)) %>%
    head(3) %>%
    sparklyr::collect() %>%
    print() 

explain(df)

username <- Sys.getenv('HADOOP_USER_NAME')
invisible(sdf_register(df, 'df'))
sql <- paste0('DROP TABLE IF EXISTS train_tmp.staging_example_', username)
dbExecute(sc, sql)

tbl_change_db(sc, "train_tmp")
table_name <- paste0('staging_example_', username) 
spark_write_table(df, name = table_name)

df <- spark_read_table(sc, table_name, repartition = 0)

df %>%
    head(3) %>%
    sparklyr::collect() %>%
    print()


explain(df)
