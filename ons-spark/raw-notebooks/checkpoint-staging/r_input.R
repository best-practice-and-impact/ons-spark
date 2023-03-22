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
    mutate(!!column_name := case_when(!!as.symbol(prev_column) > i ~ !!as.symbol(prev_column) ))
  
  
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
