
library(dplyr)
library(sparklyr)

# Set up our Spark configuration - including our library path
default_config <- spark_config()
default_config["spark.r.libpaths"] <- .libPaths()[1]

sc <- spark_connect(
  master = "local[2]",
  app_name = "r-udfs",
  config = default_config)
  


# Define dummy UDF 'libload' which loads our required function libraries
# You could just define an empty function here, but this forces Spark to output 
# a list of libraries loaded on the cluster so we can see it has worked 

libload <- function() {
  library(dplyr)
}

# All packages will be loaded on to worker nodes during first call to spark_apply in session
# so it is more efficient to do this 'on' a minimal sdf (of length 1) first

sdf_len(sc, 1) |> sparklyr::spark_apply(f = libload,
                packages = FALSE)



# Set up a dummy Spark dataframe
sdf <- sparklyr:::sdf_seq(sc, -7, 8, 2) |>
       sparklyr::mutate(half_id = id / 2) |>
       sparklyr::select(half_id)

# View the data    
sdf |>
    sparklyr::collect() |>
    print()


round_udf <- function(df) {
    x <- df |>
        dplyr::mutate(rounded_col = round(half_id)) # need to specify dplyr package
    return(x)
}


rounded <- sparklyr::spark_apply(sdf,
   f = round_udf,
   # Specify schema - works faster if you specify a schema
   columns = c(half_id = "double",
               rounded_col = "double"),
   packages = FALSE)

# View the resulting Spark dataframe
rounded

# disconnect the Spark session
spark_disconnect(sc)



# Set up our Spark configuration - including our library path
default_config <- spark_config()
default_config["spark.r.libpaths"] <- .libPaths()[1]

sc <- spark_connect(
  master = "local[2]",
  app_name = "r-udfs",
  config = default_config)


# Define the libload function to load our required packages onto the cluster
libload <- function() {
  library(dplyr)
  library(janitor)
  library(rlang)

}

# All packages will be loaded on to worker nodes during first call to spark_apply in session
# so it is more efficient to do this on a minimal sdf (of length 1) first

sdf_len(sc, 1) |> sparklyr::spark_apply(f = libload,
                packages = FALSE)
                


# Set up a dummy Spark dataframe
sdf <- sparklyr:::sdf_seq(sc, -7, 8, 2) |>
       sparklyr::mutate(half_id = id / 2) |>
       sparklyr::select(half_id)
       
# Define our UDF to take multiple arguments passed into a list named `context`
multi_arg_udf <- function(df, context) {

   col <- context$col               # Split out the `col` argument from the list of arguments in `context`
   precision <- context$precision   # Split out the `precision` argument from the list of arguments in `context`
   
   x <- df |>
        dplyr::mutate(rounded_col = round(!!rlang::sym(col), precision)) #need to specify both rlang and dplyr packages
    return(x)
}




# Run the function on our data using spark_apply and passing the `col` and `precision` arguments as a list using 'context'
multi0 <- sparklyr::spark_apply(sdf, multi_arg_udf, context = list(col = "half_id",
                                                             precision = 0), 
                                                             columns = c(half_id = "double",
               rounded_col = "double"),
               packages = FALSE)
                                                             

# Run the function on our data again using spark_apply, this time with a different `precision` using 'context'         
multi1 <- sparklyr::spark_apply(sdf, multi_arg_udf, context = list(col = "half_id",
                                                             precision = 1), 
                                                             columns = c(half_id = "double",
               rounded_col = "double"),
               packages = FALSE)               

# View the result with `col` = "half_id" and precision = 0
multi0

# View the result with `col` = "half_id" and precision = 1
multi1

spark_disconnect(sc)


library(sparklyr)
library(dplyr)

# Set up our Spark configuration - including our library path
default_config <- spark_config()
default_config["spark.r.libpaths"] <- .libPaths()[1]

# open a spark connection
sc <- spark_connect(
  master = "local[2]",
  app_name = "r-udfs",
  config = default_config)


# Define the libload function to load our required packages onto the cluster
libload <- function() {
  library(dplyr)
  library(janitor)
  library(rlang)

}

# pre-load packages onto the cluster using a dummy spark dataframe and function
sdf_len(sc, 1) |> sparklyr::spark_apply(f = libload,
                packages = FALSE)





config <- yaml::yaml.load_file("ons-spark/config.yaml")

# read in and partition data
rescue <- sparklyr::spark_read_parquet(sc, config$rescue_clean_path, repartition = 5)

# preview data
dplyr::glimpse(rescue)



# Drop unwanted columns and convert cal_year and total_cost to numeric
rescue_tidy <- rescue |>
  dplyr::select(incident_number, cal_year, total_cost, animal_group, borough) |>
  dplyr::mutate(across(c(cal_year, total_cost),
                ~as.numeric(.)))

glimpse(rescue_tidy)



# check number of partitions
num_part <- sdf_num_partitions(rescue_tidy)

num_part

# Loop over partitions and view the top few rows of each 
# Remember that partitions are numbered from zero so we need to loop over 0 to `num_part-1`
for(i in 0:num_part-1) {
  rescue_tidy |>
    filter(spark_partition_id() == i) |>
    print(head(10))
}



year_cost <- function(df, context) {

  # Split out the arguments from `context`
  animal <- context$animal_group
  borough <- context$borough
  
  x <- df |>
    dplyr::filter(animal_group == animal,
                 borough == borough) |>
    dplyr::group_by(cal_year) |>
    dplyr::summarise(total_yearly_cost_for_group = sum(total_cost, na.rm = TRUE))
  
  return(x)
  
}


# Apply our UDF
hackney_cats <- sparklyr::spark_apply(rescue_tidy,
                          year_cost, 
                          context = list(animal_group = "Cat", 
                                         borough = "Hackney"),
                          columns = c(cal_year = "double", 
                                      total_yearly_cost_for_group = "double"),
                          packages = FALSE)

# Total cost by year for Cats in Hackney
hackney_cats |> 
  arrange(cal_year) |> 
  print(n = 55)


year_cost_no_group <- function(df, context) {
  animal <- context$animal_group
  borough <- context$borough
  
  x <- df |>
    dplyr::filter(animal_group == animal,
                 borough == borough) |>
    dplyr::summarise(total_yearly_cost_for_group = sum(total_cost, na.rm = TRUE))
  
  return(x)
  
}

hackney_cats_year <- sparklyr::spark_apply(rescue_tidy,
                          group_by = "cal_year",
                          year_cost_no_group, 
                          context = list(animal_group = "Cat", 
                                         borough = "Hackney"),
                          columns = c(cal_year = "double", 
                                      total_yearly_cost_for_group = "double"),
                          packages = FALSE)


hackney_cats_year |> 
  arrange(cal_year) |> 
  print(n = 11)
