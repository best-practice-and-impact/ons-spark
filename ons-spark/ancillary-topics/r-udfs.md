## R UDFs

When coding with Spark, you will generally want to try and use sparklyr or Spark SQL functions wherever possible. However, there are instances where there may not be a Spark function available to do what you need. In cases such as this, one option is to use an R user-defined function (UDF).

The sparklyr package comes with the `spark_apply()` function, which is designed for this purpose. It allows you to apply a function written in R to a Spark object (e.g. a Spark DataFrame). In order for this to work, you need to ensure that your cluster administrator, cloud provider, or you (in the case of running Spark locally) has configured your cluster by installing either:

- R on every node
- Apache Arrow on every node (requires Spark 2.3 or later)

Although only one of these is required to make use of `spark_apply()`, in practice, it is recommended to use `spark_apply()` with Apache Arrow, as it provides significant performance improvements. In general, R UDFs should be considered a 'last resort', where there is no equivalent functionality compatible with Spark and an R function needs to be run on a large Spark DataFrame. They are generally very inefficient due to the need to serialise and deserialise the data between Spark and R. Apache Arrow speeds this up and allows data to be transferred more efficiently between Spark and R, but even with these improvements `spark_apply()` is still best avoided if an alternative can be found. For a detailed explanation of why `spark_apply()` is more efficient with Apache Arrow, see the [Distributed R chapter of Mastering Spark with R](https://therinspark.com/distributed.html#cluster-requirements). 

It is sometimes not possible to avoid using a UDF. For example, if we want to use a specialised statistical R package on our data that has no Spark equivalent. The examples below demonstrate how to use `spark_apply()`. They are written assuming that the user does not have Apache Arrow available, but if your configuration enables the use of Arrow the examples can be adapted by making sure you have the `arrow` package installed (`install.packages("arrow")`) and simply loading the `arrow` library (`library(arrow)`) before establishing the spark connection.

### Example 1: Simple UDF and loading packages onto worker nodes

This example represents a simple UDF just to demonstrate how `spark_apply()` can be used. In practice, it is far too simple to necessitate using a UDF - we could definitely do this with sparklyr code instead of R code!

The example UDF below will make use of the `mutate()` function from he `dplyr` package. In order for Spark to run our UDF, we need to make sure that the packages we need are installed on each worker node in our Spark cluster once we have set up our session. To do this, we need to pass the location of our R package library to the Spark cluster by including the `spark.r.libpaths` setting as shown below:

```{code-tab} R
library(dplyr)
library(sparklyr)

# Set up our Spark configuration - including our library path
default_config <- spark_config()
default_config["spark.r.libpaths"] <- .libPaths()[1]

sc <- spark_connect(
  master = "local[2]",
  app_name = "r-udfs",
  config = default_config)
  
```

You can replace `.libPaths()[1]` with the full path to your main R package library in quotation marks if you need to. If you are unsure where this is, running `.libPaths()` in your session console will show you a list of library locations that R searches for installed packages.

We have told Spark where to look for R packages, but they are not yet installed on the worker nodes. The first time we run `spark_apply()` in our Spark session, any packages found in the specified library path will be copied over to the Spark cluster. As a result, the most efficient way to load packages onto the cluster is to run a 'dummy' UDF on a small dataset before running our actual function. This ensures that the packages are loaded and ready for use before Spark attempts to perform more complex operations on our data. We can define and run our dummy UDF as shown:

````{tabs}
```{code-tab} R
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

```
``` plaintext
# Source:   table<`sparklyr_tmp__78b5fc02_a3fa_40b2_98d8_a156efa9cf69`> [8 x 1]
# Database: spark_connection
  result   
  <chr>    
1 dplyr    
2 stats    
3 graphics 
4 grDevices
5 utils    
6 datasets 
7 methods  
8 base
```
````    

Confusingly, to load packages using this method, note that we have had to set the `packages` argument in `spark_apply()` to `FALSE`. This is because there is another method to load packages on to the cluster, using [`spark_apply_bundle()`](https://www.rdocumentation.org/packages/sparklyr/versions/1.8.5/topics/spark_apply_bundle). This function is designed to bundle all packages into a .tar file ready to pass to the Spark cluster. Setting the `packages` argument to `TRUE` prompts Spark to search for this .tar file and extract the relevant packages. However, this method is not easily generalisable to different system setups and may not always be possible using a given configuration (e.g. it does not seem to work with S3 bucket storage). For this reason, this guide demonstrates using the alternative approach outlined above to use packages with `spark_apply()`.  

Now we have loaded our R packages on to the cluster, we can set up and run our UDF. For this example we can define a dummy Spark dataframe to work with:

````{tabs}
```{code-tab} R
# Set up a dummy Spark dataframe
sdf <- sparklyr:::sdf_seq(sc, -7, 8, 2) |>
       sparklyr::mutate(half_id = id / 2) |>
       sparklyr::select(half_id)

# View the data    
sdf |>
    sparklyr::collect() |>
    print()
```
``` plaintext
# A tibble: 8 × 1
  half_id
    <dbl>
1    -3.5
2    -2.5
3    -1.5
4    -0.5
5     0.5
6     1.5
7     2.5
8     3.5
```

Now we need to set up our UDF. R UDFs are defined using the same syntax as regular R functions. However, you may need to ensure that any calls to functions from loaded R packages are written as `<package_name>::<package_function>` to ensure that Spark can find the specified function (e.g. `dplyr::mutate()` in the example below):

```{code-tab} R
round_udf <- function(df) {
    x <- df |>
        dplyr::mutate(rounded_col = round(half_id)) # need to specify dplyr package
    return(x)
}
```

This defines a simple function to create a new column `rounded_col` by rounding the `half_id` column in our Spark dataframe. Of course in reality, we would never need to use a UDF for something so simple as this can be fully done in Spark, but this serves as a simplified example of how R UDFs can be used. We are now ready to use `spark_apply` to apply the `round_udf()` function to our dataframe:

````{tabs}
```{code-tab} R
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

```
``` plaintext
# Source:   table<`sparklyr_tmp__de87c853_8f23_434d_892e_6e9c99858e58`> [8 x 2]
# Database: spark_connection
  half_id rounded_col
    <dbl>       <dbl>
1    -3.5          -4
2    -2.5          -2
3    -1.5          -2
4    -0.5           0
5     0.5           0
6     1.5           2
7     2.5           2
8     3.5           4
```
````

Note that we have included a `columns` argument in the above example to enable us to specify a schema for the dataframe. It is a good idea to do this where possible as it speeds up the running of the UDF. If no schema is specified, Spark will need to identify it before applying the UDF which can slow things down somewhat. 

The above example can easily be adapted for any R function which takes a single argument (the dataframe). However, most functions will require additional arguments to be passed in. We will look at how to do this with the next example using the `context` argument of [`spark_apply`](https://search.r-project.org/CRAN/refmans/sparklyr/html/spark_apply.html). 

### Example 2: Passing additional arguments to a UDF

First, we need to set up our Spark connection and load any required packages onto the cluster. Please note that if you don't already have these packages installed you will need to install them **before** setting up your Spark connection so they can be found in your library and copied over to the cluster.

````{tabs}
```{code-tab} R
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
# so it is more efficient to do this 'on' a minimal sdf (of length 1) first

sdf_len(sc, 1) |> sparklyr::spark_apply(f = libload,
                packages = FALSE)
                
```
``` plaintext
# Source:   table<`sparklyr_tmp__dee5d73a_9102_4c57_be2a_d9c2a4d5f12e`> [10 x 1]
# Database: spark_connection
   result   
   <chr>    
 1 rlang    
 2 janitor  
 3 dplyr    
 4 stats    
 5 graphics 
 6 grDevices
 7 utils    
 8 datasets 
 9 methods  
10 base 
```
````

Next, we will generate a Spark dataframe to test our function on and define a more complex version of our rounding UDF from example 1. In this example, we've assumed we want to pass in two extra values to the function; `col`, which will be the name of the column we want to apply the rounding to, and `precision`, which is the number of decimal places we want to round to. However, all of the arguments you need to pass to your UDF need to be included in the `spark_apply` `context`. As a result, we need to define our UDF as having just two arguments; the dataframe, and the context, which is passed as a `list` of all the required UDF arguments. Then we can split our additional arguments out of the list and into individual objects inside the function definition as shown below.

Note that this time, when defining the function we are using both `mutate` from the `dplyr` package and `sym` from `rlang`, so we have specified both of the package names in our function definition. The `!!rlang::sym(col)` converts the column name (which is passed into the function as a string) to a column object inside the function.

```{code-tab} R
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

```

Now we can run the UDF on our Spark dataframe as before, passing values to the `col` and `precision` arguments inside the `multi_arg_udf` function from the `context` list as shown below. We'll run it a couple of times, passing a different value to `precision` just to confirm it works as expected.


````{tabs}
```{code-tab} R

# Run the function on our data using spark_apply and passing the `col` and `precision` arguments as a list using 'context'
multi0 <- sparklyr::spark_apply(sdf, multi_arg_udf, context = list(col = "half_id",
                                                             precision = 0), packages = FALSE)
                                                             

# Run the function on our data again using spark_apply, this time with a different `precision` using 'context'         
multi1 <- sparklyr::spark_apply(sdf, multi_arg_udf, context = list(col = "half_id",
                                                             precision = 1), packages = FALSE)               

# View the result with `col` = "half_id" and precision = 0
multi0

# View the result with `col` = "half_id" and precision = 1
multi1

```
``` plaintext
# Source:   table<`sparklyr_tmp__dbf96333_18ac_472a_8177_97299e5b1a18`> [8 x 2]
# Database: spark_connection
  half_id rounded_col
    <dbl>       <dbl>
1    -3.5          -4
2    -2.5          -2
3    -1.5          -2
4    -0.5           0
5     0.5           0
6     1.5           2
7     2.5           2
8     3.5           4

# Source:   table<`sparklyr_tmp__cec23e14_8a06_49e1_9104_c9574d0d5a97`> [8 x 2]
# Database: spark_connection
  half_id rounded_col
    <dbl>       <dbl>
1    -3.5        -3.5
2    -2.5        -2.5
3    -1.5        -1.5
4    -0.5        -0.5
5     0.5         0.5
6     1.5         1.5
7     2.5         2.5
8     3.5         3.5

```
````

Note that even if you only have one additional argument to pass into your UDF, you will still need to pass this in via the `context` argument as a list, but with only a single element (i.e. `context = list(arg1 = value1)`). You can define any number of additional arguments this way, provided you remember to split each one out of the context list inside your UDF. 

These first two examples are very simple and we have not been paying any attention to partitioning in our data. However, for real, large, partitioned datasets we need to think carefully about how to partition our data, since `spark_apply()` receives each partition (rather than the whole dataset) and then applies the function on each one. The example in the next section shows how care must be taken with partioning data in order to get reliable results from `spark_apply()`.

### Example 3: Partitions and the `group_by` argument

In this example, we will read in some partitioned data and use `spark_apply()` to perform an operation on it. We can set up our session and add packages to the cluster in the exact same way as we did before:

````{tabs}
```{code-tab} R
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



```
``` plaintext

# Source:   table<`sparklyr_tmp__52c88102_dd5c_4c0a_a335_ff45d4301987`> [10 x 1]
# Database: spark_connection
   result   
   <chr>    
 1 rlang    
 2 janitor  
 3 dplyr    
 4 stats    
 5 graphics 
 6 grDevices
 7 utils    
 8 datasets 
 9 methods  
10 base   

```

````

Next, we can read in the data we want to analyse. The `repartition` argument has been set to 10 just to ensure there are multiple partitions in the data.

```` {tabs}
```{code-tab} R
config <- yaml::yaml.load_file("ons-spark/config.yaml")

# read in data
rescue <- sparklyr::spark_read_parquet(sc, config$rescue_clean_path)

# preview data
dplyr::glimpse(rescue)

```

``` plaintext
Rows: ??
Columns: 21
Database: spark_connection
$ incident_number            <chr> "047943-18042017", "150426131", "51596131",…
$ datetimeofcall             <chr> "18/04/2017 14:33", "30/10/2013 14:04", "25…
$ cal_year                   <chr> "2017", "2013", "2013", "2012", "2013", "20…
$ finyear                    <chr> "2017/18", "2013/14", "2013/14", "2012/13",…
$ typeofincident             <chr> "Special Service", "Special Service", "Spec…
$ engine_count               <chr> "1.0", "1.0", "1.0", "1.0", "1.0", "1.0", "…
$ job_hours                  <chr> "1.0", "1.0", "1.0", "1.0", "1.0", "1.0", "…
$ hourly_cost                <chr> "328", "290", "290", "260", "290", "333", "…
$ total_cost                 <chr> "328.0", "290.0", "290.0", "260.0", "290.0"…
$ finaldescription           <chr> "Bird Trapped In Chimney", "Cat Trapped Bet…
$ animal_group               <chr> "Bird", "Cat", "Dog", "Bird", "Unknown - Do…
$ originofcall               <chr> "Person (land Line)", "Person (land Line)",…
$ propertytype               <chr> "House - Single Occupancy ", "Private Garde…
$ propertycategory           <chr> "Dwelling", "Non Residential", "Dwelling", …
$ specialservicetypecategory <chr> "Animal Rescue From Below Ground", "Other A…
$ specialservicetype         <chr> "Animal Rescue From Below Ground - Bird", "…
$ ward                       <chr> "College Park And Old Oak", "Abbey Wood", "…
$ borough                    <chr> "Hammersmith And Fulham", "Greenwich", "Wes…
$ stngroundname              <chr> "Acton", "Plumstead", "Soho", "Mill Hill", …
$ postcodedistrict           <chr> "W12", "Se2", "W1f", "Ha8", "N1", "En3", "N…
$ incident_duration          <chr> "1.0", "1.0", "1.0", "1.0", "1.0", "1.0", "…

```
````

- Simplify dataset and convert necessary types
- Set up UDF
- Run UDF and show how output is influenced by partitioning

- Next example of using 'group_by' to group and partition

