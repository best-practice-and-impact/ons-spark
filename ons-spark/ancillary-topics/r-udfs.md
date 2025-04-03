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

```{code-tab} r R
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
```{code-tab} r R
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
```{code-tab} plaintext R output
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
```{code-tab} r R
# Set up a dummy Spark dataframe
sdf <- sparklyr:::sdf_seq(sc, -7, 8, 2) |>
       sparklyr::mutate(half_id = id / 2) |>
       sparklyr::select(half_id)

# View the data    
sdf |>
    sparklyr::collect() |>
    print()
```
```{code-tab} plaintext R output
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

```{code-tab} r R
round_udf <- function(df) {
    x <- df |>
        dplyr::mutate(rounded_col = round(half_id)) # need to specify dplyr package
    return(x)
}
```

This defines a simple function to create a new column `rounded_col` by rounding the `half_id` column in our Spark dataframe. Of course in reality, we would never need to use a UDF for something so simple as this can be fully done in Spark, but this serves as a simplified example of how R UDFs can be used. We are now ready to use `spark_apply` to apply the `round_udf()` function to our dataframe:

````{tabs}
```{code-tab} r R
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
```{code-tab} plaintext R output
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
```{code-tab} r R
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
                
```
```{code-tab} plaintext R output
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

```{code-tab} r R
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
```{code-tab} r R

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
```{code-tab} plaintext R output
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
```{code-tab} r R
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
```{code-tab} plaintext R output

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

Next, we can read in the data we want to analyse. The `repartition` argument in `spark_read_parquet` has been set to 5 just to ensure there are multiple partitions in the data.

```` {tabs}
```{code-tab} r R
config <- yaml::yaml.load_file("ons-spark/config.yaml")

# read in and partition data
rescue <- sparklyr::spark_read_parquet(sc, config$rescue_clean_path, repartition = 5)

# preview data
dplyr::glimpse(rescue)

```

```{code-tab} plaintext R output
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
We can simplify this dataset a bit for our example and convert values that have the incorrect datatype listed:

````{tabs}
```{code-tab} r R
rescue_tidy <- rescue |>
  dplyr::select(incident_number, cal_year, total_cost, animal_group, borough) |>
  dplyr::mutate(across(c(cal_year, total_cost),
                ~as.numeric(.)))

glimpse(rescue_tidy)

```
```{code-tab} plaintext R output
Rows: ??
Columns: 5
Database: spark_connection
$ incident_number <chr> "025074-28022018", "047943-18042017", "089899-06072017…
$ cal_year        <dbl> 2018, 2017, 2017, 2013, 2011, 2013, 2013, 2012, 2009, …
$ total_cost      <dbl> 656, 328, 328, 290, 260, 290, 260, 260, 260, 290, 652,…
$ animal_group    <chr> "Dog", "Bird", "Fox", "Cat", "Cat", "Dog", "Dog", "Bir…
$ borough         <chr> "Redbridge", "Hammersmith And Fulham", "Hammersmith An…
```
````
If we check how this data has been partitioned, we can see that Spark has just taken arbitrary cuts of the data and split it across the partitions accordingly. There is no commonality between the data that is on one partition compared withthe next (for example, we don't have all of one type of animal or one calendar year on one partition and the rest on another).

````{tabs}
```{code-tab} r R
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

```
```{code-tab} plaintext R output
num_part
[1] 5

# Source:   SQL [0 x 5]
# Database: spark_connection
# ℹ 5 variables: incident_number <chr>, cal_year <dbl>, total_cost <dbl>,
#   animal_group <chr>, borough <chr>
# Source:   SQL [?? x 5]
# Database: spark_connection
   incident_number cal_year total_cost animal_group                     borough 
   <chr>              <dbl>      <dbl> <chr>                            <chr>   
 1 025074-28022018     2018        656 Dog                              Redbrid…
 2 047943-18042017     2017        328 Bird                             Hammers…
 3 089899-06072017     2017        328 Fox                              Hammers…
 4 150426131           2013        290 Cat                              Greenwi…
 5 21535111            2011        260 Cat                              Lambeth 
 6 51596131            2013        290 Dog                              Westmin…
 7 20695131            2013        260 Dog                              Waltham…
 8 157207121           2012        260 Bird                             Barnet  
 9 171757091           2009        260 Dog                              Brent   
10 174592131           2013        290 Unknown - Domestic Animal Or Pet Islingt…
# ℹ more rows
# ℹ Use `print(n = ...)` to see more rows
# Source:   SQL [?? x 5]
# Database: spark_connection
   incident_number cal_year total_cost animal_group          borough            
   <chr>              <dbl>      <dbl> <chr>                 <chr>              
 1 45633131            2013        290 Unknown - Wild Animal Ealing             
 2 60136101            2010        260 Cat                   Brent              
 3 2815151             2015        295 Cat                   Richmond Upon Tham…
 4 108909151           2015        298 Bird                  Sutton             
 5 098557-29072016     2016        326 Cat                   Southwark          
 6 34886101            2010        260 Dog                   Hammersmith And Fu…
 7 76500141            2014        295 Fox                   Croydon            
 8 39175101            2010        260 Cat                   Lewisham           
 9 64733101            2010        260 Bird                  Brent              
10 120553151           2015        298 Bird                  Ealing             
# ℹ more rows
# ℹ Use `print(n = ...)` to see more rows
# Source:   SQL [?? x 5]
# Database: spark_connection
   incident_number cal_year total_cost animal_group                     borough 
   <chr>              <dbl>      <dbl> <chr>                            <chr>   
 1 065106-29052016     2016        326 Bird                             Merton  
 2 051231-24042017     2017        328 Cat                              Tower H…
 3 91465121            2012        260 Cat                              Islingt…
 4 160750-18112015     2015        298 Fox                              City Of…
 5 89323101            2010        260 Cat                              Tower H…
 6 70683141            2014        295 Unknown - Domestic Animal Or Pet Hackney 
 7 145674101           2010        260 Bird                             Camden  
 8 132003141           2014        295 Cat                              Havering
 9 117151141           2014        295 Cat                              Bromley 
10 130721101           2010        260 Cat                              Lewisham
# ℹ more rows
# ℹ Use `print(n = ...)` to see more rows
# Source:   SQL [?? x 5]
# Database: spark_connection
   incident_number cal_year total_cost animal_group borough             
   <chr>              <dbl>      <dbl> <chr>        <chr>               
 1 28706141            2014        290 Cat          Lambeth             
 2 55883141            2014        295 Bird         Redbridge           
 3 103661-08082016     2016        326 Deer         Waltham Forest      
 4 5489151             2015        295 Cat          Southwark           
 5 111131111           2011        260 Cat          Barnet              
 6 133497111           2011        260 Fox          Southwark           
 7 36795131            2013        260 Dog          Waltham Forest      
 8 024137-26022017     2017        326 Cat          Richmond Upon Thames
 9 197524111           2011        520 Bird         Southwark           
10 93925091            2009        260 Bird         Barnet              
# ℹ more rows
# ℹ Use `print(n = ...)` to see more rows
# Source:   SQL [?? x 5]
# Database: spark_connection
   incident_number cal_year total_cost animal_group borough             
   <chr>              <dbl>      <dbl> <chr>        <chr>               
 1 63301141            2014        295 Bird         Tower Hamlets       
 2 067744-29052018     2018        333 Cat          Richmond Upon Thames
 3 16711111            2011        260 Dog          Islington           
 4 095023-14072017     2017        328 Bird         Hackney             
 5 72228101            2010        780 Lamb         Enfield             
 6 149174101           2010        260 Bird         Kingston Upon Thames
 7 013770-04022016     2016        596 Cat          Barnet              
 8 17302141            2014        290 Cat          Enfield             
 9 141747131           2013        290 Cat          Bromley             
10 74900151            2015        298 Fox          Croydon             
# ℹ more rows
# ℹ Use `print(n = ...)` to see more rows

```
````
Let us now try running a UDF on this data. Again, this example is not a good use case for a UDF as the code run could easily be run in `sparklyr` instead, but will serve to illustrate how partioning and grouping works using R UDFs.

We will write a UDF that will allow us to aggregate the data based on user input for a particular `animal_group` and `borough`, to generate a summary table of the total cost by `cal_year` of a particular animal type in a given area. 

````{tabs} 
```{code-tab} r R
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
```
````

Now, we can try running it using `spark_apply` on our data. Let us say that we want to know the total cost by calendar year of rescuing cats in Hackney:

````{tabs}
```{code-tab} r R
# Apply our UDF
hackney_cats <- sparklyr::spark_apply(rescue_tidy,
                          year_cost, 
                          context = list(animal_group = "Cat", 
                                         borough = "Hackney"),
                          packages = FALSE)

# Total cost by year for Cats in Hackney
hackney_cats |> 
  arrange(cal_year) |> 
  print(n = 55)
```
```{code-tab} plaintext R output
# Source:     SQL [55 x 2]
# Database:   spark_connection
# Ordered by: cal_year
   cal_year total_yearly_cost_for_group
      <dbl>                       <dbl>
 1     2009                       17360
 2     2009                       14525
 3     2009                       15525
 4     2009                       14510
 5     2009                       15025
 6     2010                       22100
 7     2010                       16380
 8     2010                       17160
 9     2010                       16120
10     2010                       17160
11     2011                       20020
12     2011                       15860
13     2011                       16120
14     2011                       21060
15     2011                       16380
16     2012                       15340
17     2012                       17940
18     2012                       18200
19     2012                       17160
20     2012                       17680
21     2013                       18520
22     2013                       21010
23     2013                       22400
24     2013                       16460
25     2013                       19390
26     2014                       25590
27     2014                       17065
28     2014                       17685
29     2014                       20255
30     2014                       19705
31     2015                       19939
32     2015                       20240
33     2015                       13094
34     2015                       16667
35     2015                       16634
36     2016                       25930
37     2016                       15396
38     2016                       21534
39     2016                       22978
40     2016                       27048
41     2017                       22924
42     2017                       19336
43     2017                       17032
44     2017                       18662
45     2017                       20944
46     2018                       24915
47     2018                       19612
48     2018                       21580
49     2018                       20919
50     2018                       25581
51     2019                         666
52     2019                        2331
53     2019                        1332
54     2019                         999
55     2019                         666
```
````

Something has clearly gone a bit wrong here! Instead of returning an 11 row table, with one total cost for each year, we instead have 55 rows, with 5 total costs per year. This is a result of the arbitrary partitioning applied to the data when we read it in. Since `spark_apply` only receives data from individual partitions and applies the UDF on each one separately, the output has not been recombined as if the function has been applied to the entire dataset. Instead, we have a total cost per year for each of the 5 partitions!

A better approach to running this as a UDF would be to use the [`group_by`](https://spark.posit.co/packages/sparklyr/latest/reference/spark_apply.html) argument in `spark_apply` to both group the data by `cal_year` and partition it accordingly. We will need to also adjust our UDF as there is no need to include `group_by(cal_year)` there as well. Making these changes produces the following output:

````{tabs}
```{code-tab} r R
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

                          packages = FALSE)


hackney_cats_year |> 
  arrange(cal_year) |> 
  print(n = 11)
```

```{code-tab} plaintext R output
# Source:     SQL [11 x 2]
# Database:   spark_connection
# Ordered by: cal_year
   cal_year total_yearly_cost_for_group
      <dbl>                       <dbl>
 1     2009                       76945
 2     2010                       88920
 3     2011                       89440
 4     2012                       86320
 5     2013                       97780
 6     2014                      100300
 7     2015                       86574
 8     2016                      112886
 9     2017                       98898
10     2018                      112607
11     2019                        5994
```
````

This is much better! We now have the output we expected. 

Note that if we check the number of partitions of this output dataset (using `sdf_num_partitions(hackney_cats_year)`) the number returned will be the `spark.sql.shuffle.partitions` default value (in my case, 64). It is a good idea to [check and manage partitions](../spark-concepts/partitions) after running a UDF for this reason, as you may need to repartition to optimise your code.

## Further resources
- [`spark_apply`](https://spark.posit.co/packages/sparklyr/latest/reference/spark_apply.html)
- [Additional guidance on using `spark_apply` from Chapter 11 of Mastering Spark with R](https://therinspark.com/distributed.html)
- [`arrow` R package](https://arrow.apache.org/docs/16.0/r/index.html)
- [Speeding up R and Apache Spark using Apache Arrow](https://arrow.apache.org/blog/2019/01/25/r-spark-improvements/)



