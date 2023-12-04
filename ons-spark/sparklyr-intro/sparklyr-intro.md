## Introduction to sparklyr

This article aims to give hands on experience in working with the sparklyr package in R. You can download the raw R code used in this article from the [ONS Spark repository](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/raw-notebooks/sparklyr-intro/r_input.R).

We will not aim to cover all the sparklyr DataFrame functionality or go into detail of how Spark works, but instead focus on practicality by performing some common operations on an example dataset. There are a handful of exercises that you can complete while reading this article.

Prerequisites for this article are some basic knowledge of R and dplyr. If you are completely new to R then it is recommended to complete an introductory course first; your organisation may have specific R training. Other resources include the [Introduction to dplyr](https://dplyr.tidyverse.org/articles/dplyr.html) and [R for Data Science](https://r4ds.had.co.nz/).  If you are an Python user, the [Introduction to PySpark](../pyspark-intro/pyspark-intro) article follows similar format.

### sparklyr: a quick introduction

Although this article focusses on practical usage to enable you to quickly use sparklyr, you do need to understand some basic theory of Spark and distributed computing.

Spark is a powerful tool used to process huge data in an efficient way. We can access Spark in R with the sparklyr package. Spark has DataFrames, consisting of rows and columns, similar to base R DataFrames or tibbles. Many of the operations are also translated directly from dplyr, some with a `spark_` or `sdf_` prefix: e.g. you can use [`select()`](https://dplyr.tidyverse.org/reference/select.html), [`mutate()`](https://dplyr.tidyverse.org/reference/mutate.html), [`filter()`](https://dplyr.tidyverse.org/reference/filter.html) and [`summarise()`](https://dplyr.tidyverse.org/reference/summarise.html) as you would in dplyr.

The key difference between sparklyr and base R is where the DataFrame is processed:
- base R DataFrames and tibbles are processed on the driver; this could be on a local machine using a desktop IDE such as RStudio, or on a server, e.g. in a dedicated Docker container (such as a CDSW session). The amount of data you can process is limited to the driver memory, so base R is suitable for smaller data.
- sparklyr DataFrames are processed on the Spark cluster. This is a big pool of linked machines, called nodes. sparklyr DataFrames are distributed into partitions, and are processed in parallel on the nodes in the Spark cluster. You can have much greater memory capacity with Spark and so is suitable for big data.

The DataFrame is also processed differently:
- In base R and dplyr, the DataFrame changes in memory at each point, e.g. you could create a DataFrame by reading from a CSV file, select some columns, filter the rows, add a column with `mutate()` and then write the data out. With each operation, the DataFrame is physically changing in memory. This can be useful for debugging as it is easy to see intermediate outputs.
- In sparklyr, DataFrames are lazily evaluated. We give Spark a set of instructions, called transformations, which are only evaluated when necessary, for instance to get a row count or write out data to a file, referred to as an action. In the example above, the plan is triggered once the data are set to write out to a file.

For more detail on how Spark works, you can refer to the articles in the Understanding and Optimising Spark chapter of this book. [Databricks: Learning Spark](https://pages.databricks.com/rs/094-YMS-629/images/LearningSpark2.0.pdf) is another useful resource.

### The `sparklyr` package

To use Spark with R, we make use of the [`sparklyr`](https://spark.rstudio.com/) package. There is also the [SparkR](https://spark.apache.org/docs/latest/sparkr.html) package which is an alternative, although this is not used at the ONS.

It is recommended to install the full [`tidyverse`](https://www.tidyverse.org/) first before installing `sparklyr`, to ensure that you take take advantage of the full functionality of dplyr and related packages.

When referencing functions from `sparklyr`, you can either import the package with `library()` or `require()`, or reference the function directly with `::`, e.g. `sparklyr::select()`. If you reference directly you will need to import `magrittr` to take advantage of the `%>%` pipes. See the article on [Avoiding Module Import Conflicts](../ancillary-topics/module-imports) for more information on referencing packages.

In this article we both reference the packages directly and also import the packages, so if you run the raw code you can use either method. Knowing where a package comes from can be tricky when using sparklyr, so although the generally accepted good practice is to directly reference with `::` you may find it easier to just import the packages and use the functions without referencing.

We also import `dplyr`, as we will frequently be converting sparklyr DataFrames to tibbles. As part of the setup we also read in a config file using `yaml` although as this is only being used once the package is directly referenced.
````{tabs}

```{code-tab} r R

library(sparklyr)
library(dplyr)
library(magrittr)

config <- yaml::yaml.load_file("ons-spark/config.yaml")

```
````
The help functionality in R works as usual with sparklyr functions, and can be accessed by prefixing a function with `?`, e.g. `?spark_read_csv`, although the easiest way is to look at the [documentation](https://spark.rstudio.com/packages/sparklyr/latest/reference/). There are a lot of functions in the sparklyr module and you will be very unlikely to use them all.

### Create a Spark session: `spark_connect()`

With the `sparklyr` package imported we now want to create a connection to the Spark cluster. We use [`spark_connect()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark-connections.html) and assign this to `sc`.

`spark_connect()` takes a [`spark_config()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark_config.html) as an input and this has many options for controlling the size of the Spark session; see the [Guidance on Spark Sessions](../spark-overview/spark-session-guidance) and also [Example Spark Sessions](../spark-overview/example-spark-sessions) to get an idea of what sized session to use.

For this article, we are using a tiny dataset by Spark standards, and so are using a local session. This also means that you can run this code without having access to a Spark cluster.

Note that only one Spark session can be running at once. If a session already exists then a new one will not be created, instead the connection to the existing session will be used.

We can confirm that we have a running Spark session by checking that the output of [`spark_connection_is_open()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark-connections.html) is `TRUE`.
````{tabs}

```{code-tab} r R

sc <- sparklyr::spark_connect(
  master = "local[2]",
  app_name = "sparklyr-intro",
  config = sparklyr::spark_config())
  
sparklyr::spark_connection_is_open(sc)

```
````

```plaintext
[1] TRUE
```
### Reading data: `spark_read_csv()`

For this article we will look at some open data on animal rescue incidents from the London Fire Brigade. The data are stored as a CSV, although the parquet file format is the most common when using Spark. The reason for using CSV in this article is because it is a familiar file format and allows you to adapt this code easily for your own sample data. See the article on [Reading Data in sparklyr](../pyspark-intro/reading-data-sparklyr) for more information.

Often your data will be large and stored using Hadoop, on the Hadoop Distributed File System (HDFS). This example uses a local file, enabling us to get started quickly; see the article on [Data Storage](../spark-overview/data-storage) for more information.

To read in from a CSV file, use [`spark_read_csv()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark_read_csv.html). The first argument is to supply `sc`, which is the open Spark connection defined from `spark_connect()` previously. The file path is stored in the config file as `rescue_path_csv`. Using `header=TRUE` means that the DataFrame will use the column headers from the CSV as the column names. CSV files do not contain information about the [data types](../spark-overview/data-types), so use `infer_schema=TRUE` which makes Spark scan the file to infer the data types.
````{tabs}

```{code-tab} r R

rescue_path = config$rescue_path_csv

rescue <- sparklyr::spark_read_csv(
    sc,
    path=rescue_path, 
    header=TRUE,
    infer_schema=TRUE)

```
````
We can use [`class(rescue)`](https://stat.ethz.ch/R-manual/R-devel/library/base/html/class.html) to see that `rescue` is now a sparklyr DataFrame:
````{tabs}

```{code-tab} r R

class(rescue)

```
````

```plaintext
[1] "tbl_spark" "tbl_sql"   "tbl_lazy"  "tbl"      
```
### Preview data: `glimpse()`

To view the column names, data types and get a short preview of the data use [`glimpse()`](https://pillar.r-lib.org/reference/glimpse.html) from the `pillar` package. These will be transposed in the display, meaning this option is good for previewing DataFrames with a large number of columns.

Note that `glimpse()` is an *action* and the DataFrame will get lazily evaluated when this is used. As such, `glimpse()` can take a long time when you have a lot of code. As we are only previewing a small DataFrame that is directly imported from CSV it will be relatively quick here.
````{tabs}

```{code-tab} r R

pillar::glimpse(rescue)

```
````

```plaintext
Rows: ??
Columns: 26
Database: spark_connection
$ IncidentNumber             <chr> "139091", "275091", "2075091", "2872091", "…
$ DateTimeOfCall             <chr> "01/01/2009 03:01", "01/01/2009 08:51", "04…
$ CalYear                    <int> 2009, 2009, 2009, 2009, 2009, 2009, 2009, 2…
$ FinYear                    <chr> "2008/09", "2008/09", "2008/09", "2008/09",…
$ TypeOfIncident             <chr> "Special Service", "Special Service", "Spec…
$ PumpCount                  <dbl> 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1…
$ PumpHoursTotal             <dbl> 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1…
$ HourlyNotionalCostGBP      <int> 255, 255, 255, 255, 255, 255, 255, 255, 255…
$ IncidentNotionalCostGBP    <dbl> 510, 255, 255, 255, 255, 255, 255, 255, 255…
$ FinalDescription           <chr> "DOG WITH JAW TRAPPED IN MAGAZINE RACK,B15"…
$ AnimalGroupParent          <chr> "Dog", "Fox", "Dog", "Horse", "Rabbit", "Un…
$ OriginofCall               <chr> "Person (land line)", "Person (land line)",…
$ PropertyType               <chr> "House - single occupancy ", "Railings", "P…
$ PropertyCategory           <chr> "Dwelling", "Outdoor Structure", "Outdoor S…
$ SpecialServiceTypeCategory <chr> "Other animal assistance", "Other animal as…
$ SpecialServiceType         <chr> "Animal assistance involving livestock - Ot…
$ WardCode                   <chr> "E05011467", "E05000169", "E05000558", "E05…
$ Ward                       <chr> "Crystal Palace & Upper Norwood", "Woodside…
$ BoroughCode                <chr> "E09000008", "E09000008", "E09000029", "E09…
$ Borough                    <chr> "Croydon", "Croydon", "Sutton", "Hillingdon…
$ StnGroundName              <chr> "Norbury", "Woodside", "Wallington", "Ruisl…
$ PostcodeDistrict           <chr> "SE19", "SE25", "SM5", "UB9", "RM3", "RM10"…
$ Easting_m                  <dbl> NA, 534785, 528041, 504689, NA, NA, 539013,…
$ Northing_m                 <dbl> NA, 167546, 164923, 190685, NA, NA, 186162,…
$ Easting_rounded            <int> 532350, 534750, 528050, 504650, 554650, 549…
$ Northing_rounded           <int> 170050, 167550, 164950, 190650, 192350, 184…
```
Note that when using `glimpse()` the number of rows are given as `??`. Spark uses lazy evaluation and it can return a preview of the data without having to derive the row count.

### Implicitly Preview data: `print()`

The DataFrame can be implicitly previewed with `print()` or by supplying the DataFrame name without assignment or a function.

These are interpreted as *actions*, meaning that all previous *transformations* will be ran on the Spark cluster; often this will have many *transformations*, but here we only have one, reading in the data.

By default 10 rows will be displayed; this can be changed by setting `n`, e.g. `n=5`. When there are many columns the output will be truncated, so this option is often not practical.
````{tabs}

```{code-tab} r R

rescue

```
````

```plaintext
# Source: spark<animal_rescue_f63c89cf_baf8_48c9_b6d8_bbeccadbc55f> [?? x 26]
   IncidentNumber DateTimeOfCall   CalYear FinYear TypeOfIncident  PumpCount
   <chr>          <chr>              <int> <chr>   <chr>               <dbl>
 1 139091         01/01/2009 03:01    2009 2008/09 Special Service         1
 2 275091         01/01/2009 08:51    2009 2008/09 Special Service         1
 3 2075091        04/01/2009 10:07    2009 2008/09 Special Service         1
 4 2872091        05/01/2009 12:27    2009 2008/09 Special Service         1
 5 3553091        06/01/2009 15:23    2009 2008/09 Special Service         1
 6 3742091        06/01/2009 19:30    2009 2008/09 Special Service         1
 7 4011091        07/01/2009 06:29    2009 2008/09 Special Service         1
 8 4211091        07/01/2009 11:55    2009 2008/09 Special Service         1
 9 4306091        07/01/2009 13:48    2009 2008/09 Special Service         1
10 4715091        07/01/2009 21:24    2009 2008/09 Special Service         1
# … with more rows, and 20 more variables: PumpHoursTotal <dbl>,
#   HourlyNotionalCostGBP <int>, IncidentNotionalCostGBP <dbl>,
#   FinalDescription <chr>, AnimalGroupParent <chr>, OriginofCall <chr>,
#   PropertyType <chr>, PropertyCategory <chr>,
#   SpecialServiceTypeCategory <chr>, SpecialServiceType <chr>, WardCode <chr>,
#   Ward <chr>, BoroughCode <chr>, Borough <chr>, StnGroundName <chr>,
#   PostcodeDistrict <chr>, Easting_m <dbl>, Northing_m <dbl>, …
```

````{tabs}

```{code-tab} r R

print(rescue, n=5)

```
````

```plaintext
# Source: spark<animal_rescue_205ed5b0_55ae_4fad_af90_b9b6ce17e094> [?? x 26]
  IncidentNumber DateTimeOfCall   CalYear FinYear TypeOfIncident  PumpCount
  <chr>          <chr>              <int> <chr>   <chr>               <dbl>
1 139091         01/01/2009 03:01    2009 2008/09 Special Service         1
2 275091         01/01/2009 08:51    2009 2008/09 Special Service         1
3 2075091        04/01/2009 10:07    2009 2008/09 Special Service         1
4 2872091        05/01/2009 12:27    2009 2008/09 Special Service         1
5 3553091        06/01/2009 15:23    2009 2008/09 Special Service         1
# … with more rows, and 20 more variables: PumpHoursTotal <dbl>,
#   HourlyNotionalCostGBP <int>, IncidentNotionalCostGBP <dbl>,
#   FinalDescription <chr>, AnimalGroupParent <chr>, OriginofCall <chr>,
#   PropertyType <chr>, PropertyCategory <chr>,
#   SpecialServiceTypeCategory <chr>, SpecialServiceType <chr>, WardCode <chr>,
#   Ward <chr>, BoroughCode <chr>, Borough <chr>, StnGroundName <chr>,
#   PostcodeDistrict <chr>, Easting_m <dbl>, Northing_m <dbl>, …
```
### Convert to a tibble: `collect()` and `head()`

`glimpse()` and `print()` both only give us a partial preview of the rows in the DataFrame, and do not assign the results to a variable. [`collect()`](https://dplyr.tidyverse.org/reference/compute.html) will bring the sparklyr DataFrame into the driver from the cluster as a tibble, and you can therefore take advantage of all the usual tibble and base R functionality. For instance, you may want to [visualise your results](../ancillary-topics/visualisation).

In some IDEs, printing a tibble will display a nicely formatted HTML table, so this is another option when previewing data.

Be careful: the DataFrame is currently on the *Spark cluster* with lots of memory capacity, whereas tibbles are stored on the *driver*, which will have much less. Trying to use `collect()` and a huge sparklyr DF will not work. If converting to a tibble just to view the data, use `head()` to just bring back a small number of rows. Note that [sparklyr DataFrames are not ordered by default](../spark-concepts/df-order), and `head()` on a sparklyr DataFrame will **not** always return the same rows. This is [discussed in more detail later](sort-data).

`collect()` is an *action* and will process the whole plan on the Spark cluster. In this example, it will read the CSV file, return three rows, and then convert the result to the driver as a tibble. Note that the number of rows is now `3` rather than `??`, as tibbles have known dimensions.
````{tabs}

```{code-tab} r R

rescue_tibble <- rescue %>%
    head(3) %>%
    sparklyr::collect()
    
rescue_tibble %>%
    print()

```
````

```plaintext
# A tibble: 3 × 26
  IncidentNumber DateTimeOfCall   CalYear FinYear TypeOfIncident  PumpCount
  <chr>          <chr>              <int> <chr>   <chr>               <dbl>
1 139091         01/01/2009 03:01    2009 2008/09 Special Service         1
2 275091         01/01/2009 08:51    2009 2008/09 Special Service         1
3 2075091        04/01/2009 10:07    2009 2008/09 Special Service         1
# … with 20 more variables: PumpHoursTotal <dbl>, HourlyNotionalCostGBP <int>,
#   IncidentNotionalCostGBP <dbl>, FinalDescription <chr>,
#   AnimalGroupParent <chr>, OriginofCall <chr>, PropertyType <chr>,
#   PropertyCategory <chr>, SpecialServiceTypeCategory <chr>,
#   SpecialServiceType <chr>, WardCode <chr>, Ward <chr>, BoroughCode <chr>,
#   Borough <chr>, StnGroundName <chr>, PostcodeDistrict <chr>,
#   Easting_m <dbl>, Northing_m <dbl>, Easting_rounded <int>, …
```
As the syntax in sparklyr inherits so much from dplyr new users can sometimes get confused over what type of DataFrame they have. `class()` is useful here. We can confirm that `rescue_tibble` is indeed a tibble:
````{tabs}

```{code-tab} r R

class(rescue_tibble)

```
````

```plaintext
[1] "tbl_df"     "tbl"        "data.frame"
```
### Select and drop columns: `select()`

Often your data will have too many columns that are not relevant, so we can use `select()` to just get the ones that are of interest. In our example, we can reduce the number of columns returned so that the output of `print()` is much neater.

[`select()`](https://dplyr.tidyverse.org/reference/select.html) in sparklyr works in the same way as in dplyr; simply provide the names of the columns. There are no need for vectors, lists or quotes around column names. It is recommended to use the pipe (`%>%`), which means the first argument is the result of the previous operation. Using `%>%` makes the code easier to read and also more instinctive, since the only inputs to `select()` are the columns, not the DataFrame.

Selecting columns is a *transformation*, and so will only be processed once an *action* is called. As such we are chaining this with `collect()`, which will bring the data into the driver. Remember to use `head()` if collecting the data:
````{tabs}

```{code-tab} r R

rescue %>%
    sparklyr::select(IncidentNumber, DateTimeOfCall, FinalDescription) %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

```
````

```plaintext
# A tibble: 5 × 3
  IncidentNumber DateTimeOfCall   FinalDescription                         
  <chr>          <chr>            <chr>                                    
1 139091         01/01/2009 03:01 DOG WITH JAW TRAPPED IN MAGAZINE RACK,B15
2 275091         01/01/2009 08:51 ASSIST RSPCA WITH FOX TRAPPED,B15        
3 2075091        04/01/2009 10:07 DOG CAUGHT IN DRAIN,B15                  
4 2872091        05/01/2009 12:27 HORSE TRAPPED IN LAKE,J17                
5 3553091        06/01/2009 15:23 RABBIT TRAPPED UNDER SOFA,B15            
```
`select()` can also be used to drop columns. Simply specify `-` before a column name to remove it. There are a lot of columns related to the location of the animal rescue incidents that we will not use that can be removed with `select(-column)`.

Note that we have written over the our previous DataFrame by re-assiging to `rescue`; Spark DFs are *immutable*.

We then use `glimpse()` to verify that the columns have been removed.
````{tabs}

```{code-tab} r R

rescue <- rescue %>%
    sparklyr::select(
        -WardCode,
        -BoroughCode,
        -Easting_m,
        -Northing_m,
        -Easting_rounded,
        -Northing_rounded)
        
pillar::glimpse(rescue)

```
````

```plaintext
Rows: ??
Columns: 20
Database: spark_connection
$ IncidentNumber             <chr> "139091", "275091", "2075091", "2872091", "…
$ DateTimeOfCall             <chr> "01/01/2009 03:01", "01/01/2009 08:51", "04…
$ CalYear                    <int> 2009, 2009, 2009, 2009, 2009, 2009, 2009, 2…
$ FinYear                    <chr> "2008/09", "2008/09", "2008/09", "2008/09",…
$ TypeOfIncident             <chr> "Special Service", "Special Service", "Spec…
$ PumpCount                  <dbl> 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1…
$ PumpHoursTotal             <dbl> 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1…
$ HourlyNotionalCostGBP      <int> 255, 255, 255, 255, 255, 255, 255, 255, 255…
$ IncidentNotionalCostGBP    <dbl> 510, 255, 255, 255, 255, 255, 255, 255, 255…
$ FinalDescription           <chr> "DOG WITH JAW TRAPPED IN MAGAZINE RACK,B15"…
$ AnimalGroupParent          <chr> "Dog", "Fox", "Dog", "Horse", "Rabbit", "Un…
$ OriginofCall               <chr> "Person (land line)", "Person (land line)",…
$ PropertyType               <chr> "House - single occupancy ", "Railings", "P…
$ PropertyCategory           <chr> "Dwelling", "Outdoor Structure", "Outdoor S…
$ SpecialServiceTypeCategory <chr> "Other animal assistance", "Other animal as…
$ SpecialServiceType         <chr> "Animal assistance involving livestock - Ot…
$ Ward                       <chr> "Crystal Palace & Upper Norwood", "Woodside…
$ Borough                    <chr> "Croydon", "Croydon", "Sutton", "Hillingdon…
$ StnGroundName              <chr> "Norbury", "Woodside", "Wallington", "Ruisl…
$ PostcodeDistrict           <chr> "SE19", "SE25", "SM5", "UB9", "RM3", "RM10"…
```
### Get the row count: `sdf_nrow()`

Sometimes your data will be small enough that you do not even need to use Spark. As such it is useful to know the row count, and then make the decision on whether to use Spark or just use base R or dplyr. One advantage of base R DataFrames is that there are more compatible packages available than there are with Spark.

To get the row count in sparklyr, use [`sdf_nrow()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_dim.html). This will only work on sparklyr DFs, not tibbles or base R DFs.

We saw earlier that unlike tibbles the row count is not automatically determined when the data are read in as a sparklyr DataFrame; remember that `glimpse()` had a row count of `??`. This is an example of *lazy evaluation*. As such, `sdf_nrow()` is an *action* and has to be explicitly called.
````{tabs}

```{code-tab} r R

rescue %>%
    sparklyr::sdf_nrow()

```
````

```plaintext
[1] 5898
```
### Rename columns: `rename()`

The source data has the column names in `CamelCase`, but when using R we generally prefer to use `snake_case`.

To rename columns, use [`rename()`](https://dplyr.tidyverse.org/reference/rename.html). Like `select()`, usage is the same as dplyr and is actually referenced directly with `dplyr::rename()`. Assign the new column name to be the old one with `=`, e.g `rename(incident_number = IncidentNumber)`. Once again we are re-assigning the DataFrame as it is immutable.
````{tabs}

```{code-tab} r R

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

```
````

```plaintext
Rows: ??
Columns: 20
Database: spark_connection
$ incident_number            <chr> "139091", "275091", "2075091", "2872091", "…
$ date_time_of_call          <chr> "01/01/2009 03:01", "01/01/2009 08:51", "04…
$ cal_year                   <int> 2009, 2009, 2009, 2009, 2009, 2009, 2009, 2…
$ FinYear                    <chr> "2008/09", "2008/09", "2008/09", "2008/09",…
$ TypeOfIncident             <chr> "Special Service", "Special Service", "Spec…
$ engine_count               <dbl> 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1…
$ job_hours                  <dbl> 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1…
$ HourlyNotionalCostGBP      <int> 255, 255, 255, 255, 255, 255, 255, 255, 255…
$ total_cost                 <dbl> 510, 255, 255, 255, 255, 255, 255, 255, 255…
$ FinalDescription           <chr> "DOG WITH JAW TRAPPED IN MAGAZINE RACK,B15"…
$ animal_group               <chr> "Dog", "Fox", "Dog", "Horse", "Rabbit", "Un…
$ OriginofCall               <chr> "Person (land line)", "Person (land line)",…
$ PropertyType               <chr> "House - single occupancy ", "Railings", "P…
$ PropertyCategory           <chr> "Dwelling", "Outdoor Structure", "Outdoor S…
$ SpecialServiceTypeCategory <chr> "Other animal assistance", "Other animal as…
$ SpecialServiceType         <chr> "Animal assistance involving livestock - Ot…
$ Ward                       <chr> "Crystal Palace & Upper Norwood", "Woodside…
$ Borough                    <chr> "Croydon", "Croydon", "Sutton", "Hillingdon…
$ StnGroundName              <chr> "Norbury", "Woodside", "Wallington", "Ruisl…
$ PostcodeDistrict           <chr> "SE19", "SE25", "SM5", "UB9", "RM3", "RM10"…
```
### Exercise 1

#### Exercise 1a

Rename the following columns in the `rescue` DataFrame:

`FinalDescription` --> `description`

`PostcodeDistrict` --> `postcode_district`

#### Exercise 1b

Select these columns and the seven columns that were renamed in the cell above; you should have nine in total. Reassign the result to the `rescue` DataFrame.

#### Exercise 1c

Preview the structure of the DataFrame.

<details>
<summary><b>Exercise 1: Solution</b></summary>

````{tabs}

```{code-tab} r R

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

```
````

```plaintext
Rows: ??
Columns: 9
Database: spark_connection
$ incident_number   <chr> "139091", "275091", "2075091", "2872091", "3553091",…
$ date_time_of_call <chr> "01/01/2009 03:01", "01/01/2009 08:51", "04/01/2009 …
$ animal_group      <chr> "Dog", "Fox", "Dog", "Horse", "Rabbit", "Unknown - H…
$ cal_year          <int> 2009, 2009, 2009, 2009, 2009, 2009, 2009, 2009, 2009…
$ total_cost        <dbl> 510, 255, 255, 255, 255, 255, 255, 255, 255, 255, 25…
$ job_hours         <dbl> 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1…
$ engine_count      <dbl> 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1…
$ description       <chr> "DOG WITH JAW TRAPPED IN MAGAZINE RACK,B15", "ASSIST…
$ postcode_district <chr> "SE19", "SE25", "SM5", "UB9", "RM3", "RM10", "E11", …
```
</details>

### Filter rows: `filter()`

Rows of sparklyr DataFrames can be filtered with [`filter()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html), which takes a logical condition. This works in an identical way to `dplyr::filter()`. Any column names can be referenced by name without quotes.

For instance, if we want to select all the rows where `animal_group` is equal to `Hamster`, we can use `filter(animal_group == "Hamster")`. Note the double equals sign used in a condition. We do not want to change the `rescue` DataFrame, so assign it to a new DF, `hamsters`, then preview a few of the columns:
````{tabs}

```{code-tab} r R

hamsters <- rescue %>%
    sparklyr::filter(animal_group == "Hamster")

hamsters %>%
    sparklyr::select(
        incident_number,
        animal_group,
        cal_year,
        total_cost)

```
````

```plaintext
# Source: spark<?> [?? x 4]
   incident_number animal_group cal_year total_cost
   <chr>           <chr>           <int>      <dbl>
 1 37009101        Hamster          2010        260
 2 58746101        Hamster          2010        260
 3 212716101       Hamster          2010        260
 4 140677111       Hamster          2011        260
 5 157873111       Hamster          2011        260
 6 164240111       Hamster          2011        260
 7 94146131        Hamster          2013        290
 8 105214131       Hamster          2013        290
 9 134157131       Hamster          2013        290
10 145232141       Hamster          2014        295
# … with more rows
```
For multiple conditions putting each condition on a new line makes the code easier to read:
````{tabs}

```{code-tab} r R

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

```
````

```plaintext
# Source: spark<?> [?? x 4]
  incident_number animal_group cal_year total_cost
  <chr>           <chr>           <int>      <dbl>
1 16209121        Dog              2012        780
2 17531121        Dog              2012        780
3 20818121        Dog              2012        780
4 38636121        Dog              2012        780
5 64764121        Dog              2012       1040
```
### Exercise 2

Create a new DataFrame which consists of all the rows where `animal_group` is equal to `"Fox"`, then select `incident_number`, `animal_group`, `cal_year`, and `total_cost` and preview the first ten rows.

<details>
<summary><b>Exercise 2: Solution</b></summary>

````{tabs}

```{code-tab} r R

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

```
````

```plaintext
# Source: spark<?> [?? x 4]
   incident_number animal_group cal_year total_cost
   <chr>           <chr>           <int>      <dbl>
 1 275091          Fox              2009        255
 2 12451091        Fox              2009        765
 3 12770091        Fox              2009        255
 4 18386091        Fox              2009        255
 5 20027091        Fox              2009        255
 6 30133091        Fox              2009        255
 7 73167091        Fox              2009        260
 8 80733091        Fox              2009        780
 9 105324091       Fox              2009        520
10 116595091       Fox              2009        260
```
</details>

### Adding Columns: `mutate()`

[`mutate()`](https://dplyr.tidyverse.org/reference/mutate.html) can be used to create new columns and overwrite an existing ones. One `mutate()` statement can be used to create multiple columns, which can make the code easier to read. Just like the other dplyr commands, `sparklyr::mutate()` works in an identical way to `dplyr::mutate()`. Once again, just reference the columns by name, rather than using quotes.

The statement to create each column should begin `column_name = `; note that a single `=` is being used for assignment rather than `<-`. Then just set this new column equal to a statement, which will often be derived from other columns. For instance, we do not have a column for how long an incident took in the data, but do have the columns available to derive this:
- `job_hours` gives the total number of hours for engines attending the incident, e.g. if 2 engines attended for an hour `job_hours` will be `2`
- `engine_count` gives the number of engines in attendance

So to get the duration of the incident, which we will call `incident_duration`, we have to divide `job_hours` by `engine_count`:
````{tabs}

```{code-tab} r R

rescue <- rescue %>%
    sparklyr::mutate(
        incident_duration = job_hours / engine_count)

```
````
Now preview the data with `head(5) %>% collect()` after selecting a few relevant columns, then print out the DataFrame:
````{tabs}

```{code-tab} r R

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

```
````

```plaintext
# A tibble: 5 × 5
  incident_number animal_group incident_duration job_hours engine_count
  <chr>           <chr>                    <dbl>     <dbl>        <dbl>
1 139091          Dog                          2         2            1
2 275091          Fox                          1         1            1
3 2075091         Dog                          1         1            1
4 2872091         Horse                        1         1            1
5 3553091         Rabbit                       1         1            1
```
Although the source data here is small, note that previewing the data will still take longer to process than defining the new column. Why? Remember that Spark is built on the concept of **transformations** and **actions**:
* **Transformations** are lazily evaluated expressions. These form the set of instructions called the execution plan.  
* **Actions** trigger computation to be performed on the cluster and results returned to the driver. It is actions that trigger the execution plan.

Multiple transformations can be combined, as we did to preprocess the `rescue` DataFrame above. Only when an action is called, for example `collect()` or implicitly printing the DF are these transformations and action executed on the cluster, after which the results are returned to the driver.

### Using Spark SQL Functions with `mutate()`

So far, writing sparklyr code has been very similar to using dplyr, the main difference being the consideration of where the DataFrame is being processed: either on the driver in the case of dplyr, or in the Spark cluster with sparklyr.

One key difference is that when using `mutate()` you can also make use of Spark functions directly. See the article on [Using Spark functions in sparklyr](../sparklyr-intro/sparklyr-functions) for a full explanation.

One example is casting columns from one data type to another, e.g. from string to date. In sparklyr we can use [`to_date()`](https://spark.apache.org/docs/latest/api/sql/index.html#to_date). Note that this is **not** referenced with `::`. We then verify the column type has changed with `glimpse()`.
````{tabs}

```{code-tab} r R

rescue <- rescue %>%
    sparklyr::mutate(date_time_of_call = to_date(date_time_of_call, "dd/MM/yyyy"))

rescue %>%
    sparklyr::select(
        incident_number,
        date_time_of_call) %>%
    pillar::glimpse()

```
````

```plaintext
Rows: ??
Columns: 2
Database: spark_connection
$ incident_number   <chr> "139091", "275091", "2075091", "2872091", "3553091",…
$ date_time_of_call <date> 2009-01-01, 2009-01-01, 2009-01-04, 2009-01-05, 200…
```
You will need to consult the [documentation](https://spark.apache.org/docs/latest/api/sql/index.html) for Spark SQL functions as they are not exposed to R directly; this can be demonstrated by a lack of available help for them in R. You can also only use them on sparklyr DFs, not base R DFs or tibbles.
````{tabs}

```{code-tab} r R

?to_date

```
````

```plaintext
No documentation for ‘to_date’ in specified packages and libraries:
you could try ‘??to_date’
```
(sort-data)=
### Sorting: `arrange()` and `sdf_sort()`

An important Spark concept is that DataFrames are [not ordered by default](../spark-concepts/df-order), unlike a base R DF or tibble, which have an index. Remember that a Spark DataFrame is distributed into partitions, and there is no guarantee of the order of the rows within these partitions, or which partition a particular row is on.

There are two ways to sort data in sparklyr: with [`dplyr::arrange()`](https://dplyr.tidyverse.org/reference/arrange.html) and [`sparklyr::sdf_sort()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_sort.html). Neither are perfect implementations. `arrange()` will only actually be processed if the data is being subset or directly returned, as otherwise the sorting operation gets deemed superfluous by Spark. By default columns are sorted ascending; use [`desc()`](https://dplyr.tidyverse.org/reference/desc.html) to sort descending.

`sdf_sort()` is better, but columns can only be sorted ascending and the syntax involves adding the column names as strings in a vector like in base R which makes it less readable. You can sort columns that are entirely numeric descending by creating a new column of negative values, sorting by this with `sdf_sort()`, then dropping it, but this will not work correctly with strings or `NA` values. You could also use an SQL expression for sorting. The most important principle here is consistency; try and use the same syntax as your colleagues to make the code easier to read.

Note that sorting the DataFrame is an expensive operation, as the rows move between partitions. This is a key Spark concept called a [*shuffle*](../spark-concepts/shuffling). When you are ready to optimise your Spark code you will want to read the article on Shuffling.

To show the ten highest cost incidents, use `arrange(desc(total_cost))`:
````{tabs}

```{code-tab} r R

rescue %>%
    sparklyr::select(
        incident_number,
        animal_group,
        total_cost) %>%
    dplyr::arrange(desc(total_cost)) %>%
    head(10)

```
````

```plaintext
# Source:     spark<?> [?? x 3]
# Ordered by: desc(total_cost)
   incident_number animal_group          total_cost
   <chr>           <chr>                      <dbl>
 1 098141-28072016 Cat                         3912
 2 48360131        Horse                       3480
 3 62700151        Horse                       2980
 4 092389-09072018 Horse                       2664
 5 49076141        Cat                         2655
 6 82423111        Horse                       2340
 7 101755111       Deer                        2340
 8 49189111        Horse                       2340
 9 030477-09032018 Unknown - Wild Animal       2296
10 028258-08032017 Cat                         2282
```
Horses make up a lot of the more expensive calls, which makes sense, given that they are large animals.

### Exercise 3

Sort the incidents in terms of their duration, look at the top 10 and the bottom 10. Do you notice anything strange?

<details>
<summary><b>Exercise 3: Solution</b></summary>

````{tabs}

```{code-tab} r R

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

```
````

```plaintext
# Source:     spark<?> [?? x 4]
# Ordered by: desc(incident_duration)
   incident_number animal_group                     total_cost incident_duration
   <chr>           <chr>                                 <dbl>             <dbl>
 1 48360131        Horse                                  3480               6  
 2 18627122        Horse                                  1300               5  
 3 92423121        Unknown - Domestic Animal Or Pet       1300               5  
 4 137525091       Horse                                  1300               5  
 5 62700151        Horse                                  2980               5  
 6 125704-02092018 Horse                                  1665               5  
 7 126939-05092018 Horse                                  1665               5  
 8 955141          Horse                                  1450               5  
 9 49076141        Cat                                    2655               4.5
10 64764121        Dog                                    1040               4  
# Source: spark<?> [?? x 4]
   incident_number animal_group total_cost incident_duration
   <chr>           <chr>             <dbl>             <dbl>
 1 35453121        Cat                  NA                NA
 2 4920141         Bird                 NA                NA
 3 36995121        Horse                NA                NA
 4 194986111       Horse                NA                NA
 5 51412121        Cat                  NA                NA
 6 208663091       Horse                NA                NA
 7 18325122        Dog                  NA                NA
 8 43265111        Dog                  NA                NA
 9 2602131         Cat                  NA                NA
10 118613121       Cat                  NA                NA
```
</details>

### Grouping and Aggregating: `group_by()` and `summarise()`

In most cases, we want to get insights into the raw data, for instance, by taking the sum or average of a column, or getting the largest or smallest values. This is key to what the Office for National Statistics does: we release statistics! 

In sparklyr, [`group_by()`](https://dplyr.tidyverse.org/reference/group_by.html) and [`summarise()`](https://dplyr.tidyverse.org/reference/summarise.html) can be referenced directly from dplyr and these functions work in an identical way. There is one key difference: inside `summmarise()` functions will be interpreted as Spark SQL functions where possible, e.g. [`sum()`](https://spark.apache.org/docs/latest/api/sql/index.html#sum), [`max()`](https://spark.apache.org/docs/latest/api/sql/index.html#max) etc.

For instance, to find the average cost by `animal_group` we use [`mean()`](https://spark.apache.org/docs/latest/api/sql/index.html#mean). Remember to assign the result of the aggregation to a column name:
````{tabs}

```{code-tab} r R

cost_by_animal <- rescue %>%
    dplyr::group_by(animal_group) %>%
    dplyr::summarise(average_cost = mean(total_cost))

cost_by_animal %>%
    head(5)

```
````

```plaintext
# Source: spark<?> [?? x 2]
  animal_group                     average_cost
  <chr>                                   <dbl>
1 Bird                                     326.
2 Snake                                    322.
3 Lizard                                   284.
4 Fish                                     780 
5 Unknown - Heavy Livestock Animal         363.
```
Remember that sparklyr DFs are not ordered by unless we specifically do so; here we will sort the `average_cost` descending:
````{tabs}

```{code-tab} r R

cost_by_animal %>%
    dplyr::arrange(desc(average_cost)) %>%
       head(10)

```
````

```plaintext
# Source:     spark<?> [?? x 2]
# Ordered by: desc(average_cost)
   animal_group                                     average_cost
   <chr>                                                   <dbl>
 1 Goat                                                    1180 
 2 Bull                                                     780 
 3 Fish                                                     780 
 4 Horse                                                    747.
 5 Unknown - Animal rescue from water - Farm animal         710.
 6 Cow                                                      624.
 7 Lamb                                                     520 
 8 Hedgehog                                                 520 
 9 Deer                                                     424.
10 Unknown - Wild Animal                                    390.
```
It looks like `Goat` could be an outlier as it is significantly higher than the other higher average cost incidents. We can investigate this in more detail using `filter()`:
````{tabs}

```{code-tab} r R

goats <- rescue %>%
    sparklyr::filter(animal_group == "Goat")
    
goats %>%
    sparklyr::sdf_nrow()

```
````

```plaintext
[1] 1
```
Just one expensive goat incident! Lets see the description:
````{tabs}

```{code-tab} r R

goat <- goats %>%
    sparklyr::select(incident_number, animal_group, description) %>%
    sparklyr::collect()

goat

```
````

```plaintext
# A tibble: 1 × 3
  incident_number animal_group description                             
  <chr>           <chr>        <chr>                                   
1 72214141        Goat         GOAT TRAPPED BELOW GROUND LEVEL ON LEDGE
```
Note that although we used `collect()` we did not subset the data with `head()`. This is because we know the row count is tiny, and so there was no danger of overloading the driver with too much data.

### Reading data from a Parquet file: `spark_read_parquet()`

The next section covers how to join data in Spark, but before we do, we need to read in another dataset. In our rescue data, we have a column for the postcode district, which represents the first part of the postcode. We have data for the population by postcode in another dataset, `population`.

This data are stored as a parquet file. Parquet files the most efficient way to store data when using Spark. They are compressed and so take up much less storage space, and reading parquet files with Spark is many times quicker than reading CSVs. The drawback is that they are not human readable, although you can store them as a Hive table which means they can easily be interrogated with SQL. See the article on Parquet files for more information.

The syntax for reading in a parquet file is similar to a CSV: [`spark_read_parquet()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark_read_parquet.html). Specify `sc` first, then the `path`. There is no need for the `header` or `infer_schema` argument as unlike CSVs parquet files already have the schema defined. We can then preview the data with `glimpse()`:
````{tabs}

```{code-tab} r R

population_path <- config$population_path
population <- sparklyr::spark_read_parquet(sc, population_path)

pillar::glimpse(population)

```
````

```plaintext
Rows: ??
Columns: 2
Database: spark_connection
$ postcode_district <chr> "DH7", "NW3", "NR4", "SO31", "CT18", "NG2", "NG5", "…
$ population        <dbl> 41076, 52376, 22331, 44742, 14357, 58658, 87171, 435…
```
### Joining Data `left_join()`

Now we have read the population data in, we can join it to the rescue data to get the population by postcode. Left joining in sparklyr uses the dplyr syntax of [`left_join()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/join.tbl_spark.html). Other joins use similar syntax, e.g. [`right_join()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/join.tbl_spark.html), [`inner_join()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/join.tbl_spark.html).

This article assumes that you are familiar with joins. Those who know SQL will be familiar with this term, although pandas and R users sometimes use the term *merge*. If you do not know how a join works, please read about [joins in SQL](https://en.wikipedia.org/wiki/Join_(SQL)) first; the principles are the same in Spark. Joins are expensive in Spark as they involve shuffling the data and this can make larger joins slow. See the article on [Optimising Joins](../spark-concepts/join-concepts) for more information on how to make them more efficient.

Assuming that you are using the pipe notation for the DataFrame on the left hand side of the join, the other 

`left_join()` has three arguments that you will use:
- `x`: the left hand side of the join; this will most frequently be piped in
- `y`: the right hand side of the join; if using pipes this is the first argument
- `by`: which specifies the mapping. Here we have a common column name and so can simply supply the column name. Note that we use a string for the column name. If joining on multiple columns, use a vector.
````{tabs}

```{code-tab} r R

rescue_with_pop <- rescue %>%
    sparklyr::left_join(y=population, by="postcode_district")

```
````
Once again, note how quick this code runs. This is because although a join is an expensive operation, we have only created the plan at this point. We need an action to run the plan and return a result; sort the joined DataFrame, subset the columns and then use `head(5)` to implicitly print:
````{tabs}

```{code-tab} r R

rescue_with_pop <- rescue_with_pop %>%
    sparklyr::sdf_sort(c("incident_number")) %>%
    sparklyr::select(incident_number, animal_group, postcode_district, population)

rescue_with_pop %>%
    head(5)

```
````

```plaintext
# Source: spark<?> [?? x 4]
  incident_number  animal_group                     postcode_district population
  <chr>            <chr>                            <chr>                  <dbl>
1 000014-03092018M Unknown - Heavy Livestock Animal CR8                    32307
2 000099-01012017  Dog                              BR2                    44958
3 000260-01012017  Bird                             CR0                   153812
4 000375-01012017  Dog                              TW8                    20330
5 000477-01012017  Deer                             HA7                    36046
```
### Writing data: file choice

In this article so far, we have been calling actions to preview the data, bringing back only a handful of rows each time. This is useful when developing and debugging code, but in production pipelines you will want to write the results.

The format in which the results are written out depends on what you want to do next with the data:
- If the data are intended to be human readable, e.g. as the basis for a presentation, or as a publication on the ONS website, then you will likely want to output the data as a CSV
- If the data are intended to be used as an input to another Spark process, then use parquet or a Hive table.

There are other use cases, e.g. JSON can be useful if you want the results to analysed with a different programming language, although here we only focus on CSV and parquet. See the article on [Writing Data](../spark-functions/writing-data) for more information.

### Write to a parquet: `spark_write_parquet()`

To write out our DataFrame as a parquet file, use [`spark_write_parquet()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark_write_parquet.html). This has two compulsory arguments: `x`, the DataFrame to be written, and `path`, the path to the file. If using pipes for the DataFrame then you will only need the file path.

The key difference between writing out data with Spark and writing out data with R is that the data will be distributed, which means that multiple files will be created, stored in a parent directory. Spark can read in these parent directories as one DataFrame. There will be one file written out per partition of the DataFrame.
````{tabs}

```{code-tab} r R

output_path_parquet <- config$rescue_with_pop_path_parquet

rescue_with_pop %>%
    sparklyr::spark_write_parquet(output_path_parquet)

```
````
It is worth looking at the raw data that is written out to see that it has been stored in several files in a parent directory.

When reading the data in, Spark will treat every individual file as a partition. See the article on [Managing Partitions](../spark-concepts/partitions) for more information.

### Write to a CSV: `spark_write_csv()` and `sdf_coalesce()`

CSVs will also be written out in a distributed manner as multiple files. While this is desirable in a parquet, it is not very useful with CSV, as the main benefit is to make them human readable. Below are two examples of writing out a Spark dataframe to a CSV file. The first will produce multiple CSV files across multiple partitions, whereas the **second example will write your data to a single partition and is therefore the desired output**.

First, write out the data with [`spark_write_csv()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark_write_csv.html), using the path defined in the config:
````{tabs}

```{code-tab} r R

output_path_csv <- config$rescue_with_pop_path_csv

rescue_with_pop %>%
    sparklyr::spark_write_csv(output_path_csv, header=TRUE)

```
````
Again, look at the raw data in a file browser. You can see that it has written out a directory called `rescue_with_pop.csv`, with multiple files inside. Each of these on their own is a legitimate CSV file, with the correct headers, however it makes it difficult to read your data.

Instead, it is more desirable to reduce the number of partitions and to do this you need to pipe the data into [`sdf_coalesce(partitions)`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_coalesce.html); this will combine existing partitions. Setting `partitions` to `1` will put all of the data on the same partition. 

As the file will already exist (due to the previous example), we need to tell Spark to overwrite the existing file. Use `mode="overwrite"` to do this:
````{tabs}

```{code-tab} r R

rescue_with_pop %>%
    sparklyr::sdf_coalesce(1) %>%
    sparklyr::spark_write_csv(output_path_csv, header=TRUE, mode="overwrite")

```
````
Checking the file again, you can see that although the directory still exists, it will contain only one CSV file.

### Removing files

Spark has no native way of removing files, so either use the standard R methods, or delete them manually through a file browser. If on a local file system, use [`unlink()`](https://stat.ethz.ch/R-manual/R-patched/library/base/html/unlink.html) to delete a file, setting `recursive=TRUE` to remove a directory. If using HDFS or similar, then use [`system()`](https://stat.ethz.ch/R-manual/R-devel/library/base/html/system.html). Be careful when using the `system()` command as you will not get a warning before deleting files.
````{tabs}

```{code-tab} r R

delete_file <- function(file_path){
    cmd <- paste0("hdfs dfs -rm -r -skipTrash ",
                  file_path)
    
    system(cmd,
           ignore.stdout=TRUE,
           ignore.stderr=TRUE)
}

delete_file(output_path_parquet)
delete_file(output_path_csv)

```
````
### Further Resources

Spark at the ONS Code and Articles:
- [R Code used in this article](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/raw-notebooks/sparklyr-intro/r_input.R)
- [Introduction to PySpark](../pyspark-intro/pyspark-intro)
- [Avoiding Module Import Conflicts](../ancillary-topics/module-imports)
- [Guidance on Spark Sessions](../spark-overview/spark-session-guidance)
- [Example Spark Sessions](../spark-overview/example-spark-sessions)
- [Reading Data in sparklyr](../sparklyr-intro/reading-data-sparklyr)
- [Data Storage](../spark-overview/data-storage)
- [Data Types in Spark](../spark-overview/data-types)
- [Spark and Visualisation](../ancillary-topics/visualisation)
- [Spark DataFrames Are Not Ordered](../spark-concepts/df-order)
- [Using Spark functions in sparklyr](../sparklyr-intro/sparklyr-functions)
- [Shuffling](../spark-concepts/shuffling)
- [Optimising Joins](../spark-concepts/join-concepts)
- [Writing Data](../spark-functions/writing-data)
- [Managing Partitions](../spark-concepts/partitions)

[sparklyr](https://spark.rstudio.com/packages/sparklyr/latest/reference/) and [tidyverse](https://www.tidyverse.org/) Documentation:
- [`select()`](https://dplyr.tidyverse.org/reference/select.html)
- [`mutate()`](https://dplyr.tidyverse.org/reference/mutate.html)
- [`filter()`](https://dplyr.tidyverse.org/reference/filter.html)
- [`summarise()`](https://dplyr.tidyverse.org/reference/summarise.html)
- [Spark connections](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark-connections.html): covers `spark_connect()` and `spark_connection_is_open()`
- [`spark_config()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark_config.html)
- [`glimpse()`](https://pillar.r-lib.org/reference/glimpse.html)
- [`collect()`](https://dplyr.tidyverse.org/reference/compute.html)
- [`sdf_nrow()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_dim.html)
- [`rename()`](https://dplyr.tidyverse.org/reference/rename.html)
- [`arrange()`](https://dplyr.tidyverse.org/reference/arrange.html)
- [`sdf_sort()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_sort.html)
- [`desc()`](https://dplyr.tidyverse.org/reference/desc.html)
- [`group_by()`](https://dplyr.tidyverse.org/reference/group_by.html)
- [Joins](https://spark.rstudio.com/packages/sparklyr/latest/reference/join.tbl_spark.html): covers all joins, including `left_join()`, `right_join()` and `inner_join()`
- [`spark_write_parquet()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark_write_parquet.html)
- [`spark_write_csv()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark_write_csv.html)
- [`sdf_coalesce()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_coalesce.html)

Base R Documentation:
- [`class()`](https://stat.ethz.ch/R-manual/R-devel/library/base/html/class.html) 
- [`unlink()`](https://stat.ethz.ch/R-manual/R-patched/library/base/html/unlink.html)
- [`system()`](https://stat.ethz.ch/R-manual/R-devel/library/base/html/system.html)

Spark SQL Functions Documentation:
- [`to_date`](https://spark.apache.org/docs/latest/api/sql/index.html#to_date)
- [`sum`](https://spark.apache.org/docs/latest/api/sql/index.html#sum)
- [`max`](https://spark.apache.org/docs/latest/api/sql/index.html#max)
- [`mean`](https://spark.apache.org/docs/latest/api/sql/index.html#mean)

Other Links:
- [Introduction to dplyr](https://dplyr.tidyverse.org/articles/dplyr.html)
- [R for Data Science](https://r4ds.had.co.nz/)
- [Databricks: Learning Spark](https://pages.databricks.com/rs/094-YMS-629/images/LearningSpark2.0.pdf)
- [SparkR](https://spark.apache.org/docs/latest/sparkr.html): not used at the ONS

#### Acknowledgements

Thanks to Karina Marks, Chris Musselle and Beth Ashlee for creating the initial version of this article.

```python

```
