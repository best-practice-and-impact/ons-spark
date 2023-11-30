# Reading and Writing Data in Spark
This chapter will go into more detail about the various file formats available to use with Spark, and how Spark interacts with these file formats. [Introduction to PySpark](../pyspark-intro/pyspark-intro) and [Introduction to SparklyR](../sparklyr-intro) briefly covered CSV files and Parquet files and some basic differences between them.

This chapter will provide more detail on parquet files, CSV files, ORC files and Avro files, the differences between them and how to read and write data using these formats.

Let's start by setting up a Spark session and reading in the config file.
````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession, functions as F
import yaml

spark = (SparkSession.builder.master("local[2]")
         .appName("reading-data")
         .getOrCreate())

with open("../../../config.yaml") as f:
    config = yaml.safe_load(f)
```

```{code-tab} r R

library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
  master = "local[2]",
  app_name = "ons-spark",
  config = sparklyr::spark_config(),
  )

config <- yaml::yaml.load_file("ons-spark/config.yaml")

```
````
## CSV files
### Reading in a CSV file
To read in a CSV file, you can use [`spark.read.csv()`](https://spark.apache.org/docs/latest/sql-data-sources-csv.html) with PySpark or [`spark_read_csv()`](https://rdrr.io/cran/sparklyr/man/spark_read_csv.html) with SparklyR as demonstrated below.
````{tabs}
```{code-tab} py
animal_rescue = spark.read.csv(config["rescue_path_csv"], header = True, inferSchema = True)
```

```{code-tab} r R

rescue_df <- sparklyr::spark_read_csv(sc,
                                     path = config$rescue_path_csv,
                                     header = TRUE,
                                     infer_schema = TRUE)

```
````
It's important to note the two arguments we have provided to the [`spark.read.csv()`](https://spark.apache.org/docs/latest/sql-data-sources-csv.html) function, `header` and `inferSchema`.

By setting `header` to True, we're saying that we want the top row to be used as the column names. If we did not set this argument to True, then the top rows will be treated as the first row of data, and columns will be given a default name of `_c1`, `_c2`, `_c3` and so on.

`inferSchema` is very important - a disadvantage of using a CSV file is that they are not associated with a schema in the same way parquet files are. By setting `inferSchema` to True, we're allowing the PySpark API to attempt to work out the schemas based on the contents of each column. If this were set to False, then each column would be set to a string datatype by default.

Note that `inferSchema` may not always give the result you're expecting - we can see this in the DateTimeOfCall column in the below code. We may want this as a timestamp type, but it has been read in as a string. 
````{tabs}
```{code-tab} py
animal_rescue.printSchema()
```

```{code-tab} r R

sparklyr::sdf_schema(rescue_df)

```
````

````{tabs}

```{code-tab} plaintext Python Output
root
 |-- IncidentNumber: string (nullable = true)
 |-- DateTimeOfCall: string (nullable = true)
 |-- CalYear: integer (nullable = true)
 |-- FinYear: string (nullable = true)
 |-- TypeOfIncident: string (nullable = true)
 |-- PumpCount: double (nullable = true)
 |-- PumpHoursTotal: double (nullable = true)
 |-- HourlyNotionalCost(£): integer (nullable = true)
 |-- IncidentNotionalCost(£): double (nullable = true)
 |-- FinalDescription: string (nullable = true)
 |-- AnimalGroupParent: string (nullable = true)
 |-- OriginofCall: string (nullable = true)
 |-- PropertyType: string (nullable = true)
 |-- PropertyCategory: string (nullable = true)
 |-- SpecialServiceTypeCategory: string (nullable = true)
 |-- SpecialServiceType: string (nullable = true)
 |-- WardCode: string (nullable = true)
 |-- Ward: string (nullable = true)
 |-- BoroughCode: string (nullable = true)
 |-- Borough: string (nullable = true)
 |-- StnGroundName: string (nullable = true)
 |-- PostcodeDistrict: string (nullable = true)
 |-- Easting_m: double (nullable = true)
 |-- Northing_m: double (nullable = true)
 |-- Easting_rounded: integer (nullable = true)
 |-- Northing_rounded: integer (nullable = true)
```

```{code-tab} plaintext R Output
$IncidentNumber
$IncidentNumber$name
[1] "IncidentNumber"

$IncidentNumber$type
[1] "StringType"


$DateTimeOfCall
$DateTimeOfCall$name
[1] "DateTimeOfCall"

$DateTimeOfCall$type
[1] "StringType"


$CalYear
$CalYear$name
[1] "CalYear"

$CalYear$type
[1] "IntegerType"


$FinYear
$FinYear$name
[1] "FinYear"

$FinYear$type
[1] "StringType"


$TypeOfIncident
$TypeOfIncident$name
[1] "TypeOfIncident"

$TypeOfIncident$type
[1] "StringType"


$PumpCount
$PumpCount$name
[1] "PumpCount"

$PumpCount$type
[1] "DoubleType"


$PumpHoursTotal
$PumpHoursTotal$name
[1] "PumpHoursTotal"

$PumpHoursTotal$type
[1] "DoubleType"


$HourlyNotionalCostGBP
$HourlyNotionalCostGBP$name
[1] "HourlyNotionalCostGBP"

$HourlyNotionalCostGBP$type
[1] "IntegerType"


$IncidentNotionalCostGBP
$IncidentNotionalCostGBP$name
[1] "IncidentNotionalCostGBP"

$IncidentNotionalCostGBP$type
[1] "DoubleType"


$FinalDescription
$FinalDescription$name
[1] "FinalDescription"

$FinalDescription$type
[1] "StringType"


$AnimalGroupParent
$AnimalGroupParent$name
[1] "AnimalGroupParent"

$AnimalGroupParent$type
[1] "StringType"


$OriginofCall
$OriginofCall$name
[1] "OriginofCall"

$OriginofCall$type
[1] "StringType"


$PropertyType
$PropertyType$name
[1] "PropertyType"

$PropertyType$type
[1] "StringType"


$PropertyCategory
$PropertyCategory$name
[1] "PropertyCategory"

$PropertyCategory$type
[1] "StringType"


$SpecialServiceTypeCategory
$SpecialServiceTypeCategory$name
[1] "SpecialServiceTypeCategory"

$SpecialServiceTypeCategory$type
[1] "StringType"


$SpecialServiceType
$SpecialServiceType$name
[1] "SpecialServiceType"

$SpecialServiceType$type
[1] "StringType"


$WardCode
$WardCode$name
[1] "WardCode"

$WardCode$type
[1] "StringType"


$Ward
$Ward$name
[1] "Ward"

$Ward$type
[1] "StringType"


$BoroughCode
$BoroughCode$name
[1] "BoroughCode"

$BoroughCode$type
[1] "StringType"


$Borough
$Borough$name
[1] "Borough"

$Borough$type
[1] "StringType"


$StnGroundName
$StnGroundName$name
[1] "StnGroundName"

$StnGroundName$type
[1] "StringType"


$PostcodeDistrict
$PostcodeDistrict$name
[1] "PostcodeDistrict"

$PostcodeDistrict$type
[1] "StringType"


$Easting_m
$Easting_m$name
[1] "Easting_m"

$Easting_m$type
[1] "DoubleType"


$Northing_m
$Northing_m$name
[1] "Northing_m"

$Northing_m$type
[1] "DoubleType"


$Easting_rounded
$Easting_rounded$name
[1] "Easting_rounded"

$Easting_rounded$type
[1] "IntegerType"


$Northing_rounded
$Northing_rounded$name
[1] "Northing_rounded"

$Northing_rounded$type
[1] "IntegerType"


```
````
To correct this, we can either cast the column as a date type or we can provide a schema when reading it in. 
````{tabs}
```{code-tab} py
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

custom_schema = StructType([
    StructField("IncidentNumber", StringType(), True),
    StructField("DateTimeOfCall", TimestampType(), True),
    StructField("CalYear", IntegerType(), True),
    StructField("TypeOfIncident", StringType(), True),])

animal_rescue_with_schema = spark.read.csv(path = config["rescue_path_csv"], header = True, schema = custom_schema)
animal_rescue_with_schema.printSchema()
```

```{code-tab} r R

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

```
````

````{tabs}

```{code-tab} plaintext Python Output
root
 |-- IncidentNumber: string (nullable = true)
 |-- DateTimeOfCall: timestamp (nullable = true)
 |-- CalYear: integer (nullable = true)
 |-- TypeOfIncident: string (nullable = true)
```

```{code-tab} plaintext R Output
$IncidentNumber
$IncidentNumber$name
[1] "IncidentNumber"

$IncidentNumber$type
[1] "StringType"


$DateTimeOfCall
$DateTimeOfCall$name
[1] "DateTimeOfCall"

$DateTimeOfCall$type
[1] "DateType"


$CalYear
$CalYear$name
[1] "CalYear"

$CalYear$type
[1] "IntegerType"


$TypeOfIncident
$TypeOfIncident$name
[1] "TypeOfIncident"

$TypeOfIncident$type
[1] "StringType"


```
````
We can see that DateTimeOfCall has now been read in correctly as a timestamp type. Also, providing a schema will improve the efficiency of the operation. This is because, in order to infer a schema, Spark needs to scan the dataset. A column could contain, for example, an integer for the first 1000 rows and a string for the 1001th row - in which case it would be inferred as a string, but this wouldn't be obvious from the first 1000 rows. Needing to sample the data in each column can be quite memory intensive. Providing a schema means that Spark no longer has to sample the data before reading it in. 

Generally, if you're using a small dataset it is fine to use infer schema - however if you're reading in a large dataset, it may take considerably longer to read in and this will be repeated after every action (see [Persisting in Spark](../spark-concepts/persistence) to understand why this is the case), potentially increasing your execution time even further. 

### Writing out CSV files
To write a Spark dataframe to a CSV file, you can use the [`.write.csv()`](https://sparkbyexamples.com/pyspark/pyspark-write-dataframe-to-csv-file/) method in PySpark, or [`spark_write_csv()`](https://rdrr.io/cran/sparklyr/man/spark_write_csv.html) in SparklyR, as demonstrated below:
````{tabs}
```{code-tab} py
animal_rescue.write.csv(config["temp_outputs"] + "animal_rescue.csv", header = True, mode = "overwrite")
```

```{code-tab} r R

sparklyr::spark_write_csv(rescue_df,
                         paste0(config$temp_outputs, "animal_rescue_r.csv"),
                         header = TRUE,
                         mode = 'overwrite')


```
````
Note that as mentioned above, we need to specify `header = True` for Spark to treat the top row as column names.

To overwrite an existing file, you can specify `mode = overwrite`. Other writing modes include `append` which appends the data to the pre-existing file file, `ignore` which ignores the write operation if the file already exists or `error` - this is the default and will error if the file already exists.

## Parquet files
### Reading in a parquet file
To read in a parquet file in PySpark, you can use [`spark.read.parquet()`](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) as demonstrated below. For SparklyR, you would use the [`spark_read_parquet()`](https://www.rdocumentation.org/packages/sparklyr/versions/1.8.3/topics/spark_read_parquet) method. 
````{tabs}
```{code-tab} py
animal_rescue = spark.read.parquet(config["rescue_path"])
```

```{code-tab} r R

rescue_df <- sparklyr::spark_read_parquet(sc, path = config$rescue_path)

```
````
As you can see, we didn't have to provide a schema or use the inferSchema argument - this is because parquet files already have a schema associated with them, which is stored in the metadata. 

### Writing out parquet files

To write out a Spark dataframe as a parquet file, you can use [`.write.parquet()`](https://sparkbyexamples.com/pyspark/pyspark-read-and-write-parquet-file/) in PySpark or [`spark_write_parquet()`](https://rdrr.io/cran/sparklyr/man/spark_write_parquet.html) in SparklyR.
````{tabs}
```{code-tab} py
animal_rescue.write.parquet(config["temp_outputs"] + "animal_rescue.parquet", mode = "overwrite")
```

```{code-tab} r R

sparklyr::spark_write_parquet(rescue_df, paste0(config$temp_outputs, "animal_rescue_r.parquet"), mode = 'overwrite')

```
````
There is no need to specify the `header` argument as with CSV files when saving out or reading in a parquet file - because of the metadata associated with parquet files, Spark knows whether something is a column header or not. As with CSV files, you can specify alternative modes for saving out the data including `append` and `ignore`. 

As well as having a schema associated with them, there are other benefits of working with parquet files instead of CSV files. These include:

* Column-based format - parquet files are organised by columns, rather than by row. This allows for better compression and more efficient use of storage space, as columns typically contain similar data types and repeating values. Additionally, when accessing only specific columns, Spark can skip reading in unnecessary data and only read in the columns of interest.
* Predicate pushdown - parquet supports predicate pushdowns, this means if you read in the full dataset and then filter, the filter clause will be "pushed down" to where the data is stored, meaning it can be filtered before it is read in, reducing the amount of memory used to process the data.
* Compression - parquet has built-in compression methods to reduce the required storage space.
* Complex data types - parquet files support complex data types such as nested data.

## Optimised Row Columnar (ORC) files

### Reading in ORC files
To read in an ORC file, using PySpark, you can use [`spark.read.orc()`](https://sparkbyexamples.com/spark/spark-read-orc-file-into-dataframe/). Using SparklyR, you would use [`spark_read_orc()`](https://rdrr.io/cran/sparklyr/man/spark_read_orc.html)
````{tabs}
```{code-tab} py
animal_rescue = spark.read.orc(config["rescue_path_orc"])
```

```{code-tab} r R

rescue_df <- sparklyr::spark_read_orc(sc, path = config$rescue_path_orc)

```
````
### Writing out ORC files
To write out an ORC file, you can use [`dataframe.write.orc()`](https://sparkbyexamples.com/spark/spark-read-orc-file-into-dataframe/) in PySpark or [`spark_write_orc()`](https://rdrr.io/cran/sparklyr/man/spark_write_orc.html) in SparklyR.
````{tabs}
```{code-tab} py
animal_rescue.write.orc(config["temp_outputs"] + "animal_rescue.orc", mode = "overwrite")
```

```{code-tab} r R

sparklyr::spark_write_orc(rescue_df, paste0(config$temp_outputs, "animal_rescue_r.orc"), mode = 'overwrite')

```
````
Again, there is no need to use `inferSchema` or specify a `header` argument, as with a CSV file.

An ORC file shares a lot of the same benefits that a parquet file has over a CSV, including:
* The schema is stored with the file, meaning we don't have to specify a schema
* Column-based formatting
* Predicate pushdown support
* Built-in compression
* Support of complex data types

## Avro files

### Reading in Avro files
To read in an Avro file using PySpark, you can use [`spark.read.format("avro")`](https://sparkbyexamples.com/spark/read-write-avro-file-spark-dataframe/).

Using SparklyR, you will need to install the [`sparkavro`](https://cran.r-project.org/web/packages/sparkavro/) package to use the `spark_read_avro()` function. Note that this function requires three arguments: the spark connection name, a name to assign the newly read in table (in this case "animal_rescue"), and the path to the avro file.
````{tabs}
```{code-tab} py
animal_rescue = spark.read.format("avro").load(config["rescue_path_avro"])
```
```{code-tab} r R
library(sparkavro)

animal_rescue = sparkavro::spark_read_avro(sc, "animal_rescue", config$rescue_path_avro)
```
````
Note that unlike other methods, Spark doesn't have a built in `spark.read.avro()` method so we need to use a slightly different method to read this in, by first specifying the format as "avro" and then using `.load()` to read in the file. Note that this method would also work for other formats, such as `spark.read.format("parquet").load(...)` but is slightly more verbose than the other methods demonstrated. 

### Writing out Avro files
To write out an Avro file, you can use [`dataframe.write.format("avro").save()`](https://sparkbyexamples.com/spark/read-write-avro-file-spark-dataframe/) in PySpark. Note that like the method to read in Avro files, we need to specify the format and then use `save` to specify that we want to save this out.

In SparklyR, we can use the `spark_write_avro()' function. This only requires two arguments; the name of the dataframe to be written to file and the output path. 

````{tabs}
```{code-tab} py
animal_rescue.write.mode("overwrite").format("avro").save(config["temp_outputs"] + "animal_rescue.avro")
```
```{code-tab} r R
sparkavro::spark_write_avro(animal_rescue, paste0(config$temp_outputs, "animal_rescue.avro"), mode = "overwrite")
```
````
Also note that for these methods, we pass in the mode as `write.mode("overwrite")`/`mode = "overwrite"`. 

While an Avro file has many of the benefits associated with parquet and ORC files, such as being associated with a schema and having built-in compression methods, there is one key difference:
An Avro file is row-based, not column-based.

See the section on "Which file type should you use?" for a discussion on row-based formats vs column-based formats.

## Hive tables

### Reading in Hive tables
To read in a Hive table, we can use one of the following approaches:
````{tabs}
```{code-tab} py
#using spark.sql
animal_rescue = spark.sql("SELECT * FROM train_tmp.animal_rescue")

#using spark.read.table
animal_rescue = spark.read.table("train_tmp.animal_rescue")
```

```{code-tab} r R

rescue_df <- sparklyr::sdf_sql(sc, "SELECT * FROM train_tmp.animal_rescue")

```
````
Both of the PySpark methods achieve the same thing - you can use the SQL approach if you want to combine it with additional queries or if you're more familiar with SQL syntax but the spark.read.table approach will achieve the same end result.

One thing to note is that we first specify the database name and then the name of the table. This is because Hive tables are stored within databases. This isn't necessary if you're already in the correct database - you can specify which database you're working in by using [`spark.sql("USE database_name")`](https://spark.apache.org/docs/3.0.0-preview/sql-ref-syntax-qry-select-usedb.html), so we could also read in the dataset using this code:
````{tabs}
```{code-tab} py
spark.sql("USE train_tmp")
animal_rescue = spark.read.table("animal_rescue")
```
````
### Writing out a Hive table
To write out a Hive table, we can use [`dataframe.write.mode('overwrite').saveAsTable()`](https://sparkbyexamples.com/python/pyspark-save-dataframe-to-hive-table/) or [`spark_write_table`](https://rdrr.io/cran/sparklyr/man/spark_write_table.html) in SparklyR. 
````{tabs}
```{code-tab} py
animal_rescue.write.mode('overwrite').saveAsTable("train_tmp.animal_rescue_temp")
```

```{code-tab} r R

sparklyr::spark_write_table(rescue_df, name = "train_tmp.animal_rescue_temp", mode = 'overwrite')

```
````
Again, notice that the output format is database.table_name, although, as mentioned above, we don't need to include the database name if we have ensured we're working in the correct database using `spark.sql("USE database_name)` first.

In many ways, Hive tables have a lot of the same benefits that a parquet file has, such as the storing of schemas and supporting predicate pushdowns. In fact, a Hive table may consist of parquet files - a parquet file is often the default underlying file structure Spark uses when saving out a Hive table. A Hive table can consist of any of the underlying data formats we've discussed in this chapter. The key difference with a Hive table compared to the other data formats is that a Hive table tends to store more detailed metadata.


## So, which file format should I use?
There are a number of factors to consider when deciding which file format to use.

### Do I need my data to be human-readable?

Of the file formats discussed in this chapter, only CSV files are human-readable. This means that if you need to look at the data yourself, i.e. for quality assurance purposes, you would need to ensure that your data is in a CSV file. However, CSV files are generally less efficient to read in than other file formats such as parquet or ORC files, particularly if you only require a subset of the available columns.

It's also important to note that Spark is a big data solution, so if you're only working with only a small amount of data that needs to be manually examined by a human, it may be worth reconsidering whether Spark is needed at all - it could be a good idea to read [when to use Spark](../spark-overview/when-to-use-spark.md).

### Row-based vs columnar-based formats
Generally, both row-based and columnar-based formats are fine to use with Spark. The benefits of choosing one or the other are highly dependent on downstream processing and would likely result in relatively small improvements in processing speed. For a more in-depth discussion on which types of downstream processing would be more suited to a row-based or a columnar-based file format, [this article](https://www.snowflake.com/trending/avro-vs-parquet) may be useful.


### Do you work primarily with databases/SQL?
If you're primarily working with databases/tables within databases and SQL, it may be a good idea to use a Hive table. You can use any format as the underlying data format within a Hive table - so it may be worthwhile reviewing the data formats presented in this chapter to decide which format would be most appropriate for your use case.

## Further resources
[Generic Load/Save Functions - Spark 3.4.1 Documentation](https://spark.apache.org/docs/3.2.0/sql-data-sources-load-save-functions.html)

[CSV Files - Spark 3.4.1 Documentation](https://spark.apache.org/docs/latest/sql-data-sources-csv.html)

[Parquet Files - Spark 3.4.1 Documentation](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)

[ORC Files - Spark 3.4.1 Documentation](https://spark.apache.org/docs/latest/sql-data-sources-orc.html)

[Hive Tables - Spark 3.4.1 Documentation](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html)

[Avro Files - Spark 3.4.1 Documentation](https://spark.apache.org/docs/latest/sql-data-sources-avro.html)
