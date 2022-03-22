## Data Types in Spark

Unlike some languages data types are not explicitly declared in Python or R by default. Instead, the data type is inferred when a value is assigned to a variable. Spark works in a similar way; data types often *can* be explicitly declared but in the absence of this they are inferred. We can have give greater control over the data types by supplying a *schema*, or explicitly casting one data type to another.

Data types are important in Spark and it is worth familiarising yourself with those that are most frequently used.

This article gives an overview of the most common data types and shows how to use schemas and cast a column from one data type to another.

*Data types* in this article refers specifically to the data types of the *columns* of the DataFrame; PySpark and sparklyr DataFrames are of course themselves Python and R objects respectively.

### Importing Data Types

In PySpark, data types are in the [`pyspark.sql.types`](https://spark.apache.org/docs/latest/sql-ref-datatypes.html) module. The documentation uses the `import *` style; we prefer to [import only the data types needed](../ancillary-topics/module-imports), e.g. from `pyspark.sql.types import IntegerType`.

In R, there is no need to import data types, as they can be handled with base R (e.g. [`as.numeric()`](https://stat.ethz.ch/R-manual/R-devel/library/base/html/numeric.html) or a Spark function e.g. [`bigint()`](https://spark.apache.org/docs/latest/api/sql/index.html#bigint).
````{tabs}
```{code-tab} py
# Structural types
from pyspark.sql.types import StructType, StructField

# String type
from pyspark.sql.types import StringType

# Numeric types
from pyspark.sql.types import IntegerType, DecimalType, DoubleType

# Date types
from pyspark.sql.types import DateType, TimestampType
```
````
In order to run our examples, we also need to start a Spark session:
````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession, functions as F
import yaml

with open("../../../config.yaml") as f:
    config = yaml.safe_load(f)

spark = (SparkSession.builder.master("local[2]")
         .appName("data-types")
         .getOrCreate())
```

```{code-tab} r R

library(sparklyr)
library(dplyr)

options(pillar.max_dec_width = 14)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "data-types",
    config = sparklyr::spark_config())

config <- yaml::yaml.load_file("ons-spark/config.yaml")

```
````
### Common Data Types

The [documentation](https://spark.apache.org/docs/latest/sql-ref-datatypes.html) explains all the data types available in Spark. Here we focus on a few common ones and their practical usage, e.g. when you might choose one data type over another.

There are more data types used in PySpark than sparklyr, due to the way that the code is complied in the Spark cluster. Also note that although *factors* are common in R they do not exist in sparklyr as they cannot be mapped to a Spark data type.

#### Numeric types

The choice of numeric type depends on two factors:
- Is the column all whole numbers, or does it contain decimals?
- How large are the values?

Note that there are more available data types in PySpark than sparklyr, so be careful if you use both languages.

````{tabs}
```{tab} Python Explanation

If the column only contains integers, then `IntegerType` or `LongType` will be the most suitable. `IntegerType` has a maximum range of approximately $\pm 2.1 \times 10^9$, so if there is any possibility of the values exceeding this, use `LongType`.

For decimals, you can use often use `DoubleType`. For larger numbers or those with a lot of decimal places, `DecimalType` gives greater precision as you can specify the `precision` and `scale`, e.g. `precision=5` and `scale=2` has values between $\pm 999.99$.

The types given by [`.printSchema()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.printSchema.html) are simpler than the full Spark type name, e.g. `IntegerType` is `integer`.

As a quick example, we can see what happens when a value is too long for `IntegerType`, and also see that the `DecimalType` has a fixed width to the right of the decimal point:
```

```{tab} R Explanation

If the column only contains integers, then `IntegerType` may be the most suitable. `IntegerType` has a maximum range of approximately $\pm 2.1 \times 10^9$, so if there is any possibility of the values exceeding this, use `DoubleType`. Note that `LongType` does not exist in sparklyr; instead, `DoubleType` is used.

`DoubleType` is also used for decimals; there is no `DecimalType` in sparklyr.

The types given by [`glimpse()`](https://pillar.r-lib.org/reference/glimpse.html) are abbreviated, e.g. `IntegerType` is `<int>`. You can see the automatic conversion of data types in the example below:
```
````
````{tabs}
```{code-tab} py
numeric_df = (spark.range(5)
              .withColumn("really_big_number_long", F.col("id") * 10**9)
              .withColumn("really_big_number_int", F.col("really_big_number_long").cast(IntegerType()))
              .withColumn("small_number", (F.col("id") + 9998) / 100)
              .withColumn("small_number_decimal", F.col("small_number").cast(DecimalType(5,2)))
              .drop("id"))

numeric_df.printSchema()
numeric_df.show()
```

```{code-tab} r R

numeric_df <- sparklyr::sdf_seq(sc, 0, 4) %>%
    sparklyr::mutate(
        really_big_number_double = id * 10**9,
        small_number_double = id / 10)

pillar::glimpse(numeric_df)
print(numeric_df)

```
````

````{tabs}

```{code-tab} plaintext Python Output
root
 |-- really_big_number_long: long (nullable = false)
 |-- really_big_number_int: integer (nullable = false)
 |-- small_number: double (nullable = true)
 |-- small_number_decimal: decimal(5,2) (nullable = true)

+----------------------+---------------------+------------+--------------------+
|really_big_number_long|really_big_number_int|small_number|small_number_decimal|
+----------------------+---------------------+------------+--------------------+
|                     0|                    0|       99.98|               99.98|
|            1000000000|           1000000000|       99.99|               99.99|
|            2000000000|           2000000000|       100.0|              100.00|
|            3000000000|          -1294967296|      100.01|              100.01|
|            4000000000|           -294967296|      100.02|              100.02|
+----------------------+---------------------+------------+--------------------+
```

```{code-tab} plaintext R Output
Rows: ??
Columns: 3
Database: spark_connection
$ id                       <int> 0, 1, 2, 3, 4
$ really_big_number_double <dbl> 0e+00, 1e+09, 2e+09, 3e+09, 4e+09
$ small_number_double      <dbl> 0.0, 0.1, 0.2, 0.3, 0.4
# Source: spark<?> [?? x 3]
     id really_big_number_double small_number_double
  <int>                    <dbl>               <dbl>
1     0                        0                 0  
2     1               1000000000                 0.1
3     2               2000000000                 0.2
4     3               3000000000                 0.3
5     4               4000000000                 0.4
```
````
#### String types

`StringType` is the default for character values, and can contain any string. One relatively common scenario that you may encounter is numeric values being stored as strings. See the section on Casting for information on changing data types.

In Spark 3, the fixed character width `CharType` and maximum character width `VarcharType` exist, but not in `pyspark.sql.types`; you will have to use DDL notation for these.

#### Datetime types

The two datetime types are `DateType` and `TimestampType`. `DateType` is easier to read, but is not always supported when writing out data as a Hive table, so `TimestampType` is preferred for storage. See the section on Casting for details of how to convert between the two.

Note that there are differences in how dates are handled in Spark 3 and Spark 2.4. See the [DataBricks blog](https://databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html) for more details.

The defaults when creating a DataFrame in PySpark and sparklyr are also different, as can be seen from the examples:
````{tabs}
```{code-tab} py
from datetime import datetime

dates = (spark
         .createDataFrame([
             ["March", datetime(2022, 3, 1)],
             ["April", datetime(2022, 4, 1)],
             ["May", datetime(2022, 5, 1)]],
             ["month_name", "example_timestamp"])
         .withColumn("example_date",
                     F.col("example_timestamp").cast(DateType())))

dates.show()
dates.printSchema()
```

```{code-tab} r R

dates <- sparklyr::sdf_copy_to(sc, data.frame(
        "month_name" = c("March", "April", "May"),
        "example_date" = lubridate::ymd(
            c("2022-03-01", "2022-04-01", "2022-05-01")))) %>%
    sparklyr::mutate(example_timestamp = to_timestamp(example_date))

pillar::glimpse(dates)
print(dates)

```
````

````{tabs}

```{code-tab} plaintext Python Output
+----------+-------------------+------------+
|month_name|  example_timestamp|example_date|
+----------+-------------------+------------+
|     March|2022-03-01 00:00:00|  2022-03-01|
|     April|2022-04-01 00:00:00|  2022-04-01|
|       May|2022-05-01 00:00:00|  2022-05-01|
+----------+-------------------+------------+

root
 |-- month_name: string (nullable = true)
 |-- example_timestamp: timestamp (nullable = true)
 |-- example_date: date (nullable = true)
```

```{code-tab} plaintext R Output
Rows: ??
Columns: 3
Database: spark_connection
$ month_name        <chr> "March", "April", "May"
$ example_date      <date> 2022-03-01, 2022-04-01, 2022-05-01
$ example_timestamp <dttm> 2022-03-01, 2022-04-01, 2022-05-01
# Source: spark<?> [?? x 3]
  month_name example_date example_timestamp  
  <chr>      <date>       <dttm>             
1 March      2022-03-01   2022-03-01 00:00:00
2 April      2022-04-01   2022-04-01 00:00:00
3 May        2022-05-01   2022-05-01 00:00:00
```
````
#### Other types

Other common types are `BooleanType`; although this is boolean remember that it can also contain null values in addition to `True` and `False`.

For arrays, use `ArrayType`. For more details on arrays, see the [Arrays in PySpark](../spark-functions/arrays.md) article.

### Schemas

The *schema* refers to the structure of the data, in the example of a Spark DataFrame, the column names and data types.

When reading parquet files or Hive tables with Spark the schema is already defined. For instance, We can read the Animal Rescue parquet file and then preview the data types:
````{tabs}
```{code-tab} py
rescue_path_parquet = config["rescue_path"]
rescue_from_parquet = (spark.read.parquet(rescue_path_parquet)
                       .select("incident_number", "date_time_of_call", "cal_year", "fin_year"))

rescue_from_parquet.printSchema()
```

```{code-tab} r R

rescue_from_parquet <- sparklyr::spark_read_parquet(sc, config$rescue_path) %>%
    sparklyr::select(incident_number, date_time_of_call, cal_year, fin_year)

pillar::glimpse(rescue_from_parquet)

```
````

````{tabs}

```{code-tab} plaintext Python Output
root
 |-- incident_number: string (nullable = true)
 |-- date_time_of_call: string (nullable = true)
 |-- cal_year: integer (nullable = true)
 |-- fin_year: string (nullable = true)
```

```{code-tab} plaintext R Output
Rows: ??
Columns: 4
Database: spark_connection
$ incident_number   <chr> "80771131", "141817141", "143166-22102016", "4305114…
$ date_time_of_call <chr> "25/06/2013 07:47", "22/10/2014 17:39", "22/10/2016 …
$ cal_year          <int> 2013, 2014, 2016, 2014, 2013, 2012, 2010, 2018, 2015…
$ fin_year          <chr> "2013/14", "2014/15", "2016/17", "2014/15", "2012/13…
```
````
CSV files (and other text storage formats) do not have any schema attached to them. There are two options for determining the data types in a DataFrame when the source data is a CSV file: use `inferSchema`/`infer_schema`, or supply a schema directly with the `schema`/`columns` option when reading the data in.

Inferring the schema means that Spark will scan the CSV file when reading in and try and automatically determine the data types. This may sometimes not be the exact data type that you want. Scanning the file in this way is also relatively slow, which is one of the reasons why parquet files are a better storage choice for Spark than CSVs.
````{tabs}
```{code-tab} py
rescue_path_csv = config["rescue_path_csv"]
rescue_from_csv = (spark.read.csv(rescue_path_csv, header=True, inferSchema=True)
                   .select("IncidentNumber", "DateTimeOfCall", "CalYear", "FinYear"))

rescue_from_csv.printSchema()
```

```{code-tab} r R

rescue_from_csv <- sparklyr::spark_read_csv(sc, config$rescue_path_csv, header=TRUE, infer_schema=TRUE) %>%
    sparklyr::select(IncidentNumber, DateTimeOfCall, CalYear, FinYear)
    
pillar::glimpse(rescue_from_csv)

```
````

````{tabs}

```{code-tab} plaintext Python Output
root
 |-- IncidentNumber: string (nullable = true)
 |-- DateTimeOfCall: string (nullable = true)
 |-- CalYear: integer (nullable = true)
 |-- FinYear: string (nullable = true)
```

```{code-tab} plaintext R Output
Rows: ??
Columns: 4
Database: spark_connection
$ IncidentNumber <chr> "139091", "275091", "2075091", "2872091", "3553091", "3…
$ DateTimeOfCall <chr> "01/01/2009 03:01", "01/01/2009 08:51", "04/01/2009 10:…
$ CalYear        <int> 2009, 2009, 2009, 2009, 2009, 2009, 2009, 2009, 2009, 2…
$ FinYear        <chr> "2008/09", "2008/09", "2008/09", "2008/09", "2008/09", …
```
````
The alternative is to use the `schema`/`columns` argument to supply a schema directly. This is done with a list of the column names and types. You can also use DDL notation if using PySpark.

In PySpark, supply a list of `StructField` wrapped in `StructType` to `schema`. A `StructField` consists of a column name and type. The types need to be imported from `pyspark.sql.types` and end with brackets, e.g. `StructField("incident_number", StringType())`. 

In sparklyr, use a standard named R list as an input to `columns`, with data types entered as strings.

Note that we are not supplying an entry for every column in the raw data here, just the first four columns.
````{tabs}
```{code-tab} py
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

rescue_schema = StructType([
    StructField("incident_number", StringType()),
    StructField("date_time_of_call", StringType()),
    StructField("cal_year", IntegerType()),
    StructField("fin_year", StringType())
])

rescue_from_csv_schema = spark.read.csv(rescue_path_csv, schema=rescue_schema, inferSchema=False)
rescue_from_csv_schema.printSchema()
```

```{code-tab} r R

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

```
````

````{tabs}

```{code-tab} plaintext Python Output
root
 |-- incident_number: string (nullable = true)
 |-- date_time_of_call: string (nullable = true)
 |-- cal_year: integer (nullable = true)
 |-- fin_year: string (nullable = true)
```

```{code-tab} plaintext R Output
Rows: ??
Columns: 4
Database: spark_connection
$ incident_number   <chr> "139091", "275091", "2075091", "2872091", "3553091",…
$ date_time_of_call <chr> "01/01/2009 03:01", "01/01/2009 08:51", "04/01/2009 …
$ cal_year          <int> 2009, 2009, 2009, 2009, 2009, 2009, 2009, 2009, 2009…
$ fin_year          <chr> "2008/09", "2008/09", "2008/09", "2008/09", "2008/09…
```
````
In PySpark, using Data Definition Language (DDL) to define a schema is generally quicker and easier. You may be familiar with DDL when creating database tables with SQL. Just use the names of the columns followed by their data type and then separated with commas. For ease of reading it is better to use a multi-line string and put each entry on a new line. Remember that multi-line strings in Python need to be opened and closed with `"""`.
````{tabs}
```{code-tab} py
rescue_schema_ddl = """
    `incident_number` string,
    `date_time_of_call` string,
    `cal_year` int,
    `fin_year` string
"""

rescue_from_csv_ddl = spark.read.csv(rescue_path_csv, schema=rescue_schema_ddl)
rescue_from_csv_ddl.printSchema()
```
````

````{tabs}

```{code-tab} plaintext Python Output
root
 |-- incident_number: string (nullable = true)
 |-- date_time_of_call: string (nullable = true)
 |-- cal_year: integer (nullable = true)
 |-- fin_year: string (nullable = true)
```
````
### *Casting*: Changing Data Types

The process of changing data types is referred to as *casting*. For instance, if a string column contains numbers you may want to cast this as an integer.

In PySpark, use the column methods [`.cast()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.cast.html) or [`.astype()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.astype.html). These methods are identical and just aliases of each other. It is good to be consistent within your project as to which one you use.

In sparklyr, casting can be done with either base R methods (when available), e.g. [`as.double()`](https://stat.ethz.ch/R-manual/R-devel/library/base/html/double.html), or [Spark functions](../sparklyr-intro/sparklyr-functions), e.g. [`double()`](https://spark.apache.org/docs/latest/api/sql/index.html#double), [`to_timestamp()`](https://spark.apache.org/docs/latest/api/sql/index.html#to_timestamp). Spark functions are preferred as they are easier for Spark to compile.

Be careful when casting an existing column as this can make the code harder to read and amend. Instead you may want to create a new column to hold the casted value.
````{tabs}
```{code-tab} py
casted_df = (spark.range(5)
             .withColumn("id_double",
                         F.col("id").cast(DoubleType())))
casted_df.printSchema()
casted_df.show()
```

```{code-tab} r R

casted_df <- sparklyr::sdf_seq(sc, 0, 4) %>%
    sparklyr::mutate(id_double = double(id))

pillar::glimpse(casted_df)
print(casted_df)

```
````

````{tabs}

```{code-tab} plaintext Python Output
root
 |-- id: long (nullable = false)
 |-- id_double: double (nullable = false)

+---+---------+
| id|id_double|
+---+---------+
|  0|      0.0|
|  1|      1.0|
|  2|      2.0|
|  3|      3.0|
|  4|      4.0|
+---+---------+
```

```{code-tab} plaintext R Output
Rows: ??
Columns: 2
Database: spark_connection
$ id        <int> 0, 1, 2, 3, 4
$ id_double <dbl> 0, 1, 2, 3, 4
# Source: spark<?> [?? x 2]
     id id_double
  <int>     <dbl>
1     0         0
2     1         1
3     2         2
4     3         3
5     4         4
```
````
### Further Resources

Spark at the ONS Articles:
- [Avoiding Module Import Conflicts](../ancillary-topics/module-imports)
- [Arrays in PySpark](../spark-functions/arrays.md)
- [Using Spark Functions in sparklyr](../sparklyr-intro/sparklyr-functions)

PySpark Documentation:
- [`.printSchema()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.printSchema.html)
- [`pyspark.sql.types`](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)
- [`.cast()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.cast.html)
- [`.astype()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.astype.html)

sparklyr and tidyverse Documentation:
- [`glimpse()`](https://pillar.r-lib.org/reference/glimpse.html)

Base R Documentation:
- [`as.numeric()`](https://stat.ethz.ch/R-manual/R-devel/library/base/html/numeric.html)
- [`as.double()`](https://stat.ethz.ch/R-manual/R-devel/library/base/html/double.html)

[Spark SQL Functions](../sparklyr-intro/sparklyr-functions) Documentation:
- [`bigint()`](https://spark.apache.org/docs/latest/api/sql/index.html#bigint)
- [`double()`](https://spark.apache.org/docs/latest/api/sql/index.html#double)
- [`to_timestamp()`](https://spark.apache.org/docs/latest/api/sql/index.html#to_timestamp)

sparklyr Source Code:
- [dbi_spark_connection.R](https://github.com/sparklyr/sparklyr/blob/eb3e795447887908d9e795512ad08eeeb32eede5/R/dbi_spark_connection.R#L38): shows data type mapping in sparklyr

Other links:
- [DataBricks blog: A Comprehensive Look at Dates and Timestamps in Apache Spark™ 3.0](https://databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html)

#### Acknowledgments

Thanks to Diogo Marques for assistance with the differences between dates in Spark 2.4 and 3.