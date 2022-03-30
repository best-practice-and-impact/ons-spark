## Creating DataFrames Manually

Most Spark DataFrames are created by reading in data from another source, often a parquet file or Hive table, or a CSV file. It is also possible to manually create DataFrames without reading in from another source.

One of the most common cases for manually creating DataFrames is for creating input data and expected output data while writing unit tests; see the [Unit Testing in Spark](../testing-debugging/unit-testing) article for more details.

Remember that Spark DataFrames are processed on the Spark cluster, regardless of if they were read in from another source or created manually.

### Simple one column DataFrames

[`spark.range()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.range.html)/[`sdf_seq()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_seq.html) are functions which create a simple DataFrame with one column, `id`, with the specified number of rows. This can be useful as a starting point for creating synthetic or test data, or for generating a DataFrame containing random numbers.

<details>
    <summary><b>PySpark Explanation</b></summary>

In PySpark, use `spark.range()`. There are two ways this function can be used:
- To create a DF with `id` starting from `0`, just specify the `end` value, e.g. `spark.range(5)`. In common with many other Python operations, the values start from `0` and the end number is not included in the results.
- To create a DF starting from a value other than `0`, then specify a `start` and `end` values, e.g. `spark.range(1, 6)`.

There is also an option for `step`, e.g `spark.range(start=1, end=10, step=2)` will return odd numbers.

</details>

<details>
    <summary><b>sparklyr Explanation</b></summary>
    
In sparklyr, use `sdf_seq()`. The first argument is always the Spark connection object `sc`. The range is supplied with `from` and `to`; note that both `from` and `to` values are included in the output. If `from` is not supplied then it will start from `1`.


There is also an option for the increment, `by`, e.g `sdf_seq(sc, from=1, to=9, by=2)` will return odd numbers.

</details>


As an example, start a Spark session then create a DataFrame with ten rows, and add a column of random numbers:
````{tabs}
```{code-tab} py
import pandas as pd
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[2]")
         .appName("create-DFs")
         .getOrCreate())

seed_no = 100
random_numbers = (spark.range(5)
                  .withColumn("rand_no", F.rand(seed_no)))

random_numbers.show()
```

```{code-tab} r R

library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "create-DFs",
    config = sparklyr::spark_config())

seed_no <- 100L
random_numbers = sparklyr::sdf_seq(sc, 0, 4) %>%
    sparklyr::mutate(rand_no = rand(seed_no))

random_numbers %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+---+-------------------+
| id|            rand_no|
+---+-------------------+
|  0| 0.6841403791584381|
|  1|0.21180593775249568|
|  2| 0.6121482044354868|
|  3| 0.4561043858476006|
|  4| 0.3728419130290753|
+---+-------------------+
```

```{code-tab} plaintext R Output
# A tibble: 5 × 2
     id rand_no
  <int>   <dbl>
1     0   0.684
2     1   0.212
3     2   0.612
4     3   0.456
5     4   0.373
```
````
### Spark DF from pandas/R DF

You can also create Spark DataFrames from pandas or base R DataFrames. Spark DFs are processed in the Spark cluster, which means you have more memory when using Spark, and so some operations may be easier than in the driver, e.g. a join between two pandas/R DataFrames which results in a larger DF.

Remember that there are key differences between pandas/R DFs and Spark DFs. Spark DFs are not ordered by default and also have no index, so converting to Spark and then back will not preserve the original row order. Some operations are also easier with pandas/R than they are with Spark. See the Choosing between pandas/R and Spark article for more information.

In PySpark, use [`spark.createDataFrame(pandas_df)`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.createDataFrame.html), where `pandas_df` is the pandas DataFrame. You can also specify the `schema` argument here, although generally you will not need to as pandas DFs already have data types assigned. Note that if your pandas version is earlier that `0.25.0` there may be a bug when creating the DataFrame due to the column ordering. It is recommended to update to a later version of pandas to solve this.

In sparklyr, use [`sdf_copy_to()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_copy_to.html), with `sc` as the first argument and the base R DF as the second. You can also create a temporary table with the `name` option if desired.

As an example, create a DataFrame of the five Grand National winners between 2017 and 2021 using pandas/R:
````{tabs}
```{code-tab} py
winners_pd = pd.DataFrame(
    {"year": list(range(2017, 2022)),
     "winner": ["Minella Times", None, "Tiger Roll", "Tiger Roll", "One For Arthur"],
     "starting_price": ["11/1", None, "4/1 F", "10/1", "14/1"],
     "age": [8, None, 9, 8, 8],
     "jockey": ["Rachael Blackmore", None, "Davy Russell", "Davy Russell", "Derek Fox"]
})

winners_pd
```

```{code-tab} r R

winners_rdf <- data.frame(
    "year" = 2017:2021,
    "winner" = c("Minella Times", NA, "Tiger Roll", "Tiger Roll", "One For Arthur"),
    "starting_price" = c("11/1", NA, "4/1 F", "10/1", "14/1"),
    "age" = c(8, NA, 9, 8, 8),
    "jockey" = c("Rachael Blackmore", NA, "Davy Russell", "Davy Russell", "Derek Fox")
)

winners_rdf %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
   year          winner starting_price  age             jockey
0  2017   Minella Times           11/1  8.0  Rachael Blackmore
1  2018            None           None  NaN               None
2  2019      Tiger Roll          4/1 F  9.0       Davy Russell
3  2020      Tiger Roll           10/1  8.0       Davy Russell
4  2021  One For Arthur           14/1  8.0          Derek Fox
```

```{code-tab} plaintext R Output
  year         winner starting_price age            jockey
1 2017  Minella Times           11/1   8 Rachael Blackmore
2 2018           <NA>           <NA>  NA              <NA>
3 2019     Tiger Roll          4/1 F   9      Davy Russell
4 2020     Tiger Roll           10/1   8      Davy Russell
5 2021 One For Arthur           14/1   8         Derek Fox
```
````
Then convert this into a Spark DF and preview. Remember that previewing a DataFrame involves collecting data to the driver.
````{tabs}
```{code-tab} py
winners_spark = spark.createDataFrame(winners_pd)
winners_spark.show()
```

```{code-tab} r R

winners_spark <- sparklyr::sdf_copy_to(sc, winners_rdf)

winners_spark %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+----+--------------+--------------+---+-----------------+
|year|        winner|starting_price|age|           jockey|
+----+--------------+--------------+---+-----------------+
|2017| Minella Times|          11/1|8.0|Rachael Blackmore|
|2018|          null|          null|NaN|             null|
|2019|    Tiger Roll|         4/1 F|9.0|     Davy Russell|
|2020|    Tiger Roll|          10/1|8.0|     Davy Russell|
|2021|One For Arthur|          14/1|8.0|        Derek Fox|
+----+--------------+--------------+---+-----------------+
```

```{code-tab} plaintext R Output
# A tibble: 5 × 5
   year winner         starting_price   age jockey           
  <int> <chr>          <chr>          <dbl> <chr>            
1  2017 Minella Times  11/1               8 Rachael Blackmore
2  2018 <NA>           <NA>              NA <NA>             
3  2019 Tiger Roll     4/1 F              9 Davy Russell     
4  2020 Tiger Roll     10/1               8 Davy Russell     
5  2021 One For Arthur 14/1               8 Derek Fox        
```
````
Another issue to be careful with when converting pandas DFs to Spark is the treatment of `null`/`NaN` values. See the article on `null` and `NaN` comparison for more information.

### Create DF directly

In PySpark, as well as converting a pandas DF you can also create a DataFrame directly with [`spark.createDataFrame()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.createDataFrame.html). The first argument is `data`, generally as a regular Python list with each row containing another list. If using this method then you will also want to supply the `schema`, either a list of column names, or an object containing column names and types. See the article on Data Types for more information.

You cannot create a DataFrame in sparklyr in this way; instead, create a base R DF or tibble and use `sdf_copy_to()`, as described above.
````{tabs}
```{code-tab} py
winners_spark = spark.createDataFrame(data=[
    [2021, "Minella Times", "11/1", 8, "Rachael Blackmore"],
    [2020, None, None, None, None],
    [2019, "Tiger Roll", "4/1 F", 9, "Davy Russell"],
    [2018, "Tiger Roll", "10/1", 8, "Davy Russell"],
    [2017, "One For Arthur", "14/1", 8, "Derek Fox"]],
    schema=["year", "winner", "starting_price", "age", "jockey"])

winners_spark.show()
```
````

````{tabs}

```{code-tab} plaintext Python Output
+----+--------------+--------------+----+-----------------+
|year|        winner|starting_price| age|           jockey|
+----+--------------+--------------+----+-----------------+
|2021| Minella Times|          11/1|   8|Rachael Blackmore|
|2020|          null|          null|null|             null|
|2019|    Tiger Roll|         4/1 F|   9|     Davy Russell|
|2018|    Tiger Roll|          10/1|   8|     Davy Russell|
|2017|One For Arthur|          14/1|   8|        Derek Fox|
+----+--------------+--------------+----+-----------------+
```
````
Note that the `age` column for the cancelled `2020` race is `null`, whereas it was `NaN` when converted from pandas.

### Partitions

Although most manually created DataFrames are small, they are still partitioned on the Spark cluster. The number of partitions can be set with `numPartitions` in `spark.range()` and `repartition` in `sdf_seq()`. See the article on Managing Partitions for details of how newly created DataFrames are partitioned.

### Further Resources

Spark at the ONS Articles:
- [Unit Testing in Spark](../testing-debugging/unit-testing)

PySpark Documentation:
- [`spark.createDataFrame()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.createDataFrame.html)
- [`spark.range()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.range.html)

sparklyr and tidyverse Documentation:
- [`sdf_copy_to()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_copy_to.html)
- [`sdf_seq()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_seq.html)
