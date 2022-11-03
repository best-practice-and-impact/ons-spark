## Spark DataFrames Are Not Ordered

Spark DataFrames do not have the order preserved in the same way as pandas or base R DataFrames. Subsetting a pandas or R DataFrame, e.g. with `head()`, will always return identical rows, whereas the rows returned may be different when subsetting PySpark or sparklyr DFs, e.g with [`.show()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.show.html) or `head()`.

The reason for this is that Spark DataFrames are distributed across partitions. Some operations, such as joining, grouping or sorting, cause data to be moved between [partitions](../spark-concepts/partitions), known as a [*shuffle*](../spark-concepts/shuffling). Running the exact same Spark code can result in different data being on different partitions, as well as a different order within each partition.

### Why Spark DFs are not ordered

The lack of an order in a Spark DataFrame is due to two related concepts: partitioning and lazy evaluation.

A Spark DataFrame is distributed across partitions on the Spark cluster, rather than being one complete object stored in the driver memory. Executing an identical Spark plan can result in the same rows going to different partitions. Within these partitions, the rows can also be in a different order.

The lazy evaluation will cause the DataFrame to be re-evaluated each time an action is called. Spark will only do the minimum required to return the result of the action specified, e.g. subsetting and previewing the data with `.show()`/`head() %>% ` [`collect()`](https://dplyr.tidyverse.org/reference/compute.html) may not require evaluation of the whole DataFrame (this can be demonstrated when using [caching](../spark-concepts/cache), as the cache may only be partially filled with these operations). As such, if no order is specified then the data may be returned in a different order, despite the Spark plan being identical. If the data is being subset in a non-specific way, e.g. with `.show()`/`head() %>% collect()`, then different rows may be returned.

The technical term for this is *non-determinism*, which is where the same algorithm can return different results for the same inputs. This can cause problems when regression testing (ensuring that results are identical after a code change) and unit testing your Spark code with [Pytest](../testing-debugging/unit-testing-pyspark) or [testthat](../testing-debugging/unit-testing-sparklyr).

The main lesson from this is that if you want to rely on the order of a Spark DataFrame, you need to [explicitly specify the order](../spark-functions/sorting-data); this may involve using a column as a tie-breaker (e.g. a key column such as an ID, which is unique).

### Does the order really matter?

Before looking at examples, it is worth remembering that often the order of the data does not matter. Ordering data in Spark is an expensive operation as it requires a [shuffle of the data](../spark-concepts/shuffling), so try to avoid sorting if you can. You may want to aim for only ordering the data when [writing out final results](../spark-functions/writing-data) (e.g. to a CSV if you need the result to be human readable).

### An example: different order within same partition

First, create a new Spark session and a small DataFrame; this example uses the winners of the Six Nations rugby union tournament:
````{tabs}
```{code-tab} py
import yaml
from pyspark.sql import SparkSession, functions as F

with open("../../../config.yaml") as f:
    config = yaml.safe_load(f)

spark = (SparkSession.builder.master("local[2]")
         .appName("df-order")
         .config("spark.sql.shuffle.partitions", 12)
         .getOrCreate())

winners = spark.createDataFrame([
    [2022, "France"],
    [2021, "Wales"],
    [2020, "England"],
    [2019, "Wales"],
    [2018, "Ireland"],
    [2017, "England"],
    [2016, "England"],
    [2015, "Ireland"],
    [2014, "Ireland"],
    [2013, "Wales"],
    [2012, "Wales"],
    [2011, "England"],
    [2010, "France"],
    [2009, "Ireland"],
    [2008, "Wales"],
    [2007, "France"],
    [2006, "France"],
    [2005, "Wales"],
    [2004, "France"],
    [2003, "England"],
    [2002, "France"],
    [2001, "England"],
    [2000, "England"],
    ],
    ["year", "nation"]
)
```

```{code-tab} r R

library(sparklyr)
library(dplyr)

config <- yaml::yaml.load_file("ons-spark/config.yaml")

small_config <- sparklyr::spark_config()
small_config$spark.sql.shuffle.partitions <- 12
small_config$spark.sql.shuffle.partitions.local <- 12

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "df-order",
    config = small_config)

winners <- sparklyr::sdf_copy_to(sc,
    data.frame(
        "year" = 2022:2000,
            "nation" = c(
                "France",
                "Wales",
                "England",
                "Wales",
                "Ireland",
                rep("England", 2),
                rep("Ireland", 2),
                rep("Wales", 2),
                "England",
                "France",
                "Ireland",
                "Wales",
                rep("France", 2),
                "Wales",
                "France",
                "England",
                "France",
                rep("England", 2))),
    repartition=2)

```
````
Now order the data by `nation`. This will ensure that the first nation alphabetically will be returned (`England`), but as the `year` is not specified these may be returned in a different order.

To demonstrate that these rows are all on the same partition, create a new column using [`F.spark_partition_id()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.spark_partition_id.html)/[`spark_partition_id()`](https://spark.apache.org/docs/latest/api/sql/index.html#spark_partition_id):
````{tabs}
```{code-tab} py
winners_ordered = (winners
                   .orderBy("nation")
                   .withColumn("partition_id", F.spark_partition_id()))
```

```{code-tab} r R

winners_ordered <- winners %>%
    sparklyr::sdf_sort(c("nation")) %>%
    sparklyr::mutate(partition_id = spark_partition_id())

```
````
Now preview this DataFrame several times. Remember that in Spark, the whole plan will be processed, so it will create and order the DataFrame by `nation` each time, but not the `year`, and so this may be different:
````{tabs}
```{code-tab} py
for show_no in range(3):
    winners_ordered.show(5)
```

```{code-tab} r R

for(show_no in 1:3){
    winners_ordered %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()
}

```
````

````{tabs}

```{code-tab} plaintext Python Output
+----+-------+------------+
|year| nation|partition_id|
+----+-------+------------+
|2020|England|           0|
|2017|England|           0|
|2016|England|           0|
|2011|England|           0|
|2003|England|           0|
+----+-------+------------+
only showing top 5 rows

+----+-------+------------+
|year| nation|partition_id|
+----+-------+------------+
|2011|England|           0|
|2020|England|           0|
|2016|England|           0|
|2017|England|           0|
|2003|England|           0|
+----+-------+------------+
only showing top 5 rows

+----+-------+------------+
|year| nation|partition_id|
+----+-------+------------+
|2020|England|           0|
|2017|England|           0|
|2016|England|           0|
|2011|England|           0|
|2003|England|           0|
+----+-------+------------+
only showing top 5 rows
```

```{code-tab} plaintext R Output
# A tibble: 5 × 3
   year nation  partition_id
  <int> <chr>          <int>
1  2001 England            0
2  2011 England            0
3  2003 England            0
4  2000 England            0
5  2016 England            0
# A tibble: 5 × 3
   year nation  partition_id
  <int> <chr>          <int>
1  2001 England            0
2  2011 England            0
3  2003 England            0
4  2000 England            0
5  2016 England            0
# A tibble: 5 × 3
   year nation  partition_id
  <int> <chr>          <int>
1  2011 England            0
2  2020 England            0
3  2016 England            0
4  2017 England            0
5  2003 England            0
```
````
The results not all the same. The data in the `nation` column is identical every time, as this was explicitly ordered, but the `year` is not.

This demonstrates that the data is being returned in `partition_id` order for each `nation`, but there is no fixed ordering within each partition. If the DataFrame was also sorted by `year` the same result would be returned each time.

Note that if you run the code again, you may get different results.

### Another example: different `partition_id`

During a shuffle the data can be sent to different partitions when executing the same Spark plan multiple times.

First, import the Animal Rescue CSV data and do some data cleansing. The data is also being sorted by `AnimalGroup`; this causes a shuffle which will repartition the data:
````{tabs}
```{code-tab} py
rescue_path_csv = config["rescue_path_csv"]
rescue = (spark
          .read.csv(rescue_path_csv, header=True, inferSchema=True)
          .withColumnRenamed("IncidentNumber", "incident_number")
          .withColumnRenamed("AnimalGroupParent", "animal_group")
          .select("incident_number", "animal_group")
          .orderBy("animal_group")
         )
```

```{code-tab} r R

rescue <- sparklyr::spark_read_csv(sc,
                                   config$rescue_path_csv,
                                   header=TRUE,
                                   infer_schema=TRUE) %>%
    dplyr:::rename(
        incident_number = IncidentNumber,
        animal_group = AnimalGroupParent) %>%
    sparklyr::select(incident_number, animal_group) %>%
    sparklyr::sdf_sort(c("animal_group"))

```
````
Now group the data by `animal_group`, which will cause another shuffle, count how many of each animal there are, and return `Fox`, `Goat` and `Hamster`, then show the result multiple times:
````{tabs}
```{code-tab} py
animal_counts = (rescue
                 .groupBy("animal_group")
                 .count()
                 .withColumn("partition_id", F.spark_partition_id())
                 .filter(F.col("animal_group").isin("Fox", "Goat", "Hamster")))

for show_no in range(3):
    animal_counts.show()
```

```{code-tab} r R

animal_counts <- rescue %>%
    dplyr::group_by(animal_group) %>%
    dplyr::summarise(count = n()) %>%
    sparklyr::mutate(partition_id = spark_partition_id()) %>%
    sparklyr::filter(animal_group %in% c("Fox", "Goat", "Hamster"))

for(show_no in 1:3){
    animal_counts %>%
    sparklyr::collect() %>%
    print()
}

```
````

````{tabs}

```{code-tab} plaintext Python Output
+------------+-----+------------+
|animal_group|count|partition_id|
+------------+-----+------------+
|         Fox|  238|           7|
|        Goat|    1|           8|
|     Hamster|   14|           9|
+------------+-----+------------+

+------------+-----+------------+
|animal_group|count|partition_id|
+------------+-----+------------+
|         Fox|  238|           8|
|        Goat|    1|           9|
|     Hamster|   14|          10|
+------------+-----+------------+

+------------+-----+------------+
|animal_group|count|partition_id|
+------------+-----+------------+
|         Fox|  238|           8|
|        Goat|    1|           9|
|     Hamster|   14|          10|
+------------+-----+------------+
```

```{code-tab} plaintext R Output
# A tibble: 3 × 3
  animal_group count partition_id
  <chr>        <dbl>        <int>
1 Fox            238            8
2 Goat             1            9
3 Hamster         14           10
# A tibble: 3 × 3
  animal_group count partition_id
  <chr>        <dbl>        <int>
1 Fox            238            8
2 Goat             1            9
3 Hamster         14            9
# A tibble: 3 × 3
  animal_group count partition_id
  <chr>        <dbl>        <int>
1 Fox            238            7
2 Goat             1            8
3 Hamster         14            8
```
````
Although the same plan is being executed each time, the `partition_id` can be different. The `partition_id` is allocated by the [`.groupBy()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)/[`group_by()`](https://dplyr.tidyverse.org/reference/group_by.html) operation, which causes a shuffle.

### Final thoughts

Remember that sorting the data causes a [shuffle](../spark-concepts/shuffling), which is an expensive operation. Try and only sort data when necessary.

Sometimes you do want your code to be non-deterministic, e.g. when using random numbers. You can set a seed to replicate results if needed. You may also want to use [mocking](../testing-debugging/unit-testing-pyspark.html#mocking) if unit testing in PySpark with random numbers.

### Further Resources

Spark at the ONS Articles:
- [Managing Partitions](../spark-concepts/partitions)
- [Shuffling](../spark-concepts/shuffling)
- [Sorting Spark DataFrames](../spark-functions/sorting-data)
- [Unit Testing in PySpark](../testing-debugging/unit-testing-pyspark)
    - [Mocking](../testing-debugging/unit-testing-pyspark.html#mocking)
- [Unit Testing in sparklyr](../testing-debugging/unit-testing-sparklyr)
- [Caching](../spark-concepts/cache)
- [Writing Data](../spark-functions/writing-data)

PySpark Documentation:
- [`.show()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.show.html)
- [`F.spark_partition_id()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.spark_partition_id.html)
- [`.groupBy()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)

sparklyr and tidyverse Documentation:
- [`group_by()`](https://dplyr.tidyverse.org/reference/group_by.html)
- [`collect()`](https://dplyr.tidyverse.org/reference/compute.html)

Spark SQL Functions Documentation:
- [`spark_partition_id`](https://spark.apache.org/docs/latest/api/sql/index.html#spark_partition_id)
<font size = 12>
/n </font>## Spark DataFrames Are Not Ordered

Spark DataFrames do not have the order preserved in the same way as pandas or base R DataFrames. Subsetting a pandas or R DataFrame, e.g. with `head()`, will always return identical rows, whereas the rows returned may be different when subsetting PySpark or sparklyr DFs, e.g with [`.show()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.show.html) or `head()`.

The reason for this is that Spark DataFrames are distributed across partitions. Some operations, such as joining, grouping or sorting, cause data to be moved between [partitions](../spark-concepts/partitions), known as a [*shuffle*](../spark-concepts/shuffling). Running the exact same Spark code can result in different data being on different partitions, as well as a different order within each partition.

### Why Spark DFs are not ordered

The lack of an order in a Spark DataFrame is due to two related concepts: partitioning and lazy evaluation.

A Spark DataFrame is distributed across partitions on the Spark cluster, rather than being one complete object stored in the driver memory. Executing an identical Spark plan can result in the same rows going to different partitions. Within these partitions, the rows can also be in a different order.

The lazy evaluation will cause the DataFrame to be re-evaluated each time an action is called. Spark will only do the minimum required to return the result of the action specified, e.g. subsetting and previewing the data with `.show()`/`head() %>% ` [`collect()`](https://dplyr.tidyverse.org/reference/compute.html) may not require evaluation of the whole DataFrame (this can be demonstrated when using [caching](../spark-concepts/cache), as the cache may only be partially filled with these operations). As such, if no order is specified then the data may be returned in a different order, despite the Spark plan being identical. If the data is being subset in a non-specific way, e.g. with `.show()`/`head() %>% collect()`, then different rows may be returned.

The technical term for this is *non-determinism*, which is where the same algorithm can return different results for the same inputs. This can cause problems when regression testing (ensuring that results are identical after a code change) and unit testing your Spark code with [Pytest](../testing-debugging/unit-testing-pyspark) or [testthat](../testing-debugging/unit-testing-sparklyr).

The main lesson from this is that if you want to rely on the order of a Spark DataFrame, you need to [explicitly specify the order](../spark-functions/sorting-data); this may involve using a column as a tie-breaker (e.g. a key column such as an ID, which is unique).

### Does the order really matter?

Before looking at examples, it is worth remembering that often the order of the data does not matter. Ordering data in Spark is an expensive operation as it requires a [shuffle of the data](../spark-concepts/shuffling), so try to avoid sorting if you can. You may want to aim for only ordering the data when [writing out final results](../spark-functions/writing-data) (e.g. to a CSV if you need the result to be human readable).

### An example: different order within same partition

First, create a new Spark session and a small DataFrame; this example uses the winners of the Six Nations rugby union tournament:
````{tabs}
```{code-tab} py
import yaml
from pyspark.sql import SparkSession, functions as F

with open("../../../config.yaml") as f:
    config = yaml.safe_load(f)

spark = (SparkSession.builder.master("local[2]")
         .appName("df-order")
         .config("spark.sql.shuffle.partitions", 12)
         .getOrCreate())

winners = spark.createDataFrame([
    [2022, "France"],
    [2021, "Wales"],
    [2020, "England"],
    [2019, "Wales"],
    [2018, "Ireland"],
    [2017, "England"],
    [2016, "England"],
    [2015, "Ireland"],
    [2014, "Ireland"],
    [2013, "Wales"],
    [2012, "Wales"],
    [2011, "England"],
    [2010, "France"],
    [2009, "Ireland"],
    [2008, "Wales"],
    [2007, "France"],
    [2006, "France"],
    [2005, "Wales"],
    [2004, "France"],
    [2003, "England"],
    [2002, "France"],
    [2001, "England"],
    [2000, "England"],
    ],
    ["year", "nation"]
)
```

```{code-tab} r R

library(sparklyr)
library(dplyr)

config <- yaml::yaml.load_file("ons-spark/config.yaml")

small_config <- sparklyr::spark_config()
small_config$spark.sql.shuffle.partitions <- 12
small_config$spark.sql.shuffle.partitions.local <- 12

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "df-order",
    config = small_config)

winners <- sparklyr::sdf_copy_to(sc,
    data.frame(
        "year" = 2022:2000,
            "nation" = c(
                "France",
                "Wales",
                "England",
                "Wales",
                "Ireland",
                rep("England", 2),
                rep("Ireland", 2),
                rep("Wales", 2),
                "England",
                "France",
                "Ireland",
                "Wales",
                rep("France", 2),
                "Wales",
                "France",
                "England",
                "France",
                rep("England", 2))),
    repartition=2)

```
````
Now order the data by `nation`. This will ensure that the first nation alphabetically will be returned (`England`), but as the `year` is not specified these may be returned in a different order.

To demonstrate that these rows are all on the same partition, create a new column using [`F.spark_partition_id()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.spark_partition_id.html)/[`spark_partition_id()`](https://spark.apache.org/docs/latest/api/sql/index.html#spark_partition_id):
````{tabs}
```{code-tab} py
winners_ordered = (winners
                   .orderBy("nation")
                   .withColumn("partition_id", F.spark_partition_id()))
```

```{code-tab} r R

winners_ordered <- winners %>%
    sparklyr::sdf_sort(c("nation")) %>%
    sparklyr::mutate(partition_id = spark_partition_id())

```
````
Now preview this DataFrame several times. Remember that in Spark, the whole plan will be processed, so it will create and order the DataFrame by `nation` each time, but not the `year`, and so this may be different:
````{tabs}
```{code-tab} py
for show_no in range(3):
    winners_ordered.show(5)
```

```{code-tab} r R

for(show_no in 1:3){
    winners_ordered %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()
}

```
````

````{tabs}

```{code-tab} plaintext Python Output
+----+-------+------------+
|year| nation|partition_id|
+----+-------+------------+
|2020|England|           0|
|2017|England|           0|
|2016|England|           0|
|2011|England|           0|
|2003|England|           0|
+----+-------+------------+
only showing top 5 rows

+----+-------+------------+
|year| nation|partition_id|
+----+-------+------------+
|2011|England|           0|
|2020|England|           0|
|2016|England|           0|
|2017|England|           0|
|2003|England|           0|
+----+-------+------------+
only showing top 5 rows

+----+-------+------------+
|year| nation|partition_id|
+----+-------+------------+
|2020|England|           0|
|2017|England|           0|
|2016|England|           0|
|2011|England|           0|
|2003|England|           0|
+----+-------+------------+
only showing top 5 rows
```

```{code-tab} plaintext R Output
# A tibble: 5 × 3
   year nation  partition_id
  <int> <chr>          <int>
1  2001 England            0
2  2011 England            0
3  2003 England            0
4  2000 England            0
5  2016 England            0
# A tibble: 5 × 3
   year nation  partition_id
  <int> <chr>          <int>
1  2001 England            0
2  2011 England            0
3  2003 England            0
4  2000 England            0
5  2016 England            0
# A tibble: 5 × 3
   year nation  partition_id
  <int> <chr>          <int>
1  2011 England            0
2  2020 England            0
3  2016 England            0
4  2017 England            0
5  2003 England            0
```
````
The results not all the same. The data in the `nation` column is identical every time, as this was explicitly ordered, but the `year` is not.

This demonstrates that the data is being returned in `partition_id` order for each `nation`, but there is no fixed ordering within each partition. If the DataFrame was also sorted by `year` the same result would be returned each time.

Note that if you run the code again, you may get different results.

### Another example: different `partition_id`

During a shuffle the data can be sent to different partitions when executing the same Spark plan multiple times.

First, import the Animal Rescue CSV data and do some data cleansing. The data is also being sorted by `AnimalGroup`; this causes a shuffle which will repartition the data:
````{tabs}
```{code-tab} py
rescue_path_csv = config["rescue_path_csv"]
rescue = (spark
          .read.csv(rescue_path_csv, header=True, inferSchema=True)
          .withColumnRenamed("IncidentNumber", "incident_number")
          .withColumnRenamed("AnimalGroupParent", "animal_group")
          .select("incident_number", "animal_group")
          .orderBy("animal_group")
         )
```

```{code-tab} r R

rescue <- sparklyr::spark_read_csv(sc,
                                   config$rescue_path_csv,
                                   header=TRUE,
                                   infer_schema=TRUE) %>%
    dplyr:::rename(
        incident_number = IncidentNumber,
        animal_group = AnimalGroupParent) %>%
    sparklyr::select(incident_number, animal_group) %>%
    sparklyr::sdf_sort(c("animal_group"))

```
````
Now group the data by `animal_group`, which will cause another shuffle, count how many of each animal there are, and return `Fox`, `Goat` and `Hamster`, then show the result multiple times:
````{tabs}
```{code-tab} py
animal_counts = (rescue
                 .groupBy("animal_group")
                 .count()
                 .withColumn("partition_id", F.spark_partition_id())
                 .filter(F.col("animal_group").isin("Fox", "Goat", "Hamster")))

for show_no in range(3):
    animal_counts.show()
```

```{code-tab} r R

animal_counts <- rescue %>%
    dplyr::group_by(animal_group) %>%
    dplyr::summarise(count = n()) %>%
    sparklyr::mutate(partition_id = spark_partition_id()) %>%
    sparklyr::filter(animal_group %in% c("Fox", "Goat", "Hamster"))

for(show_no in 1:3){
    animal_counts %>%
    sparklyr::collect() %>%
    print()
}

```
````

````{tabs}

```{code-tab} plaintext Python Output
+------------+-----+------------+
|animal_group|count|partition_id|
+------------+-----+------------+
|         Fox|  238|           7|
|        Goat|    1|           8|
|     Hamster|   14|           9|
+------------+-----+------------+

+------------+-----+------------+
|animal_group|count|partition_id|
+------------+-----+------------+
|         Fox|  238|           8|
|        Goat|    1|           9|
|     Hamster|   14|          10|
+------------+-----+------------+

+------------+-----+------------+
|animal_group|count|partition_id|
+------------+-----+------------+
|         Fox|  238|           8|
|        Goat|    1|           9|
|     Hamster|   14|          10|
+------------+-----+------------+
```

```{code-tab} plaintext R Output
# A tibble: 3 × 3
  animal_group count partition_id
  <chr>        <dbl>        <int>
1 Fox            238            8
2 Goat             1            9
3 Hamster         14           10
# A tibble: 3 × 3
  animal_group count partition_id
  <chr>        <dbl>        <int>
1 Fox            238            8
2 Goat             1            9
3 Hamster         14            9
# A tibble: 3 × 3
  animal_group count partition_id
  <chr>        <dbl>        <int>
1 Fox            238            7
2 Goat             1            8
3 Hamster         14            8
```
````
Although the same plan is being executed each time, the `partition_id` can be different. The `partition_id` is allocated by the [`.groupBy()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)/[`group_by()`](https://dplyr.tidyverse.org/reference/group_by.html) operation, which causes a shuffle.

### Final thoughts

Remember that sorting the data causes a [shuffle](../spark-concepts/shuffling), which is an expensive operation. Try and only sort data when necessary.

Sometimes you do want your code to be non-deterministic, e.g. when using random numbers. You can set a seed to replicate results if needed. You may also want to use [mocking](../testing-debugging/unit-testing-pyspark.html#mocking) if unit testing in PySpark with random numbers.

### Further Resources

Spark at the ONS Articles:
- [Managing Partitions](../spark-concepts/partitions)
- [Shuffling](../spark-concepts/shuffling)
- [Sorting Spark DataFrames](../spark-functions/sorting-data)
- [Unit Testing in PySpark](../testing-debugging/unit-testing-pyspark)
    - [Mocking](../testing-debugging/unit-testing-pyspark.html#mocking)
- [Unit Testing in sparklyr](../testing-debugging/unit-testing-sparklyr)
- [Caching](../spark-concepts/cache)
- [Writing Data](../spark-functions/writing-data)

PySpark Documentation:
- [`.show()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.show.html)
- [`F.spark_partition_id()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.spark_partition_id.html)
- [`.groupBy()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)

sparklyr and tidyverse Documentation:
- [`group_by()`](https://dplyr.tidyverse.org/reference/group_by.html)
- [`collect()`](https://dplyr.tidyverse.org/reference/compute.html)

Spark SQL Functions Documentation:
- [`spark_partition_id`](https://spark.apache.org/docs/latest/api/sql/index.html#spark_partition_id)
<font size = 12>
/n </font>## Spark DataFrames Are Not Ordered

Spark DataFrames do not have the order preserved in the same way as pandas or base R DataFrames. Subsetting a pandas or R DataFrame, e.g. with `head()`, will always return identical rows, whereas the rows returned may be different when subsetting PySpark or sparklyr DFs, e.g with [`.show()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.show.html) or `head()`.

The reason for this is that Spark DataFrames are distributed across partitions. Some operations, such as joining, grouping or sorting, cause data to be moved between [partitions](../spark-concepts/partitions), known as a [*shuffle*](../spark-concepts/shuffling). Running the exact same Spark code can result in different data being on different partitions, as well as a different order within each partition.

### Why Spark DFs are not ordered

The lack of an order in a Spark DataFrame is due to two related concepts: partitioning and lazy evaluation.

A Spark DataFrame is distributed across partitions on the Spark cluster, rather than being one complete object stored in the driver memory. Executing an identical Spark plan can result in the same rows going to different partitions. Within these partitions, the rows can also be in a different order.

The lazy evaluation will cause the DataFrame to be re-evaluated each time an action is called. Spark will only do the minimum required to return the result of the action specified, e.g. subsetting and previewing the data with `.show()`/`head() %>% ` [`collect()`](https://dplyr.tidyverse.org/reference/compute.html) may not require evaluation of the whole DataFrame (this can be demonstrated when using [caching](../spark-concepts/cache), as the cache may only be partially filled with these operations). As such, if no order is specified then the data may be returned in a different order, despite the Spark plan being identical. If the data is being subset in a non-specific way, e.g. with `.show()`/`head() %>% collect()`, then different rows may be returned.

The technical term for this is *non-determinism*, which is where the same algorithm can return different results for the same inputs. This can cause problems when regression testing (ensuring that results are identical after a code change) and unit testing your Spark code with [Pytest](../testing-debugging/unit-testing-pyspark) or [testthat](../testing-debugging/unit-testing-sparklyr).

The main lesson from this is that if you want to rely on the order of a Spark DataFrame, you need to [explicitly specify the order](../spark-functions/sorting-data); this may involve using a column as a tie-breaker (e.g. a key column such as an ID, which is unique).

### Does the order really matter?

Before looking at examples, it is worth remembering that often the order of the data does not matter. Ordering data in Spark is an expensive operation as it requires a [shuffle of the data](../spark-concepts/shuffling), so try to avoid sorting if you can. You may want to aim for only ordering the data when [writing out final results](../spark-functions/writing-data) (e.g. to a CSV if you need the result to be human readable).

### An example: different order within same partition

First, create a new Spark session and a small DataFrame; this example uses the winners of the Six Nations rugby union tournament:
````{tabs}
```{code-tab} py
import yaml
from pyspark.sql import SparkSession, functions as F

with open("../../../config.yaml") as f:
    config = yaml.safe_load(f)

spark = (SparkSession.builder.master("local[2]")
         .appName("df-order")
         .config("spark.sql.shuffle.partitions", 12)
         .getOrCreate())

winners = spark.createDataFrame([
    [2022, "France"],
    [2021, "Wales"],
    [2020, "England"],
    [2019, "Wales"],
    [2018, "Ireland"],
    [2017, "England"],
    [2016, "England"],
    [2015, "Ireland"],
    [2014, "Ireland"],
    [2013, "Wales"],
    [2012, "Wales"],
    [2011, "England"],
    [2010, "France"],
    [2009, "Ireland"],
    [2008, "Wales"],
    [2007, "France"],
    [2006, "France"],
    [2005, "Wales"],
    [2004, "France"],
    [2003, "England"],
    [2002, "France"],
    [2001, "England"],
    [2000, "England"],
    ],
    ["year", "nation"]
)
```

```{code-tab} r R

library(sparklyr)
library(dplyr)

config <- yaml::yaml.load_file("ons-spark/config.yaml")

small_config <- sparklyr::spark_config()
small_config$spark.sql.shuffle.partitions <- 12
small_config$spark.sql.shuffle.partitions.local <- 12

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "df-order",
    config = small_config)

winners <- sparklyr::sdf_copy_to(sc,
    data.frame(
        "year" = 2022:2000,
            "nation" = c(
                "France",
                "Wales",
                "England",
                "Wales",
                "Ireland",
                rep("England", 2),
                rep("Ireland", 2),
                rep("Wales", 2),
                "England",
                "France",
                "Ireland",
                "Wales",
                rep("France", 2),
                "Wales",
                "France",
                "England",
                "France",
                rep("England", 2))),
    repartition=2)

```
````
Now order the data by `nation`. This will ensure that the first nation alphabetically will be returned (`England`), but as the `year` is not specified these may be returned in a different order.

To demonstrate that these rows are all on the same partition, create a new column using [`F.spark_partition_id()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.spark_partition_id.html)/[`spark_partition_id()`](https://spark.apache.org/docs/latest/api/sql/index.html#spark_partition_id):
````{tabs}
```{code-tab} py
winners_ordered = (winners
                   .orderBy("nation")
                   .withColumn("partition_id", F.spark_partition_id()))
```

```{code-tab} r R

winners_ordered <- winners %>%
    sparklyr::sdf_sort(c("nation")) %>%
    sparklyr::mutate(partition_id = spark_partition_id())

```
````
Now preview this DataFrame several times. Remember that in Spark, the whole plan will be processed, so it will create and order the DataFrame by `nation` each time, but not the `year`, and so this may be different:
````{tabs}
```{code-tab} py
for show_no in range(3):
    winners_ordered.show(5)
```

```{code-tab} r R

for(show_no in 1:3){
    winners_ordered %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()
}

```
````

````{tabs}

```{code-tab} plaintext Python Output
+----+-------+------------+
|year| nation|partition_id|
+----+-------+------------+
|2020|England|           0|
|2017|England|           0|
|2016|England|           0|
|2011|England|           0|
|2003|England|           0|
+----+-------+------------+
only showing top 5 rows

+----+-------+------------+
|year| nation|partition_id|
+----+-------+------------+
|2011|England|           0|
|2020|England|           0|
|2016|England|           0|
|2017|England|           0|
|2003|England|           0|
+----+-------+------------+
only showing top 5 rows

+----+-------+------------+
|year| nation|partition_id|
+----+-------+------------+
|2020|England|           0|
|2017|England|           0|
|2016|England|           0|
|2011|England|           0|
|2003|England|           0|
+----+-------+------------+
only showing top 5 rows
```

```{code-tab} plaintext R Output
# A tibble: 5 × 3
   year nation  partition_id
  <int> <chr>          <int>
1  2001 England            0
2  2011 England            0
3  2003 England            0
4  2000 England            0
5  2016 England            0
# A tibble: 5 × 3
   year nation  partition_id
  <int> <chr>          <int>
1  2001 England            0
2  2011 England            0
3  2003 England            0
4  2000 England            0
5  2016 England            0
# A tibble: 5 × 3
   year nation  partition_id
  <int> <chr>          <int>
1  2011 England            0
2  2020 England            0
3  2016 England            0
4  2017 England            0
5  2003 England            0
```
````
The results not all the same. The data in the `nation` column is identical every time, as this was explicitly ordered, but the `year` is not.

This demonstrates that the data is being returned in `partition_id` order for each `nation`, but there is no fixed ordering within each partition. If the DataFrame was also sorted by `year` the same result would be returned each time.

Note that if you run the code again, you may get different results.

### Another example: different `partition_id`

During a shuffle the data can be sent to different partitions when executing the same Spark plan multiple times.

First, import the Animal Rescue CSV data and do some data cleansing. The data is also being sorted by `AnimalGroup`; this causes a shuffle which will repartition the data:
````{tabs}
```{code-tab} py
rescue_path_csv = config["rescue_path_csv"]
rescue = (spark
          .read.csv(rescue_path_csv, header=True, inferSchema=True)
          .withColumnRenamed("IncidentNumber", "incident_number")
          .withColumnRenamed("AnimalGroupParent", "animal_group")
          .select("incident_number", "animal_group")
          .orderBy("animal_group")
         )
```

```{code-tab} r R

rescue <- sparklyr::spark_read_csv(sc,
                                   config$rescue_path_csv,
                                   header=TRUE,
                                   infer_schema=TRUE) %>%
    dplyr:::rename(
        incident_number = IncidentNumber,
        animal_group = AnimalGroupParent) %>%
    sparklyr::select(incident_number, animal_group) %>%
    sparklyr::sdf_sort(c("animal_group"))

```
````
Now group the data by `animal_group`, which will cause another shuffle, count how many of each animal there are, and return `Fox`, `Goat` and `Hamster`, then show the result multiple times:
````{tabs}
```{code-tab} py
animal_counts = (rescue
                 .groupBy("animal_group")
                 .count()
                 .withColumn("partition_id", F.spark_partition_id())
                 .filter(F.col("animal_group").isin("Fox", "Goat", "Hamster")))

for show_no in range(3):
    animal_counts.show()
```

```{code-tab} r R

animal_counts <- rescue %>%
    dplyr::group_by(animal_group) %>%
    dplyr::summarise(count = n()) %>%
    sparklyr::mutate(partition_id = spark_partition_id()) %>%
    sparklyr::filter(animal_group %in% c("Fox", "Goat", "Hamster"))

for(show_no in 1:3){
    animal_counts %>%
    sparklyr::collect() %>%
    print()
}

```
````

````{tabs}

```{code-tab} plaintext Python Output
+------------+-----+------------+
|animal_group|count|partition_id|
+------------+-----+------------+
|         Fox|  238|           7|
|        Goat|    1|           8|
|     Hamster|   14|           9|
+------------+-----+------------+

+------------+-----+------------+
|animal_group|count|partition_id|
+------------+-----+------------+
|         Fox|  238|           8|
|        Goat|    1|           9|
|     Hamster|   14|          10|
+------------+-----+------------+

+------------+-----+------------+
|animal_group|count|partition_id|
+------------+-----+------------+
|         Fox|  238|           8|
|        Goat|    1|           9|
|     Hamster|   14|          10|
+------------+-----+------------+
```

```{code-tab} plaintext R Output
# A tibble: 3 × 3
  animal_group count partition_id
  <chr>        <dbl>        <int>
1 Fox            238            8
2 Goat             1            9
3 Hamster         14           10
# A tibble: 3 × 3
  animal_group count partition_id
  <chr>        <dbl>        <int>
1 Fox            238            8
2 Goat             1            9
3 Hamster         14            9
# A tibble: 3 × 3
  animal_group count partition_id
  <chr>        <dbl>        <int>
1 Fox            238            7
2 Goat             1            8
3 Hamster         14            8
```
````
Although the same plan is being executed each time, the `partition_id` can be different. The `partition_id` is allocated by the [`.groupBy()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)/[`group_by()`](https://dplyr.tidyverse.org/reference/group_by.html) operation, which causes a shuffle.

### Final thoughts

Remember that sorting the data causes a [shuffle](../spark-concepts/shuffling), which is an expensive operation. Try and only sort data when necessary.

Sometimes you do want your code to be non-deterministic, e.g. when using random numbers. You can set a seed to replicate results if needed. You may also want to use [mocking](../testing-debugging/unit-testing-pyspark.html#mocking) if unit testing in PySpark with random numbers.

### Further Resources

Spark at the ONS Articles:
- [Managing Partitions](../spark-concepts/partitions)
- [Shuffling](../spark-concepts/shuffling)
- [Sorting Spark DataFrames](../spark-functions/sorting-data)
- [Unit Testing in PySpark](../testing-debugging/unit-testing-pyspark)
    - [Mocking](../testing-debugging/unit-testing-pyspark.html#mocking)
- [Unit Testing in sparklyr](../testing-debugging/unit-testing-sparklyr)
- [Caching](../spark-concepts/cache)
- [Writing Data](../spark-functions/writing-data)

PySpark Documentation:
- [`.show()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.show.html)
- [`F.spark_partition_id()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.spark_partition_id.html)
- [`.groupBy()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)

sparklyr and tidyverse Documentation:
- [`group_by()`](https://dplyr.tidyverse.org/reference/group_by.html)
- [`collect()`](https://dplyr.tidyverse.org/reference/compute.html)

Spark SQL Functions Documentation:
- [`spark_partition_id`](https://spark.apache.org/docs/latest/api/sql/index.html#spark_partition_id)
<font size = 12>
/n </font>## Spark DataFrames Are Not Ordered

Spark DataFrames do not have the order preserved in the same way as pandas or base R DataFrames. Subsetting a pandas or R DataFrame, e.g. with `head()`, will always return identical rows, whereas the rows returned may be different when subsetting PySpark or sparklyr DFs, e.g with [`.show()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.show.html) or `head()`.

The reason for this is that Spark DataFrames are distributed across partitions. Some operations, such as joining, grouping or sorting, cause data to be moved between [partitions](../spark-concepts/partitions), known as a [*shuffle*](../spark-concepts/shuffling). Running the exact same Spark code can result in different data being on different partitions, as well as a different order within each partition.

### Why Spark DFs are not ordered

The lack of an order in a Spark DataFrame is due to two related concepts: partitioning and lazy evaluation.

A Spark DataFrame is distributed across partitions on the Spark cluster, rather than being one complete object stored in the driver memory. Executing an identical Spark plan can result in the same rows going to different partitions. Within these partitions, the rows can also be in a different order.

The lazy evaluation will cause the DataFrame to be re-evaluated each time an action is called. Spark will only do the minimum required to return the result of the action specified, e.g. subsetting and previewing the data with `.show()`/`head() %>% ` [`collect()`](https://dplyr.tidyverse.org/reference/compute.html) may not require evaluation of the whole DataFrame (this can be demonstrated when using [caching](../spark-concepts/cache), as the cache may only be partially filled with these operations). As such, if no order is specified then the data may be returned in a different order, despite the Spark plan being identical. If the data is being subset in a non-specific way, e.g. with `.show()`/`head() %>% collect()`, then different rows may be returned.

The technical term for this is *non-determinism*, which is where the same algorithm can return different results for the same inputs. This can cause problems when regression testing (ensuring that results are identical after a code change) and unit testing your Spark code with [Pytest](../testing-debugging/unit-testing-pyspark) or [testthat](../testing-debugging/unit-testing-sparklyr).

The main lesson from this is that if you want to rely on the order of a Spark DataFrame, you need to [explicitly specify the order](../spark-functions/sorting-data); this may involve using a column as a tie-breaker (e.g. a key column such as an ID, which is unique).

### Does the order really matter?

Before looking at examples, it is worth remembering that often the order of the data does not matter. Ordering data in Spark is an expensive operation as it requires a [shuffle of the data](../spark-concepts/shuffling), so try to avoid sorting if you can. You may want to aim for only ordering the data when [writing out final results](../spark-functions/writing-data) (e.g. to a CSV if you need the result to be human readable).

### An example: different order within same partition

First, create a new Spark session and a small DataFrame; this example uses the winners of the Six Nations rugby union tournament:
````{tabs}
```{code-tab} py
import yaml
from pyspark.sql import SparkSession, functions as F

with open("../../../config.yaml") as f:
    config = yaml.safe_load(f)

spark = (SparkSession.builder.master("local[2]")
         .appName("df-order")
         .config("spark.sql.shuffle.partitions", 12)
         .getOrCreate())

winners = spark.createDataFrame([
    [2022, "France"],
    [2021, "Wales"],
    [2020, "England"],
    [2019, "Wales"],
    [2018, "Ireland"],
    [2017, "England"],
    [2016, "England"],
    [2015, "Ireland"],
    [2014, "Ireland"],
    [2013, "Wales"],
    [2012, "Wales"],
    [2011, "England"],
    [2010, "France"],
    [2009, "Ireland"],
    [2008, "Wales"],
    [2007, "France"],
    [2006, "France"],
    [2005, "Wales"],
    [2004, "France"],
    [2003, "England"],
    [2002, "France"],
    [2001, "England"],
    [2000, "England"],
    ],
    ["year", "nation"]
)
```

```{code-tab} r R

library(sparklyr)
library(dplyr)

config <- yaml::yaml.load_file("ons-spark/config.yaml")

small_config <- sparklyr::spark_config()
small_config$spark.sql.shuffle.partitions <- 12
small_config$spark.sql.shuffle.partitions.local <- 12

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "df-order",
    config = small_config)

winners <- sparklyr::sdf_copy_to(sc,
    data.frame(
        "year" = 2022:2000,
            "nation" = c(
                "France",
                "Wales",
                "England",
                "Wales",
                "Ireland",
                rep("England", 2),
                rep("Ireland", 2),
                rep("Wales", 2),
                "England",
                "France",
                "Ireland",
                "Wales",
                rep("France", 2),
                "Wales",
                "France",
                "England",
                "France",
                rep("England", 2))),
    repartition=2)

```
````
Now order the data by `nation`. This will ensure that the first nation alphabetically will be returned (`England`), but as the `year` is not specified these may be returned in a different order.

To demonstrate that these rows are all on the same partition, create a new column using [`F.spark_partition_id()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.spark_partition_id.html)/[`spark_partition_id()`](https://spark.apache.org/docs/latest/api/sql/index.html#spark_partition_id):
````{tabs}
```{code-tab} py
winners_ordered = (winners
                   .orderBy("nation")
                   .withColumn("partition_id", F.spark_partition_id()))
```

```{code-tab} r R

winners_ordered <- winners %>%
    sparklyr::sdf_sort(c("nation")) %>%
    sparklyr::mutate(partition_id = spark_partition_id())

```
````
Now preview this DataFrame several times. Remember that in Spark, the whole plan will be processed, so it will create and order the DataFrame by `nation` each time, but not the `year`, and so this may be different:
````{tabs}
```{code-tab} py
for show_no in range(3):
    winners_ordered.show(5)
```

```{code-tab} r R

for(show_no in 1:3){
    winners_ordered %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()
}

```
````

````{tabs}

```{code-tab} plaintext Python Output
+----+-------+------------+
|year| nation|partition_id|
+----+-------+------------+
|2020|England|           0|
|2017|England|           0|
|2016|England|           0|
|2011|England|           0|
|2003|England|           0|
+----+-------+------------+
only showing top 5 rows

+----+-------+------------+
|year| nation|partition_id|
+----+-------+------------+
|2011|England|           0|
|2020|England|           0|
|2016|England|           0|
|2017|England|           0|
|2003|England|           0|
+----+-------+------------+
only showing top 5 rows

+----+-------+------------+
|year| nation|partition_id|
+----+-------+------------+
|2020|England|           0|
|2017|England|           0|
|2016|England|           0|
|2011|England|           0|
|2003|England|           0|
+----+-------+------------+
only showing top 5 rows
```

```{code-tab} plaintext R Output
# A tibble: 5 × 3
   year nation  partition_id
  <int> <chr>          <int>
1  2001 England            0
2  2011 England            0
3  2003 England            0
4  2000 England            0
5  2016 England            0
# A tibble: 5 × 3
   year nation  partition_id
  <int> <chr>          <int>
1  2001 England            0
2  2011 England            0
3  2003 England            0
4  2000 England            0
5  2016 England            0
# A tibble: 5 × 3
   year nation  partition_id
  <int> <chr>          <int>
1  2011 England            0
2  2020 England            0
3  2016 England            0
4  2017 England            0
5  2003 England            0
```
````
The results not all the same. The data in the `nation` column is identical every time, as this was explicitly ordered, but the `year` is not.

This demonstrates that the data is being returned in `partition_id` order for each `nation`, but there is no fixed ordering within each partition. If the DataFrame was also sorted by `year` the same result would be returned each time.

Note that if you run the code again, you may get different results.

### Another example: different `partition_id`

During a shuffle the data can be sent to different partitions when executing the same Spark plan multiple times.

First, import the Animal Rescue CSV data and do some data cleansing. The data is also being sorted by `AnimalGroup`; this causes a shuffle which will repartition the data:
````{tabs}
```{code-tab} py
rescue_path_csv = config["rescue_path_csv"]
rescue = (spark
          .read.csv(rescue_path_csv, header=True, inferSchema=True)
          .withColumnRenamed("IncidentNumber", "incident_number")
          .withColumnRenamed("AnimalGroupParent", "animal_group")
          .select("incident_number", "animal_group")
          .orderBy("animal_group")
         )
```

```{code-tab} r R

rescue <- sparklyr::spark_read_csv(sc,
                                   config$rescue_path_csv,
                                   header=TRUE,
                                   infer_schema=TRUE) %>%
    dplyr:::rename(
        incident_number = IncidentNumber,
        animal_group = AnimalGroupParent) %>%
    sparklyr::select(incident_number, animal_group) %>%
    sparklyr::sdf_sort(c("animal_group"))

```
````
Now group the data by `animal_group`, which will cause another shuffle, count how many of each animal there are, and return `Fox`, `Goat` and `Hamster`, then show the result multiple times:
````{tabs}
```{code-tab} py
animal_counts = (rescue
                 .groupBy("animal_group")
                 .count()
                 .withColumn("partition_id", F.spark_partition_id())
                 .filter(F.col("animal_group").isin("Fox", "Goat", "Hamster")))

for show_no in range(3):
    animal_counts.show()
```

```{code-tab} r R

animal_counts <- rescue %>%
    dplyr::group_by(animal_group) %>%
    dplyr::summarise(count = n()) %>%
    sparklyr::mutate(partition_id = spark_partition_id()) %>%
    sparklyr::filter(animal_group %in% c("Fox", "Goat", "Hamster"))

for(show_no in 1:3){
    animal_counts %>%
    sparklyr::collect() %>%
    print()
}

```
````

````{tabs}

```{code-tab} plaintext Python Output
+------------+-----+------------+
|animal_group|count|partition_id|
+------------+-----+------------+
|         Fox|  238|           7|
|        Goat|    1|           8|
|     Hamster|   14|           9|
+------------+-----+------------+

+------------+-----+------------+
|animal_group|count|partition_id|
+------------+-----+------------+
|         Fox|  238|           8|
|        Goat|    1|           9|
|     Hamster|   14|          10|
+------------+-----+------------+

+------------+-----+------------+
|animal_group|count|partition_id|
+------------+-----+------------+
|         Fox|  238|           8|
|        Goat|    1|           9|
|     Hamster|   14|          10|
+------------+-----+------------+
```

```{code-tab} plaintext R Output
# A tibble: 3 × 3
  animal_group count partition_id
  <chr>        <dbl>        <int>
1 Fox            238            8
2 Goat             1            9
3 Hamster         14           10
# A tibble: 3 × 3
  animal_group count partition_id
  <chr>        <dbl>        <int>
1 Fox            238            8
2 Goat             1            9
3 Hamster         14            9
# A tibble: 3 × 3
  animal_group count partition_id
  <chr>        <dbl>        <int>
1 Fox            238            7
2 Goat             1            8
3 Hamster         14            8
```
````
Although the same plan is being executed each time, the `partition_id` can be different. The `partition_id` is allocated by the [`.groupBy()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)/[`group_by()`](https://dplyr.tidyverse.org/reference/group_by.html) operation, which causes a shuffle.

### Final thoughts

Remember that sorting the data causes a [shuffle](../spark-concepts/shuffling), which is an expensive operation. Try and only sort data when necessary.

Sometimes you do want your code to be non-deterministic, e.g. when using random numbers. You can set a seed to replicate results if needed. You may also want to use [mocking](../testing-debugging/unit-testing-pyspark.html#mocking) if unit testing in PySpark with random numbers.

### Further Resources

Spark at the ONS Articles:
- [Managing Partitions](../spark-concepts/partitions)
- [Shuffling](../spark-concepts/shuffling)
- [Sorting Spark DataFrames](../spark-functions/sorting-data)
- [Unit Testing in PySpark](../testing-debugging/unit-testing-pyspark)
    - [Mocking](../testing-debugging/unit-testing-pyspark.html#mocking)
- [Unit Testing in sparklyr](../testing-debugging/unit-testing-sparklyr)
- [Caching](../spark-concepts/cache)
- [Writing Data](../spark-functions/writing-data)

PySpark Documentation:
- [`.show()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.show.html)
- [`F.spark_partition_id()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.spark_partition_id.html)
- [`.groupBy()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)

sparklyr and tidyverse Documentation:
- [`group_by()`](https://dplyr.tidyverse.org/reference/group_by.html)
- [`collect()`](https://dplyr.tidyverse.org/reference/compute.html)

Spark SQL Functions Documentation:
- [`spark_partition_id`](https://spark.apache.org/docs/latest/api/sql/index.html#spark_partition_id)