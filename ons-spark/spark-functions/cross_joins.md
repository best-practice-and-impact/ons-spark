## Cross Joins

A *cross joins* is used to return every combination of the rows of two DataFrames. Cross joins are also referred to as the *cartesian product* of two DataFrames. It is different to other types of joins, which depend on matching values by using join keys.

As a cross join will return every combination of the rows, the size of the returned DataFrame is equal to the product of the row count of both source DataFrames; this can quickly get large and overwhelm your Spark session. As such, use them carefully!

The syntax for cross joins is different in PySpark and sparklyr. In PySpark, DataFrames have a [`.crossJoin()`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.crossJoin.html) method. In sparklyr, use [`full_join()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/join.tbl_spark.html) with `by=character()`.

One use case for cross joins is to return every combination when producing results that involve grouping and aggregation, even when some of these are zero. Cross joins are also commonly used in salted joins, used to improve the efficiency of a join when the join keys are skewed.

### Example: producing all combinations of results

Note that the output displayed is for PySpark; the sparklyr output may be formatted slightly differently.

First, start a Spark session (disabling broadcast joins by default) read in the Animal Rescue data, group by `animal_group` and `cal_year`, and then get the sum of `total_cost`:
````{tabs}
```{code-tab} py
import subprocess
import yaml
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[2]")
         .appName("joins")
         # Disable Broadcast join by default
         .config("spark.sql.autoBroadcastJoinThreshold", -1)
         .getOrCreate())

with open("../../../config.yaml") as f:
    config = yaml.safe_load(f)
    
rescue_path = config["rescue_path"]

rescue = (spark.read.parquet(rescue_path)
          .groupBy("animal_group", "cal_year")
          .agg(F.sum("total_cost").alias("cost")))

rescue.show(5, truncate=False)
```

```{code-tab} r R

library(sparklyr)
library(dplyr)

default_config <- sparklyr::spark_config()

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "sampling",
    config = default_config)

config <- yaml::yaml.load_file("ons-spark/config.yaml")

rescue <- sparklyr::spark_read_parquet(sc, config$rescue_path) %>%
    dplyr::group_by(animal_group, cal_year) %>%
    dplyr::summarise(cost = sum(total_cost)) %>%
    dplyr::ungroup()

rescue %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

```
````

```plaintext
+--------------------------------+--------+-------+
|animal_group                    |cal_year|cost   |
+--------------------------------+--------+-------+
|Snake                           |2018    |333.0  |
|Deer                            |2015    |1788.0 |
|Unknown - Domestic Animal Or Pet|2017    |2622.0 |
|Bird                            |2012    |32240.0|
|Hedgehog                        |2009    |520.0  |
+--------------------------------+--------+-------+
only showing top 5 rows
```
If there are no animals rescued in a particular year then there are no rows to sum, and so nothing will be returned. In some cases we would prefer a zero value to be returned, rather than an empty DataFrame.

For instance, no data will be returned for `Hamster` and `2012`:
````{tabs}
```{code-tab} py
rescue.filter((F.col("animal_group") == "Hamster") & (F.col("cal_year") == 2012)).show()
```

```{code-tab} r R

rescue %>%
    dplyr::filter(animal_group == "Hamster" & cal_year == 2012) %>%
    sparklyr::collect() %>%
    print()

```
````

```plaintext
+------------+--------+----+
|animal_group|cal_year|cost|
+------------+--------+----+
+------------+--------+----+
```
A cross join can be used here, to return every combination of `animal_group` and `cal_year`, which then serves as a base DataFrame which `rescue` can be joined to. First, create a DataFrame for unique `animals` and `cal_years` and sort them:
````{tabs}
```{code-tab} py
animals = rescue.select("animal_group").distinct().orderBy("animal_group")
cal_years = rescue.select("cal_year").distinct().orderBy("cal_year")

print(f"Distinct animal count: {animals.count()}")
print(f"Distinct year count: {cal_years.count()}")
```

```{code-tab} r R


animals <- rescue %>%
    sparklyr::select(animal_group) %>%
    sparklyr::sdf_distinct() %>%
    dplyr::arrange(animal_group)
    
cal_years <- rescue %>%
    sparklyr::select(cal_year) %>%
    sparklyr::sdf_distinct() %>%
    dplyr::arrange(cal_year)

print(paste("Distinct animal count:",  sparklyr::sdf_nrow(animals), sep=" "))
print(paste("Distinct year count:",  sparklyr::sdf_nrow(cal_years), sep=" "))

```
````

```plaintext
Distinct animal count: 27
Distinct year count: 11
```
There can be issues in Spark when joining values from a DF to itself, which can be resolved by checkpointing to break the lineage of the DataFrame. See the article on checkpointing for more information.
````{tabs}
```{code-tab} py
checkpoint_path = config["checkpoint_path"]
spark.sparkContext.setCheckpointDir(checkpoint_path)

animals = animals.checkpoint()
cal_years = cal_years.checkpoint()
```

```{code-tab} r R

sparklyr::spark_set_checkpoint_dir(sc, config$checkpoint_path)

animals <- sparklyr::sdf_checkpoint(animals)
cal_years <- sparklyr::sdf_checkpoint(cal_years)

```
````
Now cross join `cal_years` and `animals`. In PySpark, use [`.crossJoin()`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.crossJoin.html), which is method applied to the DataFrame and takes one argument, the DF to cross join to. In sparklyr, there is no native cross join function, so use [`full_join()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/join.tbl_spark.html) with `by=character()`.

Note that apart from the order of the columns the result will be the same regardless of which DF is on the left and which is on the right. The `result` DF will have $27 * 11 = 297$ rows.
````{tabs}
```{code-tab} py
result = animals.crossJoin(cal_years).orderBy("animal_group", "cal_year")
result.count()
```

```{code-tab} r R

result <- animals %>%
    sparklyr::full_join(cal_years, by=character()) %>%
    dplyr::arrange(animal_group, cal_year)
    
sparklyr::sdf_nrow(result)

```
````

```plaintext
297
```
Previewing `result` we can see that every combination has been returned:
````{tabs}
```{code-tab} py
result.show(15)
```

```{code-tab} r R

result %>%
    head(15) %>%
    sparklyr::collect() %>%
    print()

```
````

```plaintext
+------------+--------+
|animal_group|cal_year|
+------------+--------+
|        Bird|    2009|
|        Bird|    2010|
|        Bird|    2011|
|        Bird|    2012|
|        Bird|    2013|
|        Bird|    2014|
|        Bird|    2015|
|        Bird|    2016|
|        Bird|    2017|
|        Bird|    2018|
|        Bird|    2019|
|      Budgie|    2009|
|      Budgie|    2010|
|      Budgie|    2011|
|      Budgie|    2012|
+------------+--------+
only showing top 15 rows
```
Now left join `rescue` to `result`, to return the `cost`. Combinations which do not exist in `rescue` will return a null `cost`.

Note that the DF has been reordered during the join, as shuffle hash join was used rather than a broadcast join. See the article on join concepts for more information on this.
````{tabs}
```{code-tab} py
result = result.join(rescue, on = ["animal_group", "cal_year"], how="left")
result.show(10)
```

```{code-tab} r R

result <- result %>%
    sparklyr::left_join(rescue, by = c("animal_group", "cal_year"))

result %>%
    head(10) %>%
    sparklyr::collect() %>%
    print()

```
````

```plaintext
+--------------------+--------+-------+
|        animal_group|cal_year|   cost|
+--------------------+--------+-------+
|                Deer|    2015| 1788.0|
|               Snake|    2018|  333.0|
|Unknown - Animal ...|    2011|   null|
|                Goat|    2013|   null|
|                Bird|    2012|32240.0|
|            Hedgehog|    2009|  520.0|
|               Sheep|    2013|   null|
|Unknown - Domesti...|    2017| 2622.0|
|              Ferret|    2017|   null|
|               Horse|    2012|16120.0|
+--------------------+--------+-------+
only showing top 10 rows
```
The final stage is to change any null values to zero and re-order the DF.

We can now filter on `Hamster` to see that `2012` now exists and is returned with a zero value:
````{tabs}
```{code-tab} py
result = result.fillna(0, "cost").orderBy("animal_group", "cal_year")
result.filter(F.col("animal_group") == "Hamster").show()
```

```{code-tab} r R

result <- result %>%
    sparklyr::mutate(cost = ifnull(cost, 0)) %>%
    dplyr::arrange(animal_group, cal_year)
    
result %>%
    sparklyr::filter(animal_group == "Hamster") %>%
    sparklyr::collect() %>%
    print()

```
````

```plaintext
+------------+--------+------+
|animal_group|cal_year|  cost|
+------------+--------+------+
|     Hamster|    2009|   0.0|
|     Hamster|    2010| 780.0|
|     Hamster|    2011| 780.0|
|     Hamster|    2012|   0.0|
|     Hamster|    2013| 870.0|
|     Hamster|    2014| 295.0|
|     Hamster|    2015|   0.0|
|     Hamster|    2016|1630.0|
|     Hamster|    2017|   0.0|
|     Hamster|    2018|   0.0|
|     Hamster|    2019|   0.0|
+------------+--------+------+
```
### Accidental Cross Joins

Cross joins can be dangerous due to the size of DataFrame that they return. In PySpark, it is possible to create a cross join accidentally when using a regular join where the key column only has one value in it. In these cases Spark will sometimes process the join as a cross join and return an error, depending on the lineage of the DataFrame. You can disable this error with `.config("spark.sql.crossJoin.enabled", "true")`.

Note that this error is specific to Spark 2; in Spark 3, [this option is enabled by default](https://github.com/apache/spark/pull/25520) and so the error will only occur if you explicitly enable it.

This error will not occur in sparklyr.

To demonstrate this in PySpark (version `2.4.0`), create a new DF, `dogs`, with just one column, `animal_group` containing just `Dog`, and join another DF to it:
````{tabs}
```{code-tab} py
from pyspark.sql.utils import AnalysisException

dogs = spark.range(5).withColumn("animal_group", F.lit("Dog"))

animal_noise = spark.createDataFrame([
    ["Cat", "Meow"],
    ["Dog", "Woof"],
    ["Cow", "Moo"]],
    ["animal_group", "animal_noise"])

try:
    dogs.join(animal_noise, on="animal_group", how="left").show()
except AnalysisException as e:
    print(e)
```
````

```plaintext
'Detected implicit cartesian product for LEFT OUTER join between logical plans\nRange (0, 5, step=1, splits=Some(2))\nand\nProject [animal_noise#203]\n+- Filter (isnotnull(animal_group#202) && (Dog = animal_group#202))\n   +- LogicalRDD [animal_group#202, animal_noise#203], false\nJoin condition is missing or trivial.\nEither: use the CROSS JOIN syntax to allow cartesian products between these\nrelations, or: enable implicit cartesian products by setting the configuration\nvariable spark.sql.crossJoin.enabled=true;'
```
To resolve this, start a new Spark session and add `.config("spark.sql.crossJoin.enabled", "true")` to the config. The DFs will have been cleared by closing the Spark session and so need to be created again.
````{tabs}
```{code-tab} py
spark.stop()

spark = (SparkSession.builder.master("local[2]")
         .appName("joins")
         .config("spark.sql.crossJoin.enabled", "true")
         .getOrCreate())

dogs = spark.range(5).withColumn("animal_group", F.lit("Dog"))

animal_noise = spark.createDataFrame([
    ["Cat", "Meow"],
    ["Dog", "Woof"],
    ["Cow", "Moo"]],
    ["animal_group", "animal_noise"])

dogs.join(animal_noise, on="animal_group", how="left").show()
```
````

```plaintext
+------------+---+------------+
|animal_group| id|animal_noise|
+------------+---+------------+
|         Dog|  0|        Woof|
|         Dog|  1|        Woof|
|         Dog|  2|        Woof|
|         Dog|  3|        Woof|
|         Dog|  4|        Woof|
+------------+---+------------+
```
Finally, clear the checkpoints that were used in the code:
````{tabs}
```{code-tab} py
p = subprocess.run(f"hdfs dfs -rm -r -skipTrash {checkpoint_path}", shell=True)

```

```{code-tab} r R

system(paste0("hdfs dfs -rm -r -skipTrash ", config$checkpoint_path))

```
````
### Salted Joins

Cross joins can also be used when *salting* a join. Salting improves the efficiency of a join by reducing the skew in join keys. See the article on salted joins for an example.### Further Resources

PySpark Documentation:
- [`.crossJoin()`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.crossJoin.html)
- [`.checkpoint()`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.checkpoint.html)
- [`.distinct()`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.distinct.html)

sparklyr Documentation:
- [`full_join()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/join.tbl_spark.html): there is no native cross join function in sparklyr; the documentation recommends using `by=character()`
- [`sdf_checkpoint()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_checkpoint.html)
- [`sdf_distinct()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_distinct.html)

Spark in ONS material:
- Join Concepts: explains shuffle hash joins and broadcast joins 
- Salted Joins: salted joins make use of cross joining during the preparation
- Lineage, Checkpoints and Staging Tables

Other links:
- [GitHub: SPARK-28621](https://github.com/apache/spark/pull/25520): pull request for setting `spark.sql.crossJoin.enabled` to be `true` by default in Spark 3