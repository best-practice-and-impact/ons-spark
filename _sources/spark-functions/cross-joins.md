## Cross Joins

A *cross join* is used to return every combination of the rows of two DataFrames. Cross joins are also referred to as the *cartesian product* of two DataFrames. It is different to other types of joins, which depend on matching values by using join keys.

As a cross join will return every combination of the rows, the size of the returned DataFrame is equal to the product of the row count of both source DataFrames; this can quickly get large and overwhelm your Spark session. As such, use them carefully!

The syntax for cross joins is different in PySpark and sparklyr. In PySpark, DataFrames have a [`.crossJoin()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.crossJoin.html) method. In sparklyr, use [`full_join()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/join.tbl_spark.html) with `by=character()`.

One use case for cross joins is to return every combination when producing results that involve grouping and aggregation, even when some of these are zero. Cross joins are also commonly used in [salted joins](../spark-concepts/salted-joins), used to improve the efficiency of a join when the join keys are skewed.

### Example: producing all combinations of results

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

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "joins",
    config = sparklyr::spark_config())

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

````{tabs}

```{code-tab} plaintext Python Output
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

```{code-tab} plaintext R Output
# A tibble: 5 × 3
  animal_group                     cal_year  cost
  <chr>                               <int> <dbl>
1 Dog                                  2018 34507
2 Horse                                2013 11890
3 Squirrel                             2011  1040
4 Fox                                  2017 12124
5 Unknown - Domestic Animal Or Pet     2015  9217
```
````
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

````{tabs}

```{code-tab} plaintext Python Output
+------------+--------+----+
|animal_group|cal_year|cost|
+------------+--------+----+
+------------+--------+----+
```

```{code-tab} plaintext R Output
# A tibble: 0 × 3
# … with 3 variables: animal_group <chr>, cal_year <int>, cost <dbl>
```
````
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

````{tabs}

```{code-tab} plaintext Python Output
Distinct animal count: 27
Distinct year count: 11
```

```{code-tab} plaintext R Output
[1] "Distinct animal count: 27"
[1] "Distinct year count: 11"
```
````
There can be issues in Spark when joining values from a DF to itself, which can be resolved by checkpointing to break the lineage of the DataFrame. See the article on [checkpointings](../raw-notebooks/checkpoint-staging/checkpoint-staging) for more information.
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
Now cross join `cal_years` and `animals`. In PySpark, use [`.crossJoin()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.crossJoin.html), which is a method applied to the DataFrame and takes one argument, the DF to cross join to. In sparklyr, there is no native cross join function, so use [`full_join()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/join.tbl_spark.html) with `by=character()`.

Note that apart from the order of the columns the result will be the same regardless of which DF is on the left and which is on the right. The `result` DF will have $27 \times 11 = 297$ rows.
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

````{tabs}

```{code-tab} plaintext Python Output
297
```

```{code-tab} plaintext R Output
[1] 297
```
````
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

````{tabs}

```{code-tab} plaintext Python Output
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

```{code-tab} plaintext R Output
# A tibble: 15 × 2
   animal_group cal_year
   <chr>           <int>
 1 Bird             2009
 2 Bird             2010
 3 Bird             2011
 4 Bird             2012
 5 Bird             2013
 6 Bird             2014
 7 Bird             2015
 8 Bird             2016
 9 Bird             2017
10 Bird             2018
11 Bird             2019
12 Budgie           2009
13 Budgie           2010
14 Budgie           2011
15 Budgie           2012
```
````
Now left join `rescue` to `result`, to return the `cost`. Combinations which do not exist in `rescue` will return a null `cost`.

Note that the DF has been reordered during the join, as [sort merge join](../spark-concepts/join-concepts.html#sort-merge-join) was used rather than a [broadcast join](../spark-concepts/join-concepts.html#broadcast-join). See the article on [optimising joins](../spark-concepts/join-concepts)for more information on this.
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

````{tabs}

```{code-tab} plaintext Python Output
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

```{code-tab} plaintext R Output
# A tibble: 10 × 3
   animal_group cal_year  cost
   <chr>           <int> <dbl>
 1 Bird             2009 25095
 2 Budgie           2009    NA
 3 Bird             2010 27300
 4 Budgie           2010    NA
 5 Bird             2011 36140
 6 Budgie           2011   260
 7 Bird             2012 32240
 8 Budgie           2012    NA
 9 Bird             2013 26350
10 Budgie           2013    NA
```
````
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

````{tabs}

```{code-tab} plaintext Python Output
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

```{code-tab} plaintext R Output
# A tibble: 11 × 3
   animal_group cal_year  cost
   <chr>           <int> <dbl>
 1 Hamster          2009     0
 2 Hamster          2010   780
 3 Hamster          2011   780
 4 Hamster          2012     0
 5 Hamster          2013   870
 6 Hamster          2014   295
 7 Hamster          2015     0
 8 Hamster          2016  1630
 9 Hamster          2017     0
10 Hamster          2018     0
11 Hamster          2019     0
```
````
### Accidental Cross Joins

Cross joins can be dangerous due to the size of DataFrame that they return. In PySpark, it is possible to create a cross join accidentally when using a regular join where the key column only has one distinct value in it. In these cases Spark will sometimes process the join as a cross join and return an error, depending on the lineage of the DataFrame. You can disable this error with `.config("spark.sql.crossJoin.enabled", "true")`.

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

````{tabs}

```{code-tab} plaintext Python Output
'Detected implicit cartesian product for LEFT OUTER join between logical plans\nRange (0, 5, step=1, splits=Some(2))\nand\nProject [animal_noise#203]\n+- Filter (isnotnull(animal_group#202) && (Dog = animal_group#202))\n   +- LogicalRDD [animal_group#202, animal_noise#203], false\nJoin condition is missing or trivial.\nEither: use the CROSS JOIN syntax to allow cartesian products between these\nrelations, or: enable implicit cartesian products by setting the configuration\nvariable spark.sql.crossJoin.enabled=true;'
```
````
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

````{tabs}

```{code-tab} plaintext Python Output
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
````
Finally, clear the checkpoints that were used in the code:
````{tabs}
```{code-tab} py
p = subprocess.run(f"hdfs dfs -rm -r -skipTrash {checkpoint_path}", shell=True)

```

```{code-tab} r R

system(paste0("hdfs dfs -rm -r -skipTrash ", config$checkpoint_path))

```
````

````{tabs}

```{code-tab} plaintext R Output
Deleted file:///home/cdsw/ons-spark/checkpoints
```
````
### Salted Joins

Cross joins can also be used when *salting* a join. Salting improves the efficiency of a join by reducing the skew in the DataFrame, by introducing new join keys. See the article on [salted joins](../spark-concepts/salted-joins) for an example.

### Further Resources

Spark at the ONS Articles:
- [Salted Joins](../spark-concepts/salted-joins)
- [Checkpoint and Staging Tables](../raw-notebooks/checkpoint-staging/checkpoint-staging)
- [Optimising Joins](../spark-concepts/join-concepts)
    - [Sort Merge Join](../spark-concepts/join-concepts.html#sort-merge-join)
	- [Broadcast Join](../spark-concepts/join-concepts.html#broadcast-join)
    
PySpark Documentation:
- [`.crossJoin()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.crossJoin.html)
- [`.checkpoint()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.checkpoint.html)
- [`.distinct()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.distinct.html)

sparklyr Documentation:
- [`full_join()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/join.tbl_spark.html): there is no native cross join function in sparklyr; the documentation recommends using `by=character()`
- [`sdf_checkpoint()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_checkpoint.html)
- [`sdf_distinct()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_distinct.html)

Other links:
- [GitHub: SPARK-28621](https://github.com/apache/spark/pull/25520): pull request for setting `spark.sql.crossJoin.enabled` to be `true` by default in Spark 3