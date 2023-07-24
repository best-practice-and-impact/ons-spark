## Sampling: `.sample()` and `sdf_sample()`

You can take a sample of a DataFrame with [`.sample()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sample.html) in PySpark or [`sdf_sample()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_sample.html) in sparklyr. This is something that you may want to do during development or initial analysis of data, as with a smaller amount of data your code will run faster and requires less memory to process.

It is important to note that sampling in Spark returns an approximate fraction of the data, rather than an exact one. The reason for this is explained in the [Returning an exact sample](#returning-an-exact-sample) section.

#### Creating spark session and loading data

First, set up the Spark session, read the Animal Rescue data, and then get the row count:
````{tabs}
```{code-tab} py
import os
import yaml
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[2]").appName("sampling").getOrCreate()

with open("../../../config.yaml") as f:
    config = yaml.safe_load(f)
    
rescue_path = config["rescue_path"]
rescue = spark.read.parquet(rescue_path)

rescue.count()
```

```{code-tab} r R

library(sparklyr)

default_config <- sparklyr::spark_config()

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "sampling",
    config = default_config)

config <- yaml::yaml.load_file("ons-spark/config.yaml")

rescue <- sparklyr::spark_read_parquet(sc, config$rescue_path)

rescue %>% sparklyr::sdf_nrow()

```
````

````{tabs}

```{code-tab} plaintext Python Output
5898
```

```{code-tab} plaintext R Output
[1] 5898
```
````
To fully test how spark sampling functions are impacted by partitions we will also make use of a skewed dataframe.

````{tabs}

```{code-tab} plaintext Python
 skewed_df = spark.range(1e6).withColumn("skew_col",F.when(F.col('id') < 100, 'A')
                                        .when(F.col('id') < 1000, 'B')
                                        .when(F.col('id') < 10000, 'C')
                                        .when(F.col('id') < 100000, 'D')
                                        .otherwise('E')
                                        )

skewed_df = skewed_df.repartition('skew_col')
```


````

### Sampling: `.sample()` and `sdf_sample()`


To use `.sample()`, set the `fraction`, which is between 0 and 1. So if we want a $20\%$ sample, use `fraction=0.2`. Note that this will give an *approximate* sample and so you will likely get slightly more or fewer rows than you expect.

You can select to sample with replacement by setting `withReplacement=True` in PySpark or `replacement=TRUE` in sparklyr, which is set to `False` by default.

For these functions it is advised to specify the arguments explicitly. One reason is that `fraction` is a compulsory argument, but in PySpark is *after* `withReplacement`. Another reason is that in sparklyr the arguments are in a different order, with `fraction` listed first; if you use both languages it is easy to make a mistake.
````{tabs}
```{code-tab} py
rescue_sample = rescue.sample(withReplacement=False, fraction=0.1)
rescue_sample.count()
```

```{code-tab} r R

rescue_sample <- rescue %>% sparklyr::sdf_sample(fraction=0.1, replacement=FALSE)
rescue_sample %>% sparklyr::sdf_nrow()

```
````

````{tabs}

```{code-tab} plaintext Python Output
588
```

```{code-tab} plaintext R Output
[1] 552
```
````
You can also set a seed, in a similar way to how random numbers generators work. This enables replication, which is useful in Spark given that the DataFrame will be otherwise be re-sampled every time an action is called.
````{tabs}
```{code-tab} py
rescue_sample_seed_1 = rescue.sample(withReplacement=None,
                      fraction=0.1,
                      seed=99)

rescue_sample_seed_2 = rescue.sample(withReplacement=None,
                      fraction=0.1,
                      seed=99)

print(f"Seed 1 count: {rescue_sample_seed_1.count()}")
print(f"Seed 2 count: {rescue_sample_seed_1.count()}")
```

```{code-tab} r R


rescue_sample_seed_1 <- rescue %>% sparklyr::sdf_sample(fraction=0.1, seed=99)
rescue_sample_seed_2 <- rescue %>% sparklyr::sdf_sample(fraction=0.1, seed=99)

print(paste0("Seed 1 count: ", rescue_sample_seed_1 %>% sparklyr::sdf_nrow()))
print(paste0("Seed 2 count: ", rescue_sample_seed_2 %>% sparklyr::sdf_nrow()))

```
````

````{tabs}

```{code-tab} plaintext Python Output
Seed 1 count: 589
Seed 2 count: 589
```

```{code-tab} plaintext R Output
[1] "Seed 1 count: 563"
[1] "Seed 2 count: 563"
```
````
We can see that both samples have returned the same number of rows due to the identical seed.

Another way of replicating results is with [persisting](../spark-concepts/persistence). [Caching](../spark-concepts/cache) or [checkpointing](../raw-notebooks/checkpoint-staging/checkpoint-staging) the DataFrame will avoid recalculation of the DF within the same Spark session. Writing out the DF to a Hive table or parquet enables it to be used in subsequent Spark sessions. See the chapter on persisting for more detail.

### Stratified samples: `.sampleBy()`

A stratified sample can be taken with [`.sampleBy()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sampleby.html) in PySpark. This takes a column, `col`, to sample by, and a dictionary of weights, `fractions`.

In common with other sampling methods this does not return an exact proportion and you can also optionally set a seed.

Note that there is no native sparklyr implementation for stratified sampling, although there is a method for weighted sampling, [`sdf_weighted_sample()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_weighted_sample.html).

In PySpark, to return $5\%$ of cats, $10\%$ of dogs and $50\%$ of hamsters:
````{tabs}
```{code-tab} py
weights = {"Cat": 0.05, "Dog": 0.1, "Hamster": 0.5}
stratified_sample = rescue.sampleBy("animal_group", fractions=weights)
stratified_sample_count = (stratified_sample
                           .groupBy("animal_group")
                           .agg(F.count("animal_group").alias("row_count"))
                           .orderBy("animal_group"))
stratified_sample_count.show()
```
````

````{tabs}

```{code-tab} plaintext Python Output
+------------+---------+
|animal_group|row_count|
+------------+---------+
|         Cat|      129|
|         Dog|       86|
|     Hamster|        9|
+------------+---------+
```
````
We can quickly compare the number of rows for each animal to the expected to confirm that they are approximately equal:
````{tabs}
```{code-tab} py
weights_df = spark.createDataFrame(list(weights.items()), schema=["animal_group", "weight"])

(rescue
    .groupBy("animal_group").count()
    .join(weights_df, on="animal_group", how="inner")
    .withColumn("expected_rows", F.round(F.col("count") * F.col("weight"), 0))
    .join(stratified_sample_count, on="animal_group", how="left")
    .orderBy("animal_group")
    .show()
)
```
````

````{tabs}

```{code-tab} plaintext Python Output
+------------+-----+------+-------------+---------+
|animal_group|count|weight|expected_rows|row_count|
+------------+-----+------+-------------+---------+
|         Cat| 2909|  0.05|        145.0|      129|
|         Dog| 1008|   0.1|        101.0|       86|
|     Hamster|   14|   0.5|          7.0|        9|
+------------+-----+------+-------------+---------+
```
````

### More details on sampling

The following section gives more detail on sampling and how it is processed on the Spark cluster. is not compulsory reading, but may be of interest to some Spark users.



### Additional sampling methods?

#### Returning an exact sample

We have demonstrated above that `.sample()`/`sdf_sample()` return an approximate fraction, not an exact one. This is because every row is independently assigned a probability equal to `fraction` of being included in the sample, e.g. with `fraction=0.2` every row has a $20\%$ probability of being in the sample. The number of rows returned in the sample therefore follows the binomial distribution.

The advantage of the sample being calculated in this way is that it is processed as a *narrow transformation*, which is more efficient than a *wide transformation*.

To return an exact sample, one method is to calculate how many rows are required in the sample, create a new column of random numbers and sort by it, and use [`.limit()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.limit.html) in PySpark or [`head()`](https://stat.ethz.ch/R-manual/R-devel/library/utils/html/head.html) in sparklyr. This requires an action and a wide transformation, and so will take longer to process than using `.sample()`.
````{tabs}
```{code-tab} py
fraction = 0.1
row_count = round(rescue.count() * fraction)
row_count
```

```{code-tab} r R

fraction <- 0.1
row_count <- round(sparklyr::sdf_nrow(rescue) * fraction)
row_count

```
````

````{tabs}

```{code-tab} plaintext Python Output
590
```

```{code-tab} plaintext R Output
[1] 590
```
````

````{tabs}
```{code-tab} py
rescue.withColumn("rand_no", F.rand()).orderBy("rand_no").limit(row_count).drop("rand_no").count()
```

```{code-tab} r R

rescue %>%
    sparklyr::mutate(rand_no = rand()) %>%
    dplyr::arrange(rand_no) %>%
    head(row_count) %>%
    sparklyr::select(-rand_no) %>%
    sparklyr::sdf_nrow()

```
````

````{tabs}

```{code-tab} plaintext Python Output
590
```

```{code-tab} plaintext R Output
[1] 590
```
````
#### Partitioning

The number of partitions will remain the same when sampling, even though the DataFrame will be smaller. If you are taking a small fraction of the data then your DataFrame may have too many partitions. You can use [`.coalesce()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.coalesce.html) in PySpark or [`sdf_coalesce()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_coalesce.html) in sparklyr to reduce the number of partitions, e.g. if your original DF had $200$ partitions and you take a $10\%$ sample, you can reduce the number of partitions to $20$ with `df.sample(fraction=0.1).coalesce(20)`.

#### Sampling consistently by filtering the data

If the primary reason for using sampling is to process less data during development then an alternative is to filter the data and specify a condition which gives approximately the desired number of rows. This will give consistent results, which may or may not be desirable. For instance, in the Animal Rescue data we could use two years data:
````{tabs}
```{code-tab} py
rescue.filter(F.col("cal_year").isin(2012, 2017)).count()
```

```{code-tab} r R

rescue %>%
    sparklyr::filter(cal_year == 2012 | cal_year == 2017) %>%
    sparklyr::sdf_nrow()

```
````

````{tabs}

```{code-tab} plaintext Python Output
1142
```

```{code-tab} plaintext R Output
[1] 1142
```
````
The disadvantage of this method is that you may have data quality issues in the original DF that will not be encountered, whereas these may be discovered with `.sample()`. Using unit testing and test driven development can mitigate the risk of these issues.


This section briefly discusses similar functions to `.sample()` and `sdf_sample()`.

#### Splitting a DF: `.randomSplit()` and `sdf_random_split()`


Every row in the DF will be allocated to one of the split DFs. In common with the other sampling methods the exact size of each split may vary. An optional seed can also be set.

For instance, to split the animal rescue data into three DFs with a weighting of $50\%$, $40\%$ and $10\%$ in PySpark:
````{tabs}
```{code-tab} py
split1, split2, split3 = rescue.randomSplit([0.5, 0.4, 0.1])

print(f"Split1: {split1.count()}")
print(f"Split2: {split2.count()}")
print(f"Split3: {split3.count()}")
```

```{code-tab} r R

splits <- rescue %>% sparklyr::sdf_random_split(
    split1 = 0.5,
    split2 = 0.4,
    split3 = 0.1)

print(paste0("Split1: ", sparklyr::sdf_nrow(splits$split1)))
print(paste0("Split2: ", sparklyr::sdf_nrow(splits$split2)))
print(paste0("Split3: ", sparklyr::sdf_nrow(splits$split3)))

```
````

````{tabs}

```{code-tab} plaintext Python Output
Split1: 2970
Split2: 2328
Split3: 600
```

```{code-tab} plaintext R Output
[1] "Split1: 2907"
[1] "Split2: 2387"
[1] "Split3: 604"
```
````
Check that the count of the splits equals the total row count:
````{tabs}
```{code-tab} py
print(f"DF count: {rescue.count()}")
print(f"Split count total: {split1.count() + split2.count() + split3.count()}")
```

```{code-tab} r R

print(paste0("DF count: ", sparklyr::sdf_nrow(rescue)))
print(paste0("Split count total: ",
             sparklyr::sdf_nrow(splits$split1) +
             sparklyr::sdf_nrow(splits$split2) +
             sparklyr::sdf_nrow(splits$split3)))

```
````

````{tabs}

```{code-tab} plaintext Python Output
DF count: 5898
Split count total: 5898
```

```{code-tab} plaintext R Output
[1] "DF count: 5898"
[1] "Split count total: 5898"
```
````

### Further Resources

Spark at the ONS Articles:
- [Persisting in Spark](../spark-concepts/persistence)
- [Caching](../spark-concepts/cache)
- [Checkpoint](../raw-notebooks/checkpoint-staging/checkpoint-staging)

PySpark Documentation:
- [`.sample()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sample.html)
- [`.limit()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.limit.html)
- [`.coalesce()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.coalesce.html)
- [`.randomSplit()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.randomSplit.html)
- [`.sampleBy()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sampleBy.html)

sparklyr Documentation:
- [`sdf_sample()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_sample.html)
- [`sdf_coalesce()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_coalesce.html)
- [`sdf_random_split()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_random_split.html)
- [`sdf_weighted_sample()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_weighted_sample.html)

R Documentation:
- [`head()`](https://stat.ethz.ch/R-manual/R-devel/library/utils/html/head.html)