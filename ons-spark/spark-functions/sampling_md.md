## Sampling (.md version)

Sampling: `.sample()` and `sdf_sample()`

You can take a sample of a DataFrame with [`.sample()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.sample) in PySpark or [`sdf_sample()`](https://spark.rstudio.com/reference/sdf_sample.html) in sparklyr. This is something that you may want to do during development or initial analysis of data, as with a smaller amount of data your code will run faster and requires less memory to process.

It is important to note that sampling in Spark returns an approximate fraction of the data, rather than an exact one. The reason for this is explained in the [Returning an exact sample](#returning-an-exact-sample) section.

### PySpark example: `.sample()`

First, set up the Spark session, read the Animal Rescue data, and then get the row count:

```python
import os
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[2]").appName("sampling").getOrCreate()

data_path = f"file:///{os.getcwd()}/../data/animal_rescue.parquet"

rescue = spark.read.parquet(data_path)

rescue.count()
```

```
5898
```

To use `.sample()`, set the `fraction`, which is between 0 and 1. So if we want a $20\%$ sample, use `fraction=0.2`. Note that this will give an *approximate* sample and so you will likely get slightly more or fewer rows than you expect.

You can select to sample with replacement by setting `withReplacement=True`, which is set to `False` by default.

```python
rescue_sample = rescue.sample(withReplacement=False, fraction=0.1)
rescue_sample.count()
```

```
594
```

You can also set a seed, in a similar way to how random numbers generators work. This enables replication, which is useful in Spark given that the DataFrame will be otherwise be re-sampled every time an action is called.

```python
rescue_sample_seed_1 = rescue.sample(withReplacement=None,
                      fraction=0.1,
                      seed=99)

rescue_sample_seed_2 = rescue.sample(withReplacement=None,
                      fraction=0.1,
                      seed=99)

print(f"Seed 1 count: {rescue_sample_seed_1.count()}")
print(f"Seed 2 count: {rescue_sample_seed_1.count()}")
```

```
Seed 1 count: 589
Seed 2 count: 589
```

We can see that both samples have returned the same number of rows due to the identical seed.

Another way of replicating results is with persisting. Caching or checkpointing the DataFrame will avoid recalculation of the DF within the same Spark session. Writing out the DF to a Hive table or parquet enables it to be used in subsequent Spark sessions. See the chapter on persisting for more detail.

### R Example: `sdf_sample()`

In sparklyr, you can use `sdf_sample()`. The `fraction`, `seed` and `replacement` (with a slight name variation) arguments work in the same way as `.sample()`. Note that the arguments are in a different default order, which is often more useful in sparklyr as `fraction` is always used whereas `replacement=TRUE` is less common.

```r
rescue_sample <- rescue %>% sparklyr::sdf_sample(fraction=0.1, replacement=FALSE, seed=99)
```
### More details on sampling

The following section gives more detail on sampling and how it is processed on the Spark cluster. is not compulsory reading, but may be of interest to some Spark users.

#### Returning an exact sample

We have demonstrated above that `.sample()` returns an approximate fraction, not an exact one. This is because every row is independently assigned a probability equal to `fraction` of being included in the sample, e.g. with `fraction=0.2` every row has a $20\%$ probability of being in the sample. The number of rows returned in the sample therefore follows the binomial distribution.

The advantage of the sample being calculated in this way is that it is processed as a *narrow transformation*, which is more efficient than a *wide transformation*.

To return an exact sample, one method is to calculate how many rows are required in the sample, create a new column of random numbers and sort by it, and use `.limit()`. This requires an action and a wide transformation, and so will take longer to process than using `.sample()`.

```python
fraction = 0.1
row_count = round(rescue.count() * fraction)
row_count
```

```
590
```

```python
rescue.withColumn("rand_no", F.rand()).orderBy("rand_no").limit(row_count).drop("rand_no").count()
```

```
590
```

#### Partitioning

The number of partitions will remain the same when sampling, even though the DataFrame will be smaller. If you are taking a small fraction of the data then your DataFrame may have too many partitions. You can use [`.coalesce()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.functions.coalesce) in PySpark or [`sdf_coalesce()`](https://spark.rstudio.com/reference/sdf_coalesce.html) in sparklyr to reduce the number of partitions, e.g. if your original DF had $200$ partitions and you take a $10\%$ sample, you can reduce the number of partitions to $20$ with `df.sample(fraction=0.1).coalesce(20)`.

#### Sampling consistently by filtering the data

If the primary reason for using sampling is to process less data during development then an alternative is to filter the data and specify a condition which gives approximately the desired number of rows. This will give consistent results, which may or may not be desirable. For instance, in the Animal Rescue data we could use two years data:

```python
rescue.filter(F.col("CalYear").isin(2012, 2017)).count()
```

```
1142
```

The disadvantage of this method is that you may have data quality issues in the original DF that will not be encountered, whereas these may be discovered with `.sample()`. Using unit testing and test driven development can mitigate the risk of these issues.

### Other sampling functions

This section briefly discusses similar functions to `.sample()` and `sdf_sample()`.

#### Splitting a DF: `.randomSplit()` and `sdf_random_split()`

You can split a DF into two or more DFs with [`.randomSplit()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.randomSplit) in PySpark and [`sdf_random_split()`](https://spark.rstudio.com/reference/sdf_random_split.html) in sparklyr. This is common when using machine learning, to generate training and test datasets.

Every row in the DF will be allocated to one of the split DFs. In common with the other sampling methods the exact size of each split may vary. An optional seed can also be set.

For instance, to split the animal rescue data into three DFs with a weighting of $50\%$, $40\%$ and $10\%$ in PySpark:

```python
split1, split2, split3 = rescue.randomSplit([0.5, 0.4, 0.1])

print(f"Split1: {split1.count()}")
print(f"Split2: {split2.count()}")
print(f"Split3: {split3.count()}")
```

```
Split1: 2930
Split2: 2367
Split3: 601
```

Check that the count of the splits equals the total row count:

```python
print(f"DF count: {rescue.count()}")
print(f"Split count total: {split1.count() + split2.count() + split3.count()}")
```

```
DF count: 5898
Split count total: 5898
```

#### Stratified samples: `.sampleBy()`

A stratified sample can be taken with [`.sampleBy()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.sampleBy) in PySpark. This takes a column, `col`, to sample by, and a dictionary of weights, `fractions`.

In common with other sampling methods this does not return an exact proportion and you can also optionally set a seed.

Note that there is no native sparklyr implementation for stratified sampling, although there is a method for weighted sampling, [`sdf_weighted_sample()`](https://spark.rstudio.com/reference/sdf_weighted_sample.html).

In PySpark, to return $5\%$ of cats, $10\%$ of dogs and $50\%$ of hamsters:

```python
weights = {"Cat": 0.05, "Dog": 0.1, "Hamster": 0.5}
stratified_sample = rescue.sampleBy("AnimalGroupParent", fractions=weights)
stratified_sample_count = (stratified_sample
                           .groupBy("AnimalGroupParent")
                           .agg(F.count("AnimalGroupParent").alias("row_count"))
                           .orderBy("AnimalGroupParent"))
stratified_sample_count.show()
```

```
+-----------------+---------+
|AnimalGroupParent|row_count|
+-----------------+---------+
|              Cat|      141|
|              Dog|      107|
|          Hamster|        7|
+-----------------+---------+
```

We can quickly compare the number of rows for each animal to the expected to confirm that they are approximately equal:

```python
weights_df = spark.createDataFrame(list(weights.items()), schema=["AnimalGroupParent", "weight"])

(rescue
    .groupBy("AnimalGroupParent").count()
    .join(weights_df, on="AnimalGroupParent", how="inner")
    .withColumn("expected_rows", F.round(F.col("count") * F.col("weight"), 0))
    .join(stratified_sample_count, on="AnimalGroupParent", how="left")
    .orderBy("AnimalGroupParent")
    .show()
)
```

```
+-----------------+-----+------+-------------+---------+
|AnimalGroupParent|count|weight|expected_rows|row_count|
+-----------------+-----+------+-------------+---------+
|              Cat| 2909|  0.05|        145.0|      141|
|              Dog| 1008|   0.1|        101.0|      107|
|          Hamster|   14|   0.5|          7.0|        7|
+-----------------+-----+------+-------------+---------+
```

### Further Resources

PySpark Documentation:
- [`.sample()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.sample)
- [`.coalesce()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.functions.coalesce)
- [`.randomSplit()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.randomSplit)
- [`.sampleBy()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.sampleBy)

sparklyr Documentation:
- [`sdf_sample()`](https://spark.rstudio.com/reference/sdf_sample.html)
- [`sdf_coalesce()`](https://spark.rstudio.com/reference/sdf_coalesce.html)
- [`sdf_random_split()`](https://spark.rstudio.com/reference/sdf_random_split.html)
- [`sdf_weighted_sample()`](https://spark.rstudio.com/reference/sdf_weighted_sample.html)

Spark in ONS material:
- Wide and narrow transformations
- Persisting in Spark
- Filtering data
- Unit testing in Spark