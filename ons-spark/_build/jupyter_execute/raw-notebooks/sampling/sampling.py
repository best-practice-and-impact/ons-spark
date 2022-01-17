#!/usr/bin/env python
# coding: utf-8

# ## Sampling: `.sample()` and `sdf_sample()`
# 
# You can take a sample of a DataFrame with [`.sample()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.sample) in PySpark or [`sdf_sample()`](https://spark.rstudio.com/reference/sdf_sample.html) in sparklyr. This is something that you may want to do during development or initial analysis of data, as with a smaller amount of data your code will run faster and requires less memory to process.
# 
# It is important to note that sampling in Spark returns an approximate fraction of the data, rather than an exact one. The reason for this is explained in the [Returning an exact sample](#returning-an-exact-sample) section.
# 
# ### Example: `.sample()` and `sdf_sample()`
# 
# First, set up the Spark session, read the Animal Rescue data, and then get the row count:

# In[1]:


import os
import yaml
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[2]").appName("sampling").getOrCreate()

with open("../../../config.yaml") as f:
    config = yaml.safe_load(f)
    
rescue_path = config["rescue_path"]
rescue = spark.read.parquet(rescue_path)

rescue.count()


# ```r
# library(sparklyr)
# 
# default_config <- sparklyr::spark_config()
# 
# sc <- sparklyr::spark_connect(
#     master = "local[2]",
#     app_name = "sampling",
#     config = default_config)
# 
# config <- yaml::yaml.load_file("ons-spark/config.yaml")
# 
# rescue <- sparklyr::spark_read_parquet(sc, config$rescue_path)
# 
# rescue %>% sparklyr::sdf_nrow()
# ```

# To use `.sample()`, set the `fraction`, which is between 0 and 1. So if we want a $20\%$ sample, use `fraction=0.2`. Note that this will give an *approximate* sample and so you will likely get slightly more or fewer rows than you expect.
# 
# You can select to sample with replacement by setting `withReplacement=True` in PySpark or `replacement=TRUE` in sparklyr, which is set to `False` by default.
# 
# For these functions it is advised to specify the arguments explicitly. One reason is that `fraction` is a compulsory argument, but in PySpark is *after* `withReplacement`. Another reason is that in sparklyr the arguments are in a different order, with `fraction` listed first; if you use both languages it is easy to make a mistake.

# In[2]:


rescue_sample = rescue.sample(withReplacement=False, fraction=0.1)
rescue_sample.count()


# ```r
# rescue_sample <- rescue %>% sparklyr::sdf_sample(fraction=0.1, replacement=FALSE)
# rescue_sample %>% sparklyr::sdf_nrow()
# ```

# You can also set a seed, in a similar way to how random numbers generators work. This enables replication, which is useful in Spark given that the DataFrame will be otherwise be re-sampled every time an action is called.

# In[3]:


rescue_sample_seed_1 = rescue.sample(withReplacement=None,
                      fraction=0.1,
                      seed=99)

rescue_sample_seed_2 = rescue.sample(withReplacement=None,
                      fraction=0.1,
                      seed=99)

print(f"Seed 1 count: {rescue_sample_seed_1.count()}")
print(f"Seed 2 count: {rescue_sample_seed_1.count()}")


# ```r
# 
# rescue_sample_seed_1 <- rescue %>% sparklyr::sdf_sample(fraction=0.1, seed=99)
# rescue_sample_seed_2 <- rescue %>% sparklyr::sdf_sample(fraction=0.1, seed=99)
# 
# print(paste0("Seed 1 count: ", rescue_sample_seed_1 %>% sparklyr::sdf_nrow()))
# print(paste0("Seed 2 count: ", rescue_sample_seed_2 %>% sparklyr::sdf_nrow()))
# ```

# We can see that both samples have returned the same number of rows due to the identical seed.
# 
# Another way of replicating results is with persisting. Caching or checkpointing the DataFrame will avoid recalculation of the DF within the same Spark session. Writing out the DF to a Hive table or parquet enables it to be used in subsequent Spark sessions. See the chapter on persisting for more detail.
# 
# ### More details on sampling
# 
# The following section gives more detail on sampling and how it is processed on the Spark cluster. is not compulsory reading, but may be of interest to some Spark users.
# 
# #### Returning an exact sample
# 
# We have demonstrated above that `.sample()`/`sdf_sample()` return an approximate fraction, not an exact one. This is because every row is independently assigned a probability equal to `fraction` of being included in the sample, e.g. with `fraction=0.2` every row has a $20\%$ probability of being in the sample. The number of rows returned in the sample therefore follows the binomial distribution.
# 
# The advantage of the sample being calculated in this way is that it is processed as a *narrow transformation*, which is more efficient than a *wide transformation*.
# 
# To return an exact sample, one method is to calculate how many rows are required in the sample, create a new column of random numbers and sort by it, and use `.limit()` in PySpark or `head()` in sparklyr. This requires an action and a wide transformation, and so will take longer to process than using `.sample()`.

# In[4]:


fraction = 0.1
row_count = round(rescue.count() * fraction)
row_count


# ```r
# fraction <- 0.1
# row_count <- round(sparklyr::sdf_nrow(rescue) * fraction)
# row_count
# ```

# In[5]:


rescue.withColumn("rand_no", F.rand()).orderBy("rand_no").limit(row_count).drop("rand_no").count()


# ```r
# rescue %>%
#     sparklyr::mutate(rand_no = rand()) %>%
#     dplyr::arrange(rand_no) %>%
#     head(row_count) %>%
#     sparklyr::select(-rand_no) %>%
#     sparklyr::sdf_nrow()
# ```

# #### Partitioning
# 
# The number of partitions will remain the same when sampling, even though the DataFrame will be smaller. If you are taking a small fraction of the data then your DataFrame may have too many partitions. You can use [`.coalesce()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.functions.coalesce) in PySpark or [`sdf_coalesce()`](https://spark.rstudio.com/reference/sdf_coalesce.html) in sparklyr to reduce the number of partitions, e.g. if your original DF had $200$ partitions and you take a $10\%$ sample, you can reduce the number of partitions to $20$ with `df.sample(fraction=0.1).coalesce(20)`.
# 
# #### Sampling consistently by filtering the data
# 
# If the primary reason for using sampling is to process less data during development then an alternative is to filter the data and specify a condition which gives approximately the desired number of rows. This will give consistent results, which may or may not be desirable. For instance, in the Animal Rescue data we could use two years data:

# In[6]:


rescue.filter(F.col("CalYear").isin(2012, 2017)).count()


# ```r
# rescue %>%
#     sparklyr::filter(CalYear == 2012 | CalYear == 2017) %>%
#     sparklyr::sdf_nrow()
# ```

# The disadvantage of this method is that you may have data quality issues in the original DF that will not be encountered, whereas these may be discovered with `.sample()`. Using unit testing and test driven development can mitigate the risk of these issues.
# 
# ### Other sampling functions
# 
# This section briefly discusses similar functions to `.sample()` and `sdf_sample()`.
# 
# #### Splitting a DF: `.randomSplit()` and `sdf_random_split()`
# 
# You can split a DF into two or more DFs with [`.randomSplit()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.randomSplit) in PySpark and [`sdf_random_split()`](https://spark.rstudio.com/reference/sdf_random_split.html) in sparklyr. This is common when using machine learning, to generate training and test datasets.
# 
# Every row in the DF will be allocated to one of the split DFs. In common with the other sampling methods the exact size of each split may vary. An optional seed can also be set.
# 
# For instance, to split the animal rescue data into three DFs with a weighting of $50\%$, $40\%$ and $10\%$ in PySpark:

# In[7]:


split1, split2, split3 = rescue.randomSplit([0.5, 0.4, 0.1])

print(f"Split1: {split1.count()}")
print(f"Split2: {split2.count()}")
print(f"Split3: {split3.count()}")


# ```r
# splits <- rescue %>% sparklyr::sdf_random_split(
#     split1 = 0.5,
#     split2 = 0.4,
#     split3 = 0.1)
# 
# print(paste0("Split1: ", sparklyr::sdf_nrow(splits$split1)))
# print(paste0("Split2: ", sparklyr::sdf_nrow(splits$split2)))
# print(paste0("Split3: ", sparklyr::sdf_nrow(splits$split3)))
# ```

# Check that the count of the splits equals the total row count:

# In[8]:


print(f"DF count: {rescue.count()}")
print(f"Split count total: {split1.count() + split2.count() + split3.count()}")


# ```r
# print(paste0("DF count: ", sparklyr::sdf_nrow(rescue)))
# print(paste0("Split count total: ",
#              sparklyr::sdf_nrow(splits$split1) +
#              sparklyr::sdf_nrow(splits$split2) +
#              sparklyr::sdf_nrow(splits$split3)))
# ```

# #### Stratified samples: `.sampleBy()`
# 
# A stratified sample can be taken with [`.sampleBy()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.sampleBy) in PySpark. This takes a column, `col`, to sample by, and a dictionary of weights, `fractions`.
# 
# In common with other sampling methods this does not return an exact proportion and you can also optionally set a seed.
# 
# Note that there is no native sparklyr implementation for stratified sampling, although there is a method for weighted sampling, [`sdf_weighted_sample()`](https://spark.rstudio.com/reference/sdf_weighted_sample.html).
# 
# In PySpark, to return $5\%$ of cats, $10\%$ of dogs and $50\%$ of hamsters:

# In[9]:


weights = {"Cat": 0.05, "Dog": 0.1, "Hamster": 0.5}
stratified_sample = rescue.sampleBy("AnimalGroupParent", fractions=weights)
stratified_sample_count = (stratified_sample
                           .groupBy("AnimalGroupParent")
                           .agg(F.count("AnimalGroupParent").alias("row_count"))
                           .orderBy("AnimalGroupParent"))
stratified_sample_count.show()


# We can quickly compare the number of rows for each animal to the expected to confirm that they are approximately equal:

# In[10]:


weights_df = spark.createDataFrame(list(weights.items()), schema=["AnimalGroupParent", "weight"])

(rescue
    .groupBy("AnimalGroupParent").count()
    .join(weights_df, on="AnimalGroupParent", how="inner")
    .withColumn("expected_rows", F.round(F.col("count") * F.col("weight"), 0))
    .join(stratified_sample_count, on="AnimalGroupParent", how="left")
    .orderBy("AnimalGroupParent")
    .show()
)


# ### Further Resources
# 
# PySpark Documentation:
# - [`.sample()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.sample)
# - [`.coalesce()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.functions.coalesce)
# - [`.randomSplit()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.randomSplit)
# - [`.sampleBy()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.sampleBy)
# 
# sparklyr Documentation:
# - [`sdf_sample()`](https://spark.rstudio.com/reference/sdf_sample.html)
# - [`sdf_coalesce()`](https://spark.rstudio.com/reference/sdf_coalesce.html)
# - [`sdf_random_split()`](https://spark.rstudio.com/reference/sdf_random_split.html)
# - [`sdf_weighted_sample()`](https://spark.rstudio.com/reference/sdf_weighted_sample.html)
# 
# Spark in ONS material:
# - Wide and narrow transformations
# - Persisting in Spark
# - Filtering data
# - Unit testing in Spark
