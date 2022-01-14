#!/usr/bin/env python
# coding: utf-8

# ## Reference columns by name: `F.col()`
# 
# There are several different ways to reference columns in a PySpark DataFrame, e.g. in a `.filter()` operation:
# - `df.filter(F.col("column_name" == value))`: references column by name; the recommended method, used throughout this book
# - `df.filter(df.column_name == value)`: references column directly from the DF
# - `df.flter(df["column_name"] == value)`: pandas style, less commonly used in PySpark
# 
# The preferred method is using `F.col()` from the `pyspark.sql.functions` module and is used throughout this book. Although all three methods above will work in some circumstances, only `F.col()` will always have the desired outcome. This is because it references the column by *name* rather than directly from the DF, which means columns not yet assigned to the DF can be used, e.g. when chaining several operations on the same DF together.
# 
# There are several cases where `F.col()` will work but one of the other methods will not:
# - [Filter the DataFrame when reading in](#filter-the-DataFrame-when-reading-in)
# - [Filter on a new column](#filter-on-a-new-column)
# - [Ensuring you are using the latest values](#ensuring-you-are-using-the-latest-values)
# - [Columns with special characters or spaces](#columns-with-special-characters-or-spaces)

# ### Filter the DataFrame when reading in
# 
# First, import the modules and create a Spark session:

# In[1]:


from pyspark.sql import SparkSession, pyspark.sql.functions as F

spark = SparkSession.builder.master("local[2]").appName("sampling").getOrCreate()
data_path = f"file:///{os.getcwd()}/../data/animal_rescue.parquet"


# We can filter on columns when reading in the DataFrame. For instance to only read `"Cat"` from the animal rescue data:

# In[2]:


cats = spark.read.csv("/training/animal_rescue.csv", header=True).filter(F.col("AnimalGroupParent") == "Cat")
cats.select("IncidentNumber", "AnimalGroupParent").show(5)


# This can't be done using the `cats.AnimalGroupParent` as we haven't defined `cats` when referencing the DataFrame. To use the other notation we need to define `rescue` then filter on `cats.AnimalGroupParent`:

# In[3]:


rescue = spark.read.csv("/training/animal_rescue.csv", header=True)
cats.filter(rescue.AnimalGroupParent == "Cat").select("IncidentNumber", "AnimalGroupParent").show(5)


# ### Filter on a new column
# 
# Read in the animal rescue data:

# In[4]:


rescue = spark.read.csv("/training/animal_rescue.csv", header=True).select("IncidentNumber", "AnimalGroupParent")


# Let's create a new column, `AnimalGroup`, which consists of the `AnimalGroupParent` in uppercase.
# 
# If we try and immediately filter on this column using `df.AnimalGroup`, it won't work. This is because we have yet to define the column in `rescue`.

# In[5]:


try:
    rescue.withColumn("AnimalGroup", F.upper(df.AnimalGroupParent)).filter(df.AnimalGroup == "CAT").show(10)
except AttributeError as e:
    print(e)


# We could split this statement up over two different lines:

# In[6]:


rescue_upper = df.withColumn("AnimalGroup", F.upper(df.AnimalGroupParent))
rescue_upper.filter(rescue_upper.AnimalGroup == "CAT").show(5)


# Here is a case where we could use `F.col()` instead:

# In[7]:


rescue.withColumn("AnimalGroup", F.upper("AnimalGroupParent")).filter(F.col("AnimalGroup") == "CAT").show(10)


# ### Example 3: Ensuring you are using the latest values
# 
# Using `df.column_name` can also result in bugs when you think you are referencing the latest values, but aren't. Here, the values in `AnimalGroupParent` are changed, but `df` is yet to be redefined, and so the old values are used. As such no data is returned:

# In[8]:


rescue = spark.read.csv("/training/animal_rescue.csv", header=True).select("IncidentNumber", "AnimalGroupParent")
rescue.withColumn("AnimalGroupParent", F.upper(rescue.AnimalGroupParent)).filter(rescue.AnimalGroupParent == "CAT").show(10)


# Changing to `F.col("AnimalGroupParent")` gives the correct result:

# In[9]:


df.withColumn("AnimalGroupParent", F.upper("AnimalGroupParent")).filter(F.col("AnimalGroupParent") == "CAT").show(10)


# ### Example 4: Columns with special characters or spaces
# 
# One final use case for this method is when your source data has column names with spaces or special characters in them. The animal rescue data has a column called `IncidentNotionalCost(£)`. You can't refer to the column using `df.IncidentNotionalCost(£)`, instead, use `F.col("df.IncidentNotionalCost(£)")`:

# In[10]:


rescue = spark.read.csv("/training/animal_rescue.csv", header=True).select("IncidentNumber", "IncidentNotionalCost(£)")
rescue.filter(F.col("IncidentNotionalCost(£)") > 2500).show()


# You can use the pandas style `df["IncidentNotionalCost(£)"]` but this notation isn't encouraged in PySpark:

# In[11]:


rescue.filter(rescue["IncidentNotionalCost(£)"] > 2500).show()


# Of course, the best idea is to rename the column something sensible:

# In[12]:


rescue = rescue.withColumnRenamed("IncidentNotionalCost(£)", "notional_cost")
rescue.filter(F.col("notional_cost") > 2500).show()


# In[ ]:


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
- Style guide

