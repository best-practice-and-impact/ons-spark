#!/usr/bin/env python
# coding: utf-8

# # Union two DataFrames with different columns

# The union of two DataFrames is the process of appending one DataFrame below another.
# 
# The [PySpark `.union()` function](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.union.html) is equivalent to the SQL `UNION ALL` function, where both DataFrames must have the same number of columns. However the [sparklyr `sdf_bind_rows()` function](https://rdrr.io/github/rstudio/sparklyr/man/sdf_bind.html) can combine two DataFrames with different number of columns, by putting `NULL` values into the rows of data.
# 
# Here's how we can use PySpark to mimic the behaviour of the `sdf_bind_rows()` function in sparklyr.

# In[1]:


import pyspark.sql.functions as F
from pyspark.sql import SparkSession


# In[2]:


spark = SparkSession.builder.appName("union-example").getOrCreate()


# Create a DataFrame of Wimbledon singles champions from 2017 to 2019

# In[3]:


df1_schema =  """
    `tournament_year` int,
    `event` string,
    `champion` string
"""

df1 = spark.createDataFrame([
    [2017, "Gentlemen's Singles", "Federer"],
    [2018, "Gentlemen's Singles", "Djokovic"],
    [2019, "Gentlemen's Singles", "Djokovic"],
    [2017, "Ladies' Singles", "Muguruza"],
    [2018, "Ladies' Singles", "Kerber"],
    [2019, "Ladies' Singles", "Halep"],
    ], 
    schema=df1_schema
)

df1.show()


# Next we want to append 2020 data. However, there was no Wimbledon tournament in 2020. We'll just create two columns.

# In[4]:


df2_schema = """
    `tournament_year` int,
    `event` string
"""

df2 = spark.createDataFrame([
    [2020, "Gentlemen's Singles"],
    [2020, "Ladies' Singles"]
    ],
    schema=df2_schema
)

df2.show()


# Let's try to union these DataFrames together

# In[5]:


try:
    df_joined = df1.union(df2)
except Exception as e:
    print(e)


# The error message says we need the same number of columns. So let's try adding a column to `df2` full of `Null` values before the union

# In[6]:


df_joined = df1.union(df2.withColumn("champion", F.lit(None)))
df_joined.printSchema()
df_joined.show()


# This time it worked. We get the result we were looking for. 
# 
# However, we need to be careful in doing this. What if the columns in `df2` were defined in a different order?

# In[7]:


df2_schema = """
    `event` string,
    `tournament_year` int
"""

df2 = spark.createDataFrame([
    ["Gentlemen's Singles", 2020],
    ["Ladies' Singles", 2020]
    ],
    schema=df2_schema
)

df2.show()


# In[8]:


df_joined = df1.union(df2.withColumn("champion", F.lit(None)))
df_joined.printSchema()
df_joined.show()


# The code runs, but the result isn't what we want. We should therefore write our code in a way that mitigates the risk of this happening. We might have the correct order now, but in future perhaps the order might change.
# 
# We'll take the column order from the DataFrame with all the columns, `df1`, and force `df2` to have the same column order before doing the union.

# In[9]:


col_order = df1.columns
df_joined = df1.union(df2.withColumn("champion", F.lit(None)).select(col_order))
df_joined.printSchema()
df_joined.show()


# Let's look at one more example where we have a third DataFrame with different columns. Such as results from the 2021 tournament, which hasn't taken place yet (at the time or writing).

# In[10]:


df3_schema = """
    `tournament_year` int
"""

df3 = spark.createDataFrame([
    [2021],
    [2021]
    ],
    schema=df3_schema
)

df3.show()


# We want a list of unique columns in all the DataFrames along with their types. We can use `set()` to get the unique column names and types, then convert into a dictionary to create key/value pairs

# In[11]:


col_dict = dict(set(df1.dtypes + df2.dtypes + df3.dtypes))
col_dict


# Next we'll create a function that checks to see if a DataFrame has all the columns we need for the union. If the DataFrame is missing a column we'll add an empty column with that name, and give it the correct type using `.cast()`

# In[12]:


def add_empty_columns(df, col_dict):
    for col in col_dict.keys():
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast(col_dict[col]))
    return df


# Next we apply the function to all three DataFrames

# In[13]:


df1 = add_empty_columns(df1, col_dict)
df2 = add_empty_columns(df2, col_dict)
df3 = add_empty_columns(df3, col_dict)    


# We need to decide on a column order for the unions, we can get this from `col_dict.keys()`

# In[14]:


col_order = list(col_dict.keys())
col_order


# And finally, do the union. Note we use `.select(col_order)` after referencing each DataFrame to make sure the columns are in a consistent order

# In[15]:


df_joined = df1.select(col_order).union(df2.select(col_order)).union(df3.select(col_order))


# In[16]:


df_joined.printSchema()
df_joined.show()

