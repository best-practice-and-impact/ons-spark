# Logistic Regression

Logistic regression is often used in predictive analysis. A logistic
regression model is trained on a dataset to learn how certain
independent variables relate to the probability of a binary outcome
occuring. This model is then applied to cases where the independent
variables are known, but the outcome is unknown, and the probability of
the outcome occuring can be estimated.

This article shows how to use the `Spark ML` functions to generate a
logistic regression model in PySpark and sparklyr, including a
discussion of the steps necessary to prepare the data and potential
issues to consider. We will also show how to access standard errors and
confidence intervals for inferential analysis and how to assemble these steps into a pipeline.

**Comment about different ordering/process in PySpark vs sparklyr**

## Preparing the data

**ADD what we we use in PySpark**
In sparklyr, we will be using the `Spark ML` functions
`ml_logistic_regression`, `ml_predict` and
`ml_generalised_linear_regression`. The syntax used to call these
functions is somewhat similar to logistic regression functions in R.
However, there are some additional steps to take in preparing the data
before the model can be run successfully.

We will read the data in as follows:

````{tabs}
```{code-tab} py 
#import packages

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import pandas as pd


#set up small Spark session

""" spark = (
    SparkSession.builder.appName("pyspark_lab_2")
    .config("spark.executor.memory", "1g")
    .config("spark.executor.cores", 1)
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.maxExecutors", 3)
    .config("spark.sql.shuffle.partitions", 12)
    .config("spark.shuffle.service.enabled", "true")
    .config("spark.ui.showConsoleProgress", "false")
    .enableHiveSupport()
    .getOrCreate()
)
 """

#local session 
spark = (SparkSession.builder.master("local[2]")
         .appName("logistic-regression")

# Set the data path
rescue_path_parquet = '/training/rescue_clean.parquet'

# Read in the data
rescue = spark.read.parquet(rescue_path_parquet)

rescue.limit(5).toPandas()

```
```{code-tab} r R 
library(sparklyr)
library(dplyr)
library(broom)

sc <- sparklyr::spark_connect(
  master = "local[2]",
  app_name = "logistic-regression",
  config = sparklyr::spark_config())


sparklyr::spark_connection_is_open(sc)

rescue_path_parquet = "/training/rescue_clean.parquet"
rescue <- sparklyr::spark_read_parquet(sc, rescue_path_parquet)
dplyr::glimpse(rescue)
```
````
Let’s say we want to use the `rescue` data to predict whether an animal
is a cat or not.

````{tabs}
```{code-tab} py
# Create is_cat column to contain target variable and select relevant predictors
rescue_cat = rescue.withColumn('is_cat', 
                               F.when(F.col('animal_group')=="Cat", 1)
                               .otherwise(0)).select("typeofincident", 
                              "engine_count", 
                              "job_hours", 
                              "hourly_cost", 
                              "total_cost", 
                              "originofcall", 
                              "propertycategory",
                              "specialservicetypecategory",
                              "incident_duration",
                              "is_cat")

# Check created column
rescue_cat.limit(20).toPandas()

# Check data types
rescue_cat.printSchema
```

```{code-tab} r R
# Create is_cat column to contain target variable and select relevant predictors
rescue_cat <- rescue %>% 
  dplyr::mutate(is_cat = ifelse(animal_group == "Cat", 1, 0)) %>% 
  sparklyr::select(typeofincident, 
                   engine_count, 
                   job_hours, 
                   hourly_cost, 
                   total_cost, 
                   originofcall, 
                   propertycategory,
                   specialservicetypecategory,
                   incident_duration,
                   is_cat)

dplyr::glimpse(rescue_cat)
```
````
Examining the dataset with `printSchema`/`glimpse()`, we can see that a few columns we have selected (such as `engine_count`) are of the type “character” when they should be numeric. We can convert all of these to numeric values by running the following:

````{tabs}
```{code-tab} py
# Convert engine_count, job_hours, hourly_cost, total_cost and incident_duration columns to numeric
rescue_cat = (
  rescue_cat.withColumn("engine_count", F.col("engine_count").cast("double"))
            .withColumn("job_hours", F.col("job_hours").cast("double"))
            .withColumn("hourly_cost", F.col("hourly_cost").cast("double"))
            .withColumn("total_cost", F.col("total_cost").cast("double"))
            .withColumn("incident_duration", F.col("incident_duration").cast("double")))


# Check data types are now correct
rescue_cat.printSchema

```
```{code-tab} r R 
# Convert engine_count, job_hours, hourly_cost, total_cost and incident_duration columns to numeric
  rescue_cat <- rescue_cat %>% 
  dplyr::mutate(across(c(engine_count:total_cost, incident_duration), 
                       ~as.numeric(.)))

dplyr::glimpse(rescue_cat)
```
````
### Missing values

You may already be familiar with functions such as `glm` for performing
logistic regression in R. Many of these functions are quite
user-friendly and will automatically account for issues such as missing
values in the data. However, the `Spark ML` functions in
`sparklyr`require the user to correct for these issues before running
the regression functions.

**Check: logistic regression in python - does it do any of this automatically like glm does?**

````{tabs}
```{code-tab} py
# Get the count of missing values for each column
missing_summary = (
    rescue_cat
    .select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in rescue_cat.columns])
)

# Show the summary
missing_summary.show(vertical = True)

# We can see that these are all on the same rows by filtering for NAs in one of the columns:
rescue_cat.filter(rescue_cat.total_cost.isNull()).limit(38).toPandas()

# For simplicity, we will just filter out these rows:
rescue_cat = rescue_cat.na.drop()

# Double check we have no nulls left:

missing_summary = (
    rescue_cat
    .select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in rescue_cat.columns])
)
```

```{code-tab} r R 
# Get the number of NAs in the dataset by column
rescue_cat %>%
  dplyr::summarise_all(~sum(as.integer(is.na(.)))) %>% 
  print(width = Inf)

# There are 38 missing values in 4 of the columns 
# We can see that these are all on the same rows by filtering for NAs in one of the columns:
rescue_cat %>%
  sparklyr::filter(is.na(total_cost)) %>%
  print(n=38)

# For simplicity, we will just filter out these rows:
rescue_cat <- rescue_cat %>%
  sparklyr::filter(!is.na(total_cost)) 

# Double check we have no NAs left: 
rescue_cat %>%
  dplyr::summarise_all(~sum(as.integer(is.na(.)))) %>% 
  print(width = Inf)
```
````

Now we have dealt with incorrect data types and missing values, we are
ready to run our logistic regression.

