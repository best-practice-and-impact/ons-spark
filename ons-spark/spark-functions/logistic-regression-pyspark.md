# Logistic Regression in PySpark

Logistic regression is often used in predictive analysis. A logistic
regression model is trained on a dataset to learn how certain
independent variables relate to the probability of a binary outcome
occuring. This model is then applied to cases where the independent
variables are known, but the outcome is unknown, and the probability of
the outcome occuring can be estimated.

This article shows how to use the `Spark ML` functions to generate a
logistic regression model in PySpark, including a
discussion of the steps necessary to prepare the data and potential
issues to consider. We will also show how to access standard errors and
confidence intervals for inferential analysis.

## Preparing the data

There are some steps we need to carry out to prepare the data before we are able to run a regression model in PySpark. 


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
````

Examining the dataset with `printSchema`, we can see that a few columns we have selected (such as `engine_count`) are of the type “character” when they should be numeric. We can convert all of these to numeric values by running the following:

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
````

### Missing values

**Check: logistic regression in python - does it do any of this automatically like glm does?

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
````
Now we have dealt with incorrect data types and missing values, we are
ready to set up and run our logistic regression.

## Running a logistic regression model
- Encode any categorical variables
  - **One hot encoding explanation from R version can be copied to here**
  - Use `StringIndexer` and `OneHotEncoder` from `pyspark.ml.feature` to deal with each categorical variable individually.
  - **Explanation of what we are doing step by step.**
  - Then use `VectorAssembler` to assemble all the predictor variables into a single new column that we will call "features"
  
````{tabs}
```{code-tab} py
# Importing the required libraries
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder

## First need to encode all our categorical variables

# Converting the specialservicetypecategory column
serviceIdx = StringIndexer(inputCol='specialservicetypecategory',
                               outputCol='serviceIndex')
serviceEncode = OneHotEncoder(inputCol='serviceIndex',
                               outputCol='serviceVec')


# Converting the originofcallcolumn
callIdx = StringIndexer(inputCol='originofcall',
                               outputCol='callIndex')
callEncode = OneHotEncoder(inputCol='callIndex',
                               outputCol='callVec')

# Converting the propertycategory column
propertyIdx = StringIndexer(inputCol='propertycategory',
                               outputCol='propertyIndex')
propertyEncode = OneHotEncoder(inputCol='propertyIndex',
                               outputCol='propertyVec')


## Next, need to vectorize all our predictors into a new column called "features" which will be our input/features class

assembler = VectorAssembler(inputCols=['engine_count', 'job_hours', 'hourly_cost',
                                       'callVec', 'propertyVec', 'serviceVec'])


```
````

We will use the `LogisticRegression` `pyspark.ml` function to carry out our regression. The model can be imported and set up like so:

````{tabs}
```{code-tab} py
from pyspark.ml.classification import LogisticRegression
  
log_reg = LogisticRegression(featuresCol='features',
                             labelCol='is_cat')
```
````

Note that these tasks haven't actually been carried out yet, we have simply defined what they are so we can call them all in succession later. This can be done by setting up a "pipeline" to call all the tasks we need to carry out the regression one by one:

````{tabs}
```{code-tab} py
# Creating the pipeline
pipe = Pipeline(stages=[serviceIdx, callIdx, propertyIdx,
                        serviceEncode, callEncode, propertyEncode,
                        assembler, log_reg])
```
````


