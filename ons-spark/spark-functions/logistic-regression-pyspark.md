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

## First we call the StringIndexer separately for each categorical variable

# Indexing the specialservicetypecategory column
serviceIdx = StringIndexer(inputCol='specialservicetypecategory',
                               outputCol='serviceIndex')

# Indexing the originofcallcolumn
callIdx = StringIndexer(inputCol='originofcall',
                               outputCol='callIndex')

# Indexing the propertycategory column
propertyIdx = StringIndexer(inputCol='propertycategory',
                               outputCol='propertyIndex')
                               
# Apply indexing to each column one by one

rescue_cat_indexed = serviceIdx.fit(rescue_cat).transform(rescue_cat)
rescue_cat_indexed = callIdx.fit(rescue_cat_indexed).transform(rescue_cat_indexed)
rescue_cat_indexed = propertyIdx.fit(rescue_cat_indexed).transform(rescue_cat_indexed)

# Check that this has worked correctly
rescue_cat_indexed.select('is_cat', 'specialservicetypecategory', 'originofcall',
                          'propertycategory', 'serviceIndex', 'callIndex', 
                          'propertyIndex').show(10)
```
````

````{tabs}
``` plaintext Python output
+------+-------------------------------+------------------+-----------------+------------+---------+-------------+
|is_cat|specialservicetypecategory     |originofcall      |propertycategory |serviceIndex|callIndex|propertyIndex|
+------+-------------------------------+------------------+-----------------+------------+---------+-------------+
|0     |Other Animal Assistance        |Person (land Line)|Dwelling         |0.0         |1.0      |0.0          |
|0     |Other Animal Assistance        |Person (land Line)|Outdoor Structure|0.0         |1.0      |3.0          |
|0     |Animal Rescue From Below Ground|Person (mobile)   |Outdoor Structure|2.0         |0.0      |3.0          |
|0     |Animal Rescue From Water       |Person (mobile)   |Non Residential  |3.0         |0.0      |2.0          |
|0     |Other Animal Assistance        |Person (mobile)   |Dwelling         |0.0         |0.0      |0.0          |
|0     |Other Animal Assistance        |Person (land Line)|Dwelling         |0.0         |1.0      |0.0          |
|0     |Other Animal Assistance        |Person (land Line)|Outdoor          |0.0         |1.0      |1.0          |
|0     |Animal Rescue From Water       |Person (mobile)   |Outdoor          |3.0         |0.0      |1.0          |
|0     |Animal Rescue From Height      |Person (land Line)|Dwelling         |1.0         |1.0      |0.0          |
|0     |Animal Rescue From Water       |Person (mobile)   |Outdoor          |3.0         |0.0      |1.0          |
+------+-------------------------------+------------------+-----------------+------------+---------+-------------+
```
````
Next we use `OneHotEncoderEstimator` to perform one-hot encoding. **add explanation from R version in here**. 

````{tabs}
```{code-tab} py
# Apply OneHotEncoderEstimator to each categorical column simultaneously
encoder = OneHotEncoderEstimator(inputCols = ['serviceIndex', 'callIndex', 'propertyIndex'], 
                                 outputCols = ['serviceVec', 'callVec', 'propertyVec'])

rescue_cat_ohe = encoder.fit(rescue_cat_indexed).transform(rescue_cat_indexed)

# Check that this has worked correctly 
rescue_cat_ohe.select('is_cat', 'specialservicetypecategory', 'originofcall',
                          'propertycategory', 'serviceVec', 'callVec', 
                          'propertyVec').show(10)

```
````

````{tabs}
``` plaintext Python output
+------+-------------------------------+------------------+-----------------+-------------+-------------+-------------+
|is_cat|specialservicetypecategory     |originofcall      |propertycategory |serviceVec   |callVec      |propertyVec  |
+------+-------------------------------+------------------+-----------------+-------------+-------------+-------------+
|0     |Other Animal Assistance        |Person (land Line)|Dwelling         |(3,[0],[1.0])|(7,[1],[1.0])|(6,[0],[1.0])|
|0     |Other Animal Assistance        |Person (land Line)|Outdoor Structure|(3,[0],[1.0])|(7,[1],[1.0])|(6,[3],[1.0])|
|0     |Animal Rescue From Below Ground|Person (mobile)   |Outdoor Structure|(3,[2],[1.0])|(7,[0],[1.0])|(6,[3],[1.0])|
|0     |Animal Rescue From Water       |Person (mobile)   |Non Residential  |(3,[],[])    |(7,[0],[1.0])|(6,[2],[1.0])|
|0     |Other Animal Assistance        |Person (mobile)   |Dwelling         |(3,[0],[1.0])|(7,[0],[1.0])|(6,[0],[1.0])|
|0     |Other Animal Assistance        |Person (land Line)|Dwelling         |(3,[0],[1.0])|(7,[1],[1.0])|(6,[0],[1.0])|
|0     |Other Animal Assistance        |Person (land Line)|Outdoor          |(3,[0],[1.0])|(7,[1],[1.0])|(6,[1],[1.0])|
|0     |Animal Rescue From Water       |Person (mobile)   |Outdoor          |(3,[],[])    |(7,[0],[1.0])|(6,[1],[1.0])|
|0     |Animal Rescue From Height      |Person (land Line)|Dwelling         |(3,[1],[1.0])|(7,[1],[1.0])|(6,[0],[1.0])|
|0     |Animal Rescue From Water       |Person (mobile)   |Outdoor          |(3,[],[])    |(7,[0],[1.0])|(6,[1],[1.0])|
+------+-------------------------------+------------------+-----------------+-------------+-------------+-------------+
```
````

Then, we need to vectorise all our predictors into a new column called "features" which will be our input/features class. We must also rename our target variable column, "is_cat" to "label":

````{tabs}
```{code-tab} py

# Call 'VectorAssembler' to vectorise all predictor columns in dataset
assembler = VectorAssembler(inputCols=['engine_count', 'job_hours', 'hourly_cost',
                                       'callVec', 'propertyVec', 'serviceVec'])
 
# Apply vectorisation                                      
rescue_cat_vectorised = assembler.transform(rescue_cat_ohe)

# Rename "is_cat" target variable column to "label" ready to pass to the regression model
rescue_cat_final = rescue_cat_vectorised.withColumnRenamed("is_cat", "label").select("label", "features")

```
````

We will use the `GeneralizedLinearRegression` `pyspark.ml` function to carry out our regression. The model can be imported and set up like so: **argument setting **

````{tabs}
```{code-tab} py
# Import GeneralizedLinearRegression
from pyspark.ml.regression import GeneralizedLinearRegression

# Define model
glr = GeneralizedLinearRegression(family="binomial", link="logit")
```
````
Once this has been done we can run the model like so:

````{tabs}
```{code-tab} py
# Run model
model = glr.fit(rescue_cat_final)

# Get model results
model_output = model.transform(rescue_cat_final)
model_output.show(10)
```
````

````{tabs}
```plaintext Python output
+-----+--------------------+-------------------+
|label|            features|         prediction|
+-----+--------------------+-------------------+
|    0|(19,[0,1,2,5],[1....|  0.583734005921554|
|    0|(19,[0,1,2,5,11],...|0.25588401926913557|
|    0|(19,[0,1,2,11,18]...| 0.3199019536753837|
|    0|(19,[0,1,2,14,16]...| 0.1586171547926693|
|    0|(19,[0,1,2],[1.0,...|   0.55724909555173|
|    0|(19,[0,1,2,5],[1....| 0.5898814716386075|
|    0|(19,[0,1,2,5,12],...| 0.4056895632592243|
|    0|(19,[0,1,2,12,16]...|0.18097012233391097|
|    0|(19,[0,1,2,5,17],...| 0.6870145858037447|
|    0|(19,[0,1,2,12,16]...|0.18097012233391097|
+-----+--------------------+-------------------+
```
````
## Inferential Analysis 

## Things to watch out for

### Singularity issue
**Investigate this for PySpark**

### Selecting reference categories
When including categorical variables as independent variables in a
regression, one of the categories must act as the reference category.
The coefficients for all other categories are relative to the reference
category.

By default, the `OneHotEncoderEstimator` function will
select the least common category as the reference category. For example, `specialservicetypecategory` has four unique values: Animal
Rescue From Below Ground, Animal Rescue From Height, Animal Rescue From
Water, and Other Animal Assistance.

````{tabs}
```{code-tab} py
rescue_cat.groupBy("specialservicetypecategory").count().orderBy("count").show(truncate = False)
```
````

````{tabs}
``` plaintext Python output
+-------------------------------+-----+
|specialservicetypecategory     |count|
+-------------------------------+-----+
|Animal Rescue From Water       |343  |
|Animal Rescue From Below Ground|593  |
|Animal Rescue From Height      |2123 |
|Other Animal Assistance        |2801 |
+-------------------------------+-----+
```
````
Since there are only 343 instances of “Animal Rescue From Water” in the data, this has been automatically selected as the reference category in the example above. Regression coefficients in the previous section are therefore shown relative
to the to the “Animal Rescue From Water” reference category.

Selecting a reference category can be particularly useful for
inferential analysis. For example, in the `rescue_cat` dataset, we might want to
select “Other Animal Assistance” as our reference category instead
because it is the largest special service type and could serve as a
useful reference point.

To do this, we can make use of the additional `stringOrderType` argument for the `StringIndexer` function we used previously to index our categorical variables. This allows us to specify the indexing order. By default, this argument is set to `frequencyDesc`, so "Animal Rescue From Water" would be ordered last and is subsequently dropped by the `OneHotEncoderEstimator` function and set as the reference category. If we want "Other Animal Assistance" to be dropped instead, we need to ensure it is placed last in the indexing order. In this case, specifying `stringOrderType = frequencyAsc` would be sufficient to achieve this. However, to keep this example as general as possible, a convenient way of ensuring any chosen reference category is ordered last is add an appropriate prefix to it, such as "000_", and order the categories in descending alphabetical order (`alphabetDesc`):

````{tabs}
```{code-tab} py
# Add "000_" prefix to selected reference categories

rescue_cat_reindex = (rescue_cat
                      .withColumn('specialservicetypecategory', 
                                       F.when(F.col('specialservicetypecategory')=="Other Animal Assistance", "000_Other Animal Assistance")
                                       .otherwise(F.col('specialservicetypecategory')))
                      .withColumn('originofcall', 
                                       F.when(F.col('originofcall') == "Person (mobile)", "000_Person (mobile)")
                                       .otherwise(F.col('originofcall')))
                      .withColumn('propertycategory', 
                                       F.when(F.col('propertycategory') == "Dwelling", "000_Dwelling")
                                       .otherwise(F.col('propertycategory'))))

# Check prefix additions 
rescue_cat_reindex.select('specialservicetypecategory', 'originofcall', 'propertycategory').show(20)

# Use stringOrderType arg of StringIndexer

# Re-ndexing the specialservicetypecategory column
serviceIdx = StringIndexer(inputCol='specialservicetypecategory',
                               outputCol='serviceIndex', 
                               stringOrderType = "alphabetDesc")

# Indexing the originofcallcolumn
callIdx = StringIndexer(inputCol='originofcall',
                               outputCol='callIndex',
                               stringOrderType = "alphabetDesc")

# Indexing the propertycategory column
propertyIdx = StringIndexer(inputCol='propertycategory',
                               outputCol='propertyIndex', 
                               stringOrderType = "alphabetDesc")

# Call indexing for each column one by one

rescue_cat_indexed = serviceIdx.fit(rescue_cat_reindex).transform(rescue_cat_reindex)
rescue_cat_indexed = callIdx.fit(rescue_cat_indexed).transform(rescue_cat_indexed)
rescue_cat_indexed = propertyIdx.fit(rescue_cat_indexed).transform(rescue_cat_indexed)
```
````

Once this is done we can run the regression again and get the
coefficients relative to the chosen reference categories:

````{tabs}
```{code-tab} py
encoder = OneHotEncoderEstimator(inputCols = ['serviceIndex', 'callIndex', 'propertyIndex'], 
                                 outputCols = ['serviceVec', 'callVec', 'propertyVec'])

rescue_cat_ohe = encoder.fit(rescue_cat_indexed).transform(rescue_cat_indexed)


## Next, need to vectorize all our predictors into a new column called "features" which will be our input/features class

assembler = VectorAssembler(inputCols=['engine_count', 'job_hours', 'hourly_cost', 
                                       'callVec', 'propertyVec', 'serviceVec'], 
                           outputCol = "features")

rescue_cat_vectorised = assembler.transform(rescue_cat_ohe)


rescue_cat_final = rescue_cat_vectorised.withColumnRenamed("is_cat", "label").select("label", "features")

# Run the model again
model = glr.fit(rescue_cat_final)

# Show summary
model.summary

```
````
### Pipelines

Note that these tasks haven't actually been carried out yet, we have simply defined what they are so we can call them all in succession later. This can be done by setting up a "pipeline" to call all the tasks we need to carry out the regression one by one:

````{tabs}
```{code-tab} py
# Creating the pipeline
pipe = Pipeline(stages=[serviceIdx, callIdx, propertyIdx,
                        serviceEncode, callEncode, propertyEncode,
                        assembler, log_reg])
```
````


