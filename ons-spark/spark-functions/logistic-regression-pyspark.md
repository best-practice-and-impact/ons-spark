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
Steps:
- Encode any categorical variables
  - Use `StringIndexer` and `OneHotEncoder` from `pyspark.ml.feature` to deal with each categorical variable individually.
  - **Explanation of what we are doing step by step.**
  - Then use `VectorAssembler` to assemble all the predictor variables into a single new column that we will call "features"
  - Run model
  
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
The `Spark ML` functions available in PySpark were largely developed
with predictive analysis in mind. This means that they have been built
primarily to specify a regression model and retrieve the prediction (as we have done above), with little information in between.

When conducting analysis at ONS, we are often not interested in
predicting unknown outcomes, but instead understanding the relationship
between the independent variables and the probability of the outcome.
This is what is referred to as *inferential analysis* in this section.

The regression coefficients from the model above can be accessed by applying the `summary` method:

````{tabs}
```{code-tab} py
model.summary
```
````

````{tabs}
``` plaintext Python output
Coefficients:
             Feature Estimate   Std Error T Value P Value
         (Intercept) -20.7632  32614.1522 -0.0006  0.9995
        engine_count  -0.6127      0.2744 -2.2328  0.0256
           job_hours  -0.0254      0.0609 -0.4160  0.6774
         hourly_cost  -0.0007      0.0010 -0.7110  0.4771
callVec_Person (m...  21.5650  32614.1521  0.0007  0.9995
callVec_Person (l...  21.6985  32614.1521  0.0007  0.9995
      callVec_Police  20.6004  32614.1521  0.0006  0.9995
   callVec_Other Frs  20.8927  32614.1521  0.0006  0.9995
callVec_Person (r...  22.2337  32614.1522  0.0007  0.9995
   callVec_Ambulance  21.4983  32614.1522  0.0007  0.9995
   callVec_Not Known  -3.4161 357614.2975  0.0000  1.0000
propertyVec_Dwelling  -0.7497      1.4258 -0.5258  0.5990
 propertyVec_Outdoor  -1.4950      1.4245 -1.0495  0.2939
propertyVec_Non R...  -1.6538      1.4277 -1.1584  0.2467
propertyVec_Outdo...  -2.1807      1.4295 -1.5255  0.1271
propertyVec_Road ...  -0.5467      1.4325 -0.3817  0.7027
propertyVec_Other...  -1.0217      1.4957 -0.6831  0.4945
serviceVec_Other ...   0.9945      0.1600  6.2153  0.0000
serviceVec_Animal...   1.4172      0.1602  8.8450  0.0000
serviceVec_Animal...   1.4412      0.1765  8.1644  0.0000

(Dispersion parameter for binomial family taken to be 1.0000)
    Null deviance: 8123.3546 on 5840 degrees of freedom
Residual deviance: 7534.9047 on 5840 degrees of freedom
AIC: 7574.9047
```
````

Unfortunately, this format is not particular user friendly. Additionally, we have not yet found or built functionality in PySpark to calculate exact confidence intervals. Instead, we can approximate the 95% confidence intervals by multiplying the standard errors by 1.96 and
subtract or add this to the estimate. As PySpark regression functions
are only necessary on datasets with a large number of observations, this
approximation is sufficient. To do this we will also need to convert the summary above to a more useful format, such as a Pandas dataframe:

````{tabs}
```{code-tab} py
# Get model output
model_output = model.transform(rescue_cat_final)

# Get feature names from the model output metadata
# Numeric and binary (categorical) metadata are accessed separately
numeric_metadata = model_output.select("features").schema[0].metadata.get('ml_attr').get('attrs').get('numeric')
binary_metadata = model_output.select("features").schema[0].metadata.get('ml_attr').get('attrs').get('binary')

# Merge the numeric and binary metadata lists to get all the feature names
merge_list = numeric_metadata + binary_metadata

# Convert the feature name list to a Pandas dataframe
full_summary = pd.DataFrame(merge_list)

# Get the regression coefficients from the model
full_summary['coefficients'] = model.coefficients

# The intercept coefficient needs to be added in separately since it is not part of the features metadata
# Define a new row for the intercept coefficient and get value from model
intercept = pd.DataFrame({'name':'intercept', 'coefficients':model.intercept}, index = [0])

# Add new row to the top of the full_summary dataframe
full_summary = pd.concat([intercept,full_summary.loc[:]]).reset_index(drop=True)

# Add standard errors, t-values and p-values from summary into the full_summary dataframe:
full_summary['std_error'] = summary.coefficientStandardErrors
full_summary['tvalues'] = summary.tValues
full_summary['pvalues'] = summary.pValues

# Manually calculate upper and lower confidence bounds and add into dataframe
full_summary['upper_ci'] = full_summary['coefficients'] + (1.96*full_summary['std_error'])
full_summary['lower_ci'] = full_summary['coefficients'] - (1.96*full_summary['std_error'])

# View final model summary
full_summary
```
````
````{tabs}
``` plaintext Python output
+--------------------+--------------------+--------------------+--------------------+--------------------+
|        coefficients|                name|           std_error|            upper_ci|            lower_ci|
+--------------------+--------------------+--------------------+--------------------+--------------------+
|  -20.76315794220359|           intercept| 0.27439316549994924| -20.225347337823692|  -21.30096854658349|
| -0.6126597088756062|        engine_count| 0.06094715323603125| -0.4932032885329849| -0.7321161292182274|
|-0.02535446471650...|           job_hours|9.849412286060397E-4|-0.02342397990843...|-0.02728494952457...|
|-7.00320718006152...|         hourly_cost|  32614.152138273617|   63923.73749069557|  -63923.73889133701|
|  21.564985360322606|callVec_Person (m...|  32614.152138267178|  63945.303176363996|  -63902.17320564335|
|  21.698455740278735|callVec_Person (l...|  32614.152138983158|   63945.43664814726|  -63902.03973666671|
|  20.600364205759604|      callVec_Police|  32614.152140241928|   63944.33855907994|  -63903.13783066841|
|  20.892709637911356|   callVec_Other Frs|  32614.152175598574|  63944.630973811116|  -63902.84555453529|
|  22.233733221849175|callVec_Person (r...|  32614.152169036308|   63945.97198453301|  -63901.50451808931|
|  21.498270939707652|   callVec_Ambulance|  357614.29750958865|   700945.5213897334|   -700902.524847854|
| -3.4160743924617485|   callVec_Not Known|  1.4258405211911767| -0.6214269709270424|  -6.210721813996455|
| -0.7497311089148078|propertyVec_Dwelling|  1.4244757580406757|   2.042241376844917|  -3.541703594674532|
| -1.4950200776261042| propertyVec_Outdoor|   1.427681376841273|   1.303235420982791|     -4.293275576235|
| -1.6537847767513523|propertyVec_Non R...|   1.429491891458024|  1.1480193305063746|  -4.455588884009079|
|  -2.180679137292761|propertyVec_Outdo...|  1.4324909069707645|  0.6270030403699374|   -4.98836131495546|
| -0.5467487331523708|propertyVec_Road ...|  1.4957287012548774|  2.3848795213071887| -3.4783769876119304|
|  -1.021736081307061|propertyVec_Other...| 0.16000819858088014|  -0.708120012088536|  -1.335352150525586|
|  0.9945046772301863|serviceVec_Other ...| 0.16022944175306697|  1.3085543830661976|   0.680454971394175|
|   1.417228201233249|serviceVec_Animal...| 0.17652526948223632|  1.7632177294184324|  1.0712386730480659|
|  1.4412252563902246|serviceVec_Animal...|   32614.15217162583|   63925.17948164301|  -63922.29703113023|
+--------------------+--------------------+--------------------+--------------------+--------------------+

```
````


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
**Add in this section to explain how to use pipelines for logistic regression**
Note that these tasks haven't actually been carried out yet, we have simply defined what they are so we can call them all in succession later. This can be done by setting up a "pipeline" to call all the tasks we need to carry out the regression one by one:

````{tabs}
```{code-tab} py
# Creating the pipeline
pipe = Pipeline(stages=[serviceIdx, callIdx, propertyIdx,
                        serviceEncode, callEncode, propertyEncode,
                        assembler, log_reg])
```
````


