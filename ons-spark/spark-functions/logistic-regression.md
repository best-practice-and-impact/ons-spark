# Logistic Regression

Logistic regression is often used in both predictive and inferential analysis. A logistic
regression model is trained on a dataset to learn how certain
independent variables relate to the probability of a binary outcome
occurring. The model is then applied to cases where the independent
variables are known but the outcome is unknown, and the probability of
the outcome occurring can be estimated.

This article shows how to use the `Spark ML` functions to generate a
logistic regression model in PySpark and sparklyr, including a
discussion of the steps necessary to prepare the data and potential
issues to consider. We will also show how to access standard errors and
confidence intervals for inferential analysis and how to assemble these steps into a pipeline.

## Preparing the data

We will be using the `Spark ML` functions `GeneralizedLinearRegression`/`ml_generalised_linear_regression`. The syntax used to call these
functions is somewhat similar to logistic regression functions in python or R.
However, there may be some additional steps to take in preparing the data
before the model can be run successfully.

We can read the data in as follows:

````{tabs}
```{code-tab} py 
#import packages

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import pandas as pd

#local session 
spark = (SparkSession.builder.master("local[2]").appName("logistic-regression").getOrCreate())

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

# set up spark session
sc <- sparklyr::spark_connect(
  master = "local[2]",
  app_name = "logistic-regression",
  config = sparklyr::spark_config())

# check connection is open
sparklyr::spark_connection_is_open(sc)

# read in data
rescue_path_parquet = "/training/rescue_clean.parquet"
rescue <- sparklyr::spark_read_parquet(sc, rescue_path_parquet)

# preview data
dplyr::glimpse(rescue)
```
````
Let’s say we want to use the `rescue` data to build a logistic regression model that predicts whether an animal is a cat or not. We need to create a new column for our target variable, `is_cat` which takes the value "1" if an animal is a cat, or "0" if it is not a cat. 

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
rescue_cat.printSchema()
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
Examining the dataset with `printSchema`/`glimpse()`, we can see that some of the columns we have selected (such as `engine_count`) are of the type “character” when they should be numeric. We can convert all of these to numeric values by running the following:

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
rescue_cat.printSchema()

```
```{code-tab} r R 
# Convert engine_count, job_hours, hourly_cost, total_cost and incident_duration columns to numeric
rescue_cat <- rescue_cat %>% 
dplyr::mutate(across(c(engine_count:total_cost, incident_duration), 
                    ~as.numeric(.)))

# Check data types are now correct
dplyr::glimpse(rescue_cat)
```
````
### Missing values

You may already be familiar with functions such as `LogisticRegression` or `glm` for performing
logistic regression in python and R. Some of these commonly used functions are quite
user-friendly and will automatically account for issues such as missing
values in the data. However, the `Spark ML` functions in
PySpark and sparklyr require the user to correct for these issues before running
the regression functions.


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
    .show(vertical = True)
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

## Running a logistic regression model

When running a logistic regression model in sparklyr using `ml_generalised_linear_regression`, it is sufficient to prepare the data and deal with missing values as shown above before running the model. The function will automatically encode categorical variables without user input.

However, using the `Spark ML` functions for logistic regression in PySpark requires the user to encode any categorical variables in the dataset so they can be represented numerically before running the regression model. Typically this is done using a method called **one-hot encoding**. One category is selected as the reference category and new columns are created in the data for each of the other
categories, assigning a 1 or 0 for each row depending on whether that
entry belongs to that category or not. 

This is done implicitly by the `ml_generalised_linear_regression` function in sparklyr, but in PySpark we will need to make use of the `StringIndexer` and `OneHotEncoderEstimator` feature transformers to prepare the data first. Additionally, running the regression model in PySpark requires us to assemble all of the predictor variables into a single `features` column using the `VectorAssembler` feature transformer. 

Note that in Spark 3.X, the `OneHotEncoderEstimator` feature transformer in PySpark has been renamed `OneHotEncoder`, so the PySpark code below will need to be amended accordingly.


<details>
<summary><b>Python Example</b></summary>


First, we will need to encode the categorical variables. This can be done in two stages. The first uses the `StringIndexer` feature transformer to convert each categorical column into numeric data. It assigns a numerical index to each unique category. By default, it will choose the most frequent category in the data to label first (starting from 0). The next most frequent category will be assigned 1, then 2, and so on. 

Note that the `StringIndexer` does not support multiple input columns so we will have to apply it to each category individually.
  
````{tabs}
```{code-tab} py
# Importing the required libraries - replace OneHotEncoderEstimator with OneHotEncoder if using Spark >= 3.0
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoderEstimator

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
                          'propertyIndex').show(10, truncate = False)
```

```{code-tab} plaintext Python output
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
Next we use `OneHotEncoderEstimator` to perform one-hot encoding. This converts each of the categories we have applied the `StringIndexer` to into a vector that indicates which category the record belongs to. 


````{tabs}
```{code-tab} py
# Apply OneHotEncoderEstimator to each categorical column simultaneously
# Replace OneHotEncoderEstimator with OneHotEncoder if using Spark >= 3.0
encoder = OneHotEncoderEstimator(inputCols = ['serviceIndex', 'callIndex', 'propertyIndex'], 
                                 outputCols = ['serviceVec', 'callVec', 'propertyVec'])

rescue_cat_ohe = encoder.fit(rescue_cat_indexed).transform(rescue_cat_indexed)

# Check that this has worked correctly 
rescue_cat_ohe.select('is_cat', 'specialservicetypecategory', 'originofcall',
                          'propertycategory', 'serviceVec', 'callVec', 
                          'propertyVec').show(10, truncate = False)

```

```{code-tab} plaintext Python output
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

The format of the vectors generated by the one-hot encoders can be difficult to understand at first glance as it is not as intuitive as simply creating new columns for categories and assigning them a 1 or 0 to indicate which category they belong to. The one-hot encoder feature transformer essentially maps the column indices we generated with the string indexer to column of binary vectors. Each vector has three elements. The first gives the number of categories in the encoded variable, minus one (the reference category). The second element gives the index of the category the entry belongs to, as generated previously with the string indexer. This is empty ("[]") if the entry belongs to the reference category. The final element holds the binary value that indicates whether the entry belongs to the index column specified by the first two elements of the vector. As a result, this third element is always "1.0" unless the entry belongs to the reference category, in which case it is empty ("[]").

As an example, let's take a closer look at the first and fourth entries for `serviceVec` in the output table shown above. The first number in the vector for both entries is "3" since this is equal to the number of categories in the `specialservicetypecategory` column minus the reference category. In this case, the reference category is `Animal Rescue From Water` and the other categories have been assigned an index of 0 (`Other Animal Assistance`), 1 (`Animal Rescue From Height`), or 2 (`Animal Rescue From Below Ground`) by the string indexer. Therefore, the top row has a `serviceVec` of (3,[0], [1.0]). This means it belongs to the 0th index of the `specialservicetypecategory` column, `Other Animal Assistance`. The fourth row on the other hand is assigned (3,[],[]), where the final two elements of the vector are empty since it belongs to the reference category rather than any of our indexed categories. Further information can be found in the [`OneHotEncoder documentation`](https://spark.apache.org/docs/2.3.0/api/python/pyspark.ml.html#pyspark.ml.feature.OneHotEncoder).


Then, we need to vectorise all our predictors into a new column called "features" which will be our input/features class. We must also rename our target variable column, "is_cat" to "label":

````{tabs}
```{code-tab} py

# Call 'VectorAssembler' to vectorise all predictor columns in dataset
assembler = VectorAssembler(inputCols=['engine_count', 'job_hours', 'hourly_cost',
                                       'callVec', 'propertyVec', 'serviceVec'],
                            outputCol = "features")
 
# Apply vectorisation                                      
rescue_cat_vectorised = assembler.transform(rescue_cat_ohe)

# Rename "is_cat" target variable column to "label" ready to pass to the regression model
rescue_cat_final = rescue_cat_vectorised.withColumnRenamed("is_cat", "label").select("label", "features")

```
````

We will use the `GeneralizedLinearRegression` `pyspark.ml` function to carry out our regression. The model can be imported and set up like so: 

````{tabs}
```{code-tab} py
# Import GeneralizedLinearRegression
from pyspark.ml.regression import GeneralizedLinearRegression

# Define model - specify family and link as shown for logistic regression
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

```{code-tab} plaintext Python output
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

</details>

<details>
<summary><b>R Example</b></summary>
We can run a logistic regression model in `sparklyr` by using the `ml_logistic_regression` function:
  

````{tabs}
```{code-tab} r R 
  
# Run the model
glm_out <- sparklyr::ml_logistic_regression(rescue_cat, 
                                            formula = "is_cat ~ engine_count + job_hours + hourly_cost + originofcall + propertycategory + specialservicetypecategory")
```
````

Where we have specified the variables to be used in the regression using
the formula syntax
`target variable ~ predictor1 + predictor2 + predictor3 +...`.

Model predictions can be accessed using `ml_predict`:

````{tabs}
```{code-tab} r R 
# Get model predictions
sparklyr::ml_predict(glm_out) %>% 
  print(n = 20, width = Inf)
```

````
</details>



</details>

## Inferential Analysis 
The `Spark ML` functions available in PySpark and sparklyr were largely developed
with predictive analysis in mind. This means that they have been built
primarily to specify a regression model and retrieve the prediction (as we have done above), with little information in between.

When conducting analysis at ONS, we are often not interested in
predicting unknown outcomes, but instead understanding the relationship
between the independent variables and the probability of the outcome.
This is what is referred to as *inferential analysis* in this section.

<details>
<summary><b>Python Example</b></summary>
The regression coefficients from the model above can be accessed by applying the `summary` method:

````{tabs}
```{code-tab} py
# Get model summary
summary = model.summary

# Show summary
summary
```

```{code-tab} plaintext Python output
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

This provides the regression coefficents, standard errors, t-values and p-values for each feature variable. Unfortunately, we have not yet found or built functionality in PySpark to calculate exact confidence intervals. Instead, we can approximate the 95% confidence intervals by multiplying the standard errors by 1.96 and
subtract or add this to the estimate. As PySpark regression functions
are only necessary on datasets with a large number of observations, this
approximation is sufficient. To make it clear which confidence intervals correspond to which features, the example below also shows how to reconstruct the summary above into a Pandas dataframe that also contains the confidence intervals:

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

```{code-tab} plaintext Python output
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
</details>

<details>
<summary><b>R Example</b></summary>
The regression coefficients from the model above can be displayed by
using the `tidy` function from the `broom` package:

````{tabs}
```{code-tab} r R 
# Run the model
glm_out <- sparklyr::ml_logistic_regression(rescue_cat, 
                                            formula = "is_cat ~ engine_count + job_hours + hourly_cost + originofcall + propertycategory + specialservicetypecategory")

# View coefficients
broom::tidy(glm_out)
```
```{code-tab} plaintext R output
# A tibble: 20 × 2
   features                                                   coefficients
   <chr>                                                             <dbl>
 1 (Intercept)                                                   -0.124   
 2 engine_count                                                  -0.619   
 3 job_hours                                                     -0.0250  
 4 hourly_cost                                                   -0.000731
 5 originofcall_Person (mobile)                                   1.25    
 6 originofcall_Person (land Line)                                1.38    
 7 originofcall_Police                                            0.280   
 8 originofcall_Other Frs                                         0.573   
 9 originofcall_Person (running Call)                             1.91    
10 originofcall_Ambulance                                         1.18    
11 originofcall_Not Known                                       -11.3     
12 propertycategory_Dwelling                                     -1.05    
13 propertycategory_Outdoor                                      -1.80    
14 propertycategory_Non Residential                              -1.96    
15 propertycategory_Outdoor Structure                            -2.49    
16 propertycategory_Road Vehicle                                 -0.851   
17 propertycategory_Other Residential                            -1.33    
18 specialservicetypecategory_Other Animal Assistance             0.995   
19 specialservicetypecategory_Animal Rescue From Height           1.42    
20 specialservicetypecategory_Animal Rescue From Below Ground     1.44    
```
````

This does not provide a lot of information. The `ml_logistic_regression`
function only produces regression coefficients and does not produce
standard errors. This means that confidence intervals cannot be
calculated.

Instead, we must use the function `ml_generalised_linear_regression`.
Here, we need to specify the `family` and `link` arguments as “binomial”
and “logit” respectively in order to run a logistic regression model
that produces standard errors.

````{tabs}
```{code-tab} r R 
# Run model with ml_generalized_linear_regression
glm_out <- sparklyr::ml_generalized_linear_regression(rescue_cat, 
                                            formula = "is_cat ~ engine_count + job_hours + hourly_cost + originofcall + propertycategory + specialservicetypecategory", 
                                            family = "binomial", 
                                            link = "logit")

# View a tibble of coefficients, std. error, p-values
broom::tidy(glm_out)
```
```{code-tab} plaintext R output
# A tibble: 20 × 5
   term                                    estimate std.error statistic  p.value
   <chr>                                      <dbl>     <dbl>     <dbl>    <dbl>
 1 (Intercept)                             -2.08e+1   3.26e+4  -6.37e-4 9.99e- 1
 2 engine_count                            -6.13e-1   2.74e-1  -2.23e+0 2.56e- 2
 3 job_hours                               -2.54e-2   6.09e-2  -4.16e-1 6.77e- 1
 4 hourly_cost                             -7.00e-4   9.85e-4  -7.11e-1 4.77e- 1
 5 originofcall_Person (mobile)             2.16e+1   3.26e+4   6.61e-4 9.99e- 1
 6 originofcall_Person (land Line)          2.17e+1   3.26e+4   6.65e-4 9.99e- 1
 7 originofcall_Police                      2.06e+1   3.26e+4   6.32e-4 9.99e- 1
 8 originofcall_Other Frs                   2.09e+1   3.26e+4   6.41e-4 9.99e- 1
 9 originofcall_Person (running Call)       2.22e+1   3.26e+4   6.82e-4 9.99e- 1
10 originofcall_Ambulance                   2.15e+1   3.26e+4   6.59e-4 9.99e- 1
11 originofcall_Not Known                  -3.42e+0   3.58e+5  -9.55e-6 1.00e+ 0
12 propertycategory_Dwelling               -7.50e-1   1.43e+0  -5.26e-1 5.99e- 1
13 propertycategory_Outdoor                -1.50e+0   1.42e+0  -1.05e+0 2.94e- 1
14 propertycategory_Non Residential        -1.65e+0   1.43e+0  -1.16e+0 2.47e- 1
15 propertycategory_Outdoor Structure      -2.18e+0   1.43e+0  -1.53e+0 1.27e- 1
16 propertycategory_Road Vehicle           -5.47e-1   1.43e+0  -3.82e-1 7.03e- 1
17 propertycategory_Other Residential      -1.02e+0   1.50e+0  -6.83e-1 4.95e- 1
18 specialservicetypecategory_Other Anima…  9.95e-1   1.60e-1   6.22e+0 5.12e-10
19 specialservicetypecategory_Animal Resc…  1.42e+0   1.60e-1   8.84e+0 0       
20 specialservicetypecategory_Animal Resc…  1.44e+0   1.77e-1   8.16e+0 4.44e-16
```
````

Individual statistics of interest can also be accessed directly using
`ml_summary`. For example, to get the standard errors:

````{tabs}
```{code-tab} r R 
glm_out$summary$coefficient_standard_errors()
```
```{code-tab} plaintext R output
 [1] 3.261415e+04 2.743932e-01 6.094715e-02 9.849412e-04 3.261415e+04
 [6] 3.261415e+04 3.261415e+04 3.261415e+04 3.261415e+04 3.261415e+04
[11] 3.576143e+05 1.425841e+00 1.424476e+00 1.427681e+00 1.429492e+00
[16] 1.432491e+00 1.495729e+00 1.600082e-01 1.602294e-01 1.765253e-01
```
````

Other statistics can also be accessed individually in this way by
replacing “coefficient\_standard\_errors” with the statistic of interest
from the list below:

````{tabs}
```{code-tab} plaintext ml_summary options
GeneralizedLinearRegressionTrainingSummary 
Access the following via `$` or `ml_summary()`. 
- aic() 
- degrees_of_freedom() 
- deviance() 
- dispersion() 
- null_deviance() 
- num_instances() 
- prediction_col 
- predictions 
- rank 
- residual_degree_of_freedom() 
- residual_degree_of_freedom_null() 
- residuals() 
- coefficient_standard_errors() 
- num_iterations 
- solver 
- p_values() 
- t_values()
```
````

Unfortunately, we have not yet found or built functionality in sparkylr
to calculate exact confidence intervals. Instead, we can approximate the
95% confidence intervals by multiplying the standard errors by 1.96 and
subtract or add this to the estimate. As sparklyr regression functions
are only necessary on datasets with a large number of observations, this
approximation is sufficient.

````{tabs}
```{code-tab} r R 
broom::tidy(glm_out) %>%
  dplyr::mutate(lower_ci = estimate - (1.96 * std.error),
                upper_ci = estimate + (1.96 * std.error))
```
```{code-tab} plaintext R output
# A tibble: 20 × 7
   term                  estimate std.error statistic  p.value lower_ci upper_ci
   <chr>                    <dbl>     <dbl>     <dbl>    <dbl>    <dbl>    <dbl>
 1 (Intercept)           -2.08e+1   3.26e+4  -6.37e-4 9.99e- 1 -6.39e+4  6.39e+4
 2 engine_count          -6.13e-1   2.74e-1  -2.23e+0 2.56e- 2 -1.15e+0 -7.48e-2
 3 job_hours             -2.54e-2   6.09e-2  -4.16e-1 6.77e- 1 -1.45e-1  9.41e-2
 4 hourly_cost           -7.00e-4   9.85e-4  -7.11e-1 4.77e- 1 -2.63e-3  1.23e-3
 5 originofcall_Person …  2.16e+1   3.26e+4   6.61e-4 9.99e- 1 -6.39e+4  6.39e+4
 6 originofcall_Person …  2.17e+1   3.26e+4   6.65e-4 9.99e- 1 -6.39e+4  6.39e+4
 7 originofcall_Police    2.06e+1   3.26e+4   6.32e-4 9.99e- 1 -6.39e+4  6.39e+4
 8 originofcall_Other F…  2.09e+1   3.26e+4   6.41e-4 9.99e- 1 -6.39e+4  6.39e+4
 9 originofcall_Person …  2.22e+1   3.26e+4   6.82e-4 9.99e- 1 -6.39e+4  6.39e+4
10 originofcall_Ambulan…  2.15e+1   3.26e+4   6.59e-4 9.99e- 1 -6.39e+4  6.39e+4
11 originofcall_Not Kno… -3.42e+0   3.58e+5  -9.55e-6 1.00e+ 0 -7.01e+5  7.01e+5
12 propertycategory_Dwe… -7.50e-1   1.43e+0  -5.26e-1 5.99e- 1 -3.54e+0  2.04e+0
13 propertycategory_Out… -1.50e+0   1.42e+0  -1.05e+0 2.94e- 1 -4.29e+0  1.30e+0
14 propertycategory_Non… -1.65e+0   1.43e+0  -1.16e+0 2.47e- 1 -4.45e+0  1.14e+0
15 propertycategory_Out… -2.18e+0   1.43e+0  -1.53e+0 1.27e- 1 -4.98e+0  6.21e-1
16 propertycategory_Roa… -5.47e-1   1.43e+0  -3.82e-1 7.03e- 1 -3.35e+0  2.26e+0
17 propertycategory_Oth… -1.02e+0   1.50e+0  -6.83e-1 4.95e- 1 -3.95e+0  1.91e+0
18 specialservicetypeca…  9.95e-1   1.60e-1   6.22e+0 5.12e-10  6.81e-1  1.31e+0
19 specialservicetypeca…  1.42e+0   1.60e-1   8.84e+0 0         1.10e+0  1.73e+0
20 specialservicetypeca…  1.44e+0   1.77e-1   8.16e+0 4.44e-16  1.10e+0  1.79e+0

```

````

</details>


## Things to watch out for

### Singularity issue
Some functions for carrying out logistic regression, such as `glm` when using R, are able to detect when a predictor in the model only takes a singular value and drop these columns from the analysis. However, the equivalent functions in PySpark and sparklyr do not have this functionality, so we need to be careful to check that variables included in the model take
more than one value in order to avoid errors.

<details>
<summary><b>Python Example</b></summary>

````{tabs}
```{code-tab} py 
# Add the `typeofincident` categorical column into analysis 
# Setup column indexing
incidentIdx = StringIndexer(inputCol='typeofincident',
                               outputCol='incidentIndex')

# Call the string indexer 
rescue_cat_singular_indexed = incidentIdx.fit(rescue_cat_indexed).transform(rescue_cat_indexed)

# Setup one-hot encoding
encoder_singular = OneHotEncoderEstimator(inputCols = ['incidentIndex'], 
                                 outputCols = ['incidentVec'])
                      
# The following returns an error "The input column incidentIndex should have at least two distinct values":
rescue_cat_singular_ohe = encoder_singular.fit(rescue_cat_singular_indexed).transform(rescue_cat_singular_indexed)

```


````

Running the code above will produce an error since the `typeofincident`
column only contains one value, `Special Service`. The regression model
will not run unless `typeofincident` is removed from the formula.

Similarly, a column with a singular numeric value will also fail. Although instead of returning an error at the encoding stage, the model will run but be unable to return a summary:
````{tabs}
```{code-tab} py
# Convert typeofincident column into numeric value
rescue_cat_singular = rescue_cat_ohe.withColumn('typeofincident', F.when(F.col('typeofincident')=="Special Service", 1)
                               .otherwise(0))

# Setup the vectorassembler to include this variable in the features column
assembler = VectorAssembler(inputCols=['typeofincident', 'engine_count', 'job_hours', 'hourly_cost', 
                                       'callVec', 'propertyVec', 'serviceVec'], 
                           outputCol = "features")

rescue_cat_vectorised_sing = assembler.transform(rescue_cat_singular)


rescue_cat_final_sing = rescue_cat_vectorised_sing.withColumnRenamed("is_cat", "label").select("label", "features")

# Run the model
model_sing = glr.fit(rescue_cat_final_sing)

# Return model summary (will give an error)
summary_sing = model_sing.summary

summary_sing

```
```{code-tab} plaintext Python output
Py4JJavaError: An error occurred while calling o1777.toString.
: java.lang.UnsupportedOperationException: No summary available for this GeneralizedLinearRegressionModel
	at org.apache.spark.ml.regression.GeneralizedLinearRegressionTrainingSummary.toString(GeneralizedLinearRegression.scala:1571)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
	at py4j.Gateway.invoke(Gateway.java:282)
	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
	at py4j.commands.CallCommand.execute(CallCommand.java:79)
	at py4j.GatewayConnection.run(GatewayConnection.java:238)
	at java.lang.Thread.run(Thread.java:745)
```
````
</details>

<details>
<summary><b>R Example</b></summary>


````{tabs}
```{code-tab} r R 
glm_singular <- sparklyr::ml_generalized_linear_regression(rescue_cat, 
                                                 formula = "is_cat ~ typeofincident + engine_count + job_hours + hourly_cost + originofcall + propertycategory + specialservicetypecategory", 
                                                 family = "binomial", 
                                                 link = "logit")
```
````


Running the code above will produce an error since the `typeofincident`
column only contains one value, `Special Service`. The regression model
will not run unless `typeofincident` is removed from the formula.


</details>

### Selecting reference categories

When including categorical variables as independent variables in a
regression, one of the categories must act as the reference category.
The coefficients for all other categories are relative to the reference
category.

By default, the `OneHotEncoderEstimator`/`ml_generalized_linear_regression` functions will select the least common category as the reference category before applying one-hot encoding to the categories so that they can be
represented as a binary numerical value. For example, `specialservicetypecategory` has four unique values: Animal
Rescue From Below Ground, Animal Rescue From Height, Animal Rescue From
Water, and Other Animal Assistance.

````{tabs}
```{code-tab} py
rescue_cat.groupBy("specialservicetypecategory").count().orderBy("count").show(truncate = False)
```
```{code-tab} r R 
rescue_cat %>% 
  dplyr::count(specialservicetypecategory) %>% 
  dplyr::arrange(n)
```
````

````{tabs}
```{code-tab} plaintext Python output
+-------------------------------+-----+
|specialservicetypecategory     |count|
+-------------------------------+-----+
|Animal Rescue From Water       |343  |
|Animal Rescue From Below Ground|593  |
|Animal Rescue From Height      |2123 |
|Other Animal Assistance        |2801 |
+-------------------------------+-----+
```
```{code-tab} plaintext R output
# Source:     spark<?> [?? x 2]
# Ordered by: n
  specialservicetypecategory          n
  <chr>                           <dbl>
1 Animal Rescue From Water          343
2 Animal Rescue From Below Ground   593
3 Animal Rescue From Height        2123
4 Other Animal Assistance          2801
```
````
Since there are only 343 instances of “Animal Rescue From Water” in the data, this has been automatically selected as the reference category in the example above. Regression coefficients in the previous section are therefore shown relative
to the to the “Animal Rescue From Water” reference category.

Selecting a reference category can be particularly useful for
inferential analysis. For example, in the `rescue_cat` dataset, we might want to
select “Other Animal Assistance” as our reference category instead
because it is the largest special service type and could serve as a
useful reference point.

<details>
<summary><b>Python Example</b></summary>

To do this, we can make use of the additional `stringOrderType` argument for the `StringIndexer` function we used previously to index our categorical variables. This allows us to specify the indexing order. By default, this argument is set to `frequencyDesc`, so "Animal Rescue From Water" would be ordered last and is subsequently dropped by the `OneHotEncoderEstimator` function and set as the reference category. If we want "Other Animal Assistance" to be dropped instead, we need to ensure it is placed last in the indexing order. In this case, specifying `stringOrderType = frequencyAsc` would be sufficient to achieve this. However, to keep this example as general as possible, a convenient way of ensuring any chosen reference category is ordered last is to add an appropriate prefix to it, such as "000_", and order the categories in descending alphabetical order (`alphabetDesc`):

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

# Use stringOrderType argument of StringIndexer

# Re-indexing the specialservicetypecategory column
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
# Encode re-indexed columns
encoder = OneHotEncoderEstimator(inputCols = ['serviceIndex', 'callIndex', 'propertyIndex'], 
                                 outputCols = ['serviceVec', 'callVec', 'propertyVec'])

rescue_cat_ohe = encoder.fit(rescue_cat_indexed).transform(rescue_cat_indexed)


# Vectorize all our predictors into a new column called "features" 

assembler = VectorAssembler(inputCols=['engine_count', 'job_hours', 'hourly_cost', 
                                       'callVec', 'propertyVec', 'serviceVec'], 
                           outputCol = "features")

rescue_cat_vectorised = assembler.transform(rescue_cat_ohe)

# Rename target variable "is_cat" to "label" ready to run regression model
rescue_cat_final = rescue_cat_vectorised.withColumnRenamed("is_cat", "label").select("label", "features")

# Run the model again
model = glr.fit(rescue_cat_final)

# Show summary
model.summary

```

```{code-tab} plaintext Python output
Coefficients:
             Feature Estimate   Std Error  T Value P Value
         (Intercept)   1.0466      0.3855   2.7150  0.0066
        engine_count  -0.6127      0.2744  -2.2328  0.0256
           job_hours  -0.0254      0.0609  -0.4160  0.6774
         hourly_cost  -0.0007      0.0010  -0.7110  0.4771
      callVec_Police  -0.9646      0.2259  -4.2704  0.0000
callVec_Person (r...   0.6687      1.5616   0.4282  0.6685
callVec_Person (l...   0.1335      0.0573   2.3311  0.0197
   callVec_Other Frs  -0.6723      0.3661  -1.8365  0.0663
   callVec_Not Known -24.9811 356123.9993  -0.0001  0.9999
  callVec_Coastguard -26.0473 356123.9993  -0.0001  0.9999
   callVec_Ambulance  -0.0667      1.4188  -0.0470  0.9625
propertyVec_Road ...   0.2030      0.1470   1.3807  0.1674
propertyVec_Outdo...  -1.4309      0.1159 -12.3498  0.0000
 propertyVec_Outdoor  -0.7453      0.0680 -10.9546  0.0000
propertyVec_Other...  -0.2720      0.4562  -0.5962  0.5511
propertyVec_Non R...  -0.9041      0.0922  -9.8075  0.0000
    propertyVec_Boat   0.7497      1.4258   0.5258  0.5990
serviceVec_Animal...  -0.9945      0.1600  -6.2153  0.0000
serviceVec_Animal...   0.4227      0.0614   6.8856  0.0000
serviceVec_Animal...   0.4467      0.0959   4.6581  0.0000

(Dispersion parameter for binomial family taken to be 1.0000)
    Null deviance: 8123.3546 on 5840 degrees of freedom
Residual deviance: 7534.9047 on 5840 degrees of freedom
AIC: 7574.9047
```
````
</details>
<details>
<summary><b>R Example</b></summary>

To do this, we can make use of the one-hot encoding concept and two of
`sparklyr`’s **Feature Transformers**; `ft_string_indexer` and `ft_one_hot_encoder`. These have limited functionality, so we must first manipulate it such that the reference category will be ordered last when using `ft_string_indexer`, to ensure that the category ordered last is dropped when applying `ft_one_hot_encoder`. A convenient way of doing this is to order the categories in *descending alphabetical* order and ensuring that our chosen category will be ordered last by adding an appropriate prefix to it. For example, adding
`000_`:

````{tabs}
```{code-tab} r R 
rescue_cat_ohe <- rescue_cat %>%
  dplyr::mutate(specialservicetypecategory = ifelse(specialservicetypecategory == "Other Animal Assistance",
                                                    "000_Other Animal Assistance",
                                                    specialservicetypecategory)) %>%
  sparklyr::ft_string_indexer(input_col = "specialservicetypecategory", 
                              output_col = "specialservicetypecategory_idx",
                              string_order_type = "alphabetDesc") %>%
  sparklyr::ft_one_hot_encoder(input_cols = c("specialservicetypecategory_idx"), 
                               output_cols = c("specialservicetypecategory_ohe"), 
                               drop_last = TRUE) %>%
  sparklyr::sdf_separate_column(column = "specialservicetypecategory_ohe",
                                into = c("specialservicetypecategory_Animal Rescue From Water", 
                                         "specialservicetypecategory_Animal Rescue From Below Ground", 
                                         "specialservicetypecategory_Animal Rescue From Height")) %>% 
  sparklyr::select(-ends_with(c("_ohe", "_idx")),
                   -specialservicetypecategory)
```
````

`sdf_separate_column` has been used to separate encoded variables into
individual columns and the intermediate columns with “\_ohe” and “\_idx”
suffixes have been dropped once the process is complete. This can be
repeated for every categorical variable as desired.

````{tabs}
```{code-tab} r R 
# originofcall
rescue_cat_ohe <- rescue_cat_ohe %>%
  dplyr::mutate(originofcall = ifelse(originofcall == "Person (mobile)",
                                      "000_Person (mobile)",
                                      originofcall )) %>%
  sparklyr::ft_string_indexer(input_col = "originofcall", 
                              output_col = "originofcall_idx",
                              string_order_type = "alphabetDesc") %>%
  sparklyr::ft_one_hot_encoder(input_cols = c("originofcall_idx"), 
                               output_cols = c("originofcall_ohe"), 
                               drop_last = TRUE) %>%
  sparklyr::sdf_separate_column(column = "originofcall_ohe",
                                into = c("originofcall_Ambulance", 
                                         "originofcall_Police", 
                                         "originofcall_Coastguard",
                                         "originofcall_Person (land line)",
                                         "originofcall_Not Known",
                                         "originofcall_Person (running Call)",
                                         "originofcall_Other Frs")) 
# propertycategory
rescue_cat_ohe <- rescue_cat_ohe %>%
  dplyr::mutate(propertycategory = ifelse(propertycategory == "Dwelling",
                                          "000_Dwelling",
                                          propertycategory)) %>%
  sparklyr::ft_string_indexer(input_col = "propertycategory", 
                              output_col = "propertycategory_idx",
                              string_order_type = "alphabetDesc") %>%
  sparklyr::ft_one_hot_encoder(input_cols = c("propertycategory_idx"), 
                               output_cols = c("propertycategory_ohe"), 
                               drop_last = TRUE) %>%
  sparklyr::sdf_separate_column(column = "propertycategory_ohe",
                                into = c("propertycategory_Outdoor", 
                                         "propertycategory_Road Vehicle", 
                                         "propertycategory_Non Residential",
                                         "propertycategory_Boat",
                                         "propertycategory_Outdoor Structure",
                                         "propertycategory_Other Residential"))


# remove _idx and _ohe intermediate columns and original data columns
# also remove `typeofincident` to avoid singularity error
rescue_cat_ohe <- rescue_cat_ohe %>% 
  sparklyr::select(-ends_with(c("_ohe", "_idx")),
                   -originofcall,
                   -propertycategory,
                   -typeofincident)
```
````

Once this is done we can run the regression again and get the
coefficients relative to the chosen reference categories:

````{tabs}
```{code-tab} r R 
# Run regression with one-hot encoded variables with chosen reference categories
glm_ohe <- sparklyr::ml_generalized_linear_regression(rescue_cat_ohe, 
                                                      formula = "is_cat ~ .", 
                                                      family = "binomial", 
                                                      link = "logit")

# Get coefficients and confidence intervals
broom::tidy(glm_ohe) %>%
  dplyr::mutate(lower_ci = estimate - (1.96 * std.error),
                upper_ci = estimate + (1.96 * std.error))
```
```{code-tab} plaintext R output
# A tibble: 20 × 7
term                  estimate std.error statistic  p.value lower_ci upper_ci
<chr>                    <dbl>     <dbl>     <dbl>    <dbl>    <dbl>    <dbl>
  1 (Intercept)            1.05e+0   3.85e-1   2.71e+0 6.63e- 3  2.91e-1  1.80e+0
2 engine_count          -6.13e-1   2.74e-1  -2.23e+0 2.56e- 2 -1.15e+0 -7.48e-2
3 job_hours             -2.54e-2   6.09e-2  -4.16e-1 6.77e- 1 -1.45e-1  9.41e-2
4 hourly_cost           -7.00e-4   9.85e-4  -7.11e-1 4.77e- 1 -2.63e-3  1.23e-3
5 specialservicetypeca… -9.95e-1   1.60e-1  -6.22e+0 5.12e-10 -1.31e+0 -6.81e-1
6 specialservicetypeca…  4.23e-1   6.14e-2   6.89e+0 5.76e-12  3.02e-1  5.43e-1
7 specialservicetypeca…  4.47e-1   9.59e-2   4.66e+0 3.19e- 6  2.59e-1  6.35e-1
8 originofcall_Ambulan… -9.65e-1   2.26e-1  -4.27e+0 1.95e- 5 -1.41e+0 -5.22e-1
9 originofcall_Police    6.69e-1   1.56e+0   4.28e-1 6.68e- 1 -2.39e+0  3.73e+0
10 originofcall_Coastgu…  1.33e-1   5.73e-2   2.33e+0 1.97e- 2  2.12e-2  2.46e-1
11 originofcall_Person … -6.72e-1   3.66e-1  -1.84e+0 6.63e- 2 -1.39e+0  4.52e-2
12 originofcall_Not Kno… -2.50e+1   3.56e+5  -7.01e-5 1.00e+ 0 -6.98e+5  6.98e+5
13 originofcall_Person … -2.60e+1   3.56e+5  -7.31e-5 1.00e+ 0 -6.98e+5  6.98e+5
14 originofcall_Other F… -6.67e-2   1.42e+0  -4.70e-2 9.62e- 1 -2.85e+0  2.71e+0
15 propertycategory_Out…  2.03e-1   1.47e-1   1.38e+0 1.67e- 1 -8.52e-2  4.91e-1
16 propertycategory_Roa… -1.43e+0   1.16e-1  -1.23e+1 0        -1.66e+0 -1.20e+0
17 propertycategory_Non… -7.45e-1   6.80e-2  -1.10e+1 0        -8.79e-1 -6.12e-1
18 propertycategory_Boat -2.72e-1   4.56e-1  -5.96e-1 5.51e- 1 -1.17e+0  6.22e-1
19 propertycategory_Out… -9.04e-1   9.22e-2  -9.81e+0 0        -1.08e+0 -7.23e-1
20 propertycategory_Oth…  7.50e-1   1.43e+0   5.26e-1 5.99e- 1 -2.04e+0  3.54e+0
```
````
</details>

### Multicollinearity

A key assumption of linear and logistic regression models is that the feature columns are independent of one another. Including features that correlate with each other in the model is a clear violation of this assumption, so we need to identify these and remove them to get a valid result.

A useful way of identifying these variables is to generate a correlation matrix of all the the features in the model. This can be done using the `corr` function from the ml subpackage `pyspark.ml.stat` in PySpark, or the `ml_corr` function in sparklyr.

In PySpark, note that the correlation function is only applicable to a vector and not to a dataframe. We will therefore have to apply it to the "features" column of the `rescue_cat_final` dataset produced after the indexing, encoding and vector assembly stages of the model setup.

````{tabs}
```{code-tab} py
from pyspark.ml.stat import Correlation

# Select feature column vector
features_vector = rescue_cat_final.select("features")

# Generate correlation matrix
matrix = Correlation.corr(features_vector, "features").collect()[0][0]

# Convert matrix into a useful format
corr_matrix = matrix.toArray().tolist() 

# Get list of features to assign to matrix columns and indices
features = pd.DataFrame(merge_list)['name'].values.tolist()

# Final correlation matrix
corr_matrix_df = pd.DataFrame(data=corr_matrix, columns = features, index = features) 

corr_matrix_df

```

```{code-tab} r R
# Get feature column names
features <- rescue_cat_ohe %>%
  select(-is_cat) %>%
  colnames()

# Generate correlation matrix  
ml_corr(rescue_cat_ohe, columns = features, method = "pearson") %>%
  print()
```
````

Looking closely at the `total_cost` column, we can see that there is very strong correlation between that column and `job_hours`, with a correlation coefficient very close to 1. Additionally, there is quite strong correlation between the `total_cost` column and the `engine_count` column, as well as a moderate correlation with both `hourly_cost` and the "Animal Rescue From Water" category in the `specialservicetypecategory` column. This is to be expected, since if we take a closer look at `total_cost` we can see it always takes a value that is equal to
`hourly_cost` multiplied by `job_hours`.

````{tabs}
```{code-tab} py
rescue_cat.select("job_hours", "hourly_cost", "total_cost").orderBy("job_hours", ascending=False).limit(30).toPandas()
```
```{code-tab} r R
rescue_cat %>%
  sparklyr::select(job_hours,
                   hourly_cost,
                   total_cost) %>%
  dplyr::arrange(desc(job_hours)) %>%
  print(n = 30)
```
````

````{tabs}
```{code-tab} plaintext Python output
	job_hours	hourly_cost	total_cost
0	12.0	326.0	3912.0
1	12.0	290.0	3480.0
2	10.0	298.0	2980.0
3	9.0	260.0	2340.0
4	9.0	260.0	2340.0
5	9.0	260.0	2340.0
6	9.0	295.0	2655.0
7	8.0	260.0	2080.0
8	8.0	333.0	2664.0
9	7.0	328.0	2296.0
10	7.0	260.0	1820.0
11	7.0	290.0	2030.0
12	7.0	260.0	1820.0
13	7.0	290.0	2030.0
14	7.0	295.0	2065.0
15	7.0	326.0	2282.0
16	6.0	298.0	1788.0
17	6.0	260.0	1560.0
18	6.0	333.0	1998.0
19	6.0	260.0	1560.0
20	6.0	298.0	1788.0
21	5.0	260.0	1300.0
22	5.0	260.0	1300.0
23	5.0	260.0	1300.0
24	5.0	260.0	1300.0
25	5.0	260.0	1300.0
26	5.0	255.0	1275.0
27	5.0	260.0	1300.0
28	5.0	260.0	1300.0
29	5.0	260.0	1300.0
```
```{code-tab} plaintext R output
# Source:     spark<?> [?? x 3]
# Ordered by: desc(job_hours)
   job_hours hourly_cost total_cost
       <dbl>       <dbl>      <dbl>
 1        12         326       3912
 2        12         290       3480
 3        10         298       2980
 4         9         260       2340
 5         9         260       2340
 6         9         260       2340
 7         9         295       2655
 8         8         260       2080
 9         8         333       2664
10         7         328       2296
11         7         260       1820
12         7         290       2030
13         7         260       1820
14         7         290       2030
15         7         295       2065
16         7         326       2282
17         6         298       1788
18         6         260       1560
19         6         333       1998
20         6         260       1560
21         6         298       1788
22         5         260       1300
23         5         295       1475
24         5         260       1300
25         5         255       1275
26         5         260       1300
27         5         260       1300
28         5         260       1300
29         5         260       1300
30         5         260       1300
```
````

Therefore, we need to drop this feature from our model since it is not independent of other variables. You may also want to remove other variables from the model based on the correlation matrix. For example, there is also quite high correlation between the `job_hours` column and several other features. Deciding on which features to remove from your model may require some experimentation, but in general, a correlation coefficient of >0.7 among two or more predictors indicates multicollinearity and some of these should be removed from the model to ensure validity.

````{tabs}
```{code-tab} plaintext Python output
Coefficients:
             Feature Estimate   Std Error  T Value P Value
         (Intercept)   1.0885      0.3722   2.9246  0.0034
        engine_count  -0.6804      0.2214  -3.0729  0.0021
         hourly_cost  -0.0007      0.0010  -0.7175  0.4730
      callVec_Police  -0.9668      0.2257  -4.2829  0.0000
callVec_Person (r...   0.6470      1.5530   0.4166  0.6770
callVec_Person (l...   0.1338      0.0572   2.3375  0.0194
   callVec_Other Frs  -0.6789      0.3657  -1.8565  0.0634
   callVec_Not Known -25.0264 356123.9993  -0.0001  0.9999
  callVec_Coastguard -26.0423 356123.9993  -0.0001  0.9999
   callVec_Ambulance  -0.0626      1.4188  -0.0441  0.9648
propertyVec_Road ...   0.2017      0.1470   1.3720  0.1701
propertyVec_Outdo...  -1.4310      0.1159 -12.3504  0.0000
 propertyVec_Outdoor  -0.7482      0.0677 -11.0556  0.0000
propertyVec_Other...  -0.2765      0.4561  -0.6062  0.5444
propertyVec_Non R...  -0.9055      0.0921  -9.8298  0.0000
    propertyVec_Boat   0.7267      1.4248   0.5100  0.6100
serviceVec_Animal...  -0.9946      0.1600  -6.2167  0.0000
serviceVec_Animal...   0.4235      0.0614   6.9004  0.0000
serviceVec_Animal...   0.4458      0.0959   4.6498  0.0000

(Dispersion parameter for binomial family taken to be 1.0000)
    Null deviance: 8123.3546 on 5841 degrees of freedom
Residual deviance: 7535.0784 on 5841 degrees of freedom
```
```{code-tab} plaintext R output
# A tibble: 19 × 7
   term                  estimate std.error statistic  p.value lower_ci upper_ci
   <chr>                    <dbl>     <dbl>     <dbl>    <dbl>    <dbl>    <dbl>
 1 (Intercept)            1.09e+0   3.72e-1   2.92e+0 3.45e- 3  3.59e-1  1.82e+0
 2 engine_count          -6.80e-1   2.21e-1  -3.07e+0 2.12e- 3 -1.11e+0 -2.46e-1
 3 hourly_cost           -7.07e-4   9.85e-4  -7.18e-1 4.73e- 1 -2.64e-3  1.22e-3
 4 specialservicetypeca… -9.95e-1   1.60e-1  -6.22e+0 5.08e-10 -1.31e+0 -6.81e-1
 5 specialservicetypeca…  4.23e-1   6.14e-2   6.90e+0 5.18e-12  3.03e-1  5.44e-1
 6 specialservicetypeca…  4.46e-1   9.59e-2   4.65e+0 3.32e- 6  2.58e-1  6.34e-1
 7 originofcall_Ambulan… -9.67e-1   2.26e-1  -4.28e+0 1.85e- 5 -1.41e+0 -5.24e-1
 8 originofcall_Police    6.47e-1   1.55e+0   4.17e-1 6.77e- 1 -2.40e+0  3.69e+0
 9 originofcall_Coastgu…  1.34e-1   5.72e-2   2.34e+0 1.94e- 2  2.16e-2  2.46e-1
10 originofcall_Person … -6.79e-1   3.66e-1  -1.86e+0 6.34e- 2 -1.40e+0  3.79e-2
11 originofcall_Not Kno… -2.50e+1   3.56e+5  -7.03e-5 1.00e+ 0 -6.98e+5  6.98e+5
12 originofcall_Person … -2.60e+1   3.56e+5  -7.31e-5 1.00e+ 0 -6.98e+5  6.98e+5
13 originofcall_Other F… -6.26e-2   1.42e+0  -4.41e-2 9.65e- 1 -2.84e+0  2.72e+0
14 propertycategory_Out…  2.02e-1   1.47e-1   1.37e+0 1.70e- 1 -8.64e-2  4.90e-1
15 propertycategory_Roa… -1.43e+0   1.16e-1  -1.24e+1 0        -1.66e+0 -1.20e+0
16 propertycategory_Non… -7.48e-1   6.77e-2  -1.11e+1 0        -8.81e-1 -6.16e-1
17 propertycategory_Boat -2.77e-1   4.56e-1  -6.06e-1 5.44e- 1 -1.17e+0  6.17e-1
18 propertycategory_Out… -9.05e-1   9.21e-2  -9.83e+0 0        -1.09e+0 -7.25e-1
19 propertycategory_Oth…  7.27e-1   1.42e+0   5.10e-1 6.10e- 1 -2.07e+0  3.52e+0
```
````

## Spark ML Pipelines

We can also combine all of the analysis steps outlined above into a single workflow using Spark's ML Pipelines. Data manipulation, feature transformation and modelling can be rewritten into a `pipeline`/`ml_pipeline()` object and saved. 

The output of this in sparklyr is in Scala code, meaning that it can be added to scheduled Spark ML jobs without any dependencies in R. However, it should be noted that we have not yet found a simple way of generating a summary coefficient table as shown in the sections above when using a pipeline for analysis. Although coefficients, p-values etc. can be obtained it is difficult to map these to the corresponding features for inferential analysis. As a result, pipelines for logistic regression in sparklyr are currently best used for predictive analysis only.

PySpark pipelines do not have this problem and we can access the coefficients in a similar way to that shown above even when using a pipeline for analysis.

<details>
<summary><b>Python Example</b></summary>

As shown above, we need to prepare the data, index categorical variables, encode them, assemble features into a single vector column and run the regression to perform our analysis. The data preparation step can be done before setting up the pipeline (in this case we will re-use the `rescue_cat_reindex` dataframe from the Selecting reference categories section of this guide as a shortcut), so our pipeline steps will be as follows:

1. String indexer for `specialservicetypecategory`
2. String indexer for `originofcall`
3. String indexer for `propertycategory`
4. One hot encoder for `specialservicetypecategory`, `originofcall`and `propertycategory`
5. Vector assembler to generate a single `features` column that contains a vector of features 
6. Logistic regression model

We will define each of these steps first to assemble the pipeline:

````{tabs}
```{code-tab} py
from pyspark.ml import Pipeline

# Rename "is_cat" to "label" before setting up pipeline stages
rescue_cat_reindex = rescue_cat_reindex.withColumnRenamed("is_cat", "label")

# 1. Indexing the specialservicetypecategory column
serviceIdx = StringIndexer(inputCol='specialservicetypecategory',
                               outputCol='serviceIndex', 
                               stringOrderType = "alphabetDesc")

# 2. Indexing the originofcall column
callIdx = StringIndexer(inputCol='originofcall',
                               outputCol='callIndex',
                               stringOrderType = "alphabetDesc")

# 3. Indexing the propertycategory column
propertyIdx = StringIndexer(inputCol='propertycategory',
                               outputCol='propertyIndex', 
                               stringOrderType = "alphabetDesc")

# 4. One-hot encoding
encoder = OneHotEncoderEstimator(inputCols = ['serviceIndex', 'callIndex', 'propertyIndex'], 
                                 outputCols = ['serviceVec', 'callVec', 'propertyVec'])

# 5. Vector assembler
assembler = VectorAssembler(inputCols=['engine_count', 'hourly_cost', 
                                       'callVec', 'propertyVec', 'serviceVec'], 
                           outputCol = "features")

# 6. Regression model
glr = GeneralizedLinearRegression(family="binomial", link="logit")

# Creating the pipeline
pipe = Pipeline(stages=[serviceIdx, callIdx, propertyIdx,
                        encoder, assembler, glr])
                        
# View the pipeline stages
pipe.getStages()
```
````
The pipeline can be fitted to the `rescue_cat_reindex` dataset by fitting the model (`fit`) and predictions can be accessed using `transform`:

````{tabs}
```{code-tab} py
fit_model = pipe.fit(rescue_cat_reindex)

# Save model results
results = fit_model.transform(rescue_cat_reindex)
  
# Showing the results
results.show()
```
````

To get the summary table with regression coefficients, we will need to access the regression model stage of the pipeline directly using `stages`:

````{tabs}
```{code-tab} py
# Get coefficients summary table
summary = fit_model.stages[-1].summary

summary
```
```{code-tab} plaintext Python output
Coefficients:
             Feature Estimate   Std Error  T Value P Value
         (Intercept)   1.0885      0.3722   2.9246  0.0034
        engine_count  -0.6804      0.2214  -3.0729  0.0021
         hourly_cost  -0.0007      0.0010  -0.7175  0.4730
      callVec_Police  -0.9668      0.2257  -4.2829  0.0000
callVec_Person (r...   0.6470      1.5530   0.4166  0.6770
callVec_Person (l...   0.1338      0.0572   2.3375  0.0194
   callVec_Other Frs  -0.6789      0.3657  -1.8565  0.0634
   callVec_Not Known -25.0264 356123.9993  -0.0001  0.9999
  callVec_Coastguard -26.0423 356123.9993  -0.0001  0.9999
   callVec_Ambulance  -0.0626      1.4188  -0.0441  0.9648
propertyVec_Road ...   0.2017      0.1470   1.3720  0.1701
propertyVec_Outdo...  -1.4310      0.1159 -12.3504  0.0000
 propertyVec_Outdoor  -0.7482      0.0677 -11.0556  0.0000
propertyVec_Other...  -0.2765      0.4561  -0.6062  0.5444
propertyVec_Non R...  -0.9055      0.0921  -9.8298  0.0000
    propertyVec_Boat   0.7267      1.4248   0.5100  0.6100
serviceVec_Animal...  -0.9946      0.1600  -6.2167  0.0000
serviceVec_Animal...   0.4235      0.0614   6.9004  0.0000
serviceVec_Animal...   0.4458      0.0959   4.6498  0.0000

(Dispersion parameter for binomial family taken to be 1.0000)
    Null deviance: 8123.3546 on 5841 degrees of freedom
Residual deviance: 7535.0784 on 5841 degrees of freedom
AIC: 7573.0784
```
````

The confidence intervals can then be calculated and the summary can be converted into a pandas dataframe as shown in the "Inferential Analysis" section if desired.

You can save a pipeline or pipeline model using the `.write().overwrite().save` command:

````{tabs}
```{code-tab} py
# Save pipeline

pipe.write().overwrite().save("rescue_pipeline")

# Save the pipeline model

fit_model.write().overwrite().save("rescue_model")

```
````
They can then be re-loaded using the `load()` command and the re-loaded model can be used to re-fit new data with the same model by supplying the name of the new spark dataframe you wish to fit.

````{tabs}
```{code-tab} py
# Load saved pipeline
reloaded_pipeline = Pipeline.load("rescue_pipeline")

# Re-fit to a subset of rescue data as an example of how pipelines can be re-used
new_model = reloaded_pipeline.fit(rescue_cat_reindex.sample(withReplacement=None,
                      fraction=0.1, seed = 99))
                      
# View new model summary
new_model.stages[-1].summary
```
````

````{tabs}
```{code-tab} py
# Close the spark session
spark.stop()
```
````

</details>


<details>
<summary><b>R Example</b></summary>

As above, we still need to generate the `is_cat` column from the original data, select our predictors and remove missing values. In order to select our reference categories we can also add the "000_" prefix to these categories at this stage. All of this data preparation can later be called in the pipeline in a single step using the `ft_dplyr_transformer` feature transformer. This function converts `dplyr` code into a SQL feature transformer that can then be used in a pipeline. We will set up the transformation as follows:

````{tabs}
```{code-tab} r R
rescue_cat <- rescue %>% 
# generate is_cat column
  dplyr::mutate(is_cat = ifelse(animal_group == "Cat", 1, 0)) %>% 
  sparklyr::select(engine_count, 
                   hourly_cost, 
                   originofcall, 
                   propertycategory,
                   specialservicetypecategory,
                   is_cat) %>%
# ensure numeric columns have the correct type
  dplyr::mutate(across(c(engine_count, hourly_cost), 
                   ~as.numeric(.))) %>%
# remove missing values
  sparklyr::filter(!is.na(engine_count)) %>%
# select reference categories and ensure they will be ordered last by the indexer
  dplyr::mutate(specialservicetypecategory = ifelse(specialservicetypecategory == "Other Animal Assistance",
                         "000_Other Animal Assistance",
                         specialservicetypecategory)) %>%
  dplyr::mutate(originofcall = ifelse(originofcall == "Person (mobile)",
                         "000_Person (mobile)",
                          originofcall )) %>%
  dplyr::mutate(propertycategory = ifelse(propertycategory == "Dwelling",
                         "000_Dwelling",
                         propertycategory))

```
````
The resulting pipeline stage is produced from the `dplyr` code:

````{tabs}
```{code-tab} r R
ft_dplyr_transformer(sc, rescue_cat)
```

```{code-tab} plaintext R output
SQLTransformer (Transformer)
<dplyr_transformer__694d2090_c856_4d4a_a9f5_e5320b158e38> 
 (Parameters -- Column Names)
```
````

### Creating the Pipeline

Our sparklyr pipeline will have a total of 9 steps:

1. SQL transformer - resulting from the `ft_dplyr_transformer()` transformation
2. String indexer for `specialservicetypecategory`
3. String indexer for `originofcall`
4. String indexer for `propertycategory`
5. One hot encoder for `specialservicetypecategory`
6. One hot encoder for `originofcall` 
7. One hot encoder for `propertycategory`
8. Vector assembler - to generate a single `features` column that contains a vector of feature values (`ft_vector_assembler()`)
9. Logistic regression model

Unfortunately, in Spark 2.x there is no way of combining multiple string indexers or one hot encoders for different columns, so we need a pipeline step for each column we need to index and encode. For Spark 3.x the `ft_one_hot_encoder_estimator()` function can be used in the place of `ft_one_hot_encoder()` which allows the selection of multiple columns. See [here](https://search.r-project.org/CRAN/refmans/sparklyr/html/ft_one_hot_encoder_estimator.html) for further details.

The pipeline can be constructed as follows:

````{tabs}
```{code-tab} r R
rescue_pipeline <- ml_pipeline(sc) %>%
  sparklyr::ft_dplyr_transformer(rescue_cat) %>%
  sparklyr::ft_string_indexer(input_col = "specialservicetypecategory", 
                    output_col = "specialservicetypecategory_idx",
                    string_order_type = "alphabetDesc") %>% 
  sparklyr::ft_string_indexer(input_col = "originofcall", 
                    output_col = "originofcall_idx",
                    string_order_type = "alphabetDesc") %>%
  sparklyr::ft_string_indexer(input_col = "propertycategory", 
                    output_col = "propertycategory_idx",
                    string_order_type = "alphabetDesc") %>%
  sparklyr::ft_one_hot_encoder(input_cols = c("specialservicetypecategory_idx"), 
                     output_cols = c("specialservicetypecategory_ohe"), 
                     drop_last = TRUE) %>%
  sparklyr::ft_one_hot_encoder(input_cols = c("originofcall_idx"), 
                     output_cols = c("originofcall_ohe"), 
                     drop_last = TRUE) %>%
  sparklyr::ft_one_hot_encoder(input_cols = c("propertycategory_idx"), 
                     output_cols = c("propertycategory_ohe"), 
                     drop_last = TRUE)  %>%
  sparklyr::ft_vector_assembler(input_cols = c("engine_count", "hourly_cost", "originofcall_ohe", "propertycategory_ohe",
                                              "specialservicetypecategory_ohe"), 
                                output_col = "features") %>%
  ml_generalized_linear_regression(features_col = "features",
                                   label_col = "is_cat",
                                   family = "binomial", 
                                   link = "logit") 
                                   
# View the pipeline
rescue_pipeline

```
````
The pipeline can be fitted to the `rescue` dataset by applying `ml_fit()` and predictions can be accessed using `ml_transform()`.

````{tabs}
```{code-tab} r R
# Fit the pipeline 
fitted_pipeline <- ml_fit(rescue_pipeline, rescue)

# View the fitted pipeline - notice output now shows model coeffiecients
fitted_pipeline

# Get predictions
predictions <- ml_transform(fitted_pipeline, rescue) 
```
````
It is possible to generate a list of model coefficents, p-values etc. using the `ml_stage` function to access the logistic regression stage of the model, combined with `ml_summary` as we did before. However, it is not easy to map the coefficients back to the feature that they correspond to. 

````{tabs}
```{code-tab} r R
# Access the logistic regression stage of the pipeline
model_details <- ml_stage(fitted_pipeline, 'generalized_linear_regression')

# Get coefficients and summary statistics from model
summary <- tibble(coefficients = c(model_details$intercept, model_details$coefficients), 
                  std_errors = model_details$summary$coefficient_standard_errors(), 
                  p_values = model_details$summary$p_values()) %>%
  dplyr::mutate(lower_ci = coefficients - (1.96 * std_errors),
                upper_ci = coefficients + (1.96 * std_errors))
                
# View the summary
summary

```
```{code-tab} plaintext R output
# A tibble: 19 × 5
   coefficients    std_errors p_values      lower_ci     upper_ci
          <dbl>         <dbl>    <dbl>         <dbl>        <dbl>
 1     1.09          0.372    3.45e- 3       0.359        1.82   
 2    -0.680         0.221    2.12e- 3      -1.11        -0.246  
 3    -0.000707      0.000985 4.73e- 1      -0.00264      0.00122
 4    -0.967         0.226    1.85e- 5      -1.41        -0.524  
 5     0.647         1.55     6.77e- 1      -2.40         3.69   
 6     0.134         0.0572   1.94e- 2       0.0216       0.246  
 7    -0.679         0.366    6.34e- 2      -1.40         0.0379 
 8   -25.0      356124.       1.00e+ 0 -698028.      697978.     
 9   -26.0      356124.       1.00e+ 0 -698029.      697977.     
10    -0.0626        1.42     9.65e- 1      -2.84         2.72   
11     0.202         0.147    1.70e- 1      -0.0864       0.490  
12    -1.43          0.116    0             -1.66        -1.20   
13    -0.748         0.0677   0             -0.881       -0.616  
14    -0.277         0.456    5.44e- 1      -1.17         0.617  
15    -0.905         0.0921   0             -1.09        -0.725  
16     0.727         1.42     6.10e- 1      -2.07         3.52   
17    -0.995         0.160    5.08e-10      -1.31        -0.681  
18     0.423         0.0614   5.18e-12       0.303        0.544  
19     0.446         0.0959   3.32e- 6       0.258        0.634  
```
````
You can save a pipeline or pipeline model using the `ml_save` command:

````{tabs}
```{code-tab} r R
# Save pipeline
ml_save(
  rescue_pipeline,
  "rescue_pipeline",
  overwrite = TRUE
)

# Save pipeline model
ml_save(
  fitted_pipeline,
  "rescue_model",
  overwrite = TRUE
)
```
````

They can then be re-loaded using the `ml_load()` command and the re-loaded model can be used to re-fit new data with the same model by supplying the name of the new spark dataframe you wish to fit.

````{tabs}
```{code-tab} r R
# Reload our saved pipeline
reloaded_pipeline <- ml_load(sc, "rescue_pipeline")

# Re-fit to a subset of rescue data as an example of how pipelines can be re-used
new_model <- ml_fit(reloaded_pipeline, sample_frac(rescue, 0.1))

```
````

````{tabs}
```{code-tab} r R
# Close the spark session
spark_disconnect(sc)
```
````

</details>


## Further resources
**PySpark references and documentation:**
- [`pyspark.ml` package](https://spark.apache.org/docs/2.3.0/api/python/pyspark.ml.html)
- [`GeneralizedLinearRegression`](https://spark.apache.org/docs/2.3.0/api/python/pyspark.ml.html#pyspark.ml.regression.GeneralizedLinearRegression)
- [`Correlation`](https://spark.apache.org/docs/2.3.0/api/python/pyspark.ml.html#pyspark.ml.stat.Correlation)
- [`Pipeline`](https://spark.apache.org/docs/2.3.0/api/python/pyspark.ml.html#pyspark.ml.Pipeline)
- [`StringIndexer`](https://spark.apache.org/docs/2.3.0/api/python/pyspark.ml.html#pyspark.ml.feature.StringIndexer)
- [`OneHotEncoder`](https://spark.apache.org/docs/2.3.0/api/python/pyspark.ml.html#pyspark.ml.feature.OneHotEncoder)
- [`VectorAssembler`](https://spark.apache.org/docs/2.3.0/api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler)


**Sparklyr references and documentation:**
  
- [Spark Machine Learning Library](https://spark.rstudio.com/guides/mlib.html)
- [Reference - Spark Machine Learning](https://spark.rstudio.com/packages/sparklyr/latest/reference/#spark-machine-learning)
- [Spark ML Pipelines](https://spark.rstudio.com/guides/pipelines.html)
- [`broom` documentation](https://broom.tidymodels.org/)
- [`ml_generalized_linear_regression()`](https://www.rdocumentation.org/packages/sparklyr/versions/1.8.2/topics/ml_generalized_linear_regression)
- [`ml_logistic_regression`](https://www.rdocumentation.org/packages/sparklyr/versions/0.4/topics/ml_logistic_regression)



### Acknowledgements
              
Thanks to Ted Dolby for providing guidance on logistic regression in sparklyr which was adapted to produce this page.