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

## Preparing the data

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

## Running a logistic regression model

<details>
<summary><b>Python Explanation</b></summary>


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

</details>

<details>
<summary><b>R Explanation</b></summary>
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
model.summary
```

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
````

Individual statistics of interest can also be accessed directly using
`ml_summary`. For example, to get the standard errors:

````{tabs}
```{code-tab} r R 
glm_out$summary$coefficient_standard_errors()
```
````

Other statistics can also be accessed individually in this way by
replacing “coefficient\_standard\_errors” with the statistic of interest
from the list below:

````{tabs}
``` plaintext options
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
````

</details>


## Things to watch out for

### Singularity issue
We need to be careful to check that variables included in the model take
more than one value in order to avoid errors.

If we were carrying out logistic regression in R using functions such as
`glm`, these variables would be dropped automatically. However, this is
not the case in `sparklyr`.

**PySpark to be added**

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
``` plaintext R output
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
</details>
<details>
<summary><b>R Example</b></summary>

To do this, we can make use of the one-hot encoding concept and two of
`sparklyr`’s **Feature Transformers** (`ft_string_indexer` and `ft_one_hot_encoder`) to achieve this. These functions have limited functionality, so we must first manipulate it such that the reference category will be ordered last when using `ft_string_indexer`, and then
we can drop the last category using `ft_one_hot_encoder`. A convenient way of doing this is to order the categories in *descending alphabetical* order and ensuring that our chosen category will be ordered last by adding an appropriate prefix to it. For example, adding
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
rescue_cat_ohe <- rescue_cat_ohe %>% 
  sparklyr::select(-ends_with(c("_ohe", "_idx")),
                   -originofcall,
                   -propertycategory)
```
````

Once this is done we can run the regression again and get the
coefficients relative to the chosen reference categories:

````{tabs}
```{code-tab} r R 
# Run regression with one-hot encoded variables with chosen reference categories
glm_ohe <- dparklyr::ml_generalized_linear_regression(rescue_cat_ohe, 
                                                      formula = "is_cat ~ .", 
                                                      family = "binomial", 
                                                      link = "logit")

# Get coefficients and confidence intervals
broom::tidy(glm_ohe) %>%
  dplyr::mutate(lower_ci = estimate - (1.96 * std.error),
                upper_ci = estimate + (1.96 * std.error))
```
````

````{tabs}
``` plaintext R output
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
**Needs pyspark equivalent adding**  

A key assumption of linear and logistic regression models is that the feature columns are independent of one another. Including features that correlate with each other in the model is a clear violation of this assumption, so we need to identify these and remove them to get a valid result.

A useful way of identifying these variables is to generate a correlation matrix of all the the features in the model. This can be done using the `ml_corr` function: 

````{tabs}
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
```{code-tab} r R
rescue_cat %>%
  sparklyr::select(job_hours,
                   hourly_cost,
                   total_cost) %>%
  dplyr::arrange(desc(job_hours)) %>%
  print(n = 30)
```
````
Therefore, we need to remove this feature from our model since it is not independent of other variables. You may also want to remove other variables from the model based on the correlation matrix. For example, there is also quite high correlation between the `job_hours` column and several other features. Deciding on which features to remove from your model may require some experimentation, but in general, a correlation coefficient of >0.7 among two or more predictors indicates multicollinearity and some of these should be removed from the model to ensure validity.

````{tabs}
```plaintext R output
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

We can also combine all of the analysis steps outlined above into a single workflow using Spark's ML Pipelines. Data manipulation, feature transformation and modelling can be rewritten into a `pipeline`/`ml_pipeline()` object and saved. The output of this in sparklyr is in Scala code, meaning that it can be added to scheduled Spark ML jobs without any dependencies in R.

Note that in sparklyr, we have not yet found a simple way of generating a summary coefficient table as shown in the sections above when using a pipeline for analysis. Although coefficients, p-values etc. can be obtained it is difficult to map these to the corresponding features for inferential analysis. As a result, pipelines for logistic regression in sparklyr are currently best used for predictive analysis only.

<details>
<summary><b>Python Example</b></summary>

Note that these tasks haven't actually been carried out yet, we have simply defined what they are so we can call them all in succession later. This can be done by setting up a "pipeline" to call all the tasks we need to carry out the regression one by one:

````{tabs}
```{code-tab} py
# Creating the pipeline
pipe = Pipeline(stages=[serviceIdx, callIdx, propertyIdx,
                        serviceEncode, callEncode, propertyEncode,
                        assembler, log_reg])
```
````

</details>


<details>
<summary><b>R Example</b></summary>

As above, we still need to generate the `is_cat` column from the original data, select our predictors and remove missing values. In order to select our reference categories we can also add the "000_" prefix to these categories at this stage. This step can later be called in the pipeline using the `ft_dplyr_transformer` feature transformer. This function converts `dplyr` code into a SQL feature transformer that can then be used in a pipeline. We will set up the transformation as follows:

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
                   ~as.numeric(.))) %>
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

``` plaintext R output
SQLTransformer (Transformer)
<dplyr_transformer__694d2090_c856_4d4a_a9f5_e5320b158e38> 
 (Parameters -- Column Names)
```
````

### Creating the Pipeline

Our pipeline will have a total of 9 steps:

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
It is possible to generate a list of model coefficents, p-values etc. using the `ml_stage` function to access the logistic regression stage of the model, combined with `ml_summary` as we did before. However, it is not easy to map the coefficients to the feature that they correspond to. 

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
````

````{tabs}
``` plaintext R output
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
</details>


## Further resources

Sparklyr and tidyverse documentation:
  
- [Spark Machine Learning Library](https://spark.rstudio.com/guides/mlib.html)
- [Reference - Spark Machine Learning](https://spark.rstudio.com/packages/sparklyr/latest/reference/#spark-machine-learning)
- [Spark ML Pipelines](https://spark.rstudio.com/guides/pipelines.html)
- [`broom` documentation](https://broom.tidymodels.org/)
- [`ml_generalized_linear_regression()`](https://www.rdocumentation.org/packages/sparklyr/versions/1.8.2/topics/ml_generalized_linear_regression)
- [`ml_logistic_regression`](https://www.rdocumentation.org/packages/sparklyr/versions/0.4/topics/ml_logistic_regression)
               
### Acknowledgements
              
Thanks to Ted Dolby for providing guidance on logistic regression in sparklyr which was adapted to produce this page.