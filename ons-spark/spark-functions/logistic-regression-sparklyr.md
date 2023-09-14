# Logistic Regression in sparklyr

Logistic regression is often used in predictive analysis. A logistic
regression model is trained on a dataset to learn how certain
independent variables relate to the probability of a binary outcome
occuring. This model is then applied to cases where the independent
variables are known, but the outcome is unknown, and the probability of
the outcome occuring can be estimated.

This article shows how to use the `Spark ML` functions to generate a
logistic regression model in sparklyr, including a
discussion of the steps necessary to prepare the data and potential
issues to consider. We will also show how to access standard errors and
confidence intervals for inferential analysis.

## Preparing the data

In sparklyr, we will be using the `Spark ML` functions
`ml_logistic_regression`, `ml_predict` and
`ml_generalised_linear_regression`. The syntax used to call these
functions is somewhat similar to logistic regression functions in R.
However, there are some additional steps to take in preparing the data
before the model can be run successfully.


We will read the data in as follows:

````{tabs}
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

Examining the dataset with `glimpse()`, we can see that a few columns we have selected (such as `engine_count`) are of the type “character” when they should be numeric. We can convert all of these to numeric values by running the following:


````{tabs}
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


````{tabs}
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

## Inferential analysis

The `Spark ML` functions available in sparklyr were largely developed
with predictive analysis in mind. This means that they have been built
primarily to specify a regression model and retrieve the prediction (as
                                                                     we have done above), with little information in between.

When conducting analysis at ONS, we are often not interested in
predicting unknown outcomes, but instead understanding the relationship
between the independent variables and the probability of the outcome.
This is what is referred to as *inferential analysis* in this section.


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
c("coefficient_standard_errors") %>%
  purrr::map(~ml_summary(glm_out, .x, allow_null = TRUE)) %>%
  purrr::map_if(is.function, ~.x())
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

## Things to watch out for

### Singularity issues

We need to be careful to check that variables included in the model take
more than one value in order to avoid errors.

If we were carrying out logistic regression in R using functions such as
`glm`, these variables would be dropped automatically. However, this is
not the case in `sparklyr`.

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

By default, the `ml_generalized_linear_regression` function in R will
select the least common category as the reference category before
running the model, without input from the user. It also implicitly
applies one-hot encoding to the categories so that they can be
represented as a binary numerical value.

For example, `specialservicetypecategory` has four unique values: Animal
Rescue From Below Ground, Animal Rescue From Height, Animal Rescue From
Water, and Other Animal Assistance.

````{tabs}
```{code-tab} r R 
rescue_cat %>% 
  dplyr::count(specialservicetypecategory) %>% 
  dplyr::arrange(n)
```
````
````{tabs}
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

In this case, `ml_generalized_linear_regression` would select “Animal
Rescue From Water” as the reference category, since there are only 343
instances of it in the data. For the purpose of running the model, it
would also create a new column in the data for each of the other
categories, assigning a 1 or 0 for each row depending on whether that
entry belongs to that category or not. Regression coefficients would
then be shown for each of these new columns in the final model, relative
to the to the “Animal Rescue From Water” reference category.

Selecting a reference category can be particularly useful for
inferential analysis. For example, in the `rescue_cat` dataset, we might want to
select “Other Animal Assistance” as our reference category instead
because it is the largest special service type and could serve as a
useful reference point.

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

### Multicollinearity

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

We can also combine all of the analysis steps outlined above into a single workflow using Spark's ML Pipelines. Data manipulation, feature transformation and modelling can be rewritten into a `ml_pipeline()` object and saved. The output of this in sparklyr is in Scala code, meaning that it can be added to scheduled Spark ML jobs without any dependencies in R.

Note that in sparklyr, we have not yet found a simple way of generating a summary coefficient table as shown in the sections above when using a pipeline for analysis. Although coefficients, p-values etc. can be obtained it is difficult to map these to the corresponding features for inferential analysis. As a result, pipelines for logistic regression in sparklyr are currently best used for predictive analysis only.

### ft_dplyr_transformer

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
```{code-tabs} r R
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
```{code-tabs} r R
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
               
