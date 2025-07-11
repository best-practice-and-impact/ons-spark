## Synthpop: Synthetic Data in R

### 1. Introduction?

Synthetic data consists of artificially generated records that mimic the structure and statistical properties of real-world data, without containing any actual personal or sensitive information. It is widely used for:

- Testing and validating data pipelines and statistical models
- Protecting privacy and confidentiality in data sharing
- Simulating rare or edge-case scenarios
- Teaching and training with realistic, non-disclosive datasets
- Developing and prototyping systems in non-secure environments

**Learning Objectives:**

By the end of this section, you will be able to:
- Understand the concept and importance of synthetic data
- Recognise key use cases for synthetic data in official statistics and research
- Learn two main approaches for generating synthetic data in R:
  1. Writing your own code
  2. Using the `synthpop` R package

### 2. Generating synthetic data in R from scratch
````{tabs}

```{code-tab} r R

# Load the R packages
rm(list = ls()) 
library(dplyr)
library(readr)
library(janitor)
library(magrittr)

```
````
#### 2.1 Numerical variables

In this section, we will generate synthetic numerical variables using different distributions in R. We will use the following distributions:

1. Uniform Distribution
2. Normal Distribution
3. Binomial Distribution
4. Poisson Distribution

Below are the R codes to generate these variables.

Note that in the interest of reproducible results, we can use the  [`set.seed(n)`](https://www.rdocumentation.org/packages/base/versions/3.6.2/topics/Random) function.
````{tabs}

```{code-tab} r R

set.seed(12345)

```
````
**1. Uniform Distribution**: The uniform distribution generates data where each value within a specified range is equally likely. This is useful for for simulating random allocation of survey respondents to experiemental groups, where each group has an equal chance of selection.

[`runif()`](https://github.com/best-practice-and-impact/ons-spark/blob/synthetic-data-branch/ons-spark/raw-notebooks/synthetic-data/synthpop_with_r.ipynb) generates data where each value within the specified range is equally likely. We can visualise this data using the following example:
````{tabs}

```{code-tab} r R

uniform_dist <- runif(n = 1000, 
                      min = 0,  
                      max = 1)
hist(uniform_dist, main="Uniform Distribution Example", xlab="Value")
summary(uniform_dist)

```
````
**2. Normal Distribution**: The normal distribution, or Gaussian distribution, is useful for simulating data that clusters around a mean. This is common in many natural phenomena like modelling adult heights or test scores, which tend to cluster around a mean in population health or education statistics.

[`rnorm()`](https://www.rdocumentation.org/packages/stats/versions/3.6.2/topics/Normal) generates data that clusters around a specified mean, with a given standard deviation. We explore the generated data using the `hist()` and `summary()` functions.
````{tabs}

```{code-tab} r R

normal_dist <- rnorm(n = 1000,
                     mean = 50,
                     sd = 4)
hist(normal_dist, main="Normal Distribution Example", xlab="Value")
summary(normal_dist)

```
````
**3. Poisson Distribution**: The Poisson distribution is used for count data, where you are counting the number of events in a fixed interval of time or space. Typical examples is simulating the number of visits to a government website per day or modelling the number of emergency service calls.

[`rpois()`](https://www.rdocumentation.org/packages/stats/versions/3.6.2/topics/Poisson) generates count data, useful for modeling the number of events in a fixed interval.
````{tabs}

```{code-tab} r R

poisson_dist <- rpois(n = 1000,  
                      lambda = 4)  

hist(poisson_dist, main="Poisson Distribution Example", xlab="Number of Events")
summary(poisson_dist)

```
````
**4. Binomial Distribution**: The binomial distribution is useful for simulating the number of successes in a fixed number of trials, each with the same probability of success. This is suitable for estimating the number of individuals in a survey who respond "yes" to a yes/no question, such as employment status or vaccine uptake.

[`rbinom()`](https://www.rdocumentation.org/packages/stats/versions/3.6.2/topics/Binomial) generates data representing the number of successes in a fixed number of trials, each with the same probability of success.
````{tabs}

```{code-tab} r R

binomial_dist <- rbinom(n = 1000,   
                        size = 20,  
                        prob = 0.2)  

hist(binomial_dist, main="Binomial Distribution Example", xlab="Number of Successes")
summary(binomial_dist)

```
````
#### 2.2 Character/factor variables

In this section, we will generate synthetic `character (factor)` variables using random sampling in R. These variables are useful for simulating categorical data, such as demographic information.

**1. Random sampling from a vector**: Random sampling from a vector allows you to generate categorical data by randomly selecting elements from a specified set. This is useful for creating variables like gender, where each observation is randomly assigned a category.
````{tabs}

```{code-tab} r R

gender <- sample(x = c("M", "F"),  
              size = 1000,      
              replace = TRUE)   

table(gender)
prop.table(table(gender))

```
````
**2. Weighted sampling from a vector**: Weighted sampling allows you to generate categorical data with specified probabilities for each category. This is useful for creating variables like marital status, where each category has a different likelihood of being selected.


````{tabs}

```{code-tab} r R

marriage_status <- sample(x = c("Single", "Married", "Divorced", "Widowed"),  
                          size = 1000,    
                          replace = TRUE,
                          prob = c(0.35, 0.50, 0.10, 0.05))

table(marriage_status)
prop.table(table(marriage_status))

```
````
Here is the explanation of the code snippet above:

1. Setting a Seed (`set.seed`):

* `set.seed(12345)`: Sets the seed for random number generation. The number `12345` can be any integer. Using the same seed will produce the same sequence of random numbers each time the code is run.
2. Random Sampling (`sample`):

* `x`: The vector of elements to choose from.
* `size`: The number of observations to generate.
* `replace`: Whether sampling is with replacement (`TRUE`) or without replacement (`FALSE`).
* Further reading can be found here: [sampling](../spark-functions/sampling).
3. Weighted Sampling (`sample` with `prob`):

* `prob`: A vector of probabilities corresponding to the likelihood of each element in `x` being selected. The probabilities must sum to `1`.
* Further reading can be found here: [sampling](../spark-functions/sampling).

#### 2.3 Combining data into a synthetic dataset

Now, we will combine the numerical and categorical data into a single synthetic dataset using the `data.table` package:
````{tabs}

```{code-tab} r R

library(data.table)

synthetic_data1 <- data.table(uniform_dist,
                              normal_dist,
                              poisson_dist,
                              binomial_dist,
                              gender,
                              marriage_status)

print(head(synthetic_data1))
summary(synthetic_data1)
str(synthetic_data1)

```
````
**Converting Character Variables to Factors**  
To ensure that the categorical variables are treated appropriately in analyses, we should convert them to factor variables:
````{tabs}

```{code-tab} r R

# Change gender and marriage_status to factor variables
synthetic_data1$gender <- as.factor(synthetic_data1$gender)
synthetic_data1$marriage_status <- as.factor(synthetic_data1$marriage_status)

# Check the structure of the dataset again
str(synthetic_data1)

```
````
This combined synthetic dataset can now be used for various analyses, simulations, and testing purposes. It provides a comprehensive representation of both numerical and categorical variables, making it suitable for a wide range of applications.

These snippets provide a starting point for generating synthetic categorical data using random and weighted sampling with reproducibility. You can adjust the parameters to fit the specific characteristics of the population you are modeling. For more detailed information on the `sample` function, refer to the [R documentation](https://www.rdocumentation.org/packages/base/versions/3.6.2/topics/sample).

### 3. Generating synthetic data using `synthpop`

In the guidiance, we will work with the R package `synthpop` [(Nowok, Raab, and Dibben 2016)](https://www.synthpop.org.uk/get-started.html), which is one of the most advanced and dedicated packages in R to create synthetic data.


Synthpop generates synthetic data by modeling the relationships in the original dataset and then simulating new data that preserves the statistical properties of the original, while reducing disclosure risk.

#### 3.1 Setting up `synthpop` and required packages
````{tabs}

```{code-tab} r R

install.packages("synthpop")
install.packages("sparklyr")

```
````


This will install `synthpop` and its dependencies and `sparklyr` from [ONS Artifactory](https://onsart-01/ui/login/).


First clear the workspace and load packages using the `library()` function.


````{tabs}

```{code-tab} r R

rm(list = ls())
library(synthpop)

```
````


To get a list of all `synthpop` functions, use:



```{r, results='hide'}
help(package = synthpop)
```



To quickly access a help file for a specific function, such as the main `synthpop` function [`syn()`](https://www.rdocumentation.org/packages/synthpop/versions/1.8-0/topics/syn), you can type its name preceded by `?`.


```{r, results='hide'}
?synthpop
```

```{r, results='hide'}
?syn
```

### Case 1: Synthesise `synthpop` sample data `SD2011`

In most cases, you will be working with your own data, but for practice, you can use the sample data [`SD2011`](https://www.rdocumentation.org/packages/synthpop/versions/1.8-0/topics/SD2011) provided with the `synthpop` package.

**Read the data**  
Read the data you want to synthesise into `R`. You can use the `synthpop` function [`read.obs()`](https://www.rdocumentation.org/packages/synthpop/versions/1.9-0/topics/read.obs) to read data from other formats.

**NOTE**:  `synthpop` is an R package that generates synthetic data within R. While `synthpop` itself does not generate data directly in Spark, the synthetic datasets it produces can be easily transferred to Spark using `sparklyr` for scalable post-processing and analysis. Thus, Spark is used for processing and analysing the synthetic data, not for the initial data generation with `synthpop`.

**Examine your data**  
Start with a modest number of variables (8-12) to understand `synthpop`. If your data have more variables, make a selection. The package is intended for large datasets (at least `500` observations).

Use the [`codebook.syn()`](https://www.rdocumentation.org/packages/synthpop/versions/1.8-0/topics/codebook.syn) function to examine the features relevant to synthesising.



In this section, we will guide you through a sample synthesis using the `synthpop` package in R. This example uses the [`SD2011` dataset](https://www.rdocumentation.org/packages/synthpop/versions/1.8-0/topics/SD2011) sample, provided with the `synthpop` package and [demonstrates how to generate synthetic data](https://www.synthpop.org.uk/assets/firstsynthesis.r) with a smaller subset of variables. The SD2011 dataset is a social diagnosis survey from 2011, Poland


In this example, we will load the SD2011 dataset, select a subset of variables, generate a synthetic version using synthpop, and compare the synthetic data to the original.

#### Step-by-step guide

1. **Explore the `SD2011` dataset**  
Here, we will use the `help()` method to obtain the information about the SD2011, `dim()` method to get the size of the dataframe and `codebook.syn()` method to obtain the summary information about variables.


Note: SD2011 is included with the synthpop package. If you cannot find it, load it with data(SD2011). This example was tested in RStudio/VS Code/CDP with synthpop installed.

```{r, results='hide'}
help(SD2011) # this will give you information about it
```
````{tabs}

```{code-tab} r R

# This will give you the dimensions (rows, columns) of the SD2011 dataset
dim(SD2011)  # Output: 5000 rows, 35 variables

```
````

````{tabs}

```{code-tab} r R

# Show summary information for a subset of variables; here we show only the first five variables for brevity.
synthpop::codebook.syn(SD2011[, 1:5])$tab

```
````
2. **Select a subset of variables**
* After loading the data, inspect it to understand its structure and contents.
* You can review a `summary()` of all variables to generate descriptive statistics.
````{tabs}

```{code-tab} r R

# Select a smaller subset of variables for demonstration
mydata <- SD2011[, c(1, 2, 3, 6, 8, 10, 11)]
# Preview the first few rows of the dataset
head(mydata)
# Get a summary of the dataset
summary(mydata)

```
````
3. **Checking for missing values**
The following code checks for negative and missing values in the income variable.
````{tabs}

```{code-tab} r R

# Check for negative income values
table(mydata$income[mydata$income < 0], useNA = "ifany")

```
````
4. **Synthesise data**  
 * Use the `syn()` function from the `synthpop` package to create a synthetic version of your data, using the default settings.
 * Fix the random seed before generating the synthetic data, so that you can compare your output with our output.
````{tabs}

```{code-tab} r R

set.seed(123)

# Synthesise data, handling -8 as a missing value for income
mysyn <- synthpop::syn(mydata, cont.na = list(income = -8))

```
````
We have now created the object `mysyn`, which is of class `synds` and contains several pieces of information related to the synthetic data. We will quickly go over the most important output. We can examine the synthesised data using the summary and head methods.
````{tabs}

```{code-tab} r R

summary(mysyn)

```
````

````{tabs}

```{code-tab} r R

head(mysyn$syn) 

```
````
The `compare()` function below produces plots and statistics that help you assess how closely the synthetic data matches the original data for each variable:

- The bar plots show the distribution of counts for each value/category, with a legend distinguishing "Observed" (original data) and "Synthetic" (synthetic data).
- If the bars for observed and synthetic data are similar in height for each value, this indicates that the synthetic data replicates the original distribution well.
- The S_pMSE (Standardized Propensity Mean Squared Error) value, shown on each plot, quantifies the similarity between the observed and synthetic distributions. 
    - **A lower S_pMSE value (closer to 0)** means the synthetic data closely matches the original data for that variable.
    - **A higher S_pMSE value (>> 1)** means there are greater differences between the synthetic and original data.

In summary, look for S_pMSE values near 0 and similar bar heights to conclude that the synthetic data replicates the original data well for a given variable. You can also get the S_pMSE values from the `print(mysyn)` method
````{tabs}

```{code-tab} r R

# Compare the synthetic data with the original data. In most cases, you would need to press "Press return for next variable(s)"
synthpop::compare(mysyn, mydata, stat = "counts")

```
````
This figure shows examples of visuals from using the `multi.compare()` function:

```{figure} ../images/Rplot_Observed_versus_synthetic_data.png
---
width: 100%
name: MultiCompareDiagram
alt: Diagram showing visuals from using the `multi.compare()` function
---
Observed_versus_synthetic_data
```

5. **Export synthetic data**
````{tabs}

```{code-tab} r R

# Export synthetic data to CSV format
write.syn(mysyn, filename = "mysyn_SD2011", filetype = "csv")

```
````
6. **Explore synthetic data**  
After generating synthetic data using the `synthpop` package, it is important to explore and understand the structure and components of the synthetic data object. This section provides guidance on how to explore the synthetic data object and perform additional comparisons.

**I. Retrieve component names**
````{tabs}

```{code-tab} r R

names(mysyn)

```
````
* This command retrieves the names of the components within the `mysyn` object, helping you understand its structure.

**II. Explanation of some of the components**
1. [`syn`](https://www.synthpop.org.uk/assets/firstsynthesis.r): The synthetic data set.
2. `method`: The methods used for synthesising each variable.
3. `predictor.matrix`: The matrix indicating which variables were used as predictors for each synthesised variable.
4. `visit.sequence`: The order in which the variables were synthesised.
5. `cont.na`: Information about how missing values were handled.
6. `rules`: Any rules applied during synthesis.
7. `rvalues`: Values used for rules.
8. `m`: Number of synthetic datasets created (if multiple).
9. `proper`: Indicates if proper synthesis was used.
10. `seed`: The seed used for random number generation.
11. `call`: The original call to the syn() function.
12. `data`: The original data used for synthesis.

**III. Inspect key components**
````{tabs}

```{code-tab} r R

mysyn$method
mysyn$predictor.matrix
mysyn$visit.sequence
mysyn$cont.na
mysyn$seed

```
````
**IV. Additional comparisons**  
To further validate the synthetic data, you can perform additional comparisons between the synthetic and original data using the [`multi.compare()`](https://www.rdocumentation.org/packages/synthpop/versions/1.8-0/topics/multi.compare) function.
````{tabs}

```{code-tab} r R

# Additional comparisons
synthpop::multi.compare(mysyn, mydata, var = "marital", by = "sex")
synthpop::multi.compare(mysyn, mydata, var = "income", by = "agegr")
synthpop::multi.compare(mysyn, mydata, var = "income", by = "edu", cont.type = "boxplot")

```
````
These figures show examples of visuals from using the `multi.compare()` function:

```{figure} ../images/synthetic_data_marital_sex_relationship.png
---
width: 100%
name: MultiCompareDiagram
alt: Compares the distribution of the `marital` variable by `sex` between the synthetic and original data
---
Distribution_of_marital_variable_by_sex-synthetic_versus_original_data
```

```{figure} ../images/synthetic_data_income_agegroup_relationship.png
---
width: 100%
name: MultiCompareDiagram
alt: Compares the distribution of the `income` variable by `age group` between the synthetic and original data
---
Distribution_of_income_variable_by_agegroup-synthetic_versus_original_data
```

```{figure} ../images/synthetic_data_income_edu_relationship.png
---
width: 100%
name: MultiCompareDiagram
alt: Compares the distribution of the `income` variable by `education level` using boxplots between the synthetic and original data
---
Distribution_of_income_variable_by_educationlevel-synthetic_versus_original_data
```

These steps help ensure that the synthetic data accurately reflects the structure and relationships present in the original data, making it suitable for analysis while protecting the privacy of the original data.

### Using `synthpop` with `sparklyr` for scalable synthetic data processing

You can use `synthpop` together with `sparklyr` to generate synthetic data in R and then process it at scale with Apache Spark. In the example below, we build on Case 1 by copying the synthetic SD2011 data to Spark and using `dplyr` verbs for distributed analysis. We demonstrate how to group the data by `sex` and calculate the mean `income`, but you can apply any other Spark operations as needed. The `mysyn` object (of class `synds`) generated earlier is used for this Spark analysis.
````{tabs}

```{code-tab} r R

install.packages("sparklyr")
library(sparklyr)

sc <- sparklyr::spark_connect(
  master = "local[2]",
  app_name = "synthpop-spark",
  config = sparklyr::spark_config())

# Access the synthetic data generated by synthpop (see Case 1)
synthetic_sd2011 <- mysyn$syn

# Copy the synthetic data frame from R into the Spark session as a Spark table
synthetic_sd2011_tbl <- copy_to(
  sc,
  synthetic_sd2011,
  "synthetic_sd2011",
  overwrite = TRUE
)

# Now you can use dplyr verbs with Spark for scalable processing
result_tbl <- synthetic_sd2011_tbl %>%
  group_by(sex) %>%
  summarise(
    mean_income = mean(income, na.rm = TRUE),
    count = n()
  )
  
result_tbl %>% collect()

# spark_disconnect(sc)

```
````
### Case 2: Synthesise census teaching data

In this section, we will guide you through the process of synthesising another case study as presented by Iain Dove, a census teaching data using the `synthpop` package in R. This demo is an alternative to the synthesis of `SD2011` in `Case 1`. This involves loading the data, creating a subset, synthesising the data, and comparing the synthetic data with the original data.

#### Step-by-step guide

1. **Loading the data**
````{tabs}

```{code-tab} r R

# Load the configuration and set the path to the census teaching data
config <- yaml::yaml.load_file("/home/cdsw/ons-spark/config.yaml")
census_2011_path = config$census_2011_teaching_data_path_csv
census_teaching_data <- data.table::fread(census_2011_path, skip = 1) %>%
                        janitor::clean_names()

```
````
2. **Create a subset of the data**

As before, we first create a subset of the data to reduce run time.
````{tabs}

```{code-tab} r R

# Create a subset of the data
small_census_teaching_data <- census_teaching_data[1:100000,]

```
````


3. **First synthesis of census data**

Let's now create the synthetic data.


````{tabs}

```{code-tab} r R

# First synthesis
synthetic_census_teaching_data <- synthpop::syn(data = small_census_teaching_data,
                                                method = "sample",
                                                k = 100000)

```
````


You have created a synthetic data object `synthetic_census_teaching_data` of class `synds`, which is a list with a number of components including the synthesised data and information on how they were created.


````{tabs}

```{code-tab} r R

# Check the class of the synthetic data object
class(synthetic_census_teaching_data)

```
````
4. **Access the synthetic dataframe**

We have now created the object `mysyn`, which is of class `synds` (the main output class from synthpop, containing the synthetic data and metadata).

To view the synthetic data, use `synthetic_census_teaching_data$syn`, and use `summary()` to review all variables 

```{r, results='hide'}
# Access the synthetic dataframe
synthetic_census_teaching_data$syn
```
````{tabs}

```{code-tab} r R

summary(synthetic_census_teaching_data)

```
````
5. **Comparison with real data**

`synthpop` contains functions to assess the accuracy of the synthetic data. Univariate measures are quite easy to satisfy, but relationships between multiple variables are much more difficult.
````{tabs}

```{code-tab} r R

# Compare synthetic data with real data
synthpop::compare(object = synthetic_census_teaching_data,
                  data = small_census_teaching_data)

```
````
#### Relationships between multiple variables

Relationships between multiple variables are more complex to model. The `synthpop` default uses a saturated model, which can be challenging for large datasets. Here, we use an input `CSV` file to specify which variables are related.

1. **Load the input CSV file**
````{tabs}

```{code-tab} r R

config <- yaml::yaml.load_file("/home/cdsw/ons-spark/config.yaml")
census_relationship_path = config$census_relationship_file_path_csv

input <- read.csv(census_relationship_path,
                  stringsAsFactors = FALSE,
                  skip = 1) %>%
         dplyr::select(variable, description, predictors)

# Add row names to make it easier
rownames(input) <- input$variable

```
````


2. **Create predictor matrix**

The predictor matrix tells `synthpop` which variables are related and which variables predict each other. If variable `X` is used as a predictor for variable `Y`, the relationship between `X` and `Y` will be preserved to some extent.

The predictor matrix should have the following structure:
* Each row represents a variable that is being predicted.
* Each column represents a variable that is used as a predictor.
* A value of 1 in the matrix indicates that the column variable is used as a predictor for the row variable.
* A value of 0 indicates that the column variable is not used as a predictor for the row variable.


````{tabs}

```{code-tab} r R

# Create predictor matrix in appropriate format for synthpop from CSV input
predictor_matrix <- as.data.frame(matrix(0, nrow = nrow(input), 
                                         ncol = nrow(input), 
                                         dimnames = list(input$variable, input$variable)))

# Use predictors
for (var in input$variable) {
  predictor_matrix[var, as.vector((strsplit(input[var, 'predictors'], " "))[[1]])] <- 1
}

```
````
Relationships can be added manually (`row` variable predicted by `column` variable)

Sensible predictors could be Marital Status being predicted by `Age` (under 16s not married) hours worked being predicted by `Economic Activity`.
````{tabs}

```{code-tab} r R

# Relationships can be added manually (row variable predicted by column variable)
predictor_matrix['Marital_Status', 'Age'] <- 1
predictor_matrix['Economic_Activity', 'Age'] <- 1
predictor_matrix['Occupation', 'Economic_Activity'] <- 1
predictor_matrix['Hours_worked_per_week', 'Economic_Activity'] <- 1

```
````


3. **Synthesise data with relationships**

These relationships are used in the synthesis model. The `minnumlevels` parameter converts numeric variables with few unique values into factor variables for the synthesis.


````{tabs}

```{code-tab} r R

# Synthesise data with relationships
synthetic_census_teaching_data_2 <- synthpop::syn(data = small_census_teaching_data,
                                                  predictor.matrix = as.matrix(predictor_matrix),
                                                  minnumlevels = 10)

```
````
#### Comparison with real data

To compare the synthetic and original data, use:
````{tabs}

```{code-tab} r R

# Compare synthetic data with real data
synthpop::compare(data = small_census_teaching_data,
                  object = synthetic_census_teaching_data_2)

```
````
**Press `enter` on your keyboard for the next variable**



You can write your own code to compare the datasets, particularly on variables/statistics of interest, e.g., compare bi-variate counts.


````{tabs}

```{code-tab} r R

# Define variables to compare
compare_vars <- c("economic_activity", "hours_worked_per_week")

# Frequency table of compare_vars in original data
orig_table <- small_census_teaching_data[, .(orig_count = .N), keyby = compare_vars]

# Convert to factors
orig_table <- orig_table %>% 
              dplyr::mutate(economic_activity = as.factor(economic_activity),
                            hours_worked_per_week = as.factor(hours_worked_per_week))

orig_table

```
````

````{tabs}

```{code-tab} r R

# Frequency table of compare_vars in synthetic data with relationships
synth2_table <- as.data.table(synthetic_census_teaching_data_2$syn)[, .(synth2_count = .N), keyby = compare_vars]

synth2_table

# Convert economic_activity in synth2_table to a factor
synth2_table <- synth2_table %>% 
                dplyr::mutate(economic_activity = as.factor(economic_activity),
                              hours_worked_per_week = as.factor(hours_worked_per_week))

```
````

````{tabs}

```{code-tab} r R

# Frequency table of compare_vars in basic synthetic data
synth1_table <- as.data.table(synthetic_census_teaching_data$syn)[, .(synth1_count = .N), keyby = compare_vars]

synth1_table

# Convert to factors
synth1_table <- synth1_table %>% 
              dplyr::mutate(economic_activity = as.factor(economic_activity),
                            hours_worked_per_week = as.factor(hours_worked_per_week))


```
````

````{tabs}

```{code-tab} r R

# Combine counts from both datasets for comparisons
comparison2_table <- merge(orig_table, synth2_table, all = TRUE)
comparison1_table <- merge(orig_table, synth1_table, all = TRUE)

# Replace NA with 0
comparison1_table <- comparison1_table %>% 
                     dplyr::mutate(orig_count = as.double(orig_count)) %>% 
                     dplyr::mutate(orig_count = if_else(condition = is.na(orig_count),
                                                        true = 0, 
                                                        false = orig_count))


# NAs occur if the combination is not present in one of the datasets, convert these to zero counts
comparison2_table[is.na(orig_count), orig_count := 0]
comparison2_table[is.na(synth2_count), synth2_count := 0]
comparison1_table[is.na(orig_count), orig_count := 0]
comparison1_table[is.na(synth1_count), synth1_count := 0]

# Average absolute distance
comparison2_table[, mean(abs(orig_count - synth2_count))]
comparison1_table[, mean(abs(orig_count - synth1_count))]

# Average percentage difference
comparison2_table[, 100 * mean((abs(orig_count - synth2_count) / orig_count))]
comparison1_table[, 100 * mean((abs(orig_count - synth1_count) / orig_count))]

```
````


This guide provides a concise overview of synthesizing census teaching data with relationships between multiple variables using the `synthpop` package in R. For more detailed information, refer to the [synthpop documentation](https://www.synthpop.org.uk/get-started.html).

### References

* [Synthetic data at ONS](https://www.ons.gov.uk/methodology/methodologicalpublications/generalmethodology/onsworkingpaperseries/onsmethodologyworkingpaperseriesnumber16syntheticdatapilot)
* [`synthpop` (Nowok, Raab, and Dibben 2016)](https://www.synthpop.org.uk/get-started.html)
* [Synthetic data in R: Generating synthetic data with high utility using synthpop (Thom Volker, Raoul Schram, Erik-Jan van Kesteren)](https://thomvolker.github.io/osf_synthetic/osf_synthetic_workshop.html)
* [Synthetic Data in Python](../ancillary-topics/synthetic_data_python)

### Acknowledgments
  
Special thanks to Iain Dove for sharing his knowledge of the `Synthetic data, a useful tool for ONS`, and to Wil Roberts, Elisha Mercado and Vicky Pickering for inspiring this tip!