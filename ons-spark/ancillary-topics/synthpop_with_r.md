## Synthpop: Syntheic Data in `R`

### 1. What is synthetic data?

According to the US Census Bureau, “Synthetic data are microdata records created to improve data utility while preventing disclosure of confidential respondent information. Synthetic data is created by statistically modelling original data and then using those models to generate new data values that reproduce the original data’s statistical properties. Users are unable to identify the information of the entities that provided the original data.”​

There are many situations in ONS where the generation of synthetic data could be used to improve outputs. These are listed in [Synthetic data at ONS](https://www.ons.gov.uk/methodology/methodologicalpublications/generalmethodology/onsworkingpaperseries/onsmethodologyworkingpaperseriesnumber16syntheticdatapilot) and they include:

* provision of microdata to users
* testing systems
* developing systems or methods in non-secure environments
* obtaining non-disclosive data from data suppliers
* teaching – a useful way to promote the use of ONS data sources

**Methods for Generating Synthetic Data in R**

We will look at two methods for generating synthetic data:

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
**1. Uniform Distribution**: The uniform distribution generates data where each value within a specified range is equally likely. This is useful for simulating data with a consistent spread.

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

```plaintext
    Min.  1st Qu.   Median     Mean  3rd Qu.     Max. 

0.001137 0.268837 0.521491 0.514048 0.756667 0.996996 

```
**2. Normal Distribution**: The normal distribution, or Gaussian distribution, is useful for simulating data that clusters around a mean. This is common in many natural phenomena.

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

```plaintext
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 

  36.66   47.27   49.92   49.93   52.73   63.32 

```
**3. Poisson Distribution**: The Poisson distribution is used for count data, where you are counting the number of events in a fixed interval of time or space. Typical examples is simulating the number of visits to a government website per day.

[`rpois()`](https://www.rdocumentation.org/packages/stats/versions/3.6.2/topics/Poisson) generates count data, useful for modeling the number of events in a fixed interval.
````{tabs}

```{code-tab} r R

poisson_dist <- rpois(n = 1000,  
                      lambda = 4)  

hist(poisson_dist, main="Poisson Distribution Example", xlab="Number of Events")
summary(poisson_dist)

```
````

```plaintext
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 

  0.000   2.000   4.000   3.921   5.000  12.000 

```
**4. Binomial Distribution**: The binomial distribution is useful for simulating the number of successes in a fixed number of trials, each with the same probability of success.

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

```plaintext
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 

  0.000   3.000   4.000   3.939   5.000  10.000 

```
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

```plaintext
gender

  F   M 

502 498 

gender

    F     M 

0.502 0.498 

```
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

```plaintext
marriage_status

Divorced  Married   Single  Widowed 

     105      518      329       48 

marriage_status

Divorced  Married   Single  Widowed 

   0.105    0.518    0.329    0.048 

```
Here is the explanation of the code snippet above:

1. Setting a Seed (`set.seed`):

* `set.seed(12345)`: Sets the seed for random number generation. The number `12345` can be any integer. Using the same seed will produce the same sequence of random numbers each time the code is run.
2. Random Sampling (`sample`):

* `x`: The vector of elements to choose from.
* `size`: The number of observations to generate.
* `replace`: Whether sampling is with replacement (`TRUE`) or without replacement (`FALSE`).
3. Weighted Sampling (`sample` with `prob`):

* `prob`: A vector of probabilities corresponding to the likelihood of each element in `x` being selected. The probabilities must sum to `1`.

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

```plaintext
   uniform_dist normal_dist poisson_dist binomial_dist gender marriage_status

          <num>       <num>        <int>         <int> <char>          <char>

1:    0.7209039    44.31870            3             3      M          Single

2:    0.8757732    40.13225            6             4      M         Married

3:    0.7609823    51.93886            3             6      M         Married

4:    0.8861246    46.24811            5             3      F         Married

5:    0.4564810    63.32293            5             3      M         Married

6:    0.1663718    49.34822            5             6      F        Divorced

  uniform_dist       normal_dist     poisson_dist    binomial_dist   

 Min.   :0.001137   Min.   :36.66   Min.   : 0.000   Min.   : 0.000  

 1st Qu.:0.268837   1st Qu.:47.27   1st Qu.: 2.000   1st Qu.: 3.000  

 Median :0.521491   Median :49.92   Median : 4.000   Median : 4.000  

 Mean   :0.514048   Mean   :49.93   Mean   : 3.921   Mean   : 3.939  

 3rd Qu.:0.756667   3rd Qu.:52.73   3rd Qu.: 5.000   3rd Qu.: 5.000  

 Max.   :0.996996   Max.   :63.32   Max.   :12.000   Max.   :10.000  

    gender          marriage_status   

 Length:1000        Length:1000       

 Class :character   Class :character  

 Mode  :character   Mode  :character  

                                      

                                      

                                      

Classes 'data.table' and 'data.frame':	1000 obs. of  6 variables:

 $ uniform_dist   : num  0.721 0.876 0.761 0.886 0.456 ...

 $ normal_dist    : num  44.3 40.1 51.9 46.2 63.3 ...

 $ poisson_dist   : int  3 6 3 5 5 5 7 2 2 6 ...

 $ binomial_dist  : int  3 4 6 3 3 6 6 4 7 6 ...

 $ gender         : chr  "M" "M" "M" "F" ...

 $ marriage_status: chr  "Single" "Married" "Married" "Married" ...

 - attr(*, ".internal.selfref")=<externalptr> 

```
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

```plaintext
Classes 'data.table' and 'data.frame':	1000 obs. of  6 variables:

 $ uniform_dist   : num  0.721 0.876 0.761 0.886 0.456 ...

 $ normal_dist    : num  44.3 40.1 51.9 46.2 63.3 ...

 $ poisson_dist   : int  3 6 3 5 5 5 7 2 2 6 ...

 $ binomial_dist  : int  3 4 6 3 3 6 6 4 7 6 ...

 $ gender         : Factor w/ 2 levels "F","M": 2 2 2 1 2 1 2 2 2 1 ...

 $ marriage_status: Factor w/ 4 levels "Divorced","Married",..: 3 2 2 2 2 1 2 4 1 2 ...

 - attr(*, ".internal.selfref")=<externalptr> 

```
This combined synthetic dataset can now be used for various analyses, simulations, and testing purposes. It provides a comprehensive representation of both numerical and categorical variables, making it suitable for a wide range of applications.

These snippets provide a starting point for generating synthetic categorical data using random and weighted sampling with reproducibility. You can adjust the parameters to fit the specific characteristics of the population you are modeling. For more detailed information on the `sample` function, refer to the [R documentation](https://www.rdocumentation.org/packages/base/versions/3.6.2/topics/sample).

### 3. Generating synthetic data using `synthpop`

In the guidiance, we will work with the R package `synthpop` [(Nowok, Raab, and Dibben 2016)](https://www.synthpop.org.uk/get-started.html), which is one of the most advanced and dedicated packages in R to create synthetic data.


Synthpop generates synthetic data by modeling the relationships in the original dataset and then simulating new data that preserves the statistical properties of the original, while reducing disclosure risk.

#### 3.1 Setting up `synthpop` and required packages
````{tabs}

```{code-tab} r R

install.packages("synthpop")

```
````

```plaintext
package 'synthpop' successfully unpacked and MD5 sums checked



The downloaded binary packages are in

	C:\Users\njobud\AppData\Local\Temp\RtmpEpyKPm\downloaded_packages

```


This will install `synthpop` and its dependencies from [ONS Artifactory](https://onsart-01/ui/login/).


First clear the workspace and load packages using the `library()` function.


````{tabs}

```{code-tab} r R

# Clean out workspace
rm(list = ls())

```
````

````{tabs}

```{code-tab} r R

library(synthpop)
library(dplyr)
library(readr)
library(janitor)
library(magrittr)
library(data.table)

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

**Examine your data**  
Start with a modest number of variables (8-12) to understand `synthpop`. If your data have more variables, make a selection. The package is intended for large datasets (at least `500` observations).

Use the [`codebook.syn()`](https://www.rdocumentation.org/packages/synthpop/versions/1.8-0/topics/codebook.syn) function to examine the features relevant to synthesising:



In this section, we will guide you through a sample synthesis using the `synthpop` package in R. This example uses the [`SD2011` dataset](https://www.rdocumentation.org/packages/synthpop/versions/1.8-0/topics/SD2011) sample, provided with the `synthpop` package and [demonstrates how to generate synthetic data](https://www.synthpop.org.uk/assets/firstsynthesis.r) with a smaller subset of variables. The SD2011 dataset is a social diagnosis survey from 2011, included as a sample dataset in the synthpop package.


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

```plaintext
[1] 5000   35

```

````{tabs}

```{code-tab} r R

# Show summary information for a subset of variables; here we show only the first five variables for brevity.
codebook.syn(SD2011[, 1:5])$tab

```
````

```plaintext
   variable   class nmiss perctmiss ndistinct           details

1       sex  factor     0      0.00         2   'MALE' 'FEMALE'

2       age numeric     0      0.00        79    Range: 16 - 97

3     agegr  factor     4      0.08         6 See table in labs

4 placesize  factor     0      0.00         6 See table in labs

5    region  factor     0      0.00        16 See table in labs

```
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

```plaintext
     sex age agegr                  edu          socprof income marital

1 FEMALE  57 45-59   VOCATIONAL/GRAMMAR          RETIRED    800 MARRIED

2   MALE  20 16-24   VOCATIONAL/GRAMMAR PUPIL OR STUDENT    350  SINGLE

3 FEMALE  18 16-24   VOCATIONAL/GRAMMAR PUPIL OR STUDENT     NA  SINGLE

4 FEMALE  78   65+ PRIMARY/NO EDUCATION          RETIRED    900 WIDOWED

5 FEMALE  54 45-59   VOCATIONAL/GRAMMAR    SELF-EMPLOYED   1500 MARRIED

6   MALE  20 16-24            SECONDARY PUPIL OR STUDENT     -8  SINGLE

     sex            age          agegr                            edu      

 MALE  :2182   Min.   :16.00   16-24: 702   PRIMARY/NO EDUCATION    : 962  

 FEMALE:2818   1st Qu.:32.00   25-34: 726   VOCATIONAL/GRAMMAR      :1613  

               Median :49.00   35-44: 748   SECONDARY               :1482  

               Mean   :47.68   45-59:1361   POST-SECONDARY OR HIGHER: 936  

               3rd Qu.:61.00   60-64: 516   NA's                    :   7  

               Max.   :97.00   65+  : 943                                  

                               NA's :   4                                  

                        socprof         income                    marital    

 RETIRED                    :1241   Min.   :   -8   SINGLE            :1253  

 EMPLOYED IN PRIVATE SECTOR : 994   1st Qu.:  700   MARRIED           :2979  

 EMPLOYED IN PUBLIC SECTOR  : 600   Median : 1200   WIDOWED           : 531  

 PUPIL OR STUDENT           : 548   Mean   : 1411   DIVORCED          : 199  

 OTHER ECONOMICALLY INACTIVE: 444   3rd Qu.: 1800   LEGALLY SEPARATED :   7  

 (Other)                    :1140   Max.   :16000   DE FACTO SEPARATED:  22  

 NA's                       :  33   NA's   :683     NA's              :   9  

```
3. **Checking for missing values**
The following code checks for negative and missing values in the income variable.
````{tabs}

```{code-tab} r R

# Check for negative income values
table(mydata$income[mydata$income < 0], useNA = "ifany")

```
````

```plaintext


  -8 <NA> 

 603  683 

```
4. **Synthesise data**  
 * Use the `syn()` function from the `synthpop` package to create a synthetic version of your data, using the default settings.
 * Fix the random seed before generating the synthetic data, so that you can compare your output with our output.
````{tabs}

```{code-tab} r R

set.seed(123)

# Synthesise data, handling -8 as a missing value for income
mysyn <- syn(mydata, cont.na = list(income = -8))

```
````

```plaintext


Synthesis

-----------

 sex age agegr edu socprof income marital

```
We have now created the object `mysyn`, which is of class `synds` and contains several pieces of information related to the synthetic data. We will quickly go over the most important output.
````{tabs}

```{code-tab} r R

summary(mysyn)

```
````

```plaintext
Synthetic object with one synthesis using methods:

     sex      age    agegr      edu  socprof   income  marital 

"sample"   "cart"   "cart"   "cart"   "cart"   "cart"   "cart" 



     sex            age          agegr                            edu      

 MALE  :2188   Min.   :16.00   16-24: 685   PRIMARY/NO EDUCATION    : 958  

 FEMALE:2812   1st Qu.:32.00   25-34: 745   VOCATIONAL/GRAMMAR      :1612  

               Median :49.00   35-44: 757   SECONDARY               :1464  

               Mean   :47.62   45-59:1381   POST-SECONDARY OR HIGHER: 964  

               3rd Qu.:61.00   60-64: 500   NA's                    :   2  

               Max.   :97.00   65+  : 930                                  

                               NA's :   2                                  

                        socprof         income                    marital    

 RETIRED                    :1188   Min.   :   -8   SINGLE            :1254  

 EMPLOYED IN PRIVATE SECTOR :1004   1st Qu.:  700   MARRIED           :3022  

 EMPLOYED IN PUBLIC SECTOR  : 600   Median : 1200   WIDOWED           : 504  

 PUPIL OR STUDENT           : 528   Mean   : 1419   DIVORCED          : 183  

 OTHER ECONOMICALLY INACTIVE: 433   3rd Qu.: 1800   LEGALLY SEPARATED :  10  

 (Other)                    :1213   Max.   :15000   DE FACTO SEPARATED:  16  

 NA's                       :  34   NA's   :693     NA's              :  11  

```
The following output is produced by the `compare()` function. It provides statistical measures and significance tests that indicate how well the synthetic data replicates the distributions and relationships found in the original data. These results help you assess the quality of the synthetic data.
````{tabs}

```{code-tab} r R

# Compare the synthetic data with the original data
compare(mysyn, mydata, stat = "counts")

```
````

```plaintext
Calculations done for sex 

Calculations done for age 

Calculations done for agegr 

Calculations done for edu 

Calculations done for socprof 

Calculations done for income 

Calculations done for marital 



Comparing counts observed with synthetic



Press return for next variable(s): 



Selected utility measures:

            pMSE   S_pMSE df

sex     0.000000 0.029265  1

age     0.000028 0.554077  4

agegr   0.000042 0.554113  6

edu     0.000083 1.654516  4

socprof 0.000143 1.274998  9

income  0.000104 1.391121  6

marital 0.000084 1.119933  6

```
5. **Export synthetic data**
````{tabs}

```{code-tab} r R

# Export synthetic data to CSV format
write.syn(mysyn, filename = "mysyn_SD2001", filetype = "csv")

```
````

```plaintext
Synthetic data exported as csv file(s).

Information on synthetic data written to

  D:/dapcats_guidance/ons-spark/synthesis_info_mysyn_SD2001.txt 

```
6. **Explore synthetic data**  
After generating synthetic data using the `synthpop` package, it is important to explore and understand the structure and components of the synthetic data object. This section provides guidance on how to explore the synthetic data object and perform additional comparisons.

**I. Retrieve component names**
````{tabs}

```{code-tab} r R

names(mysyn)

```
````

```plaintext
 [1] "call"             "m"                "syn"              "method"          

 [5] "visit.sequence"   "predictor.matrix" "smoothing"        "event"           

 [9] "denom"            "proper"           "n"                "k"               

[13] "rules"            "rvalues"          "cont.na"          "semicont"        

[17] "drop.not.used"    "drop.pred.only"   "models"           "seed"            

[21] "var.lab"          "val.lab"          "obs.vars"         "numtocat"        

[25] "catgroups"       

```
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

```plaintext
     sex      age    agegr      edu  socprof   income  marital 

"sample"   "cart"   "cart"   "cart"   "cart"   "cart"   "cart" 

        sex age agegr edu socprof income marital

sex       0   0     0   0       0      0       0

age       1   0     0   0       0      0       0

agegr     1   1     0   0       0      0       0

edu       1   1     1   0       0      0       0

socprof   1   1     1   1       0      0       0

income    1   1     1   1       1      0       0

marital   1   1     1   1       1      1       0

    sex     age   agegr     edu socprof  income marital 

      1       2       3       4       5       6       7 

$sex

[1] NA



$age

[1] NA



$agegr

[1] NA



$edu

[1] NA



$socprof

[1] NA



$income

[1] -8



$marital

[1] NA



[1] 161401295

```
**IV. Additional comparisons**  
To further validate the synthetic data, you can perform additional comparisons between the synthetic and original data using the [`multi.compare()`](https://www.rdocumentation.org/packages/synthpop/versions/1.8-0/topics/multi.compare) function.
````{tabs}

```{code-tab} r R

# Additional comparisons
multi.compare(mysyn, mydata, var = "marital", by = "sex")
multi.compare(mysyn, mydata, var = "income", by = "agegr")
multi.compare(mysyn, mydata, var = "income", by = "edu", cont.type = "boxplot")

```
````

```plaintext


Plots of  marital  by  sex 

Numbers in each plot (observed data):



sex

  MALE FEMALE 

  2182   2818 



Plots of  income  by  agegr 

Numbers in each plot (observed data):



agegr

16-24 25-34 35-44 45-59 60-64   65+  <NA> 

  702   726   748  1361   516   943     4 



Plots of  income  by  edu 

Numbers in each plot (observed data):



edu

    PRIMARY/NO EDUCATION       VOCATIONAL/GRAMMAR                SECONDARY 

                     962                     1613                     1482 

POST-SECONDARY OR HIGHER                     <NA> 

                     936                        7 

```
* `multi.compare(mysyn, mydata, var = "marital", by = "sex")`: Compares the distribution of the `marital` variable by `sex` between the synthetic and original data.
* `multi.compare(mysyn, mydata, var = "income", by = "agegr")`: Compares the distribution of the `income` variable by `age group` between the synthetic and original data.
* `multi.compare(mysyn, mydata, var = "income", by = "edu", cont.type = "boxplot")`: Compares the distribution of the `income` variable by `education level` using boxplots between the synthetic and original data.

These steps help ensure that the synthetic data accurately reflects the structure and relationships present in the original data, making it suitable for analysis while protecting the privacy of the original data.

This figure shows examples of visuals from using the `multi.compare()` function:

```{figure} ../images/Rplot_Observed_versus_synthetic_data.png
---
width: 100%
name: MultiCompareDiagram
alt: Diagram showing visuals from using the `multi.compare()` function
---
Observed_versus_synthetic_data
```

### Case 2: Synthesise census teaching data

In this section, we will guide you through the process of synthesising another case study as presented by Iain Dove, a census teaching data using the `synthpop` package in R. This demo is an alternative to the synthesis of `SD2011` in `Case 1`. This involves loading the data, creating a subset, synthesising the data, and comparing the synthetic data with the original data.

#### Step-by-step guide

1. **Loading the data**
````{tabs}

```{code-tab} r R

# Load the configuration and set the path to the census teaching data
# config <- yaml::yaml.load_file("/home/cdsw/ons-spark/config.yaml")
config <- yaml::yaml.load_file("D:/dapcats_guidance/ons-spark/config.yaml")
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

```plaintext
Variable residence_type has only one value so its method has been changed to "constant".



Synthesis

-----------

 person_id region residence_type family_composition population_base sex age marital_status student country_of_birth

 health ethnic_group religion economic_activity occupation industry hours_worked_per_week approximated_social_grade

```


You have created a synthetic data object `synthetic_census_teaching_data` of class `synds`, which is a list with a number of components including the synthesised data and information on how they were created.


````{tabs}

```{code-tab} r R

# Check the class of the synthetic data object
class(synthetic_census_teaching_data)

```
````

```plaintext
[1] "synds"

```
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

```plaintext
Synthetic object with one synthesis using methods:

                person_id                    region            residence_type 

                 "sample"                  "sample"                "constant" 

       family_composition           population_base                       sex 

                 "sample"                  "sample"                  "sample" 

                      age            marital_status                   student 

                 "sample"                  "sample"                  "sample" 

         country_of_birth                    health              ethnic_group 

                 "sample"                  "sample"                  "sample" 

                 religion         economic_activity                occupation 

                 "sample"                  "sample"                  "sample" 

                 industry     hours_worked_per_week approximated_social_grade 

                 "sample"                  "sample"                  "sample" 



   person_id          region          residence_type     family_composition

 Min.   :7394484   Length:100000      Length:100000      Min.   :-9.000    

 1st Qu.:7419840   Class :character   Class :character   1st Qu.: 2.000    

 Median :7445549   Mode  :character   Mode  :character   Median : 2.000    

 Mean   :7445451                                         Mean   : 2.231    

 3rd Qu.:7470875                                         3rd Qu.: 3.000    

 Max.   :7910302                                         Max.   : 6.000    

 population_base      sex             age       marital_status     student    

 Min.   :1.000   Min.   :1.000   Min.   :1.00   Min.   :1.000   Min.   :1.00  

 1st Qu.:1.000   1st Qu.:1.000   1st Qu.:2.00   1st Qu.:1.000   1st Qu.:2.00  

 Median :1.000   Median :2.000   Median :4.00   Median :2.000   Median :2.00  

 Mean   :1.016   Mean   :1.502   Mean   :3.93   Mean   :1.849   Mean   :1.78  

 3rd Qu.:1.000   3rd Qu.:2.000   3rd Qu.:6.00   3rd Qu.:2.000   3rd Qu.:2.00  

 Max.   :3.000   Max.   :2.000   Max.   :8.00   Max.   :5.000   Max.   :2.00  

 country_of_birth      health        ethnic_group       religion     

 Min.   :-9.0000   Min.   :-9.000   Min.   :-9.000   Min.   :-9.000  

 1st Qu.: 1.0000   1st Qu.: 1.000   1st Qu.: 1.000   1st Qu.: 1.000  

 Median : 1.0000   Median : 2.000   Median : 1.000   Median : 2.000  

 Mean   : 0.9973   Mean   : 1.672   Mean   : 1.132   Mean   : 2.349  

 3rd Qu.: 1.0000   3rd Qu.: 2.000   3rd Qu.: 1.000   3rd Qu.: 2.000  

 Max.   : 2.0000   Max.   : 5.000   Max.   : 5.000   Max.   : 9.000  

 economic_activity   occupation        industry      hours_worked_per_week

 Min.   :-9.0000   Min.   :-9.000   Min.   :-9.000   Min.   :-9.000       

 1st Qu.: 1.0000   1st Qu.:-9.000   1st Qu.:-9.000   1st Qu.:-9.000       

 Median : 1.0000   Median : 4.000   Median : 4.000   Median :-9.000       

 Mean   : 0.6395   Mean   : 1.482   Mean   : 2.474   Mean   :-3.485       

 3rd Qu.: 5.0000   3rd Qu.: 7.000   3rd Qu.: 8.000   3rd Qu.: 3.000       

 Max.   : 9.0000   Max.   : 9.000   Max.   :12.000   Max.   : 4.000       

 approximated_social_grade

 Min.   :-9.000           

 1st Qu.: 1.000           

 Median : 2.000           

 Mean   : 0.272           

 3rd Qu.: 3.000           

 Max.   : 4.000           

```
5. **Comparison with real data**

`synthpop` contains functions to assess the accuracy of the synthetic data. Univariate measures are quite easy to satisfy, but relationships between multiple variables are much more difficult.
````{tabs}

```{code-tab} r R

# Compare synthetic data with real data
synthpop::compare(object = synthetic_census_teaching_data,
                  data = small_census_teaching_data)

```
````

```plaintext
Calculations done for person_id 

Calculations done for region 

Calculations done for residence_type 

Calculations done for family_composition 

Calculations done for population_base 

Only 2 groups produced for sex even after changing method.

Calculations done for sex 

Calculations done for age 

Calculations done for marital_status 

Only 2 groups produced for student even after changing method.

Calculations done for student 

Calculations done for country_of_birth 

Calculations done for health 

Grouping changed from 'quantile' to  'equal' in function numtocat.syn for ethnic_group because only 2  groups were produced

Calculations done for ethnic_group 

Calculations done for religion 

Calculations done for economic_activity 

Calculations done for occupation 

Calculations done for industry 

Calculations done for hours_worked_per_week 

Calculations done for approximated_social_grade 



Comparing percentages observed with synthetic



Press return for next variable(s): 

Press return for next variable(s): 

Press return for next variable(s): 

Press return for next variable(s): 



Selected utility measures:

                           pMSE   S_pMSE df

person_id                 1e-06 0.469578  4

region                    3e-06 0.642902  7

residence_type            0e+00      NaN  0

family_composition        0e+00 0.002530  2

population_base           1e-06 1.082039  2

sex                       3e-06 4.329842  1

age                       5e-06 2.061816  4

marital_status            2e-06 0.783082  4

student                   0e+00 0.013090  1

country_of_birth          1e-06 0.752260  2

health                    1e-06 0.750641  2

ethnic_group              1e-06 0.780810  2

religion                  6e-06 4.579195  2

economic_activity         0e+00 0.189024  3

occupation                4e-06 2.019962  3

industry                  0e+00 0.122714  3

hours_worked_per_week     2e-06 0.810361  4

approximated_social_grade 2e-06 0.793122  4

```
#### Relationships between multiple variables

Relationships between multiple variables are more complex to model. The `synthpop` default uses a saturated model, which can be challenging for large datasets. Here, we use an input `CSV` file to specify which variables are related.

1. **Load the input CSV file**
````{tabs}

```{code-tab} r R

#config <- yaml::yaml.load_file("/home/cdsw/ons-spark/config.yaml")
config <- yaml::yaml.load_file("D:/dapcats_guidance/ons-spark/config.yaml")
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

```plaintext


Variable(s): region have been changed for synthesis from character to factor.



Variable(s): age, marital_status, student, ethnic_group, religion, economic_activity, occupation, hours_worked_per_week, approximated_social_grade numeric but with only 10 or fewer distinct values turned into factor(s) for synthesis.



Variable residence_type has only one value so its method has been changed to "constant".



Method "cart" is not valid for a variable without predictors (region)

Method has been changed to "sample"





Method "cart" is not valid for a variable without predictors (family_composition)

Method has been changed to "sample"





Method "cart" is not valid for a variable without predictors (population_base)

Method has been changed to "sample"





Method "cart" is not valid for a variable without predictors (sex)

Method has been changed to "sample"





Method "cart" is not valid for a variable without predictors (age)

Method has been changed to "sample"





Method "cart" is not valid for a variable without predictors (country_of_birth)

Method has been changed to "sample"





Method "cart" is not valid for a variable without predictors (health)

Method has been changed to "sample"





Synthesis

-----------

 person_id region residence_type family_composition population_base sex age marital_status student country_of_birth

 health ethnic_group religion economic_activity occupation industry hours_worked_per_week approximated_social_grade

```
#### Comparison with real data

To compare the synthetic and original data, use:
````{tabs}

```{code-tab} r R

# Compare synthetic data with real data
synthpop::compare(data = small_census_teaching_data,
                  object = synthetic_census_teaching_data_2)

```
````

```plaintext
Calculations done for person_id 

Calculations done for region 

Calculations done for residence_type 

Calculations done for family_composition 

Calculations done for population_base 

Only 2 groups produced for sex even after changing method.

Calculations done for sex 

Calculations done for age 

Calculations done for marital_status 

Only 2 groups produced for student even after changing method.

Calculations done for student 

Calculations done for country_of_birth 

Calculations done for health 

Grouping changed from 'quantile' to  'equal' in function numtocat.syn for ethnic_group because only 2  groups were produced

Calculations done for ethnic_group 

Calculations done for religion 

Calculations done for economic_activity 

Calculations done for occupation 

Calculations done for industry 

Calculations done for hours_worked_per_week 

Calculations done for approximated_social_grade 



Comparing percentages observed with synthetic



Press return for next variable(s): 

Press return for next variable(s): 

Press return for next variable(s): 

Press return for next variable(s): 



Selected utility measures:

                             pMSE    S_pMSE df

person_id                 2.0e-06  0.935440  4

region                    1.0e-06  0.176441  7

residence_type            0.0e+00       NaN  0

family_composition        1.0e-06  0.593263  2

population_base           0.0e+00  0.217593  2

sex                       6.0e-06 10.162976  1

age                       4.0e-06  1.633541  4

marital_status            3.0e-06  1.106891  4

student                   2.0e-06  3.309619  1

country_of_birth          1.0e-06  0.558046  2

health                    0.0e+00  0.111749  2

ethnic_group              1.0e-06  0.891315  2

religion                  1.0e-06  0.871088  2

economic_activity         2.0e-06  0.895892  3

occupation                7.9e-05 42.373300  3

industry                  3.7e-05 19.723638  3

hours_worked_per_week     1.0e-05  4.089040  4

approximated_social_grade 5.2e-05 20.624287  4

```
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

```plaintext
Key: <economic_activity, hours_worked_per_week>

    economic_activity hours_worked_per_week orig_count

               <fctr>                <fctr>      <int>

 1:                -9                    -9      19948

 2:                 1                     1       2649

 3:                 1                     2       7813

 4:                 1                     3      24310

 5:                 1                     4       4117

 6:                 2                     1        692

 7:                 2                     2       1227

 8:                 2                     3       2966

 9:                 2                     4       1468

10:                 3                    -9       3388

11:                 4                    -9        557

12:                 4                     1       1238

13:                 4                     2        413

14:                 4                     3        307

15:                 4                     4         33

16:                 5                    -9      16588

17:                 6                    -9       4114

18:                 7                    -9       3266

19:                 8                    -9       3324

20:                 9                    -9       1582

    economic_activity hours_worked_per_week orig_count

```

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

```plaintext
Key: <economic_activity, hours_worked_per_week>

    economic_activity hours_worked_per_week synth2_count

                <num>                 <num>        <int>

 1:                -9                    -9        20103

 2:                 1                     1         2602

 3:                 1                     2         8056

 4:                 1                     3        24172

 5:                 1                     4         3966

 6:                 2                     1          715

 7:                 2                     2         1295

 8:                 2                     3         2947

 9:                 2                     4         1505

10:                 3                    -9         3405

11:                 4                    -9          375

12:                 4                     1         1110

13:                 4                     2          407

14:                 4                     3          536

15:                 4                     4           76

16:                 5                    -9        16446

17:                 6                    -9         4155

18:                 7                    -9         3187

19:                 8                    -9         3326

20:                 9                    -9         1616

    economic_activity hours_worked_per_week synth2_count

```

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

```plaintext
Key: <economic_activity, hours_worked_per_week>

    economic_activity hours_worked_per_week synth1_count

                <int>                 <int>        <int>

 1:                -9                    -9        10636

 2:                -9                     1          859

 3:                -9                     2         1860

 4:                -9                     3         5453

 5:                -9                     4         1123

 6:                 1                    -9        20467

 7:                 1                     1         1724

 8:                 1                     2         3692

 9:                 1                     3        10691

10:                 1                     4         2226

11:                 2                    -9         3378

12:                 2                     1          311

13:                 2                     2          612

14:                 2                     3         1713

15:                 2                     4          326

16:                 3                    -9         1775

17:                 3                     1          164

18:                 3                     2          342

19:                 3                     3          891

20:                 3                     4          179

21:                 4                    -9         1360

22:                 4                     1          122

23:                 4                     2          231

24:                 4                     3          744

25:                 4                     4          147

26:                 5                    -9         8965

27:                 5                     1          796

28:                 5                     2         1514

29:                 5                     3         4595

30:                 5                     4          974

31:                 6                    -9         2141

32:                 6                     1          191

33:                 6                     2          379

34:                 6                     3         1145

35:                 6                     4          239

36:                 7                    -9         1718

37:                 7                     1          138

38:                 7                     2          302

39:                 7                     3          906

40:                 7                     4          179

41:                 8                    -9         1721

42:                 8                     1          143

43:                 8                     2          305

44:                 8                     3          879

45:                 8                     4          191

46:                 9                    -9          814

47:                 9                     1           76

48:                 9                     2          144

49:                 9                     3          430

50:                 9                     4           89

    economic_activity hours_worked_per_week synth1_count

```

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

```plaintext
[1] 89.2

[1] 1987.4

[1] 13.91346

[1] Inf

```


This guide provides a concise overview of synthesizing census teaching data with relationships between multiple variables using the `synthpop` package in R. For more detailed information, refer to the [synthpop documentation](https://www.synthpop.org.uk/get-started.html).

### References

* [Synthetic data at ONS](https://www.ons.gov.uk/methodology/methodologicalpublications/generalmethodology/onsworkingpaperseries/onsmethodologyworkingpaperseriesnumber16syntheticdatapilot)
* [`synthpop` (Nowok, Raab, and Dibben 2016)](https://www.synthpop.org.uk/get-started.html)
* [Synthetic data in R: Generating synthetic data with high utility using synthpop (Thom Volker, Raoul Schram, Erik-Jan van Kesteren)](https://thomvolker.github.io/osf_synthetic/osf_synthetic_workshop.html)
* [Synthetic Data in Python](../ancillary-topics/synthetic_data_python)

### Acknowledgments
  
Special thanks to Iain Dove for sharing his knowledge of the `Synthetic data, a useful tool for ONS`, and to Wil Roberts, Elisha Mercado and Vicky Pickering for inspiring this tip.