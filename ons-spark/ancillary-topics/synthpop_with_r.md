## Synthpop: Syntheic Data in `R`

### 1. What is synthetic data?

According to the US Census Bureau, “Synthetic data are microdata records created to improve data utility while preventing disclosure of confidential respondent information. Synthetic data is created by statistically modelling original data and then using those models to generate new data values that reproduce the original data’s statistical properties. Users are unable to identify the information of the entities that provided the original data.”​

There are many situations in ONS where the generation of synthetic data could be used to improve outputs. These are listed in [Synthetic data at ONS](https://www.ons.gov.uk/methodology/methodologicalpublications/generalmethodology/onsworkingpaperseries/onsmethodologyworkingpaperseriesnumber16syntheticdatapilot) and they include:

* provision of microdata to users
* testing systems
* developing systems or methods in non-secure environments
* obtaining non-disclosive data from data suppliers
* teaching – a useful way to promote the use of ONS data sources

**Methods for Generating Synthetic Data in `R`**

We will look at two methods for generating synthetic data:

1. Writing your own code
2. Using the `synthpop` `R` package

### 2. Generating synthetic data in `R` from scratch
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

In this section, we will generate synthetic numerical variables using different distributions in `R`. We will use the following distributions:

1. Uniform Distribution
2. Normal Distribution
3. Binomial Distribution
4. Poisson Distribution

Below are the `R` codes to generate these variables.

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

````{tabs}

```{code-tab} plaintext R Output
    Min.  1st Qu.   Median     Mean  3rd Qu.     Max. 
0.001137 0.268837 0.521491 0.514048 0.756667 0.996996 
```
````
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

````{tabs}

```{code-tab} plaintext R Output
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
  36.66   47.27   49.92   49.93   52.73   63.32 
```
````
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

````{tabs}

```{code-tab} plaintext R Output
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
  0.000   2.000   4.000   3.921   5.000  12.000 
```
````
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

````{tabs}

```{code-tab} plaintext R Output
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
  0.000   3.000   4.000   3.939   5.000  10.000 
```
````
#### 2.2 Character/factor variables

In this section, we will generate synthetic `character (factor)` variables using random sampling in `R`. These variables are useful for simulating categorical data, such as demographic information.

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

````{tabs}

```{code-tab} plaintext R Output
gender
  F   M 
502 498 
gender
    F     M 
0.502 0.498 
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

````{tabs}

```{code-tab} plaintext R Output
marriage_status
Divorced  Married   Single  Widowed 
     105      518      329       48 
marriage_status
Divorced  Married   Single  Widowed 
   0.105    0.518    0.329    0.048 
```
````
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

````{tabs}

```{code-tab} plaintext R Output
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
                                      
                                      
                                      
Classes ‘data.table’ and 'data.frame':	1000 obs. of  6 variables:
 $ uniform_dist   : num  0.721 0.876 0.761 0.886 0.456 ...
 $ normal_dist    : num  44.3 40.1 51.9 46.2 63.3 ...
 $ poisson_dist   : int  3 6 3 5 5 5 7 2 2 6 ...
 $ binomial_dist  : int  3 4 6 3 3 6 6 4 7 6 ...
 $ gender         : chr  "M" "M" "M" "F" ...
 $ marriage_status: chr  "Single" "Married" "Married" "Married" ...
 - attr(*, ".internal.selfref")=<externalptr> 
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

````{tabs}

```{code-tab} plaintext R Output
Classes ‘data.table’ and 'data.frame':	1000 obs. of  6 variables:
 $ uniform_dist   : num  0.721 0.876 0.761 0.886 0.456 ...
 $ normal_dist    : num  44.3 40.1 51.9 46.2 63.3 ...
 $ poisson_dist   : int  3 6 3 5 5 5 7 2 2 6 ...
 $ binomial_dist  : int  3 4 6 3 3 6 6 4 7 6 ...
 $ gender         : Factor w/ 2 levels "F","M": 2 2 2 1 2 1 2 2 2 1 ...
 $ marriage_status: Factor w/ 4 levels "Divorced","Married",..: 3 2 2 2 2 1 2 4 1 2 ...
 - attr(*, ".internal.selfref")=<externalptr> 
```
````
This combined synthetic dataset can now be used for various analyses, simulations, and testing purposes. It provides a comprehensive representation of both numerical and categorical variables, making it suitable for a wide range of applications.

These snippets provide a starting point for generating synthetic categorical data using random and weighted sampling with reproducibility. You can adjust the parameters to fit the specific characteristics of the population you are modeling. For more detailed information on the `sample` function, refer to the [R documentation](https://www.rdocumentation.org/packages/base/versions/3.6.2/topics/sample).

### 3. `synthpop` for generating synthetic data in `R`

In the guidiance, we will work with the `R` package `synthpop` [(Nowok, Raab, and Dibben 2016)](https://www.synthpop.org.uk/get-started.html), which is one of the most advanced and dedicated packages in `R` to create synthetic data.

#### 3.1 Install the `synthpop` package
````{tabs}

```{code-tab} r R

install.packages("synthpop")

```
````


This will install `synthpop` and its dependencies from [ONS Artifactory](https://onsart-01/ui/login/).


#### 3.2 Start `synthpop`

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
library(psych)
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

### 3.3 Case I: synthesise `synthpop` sample data `SD2011`

In most cases, you will be working with your own data, but for practice, you can use the sample data [`SD2011`](https://www.rdocumentation.org/packages/synthpop/versions/1.8-0/topics/SD2011) provided with the `synthpop` package.

**Read the data**  
Read the data you want to synthesise into `R`. You can use the `synthpop` function [`read.obs()`](https://www.rdocumentation.org/packages/synthpop/versions/1.9-0/topics/read.obs) to read data from other formats.

**Examine your data**  
Start with a modest number of variables (8-12) to understand `synthpop`. If your data have more variables, make a selection. The package is intended for large datasets (at least `500` observations).

Use the [`codebook.syn()`](https://www.rdocumentation.org/packages/synthpop/versions/1.8-0/topics/codebook.syn) function to examine the features relevant to synthesising:



#### 3.3.1 Sample synthesis using sample data `SD2011` provided with the `synthpop` package

In this section, we will guide you through a sample synthesis using the `synthpop` package in `R`. This example uses the [`SD2011` dataset](https://www.rdocumentation.org/packages/synthpop/versions/1.8-0/topics/SD2011), sample provided with the `synthpop` package and [demonstrates how to generate synthetic data](https://www.synthpop.org.uk/assets/firstsynthesis.r) with a smaller subset of variables.

1. **Explore the `SD2011` dataset**  
Here, we will use the `help()`, `dim()` and `codebook.syn()` methods to obtain the information about the SD2001, get the size of the dataframe, and the summary information about variables respectively.

```{r, results='hide'}
help(SD2011)
```
````{tabs}

```{code-tab} r R

dim(SD2011)

```
````

````{tabs}

```{code-tab} plaintext R Output
[1] 5000   35
```
````

````{tabs}

```{code-tab} r R

codebook.syn(SD2011)$tab

```
````

````{tabs}

```{code-tab} plaintext R Output
     variable     class nmiss perctmiss ndistinct
1         sex    factor     0      0.00         2
2         age   numeric     0      0.00        79
3       agegr    factor     4      0.08         6
4   placesize    factor     0      0.00         6
5      region    factor     0      0.00        16
6         edu    factor     7      0.14         4
7     eduspec    factor    20      0.40        27
8     socprof    factor    33      0.66         9
9    unempdur   numeric     0      0.00        30
10     income   numeric   683     13.66       406
11    marital    factor     9      0.18         6
12      mmarr   numeric  1350     27.00        12
13      ymarr   numeric  1320     26.40        74
14    msepdiv   numeric  4300     86.00        12
15    ysepdiv   numeric  4275     85.50        50
16         ls    factor     8      0.16         7
17    depress   numeric    89      1.78        22
18      trust    factor    37      0.74         3
19   trustfam    factor    11      0.22         3
20 trustneigh    factor    11      0.22         3
21      sport    factor    41      0.82         2
22   nofriend   numeric     0      0.00        44
23      smoke    factor    10      0.20         2
24     nociga   numeric     0      0.00        30
25   alcabuse    factor     7      0.14         2
26     alcsol    factor    82      1.64         2
27     workab    factor   438      8.76         2
28    wkabdur character     0      0.00        33
29    wkabint    factor    36      0.72         3
30 wkabintdur    factor  4697     93.94         5
31       emcc    factor  4714     94.28        17
32    englang    factor    15      0.30         3
33     height   numeric    35      0.70        64
34     weight   numeric    53      1.06        90
35        bmi   numeric    59      1.18      1387
                                                                            details
1                                                                   'MALE' 'FEMALE'
2                                                                    Range: 16 - 97
3                                                                 See table in labs
4                                                                 See table in labs
5                                                                 See table in labs
6                                                                 See table in labs
7                                                                 See table in labs
8                                                                 See table in labs
9                                                                    Range: -8 - 48
10                                                                Range: -8 - 16000
11                                                                See table in labs
12                                                                    Range: 1 - 12
13                                                               Range: 1937 - 2011
14                                                                    Range: 1 - 12
15                                                               Range: 1944 - 2011
16                                                                See table in labs
17                                                                    Range: 0 - 21
18 'MOST PEOPLE CAN BE TRUSTED' 'ONE CAN`T BE TOO CAREFUL' 'IT`S DIFFICULT TO TELL'
19                                                          'YES' 'NO' 'NO OPINION'
20                                                          'YES' 'NO' 'NO OPINION'
21                                                                       'YES' 'NO'
22                                                                   Range: -8 - 99
23                                                                       'YES' 'NO'
24                                                                   Range: -8 - 60
25                                                                       'YES' 'NO'
26                                                                       'YES' 'NO'
27                                                                       'YES' 'NO'
28                                                                    Max length: 2
29                               'YES, TO EU COUNTRY' 'YES, TO NON-EU COUNTRY' 'NO'
30                                                                See table in labs
31                                                                See table in labs
32                                                        'ACTIVE' 'PASSIVE' 'NONE'
33                                                                 Range: 116 - 202
34                                                                  Range: 37 - 150
35                                        Range: 12.962962962963 - 449.979730642764
```
````
2. **Select a subset of variables**
* After loading the data, inspect it to understand its structure and contents.
* You can review a `summary()` of all variables or use the `describe()` function from the `psych` package to generate descriptive statistics.
````{tabs}

```{code-tab} r R

# Select a subset of variables from SD2011 to reduce demo runtimes
mydata <- SD2011[, c(1, 3, 6, 8, 11, 17, 18, 19, 20, 10)]

```
````

````{tabs}

```{code-tab} r R

# Inspect the SD2011 dataset
head(mydata)

```
````

````{tabs}

```{code-tab} plaintext R Output
     sex agegr                  edu          socprof marital depress
1 FEMALE 45-59   VOCATIONAL/GRAMMAR          RETIRED MARRIED       6
2   MALE 16-24   VOCATIONAL/GRAMMAR PUPIL OR STUDENT  SINGLE       0
3 FEMALE 16-24   VOCATIONAL/GRAMMAR PUPIL OR STUDENT  SINGLE       0
4 FEMALE   65+ PRIMARY/NO EDUCATION          RETIRED WIDOWED      16
5 FEMALE 45-59   VOCATIONAL/GRAMMAR    SELF-EMPLOYED MARRIED       4
6   MALE 16-24            SECONDARY PUPIL OR STUDENT  SINGLE       5
                       trust trustfam trustneigh income
1   ONE CAN`T BE TOO CAREFUL      YES         NO    800
2     IT`S DIFFICULT TO TELL      YES NO OPINION    350
3 MOST PEOPLE CAN BE TRUSTED      YES         NO     NA
4   ONE CAN`T BE TOO CAREFUL       NO        YES    900
5   ONE CAN`T BE TOO CAREFUL      YES        YES   1500
6   ONE CAN`T BE TOO CAREFUL      YES NO OPINION     -8
```
````

````{tabs}

```{code-tab} r R

# Inspect the SD2011 dataset
tail(mydata)

```
````

````{tabs}

```{code-tab} plaintext R Output
        sex agegr                  edu                     socprof marital
4995   MALE   65+ PRIMARY/NO EDUCATION                     RETIRED MARRIED
4996   MALE 45-59            SECONDARY  EMPLOYED IN PRIVATE SECTOR MARRIED
4997 FEMALE 45-59            SECONDARY OTHER ECONOMICALLY INACTIVE MARRIED
4998 FEMALE 16-24            SECONDARY  EMPLOYED IN PRIVATE SECTOR  SINGLE
4999   MALE 25-34            SECONDARY   EMPLOYED IN PUBLIC SECTOR MARRIED
5000   MALE 35-44 PRIMARY/NO EDUCATION OTHER ECONOMICALLY INACTIVE  SINGLE
     depress                      trust trustfam trustneigh income
4995      12   ONE CAN`T BE TOO CAREFUL       NO         NO     -8
4996       2   ONE CAN`T BE TOO CAREFUL      YES        YES   2500
4997       5   ONE CAN`T BE TOO CAREFUL      YES        YES     -8
4998       1   ONE CAN`T BE TOO CAREFUL      YES        YES   1000
4999       1   ONE CAN`T BE TOO CAREFUL      YES         NO   2600
5000       3 MOST PEOPLE CAN BE TRUSTED       NO         NO     NA
```
````

````{tabs}

```{code-tab} r R

# Get a summary of the dataset
summary(mydata)

```
````

````{tabs}

```{code-tab} plaintext R Output
     sex         agegr                            edu      
 MALE  :2182   16-24: 702   PRIMARY/NO EDUCATION    : 962  
 FEMALE:2818   25-34: 726   VOCATIONAL/GRAMMAR      :1613  
               35-44: 748   SECONDARY               :1482  
               45-59:1361   POST-SECONDARY OR HIGHER: 936  
               60-64: 516   NA's                    :   7  
               65+  : 943                                  
               NA's :   4                                  
                        socprof                   marital        depress      
 RETIRED                    :1241   SINGLE            :1253   Min.   : 0.000  
 EMPLOYED IN PRIVATE SECTOR : 994   MARRIED           :2979   1st Qu.: 1.000  
 EMPLOYED IN PUBLIC SECTOR  : 600   WIDOWED           : 531   Median : 4.000  
 PUPIL OR STUDENT           : 548   DIVORCED          : 199   Mean   : 4.493  
 OTHER ECONOMICALLY INACTIVE: 444   LEGALLY SEPARATED :   7   3rd Qu.: 7.000  
 (Other)                    :1140   DE FACTO SEPARATED:  22   Max.   :21.000  
 NA's                       :  33   NA's              :   9   NA's   :89      
                        trust            trustfam         trustneigh  
 MOST PEOPLE CAN BE TRUSTED: 678   YES       :4470   YES       :2959  
 ONE CAN`T BE TOO CAREFUL  :3777   NO        : 191   NO        : 955  
 IT`S DIFFICULT TO TELL    : 508   NO OPINION: 328   NO OPINION:1075  
 NA's                      :  37   NA's      :  11   NA's      :  11  
                                                                      
                                                                      
                                                                      
     income     
 Min.   :   -8  
 1st Qu.:  700  
 Median : 1200  
 Mean   : 1411  
 3rd Qu.: 1800  
 Max.   :16000  
 NA's   :683    
```
````
```{r, results='hide'}
# Get summary information about the selected variables
codebook.syn(mydata)$tab
```
````{tabs}

```{code-tab} r R

# Get the distributional distribution of the variables
describe(mydata)

```
````

````{tabs}

```{code-tab} plaintext R Output
            vars    n    mean      sd median trimmed    mad min   max range
sex*           1 5000    1.56    0.50      2    1.58   0.00   1     2     1
agegr*         2 4996    3.62    1.65      4    3.65   1.48   1     6     5
edu*           3 4993    2.48    1.01      2    2.47   1.48   1     4     3
socprof*       4 4967    4.76    2.68      6    4.72   2.97   1     9     8
marital*       5 4991    1.96    0.77      2    1.88   0.00   1     6     5
depress        6 4911    4.49    4.24      4    3.94   4.45   0    21    21
trust*         7 4963    1.97    0.49      2    1.96   0.00   1     3     2
trustfam*      8 4989    1.17    0.52      1    1.01   0.00   1     3     2
trustneigh*    9 4989    1.62    0.82      1    1.53   0.00   1     3     2
income        10 4317 1411.09 1271.65   1200 1248.68 830.26  -8 16000 16008
             skew kurtosis    se
sex*        -0.26    -1.93  0.01
agegr*      -0.08    -1.08  0.02
edu*         0.04    -1.07  0.01
socprof*    -0.13    -1.30  0.04
marital*     1.24     3.51  0.01
depress      1.02     0.74  0.06
trust*      -0.08     1.17  0.01
trustfam*    2.96     7.19  0.01
trustneigh*  0.79    -1.04  0.01
income       3.12    20.70 19.35
```
````
3. **Handle missing values**
````{tabs}

```{code-tab} r R

# Check for negative income values
table(mydata$income[mydata$income < 0], useNA = "ifany")

```
````

````{tabs}

```{code-tab} plaintext R Output

  -8 <NA> 
 603  683 
```
````
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

````{tabs}

```{code-tab} plaintext R Output

Synthesis
-----------
 sex agegr edu socprof marital depress trust trustfam trustneigh income

```
````
We have now created the object `mysyn`, which is of class `synds` and contains several pieces of information related to the synthetic data. We will quickly go over the most important output.
````{tabs}

```{code-tab} r R

summary(mysyn)

```
````

````{tabs}

```{code-tab} plaintext R Output
Synthetic object with one synthesis using methods:
       sex      agegr        edu    socprof    marital    depress      trust 
  "sample"     "cart"     "cart"     "cart"     "cart"     "cart"     "cart" 
  trustfam trustneigh     income 
    "cart"     "cart"     "cart" 

     sex         agegr                            edu      
 MALE  :2188   16-24: 702   PRIMARY/NO EDUCATION    : 934  
 FEMALE:2812   25-34: 740   VOCATIONAL/GRAMMAR      :1662  
               35-44: 777   SECONDARY               :1493  
               45-59:1378   POST-SECONDARY OR HIGHER: 903  
               60-64: 523   NA's                    :   8  
               65+  : 878                                  
               NA's :   2                                  
                        socprof                   marital        depress      
 RETIRED                    :1239   SINGLE            :1304   Min.   : 0.000  
 EMPLOYED IN PRIVATE SECTOR : 950   MARRIED           :2965   1st Qu.: 1.000  
 PUPIL OR STUDENT           : 582   WIDOWED           : 488   Median : 4.000  
 EMPLOYED IN PUBLIC SECTOR  : 557   DIVORCED          : 203   Mean   : 4.446  
 OTHER ECONOMICALLY INACTIVE: 478   LEGALLY SEPARATED :   5   3rd Qu.: 7.000  
 (Other)                    :1174   DE FACTO SEPARATED:  28   Max.   :21.000  
 NA's                       :  20   NA's              :   7   NA's   :87      
                        trust            trustfam         trustneigh  
 MOST PEOPLE CAN BE TRUSTED: 667   YES       :4456   YES       :2982  
 ONE CAN`T BE TOO CAREFUL  :3804   NO        : 211   NO        : 927  
 IT`S DIFFICULT TO TELL    : 495   NO OPINION: 322   NO OPINION:1083  
 NA's                      :  34   NA's      :  11   NA's      :   8  
                                                                      
                                                                      
                                                                      
     income     
 Min.   :   -8  
 1st Qu.:  727  
 Median : 1200  
 Mean   : 1418  
 3rd Qu.: 1850  
 Max.   :16000  
 NA's   :738    
```
````

````{tabs}

```{code-tab} r R

# Compare the synthetic data with the original data
compare(mysyn, mydata, stat = "counts")

```
````

````{tabs}

```{code-tab} plaintext R Output
Calculations done for sex 
Calculations done for agegr 
Calculations done for edu 
Calculations done for socprof 
Calculations done for marital 
Calculations done for depress 
Calculations done for trust 
Calculations done for trustfam 
Calculations done for trustneigh 
Calculations done for income 

Comparing counts observed with synthetic

Press return for next variable(s): 
Press return for next variable(s): 

Selected utility measures:
               pMSE   S_pMSE df
sex        0.000000 0.029265  1
agegr      0.000096 1.274889  6
edu        0.000046 0.923070  4
socprof    0.000225 2.000709  9
marital    0.000105 1.402613  6
depress    0.000042 0.841409  4
trust      0.000012 0.320920  3
trustfam   0.000027 0.714912  3
trustneigh 0.000025 0.672641  3
income     0.000128 1.709455  6
```
````
5. **Export synthetic data**
````{tabs}

```{code-tab} r R

# Export synthetic data to CSV format
write.syn(mysyn, filename = "mysyn_SD2001", filetype = "csv")

```
````

````{tabs}

```{code-tab} plaintext R Output
Synthetic data exported as csv file(s).
Information on synthetic data written to
  /home/cdsw/ons-spark/synthesis_info_mysyn_SD2001.txt 
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

````{tabs}

```{code-tab} plaintext R Output
 [1] "call"             "m"                "syn"              "method"          
 [5] "visit.sequence"   "predictor.matrix" "smoothing"        "event"           
 [9] "denom"            "proper"           "n"                "k"               
[13] "rules"            "rvalues"          "cont.na"          "semicont"        
[17] "drop.not.used"    "drop.pred.only"   "models"           "seed"            
[21] "var.lab"          "val.lab"          "obs.vars"         "numtocat"        
[25] "catgroups"       
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

````{tabs}

```{code-tab} plaintext R Output
       sex      agegr        edu    socprof    marital    depress      trust 
  "sample"     "cart"     "cart"     "cart"     "cart"     "cart"     "cart" 
  trustfam trustneigh     income 
    "cart"     "cart"     "cart" 
           sex agegr edu socprof marital depress trust trustfam trustneigh
sex          0     0   0       0       0       0     0        0          0
agegr        1     0   0       0       0       0     0        0          0
edu          1     1   0       0       0       0     0        0          0
socprof      1     1   1       0       0       0     0        0          0
marital      1     1   1       1       0       0     0        0          0
depress      1     1   1       1       1       0     0        0          0
trust        1     1   1       1       1       1     0        0          0
trustfam     1     1   1       1       1       1     1        0          0
trustneigh   1     1   1       1       1       1     1        1          0
income       1     1   1       1       1       1     1        1          1
           income
sex             0
agegr           0
edu             0
socprof         0
marital         0
depress         0
trust           0
trustfam        0
trustneigh      0
income          0
       sex      agegr        edu    socprof    marital    depress      trust 
         1          2          3          4          5          6          7 
  trustfam trustneigh     income 
         8          9         10 
$sex
[1] NA

$agegr
[1] NA

$edu
[1] NA

$socprof
[1] NA

$marital
[1] NA

$depress
[1] NA

$trust
[1] NA

$trustfam
[1] NA

$trustneigh
[1] NA

$income
[1] -8

[1] 161401295
```
````
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

````{tabs}

```{code-tab} plaintext R Output

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
````
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

#### 3.4 Synthesise census teaching data

In this section, we will guide you through the process of synthesising another case study as presented by Iain Dove, a census teaching data using the `synthpop` package in `R`. This demo is an alternative to the synthesis of `SD2011` in `section 3.3`. This involves loading the data, creating a subset, synthesising the data, and comparing the synthetic data with the original data.

#### Step-by-step guide

1. **Loading the data**
````{tabs}

```{code-tab} r R

#config <- yaml::yaml.load_file("../../../config.yaml")
config <- yaml::yaml.load_file("/home/cdsw/ons-spark/config.yaml")
census_2011_path = config$census_2011_teaching_data_path_csv

```
````

````{tabs}

```{code-tab} r R

census_teaching_data <- data.table::fread(census_2011_path, skip = 1) %>%
                        janitor::clean_names()

```
````


2. **Create a subset of the data**

To reduce the demo run time, we will create a subset of the data.


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

````{tabs}

```{code-tab} plaintext R Output
Variable residence_type has only one value so its method has been changed to "constant".

Synthesis
-----------
 person_id region residence_type family_composition population_base sex age marital_status student country_of_birth
 health ethnic_group religion economic_activity occupation industry hours_worked_per_week approximated_social_grade
```
````


You have created a synthetic data object `synthetic_census_teaching_data` of class `synds`, which is a list with a number of components including the synthesised data and information on how they were created.


````{tabs}

```{code-tab} r R

# Check the class of the synthetic data object
class(synthetic_census_teaching_data)

```
````

````{tabs}

```{code-tab} plaintext R Output
[1] "synds"
```
````


4. **Access the synthetic dataframe**

We can access the synthetic dataframe by selecting it from the `synds` object as well as get the summary of the synthetic data.



```{r, results='hide'}
# Access the synthetic dataframe
synthetic_census_teaching_data$syn
```
````{tabs}

```{code-tab} r R

summary(synthetic_census_teaching_data)

```
````

````{tabs}

```{code-tab} plaintext R Output
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
 Min.   :7394485   Length:100000      Length:100000      Min.   :-9.000    
 1st Qu.:7420049   Class :character   Class :character   1st Qu.: 2.000    
 Median :7445482   Mode  :character   Mode  :character   Median : 2.000    
 Mean   :7445480                                         Mean   : 2.236    
 3rd Qu.:7470645                                         3rd Qu.: 3.000    
 Max.   :7910302                                         Max.   : 6.000    
 population_base      sex             age        marital_status 
 Min.   :1.000   Min.   :1.000   Min.   :1.000   Min.   :1.000  
 1st Qu.:1.000   1st Qu.:1.000   1st Qu.:2.000   1st Qu.:1.000  
 Median :1.000   Median :2.000   Median :4.000   Median :2.000  
 Mean   :1.015   Mean   :1.504   Mean   :3.925   Mean   :1.855  
 3rd Qu.:1.000   3rd Qu.:2.000   3rd Qu.:6.000   3rd Qu.:2.000  
 Max.   :3.000   Max.   :2.000   Max.   :8.000   Max.   :5.000  
    student      country_of_birth     health        ethnic_group   
 Min.   :1.000   Min.   :-9.000   Min.   :-9.000   Min.   :-9.000  
 1st Qu.:2.000   1st Qu.: 1.000   1st Qu.: 1.000   1st Qu.: 1.000  
 Median :2.000   Median : 1.000   Median : 2.000   Median : 1.000  
 Mean   :1.781   Mean   : 1.002   Mean   : 1.675   Mean   : 1.129  
 3rd Qu.:2.000   3rd Qu.: 1.000   3rd Qu.: 2.000   3rd Qu.: 1.000  
 Max.   :2.000   Max.   : 2.000   Max.   : 5.000   Max.   : 5.000  
    religion      economic_activity   occupation        industry     
 Min.   :-9.000   Min.   :-9.0000   Min.   :-9.000   Min.   :-9.000  
 1st Qu.: 1.000   1st Qu.: 1.0000   1st Qu.:-9.000   1st Qu.:-9.000  
 Median : 2.000   Median : 1.0000   Median : 3.000   Median : 4.000  
 Mean   : 2.336   Mean   : 0.6327   Mean   : 1.418   Mean   : 2.462  
 3rd Qu.: 2.000   3rd Qu.: 5.0000   3rd Qu.: 7.000   3rd Qu.: 8.000  
 Max.   : 9.000   Max.   : 9.0000   Max.   : 9.000   Max.   :12.000  
 hours_worked_per_week approximated_social_grade
 Min.   :-9.000        Min.   :-9.0000          
 1st Qu.:-9.000        1st Qu.: 1.0000          
 Median :-9.000        Median : 2.0000          
 Mean   :-3.451        Mean   : 0.2669          
 3rd Qu.: 3.000        3rd Qu.: 3.0000          
 Max.   : 4.000        Max.   : 4.0000          
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

````{tabs}

```{code-tab} plaintext R Output
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
person_id                 5e-06 1.825003  4
region                    5e-06 1.081370  7
residence_type            0e+00      NaN  0
family_composition        0e+00 0.249866  2
population_base           2e-06 1.639153  2
sex                       0e+00 0.196017  1
age                       4e-06 1.403554  4
marital_status            1e-06 0.293751  4
student                   1e-06 1.848258  1
country_of_birth          0e+00 0.052328  2
health                    2e-06 1.337487  2
ethnic_group              0e+00 0.048534  2
religion                  0e+00 0.090715  2
economic_activity         0e+00 0.086581  3
occupation                2e-06 1.207111  3
industry                  2e-06 1.176398  3
hours_worked_per_week     1e-06 0.564426  4
approximated_social_grade 2e-06 0.695544  4
```
````
#### Relationships between multiple variables

Relationships between multiple variables are more complex to model. The `synthpop` default uses a saturated model, which can be challenging for large datasets. Here, we use an input `CSV` file to specify which variables are related.

1. **Load the input CSV file**
````{tabs}

```{code-tab} r R

#config <- yaml::yaml.load_file("../../../config.yaml")
config <- yaml::yaml.load_file("/home/cdsw/ons-spark/config.yaml")
census_relationship_path = config$census_relationship_file_path_csv

```
````

````{tabs}

```{code-tab} r R

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

````{tabs}

```{code-tab} plaintext R Output

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
````
#### Comparison with real data

Assess the accuracy of the synthetic data by comparing it with the original data.
````{tabs}

```{code-tab} r R

# Compare synthetic data with real data
synthpop::compare(data = small_census_teaching_data,
                  object = synthetic_census_teaching_data_2)

```
````

````{tabs}

```{code-tab} plaintext R Output
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
person_id                 4.0e-06  1.666253  4
region                    3.0e-06  0.620390  7
residence_type            0.0e+00       NaN  0
family_composition        0.0e+00  0.270374  2
population_base           2.0e-06  1.612327  2
sex                       0.0e+00  0.196017  1
age                       1.0e-06  0.375270  4
marital_status            2.0e-06  0.614401  4
student                   0.0e+00  0.679632  1
country_of_birth          1.0e-06  0.739237  2
health                    3.0e-06  2.485103  2
ethnic_group              1.0e-06  0.491428  2
religion                  1.0e-06  0.505577  2
economic_activity         2.0e-06  1.255182  3
occupation                9.6e-05 51.353084  3
industry                  4.6e-05 24.288815  3
hours_worked_per_week     3.0e-06  1.333879  4
approximated_social_grade 7.0e-05 27.805141  4
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

```{code-tab} plaintext R Output
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

```{code-tab} plaintext R Output
Key: <economic_activity, hours_worked_per_week>
    economic_activity hours_worked_per_week synth2_count
                <num>                 <num>        <int>
 1:                -9                    -9        19860
 2:                 1                     1         2705
 3:                 1                     2         8013
 4:                 1                     3        23910
 5:                 1                     4         4100
 6:                 2                     1          721
 7:                 2                     2         1201
 8:                 2                     3         2973
 9:                 2                     4         1377
10:                 3                    -9         3396
11:                 4                    -9          414
12:                 4                     1         1134
13:                 4                     2          425
14:                 4                     3          538
15:                 4                     4           82
16:                 5                    -9        16742
17:                 6                    -9         4117
18:                 7                    -9         3290
19:                 8                    -9         3414
20:                 9                    -9         1588
    economic_activity hours_worked_per_week synth2_count
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

```{code-tab} plaintext R Output
Key: <economic_activity, hours_worked_per_week>
    economic_activity hours_worked_per_week synth1_count
                <int>                 <int>        <int>
 1:                -9                    -9        10553
 2:                -9                     1          885
 3:                -9                     2         1891
 4:                -9                     3         5563
 5:                -9                     4         1110
 6:                 1                    -9        20568
 7:                 1                     1         1726
 8:                 1                     2         3636
 9:                 1                     3        10719
10:                 1                     4         2193
11:                 2                    -9         3285
12:                 2                     1          300
13:                 2                     2          638
14:                 2                     3         1787
15:                 2                     4          367
16:                 3                    -9         1806
17:                 3                     1          166
18:                 3                     2          298
19:                 3                     3          915
20:                 3                     4          196
21:                 4                    -9         1300
22:                 4                     1          105
23:                 4                     2          259
24:                 4                     3          695
25:                 4                     4          148
26:                 5                    -9         8724
27:                 5                     1          757
28:                 5                     2         1538
29:                 5                     3         4599
30:                 5                     4          957
31:                 6                    -9         2185
32:                 6                     1          179
33:                 6                     2          381
34:                 6                     3         1143
35:                 6                     4          257
36:                 7                    -9         1660
37:                 7                     1          125
38:                 7                     2          321
39:                 7                     3          907
40:                 7                     4          177
41:                 8                    -9         1752
42:                 8                     1          181
43:                 8                     2          352
44:                 8                     3          924
45:                 8                     4          176
46:                 9                    -9          864
47:                 9                     1           78
48:                 9                     2          128
49:                 9                     3          441
50:                 9                     4           85
    economic_activity hours_worked_per_week synth1_count
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

````{tabs}

```{code-tab} plaintext R Output
[1] 86.9
[1] 1993.16
[1] 14.28429
[1] Inf
```
````


This guide provides a concise overview of synthesizing census teaching data with relationships between multiple variables using the `synthpop` package in R. For more detailed information, refer to the [synthpop documentation](https://www.synthpop.org.uk/get-started.html).

### References

* [Synthetic data at ONS](https://www.ons.gov.uk/methodology/methodologicalpublications/generalmethodology/onsworkingpaperseries/onsmethodologyworkingpaperseriesnumber16syntheticdatapilot)
* [`synthpop` (Nowok, Raab, and Dibben 2016)](https://www.synthpop.org.uk/get-started.html)
* [Synthetic data in R: Generating synthetic data with high utility using synthpop (Thom Volker, Raoul Schram, Erik-Jan van Kesteren)](https://thomvolker.github.io/osf_synthetic/osf_synthetic_workshop.html)
* [Faker: Synthetic Data in Python](../ancillary-topics/faker)
* [Mimesis: Synthetic Data in Python](../ancillary-topics/mimesis)

### Acknowledgments
  
Special thanks to Iain Dove for sharing his knowledge of the `Synthetic data, a useful tool for ONS`, and to Wil Roberts, Elisha Mercado and Vicky Pickering for inspiring this tip!