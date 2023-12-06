## Data binning

Data binning is a data pre-proccessing method which transforms continuous or discrete
data to categorical. The original values which fall into a specific interval are replaced
by a value representive of that interval.

In a spark dataframe this is easily implemented by applying the [`when()`](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.when.html) function for PySpark  and the [`if_else()`](https://dplyr.tidyverse.org/reference/if_else.html) function from [`dplyr`](https://dplyr.tidyverse.org/) package for sparklyr.

### Creating spark session and sample data
 
We will start by creating a sample dataframe with Python or R and moving it into a spark cluster.
For demonstation reasons we will create a dataframe with 10 rows  with  `id` 
and `age` columns, we will also populate `age` column with random values from 1 to 30.

````{tabs}
```{code-tab} py 
#import packages

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
import numpy as np

spark = (SparkSession.builder.master("local[2]")
         .appName("bin-continuous-variables-pyspark")
         .getOrCreate())

np.random.seed(2023)

#number of rows to create synthetic data
n = 10

#create pandas dataframe
df = pd.DataFrame({
  'id':np.arange(n),
  'age':np.random.randint(1,30,n)
  })

#convert to spark dataframe
sdf = spark.createDataFrame(df)

#quick look
sdf.show()
```
```{code-tab} r R 
library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
  master = "local[2]",
  app_name = "bin-continuous-variables-sparklyr",
  config = sparklyr::spark_config())
  
set.seed(2024)

#number of rows to create synthetic data
n = 10

df <- data.frame(
  "id" = 1:n,
  "age" = sample(1:30,n,rep=TRUE))

sdf <- copy_to(sc ,df)

#quick look
sdf
```
````
Let's have a quick look at the data:

````{tabs}
```{code-tab} plaintext Python Output
+---+---+
|age| id|
+---+---+
| 24|  0|
| 26|  1|
|  7|  2|
| 24|  3|
|  2|  4|
| 29|  5|
|  4|  6|
| 21|  7|
| 21|  8|
| 23|  9|
+---+---+
```

```{code-tab} plaintext R Output
# Source: spark<?> [?? x 3]
      id   age
   <int> <int>
 1     1    26
 2     2    10
 3     3    21
 4     4    21
 5     5    14
 6     6    22
 7     7    13
 8     8    10
 9     9    27
10    10     4
```
````

### Binning data into 2 groups

The next step is defining how to parse the values into bins. This article focuses on the implentation of binning in a spark cluster, the allocation of data into bins is outside the scope of this article. For demonstation reasons we will create a new column `age_bracket` with the value `young` when `age` is less than or equal to 9 and `old` otherwise.

````{tabs}
```{code-tab} py 
sdf = (
    sdf.withColumn('age_bracket',
                  F.when(sdf.age <= 9, 'young')
                    .otherwise('old')
                    )
)

#quick look
sdf.show()
```
```{code-tab} r R 
sdf <- sdf %>%
  mutate(age_bracket = ifelse(age <= 9,
                              "young",
                              "old"
                              )
        )

#quick look
sdf
```
````
Let's have another look at the data:

````{tabs}
```{code-tab} plaintext Python Output
+---+---+-----------+
|age| id|age_bracket|
+---+---+-----------+
| 24|  0|        old|
| 26|  1|        old|
|  7|  2|      young|
| 24|  3|        old|
|  2|  4|      young|
| 29|  5|        old|
|  4|  6|      young|
| 21|  7|        old|
| 21|  8|        old|
| 23|  9|        old|
+---+---+-----------+

```

```{code-tab} plaintext R Output
# Source: spark<?> [?? x 3]
      id   age age_bracket
   <int> <int> <chr>      
 1     1    26 old        
 2     2    10 old        
 3     3    21 old        
 4     4    21 old        
 5     5    14 old        
 6     6    22 old        
 7     7    13 old        
 8     8    10 old        
 9     9    27 old        
10    10     4 young      
```
````
### Binning data into 3 or more groups

To split assign the data to 3 or more categories the same approach can be implemented as shown below.
In this example, `age_bracket` is labeled as `young` when `age` is less than or equal to 9,
as `old` when `age` is less than or equal to 19 and `oldest` otherwise.

````{tabs}
```{code-tab} py 
sdf = (
    sdf.withColumn('age_bracket',
                  F.when(sdf.age <= 9, 'young')
                    .when(sdf.age <= 19, 'older')
                    .otherwise('oldest')
                    )
                  
)

sdf.show()
```
```{code-tab} r R 
sdf <- sdf %>%

  mutate(age_bracket = ifelse(age <= 9 ,
                              "young",
                              ifelse(
                                  age <= 19,
                                  "old",
                                  "oldest")
                              )
        )

sdf
```
````
Let's check the output again:

````{tabs}
```{code-tab} plaintext Python Output
+---+---+-----------+
|age| id|age_bracket|
+---+---+-----------+
| 24|  0|     oldest|
| 26|  1|     oldest|
|  7|  2|      young|
| 24|  3|     oldest|
|  2|  4|      young|
| 29|  5|     oldest|
|  4|  6|      young|
| 21|  7|     oldest|
| 21|  8|     oldest|
| 23|  9|     oldest|
+---+---+-----------+
```

```{code-tab} plaintext R Output
# Source: spark<?> [?? x 3]
      id   age age_bracket
   <int> <int> <chr>      
 1     1    26 oldest     
 2     2    10 old        
 3     3    21 oldest     
 4     4    21 oldest     
 5     5    14 old        
 6     6    22 oldest     
 7     7    13 old        
 8     8    10 old        
 9     9    27 oldest     
10    10     4 young   
```
````