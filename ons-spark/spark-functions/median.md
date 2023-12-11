# Median in Spark

### Introduction

When working with big data, simple operations like computing the median can have significant computational costs associated with them. In Spark, estimation strategies are employed to compute medians and other quantiles, with the accuracy of the estimation being balanced against the computational cost.


First we will set-up a Spark session and read in the config file.
````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession, functions as F
import yaml

spark = (SparkSession.builder.master("local[2]")
         .appName("ons-spark")
         .getOrCreate())

with open("../../../config.yaml") as f:
    config = yaml.safe_load(f)
```

```{code-tab} r R

library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
  master = "local[2]",
  app_name = "ons-spark",
  config = sparklyr::spark_config(),
  )

config <- yaml::yaml.load_file("ons-spark/config.yaml")

```
````
Read in population dataset
````{tabs}
```{code-tab} py
pop_df = spark.read.parquet(config["population_path"])

pop_df.printSchema()
```

```{code-tab} r R

pop_df <- sparklyr::spark_read_parquet(sc, path = config$population_path)
                                     
sparklyr::sdf_schema(pop_df)

```
````

````{tabs}

```{code-tab} plaintext Python Output
root
 |-- postcode_district: string (nullable = true)
 |-- population: long (nullable = true)
```

```{code-tab} plaintext R Output
$postcode_district
$postcode_district$name
[1] "postcode_district"

$postcode_district$type
[1] "StringType"


$population
$population$name
[1] "population"

$population$type
[1] "LongType"


```
````
### Computing the median for a Spark DataFrame

We can compute the medians for big data in Spark using the [Greenwald-Khanna](http://infolab.stanford.edu/~datar/courses/cs361a/papers/quantiles.pdf) algorithm that approximates a given quantile or quantiles of a distribution where the number of observations is very large. The relative error of the function can be adjusted to give a more accurate estimate for a given quantile at the cost of increased computation. 

In PySpark, the Greenwald-Khanna algorithm is implemented with [`approxQuantile`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.approxQuantile.html), which extends [`pyspark.sql.DataFrame`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html#). To find the exact median of the `population` column with PySpark, we apply the `approxQuantile` to our population DataFrame and specify the column name, an array containing the quantile of interest (in this case, the median or second quartile, 0.5), and the relative error, which is set to 0 to give the exact median.

In SparklyR, the Greenwald-Khanna algorithm is implemented with [`sdf_quantile`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_quantile.html#sdf_quantile). To find the exact median of the `population` column with SparklyR, we apply `sdf_quantile` to our popuation DataFrame, specifying the same parameters as in the PySpark example - an array containing the quantile to compute (the median, 0.5) and the relative error (0).
````{tabs}
```{code-tab} py
pop_df.approxQuantile("population", [0.5], 0)
```

```{code-tab} r R

sdf_quantile(pop_df, "population", probabilities = 0.5, relative.error = 0)

```
````

````{tabs}

```{code-tab} plaintext Python Output
[22331.0]
```

```{code-tab} plaintext R Output
  50% 
22331 
```
````
In the next example, we will compute the 1st, 2nd, and 3rd quartiles of the `population` column, which we will do by passing an array containing the numeric values of the three quartiles as our second argument (0.25, 0.5 and 0.75, respectively). We will assume that computing three quantiles exactly would be too computationally expensive given our available resources, so we will increase the relative error parameter to reduce the accuracy of our estimates in return for decreased computation cost. We will increase the relative error parameter to 0.01, or 1%.
````{tabs}
```{code-tab} py
pop_df.approxQuantile("population", [0.25, 0.5, 0.75], 0.01)
```

```{code-tab} r R

sdf_quantile(pop_df, "population", probabilities = c(0.25, 0.5, 0.75), relative.error = 0.01)

```
````

````{tabs}

```{code-tab} plaintext Python Output
[12051.0, 22059.0, 33495.0]
```

```{code-tab} plaintext R Output
  25%   50%   75% 
12051 22334 33495 
```
````
What is the relationship between the relative error parameter and the accuracy of the estimated result?
````{tabs}
```{code-tab} py
exact = pop_df.approxQuantile("population", [0.5], 0)[0]
estimate = pop_df.approxQuantile("population", [0.5], 0.01)[0]

round((exact - estimate)/exact, 3)
```

```{code-tab} r R

exact = sdf_quantile(pop_df, "population", probabilities = c(0.5), relative.error = 0)[[1]]
estimate = sdf_quantile(pop_df, "population", probabilities = c(0.5), relative.error = 0.01)[[1]]

print(round((exact-estimate)/exact, 3))

```
````

````{tabs}

```{code-tab} plaintext Python Output
0.012
```

```{code-tab} plaintext R Output
[1] 0.012
```
````
By specificying a relative error of 1%, we can see that the difference between the estimated median for the `population` column and the exact median is approximately 1%.

### Computing the median for aggregated data

Usually when performing data analysis you will want to find the median of aggregations created from your dataset, rather than just computing the median of entire columns. We will first read in the borough and postcode information from the animal rescue dataset and join these with the population dataset.
````{tabs}
```{code-tab} py
borough_df = spark.read.parquet(config["rescue_clean_path"])

borough_df = borough_df.select("borough", F.upper(F.col("postcodedistrict")).alias("postcode_district"))

pop_borough_df = borough_df.join(
    pop_df,
    on = "postcode_district",
    how = "left"
).filter(F.col("borough").isNotNull())

pop_borough_df.show(10)
```

```{code-tab} r R

borough_df <- sparklyr::spark_read_parquet(sc, path = config$rescue_clean_path)

borough_df <- borough_df %>%
    select(borough, postcode_district = postcodedistrict) %>%
    mutate(postcode_district = upper(postcode_district))
    
pop_borough_df <- borough_df %>%
    left_join(pop_df, by = "postcode_district") %>%
    filter(!is.na(borough))
    
print(pop_borough_df)

```
````

````{tabs}

```{code-tab} plaintext Python Output
+-----------------+--------------------+----------+
|postcode_district|             borough|population|
+-----------------+--------------------+----------+
|             SE19|             Croydon|     27639|
|             SE25|             Croydon|     34521|
|              SM5|              Sutton|     38291|
|              UB9|          Hillingdon|     14336|
|              RM3|            Havering|     40272|
|             RM10|Barking And Dagenham|     38157|
|              E11|      Waltham Forest|     55128|
|              E12|           Redbridge|     41869|
|              CR0|             Croydon|    153812|
|               E5|             Hackney|     47669|
+-----------------+--------------------+----------+
only showing top 10 rows
```

```{code-tab} plaintext R Output
# Source: spark<?> [?? x 3]
   borough              postcode_district population
   <chr>                <chr>                  <dbl>
 1 Croydon              SE19                   27639
 2 Croydon              SE25                   34521
 3 Sutton               SM5                    38291
 4 Hillingdon           UB9                    14336
 5 Havering             RM3                    40272
 6 Barking And Dagenham RM10                   38157
 7 Waltham Forest       E11                    55128
 8 Redbridge            E12                    41869
 9 Croydon              CR0                   153812
10 Hackney              E5                     47669
# ℹ more rows
```
````
Next, we will aggregate the population data across boroughs in the combined `pop_borough_df` and find the median postcode population for each borough.

The DAP environment is currently limited to using PySpark version 2.4.0, but there are two functions present in later versions that are useful for calculating quantiles/medians.

- In PySpark 3.1.0, [`percentile_approx`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.percentile_approx.html) was added to [`pyspark.sql.functions`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#), which allows you to calculate quantiles on aggregatations. The parameters required in `percentile_approx` are similar to that of `approxQuantile`, with the difference being that you must provide the accuracy instead of relative error, where accuracy is defined as $ \frac{1}{relative\ error} $

``` py
pop_borough_df.groupBy("borough").agg(
        percentile_approx("population", [0.5], 100000).alias("median_postcode_population")
).show()
```
- In PySpark 3.4.0, [`median`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.median.html#pyspark.sql.functions.median) was added to `pyspark.sql.functions`, which further simplifies the process of of computing the median within aggregations, as it does not require a parameter specifying the quantile, or an accuracy parameter. 

``` py
pop_borough_df.groupBy("borough").agg(
        median("population").alias("median_postcode_population")
).show()
```

In PySpark 2.4.0, to calculate quantiles inside aggregations we can use the built-in Spark SQL [`approx_percentile`](https://spark.apache.org/docs/latest/api/sql/index.html#approx_percentile) function by passing SQL code to the PySpark API as a string inside of an `expr`. The parameters of the SQL function are identical to the PySpark `percentile_approx` function, however we must give the list of quantiles we wish to calculate inside of a SQL `array()`.

In SparklyR, we are able to apply the `percentile_approx` function to an aggregation, inside of a `summarise` function. The arguments passed to `percentile_approx` in SparklyR are identical to the PySpark implementation of `percentile_approx`.
````{tabs}
```{code-tab} py
pop_borough_df.groupBy("borough").agg(
        F.expr('approx_percentile(population, array(0.5), 100000)').alias("median_postcode_population")
).show()
```

```{code-tab} r R

pop_borough_df %>%
    group_by(borough) %>%
    summarise(median_postcode_population = percentile_approx(population, c(0.5), 100000)) %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+--------------------+--------------------------+
|             borough|median_postcode_population|
+--------------------+--------------------------+
|       Epping Forest|                   [23599]|
|             Croydon|                   [48428]|
|          Wandsworth|                   [64566]|
|              Bexley|                   [34767]|
|Kingston Upon Thames|                   [31384]|
|             Lambeth|                   [43845]|
|Kensington And Ch...|                   [22381]|
|              Camden|                   [60910]|
|           Brentwood|                   [24715]|
|           Greenwich|                   [63082]|
|Barking And Dagenham|                   [38157]|
|              Newham|                   [52244]|
|       Tower Hamlets|                   [69523]|
|              Barnet|                   [29882]|
|            Hounslow|                   [36147]|
|              Harrow|                   [44781]|
|      City Of London|                      [39]|
|           Islington|                   [47204]|
|               Brent|                   [67621]|
|            Haringey|                   [43025]|
+--------------------+--------------------------+
only showing top 20 rows
```

```{code-tab} plaintext R Output
# Source: spark<?> [?? x 2]
   borough              median_postcode_population
   <chr>                                     <dbl>
 1 Redbridge                                 35543
 2 Newham                                    52244
 3 Epping Forest                             23599
 4 Broxbourne                                21884
 5 Waltham Forest                            60262
 6 Haringey                                  43025
 7 Ealing                                    45703
 8 City Of London                               39
 9 Barking And Dagenham                      38157
10 Brent                                     67621
# ℹ more rows
```
````
Close the Spark session to prevent any hanging processes.
````{tabs}
```{code-tab} py
spark.stop()
```

```{code-tab} r R

sparklyr::spark_disconnect(sc)

```
````
### The relationship between relative error and computational cost

The Greenwald-Khanna algorithm has a worst-case space requirement of $ O(\frac{1}{\epsilon}log(\epsilon N)) $, where $ \epsilon $ is the relative error and $ N $ is the size of our input data. Given a constant input size, empirical measurements show that increasing the precision of $\epsilon$ by a factor of 10 from 0.1 to 0.01 leads to a space requirement increase of between 8 and 10 times, depending on the size of the input data. Increasing the precision of $\epsilon$ by a factor of 10 from 0.01 to 0.001 leads to a smaller space requirement increase of between 7 and 9 times. For most Spark applications, you will be able to able to specify a high degree of precision in calculating quantiles using implementations of the Greenwald-Khanna algorithm due to the logarithmic relationship between relative error and space requirements.

### Further reading

- [Greenwald, Michael, and Sanjeev Khanna. “Space-efficient online computation of quantile summaries.”](http://infolab.stanford.edu/%7Edatar/courses/cs361a/papers/quantiles.pdf)

- [Implementation of Greenwald-Khanna algorithm](https://aakinshin.net/posts/greenwald-khanna-quantile-estimator/)