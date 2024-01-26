# Flags in Spark

A flag is a column that indicates whether a certain condition, such as a threshold value, is met. The flag is typically a 0 or 1 depending on whether the condition has been met.

Our example will show how you can create a flag for age differences greater than a threshold wihtin a given group. **Note**: Additional use cases for creating flags will be added to this page at a later date.

## Creating an age-difference flag

Given an `group` and `age` columns, we want to highlight if the top 2 values in `age` column per `group` have difference greater than a specified threshold value.

It is likely that there are *many* ways of doing this. Below is one method using the functions available from `pyspark.sql` and some dummy data. 

As is good practice, first import the packages you will be using and start your Spark session:
````{tabs}
```{code-tab} py
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window
```

```{code-tab} r R

library(sparklyr)
library(dplyr)

```
````

````{tabs}
```{code-tab} py
spark = (SparkSession.builder.master("local[2]")
         .appName("flags")
         .getOrCreate())
```

```{code-tab} r R

default_config <- sparklyr::spark_config()

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "flags",
    config = default_config)


```
````
### Creating your dummy data

The dataframe `df` will have an `id` column from 0 to 4. To create multiple `id` entries we will do a `.CrossJoin` with the numbers from 0 to 2 and then drop the latter column. To find more information on cross joins please refer to the [page on cross joins](https://best-practice-and-impact.github.io/ons-spark/spark-functions/cross-joins.html). Finally, we will add an `age` column with random numbers from 1 to 10. 
````{tabs}
```{code-tab} py
df = (
    spark.range(5).select((F.col("id")).alias("group"))
    .crossJoin(
        spark.range(3)
        .withColumnRenamed("id","drop")
    ).drop("drop")
    .withColumn("age", F.ceil(F.rand(seed=42)*10)))

df.show()
```

```{code-tab} r R

set.seed(42)

df = sparklyr::sdf_seq(sc, 0, 4) %>%
    rename("group" = "id") %>%
    cross_join(sparklyr::sdf_seq(sc, 0, 2)) %>%
    rename("drop" = "id") %>%
    select(-c(drop)) %>%
    mutate(age = ceil(rand()*10))
       
print(df,n=15)

```
````

````{tabs}

```{code-tab} plaintext Python Output
+-----+---+
|group|age|
+-----+---+
|    0|  7|
|    0|  9|
|    0| 10|
|    1|  9|
|    1|  5|
|    1|  6|
|    2|  4|
|    2|  3|
|    2| 10|
|    3|  1|
|    3|  4|
|    3| 10|
|    4|  8|
|    4|  7|
|    4|  3|
+-----+---+
```

```{code-tab} plaintext R Output
# Source: spark<?> [?? x 2]
   group   age
   <int> <dbl>
 1     0     7
 2     1     1
 3     0     3
 4     0     6
 5     1     5
 6     1     5
 7     2     1
 8     3     1
 9     4     2
10     2     8
11     2     7
12     3     5
13     3    10
14     4     3
15     4     4
```
````
### Creating your columns 

There will be more than one way to get the desired result. Note that the method outlined here spells out the process in detail; please feel free to combine some of the steps (without sacrificing code readability of course!).

Now that we have dummy data we want to create a `window` over `id` that is ordered by `age` in descending order. We then want to create the following columns:

- `age_lag` showing the next highest age within an id
- `age_diff` the difference between `age` and `age_lag` columns
- `age_order` numbered oldest to youngest
- `top_two_age_diff` returns the age difference between two oldest entries within an id, 0 for other rows
- `age_diff_flag` flag to tell us if the age difference is greater than some threshold, 5 chosen here

The two columns at the end are intermediary, therefore these could be dropped if no longer needed, using `.drop`.
````{tabs}
```{code-tab} py
order_window = Window.partitionBy("group").orderBy(F.desc("age"))

df = df.withColumn("age_lag", F.lag(F.col("age"), -1).over(order_window))
df = df.withColumn("age_diff", F.col("age") - F.col("age_lag"))
df = df.withColumn("age_order", F.row_number().over(order_window))
df = df.withColumn("top_two_age_diff", F.when((F.col("age_order") == 1), F.col("age_diff")).otherwise(0))
df = df.withColumn("age_diff_flag", F.when(F.col("top_two_age_diff") > 5, 1).otherwise(0))

df.show()
```

```{code-tab} r R

df <- df %>%
    group_by(group) %>%
    arrange(desc(age)) %>%
    mutate(age_lag = lag(age,n = -1))  %>%
    mutate(age_diff = age - age_lag) %>%
    group_by(group) %>%
    mutate(age_order = row_number()) %>%
    mutate(top_two_age_diff = ifelse(age_order == 1,
                                  age_diff,
                                  0
                                  )) %>%
    mutate(age_diff_flag = ifelse(top_two_age_diff > 5,
                                  1,
                                  0
                                  ))

print(df, n = 15)

```
````

````{tabs}

```{code-tab} plaintext Python Output
+-----+---+-------+--------+---------+----------------+-------------+
|group|age|age_lag|age_diff|age_order|top_two_age_diff|age_diff_flag|
+-----+---+-------+--------+---------+----------------+-------------+
|    0| 10|      9|       1|        1|               1|            0|
|    0|  9|      7|       2|        2|               0|            0|
|    0|  7|   null|    null|        3|               0|            0|
|    1|  9|      6|       3|        1|               3|            0|
|    1|  6|      5|       1|        2|               0|            0|
|    1|  5|   null|    null|        3|               0|            0|
|    3| 10|      4|       6|        1|               6|            1|
|    3|  4|      1|       3|        2|               0|            0|
|    3|  1|   null|    null|        3|               0|            0|
|    2| 10|      4|       6|        1|               6|            1|
|    2|  4|      3|       1|        2|               0|            0|
|    2|  3|   null|    null|        3|               0|            0|
|    4|  8|      7|       1|        1|               1|            0|
|    4|  7|      3|       4|        2|               0|            0|
|    4|  3|   null|    null|        3|               0|            0|
+-----+---+-------+--------+---------+----------------+-------------+
```

```{code-tab} plaintext R Output
# Source:     spark<?> [?? x 7]
# Groups:     group
# Ordered by: desc(age)
   group   age age_lag age_diff age_order top_two_age_diff age_diff_flag
   <int> <dbl>   <dbl>    <dbl>     <int>            <dbl>         <dbl>
 1     1     9       8        1         1                1             0
 2     1     8       4        4         2                0             0
 3     1     4      NA       NA         3                0             0
 4     3    10       8        2         1                2             0
 5     3     8       5        3         2                0             0
 6     3     5      NA       NA         3                0             0
 7     0     8       8        0         1                0             0
 8     0     8       4        4         2                0             0
 9     0     4      NA       NA         3                0             0
10     2     9       6        3         1                3             0
11     2     6       2        4         2                0             0
12     2     2      NA       NA         3                0             0
13     4     6       2        4         1                4             0
14     4     2       1        1         2                0             0
15     4     1      NA       NA         3                0             0
```
````
As you can see in the table above, a flag column has been created with the values 0 and 1. If the age difference is greater than 5 then the flag = 1, if less than 5 then the flag = 0. 

### Further Resources

Spark at the ONS articles:
- [Cross joins](https://best-practice-and-impact.github.io/ons-spark/spark-functions/cross-joins.html)
- [Window functions](https://best-practice-and-impact.github.io/ons-spark/spark-functions/window-functions.html)