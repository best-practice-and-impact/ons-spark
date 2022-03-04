## Rounding differences in Python, R and Spark

Python, R and Spark have different ways of rounding numbers which end in $.5$; Python and R round to the **nearest even integer** (sometimes called *bankers rounding*), whereas Spark will round **away from zero** (up in the conventional mathematical way for positive numbers, and round down for negative numbers), in the same way as in Excel.

This can be confusing when using PySpark and sparklyr if you are used to the behaviour in Python and R.

### Comparison of rounding methods

Create a DataFrame with numbers all ending in `.5`, both positive and negative:
````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession, functions as F
import pandas as pd
import numpy as np

spark = (SparkSession.builder.master("local[2]")
         .appName("rounding")
         .getOrCreate())

sdf = spark.range(-7, 8, 2).select((F.col("id") / 2).alias("half_id"))
sdf.show()
```

```{code-tab} r R

#library(sparklyr)
#library(dplyr)
library(magrittr)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "window-functions",
    config = sparklyr::spark_config())

sdf <- sparklyr:::sdf_seq(sc, -7, 8, 2) %>%
    sparklyr::mutate(half_id = id / 2) %>%
    sparklyr::select(half_id)
    
sdf %>%
    sparklyr::collect() %>%
    print()

```
````

```plaintext
+-------+
|half_id|
+-------+
|   -3.5|
|   -2.5|
|   -1.5|
|   -0.5|
|    0.5|
|    1.5|
|    2.5|
|    3.5|
+-------+
```

```plaintext
# A tibble: 8 × 1
  half_id
    <dbl>
1    -3.5
2    -2.5
3    -1.5
4    -0.5
5     0.5
6     1.5
7     2.5
8     3.5
```
Round using Spark; this will round away from zero (up for positive numbers and down for negative):
````{tabs}
```{code-tab} py
sdf = sdf.withColumn("spark_round", F.round("half_id"))
sdf.toPandas()
```

```{code-tab} r R

sdf <- sdf %>%
    sparklyr::mutate(spark_round = round(half_id))

sdf %>%
    sparklyr::collect() %>%
    print()

```
````

```plaintext
   half_id  spark_round
0     -3.5         -4.0
1     -2.5         -3.0
2     -1.5         -2.0
3     -0.5         -1.0
4      0.5          1.0
5      1.5          2.0
6      2.5          3.0
7      3.5          4.0
```

```plaintext
# A tibble: 8 × 2
  half_id spark_round
    <dbl>       <dbl>
1    -3.5          -4
2    -2.5          -3
3    -1.5          -2
4    -0.5          -1
5     0.5           1
6     1.5           2
7     2.5           3
8     3.5           4
```
Now try using Python/R; this will use the bankers method of rounding:
````{tabs}
```{code-tab} py
pdf = sdf.toPandas()
pdf["python_round"] = round(pdf["half_id"], 0)
pdf
```

```{code-tab} r R

tdf <- sdf %>%
    sparklyr::collect() %>%
    sparklyr::mutate(r_round = round(half_id)) %>%
    print()

```
````

```plaintext
   half_id  spark_round  python_round
0     -3.5         -4.0          -4.0
1     -2.5         -3.0          -2.0
2     -1.5         -2.0          -2.0
3     -0.5         -1.0          -0.0
4      0.5          1.0           0.0
5      1.5          2.0           2.0
6      2.5          3.0           2.0
7      3.5          4.0           4.0
```

```plaintext
# A tibble: 8 × 3
  half_id spark_round r_round
    <dbl>       <dbl>   <dbl>
1    -3.5          -4      -4
2    -2.5          -3      -2
3    -1.5          -2      -2
4    -0.5          -1       0
5     0.5           1       0
6     1.5           2       2
7     2.5           3       2
8     3.5           4       4
```
The two methods have returned different results, despite both using functions named `round()`.

Just like in Python, pandas and numpy also use bankers rounding:
````{tabs}
```{code-tab} py
pdf["pd_round"] = pdf["half_id"].round()
pdf["np_round"] = np.round(pdf["half_id"])
pdf
```
````

```plaintext
   half_id  spark_round  python_round  pd_round  np_round
0     -3.5         -4.0          -4.0      -4.0      -4.0
1     -2.5         -3.0          -2.0      -2.0      -2.0
2     -1.5         -2.0          -2.0      -2.0      -2.0
3     -0.5         -1.0          -0.0      -0.0      -0.0
4      0.5          1.0           0.0       0.0       0.0
5      1.5          2.0           2.0       2.0       2.0
6      2.5          3.0           2.0       2.0       2.0
7      3.5          4.0           4.0       4.0       4.0
```
You can use the Python and R style of bankers rounding in Spark with `bround()`:
````{tabs}
```{code-tab} py
sdf = sdf.withColumn("spark_bround", F.bround("half_id"))
sdf.toPandas()
```

```{code-tab} r R

sdf <- sdf %>%
    sparklyr::mutate(spark_bround = bround(half_id))

sdf %>%
    sparklyr::collect() %>%
    print()

```
````

```plaintext
   half_id  spark_round  spark_bround
0     -3.5         -4.0          -4.0
1     -2.5         -3.0          -2.0
2     -1.5         -2.0          -2.0
3     -0.5         -1.0           0.0
4      0.5          1.0           0.0
5      1.5          2.0           2.0
6      2.5          3.0           2.0
7      3.5          4.0           4.0
```

```plaintext
# A tibble: 8 × 3
  half_id spark_round spark_bround
    <dbl>       <dbl>        <dbl>
1    -3.5          -4           -4
2    -2.5          -3           -2
3    -1.5          -2           -2
4    -0.5          -1            0
5     0.5           1            0
6     1.5           2            2
7     2.5           3            2
8     3.5           4            4
```
### Other information on rounding

#### UDFs and `spark_apply()`

User Defined Functions (UDFs) in Python, and R code ran on the Spark cluster with `spark_apply()` will use bankers rounding, in common with Python and R.

#### Python 2
 
The rounding method changed to bankers rounding in Python 3. In Python 2, it used the round away from zero method, the same as Spark. It is strongly recommended to use Python 3 for any new code development. Spark 3 has dropped support for Python 2.

#### Other common software

Both Excel and SPSS Statistics use the Spark method of rounding away from zero. If you are new to coding and are learning Python or R predominately to use Spark, be careful when using regular Python or R functions.

#### Testing

Given that there are different ways of rounding depending on the language used, it is a good idea to thoroughly unit test your functions to ensure that they behave as expected.

### Further Resources

TBC