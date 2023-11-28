## Window Functions in Spark

Window functions use values from other rows within the same group, or *window*, and return a value in a new column for every row. This can be in the form of aggregations (similar to a [`.groupBy()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)/[`group_by()`](https://dplyr.tidyverse.org/reference/group_by.html) but preserving the original DataFrame), ranking rows within groups, or returning values from previous rows. If you're familiar with SQL then a window function in PySpark works in the same way.

This article explains how to use window functions in three ways: for aggregation, ranking, and referencing the previous row. An SQL example is also given.

### Window Functions for Aggregations

You can use a window function for aggregations. Rather than returning an aggregated DataFrame, the result of the aggregation will be placed in a new column.

One example of where this is useful is for deriving a total to be used as the denominator for another calculation. For instance, in the Animal Rescue data we may want to work out what percentage of animals rescued each year are dogs. We can do this by getting the total of all animals by year, then dividing each animal group count by this. 

First, import the relevant packages and start a Spark session. To use window functions in PySpark, we need to import [`Window`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html) from `pyspark.sql.window`. No extra packages are needed for sparklyr, as Spark functions are referenced inside [`mutate()`](https://dplyr.tidyverse.org/reference/mutate.html).
````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import yaml

with open("../../../config.yaml") as f:
    config = yaml.safe_load(f)
    
spark = (SparkSession.builder.master("local[2]")
         .appName("window-functions")
         .getOrCreate())

rescue_path = config["rescue_path"]
```

```{code-tab} r R

library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "window-functions",
    config = sparklyr::spark_config())

config <- yaml::yaml.load_file("ons-spark/config.yaml")

```
````
In this example we will use an aggregated version of the Animal Rescue data, containing `animal_group`, `cal_year` and `animal_count`.
````{tabs}
```{code-tab} py
rescue_agg = (
    spark.read.parquet(rescue_path)
    .withColumn("animal_group", F.initcap(F.col("animal_group")))
    .groupBy("animal_group", "cal_year")
    .agg(F.count("animal_group").alias("animal_count")))

rescue_agg.show(5, truncate=False)
```

```{code-tab} r R

rescue_agg <- sparklyr::spark_read_parquet(sc, config$rescue_path) %>%
    sparklyr::mutate(animal_group = initcap(animal_group)) %>%
    dplyr::group_by(animal_group, cal_year) %>%
    dplyr::summarise(animal_count = n()) %>%
    dplyr::ungroup()
    
rescue_agg %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+------------------------------------------------+--------+------------+
|animal_group                                    |cal_year|animal_count|
+------------------------------------------------+--------+------------+
|Snake                                           |2018    |1           |
|Deer                                            |2015    |6           |
|Unknown - Animal Rescue From Water - Farm Animal|2014    |1           |
|Unknown - Domestic Animal Or Pet                |2017    |8           |
|Bird                                            |2012    |112         |
+------------------------------------------------+--------+------------+
only showing top 5 rows
```

```{code-tab} plaintext R Output
# A tibble: 5 × 3
  animal_group                     cal_year animal_count
  <chr>                               <int>        <dbl>
1 Dog                                  2018           91
2 Horse                                2013           16
3 Squirrel                             2011            4
4 Fox                                  2017           33
5 Unknown - Domestic Animal Or Pet     2015           29
```
````
We want to calculate the percentage of animals rescued each year that are dogs. To do this, we first need to calculate the annual totals, and can then divide the number of dogs in each year by this.

We could create a new DataFrame by grouping and aggregating and then joining back to the original DF; this would get the correct result, but a window function is much more efficient as it will reduce the number of shuffles required, as well as making the code more succinct and readable.

The syntax is quite different between PySpark and sparklyr, although the principle is identical in each, and Spark will process them in the same way. The process for using a window function for aggregation in PySpark is as follows:
- First, use [`.withColumn()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html), as the result is stored in a new column in the DataFrame.
- Then do the aggregation: [`F.sum("animal_count")`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sum.html).
- Then perform this [over](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.over.html) a window with `.over(Window.partitionBy("cal_year"))`. Note that this uses [`.partitionBy()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.partitionBy.html) rather than `.groupBy()` (for some window functions you will also use [`.orderBy()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.orderBy.html), but we do not need to here).

In sparklyr:
- Use `group_by(cal_year)` to partition the data.
- Then define a new column, `annual_count`, as [`sum(animal_count)`](https://spark.apache.org/docs/latest/api/sql/index.html#sum)) inside `mutate()` (rather than [`summarise()`](https://dplyr.tidyverse.org/reference/summarise.html), which is used for regular aggregations).
- Finally, [`ungroup()`](https://dplyr.tidyverse.org/reference/group_by.html) to remove the grouping from the DataFrame.
````{tabs}
```{code-tab} py
rescue_annual = (rescue_agg
                 .withColumn("annual_count",
                     F.sum("animal_count").over(Window.partitionBy("cal_year"))))
```

```{code-tab} r R

rescue_annual <- rescue_agg %>%
    dplyr::group_by(cal_year) %>%
    sparklyr::mutate(annual_count = sum(animal_count)) %>%
    dplyr::ungroup()

```
````
Now display the DF, using `Cat`, `Dog` and `Hamster` between `2012` and `2014` as an example:
````{tabs}
```{code-tab} py
(rescue_annual
    .filter(
        (F.col("animal_group").isin("Cat", "Dog", "Hamster")) &
        (F.col("cal_year").between(2012, 2014)))
    .orderBy("cal_year", "animal_group")
    .show())
```

```{code-tab} r R

rescue_annual %>%
    sparklyr::filter(
        (animal_group %in% c("Cat", "Dog", "Hamster")) &
        (cal_year %in% 2012:2014)) %>%
    sparklyr::sdf_sort(c("cal_year", "animal_group")) %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+------------+--------+------------+------------+
|animal_group|cal_year|animal_count|annual_count|
+------------+--------+------------+------------+
|         Cat|    2012|         305|         603|
|         Dog|    2012|         100|         603|
|         Cat|    2013|         313|         585|
|         Dog|    2013|          93|         585|
|     Hamster|    2013|           3|         585|
|         Cat|    2014|         298|         583|
|         Dog|    2014|          90|         583|
|     Hamster|    2014|           1|         583|
+------------+--------+------------+------------+
```

```{code-tab} plaintext R Output
# A tibble: 8 × 4
  animal_group cal_year animal_count annual_count
  <chr>           <int>        <dbl>        <dbl>
1 Cat              2012          305          603
2 Dog              2012          100          603
3 Cat              2013          313          585
4 Dog              2013           93          585
5 Hamster          2013            3          585
6 Cat              2014          298          583
7 Dog              2014           90          583
8 Hamster          2014            1          583
```
````
The values in `annual_count` are repeated in every year, as the original rows in the DF have been preserved. Had we aggregated this in the usual way we would have lost the `animal_group` and `animal_count` columns, and only returned one `annual_count` for each `cal_year`.

Once we have the `annual_count` column we can complete our calculation with a simple narrow transformation to get the percentage and filter on `"Dog"`:
````{tabs}
```{code-tab} py
rescue_annual = (rescue_annual
                 .withColumn("animal_pct",
                             F.round(
                                 (F.col("animal_count") / F.col("annual_count")) * 100, 2)))

rescue_annual.filter(F.col("animal_group") == "Dog").orderBy("cal_year").show()
```

```{code-tab} r R

rescue_annual <- rescue_annual %>%
    sparklyr::mutate(animal_pct = round((animal_count / annual_count) * 100, 2))

rescue_annual %>%
    sparklyr::filter(animal_group == "Dog") %>%
    sparklyr::sdf_sort("animal_group") %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+------------+--------+------------+------------+----------+
|animal_group|cal_year|animal_count|annual_count|animal_pct|
+------------+--------+------------+------------+----------+
|         Dog|    2009|         132|         568|     23.24|
|         Dog|    2010|         122|         611|     19.97|
|         Dog|    2011|         103|         620|     16.61|
|         Dog|    2012|         100|         603|     16.58|
|         Dog|    2013|          93|         585|      15.9|
|         Dog|    2014|          90|         583|     15.44|
|         Dog|    2015|          88|         540|      16.3|
|         Dog|    2016|         107|         604|     17.72|
|         Dog|    2017|          81|         539|     15.03|
|         Dog|    2018|          91|         609|     14.94|
|         Dog|    2019|           1|          36|      2.78|
+------------+--------+------------+------------+----------+
```

```{code-tab} plaintext R Output
# A tibble: 11 × 5
   animal_group cal_year animal_count annual_count animal_pct
   <chr>           <int>        <dbl>        <dbl>      <dbl>
 1 Dog              2009          132          568      23.2 
 2 Dog              2011          103          620      16.6 
 3 Dog              2014           90          583      15.4 
 4 Dog              2015           88          540      16.3 
 5 Dog              2019            1           36       2.78
 6 Dog              2010          122          611      20.0 
 7 Dog              2016          107          604      17.7 
 8 Dog              2017           81          539      15.0 
 9 Dog              2012          100          603      16.6 
10 Dog              2013           93          585      15.9 
11 Dog              2018           91          609      14.9 
```
````
This example used `F.sum()`/`sum()` but other aggregations are possible too, e.g. [`F.mean()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.mean.html)/[`mean()`](https://spark.apache.org/docs/latest/api/sql/index.html#mean), [`F.max()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.max.html)/[`max()`](https://spark.apache.org/docs/latest/api/sql/index.html#max). In PySpark, use multiple `.withColumn()` statements; in sparklyr, you can combine them in `mutate()`. In this example we filter on `"Snake"`:
````{tabs}
```{code-tab} py
rescue_annual = (rescue_annual
          .withColumn("avg_count",
                     F.mean("animal_count").over(Window.partitionBy("cal_year")))
          .withColumn("max_count",
                     F.max("animal_count").over(Window.partitionBy("cal_year"))                     
                     ))
          
rescue_annual.filter(F.col("animal_group") == "Snake").show()
```

```{code-tab} r R

rescue_annual = rescue_annual %>%
    dplyr::group_by(cal_year) %>%
    sparklyr::mutate(
        avg_count = mean(animal_count),
        max_count = max(animal_count)) %>%
    dplyr::ungroup()

rescue_annual %>%
    sparklyr::filter(animal_group == "Snake") %>%
    head(10) %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+------------+--------+------------+------------+----------+------------------+---------+
|animal_group|cal_year|animal_count|annual_count|animal_pct|         avg_count|max_count|
+------------+--------+------------+------------+----------+------------------+---------+
|       Snake|    2018|           1|         609|      0.16|           38.0625|      305|
|       Snake|    2013|           2|         585|      0.34|              45.0|      313|
|       Snake|    2009|           3|         568|      0.53| 37.86666666666667|      263|
|       Snake|    2016|           1|         604|      0.17|43.142857142857146|      297|
|       Snake|    2017|           1|         539|      0.19|              49.0|      258|
+------------+--------+------------+------------+----------+------------------+---------+
```

```{code-tab} plaintext R Output
# A tibble: 5 × 7
  animal_group cal_year animal_count annual_count animal_pct avg_count max_count
  <chr>           <int>        <dbl>        <dbl>      <dbl>     <dbl>     <dbl>
1 Snake            2009            3          568       0.53      37.9       263
2 Snake            2016            1          604       0.17      43.1       297
3 Snake            2017            1          539       0.19      49         258
4 Snake            2013            2          585       0.34      45         313
5 Snake            2018            1          609       0.16      38.1       305
```
````
The alternative to window functions is creating a new grouped and aggregated DF, then joining it back to the original one. As well as being less efficient, the code will also be harder to read. For example:
````{tabs}
```{code-tab} py
rescue_counts = rescue_agg.groupBy("cal_year").agg(F.sum("animal_count").alias("annual_count"))
rescue_annual_alternative = rescue_agg.join(rescue_counts, on="cal_year", how="left")
rescue_annual_alternative.filter(F.col("animal_group") == "Dog").orderBy("cal_year").show()
```

```{code-tab} r R

rescue_counts <- rescue_agg %>%
    dplyr::group_by(cal_year) %>%
    dplyr::summarise(annual_count = sum(animal_count))

rescue_annual_alternative <- rescue_agg %>%
    sparklyr::left_join(rescue_counts, by="cal_year")

rescue_annual_alternative %>%
    sparklyr::filter(animal_group == "Dog") %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+--------+------------+------------+------------+
|cal_year|animal_group|animal_count|annual_count|
+--------+------------+------------+------------+
|    2009|         Dog|         132|         568|
|    2010|         Dog|         122|         611|
|    2011|         Dog|         103|         620|
|    2012|         Dog|         100|         603|
|    2013|         Dog|          93|         585|
|    2014|         Dog|          90|         583|
|    2015|         Dog|          88|         540|
|    2016|         Dog|         107|         604|
|    2017|         Dog|          81|         539|
|    2018|         Dog|          91|         609|
|    2019|         Dog|           1|          36|
+--------+------------+------------+------------+
```

```{code-tab} plaintext R Output
# A tibble: 11 × 4
   animal_group cal_year animal_count annual_count
   <chr>           <int>        <dbl>        <dbl>
 1 Dog              2018           91          609
 2 Dog              2016          107          604
 3 Dog              2014           90          583
 4 Dog              2017           81          539
 5 Dog              2019            1           36
 6 Dog              2013           93          585
 7 Dog              2009          132          568
 8 Dog              2012          100          603
 9 Dog              2015           88          540
10 Dog              2010          122          611
11 Dog              2011          103          620
```
````
### Using Window Functions for Ranking

Window functions can also be ordered as well as grouped. This can be combined with [`F.rank()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rank.html)/[`rank()`](https://spark.apache.org/docs/latest/api/sql/index.html#rank) or [`F.row_number()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.row_number.html)/[`row_number()`](https://spark.apache.org/docs/latest/api/sql/index.html#rank) to get ranks within groups. For instance, we can get the ranking of the most commonly rescued animals by year, then filter on the top three.

The syntax is again different between PySpark and sparklyr. In PySpark, use the same method as described above for aggregations, but replace `F.sum()` with `F.rank()` (or another ordered function), and add `orderBy()`. In this example, use [`F.desc("animal_count")`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.desc.html) to sort descending. The `.partitionBy()` step is optional; without a `.partitionBy()` it will treat the whole DataFrame as one group.

In sparklyr, the method is also almost the same as using aggregations. The ordering is done directly with the `rank()` function. `desc(animal_count)` is used to sort descending.
````{tabs}
```{code-tab} py
rescue_rank = (
    rescue_agg
    .withColumn("rank",
                F.rank().over(
                    Window.partitionBy("cal_year").orderBy(F.desc("animal_count")))))
```

```{code-tab} r R

rescue_rank = rescue_agg %>%
    dplyr::group_by(cal_year) %>%
    sparklyr::mutate(rank = rank(desc(animal_count))) %>%
    dplyr::ungroup()

```
````
Once we have the `rank` column we can filter on those less than or equal to `3`, to get the top 3 animals rescued by year:
````{tabs}
```{code-tab} py
rescue_rank.filter(F.col("rank") <= 3).orderBy("cal_year", "rank").show(12, truncate=False)
```

```{code-tab} r R

rescue_rank %>%
    sparklyr::filter(rank <= 3) %>%
    sparklyr::sdf_sort(c("cal_year", "rank")) %>%
    head(12) %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+------------+--------+------------+----+
|animal_group|cal_year|animal_count|rank|
+------------+--------+------------+----+
|Cat         |2009    |263         |1   |
|Dog         |2009    |132         |2   |
|Bird        |2009    |89          |3   |
|Cat         |2010    |297         |1   |
|Dog         |2010    |122         |2   |
|Bird        |2010    |99          |3   |
|Cat         |2011    |309         |1   |
|Bird        |2011    |120         |2   |
|Dog         |2011    |103         |3   |
|Cat         |2012    |305         |1   |
|Bird        |2012    |112         |2   |
|Dog         |2012    |100         |3   |
+------------+--------+------------+----+
only showing top 12 rows
```

```{code-tab} plaintext R Output
# A tibble: 12 × 4
   animal_group cal_year animal_count  rank
   <chr>           <int>        <dbl> <int>
 1 Cat              2009          263     1
 2 Dog              2009          132     2
 3 Bird             2009           89     3
 4 Cat              2010          297     1
 5 Dog              2010          122     2
 6 Bird             2010           99     3
 7 Cat              2011          309     1
 8 Bird             2011          120     2
 9 Dog              2011          103     3
10 Cat              2012          305     1
11 Bird             2012          112     2
12 Dog              2012          100     3
```
````
Another common use case is getting just the top row from each group:
````{tabs}
```{code-tab} py
rescue_rank.filter(F.col("rank") == 1).orderBy("cal_year").show(truncate=False)
```

```{code-tab} r R

rescue_rank %>%
    sparklyr::filter(rank == 1) %>%
    sparklyr::sdf_sort("cal_year") %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+------------+--------+------------+----+
|animal_group|cal_year|animal_count|rank|
+------------+--------+------------+----+
|Cat         |2009    |263         |1   |
|Cat         |2010    |297         |1   |
|Cat         |2011    |309         |1   |
|Cat         |2012    |305         |1   |
|Cat         |2013    |313         |1   |
|Cat         |2014    |298         |1   |
|Cat         |2015    |263         |1   |
|Cat         |2016    |297         |1   |
|Cat         |2017    |258         |1   |
|Cat         |2018    |305         |1   |
|Cat         |2019    |16          |1   |
+------------+--------+------------+----+
```

```{code-tab} plaintext R Output
# A tibble: 11 × 4
   animal_group cal_year animal_count  rank
   <chr>           <int>        <dbl> <int>
 1 Cat              2009          263     1
 2 Cat              2010          297     1
 3 Cat              2011          309     1
 4 Cat              2012          305     1
 5 Cat              2013          313     1
 6 Cat              2014          298     1
 7 Cat              2015          263     1
 8 Cat              2016          297     1
 9 Cat              2017          258     1
10 Cat              2018          305     1
11 Cat              2019           16     1
```
````
#### Comparison of ranking methods

Note that you can have duplicate ranks within each group when using `rank()`; if this is not desirable then one method is to partition by more columns to break ties. There are also alternatives to `rank()` depending on your use case:

- `F.rank()`/`rank()` will assign the same value to ties.
- [`F.dense_rank()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dense_rank.html)/[`dense_rank()`](https://spark.apache.org/docs/latest/api/sql/index.html#dense_rank) will not skip a rank after ties.
- `F.row_number()`/`row_number()` will give a unique number to each row within the grouping specified. Note that this can be non-deterministic if there are duplicate rows for the ordering condition specified. This can be avoided by specifying extra columns to essentially use as a tiebreaker.

We can see the difference by comparing the three methods:
````{tabs}
```{code-tab} py
rank_comparison = (rescue_agg
    .withColumn("rank",
                F.rank().over(
                    Window
                    .partitionBy("cal_year")
                    .orderBy(F.desc("animal_count"))))
    .withColumn("dense_rank",
                F.dense_rank().over(
                    Window
                    .partitionBy("cal_year")
                    .orderBy(F.desc("animal_count"))))
    .withColumn("row_number",
                F.row_number().over(
                    Window
                    .partitionBy("cal_year")
                    .orderBy(F.desc("animal_count"))))
)

(rank_comparison
    .filter(F.col("cal_year") == 2012)
    .orderBy("cal_year", "row_number")
    .show(truncate=False))
```

```{code-tab} r R

rank_comparison <- rescue_agg %>%
    dplyr::group_by(cal_year) %>%
    sparklyr::mutate(
        rank = rank(desc(animal_count)),
        dense_rank = dense_rank(desc(animal_count)),
        row_number = row_number(desc(animal_count))) %>%
    dplyr::ungroup()

rank_comparison %>%
    sparklyr::filter(cal_year == 2012) %>%
    sparklyr::sdf_sort(c("cal_year", "row_number")) %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+------------------------------------------------+--------+------------+----+----------+----------+
|animal_group                                    |cal_year|animal_count|rank|dense_rank|row_number|
+------------------------------------------------+--------+------------+----+----------+----------+
|Cat                                             |2012    |305         |1   |1         |1         |
|Bird                                            |2012    |112         |2   |2         |2         |
|Dog                                             |2012    |100         |3   |3         |3         |
|Horse                                           |2012    |28          |4   |4         |4         |
|Unknown - Domestic Animal Or Pet                |2012    |18          |5   |5         |5         |
|Fox                                             |2012    |14          |6   |6         |6         |
|Deer                                            |2012    |7           |7   |7         |7         |
|Squirrel                                        |2012    |4           |8   |8         |8         |
|Unknown - Wild Animal                           |2012    |4           |8   |8         |9         |
|Unknown - Heavy Livestock Animal                |2012    |4           |8   |8         |10        |
|Cow                                             |2012    |3           |11  |9         |11        |
|Ferret                                          |2012    |1           |12  |10        |12        |
|Lamb                                            |2012    |1           |12  |10        |13        |
|Sheep                                           |2012    |1           |12  |10        |14        |
|Unknown - Animal Rescue From Water - Farm Animal|2012    |1           |12  |10        |15        |
+------------------------------------------------+--------+------------+----+----------+----------+
```

```{code-tab} plaintext R Output
# A tibble: 15 × 6
   animal_group                cal_year animal_count  rank dense_rank row_number
   <chr>                          <int>        <dbl> <int>      <int>      <int>
 1 Cat                             2012          305     1          1          1
 2 Bird                            2012          112     2          2          2
 3 Dog                             2012          100     3          3          3
 4 Horse                           2012           28     4          4          4
 5 Unknown - Domestic Animal …     2012           18     5          5          5
 6 Fox                             2012           14     6          6          6
 7 Deer                            2012            7     7          7          7
 8 Unknown - Wild Animal           2012            4     8          8          8
 9 Squirrel                        2012            4     8          8          9
10 Unknown - Heavy Livestock …     2012            4     8          8         10
11 Cow                             2012            3    11          9         11
12 Sheep                           2012            1    12         10         12
13 Unknown - Animal Rescue Fr…     2012            1    12         10         13
14 Lamb                            2012            1    12         10         14
15 Ferret                          2012            1    12         10         15
```
````
For all the values where `animal_count` is `4`, `rank` and `dense_rank` have `8`, whereas `row_number` gives a unique number from `8` to `10`. As no other sorting columns were specified, these three rows could be assigned differently on subsequent runs of the code.

For `animal_count` less than  `4`, `dense_rank` has left no gap in the ranking sequence, whereas `rank` will leave gaps.

`row_number` has a unique value for each row, even for tied values.

#### Generating unique row numbers

Spark DataFrames do not have an index in the same way as pandas or base R DataFrames as they are partitioned on the cluster. You can however use `row_number()` to generate a unique identifier for each row. 

Whereas in the previous example we ranked within groups, here we need to treat the whole DataFrame as one group.

To do this in PySpark, use just `Window.orderBy(col1, col2, ...)` without the `partitionBy()`. In sparklyr, just use `mutate()` without `group_by()` and `ungroup()`.

Remember to be careful as this can be non-deterministic if there are duplicate rows for the ordering condition specified.
````{tabs}
```{code-tab} py
(rescue_agg
    .withColumn("row_number",
                F.row_number().over(Window.orderBy("cal_year")))
    .show(10, truncate=False))
```

```{code-tab} r R

rescue_agg %>%
    sparklyr::mutate(row_number = row_number(cal_year)) %>%
    head(10) %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+--------------------------------+--------+------------+----------+
|animal_group                    |cal_year|animal_count|row_number|
+--------------------------------+--------+------------+----------+
|Hedgehog                        |2009    |1           |1         |
|Dog                             |2009    |132         |2         |
|Sheep                           |2009    |1           |3         |
|Deer                            |2009    |8           |4         |
|Lizard                          |2009    |1           |5         |
|Unknown - Heavy Livestock Animal|2009    |14          |6         |
|Unknown - Wild Animal           |2009    |6           |7         |
|Rabbit                          |2009    |1           |8         |
|Bird                            |2009    |89          |9         |
|Horse                           |2009    |19          |10        |
+--------------------------------+--------+------------+----------+
only showing top 10 rows
```

```{code-tab} plaintext R Output
# A tibble: 10 × 4
   animal_group                     cal_year animal_count row_number
   <chr>                               <int>        <dbl>      <int>
 1 Hedgehog                             2009            1          1
 2 Dog                                  2009          132          2
 3 Squirrel                             2009            4          3
 4 Fox                                  2009           16          4
 5 Cat                                  2009          263          5
 6 Unknown - Domestic Animal Or Pet     2009           10          6
 7 Bird                                 2009           89          7
 8 Unknown - Heavy Livestock Animal     2009           14          8
 9 Unknown - Wild Animal                2009            6          9
10 Sheep                                2009            1         10
```
````
### Reference other rows with `lag()` and `lead()`

The window function [`F.lag()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lag.html)/[`lag()`](https://spark.apache.org/docs/latest/api/sql/index.html#lag)  allows you to reference the values of previous rows within a group, and [`F.lead()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lead.html)/[`lead()`](https://spark.apache.org/docs/latest/api/sql/index.html#lead) will do the same for subsequent rows. You can specify how many previous rows you want to reference with the `count` argument. By default this is `1`. Note that `count` can be negative, so `lag(col, count=1)` is the same as `lead(col, count=-1)`.

The first or last row within the window partition will be null values, as they do not have a previous or subsequent row to reference. This can be changed by setting the `default` parameter, which by default is `None`.

These window functions differ from `rank()` and `row_number()` as they are referencing values, rather than returning a rank. They do however use ordering in the same way.

We can use `lag()` or `lead()` to get the number of animals rescued in the previous year, with the intention of calculating the annual change. The process for this is identical to the Using Window Functions for Ranking section, just using `lag()` as the function within the window.
````{tabs}
```{code-tab} py
(rescue_agg
    .withColumn("previous_count",
                F.lag("animal_count").over(
                    Window.partitionBy("animal_group").orderBy("cal_year")))
    .show(10, truncate=False))
```

```{code-tab} r R

rescue_agg %>%
    dplyr::group_by(animal_group) %>%
    sparklyr::mutate(previous_count = lag(animal_count, order_by = cal_year)) %>%
    dplyr::ungroup() %>%
    head(10) %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+------------------------------------------------+--------+------------+--------------+
|animal_group                                    |cal_year|animal_count|previous_count|
+------------------------------------------------+--------+------------+--------------+
|Unknown - Animal Rescue From Water - Farm Animal|2012    |1           |null          |
|Unknown - Animal Rescue From Water - Farm Animal|2014    |1           |1             |
|Unknown - Animal Rescue From Water - Farm Animal|2019    |1           |1             |
|Cow                                             |2010    |2           |null          |
|Cow                                             |2012    |3           |2             |
|Cow                                             |2014    |1           |3             |
|Cow                                             |2016    |1           |1             |
|Horse                                           |2009    |19          |null          |
|Horse                                           |2010    |15          |19            |
|Horse                                           |2011    |22          |15            |
+------------------------------------------------+--------+------------+--------------+
only showing top 10 rows
```

```{code-tab} plaintext R Output
# A tibble: 10 × 4
   animal_group cal_year animal_count previous_count
   <chr>           <int>        <dbl>          <dbl>
 1 Bird             2009           89             NA
 2 Bird             2010           99             89
 3 Bird             2011          120             99
 4 Bird             2012          112            120
 5 Bird             2013           85            112
 6 Bird             2014          110             85
 7 Bird             2015          106            110
 8 Bird             2016          120            106
 9 Bird             2017          124            120
10 Bird             2018          126            124
```
````
Be careful if using `lag()` with incomplete data: where there were no animals rescued in a year the `previous_count` will not be correct.

There are several ways to resolve this; one method is using a [cross join](../spark-functions/cross-join) to get all the combinations of `animal_group` and `cal_year`, join the `rescue_agg` to this, fill the null values with `0`, and then do the window calculation:
````{tabs}
```{code-tab} py
# Create a DF of all combinations of animal_group and cal_year
all_animals_years = (rescue_agg
                     .select("animal_group")
                     .distinct()
                     .crossJoin(
                         rescue_agg
                         .select("cal_year")
                         .distinct()))

# Use this DF as a base to join the rescue_agg DF to
rescue_agg_prev = (
    all_animals_years
    .join(rescue_agg, on=["animal_group", "cal_year"], how="left")
    # Replace null with 0
    .fillna(0, "animal_count")
    # lag will then reference previous year, even if 0
    .withColumn("previous_count",
                F.lag("animal_count").over(
                    Window.partitionBy("animal_group").orderBy("cal_year"))))

rescue_agg_prev.orderBy("animal_group", "cal_year").show(truncate=False)
```

```{code-tab} r R

# Create a DF of all combinations of animal_group and cal_year
all_animals_years <- rescue_agg %>%
    sparklyr::select(animal_group) %>%
    sparklyr::sdf_distinct() %>%
    sparklyr::full_join(
        rescue_agg %>% sparklyr::select(cal_year) %>% sparklyr::sdf_distinct(),
        by=character()) %>%
    sparklyr::sdf_sort(c("animal_group", "cal_year"))

# Use this DF as a base to join the rescue_agg DF to
rescue_agg_prev <- all_animals_years %>%
    sparklyr::left_join(rescue_agg, by=c("animal_group", "cal_year")) %>%
    # Replace null with 0
    sparklyr::mutate(animal_count = ifnull(animal_count, 0)) %>%
    dplyr::group_by(animal_group) %>%
    # lag will then reference previous year, even if 0
    sparklyr::mutate(previous_count = lag(animal_count, order_by = cal_year)) %>%
    dplyr::ungroup()

rescue_agg_prev %>%
    sparklyr::sdf_sort(c("animal_group", "cal_year")) %>%
    head(20) %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+------------+--------+------------+--------------+
|animal_group|cal_year|animal_count|previous_count|
+------------+--------+------------+--------------+
|Bird        |2009    |89          |null          |
|Bird        |2010    |99          |89            |
|Bird        |2011    |120         |99            |
|Bird        |2012    |112         |120           |
|Bird        |2013    |85          |112           |
|Bird        |2014    |110         |85            |
|Bird        |2015    |106         |110           |
|Bird        |2016    |120         |106           |
|Bird        |2017    |124         |120           |
|Bird        |2018    |126         |124           |
|Bird        |2019    |9           |126           |
|Budgie      |2009    |0           |null          |
|Budgie      |2010    |0           |0             |
|Budgie      |2011    |1           |0             |
|Budgie      |2012    |0           |1             |
|Budgie      |2013    |0           |0             |
|Budgie      |2014    |0           |0             |
|Budgie      |2015    |0           |0             |
|Budgie      |2016    |0           |0             |
|Budgie      |2017    |0           |0             |
+------------+--------+------------+--------------+
only showing top 20 rows
```

```{code-tab} plaintext R Output
# A tibble: 20 × 4
   animal_group cal_year animal_count previous_count
   <chr>           <int>        <dbl>          <dbl>
 1 Bird             2009           89             NA
 2 Bird             2010           99             89
 3 Bird             2011          120             99
 4 Bird             2012          112            120
 5 Bird             2013           85            112
 6 Bird             2014          110             85
 7 Bird             2015          106            110
 8 Bird             2016          120            106
 9 Bird             2017          124            120
10 Bird             2018          126            124
11 Bird             2019            9            126
12 Budgie           2009            0             NA
13 Budgie           2010            0              0
14 Budgie           2011            1              0
15 Budgie           2012            0              1
16 Budgie           2013            0              0
17 Budgie           2014            0              0
18 Budgie           2015            0              0
19 Budgie           2016            0              0
20 Budgie           2017            0              0
```
````
### Window Functions in SQL

You can also use the regular SQL syntax for window functions when using Spark, `OVER(PARTITION BY...GROUP BY)`. This needs an SQL wrapper to be processed in Spark, [`spark.sql()`](https://spark.apache.org/docs/latest/sql-getting-started.html#running-sql-queries-programmatically) in PySpark and [`tbl(sc, sql())`](https://dplyr.tidyverse.org/reference/tbl.html) in sparklyr. Remember that SQL works on tables rather than DataFrames, so register the DataFrame first.
````{tabs}
```{code-tab} py
rescue_agg.registerTempTable("rescue_agg")

sql_window = spark.sql(
    """
    SELECT
        cal_year,
        animal_group,
        animal_count,
        SUM(animal_count) OVER(PARTITION BY cal_year) AS annual_count
    FROM rescue_agg
    """
)

sql_window.filter(F.col("animal_group") == "Snake").show()
```

```{code-tab} r R

sparklyr::sdf_register(rescue_agg, "rescue_agg")

sql_window <- dplyr::tbl(sc, dplyr::sql(
    "
    SELECT
        cal_year,
        animal_group,
        animal_count,
        SUM(animal_count) OVER(PARTITION BY cal_year) AS annual_count
    FROM rescue_agg
    "))

sql_window %>%
    sparklyr::filter(animal_group == "Snake") %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+--------+------------+------------+------------+
|cal_year|animal_group|animal_count|annual_count|
+--------+------------+------------+------------+
|    2018|       Snake|           1|         609|
|    2013|       Snake|           2|         585|
|    2009|       Snake|           3|         568|
|    2016|       Snake|           1|         604|
|    2017|       Snake|           1|         539|
+--------+------------+------------+------------+
```

```{code-tab} plaintext R Output
# Source: spark<rescue_agg> [?? x 3]
   animal_group                                     cal_year animal_count
   <chr>                                               <int>        <dbl>
 1 Dog                                                  2018           91
 2 Horse                                                2013           16
 3 Squirrel                                             2011            4
 4 Fox                                                  2017           33
 5 Unknown - Domestic Animal Or Pet                     2015           29
 6 Fox                                                  2018           33
 7 Sheep                                                2012            1
 8 Unknown - Wild Animal                                2010            2
 9 Unknown - Animal Rescue From Water - Farm Animal     2019            1
10 Ferret                                               2018            1
# … with more rows
# A tibble: 5 × 4
  cal_year animal_group animal_count annual_count
     <int> <chr>               <dbl>        <dbl>
1     2009 Snake                   3          568
2     2016 Snake                   1          604
3     2017 Snake                   1          539
4     2013 Snake                   2          585
5     2018 Snake                   1          609
```
````
### Further Resources

Spark at the ONS Articles:
- [Cross Joins](../spark-functions/cross-join)

PySpark Documentation:
- [`.groupBy()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)
- [`Window`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html)
- [`.withColumn()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html)
- [`F.sum()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sum.html)
- [`.over()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.over.html)
- [`.partitionBy()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.partitionBy.html)
- [`.orderBy()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.orderBy.html)
- [`F.mean()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.mean.html)
- [`F.max()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.max.html)
- [`F.rank()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rank.html)
- [`F.row_number()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.row_number.html)
- [`F.desc()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.desc.html)
- [`F.dense_rank()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dense_rank.html)
- [`F.lag()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lag.html)
- [`F.lead()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lead.html)
- [`spark.sql()`](https://spark.apache.org/docs/latest/sql-getting-started.html#running-sql-queries-programmatically)

sparklyr and tidyverse Documentation:
- [`group_by()` and `ungroup()`](https://dplyr.tidyverse.org/reference/group_by.html)
- [`mutate()`](https://dplyr.tidyverse.org/reference/mutate.html)
- [`summarise()`](https://dplyr.tidyverse.org/reference/summarise.html)
- [`tbl()`](https://dplyr.tidyverse.org/reference/tbl.html)

Spark SQL Functions Documentation:
- [`sum`](https://spark.apache.org/docs/latest/api/sql/index.html#sum)
- [`mean`](https://spark.apache.org/docs/latest/api/sql/index.html#mean)
- [`max`](https://spark.apache.org/docs/latest/api/sql/index.html#max)
- [`rank`](https://spark.apache.org/docs/latest/api/sql/index.html#rank)
- [`row_number`](https://spark.apache.org/docs/latest/api/sql/index.html#rank)
- [`dense_rank`](https://spark.apache.org/docs/latest/api/sql/index.html#dense_rank)
- [`lag`](https://spark.apache.org/docs/latest/api/sql/index.html#lag)
- [`lead`](https://spark.apache.org/docs/latest/api/sql/index.html#lead)