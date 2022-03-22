## Pivot tables in Spark

A pivot table is a way of displaying the result of grouped and aggregated data as a two dimensional table, rather than in the list form that you get from regular grouping and aggregating. You might be familiar with them from Excel.

The principles are the same in PySpark and sparklyr, although unlike some Spark functions that are used in both PySpark and sparklyr the syntax is very different.

<details>
<summary><b>Python Explanation</b></summary>
    
You can create pivot tables in PySpark by using [`.pivot()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.pivot.html) with [`.groupBy()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.groupBy.html). If you group your data by two or more columns then you may find it easier to view the data in this way.

`.pivot()` has two arguments. `pivot_col` is the column used to create the output columns, and has to be a single column; it cannot accept a list of multiple columns. The second argument, `values`, is optional but recommended. You can specify the exact columns that you want returned. If left blank, Spark will automatically use all possible values as output columns; calculating this can be inefficient and the output will look untidy if there are a large number of columns.
</details>

<details>
<summary><b>R Explanation</b></summary>

You can create pivot tables in sparklyr with [`sdf_pivot()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_pivot.html). This is a sparklyr specific function and so it cannot be used on base R DataFrames or tibbles. An example of [pivoting on a tibble](#comparison-with-pivot-wider) is given at the end for comparison.

`sdf_pivot(x, formula, fun.aggregate)` has three arguments. The first, `x` is the sparklyr DataFrame, the second, formula is an R formula with grouped columns on the left and pivot column on the right, separated by a tilde (e.g. `col1 + col2 ~ pivot_col`), and the third, `fun.aggregate`, is the functions used for aggregation; by default it will count the rows if left blank. Be careful with pivoting data where your pivot column has a large number of distinct values; it will return a very wide DataFrame that will be untidy to view. It is recommended to [`filter()`](https://dplyr.tidyverse.org/reference/filter.html) the data first to only include the values you want in the output columns. The second example uses `filter()`.
</details>

### Example 1: Group by one column and count

Create a new Spark session and read the Animal Rescue data. To make the example easier to read, just filter on a few animal groups:
````{tabs}
```{code-tab} py
import yaml
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[2]")
         .appName("pivot")
         .getOrCreate())

with open("../../../config.yaml") as f:
    config = yaml.safe_load(f)
    
rescue_path = config["rescue_path"]
rescue = (spark.read.parquet(rescue_path)
          .select("incident_number", "animal_group", "cal_year", "total_cost", "origin_of_call")
          .filter(F.col("animal_group").isin("Cat", "Dog", "Hamster", "Sheep")))
```

```{code-tab} r R

library(sparklyr)
library(dplyr)
options(pillar.print_max = Inf, pillar.width=Inf)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "pivot",
    config = sparklyr::spark_config())

config <- yaml::yaml.load_file("ons-spark/config.yaml")

rescue <- sparklyr::spark_read_parquet(sc, config$rescue_path) %>%
    sparklyr::select(incident_number, animal_group, cal_year, total_cost, origin_of_call) %>%
    sparklyr::filter(animal_group %in% c("Cat", "Dog", "Hamster", "Sheep"))

```
````
The minimal example is grouping by just one column, pivoting on another, just counting the rows, rather than an aggregating values in another column.

<details>
<summary><b>Python Example</b></summary>

In PySpark, use `.groupBy()` and [`.count()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.count.html) as you normally would when grouping and getting the row count, but add `.pivot()` between the two functions.
````{tabs}
```{code-tab} py
rescue_pivot = (rescue
                .groupBy("animal_group")
                .pivot("cal_year")
                .count())

rescue_pivot.show()
```
````

```plaintext
+------------+----+----+----+----+----+----+----+----+----+----+----+
|animal_group|2009|2010|2011|2012|2013|2014|2015|2016|2017|2018|2019|
+------------+----+----+----+----+----+----+----+----+----+----+----+
|     Hamster|null|   3|   3|null|   3|   1|null|   4|null|null|null|
|         Cat| 262| 294| 309| 302| 312| 295| 262| 296| 257| 304|  16|
|         Dog| 132| 122| 103| 100|  93|  90|  88| 107|  81|  91|   1|
|       Sheep|   1|null|null|   1|null|null|   1|   1|null|null|null|
+------------+----+----+----+----+----+----+----+----+----+----+----+
```
</details>

<details>
<summary><b>R Example</b></summary>

In sparklyr, use `sdf_pivot()`. As the pipe (`%>%`) is being used to apply the function to the DataFrame, this minimal example takes just one argument, `formula`, which is a tilde expression. The left hand side is the grouping column, `animal_group`, and the right hand side is the pivot column, `cal_year`. The default aggregation is to get the row count, so there is no need to specify the other argument, `fun.aggregate`.

Note that the R output will spill over to multiple rows. The second example resolves this by filtering on what will become the pivot columns.
````{tabs}

```{code-tab} r R

rescue_pivot <- rescue %>%
    sparklyr::sdf_pivot(animal_group ~ cal_year)

rescue_pivot %>%
    sparklyr::collect() %>%
    print()

```
````

```plaintext
# A tibble: 4 × 12
  animal_group `2009` `2010` `2011` `2012` `2013` `2014` `2015` `2016` `2017`
  <chr>         <dbl>  <dbl>  <dbl>  <dbl>  <dbl>  <dbl>  <dbl>  <dbl>  <dbl>
1 Dog             132    122    103    100     93     90     88    107     81
2 Cat             262    294    309    302    312    295    262    296    257
3 Hamster          NA      3      3     NA      3      1     NA      4     NA
4 Sheep             1     NA     NA      1     NA     NA      1      1     NA
  `2018` `2019`
   <dbl>  <dbl>
1     91      1
2    304     16
3     NA     NA
4     NA     NA
```
</details>

Another way of viewing the same information would be to just use a regular grouping expression, but it is harder to compare between years when displaying the DataFrame in this way:

<details>
<summary><b>Python Example</b></summary>

````{tabs}
```{code-tab} py
rescue_grouped = (rescue
                  .groupBy("animal_group", "cal_year")
                  .count()
                  .orderBy("animal_group", "cal_year"))

rescue_grouped.show(40)
```
````

```plaintext
+------------+--------+-----+
|animal_group|cal_year|count|
+------------+--------+-----+
|         Cat|    2009|  262|
|         Cat|    2010|  294|
|         Cat|    2011|  309|
|         Cat|    2012|  302|
|         Cat|    2013|  312|
|         Cat|    2014|  295|
|         Cat|    2015|  262|
|         Cat|    2016|  296|
|         Cat|    2017|  257|
|         Cat|    2018|  304|
|         Cat|    2019|   16|
|         Dog|    2009|  132|
|         Dog|    2010|  122|
|         Dog|    2011|  103|
|         Dog|    2012|  100|
|         Dog|    2013|   93|
|         Dog|    2014|   90|
|         Dog|    2015|   88|
|         Dog|    2016|  107|
|         Dog|    2017|   81|
|         Dog|    2018|   91|
|         Dog|    2019|    1|
|     Hamster|    2010|    3|
|     Hamster|    2011|    3|
|     Hamster|    2013|    3|
|     Hamster|    2014|    1|
|     Hamster|    2016|    4|
|       Sheep|    2009|    1|
|       Sheep|    2012|    1|
|       Sheep|    2015|    1|
|       Sheep|    2016|    1|
+------------+--------+-----+
```
</details>

<details>
<summary><b>R Example</b></summary>

````{tabs}

```{code-tab} r R

rescue_grouped <- rescue %>%
    dplyr::group_by(animal_group, cal_year) %>%
    dplyr::summarise(n()) %>%
    sparklyr::sdf_sort(c("animal_group", "cal_year"))

rescue_grouped %>%
    sparklyr::collect() %>%
    print()

```
````

```plaintext
# A tibble: 31 × 3
   animal_group cal_year `n()`
   <chr>           <int> <dbl>
 1 Cat              2009   262
 2 Cat              2010   294
 3 Cat              2011   309
 4 Cat              2012   302
 5 Cat              2013   312
 6 Cat              2014   295
 7 Cat              2015   262
 8 Cat              2016   296
 9 Cat              2017   257
10 Cat              2018   304
11 Cat              2019    16
12 Dog              2009   132
13 Dog              2010   122
14 Dog              2011   103
15 Dog              2012   100
16 Dog              2013    93
17 Dog              2014    90
18 Dog              2015    88
19 Dog              2016   107
20 Dog              2017    81
21 Dog              2018    91
22 Dog              2019     1
23 Hamster          2010     3
24 Hamster          2011     3
25 Hamster          2013     3
26 Hamster          2014     1
27 Hamster          2016     4
28 Sheep            2009     1
29 Sheep            2012     1
30 Sheep            2015     1
31 Sheep            2016     1
```
</details>

### Example 2: Aggregate by another column and specify values

<details>
<summary><b>Python Example</b></summary>

You can use [`.agg()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html) with `.pivot()` in the same way as you do with `.groupBy()`. This example will sum the `total_cost`.

The [documentation](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.pivot.html) explains why it is more efficient to manually provide the `values` argument; as an example, we just look at three years.
````{tabs}
```{code-tab} py
rescue_pivot = (rescue
                .groupBy("animal_group")
                .pivot("cal_year", values=["2009", "2010", "2011"])
                .agg(F.sum("total_cost")))

rescue_pivot.show()
```
````

```plaintext
+------------+-------+-------+-------+
|animal_group|   2009|   2010|   2011|
+------------+-------+-------+-------+
|     Hamster|   null|  780.0|  780.0|
|         Cat|76685.0|88140.0|89440.0|
|         Dog|39295.0|38480.0|31200.0|
|       Sheep|  255.0|   null|   null|
+------------+-------+-------+-------+
```
</details>
    
<details>
<summary><b>R Example</b></summary>

To group by several columns express this on the left side of the `formula` argument, concatenating them with `+`, in this example `AnimalGroup + OriginOfCall ~ CalYear`.

To only look at a certain subset of the pivot column you can just use `filter()` before pivoting. This is a good idea if your pivot column has a large number of distinct values. As an example, we just look at three years.
````{tabs}

```{code-tab} r R

rescue_pivot <- rescue %>%
    sparklyr::filter(cal_year %in% c("2009", "2010", "2011")) %>%
    sparklyr::sdf_pivot(
        animal_group ~ cal_year,
        fun.aggregate = list(total_cost = "sum"))

rescue_pivot %>%
    sparklyr::collect() %>%
    print()

```
````

```plaintext
# A tibble: 4 × 4
  animal_group `2009` `2010` `2011`
  <chr>         <dbl>  <dbl>  <dbl>
1 Cat           76685  88140  89440
2 Dog           39295  38480  31200
3 Hamster          NA    780    780
4 Sheep           255     NA     NA
```
</details>

### Example 3: Multiple groupings and aggregations, fill nulls and sort

<details>
<summary><b>Python Example</b></summary>

You can only supply one column to `.pivot()`, but you can have multiple aggregations. Adding an [`.alias()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.alias.html) makes the result easier to read.

Any missing combinations of the grouping and pivot will be returned as `null`, e.g. there are no incidents with `Hamster`, `Person (land line)` and `2009`. To set this to zero, use [`.fillna()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.fillna.html).

If grouping by multiple columns you may also want to add [`.orderBy()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.orderBy.html).
````{tabs}
```{code-tab} py
rescue_pivot = (rescue
           .groupBy("animal_group", "origin_of_call")
           .pivot("cal_year", values = ["2009", "2010", "2011"])
           .agg(F.sum("total_cost").alias("sum"), F.max("total_cost").alias("max"))
           .fillna(0)
           .orderBy("animal_group", "origin_of_call"))

rescue_pivot.show()
```
````

```plaintext
+------------+--------------------+--------+--------+--------+--------+--------+--------+
|animal_group|      origin_of_call|2009_sum|2009_max|2010_sum|2010_max|2011_sum|2011_max|
+------------+--------------------+--------+--------+--------+--------+--------+--------+
|         Cat|           Ambulance|     0.0|     0.0|     0.0|     0.0|     0.0|     0.0|
|         Cat|           Other FRS|   260.0|   260.0|   520.0|   260.0|  1040.0|   520.0|
|         Cat|  Person (land line)| 45365.0|   780.0| 53040.0|  1040.0| 53040.0|  1040.0|
|         Cat|     Person (mobile)| 30545.0|  1820.0| 33800.0|  2080.0| 34580.0|  1040.0|
|         Cat|Person (running c...|     0.0|     0.0|   260.0|   260.0|     0.0|     0.0|
|         Cat|              Police|   515.0|   260.0|   520.0|   260.0|   780.0|   260.0|
|         Dog|           Ambulance|   255.0|   255.0|     0.0|     0.0|     0.0|     0.0|
|         Dog|           Other FRS|  1540.0|   765.0|  1040.0|   520.0|     0.0|     0.0|
|         Dog|  Person (land line)| 13460.0|   780.0|  9880.0|  1040.0|  9100.0|   520.0|
|         Dog|     Person (mobile)| 20675.0|   780.0| 24180.0|  1040.0| 21320.0|  1040.0|
|         Dog|              Police|  3365.0|   765.0|  3380.0|  1300.0|   780.0|   260.0|
|     Hamster|  Person (land line)|     0.0|     0.0|   260.0|   260.0|   520.0|   260.0|
|     Hamster|     Person (mobile)|     0.0|     0.0|   520.0|   260.0|   260.0|   260.0|
|       Sheep|           Other FRS|     0.0|     0.0|     0.0|     0.0|     0.0|     0.0|
|       Sheep|  Person (land line)|   255.0|   255.0|     0.0|     0.0|     0.0|     0.0|
|       Sheep|     Person (mobile)|     0.0|     0.0|     0.0|     0.0|     0.0|     0.0|
+------------+--------------------+--------+--------+--------+--------+--------+--------+
```

</details>

<details>
<summary><b>R Example</b></summary>

`sdf_pivot()` is quite awkward with multiple aggregations on the same column. `fun.aggregate` can take a named list, but only one aggregation can be applied to each column. As we want to get the `sum` and `max` of `total_cost`, we can create another column, `total_cost_copy`, and aggregate on this. To rename the result columns dynamically, use [`rename_with()`](https://dplyr.tidyverse.org/reference/rename.html).

Any missing combinations of the grouping and pivot will be returned as `NA`, e.g. there are no incidents with `Hamster`, `Person (land line)` and `2009`. To set this to zero, use `na.replace()`.

If grouping by multiple columns you may also want to add [`sdf_sort()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_sort.html).
````{tabs}

```{code-tab} r R

rescue_pivot <- rescue %>%
    sparklyr::filter(cal_year %in% c("2009", "2010", "2011")) %>%
    sparklyr::mutate(total_cost_copy = total_cost) %>%
    sparklyr::sdf_pivot(
        animal_group + origin_of_call ~ cal_year,
        fun.aggregate = list(
            total_cost_copy = "sum",
            total_cost = "max"
        )) %>%
    dplyr::rename_with(~substr(., 1, 8), contains(c("_max", "_sum"))) %>%
    sparklyr::sdf_sort(c("animal_group", "origin_of_call")) %>%
    sparklyr::na.replace(0)

rescue_pivot %>%
    sparklyr::collect() %>%
    print()

```
````

```plaintext
# A tibble: 13 × 8
   animal_group origin_of_call        `2009_max` `2009_sum` `2010_max`
   <chr>        <chr>                      <dbl>      <dbl>      <dbl>
 1 Cat          Other FRS                    260        260        260
 2 Cat          Person (land line)           780      45365       1040
 3 Cat          Person (mobile)             1820      30545       2080
 4 Cat          Person (running call)          0          0        260
 5 Cat          Police                       260        515        260
 6 Dog          Ambulance                    255        255          0
 7 Dog          Other FRS                    765       1540        520
 8 Dog          Person (land line)           780      13460       1040
 9 Dog          Person (mobile)              780      20675       1040
10 Dog          Police                       765       3365       1300
11 Hamster      Person (land line)             0          0        260
12 Hamster      Person (mobile)                0          0        260
13 Sheep        Person (land line)           255        255          0
   `2010_sum` `2011_max` `2011_sum`
        <dbl>      <dbl>      <dbl>
 1        520        520       1040
 2      53040       1040      53040
 3      33800       1040      34580
 4        260          0          0
 5        520        260        780
 6          0          0          0
 7       1040          0          0
 8       9880        520       9100
 9      24180       1040      21320
10       3380        260        780
11        260        260        520
12        520        260        260
13          0          0          0
```

</details>

### Comparison with `pivot_wider()`

This section is just for those interested in R and dplyr.

<details>
<summary><b>R Explanation</b></summary>

`sdf_pivot()` can only be used on sparklyr DataFrames. If you have a base R DataFrame or tibble you can use [`tidyr::pivot_wider()`](https://tidyr.tidyverse.org/reference/pivot_wider.html). The [documentation](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_pivot.html) for `sdf_pivot()` explains that it was based on `reshape2::dcast()`, but it is now recommended to use the `tidyr` package rather than `reshape2`. The syntax is different to `sdf_pivot()` and so it is worth looking at an example for comparison.

First, filter the sparklyr DataFrame and convert to a tibble. Be careful when collecting data from the Spark cluster to the driver; in this example the `rescue` DataFrame is small, but it will not work if your DataFrame is large:
````{tabs}

```{code-tab} r R

rescue_tibble <- rescue %>%
    sparklyr::filter(cal_year %in% c("2009", "2010", "2011")) %>%
    sparklyr::collect()

# Check that this is a tibble
class(rescue_tibble)

```
````

```plaintext
[1] "tbl_df"     "tbl"        "data.frame"
```
Now use `pivot_wider()`; note that rather than a formula with `~` it used `names_from` and `names_to`, and it groups by all columns not given in these arguments:
````{tabs}

```{code-tab} r R

tibble_pivot <- rescue_tibble %>%
    sparklyr::select(animal_group, origin_of_call, cal_year, total_cost) %>%
    tidyr::pivot_wider(
        names_from = cal_year,
        values_from = total_cost,
        values_fn = list(total_cost = sum)) %>%
    dplyr::arrange(animal_group, origin_of_call)
    
tibble_pivot %>%
    print()

```
````

```plaintext
# A tibble: 13 × 5
   animal_group origin_of_call        `2011` `2009` `2010`
   <chr>        <chr>                  <dbl>  <dbl>  <dbl>
 1 Cat          Other FRS               1040    260    520
 2 Cat          Person (land line)     53040     NA  53040
 3 Cat          Person (mobile)        34580     NA  33800
 4 Cat          Person (running call)     NA     NA    260
 5 Cat          Police                   780    515    520
 6 Dog          Ambulance                 NA    255     NA
 7 Dog          Other FRS                 NA   1540   1040
 8 Dog          Person (land line)        NA  13460   9880
 9 Dog          Person (mobile)        21320  20675  24180
10 Dog          Police                   780   3365   3380
11 Hamster      Person (land line)       520     NA    260
12 Hamster      Person (mobile)          260     NA    520
13 Sheep        Person (land line)        NA    255     NA
```
</details>

### Further Resources

PySpark Documentation:
- [`.pivot()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.pivot.html)
- [`.groupBy()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.groupBy.html)
- [`.count()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.count.html) 
- [`.agg()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html)
- [`.alias()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.alias.html)
- [`.fillna()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.fillna.html)
- [`.orderBy()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.orderBy.html)

sparklyr and tidyverse Documentation:
- [`sdf_pivot()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_pivot.html)
- [`filter()`](https://dplyr.tidyverse.org/reference/filter.html) 
- [`pivot_wider()`](https://tidyr.tidyverse.org/reference/pivot_wider.html)
- [`rename_with()`](https://dplyr.tidyverse.org/reference/rename.html)
- [`sdf_sort()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_sort.html)