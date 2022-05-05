## Add leading zeros with `lpad()`

There are some datasets that consist of code columns, which are fixed width and are stored as strings even though they look like numbers. For instance, the data might have values such as `"000123"` and `"456789"`.

However, sometimes these can appear as integers which will lose the initial zeros. So `"000123"` becomes `123`. Commonly this can happen if at some point the data is manipulated in Excel and later read in a CSV file; it can also happen if the column is converted to a numeric type. An ONS example is [Standard Industrial Classification (SIC)](https://www.ons.gov.uk/methodology/classificationsandstandards/ukstandardindustrialclassificationofeconomicactivities/uksic2007). SIC codes are five digits, but some begin with $0$.

There is an easy way to change this back into the correct format: [`F.lpad()`](https://spark.apache.org/docs/latest/api/python//reference/api/pyspark.sql.functions.lpad.html) from the `functions` package in PySpark and the [Spark function](../sparklyr-intro/sparklyr-functions) [`lpad()`](https://spark.apache.org/docs/latest/api/sql/index.html#lpad) inside `mutate()` in sparklyr. This will add a string that you specify to the start, making every value a fixed width string. For instance, `"123"` becomes `"000123"` but `"456789"` remains the same.

### Example: Incident Numbers

The Animal Rescue data has an `incident_number` column, which is unique and of variable length. We will add leading zeros to this column to make it of a consistent length.

First, start a Spark session and read in the Animal Rescue data, filter on `"Police"` and select the relevant columns:
````{tabs}
```{code-tab} py
import yaml
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[2]").appName("padding").getOrCreate()

with open("../../../config.yaml") as f:
    config = yaml.safe_load(f)
    
rescue_path = config["rescue_path"]
rescue = (spark.read.parquet(rescue_path)
          .filter(F.col("origin_of_call") == "Police")
          .orderBy("date_time_of_call")
          .select("incident_number", "origin_of_call"))
rescue.show(5)
```

```{code-tab} r R

library(sparklyr)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "padding",
    config = sparklyr::spark_config())

config <- yaml::yaml.load_file("ons-spark/config.yaml")

rescue <- sparklyr::spark_read_parquet(sc, config$rescue_path) %>%
    sparklyr::filter(origin_of_call == "Police") %>%
    dplyr::arrange(date_time_of_call) %>%
    sparklyr::select(incident_number, origin_of_call)

rescue %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()    

```
````

````{tabs}

```{code-tab} plaintext Python Output
+---------------+--------------+
|incident_number|origin_of_call|
+---------------+--------------+
|      146647151|        Police|
|       66969111|        Police|
|      103407111|        Police|
|      137525091|        Police|
|      158794091|        Police|
+---------------+--------------+
only showing top 5 rows
```

```{code-tab} plaintext R Output
# A tibble: 5 × 2
  incident_number origin_of_call
  <chr>           <chr>         
1 146647151       Police        
2 66969111        Police        
3 103407111       Police        
4 137525091       Police        
5 158794091       Police        
```
````
The input to `lpad()` will most often be either a string or an integer. In this example, `incident_number` is a string:
````{tabs}
```{code-tab} py
rescue.printSchema()
```

```{code-tab} r R

pillar::glimpse(rescue)

```
````

````{tabs}

```{code-tab} plaintext Python Output
root
 |-- incident_number: string (nullable = true)
 |-- origin_of_call: string (nullable = true)
```

```{code-tab} plaintext R Output
Rows: ??
Columns: 2
Database: spark_connection
Ordered by: date_time_of_call
$ incident_number <chr> "146647151", "66969111", "103407111", "137525091", "15…
$ origin_of_call  <chr> "Police", "Police", "Police", "Police", "Police", "Pol…
```
````
Use [`F.length()`](https://spark.apache.org/docs/latest/api/python//reference/api/pyspark.sql.functions.length.html)/[`length()`](https://spark.apache.org/docs/latest/api/sql/index.html#length) to demonstrate that the `incident_number` column is not always the same size:
````{tabs}
```{code-tab} py
rescue = rescue.withColumn("incident_no_length", F.length("incident_number"))
rescue.groupBy("incident_no_length").count().orderBy("incident_no_length").show()
```

```{code-tab} r R

rescue <- rescue %>%
    sparklyr::mutate(incident_no_length = length(incident_number))

rescue %>%
    dplyr::group_by(incident_no_length, origin_of_call) %>%
    dplyr::summarise(count = n()) %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+------------------+-----+
|incident_no_length|count|
+------------------+-----+
|                 6|    1|
|                 7|    6|
|                 8|   57|
|                 9|   65|
+------------------+-----+
```

```{code-tab} plaintext R Output
# A tibble: 4 × 3
  incident_no_length origin_of_call count
               <int> <chr>          <dbl>
1                  6 Police             1
2                  8 Police            57
3                  7 Police             6
4                  9 Police            65
```
````
We want the `incident_number` column to be a string with a fixed width of $9$, with zeros at the start if the length is shorter than this.

`lpad()` takes three arguments. The first argument, `col` is the column name, the second, `len` is the fixed width of the string, and the third, `pad`, the value to pad it with if it is too short, often `"0"`. The data type returned from `lpad()` will always be a string.
````{tabs}
```{code-tab} py
rescue = rescue.withColumn("padded_incident_no", F.lpad(F.col("incident_number"), 9, "0"))
rescue.orderBy("incident_no_length").show(5)
rescue.orderBy("incident_no_length", ascending=False).show(5)
```

```{code-tab} r R

rescue <- rescue %>%
    sparklyr::mutate(padded_incident_no = lpad(incident_number, 9, "0"))

rescue %>%
    dplyr::arrange(incident_no_length) %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

rescue %>%
    dplyr::arrange(desc(incident_no_length)) %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+---------------+--------------+------------------+------------------+
|incident_number|origin_of_call|incident_no_length|padded_incident_no|
+---------------+--------------+------------------+------------------+
|         955141|        Police|                 6|         000955141|
|        7003121|        Police|                 7|         007003121|
|        6311101|        Police|                 7|         006311101|
|        5930131|        Police|                 7|         005930131|
|        3223101|        Police|                 7|         003223101|
+---------------+--------------+------------------+------------------+
only showing top 5 rows

+---------------+--------------+------------------+------------------+
|incident_number|origin_of_call|incident_no_length|padded_incident_no|
+---------------+--------------+------------------+------------------+
|      205017101|        Police|                 9|         205017101|
|      207037111|        Police|                 9|         207037111|
|      135844101|        Police|                 9|         135844101|
|      216289101|        Police|                 9|         216289101|
|      145879151|        Police|                 9|         145879151|
+---------------+--------------+------------------+------------------+
only showing top 5 rows
```

```{code-tab} plaintext R Output
# A tibble: 5 × 4
  incident_number origin_of_call incident_no_length padded_incident_no
  <chr>           <chr>                       <int> <chr>             
1 955141          Police                          6 000955141         
2 3215101         Police                          7 003215101         
3 3223101         Police                          7 003223101         
4 7003121         Police                          7 007003121         
5 6311101         Police                          7 006311101         
# A tibble: 5 × 4
  incident_number origin_of_call incident_no_length padded_incident_no
  <chr>           <chr>                       <int> <chr>             
1 163121101       Police                          9 163121101         
2 103225141       Police                          9 103225141         
3 114153091       Police                          9 114153091         
4 101172091       Police                          9 101172091         
5 110211101       Police                          9 110211101         
```
````
Be careful; if you set the fixed width to be too short you can lose data:
````{tabs}
```{code-tab} py
rescue = rescue.withColumn("too_short_inc_no", F.lpad(F.col("incident_number"), 3, "0"))
rescue.orderBy("incident_no_length", ascending=False).show(5)
```

```{code-tab} r R

rescue <- rescue %>%
    sparklyr::mutate(too_short_inc_no = lpad(incident_number, 3, "0"))

rescue %>%
    dplyr::arrange(desc(incident_no_length)) %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+---------------+--------------+------------------+------------------+----------------+
|incident_number|origin_of_call|incident_no_length|padded_incident_no|too_short_inc_no|
+---------------+--------------+------------------+------------------+----------------+
|      114153091|        Police|                 9|         114153091|             114|
|      138096091|        Police|                 9|         138096091|             138|
|      110211101|        Police|                 9|         110211101|             110|
|      101172091|        Police|                 9|         101172091|             101|
|      102278091|        Police|                 9|         102278091|             102|
+---------------+--------------+------------------+------------------+----------------+
only showing top 5 rows
```

```{code-tab} plaintext R Output
# A tibble: 5 × 5
  incident_number origin_of_call incident_no_length padded_incident_no
  <chr>           <chr>                       <int> <chr>             
1 163121101       Police                          9 163121101         
2 103225141       Police                          9 103225141         
3 114153091       Police                          9 114153091         
4 101172091       Police                          9 101172091         
5 110211101       Police                          9 110211101         
# … with 1 more variable: too_short_inc_no <chr>
```
````
You can have values other than zero for the last argument and they do not have to be width 1, although there are fewer use cases for this. For example:
````{tabs}
```{code-tab} py
rescue = rescue.withColumn("silly_example", F.lpad(F.col("incident_number"), 14, "xyz"))
rescue.orderBy("incident_no_length").show(5)
```

```{code-tab} r R

rescue <- rescue %>%
    sparklyr::mutate(silly_example = lpad(incident_number, 14, "xyz"))
    
rescue %>%
    dplyr::arrange(incident_no_length) %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+---------------+--------------+------------------+------------------+----------------+--------------+
|incident_number|origin_of_call|incident_no_length|padded_incident_no|too_short_inc_no| silly_example|
+---------------+--------------+------------------+------------------+----------------+--------------+
|         955141|        Police|                 6|         000955141|             955|xyzxyzxy955141|
|        7003121|        Police|                 7|         007003121|             700|xyzxyzx7003121|
|        6311101|        Police|                 7|         006311101|             631|xyzxyzx6311101|
|        5930131|        Police|                 7|         005930131|             593|xyzxyzx5930131|
|        3223101|        Police|                 7|         003223101|             322|xyzxyzx3223101|
+---------------+--------------+------------------+------------------+----------------+--------------+
only showing top 5 rows
```

```{code-tab} plaintext R Output
# A tibble: 5 × 6
  incident_number origin_of_call incident_no_length padded_incident_no
  <chr>           <chr>                       <int> <chr>             
1 955141          Police                          6 000955141         
2 3215101         Police                          7 003215101         
3 3223101         Police                          7 003223101         
4 7003121         Police                          7 007003121         
5 6311101         Police                          7 006311101         
# … with 2 more variables: too_short_inc_no <chr>, silly_example <chr>
```
````
### Padding on the right: `rpad()`

There is also a similar function, [`F.rpad()`](https://spark.apache.org/docs/latest/api/python//reference/api/pyspark.sql.functions.rpad.html)/[`rpad`](https://spark.apache.org/docs/latest/api/sql/index.html#rpad), that works in the same way with identical arguments, just padding to the right instead:
````{tabs}
```{code-tab} py
rescue = rescue.withColumn("right_padded_inc_no", F.rpad(F.col("incident_number"), 9, "0"))
rescue.select("incident_number", "right_padded_inc_no").show(5)
```

```{code-tab} r R

rescue <- rescue %>%
    sparklyr::mutate(right_padded_inc_no = rpad(incident_number, 9, "0"))
    
rescue %>%
    dplyr::arrange(right_padded_inc_no) %>%
    sparklyr::select(incident_number, right_padded_inc_no) %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

```
````

````{tabs}

```{code-tab} plaintext Python Output
+---------------+-------------------+
|incident_number|right_padded_inc_no|
+---------------+-------------------+
|      146647151|          146647151|
|       66969111|          669691110|
|      103407111|          103407111|
|      137525091|          137525091|
|      158794091|          158794091|
+---------------+-------------------+
only showing top 5 rows
```

```{code-tab} plaintext R Output
# A tibble: 5 × 2
  incident_number right_padded_inc_no
  <chr>           <chr>              
1 53242091        532420910          
2 45531111        455311110          
3 19747141        197471410          
4 21512131        215121310          
5 220308091       220308091          
```
````
## Further Resources

Spark at the ONS Articles:
- [Using Spark Functions in sparklyr](../sparklyr-intro/sparklyr-functions): note that `lpad`/`rpad` are inherited directly from Spark SQL and need to be used inside `mutate()`, rather than imported from the sparklyr package as standalone functions

PySpark Documentation:
- [`F.lpad()`](https://spark.apache.org/docs/latest/api/python//reference/api/pyspark.sql.functions.lpad.html)
- [`F.rpad()`](https://spark.apache.org/docs/latest/api/python//reference/api/pyspark.sql.functions.rpad.html)
- [`F.length()`](https://spark.apache.org/docs/latest/api/python//reference/api/pyspark.sql.functions.length.html)

Spark SQL Documentation:
- [`lpad`](https://spark.apache.org/docs/latest/api/sql/index.html#lpad)
- [`rpad`](https://spark.apache.org/docs/latest/api/sql/index.html#rpad)
- [`length`](https://spark.apache.org/docs/latest/api/sql/index.html#length)

Other links:
- [ONS Standard Industrial Classification (SIC)](https://www.ons.gov.uk/methodology/classificationsandstandards/ukstandardindustrialclassificationofeconomicactivities/uksic2007)