## Reference columns by name: `F.col()`

There are several different ways to reference columns in a PySpark DataFrame `df`, e.g. in a [`.filter()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.filter.html) operation:
- `df.filter(F.col("column_name") == value)`: references column by name; the recommended method, used throughout this book
- `df.filter(df.column_name == value)`: references column directly from the DF
- `df.flter(df["column_name"] == value)`: pandas style, less commonly used in PySpark

The preferred method is using [`F.col()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.col.html) from the `pyspark.sql.functions` module and is used throughout this book. Although all three methods above will work in some circumstances, only `F.col()` will always have the desired outcome. This is because it references the column by *name* rather than directly from the DF, which means columns not yet assigned to the DF can be used, e.g. when chaining several operations on the same DF together.

There are several cases where `F.col()` will work but one of the other methods may not:
- [Filter the DataFrame when reading in](f-col:example-1)
- [Filter on a new column](f-col:example-2)
- [Ensuring you are using the latest values](f-col:example-3)
- [Columns with special characters or spaces](f-col:example-4)

(f-col:example-1)=
### Example 1: Filter the DataFrame when reading in

First, import the modules and create a Spark session:
````{tabs}
```{code-tab} py
import yaml
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[2]").appName("f-col").getOrCreate()

with open("../../../config.yaml") as f:
    config = yaml.safe_load(f)
    
rescue_path = config["rescue_path"]
rescue_path_csv = config["rescue_path_csv"]
```
````
We can filter on columns when reading in the DataFrame. For instance to only read `"Cat"` from the animal rescue data:
````{tabs}
```{code-tab} py
cats = spark.read.parquet(rescue_path).filter(F.col("animal_group") == "Cat")
cats.select("incident_number", "animal_group").show(5)
```
````

```plaintext
+---------------+------------+
|incident_number|animal_group|
+---------------+------------+
|       80771131|         Cat|
|       43051141|         Cat|
|126246-03092018|         Cat|
|       17398141|         Cat|
|129971-26092017|         Cat|
+---------------+------------+
only showing top 5 rows
```
This can't be done using `cats.animal_group` as we haven't defined `cats` when referencing the DataFrame. To use the other notation we need to define `rescue` then filter on `cats.animal_group`:
````{tabs}
```{code-tab} py
rescue = spark.read.parquet(rescue_path)
cats.filter(cats.animal_group == "Cat").select("incident_number", "animal_group").show(5)
```
````

```plaintext
+---------------+------------+
|incident_number|animal_group|
+---------------+------------+
|       80771131|         Cat|
|       43051141|         Cat|
|126246-03092018|         Cat|
|       17398141|         Cat|
|129971-26092017|         Cat|
+---------------+------------+
only showing top 5 rows
```
(f-col:example-2)=
### Example 2: Filter on a new column

Read in the animal rescue data:
````{tabs}
```{code-tab} py
rescue = spark.read.parquet(rescue_path).select("incident_number", "animal_group")
```
````
Let's create a new column, `animal_group_upper`, which consists of the `animal_group` in uppercase.

If we try and immediately filter on this column using `rescue.animal_group_upper`, it won't work. This is because we have yet to define the column in `rescue`.
````{tabs}
```{code-tab} py
try:
    (rescue
     .withColumn("animal_group_upper", F.upper(rescue.animal_group))
     .filter(rescue.animal_group_upper == "CAT")
     .show(5))
except AttributeError as e:
    print(e)
```
````

```plaintext
'DataFrame' object has no attribute 'animal_group_upper'
```
We could split this statement up over two different lines:
````{tabs}
```{code-tab} py
rescue_upper = rescue.withColumn("animal_group_upper", F.upper(rescue.animal_group))
rescue_upper.filter(rescue_upper.animal_group_upper == "CAT").show(5)
```
````

```plaintext
+---------------+------------+------------------+
|incident_number|animal_group|animal_group_upper|
+---------------+------------+------------------+
|       80771131|         Cat|               CAT|
|       43051141|         Cat|               CAT|
|126246-03092018|         Cat|               CAT|
|       17398141|         Cat|               CAT|
|129971-26092017|         Cat|               CAT|
+---------------+------------+------------------+
only showing top 5 rows
```
Using `F.col()` is instead is much neater:
````{tabs}
```{code-tab} py
(rescue
    .withColumn("animal_group_upper", F.upper(F.col("animal_group")))
    .filter(F.col("animal_group_upper") == "CAT")
    .show(5))
```
````

```plaintext
+---------------+------------+------------------+
|incident_number|animal_group|animal_group_upper|
+---------------+------------+------------------+
|       80771131|         Cat|               CAT|
|       43051141|         Cat|               CAT|
|126246-03092018|         Cat|               CAT|
|       17398141|         Cat|               CAT|
|129971-26092017|         Cat|               CAT|
+---------------+------------+------------------+
only showing top 5 rows
```
(f-col:example-3)=
### Example 3: Ensuring you are using the latest values

Using `df.column_name` can also result in bugs when you think you are referencing the latest values, but are actually using the original ones. Here, the values in `animal_group` are changed, but `rescue` is yet to be redefined, and so the old values are used. As such no data is returned:
````{tabs}
```{code-tab} py
rescue = spark.read.parquet(rescue_path).select("incident_number", "animal_group")
(rescue
    .withColumn("animal_group", F.upper(rescue.animal_group))
    .filter(rescue.animal_group == "CAT")
    .show(5))
```
````

```plaintext
+---------------+------------+
|incident_number|animal_group|
+---------------+------------+
+---------------+------------+
```
Changing to `F.col("animal_group")` gives the correct result:
````{tabs}
```{code-tab} py
(rescue
    .withColumn("animal_group", F.upper(F.col("animal_group")))
    .filter(F.col("animal_group") == "CAT")
    .show(5))
```
````

```plaintext
+---------------+------------+
|incident_number|animal_group|
+---------------+------------+
|       80771131|         CAT|
|       43051141|         CAT|
|126246-03092018|         CAT|
|       17398141|         CAT|
|129971-26092017|         CAT|
+---------------+------------+
only showing top 5 rows
```
(f-col:example-4)=
### Example 4: Columns with special characters or spaces

One final use case for this method is when your source data has column names with spaces or special characters in them. This can happen if reading in from a CSV file rather than parquet or Hive table. The animal rescue CSV has a column called `IncidentNotionalCost(£)`. You can't refer to the column using `rescue.IncidentNotionalCost(£)`, instead, use `F.col("IncidentNotionalCost(£)")`:
````{tabs}
```{code-tab} py
rescue = (spark.read.csv(rescue_path_csv, header=True)
          .select("IncidentNumber", "IncidentNotionalCost(£)"))
rescue.filter(F.col("IncidentNotionalCost(£)") > 2500).show()
```
````

```plaintext
+---------------+-----------------------+
| IncidentNumber|IncidentNotionalCost(£)|
+---------------+-----------------------+
|       48360131|                 3480.0|
|       49076141|                 2655.0|
|       62700151|                 2980.0|
|098141-28072016|                 3912.0|
|092389-09072018|                 2664.0|
+---------------+-----------------------+
```
You can use the pandas style `rescue["IncidentNotionalCost(£)"]` but this notation isn't encouraged in PySpark:
````{tabs}
```{code-tab} py
rescue.filter(rescue["IncidentNotionalCost(£)"] > 2500).show()
```
````

```plaintext
+---------------+-----------------------+
| IncidentNumber|IncidentNotionalCost(£)|
+---------------+-----------------------+
|       48360131|                 3480.0|
|       49076141|                 2655.0|
|       62700151|                 2980.0|
|098141-28072016|                 3912.0|
|092389-09072018|                 2664.0|
+---------------+-----------------------+
```
Of course, the best idea is to rename the column something sensible, which is easier to reference:
````{tabs}
```{code-tab} py
rescue = (rescue
          .withColumnRenamed("IncidentNotionalCost(£)", "notional_cost")
          .withColumnRenamed("IncidentNumber", "incident_number"))
rescue.filter(F.col("notional_cost") > 2500).show()
```
````

```plaintext
+---------------+-------------+
|incident_number|notional_cost|
+---------------+-------------+
|       48360131|       3480.0|
|       49076141|       2655.0|
|       62700151|       2980.0|
|098141-28072016|       3912.0|
|092389-09072018|       2664.0|
+---------------+-------------+
```
If your data is stored as CSV with non-standard column names you may want to create a data cleansing stage, which reads in the CSV and renames the columns, then write this out as a parquet file or Hive table. Parquet files and Hive tables also have the advantage of being far quicker for Spark to process

### Further Resources

PySpark Documentation:
- [`.filter()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.filter.html)
- [`F.col()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.col.html)

Spark in ONS material:
- Error handling in PySpark
- Storing data as parquet file