## Introduction to PySpark: Exercises

These exercises relate to the Introduction to PySpark article.

Before starting the exercises, use the following code to start a Spark session and read the data:

````{tabs}
```{code-tab} python
# Load packages
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import yaml

# Open config
with open("../../config.yaml") as f:
    config = yaml.safe_load(f)

# Start a local Spark session
spark = (SparkSession.builder.master("local[2]")
         .appName("pyspark-basics")
         .getOrCreate())

# Read in the Animal Rescue CSV
rescue_path_csv = config["rescue_path_csv"]
rescue = spark.read.csv(rescue_path_csv, header=True, inferSchema=True)

rescue = (rescue
          .withColumnRenamed("AnimalGroupParent", "animal_group")
          .withColumn(
              "incident_duration", 
              F.col("PumpHoursTotal") / F.col("PumpCount")))
```
````



````{tabs}
```{tab} Exercise 1

Rename the following columns in the `rescue` DataFrame:

`StnGroundName` --> `station_name`

`FinYear` --> `fin_year`

Select these columns, `animal_group` and `incident_duration` and preview the first five rows of the DataFrame
```
```{code-tab} python Solution 1
# Chain .withColumnRenamed() and select()
rescue = (rescue
          .withColumnRenamed("StnGroundName", "station_name")
          .withColumnRenamed("FinYear", "financial_year")
          .select("animal_group", "station_name", "financial_year", "incident_duration"))

# Preview with .show()
rescue.show(5)

# Can also use .limit(5).toPandas()
rescue.limit(5).toPandas()
```
````

````{tabs}
```{tab} Exercise 2

Create a new DataFrame which consists of all the rows where `animal_group` is equal to `Fox` and preview the first ten rows

```

```{code-tab} python Solution 2
# Use F.col() to filter, ensuring that a new DataFrame is created
foxes = rescue.filter(F.col("animal_group") == "Fox")

# Can also use a string condition instead
foxes = rescue.filter("animal_group == 'Fox'")

# Preview with .show()
foxes.show(10)

# Can also use .limit(10).toPandas()
foxes.limit(10).toPandas()
```
````

````{tabs}
```{tab} Exercise 3

Sort the incidents in terms of their duration, look at the top 10 and the bottom 10.
Do you notice anything strange?

```

```{code-tab} python Solution 3
# To get the top 10, sort the DF descending
top10 = (rescue
         .orderBy("incident_duration", ascending=False)
         .limit(10))

top10.show()

# The bottom 10 can just be sorted ascending
bottom10 = (rescue
         .orderBy("incident_duration")
         .limit(10))

# When previewing the results, the incident_duration are all null
bottom10.show()
```
````

### Further Resources

Spark in ONS material:
- Introduction to PySpark
