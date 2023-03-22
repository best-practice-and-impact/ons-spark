# Checkpoints and Staging Tables

## Checkpoint

### Persisting to disk

Spark uses lazy evaluation. As we build up many transformations Spark creates an execution plan for the DataFrame and the plan is executed when an action is called. This execution plan represents the DataFrame's lineage.

Sometimes the DataFrame's lineage can grow long and complex, which will slow down the processing and maybe even return an error. However, we can get around this by breaking the lineage.

There is more than one way of breaking the lineage, this is discussed in more detail in the [Persisting](../spark-concepts/persistence) article. In this article we cover a simple method of persisting to disk called checkpointing, which is essentially an out of the box shortcut to a write/read operation.

### Experiment

To demonstrate the benefit of checkpointing we'll time how long it takes to create a DataFrame using an iterative calculation. We will run the process without persisting, then again using a checkpoint. 

We'll create a new Spark session each time just in case there's an advantage when processing the DataFrame a second time in the same session. We will also use the Python module [`time`](https://docs.python.org/3/library/time.html) to measure the time taken to create the DataFrame. 

We're going to create a new DataFrame with an `id` column and a column called `col_0` that will consist of random numbers. We'll then create a loop to add new columns where the values depend on a previous column. The contents of the columns isn't important here. What is important is that Spark is creating an execution plan that it getting longer with each iteration of the loop.

In general, we try to avoid using loops with Spark and this example shows why. A better solution to this problem using Spark would be to add new rows with each iteration as opposed to columns.

We will set a `seed_num` when creating the random numbers to make the results repeatable. The DataFrame will have `num_rows` amount of rows, which we will set to a thousand and the loop will iterate 11 times to create `col_1` to `col_11`.
````{tabs}
```{code-tab} py
import os
from pyspark.sql import SparkSession, functions as F
from time import time
import yaml

spark = (SparkSession.builder.master("local[2]")
         .appName("checkpoint")
         .getOrCreate())

new_cols = 12
seed_num = 42
num_rows = 10**3
```

```{code-tab} r R
 
library(sparklyr)
library(dplyr)
library(DBI)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "checkpoint",
    config = sparklyr::spark_config())


set.seed(42)
new_cols <- 12
num_rows <- 10^3

```
````
#### Without persisting
````{tabs}
```{code-tab} py
start_time = time()

df = spark.range(num_rows)
df = df.withColumn("col_0", F.ceil(F.rand(seed_num) * new_cols))

for i in range(1, new_cols):
    df = (df.withColumn("col_"+str(i), 
                        F.when(F.col("col_"+str(i-1)) > i, 
                               F.col("col_"+str(i-1)))
                        .otherwise(0)))

df.show(10)

time_taken = time() - start_time
print(f"Time taken to create the DataFrame:  {time_taken}")
```

```{code-tab} r R

start_time <- Sys.time()

df = sparklyr::sdf_seq(sc, 1, num_rows) %>%
    sparklyr::mutate(col_0 = ceiling(rand()*new_cols))

for (i in 1: new_cols)
{
  column_name = paste0('col_', i)
  prev_column = paste0('col_', i-1)
  df <- df %>%
    mutate(
      !!column_name := case_when(
        !!as.symbol(prev_column) > i ~ !!as.symbol(prev_column)))
  
}

df %>%
    head(10)%>%
    collect()%>%
    print()

end_time <- Sys.time()
time_taken = end_time - start_time

cat("Time taken to create DataFrame", time_taken)


```
````

````{tabs}

```{code-tab} plaintext Python Output
+---+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+------+------+
| id|col_0|col_1|col_2|col_3|col_4|col_5|col_6|col_7|col_8|col_9|col_10|col_11|
+---+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+------+------+
|  0|    8|    8|    8|    8|    8|    8|    8|    8|    0|    0|     0|     0|
|  1|   11|   11|   11|   11|   11|   11|   11|   11|   11|   11|    11|     0|
|  2|   11|   11|   11|   11|   11|   11|   11|   11|   11|   11|    11|     0|
|  3|   11|   11|   11|   11|   11|   11|   11|   11|   11|   11|    11|     0|
|  4|    6|    6|    6|    6|    6|    6|    0|    0|    0|    0|     0|     0|
|  5|    7|    7|    7|    7|    7|    7|    7|    0|    0|    0|     0|     0|
|  6|    1|    0|    0|    0|    0|    0|    0|    0|    0|    0|     0|     0|
|  7|    2|    2|    0|    0|    0|    0|    0|    0|    0|    0|     0|     0|
|  8|    4|    4|    4|    4|    0|    0|    0|    0|    0|    0|     0|     0|
|  9|    9|    9|    9|    9|    9|    9|    9|    9|    9|    0|     0|     0|
+---+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+------+------+
only showing top 10 rows

Time taken to create the DataFrame:  8.401437520980835
```

```{code-tab} plaintext R Output
# A tibble: 10 × 14
      id col_0 col_1 col_2 col_3 col_4 col_5 col_6 col_7 col_8 col_9 col_10
   <int> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>  <dbl>
 1     1     8     8     8     8     8     8     8     8    NA    NA     NA
 2     2     8     8     8     8     8     8     8     8    NA    NA     NA
 3     3     1    NA    NA    NA    NA    NA    NA    NA    NA    NA     NA
 4     4     6     6     6     6     6     6    NA    NA    NA    NA     NA
 5     5     5     5     5     5     5    NA    NA    NA    NA    NA     NA
 6     6     8     8     8     8     8     8     8     8    NA    NA     NA
 7     7    12    12    12    12    12    12    12    12    12    12     12
 8     8     9     9     9     9     9     9     9     9     9    NA     NA
 9     9     2     2    NA    NA    NA    NA    NA    NA    NA    NA     NA
10    10    11    11    11    11    11    11    11    11    11    11     11
# … with 2 more variables: col_11 <dbl>, col_12 <dbl>
Time taken to create DataFrame 14.71731```
````
The result above shows how long Spark took to create the plan and execute it to show the top 10 rows. 

#### With checkpoints

Next we will stop the Spark session and start a new one to repeat the operation using checkpoints. 

To perform a checkpoint we need to set up a checkpoint directory on the file system, which is where the checkpointed DataFrames will be stored. It's important to practice good housekeeping with this directory because new files are created with every checkpoint, but they are **not automatically deleted**.
````{tabs}
```{code-tab} py
spark.stop()

spark = (SparkSession.builder.master("local[2]")
         .appName("checkpoint")
         .getOrCreate())

with open("../../../config.yaml") as f:
    config = yaml.safe_load(f)
    
checkpoint_path = config["checkpoint_path"]
spark.sparkContext.setCheckpointDir(checkpoint_path)
```

```{code-tab} r R
 

spark_disconnect(sc)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "checkpoint",
    config = sparklyr::spark_config())


config <- yaml::yaml.load_file("/home/cdsw/ons-spark/config.yaml")

spark_set_checkpoint_dir(sc, config$checkpoint_path)


```
````
We will checkpoint the DataFrame every 3 iterations of the loop so that the lineage doesn't grow as long. Again, we will time how long it takes for Spark to complete the operation.
````{tabs}
```{code-tab} py
start_time = time()

df = spark.range(num_rows)
df = df.withColumn("col_0", F.ceil(F.rand(seed_num) * new_cols))

for i in range(1, new_cols):
    df = (df.withColumn("col_"+str(i), 
                       F.when(F.col("col_"+str(i-1)) > i, 
                              F.col("col_"+str(i-1)))
                       .otherwise(0)))
    if i % 3 == 0: # this means if i is divisable by three then...
        df = df.checkpoint() # here is the checkpoint
        
df.show(10)

time_taken = time() - start_time
print(f"Time taken to create the DataFrame:  {time_taken}")
```

```{code-tab} r R
 
start_time <- Sys.time()

df1 = sparklyr::sdf_seq(sc, 1, num_rows) %>%
    sparklyr::mutate(col_0 = ceiling(rand()*new_cols))

for (i in 1: new_cols)
{
  column_name = paste0('col_', i)
  prev_column = paste0('col_', i-1)
  df1 <- df1 %>%
    mutate(!!column_name := case_when(!!as.symbol(prev_column) > i ~ !!as.symbol(prev_column) ))
  
  
  if (i %% 3 == 0) 
  {
    sdf_checkpoint(df1, eager= TRUE)
  }
}

df1 %>%
    head(10)%>%
    collect()%>%
    print()

end_time <- Sys.time()
time_taken = end_time - start_time


cat("Time taken to create DataFrame: ", time_taken)

```
````

````{tabs}

```{code-tab} plaintext Python Output
+---+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+------+------+
| id|col_0|col_1|col_2|col_3|col_4|col_5|col_6|col_7|col_8|col_9|col_10|col_11|
+---+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+------+------+
|  0|    8|    8|    8|    8|    8|    8|    8|    8|    0|    0|     0|     0|
|  1|   11|   11|   11|   11|   11|   11|   11|   11|   11|   11|    11|     0|
|  2|   11|   11|   11|   11|   11|   11|   11|   11|   11|   11|    11|     0|
|  3|   11|   11|   11|   11|   11|   11|   11|   11|   11|   11|    11|     0|
|  4|    6|    6|    6|    6|    6|    6|    0|    0|    0|    0|     0|     0|
|  5|    7|    7|    7|    7|    7|    7|    7|    0|    0|    0|     0|     0|
|  6|    1|    0|    0|    0|    0|    0|    0|    0|    0|    0|     0|     0|
|  7|    2|    2|    0|    0|    0|    0|    0|    0|    0|    0|     0|     0|
|  8|    4|    4|    4|    4|    0|    0|    0|    0|    0|    0|     0|     0|
|  9|    9|    9|    9|    9|    9|    9|    9|    9|    9|    0|     0|     0|
+---+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+------+------+
only showing top 10 rows

Time taken to create the DataFrame:  1.0542099475860596
```

```{code-tab} plaintext R Output
1# A tibble: 10 × 14
      id col_0 col_1 col_2 col_3 col_4 col_5 col_6 col_7 col_8 col_9 col_10
   <int> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>  <dbl>
 1     1    12    12    12    12    12    12    12    12    12    12     12
 2     2     7     7     7     7     7     7     7    NA    NA    NA     NA
 3     3    10    10    10    10    10    10    10    10    10    10     NA
 4     4    10    10    10    10    10    10    10    10    10    10     NA
 5     5     4     4     4     4    NA    NA    NA    NA    NA    NA     NA
 6     6     1    NA    NA    NA    NA    NA    NA    NA    NA    NA     NA
 7     7    12    12    12    12    12    12    12    12    12    12     12
 8     8     2     2    NA    NA    NA    NA    NA    NA    NA    NA     NA
 9     9     6     6     6     6     6     6    NA    NA    NA    NA     NA
10    10    11    11    11    11    11    11    11    11    11    11     11
# … with 2 more variables: col_11 <dbl>, col_12 <dbl>
Time taken to create DataFrame:  23.7858```
````
The exact times will vary with each run of this notebook, but hopefully you will see that using the [`.checkpoint()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.checkpoint.html) was more efficient.

As mentioned earlier, the checkpoint files are not deleted on HDFS automatically. The files are not intended to be used after you stop the Spark session, so make sure you delete these files after a session.

Often the easiest way to delete files is through some GUI, but the cell below is handy to have at the end of your scripts when using checkpoints to make sure you don't forget to empty the checkpoint folder.
````{tabs}
```{code-tab} py
import subprocess
cmd = f'hdfs dfs -rm -r -skipTrash {checkpoint_path}' 
p = subprocess.run(cmd, shell=True)
```

```{code-tab} r R
 
cmd <- paste0("hdfs dfs -rm -r -skipTrash ", config$checkpoint_path)
p <- system(command = cmd)

```
````

````{tabs}

```{code-tab} plaintext R Output
6Deleted file:///home/cdsw/ons-spark/checkpoints
```
````
### How often should I checkpoint?

How did we come up with the number 3 for number of iterations to checkpoint? Trial and error. Unfortunately, you may not have the luxury of trying to find the optimum number, but have a go at checkpointing and see if you can get any improvements in performance.

More frequent checkpointing means more writing and reading data, which does take some time, but the aim is to save some time by simplifying the execution plan.

As mentioned above, the use of loops shown here is not considered good practice with Spark, but it was a convenient example of using checkpoints. Of course, checkpointing can also be used outside loops, see the [Persisting](../spark-concepts/persistence) article for more information on the different forms of persisting data in Spark and their applications.

## Staging Tables

Staging tables are an alternative way of checkpointing data in Spark, in which the data is written out as a named Hive table in a database, rather than to the checkpointing location.

### Staging tables: the concept

You can write a staging table to HDFS with `df.write.mode("overwrite").saveAsTable(table_name, format="parquet")` or `df.write.insertInto(table_name, overwrite=True)`(of course, if using `.insertInto()` you will need to create the table first). You can then read the table back in with `spark.read.table()`. Like with checkpointing, this will break the lineage of the DataFrame, and therefore they can be useful in large, complex pipelines, or those that involve processes in a loop. As Spark is more efficient at reading in tables than CSV files, another use case is staging CSV files as tables at the start of your code before doing any complex calculations.

Staging has some advantages over checkpointing:
- The same table can be overwritten, meaning there is no need to clean up old checkpointed files
- It is stored in a location that is easier to access, rather than the checkpointing folder, which can help with debugging and testing changes to the code
- They can be re-used elsewhere
- If `.insertInto()` is used, you can take advantage of the table schema, as an exception will be raised if the DataFrame and table schemas do not match
- It is more efficient for Spark to read Hive tables than CSV files as the underlying format is Parquet, so if your data are delivered as CSV files you may want to stage them as Hive tables first. 

There are also some disadvantages:
- Takes longer to write the code
- More difficult to maintain, especially if `.insertInto()` is used, as you will have to alter the table if the DataFrame structure changes
- Ensure that you are not using them unnecessarily (the same is true with any method of persisting data)

The examples here use PySpark, but the same principles apply to R users who are using sparklyr in DAP.

### Example

Our example will be very simple, and show how to read a CSV file, perform some basic data cleansing, then stage as a Hive table, and then read it back in as a DataFrame. 

Often staging tables are most useful in large, complex pipelines; for obvious reasons our example will instead be simple!

First, import the relevant modules and create a Spark session:
````{tabs}
```{code-tab} py
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F 

spark = (SparkSession.builder.master("local[2]")
         .appName("staging-tables")
         .getOrCreate())
```
````
Now read in the CSV:
````{tabs}
```{code-tab} py
df = spark.read.csv("/training/animal_rescue.csv", header = True)
```
````
Then do some preparation: drop and rename some columns, change the format, then sort.

Note that if saving as a Hive table there are some stricter rules, including:
- Some characters aren't allowed in column names, including `£`
- The table won't load in the browser in HUE if you use date, but will accept a timestamp

We then preview the DataFrame with `.toPandas()` (remember to use `.limit()` when looking at data in this way):
````{tabs}
```{code-tab} py
df = (df.
    drop(
        "WardCode", 
        "BoroughCode", 
        "Easting_m", 
        "Northing_m", 
        "Easting_rounded", 
        "Northing_rounded")
    .withColumnRenamed("PumpCount", "EngineCount")
    .withColumnRenamed("FinalDescription", "Description")
    .withColumnRenamed("HourlyNotionalCost(£)", "HourlyCost")
    .withColumnRenamed("IncidentNotionalCost(£)", "TotalCost")
    .withColumnRenamed("OriginofCall", "OriginOfCall")
    .withColumnRenamed("PumpHoursTotal", "JobHours")
    .withColumnRenamed("AnimalGroupParent", "AnimalGroup")
    .withColumn(
        "DateTimeOfCall", F.to_timestamp(F.col("DateTimeOfCall"), "dd/MM/yyyy"))
    .orderBy("IncidentNumber")
    )

df.limit(3).toPandas()
```
````

````{tabs}

```{code-tab} plaintext Python Output
     IncidentNumber DateTimeOfCall CalYear  FinYear   TypeOfIncident  \
0  000014-03092018M     2018-09-03    2018  2018/19  Special Service   
1   000099-01012017     2017-01-01    2017  2016/17  Special Service   
2   000260-01012017     2017-01-01    2017  2016/17  Special Service   

  EngineCount JobHours HourlyCost TotalCost  \
0         2.0      3.0        333     999.0   
1         1.0      2.0        326     652.0   
2         1.0      1.0        326     326.0   

                                         Description  \
0                                               None   
1    DOG WITH HEAD STUCK IN RAILINGS CALLED BY OWNER   
2  BIRD TRAPPED IN NETTING BY THE 02 SHOP AND NEA...   

                        AnimalGroup        OriginOfCall          PropertyType  \
0  Unknown - Heavy Livestock Animal           Other FRS  Animal harm outdoors   
1                               Dog     Person (mobile)              Railings   
2                              Bird  Person (land line)          Single shop    

    PropertyCategory SpecialServiceTypeCategory  \
0            Outdoor    Other animal assistance   
1  Outdoor Structure    Other animal assistance   
2    Non Residential  Animal rescue from height   

                 SpecialServiceType                             Ward  Borough  \
0   Animal harm involving livestock  CARSHALTON SOUTH AND CLOCKHOUSE   SUTTON   
1    Assist trapped domestic animal                     BROMLEY TOWN  BROMLEY   
2  Animal rescue from height - Bird                        Fairfield  CROYDON   

  StnGroundName PostcodeDistrict  
0    Wallington              CR8  
1       Bromley              BR2  
2       Croydon              CR0  
```
````
Let's look at the plan with `df.explain()`. This displays what precisely Spark will do once an action is called (*lazy evaluation*). This is a simple example but in long pipelines this plan can get complicated. Using a staging table can split this process, referred to as *cutting the lineage*.
````{tabs}
```{code-tab} py
df.explain()
```
````

````{tabs}

```{code-tab} plaintext Python Output
== Physical Plan ==
*(2) Sort [IncidentNumber#439 ASC NULLS FIRST], true, 0
+- Exchange rangepartitioning(IncidentNumber#439 ASC NULLS FIRST, 200)
   +- *(1) Project [IncidentNumber#439, cast(unix_timestamp(DateTimeOfCall#440, dd/MM/yyyy, Some(Europe/London)) as timestamp) AS DateTimeOfCall#658, CalYear#441, FinYear#442, TypeOfIncident#443, PumpCount#444 AS EngineCount#511, PumpHoursTotal#445 AS JobHours#616, HourlyNotionalCost(£)#446 AS HourlyCost#553, IncidentNotionalCost(£)#447 AS TotalCost#574, FinalDescription#448 AS Description#532, AnimalGroupParent#449 AS AnimalGroup#637, OriginofCall#450 AS OriginOfCall#595, PropertyType#451, PropertyCategory#452, SpecialServiceTypeCategory#453, SpecialServiceType#454, Ward#456, Borough#458, StnGroundName#459, PostcodeDistrict#460]
      +- *(1) FileScan csv [IncidentNumber#439,DateTimeOfCall#440,CalYear#441,FinYear#442,TypeOfIncident#443,PumpCount#444,PumpHoursTotal#445,HourlyNotionalCost(£)#446,IncidentNotionalCost(£)#447,FinalDescription#448,AnimalGroupParent#449,OriginofCall#450,PropertyType#451,PropertyCategory#452,SpecialServiceTypeCategory#453,SpecialServiceType#454,Ward#456,Borough#458,StnGroundName#459,PostcodeDistrict#460] Batched: false, Format: CSV, Location: InMemoryFileIndex[hdfs://dnt01/training/animal_rescue.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<IncidentNumber:string,DateTimeOfCall:string,CalYear:string,FinYear:string,TypeOfIncident:s...
```
````
Now save the DataFrame as table, using `mode("overwrite")`, which overwrites the existing table if there is one. The first time you create a staging table this option will be redundant, but on subsequent runs on the code you will get an error without this as the table will already exist.
````{tabs}
```{code-tab} py
username = os.getenv('HADOOP_USER_NAME') 
table_name = f"train_tmp.staging_example_{username}"

df.write.mode("overwrite").saveAsTable(table_name, format="parquet")
```
````
Now read the data in again and preview:
````{tabs}
```{code-tab} py
df = spark.read.table(table_name)
df.limit(3).toPandas()
```
````

````{tabs}

```{code-tab} plaintext Python Output
    IncidentNumber DateTimeOfCall CalYear  FinYear   TypeOfIncident  \
0  004812-12012017     2017-01-12    2017  2016/17  Special Service   
1  004997-14012016     2016-01-14    2016  2015/16  Special Service   
2  005140-12012017     2017-01-12    2017  2016/17  Special Service   

  EngineCount JobHours HourlyCost TotalCost  \
0         1.0      1.0        326     326.0   
1         1.0      1.0        298     298.0   
2         1.0      1.0        326     326.0   

                                         Description AnimalGroup  \
0  CAT TRAPPED BETWEEN 2 WALLS  WEDGED BEHIND MET...         Cat   
1  PIDGEON CAUGHT IN NETTING  CALL FOR ASSISTANCE...        Bird   
2                        CAT TRAPPED BEHIND CUPBOARD         Cat   

         OriginOfCall                                  PropertyType  \
0     Person (mobile)                                         Fence   
1  Person (land line)                    Electricity power station    
2     Person (mobile)  Converted Flat/Maisonette - Up to 2 storeys    

    PropertyCategory SpecialServiceTypeCategory  \
0  Outdoor Structure    Other animal assistance   
1    Non Residential    Other animal assistance   
2           Dwelling    Other animal assistance   

               SpecialServiceType             Ward               Borough  \
0  Assist trapped domestic animal          HAMPTON  RICHMOND UPON THAMES   
1      Assist trapped wild animal    FIGGE'S MARSH                MERTON   
2  Assist trapped domestic animal  TOTTENHAM GREEN              HARINGEY   

  StnGroundName PostcodeDistrict  
0    Twickenham             TW12  
1       Mitcham              CR4  
2     Tottenham              N15  
```
````
The DataFrame has the same structure as previously, but when we look at the plan with `df.explain()` we can see that less is being done. This is an example of cutting the lineage and can be useful when you have complex plans.
````{tabs}
```{code-tab} py
df.explain()
```
````

````{tabs}

```{code-tab} plaintext Python Output
== Physical Plan ==
*(1) FileScan parquet train_tmp.staging_example_mitchs[IncidentNumber#739,DateTimeOfCall#740,CalYear#741,FinYear#742,TypeOfIncident#743,EngineCount#744,JobHours#745,HourlyCost#746,TotalCost#747,Description#748,AnimalGroup#749,OriginOfCall#750,PropertyType#751,PropertyCategory#752,SpecialServiceTypeCategory#753,SpecialServiceType#754,Ward#755,Borough#756,StnGroundName#757,PostcodeDistrict#758] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://dnt01/training/train_tmp/hive/staging_example_mitchs], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<IncidentNumber:string,DateTimeOfCall:timestamp,CalYear:string,FinYear:string,TypeOfInciden...
```
````
### Using `.insertInto()`

Another method is to create an empty table and then use `.insertInto()`; here we will just use a small number of columns as an example:
````{tabs}
```{code-tab} py
small_table = f"train_tmp.staging_small_{username}"

spark.sql(f"""
    CREATE TABLE {small_table} (
        IncidentNumber STRING,
        CalYear INT,
        EngineCount INT,
        AnimalGroup STRING
    )
    STORED AS PARQUET
    """)
```
````

````{tabs}

```{code-tab} plaintext Python Output
DataFrame[]
```
````
Note that the columns will be inserted by position, not name, so it's a good idea to re-select the column order to match that of the table before inserting in:
````{tabs}
```{code-tab} py
col_order = spark.read.table(small_table).columns
df.select(col_order).write.insertInto(small_table, overwrite=True)
```
````
This can then be read in as before:
````{tabs}
```{code-tab} py
df = spark.read.table(small_table)
df.show(5)
```
````

````{tabs}

```{code-tab} plaintext Python Output
+---------------+-------+-----------+-----------+
| IncidentNumber|CalYear|EngineCount|AnimalGroup|
+---------------+-------+-----------+-----------+
|004812-12012017|   2017|          1|        Cat|
|004997-14012016|   2016|          1|       Bird|
|005140-12012017|   2017|          1|        Cat|
|005168-13012019|   2019|          1|        Cat|
|005178-13012018|   2018|          1|       Bird|
+---------------+-------+-----------+-----------+
only showing top 5 rows
```
````
Finally we will drop the tables used in this example, which we can do with the `DROP` SQL statement. This is much easier than deleting a checkpointed file.

Of course, with staging tables you generally want to keep the table, but just overwrite the data each time, so this step often won't be needed.

Always be very careful when using `DROP` as this will delete the table without warning!
````{tabs}
```{code-tab} py
spark.sql(f"DROP TABLE {table_name}")
spark.sql(f"DROP TABLE {small_table}")
```
````

````{tabs}

```{code-tab} plaintext Python Output
DataFrame[]
```
````
### Further Resources

Spark at the ONS Articles:
- [Persisting](../../spark-concepts/persistence)
- [Caching](../../spark-concepts/cache)

PySpark Documentation:
- [`.checkpoint()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.checkpoint.html)
- [df.write.insertInto()](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter.insertInto)
- [df.write.saveAsTable()](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter.saveAsTable)
- [spark.read.csv()](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.csv)
- [spark.read.table()](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.table)

Python Documentation:
- [`time`](https://docs.python.org/3/library/time.html)

Other material:
- <a href="https://en.wikipedia.org/wiki/Staging_(data)">Staging (data) article on Wikipedia</a>