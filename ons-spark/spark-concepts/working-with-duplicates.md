# Working with Duplicates

Duplicated data can cause many issues when it comes to running big data pipelines. There are several things to consider when working with duplicates in your data:
1. Dealing with duplicates is something that should be done at the start of your pipeline. However, you will also need to keep in mind that transformations further on in the pipeline (e.g. joins) may also create duplicates and this will need to be checked. 
2. You need to take into account which variables you are looking for duplicates in: a true duplicate will have duplicated data in all variables. However, in reality, you will most likely find rows which have duplicated data (e.g. id numbers or identifiers) but have differing data in their other variables. As such, you will need to determine how to deal with such data and whether to keep these entries or filter them based on other variable characteristics. 

There are several ways of dealing with duplicates in Spark. This section will cover two of the main ways: using Spark's in-built drop duplicates function and an alternative using a Window with a row number.

The first thing we will do is start our Spark session and read in our dataset.
````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


spark = (SparkSession.builder
          .appName('working_with_duplicates')
          .getOrCreate())

```

```{code-tab} r R


library(sparklyr)
library(dplyr)
library(rlang)

default_config <- sparklyr::spark_config()
default_config$spark.local.dir <- "D:/tmp"

sc <- sparklyr::spark_connect(
  master = "local",
  app_name = "working_with_duplicates",
  config = default_config)

connection_is_open(sc)


```
````

````{tabs}

```{code-tab} plaintext Python Output
Setting spark.hadoop.yarn.resourcemanager.principal to johanna.hall
```

```{code-tab} plaintext R Output
[1] TRUE

```
````

````{tabs}
```{code-tab} py
#read in MOT dataset and select columns we will be working with

#mot_path = "s3a://onscdp-dev-data01-5320d6ca/bat/dapcats/mot_test_results.csv"
# mot_path = "C:/Users/hallj/repos/data/mot_test_results.csv"
mot_path = "D:/dapcats_guidance/working-with-duplicates/mot_test_results.csv"

mot = (spark.read.csv(mot_path, header=True, inferSchema=True)
                 .select(['test_id', 
                         'vehicle_id', 
                         'test_date', 
                         'test_mileage', 
                         'postcode_area', 
                         'make', 
                         'colour', 
                         'test_result']))

#change the date format to yyyy-MM-dd
mot = mot.withColumn("test_date", F.to_date("test_date", "yyyy-MM-dd"))

#remove rows with nulls
mot = mot.dropna()
```

```{code-tab} r R

#read in the MOT dataset and select columns we want to work with
#mot_path = "s3a://onscdp-dev-data01-5320d6ca/bat/dapcats/mot_test_results.csv"
#mot_path = "C:/Users/hallj/repos/data/mot_test_results.csv"
mot_path = "D:/dapcats_guidance/working-with-duplicates/mot_test_results.csv"

mot <- sparklyr::spark_read_csv(sc,
    mot_path, 
    source = "csv", 
    header = TRUE, 
    infer_schema = TRUE) %>% 
    sparklyr::select(
      test_id,
      vehicle_id, 
      test_date, 
      test_mileage, 
      postcode_area, 
      make, 
      colour,
      test_result)

#change the date format to yyyy-MM-dd
mot <- mot %>%
    dplyr::mutate(test_date = dplyr::sql("CAST(test_date AS DATE)"))

#remove rows with nulls
mot <- na.omit(mot)

```
````

````{tabs}

```{code-tab} plaintext Python Output
                                                                                
```
````

````{tabs}
```{code-tab} py
#check the schema of the data
mot.printSchema()
```

```{code-tab} r R

#check schema and preview data
pillar::glimpse(mot)

```
````

````{tabs}

```{code-tab} plaintext Python Output
root
 |-- test_id: integer (nullable = true)
 |-- vehicle_id: integer (nullable = true)
 |-- test_date: date (nullable = true)
 |-- test_mileage: integer (nullable = true)
 |-- postcode_area: string (nullable = true)
 |-- make: string (nullable = true)
 |-- colour: string (nullable = true)
 |-- test_result: string (nullable = true)
```

```{code-tab} plaintext R Output
Rows: ??

Columns: 8

Database: spark_connection

$ test_id       [3m[38;5;246m<int>[39m[23m 1687460261, 1007821341, 1268040905, 232884115, 125102284…

$ vehicle_id    [3m[38;5;246m<int>[39m[23m 1131526890, 211522675, 1218838379, 159956284, 851633977,…

$ test_date     [3m[38;5;246m<date>[39m[23m 2023-04-28, 2023-04-28, 2023-04-28, 2023-04-28, 2023-04…

$ test_mileage  [3m[38;5;246m<int>[39m[23m 129661, 26597, 11803, 148187, 12915, 22281, 30140, 23462…

$ postcode_area [3m[38;5;246m<chr>[39m[23m "HU", "IV", "LA", "BD", "PO", "WA", "PH", "B", "S", "PA"…

$ make          [3m[38;5;246m<chr>[39m[23m "BMW", "FORD", "THE EXPLORER GROUP", "AUDI", "DACIA", "V…

$ colour        [3m[38;5;246m<chr>[39m[23m "BLACK", "BLUE", "WHITE", "WHITE", "ORANGE", "BLUE", "GR…

$ test_result   [3m[38;5;246m<chr>[39m[23m "F", "P", "P", "F", "P", "P", "P", "P", "P", "P", "P", "…

```
````

````{tabs}
```{code-tab} py
#check the size of the data we are working with
mot.count()
```

```{code-tab} r R


#check the size of the data we are working with
mot %>%
    sparklyr::sdf_nrow() %>%
    print()



```
````

````{tabs}

```{code-tab} plaintext Python Output
                                                                                
41892592
```

```{code-tab} plaintext R Output
[1] 41892592

```
````
Lets start by having a look at a subset of our data. We will be using the DVSA MOT dataset from 2023. We can see that vehicle ID 223981155 is duplicated 863 times!
````{tabs}
```{code-tab} py
#Filter to vehicles that have passed their MOT and groupby vehicle_id
duplicate_count = (mot.filter(F.col('test_result')=="P")
                .groupBy("vehicle_id").agg(F.count("*").alias("count"))
                .filter(F.col("count") > 6)
                .orderBy('count', ascending=False))

#Show top 10 rows
duplicate_count.limit(10).toPandas()
```

```{code-tab} r R


#Filter to vehicles that have passed their MOT and groupby vehicle_id
duplicate_count <- mot %>%
  dplyr::filter(test_result == "P") %>%
  dplyr::group_by(vehicle_id) %>%
  dplyr::summarise(count = dplyr::n(), .groups = 'drop') %>%
  dplyr::arrange(dplyr::desc(count)) %>%
  dplyr::ungroup()

#Show top 10 rows
duplicate_count %>%
  head(10) %>%
  collect() %>%
  print(width = Inf)


```
````

````{tabs}

```{code-tab} plaintext Python Output
                                                                                
   vehicle_id  count
0   223981155    863
1  1214612397      9
2  1478780109      7
3   371225013      7
4  1497793597      7
5   401693765      7
6    43095996      7
7  1316457909      7
8  1357224061      7
```

```{code-tab} plaintext R Output
[38;5;246m# A tibble: 10 × 2[39m

   vehicle_id count

        [3m[38;5;246m<int>[39m[23m [3m[38;5;246m<dbl>[39m[23m

[38;5;250m 1[39m  223[4m9[24m[4m8[24m[4m1[24m155   863

[38;5;250m 2[39m [4m1[24m214[4m6[24m[4m1[24m[4m2[24m397     9

[38;5;250m 3[39m [4m1[24m316[4m4[24m[4m5[24m[4m7[24m909     7

[38;5;250m 4[39m  401[4m6[24m[4m9[24m[4m3[24m765     7

[38;5;250m 5[39m   43[4m0[24m[4m9[24m[4m5[24m996     7

[38;5;250m 6[39m  371[4m2[24m[4m2[24m[4m5[24m013     7

[38;5;250m 7[39m [4m1[24m357[4m2[24m[4m2[24m[4m4[24m061     7

[38;5;250m 8[39m [4m1[24m497[4m7[24m[4m9[24m[4m3[24m597     7

[38;5;250m 9[39m [4m1[24m478[4m7[24m[4m8[24m[4m0[24m109     7

[38;5;250m10[39m  936[4m5[24m[4m4[24m[4m2[24m225     6

```
````
For demonstration purposes we will take a small sample of vehicle id 223981155. We will restrict results to those vehicles that have passed their MOT. We can then look how many of this vehicle id are in each postcode area. 
````{tabs}
```{code-tab} py
#sample_path = "s3a://onscdp-dev-data01-5320d6ca/bat/dapcats/mot_duplicate_sample.parquet"
# sample_path = "C:/Users/hallj/repos/ons-spark/ons-spark/ons-spark/data/mot_duplicate_sample.parquet"
sample_path = "D:/dapcats_guidance/working-with-duplicates/mot_duplicate_sample.parquet"

#Read in a 10% sample of vehicle id 223981155 for demo purposes
sample = spark.read.parquet(sample_path)


#Check count of this vehicle per postcode area
(sample.groupBy('postcode_area').agg(F.count('*').alias('count'))
    .orderBy('count', ascending=False)
    .show())
```

```{code-tab} r R


#sample_path = "s3a://onscdp-dev-data01-5320d6ca/bat/dapcats/mot_duplicate_sample.parquet"
#sample_path = "C:/Users/hallj/repos/ons-spark/ons-spark/ons-spark/data/mot_duplicate_sample.parquet"
sample_path = "D:/dapcats_guidance/working-with-duplicates/mot_duplicate_sample.parquet"

#Read in a 10% sample of vehicle id 223981155 for demo purposes
sample <- sparklyr::spark_read_parquet(sc,
                                   sample_path,
                                   source = "parquet")

#Check count of this vehicle per postcode area
sample %>%
  dplyr::group_by(postcode_area) %>%
  dplyr::summarise(count = dplyr::n(), .groups = 'drop') %>%
  dplyr::arrange(dplyr::desc(count)) %>%
  collect() %>%
  print(width = Inf) 


```
````

````{tabs}

```{code-tab} plaintext Python Output
[Stage 9:>                                                          (0 + 1) / 1]
+-------------+-----+
|postcode_area|count|
+-------------+-----+
|           BS|   21|
|           NP|   20|
|           PE|    8|
|           NN|    7|
|           WN|    7|
|           BB|    7|
|           NW|    5|
|           SE|    4|
|           LU|    2|
|           ML|    2|
|            E|    1|
|           CO|    1|
|           TS|    1|
|           CF|    1|
|           CH|    1|
|            N|    1|
|           EH|    1|
|           DH|    1|
|           SR|    1|
+-------------+-----+
                                                                                
```

```{code-tab} plaintext R Output
9     6

[38;5;246m# A tibble: 19 × 2[39m

   postcode_area count

   [3m[38;5;246m<chr>[39m[23m         [3m[38;5;246m<dbl>[39m[23m

[38;5;250m 1[39m BS               21

[38;5;250m 2[39m NP               20

[38;5;250m 3[39m PE                8

[38;5;250m 4[39m BB                7

[38;5;250m 5[39m WN                7

[38;5;250m 6[39m NN                7

[38;5;250m 7[39m NW                5

[38;5;250m 8[39m SE                4

[38;5;250m 9[39m LU                2

[38;5;250m10[39m ML                2

[38;5;250m11[39m N                 1

[38;5;250m12[39m SR                1

[38;5;250m13[39m CH                1

[38;5;250m14[39m E                 1

[38;5;250m15[39m EH                1

[38;5;250m16[39m TS                1

[38;5;250m17[39m CO                1

[38;5;250m18[39m DH                1

[38;5;250m19[39m CF                1

```
````
## `.dropDuplicates()` or `sparklyr::sdf_drop_duplicates()`


We can use the `.dropDuplicates()` or `sparklyr::sdf_drop_duplicates()` function on our sample, which in our case works fine as we are using it on the whole of the sample dataset. This means Spark will search for duplicates across all variables. We can see that there are multiple entries for various postcode areas, but they have different information for `test_id`, `test_date` and `test_mileage` despite having the same `vehicle_id` and `postcode_area`, so are not true duplicates and thus retained.
````{tabs}
```{code-tab} py
sample.dropDuplicates().sort(['postcode_area', F.desc('test_mileage')]).showpar()
```

```{code-tab} r R


#Drop duplicates based on all columns
sample %>% 
  sparklyr::sdf_drop_duplicates() %>%
  dplyr::arrange(postcode_area, desc(test_mileage)) %>%   
  collect() %>%
  print(width = Inf)


```
````

````{tabs}

```{code-tab} plaintext Python Output
[Stage 47:>                                                         (0 + 1) / 1]
+----------+----------+----------+------------+-------------+----+------+-----------+
|   test_id|vehicle_id| test_date|test_mileage|postcode_area|make|colour|test_result|
+----------+----------+----------+------------+-------------+----+------+-----------+
|1712482407| 223981155|2023-09-08|      315802|           BB|DVSA| BEIGE|          P|
| 608299587| 223981155|2023-09-08|      129365|           BB|DVSA| BEIGE|          P|
|1243548035| 223981155|2023-09-08|      118059|           BB|DVSA| BEIGE|          P|
|1041508715| 223981155|2023-08-15|      111890|           BB|DVSA| BEIGE|          P|
|1569660147| 223981155|2023-09-05|       68311|           BB|DVSA| BEIGE|          P|
|1531481473| 223981155|2023-09-09|       37932|           BB|DVSA| BEIGE|          P|
| 693596559| 223981155|2023-09-08|       30568|           BB|DVSA| BEIGE|          P|
| 243918949| 223981155|2023-09-16|      218001|           BS|DVSA| BEIGE|          P|
| 890662963| 223981155|2023-09-16|      197459|           BS|DVSA| BEIGE|          P|
| 537036175| 223981155|2023-08-29|      188907|           BS|DVSA| BEIGE|          P|
| 250905065| 223981155|2023-08-28|      186254|           BS|DVSA| BEIGE|          P|
| 746058855| 223981155|2023-09-19|      185623|           BS|DVSA| BEIGE|          P|
|1096998015| 223981155|2023-08-23|      180932|           BS|DVSA| BEIGE|          P|
|1517807321| 223981155|2023-09-22|      156503|           BS|DVSA| BEIGE|          P|
|1563278235| 223981155|2023-06-20|      155722|           BS|DVSA| BEIGE|          P|
|1141589769| 223981155|2023-09-19|      155482|           BS|DVSA| BEIGE|          P|
| 291027007| 223981155|2023-09-19|      149706|           BS|DVSA| BEIGE|          P|
|1803896485| 223981155|2023-08-11|      143010|           BS|DVSA| BEIGE|          P|
|1745334919| 223981155|2023-09-12|      133314|           BS|DVSA| BEIGE|          P|
| 972921705| 223981155|2023-08-16|      127893|           BS|DVSA| BEIGE|          P|
+----------+----------+----------+------------+-------------+----+------+-----------+
only showing top 20 rows
                                                                                
```

```{code-tab} plaintext R Output
[38;5;246m# A tibble: 92 × 8[39m

      test_id vehicle_id test_date  test_mileage postcode_area make  colour

        [3m[38;5;246m<int>[39m[23m      [3m[38;5;246m<int>[39m[23m [3m[38;5;246m<date>[39m[23m            [3m[38;5;246m<int>[39m[23m [3m[38;5;246m<chr>[39m[23m         [3m[38;5;246m<chr>[39m[23m [3m[38;5;246m<chr>[39m[23m 

[38;5;250m 1[39m [4m1[24m712[4m4[24m[4m8[24m[4m2[24m407  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-08       [4m3[24m[4m1[24m[4m5[24m802 BB            DVSA  BEIGE 

[38;5;250m 2[39m  608[4m2[24m[4m9[24m[4m9[24m587  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-08       [4m1[24m[4m2[24m[4m9[24m365 BB            DVSA  BEIGE 

[38;5;250m 3[39m [4m1[24m243[4m5[24m[4m4[24m[4m8[24m035  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-08       [4m1[24m[4m1[24m[4m8[24m059 BB            DVSA  BEIGE 

[38;5;250m 4[39m [4m1[24m041[4m5[24m[4m0[24m[4m8[24m715  223[4m9[24m[4m8[24m[4m1[24m155 2023-08-15       [4m1[24m[4m1[24m[4m1[24m890 BB            DVSA  BEIGE 

[38;5;250m 5[39m [4m1[24m569[4m6[24m[4m6[24m[4m0[24m147  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-05        [4m6[24m[4m8[24m311 BB            DVSA  BEIGE 

[38;5;250m 6[39m [4m1[24m531[4m4[24m[4m8[24m[4m1[24m473  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-09        [4m3[24m[4m7[24m932 BB            DVSA  BEIGE 

[38;5;250m 7[39m  693[4m5[24m[4m9[24m[4m6[24m559  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-08        [4m3[24m[4m0[24m568 BB            DVSA  BEIGE 

[38;5;250m 8[39m  243[4m9[24m[4m1[24m[4m8[24m949  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-16       [4m2[24m[4m1[24m[4m8[24m001 BS            DVSA  BEIGE 

[38;5;250m 9[39m  890[4m6[24m[4m6[24m[4m2[24m963  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-16       [4m1[24m[4m9[24m[4m7[24m459 BS            DVSA  BEIGE 

[38;5;250m10[39m  537[4m0[24m[4m3[24m[4m6[24m175  223[4m9[24m[4m8[24m[4m1[24m155 2023-08-29       [4m1[24m[4m8[24m[4m8[24m907 BS            DVSA  BEIGE 

   test_result

   [3m[38;5;246m<chr>[39m[23m      

[38;5;250m 1[39m P          

[38;5;250m 2[39m P          

[38;5;250m 3[39m P          

[38;5;250m 4[39m P          

[38;5;250m 5[39m P          

[38;5;250m 6[39m P          

[38;5;250m 7[39m P          

[38;5;250m 8[39m P          

[38;5;250m 9[39m P          

[38;5;250m10[39m P          

[38;5;246m# ℹ 82 more rows[39m

```
````
One thing to be aware of when using the `.dropDuplicates()` or `sparklyr::sdf_drop_duplicates()` function is that it is non-deterministic which means it may not always produce the same results on different runs of the code, depending on which variables you are using it on and how your data is partitioned. This is especially pertinent when dropping duplicates on a subset of your data (e.g. specifying a variable, rather than using the function on all variables as was shown above). 

Let's say we want to keep only one entry from vehicle id 223981155 per postcode area. To do this, we can use `.dropDuplicates()` or `sparklyr::sdf_drop_duplicates()` on the postcode_area variable. We have multiple entries for various postcodes and because we are only providing a single variable to drop duplicates on, Spark will simply keep the first entry of that variable it finds (without considering the values in the other variables). 

The important thing to note with this is that the results of your code will change **depending on how your data is partitioned**. There is no inherent order to how Spark processes the partitions when using this method, hence the variable results (unless your data is always only stored on 1 partition, then the result will remain the same).

The below provides an illustration of this.
````{tabs}
```{code-tab} py
#Check the current partitioning of our data
sample.rdd.getNumPartitions()
```

```{code-tab} r R


#Check current partition of the data
sparklyr::sdf_num_partitions(sample) %>% print()


```
````

````{tabs}

```{code-tab} plaintext Python Output
1
```
````

````{tabs}
```{code-tab} py
#Drop duplicates on postcode area
sample.dropDuplicates(['postcode_area']).show()
```

```{code-tab} r R


#Drop duplicates based on postcode_area
sample %>%
  sparklyr::sdf_drop_duplicates(cols = "postcode_area") %>%
  collect() %>%
  print(width = Inf)


```
````

````{tabs}

```{code-tab} plaintext Python Output
[Stage 12:>                                                         (0 + 1) / 1]
+----------+----------+----------+------------+-------------+----+------+-----------+
|   test_id|vehicle_id| test_date|test_mileage|postcode_area|make|colour|test_result|
+----------+----------+----------+------------+-------------+----+------+-----------+
|1712482407| 223981155|2023-09-08|      315802|           BB|DVSA| BEIGE|          P|
| 979064019| 223981155|2023-09-08|       91546|           BS|DVSA| BEIGE|          P|
| 952073263| 223981155|2023-07-25|       43980|           CF|DVSA| BEIGE|          P|
|1595426675| 223981155|2023-09-02|      184784|           CH|DVSA| BEIGE|          P|
| 194619757| 223981155|2023-08-31|      107114|           CO|DVSA| BEIGE|          P|
| 363813511| 223981155|2023-07-24|       68944|           DH|DVSA| BEIGE|          P|
| 818121815| 223981155|2023-05-31|      139581|            E|DVSA| BEIGE|          P|
| 130590349| 223981155|2023-02-06|      125337|           EH|DVSA| BEIGE|          P|
|1621487003| 223981155|2023-04-11|       96702|           LU|DVSA| BEIGE|          P|
| 217703609| 223981155|2023-07-28|       32574|           ML|DVSA| BEIGE|          P|
| 364306397| 223981155|2023-02-23|       71745|            N|DVSA| BEIGE|          P|
| 648395111| 223981155|2023-08-03|      185442|           NN|DVSA| BEIGE|          P|
| 182333359| 223981155|2023-09-07|      158000|           NP|DVSA| BEIGE|          P|
| 855822481| 223981155|2023-07-20|      152541|           NW|DVSA| BEIGE|          P|
| 488527389| 223981155|2023-04-12|      105257|           PE|DVSA| BEIGE|          P|
| 585395909| 223981155|2023-03-15|       65240|           SE|DVSA| BEIGE|          P|
|  74140977| 223981155|2023-01-14|      144703|           SR|DVSA| BEIGE|          P|
|1493456623| 223981155|2023-10-05|       49493|           TS|DVSA| BEIGE|          P|
| 378218713| 223981155|2023-04-14|      156723|           WN|DVSA| BEIGE|          P|
+----------+----------+----------+------------+-------------+----+------+-----------+
                                                                                
```

```{code-tab} plaintext R Output
[38;5;246m# A tibble: 19 × 8[39m

      test_id vehicle_id test_date  test_mileage postcode_area make  colour

        [3m[38;5;246m<int>[39m[23m      [3m[38;5;246m<int>[39m[23m [3m[38;5;246m<date>[39m[23m            [3m[38;5;246m<int>[39m[23m [3m[38;5;246m<chr>[39m[23m         [3m[38;5;246m<chr>[39m[23m [3m[38;5;246m<chr>[39m[23m 

[38;5;250m 1[39m [4m1[24m712[4m4[24m[4m8[24m[4m2[24m407  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-08       [4m3[24m[4m1[24m[4m5[24m802 BB            DVSA  BEIGE 

[38;5;250m 2[39m  979[4m0[24m[4m6[24m[4m4[24m019  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-08        [4m9[24m[4m1[24m546 BS            DVSA  BEIGE 

[38;5;250m 3[39m  952[4m0[24m[4m7[24m[4m3[24m263  223[4m9[24m[4m8[24m[4m1[24m155 2023-07-25        [4m4[24m[4m3[24m980 CF            DVSA  BEIGE 

[38;5;250m 4[39m [4m1[24m595[4m4[24m[4m2[24m[4m6[24m675  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-02       [4m1[24m[4m8[24m[4m4[24m784 CH            DVSA  BEIGE 

[38;5;250m 5[39m  194[4m6[24m[4m1[24m[4m9[24m757  223[4m9[24m[4m8[24m[4m1[24m155 2023-08-31       [4m1[24m[4m0[24m[4m7[24m114 CO            DVSA  BEIGE 

[38;5;250m 6[39m  363[4m8[24m[4m1[24m[4m3[24m511  223[4m9[24m[4m8[24m[4m1[24m155 2023-07-24        [4m6[24m[4m8[24m944 DH            DVSA  BEIGE 

[38;5;250m 7[39m  818[4m1[24m[4m2[24m[4m1[24m815  223[4m9[24m[4m8[24m[4m1[24m155 2023-05-31       [4m1[24m[4m3[24m[4m9[24m581 E             DVSA  BEIGE 

[38;5;250m 8[39m  130[4m5[24m[4m9[24m[4m0[24m349  223[4m9[24m[4m8[24m[4m1[24m155 2023-02-06       [4m1[24m[4m2[24m[4m5[24m337 EH            DVSA  BEIGE 

[38;5;250m 9[39m [4m1[24m621[4m4[24m[4m8[24m[4m7[24m003  223[4m9[24m[4m8[24m[4m1[24m155 2023-04-11        [4m9[24m[4m6[24m702 LU            DVSA  BEIGE 

[38;5;250m10[39m  217[4m7[24m[4m0[24m[4m3[24m609  223[4m9[24m[4m8[24m[4m1[24m155 2023-07-28        [4m3[24m[4m2[24m574 ML            DVSA  BEIGE 

[38;5;250m11[39m  364[4m3[24m[4m0[24m[4m6[24m397  223[4m9[24m[4m8[24m[4m1[24m155 2023-02-23        [4m7[24m[4m1[24m745 N             DVSA  BEIGE 

[38;5;250m12[39m  648[4m3[24m[4m9[24m[4m5[24m111  223[4m9[24m[4m8[24m[4m1[24m155 2023-08-03       [4m1[24m[4m8[24m[4m5[24m442 NN            DVSA  BEIGE 

[38;5;250m13[39m  182[4m3[24m[4m3[24m[4m3[24m359  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-07       [4m1[24m[4m5[24m[4m8[24m000 NP            DVSA  BEIGE 

[38;5;250m14[39m  855[4m8[24m[4m2[24m[4m2[24m481  223[4m9[24m[4m8[24m[4m1[24m155 2023-07-20       [4m1[24m[4m5[24m[4m2[24m541 NW            DVSA  BEIGE 

[38;5;250m15[39m  488[4m5[24m[4m2[24m[4m7[24m389  223[4m9[24m[4m8[24m[4m1[24m155 2023-04-12       [4m1[24m[4m0[24m[4m5[24m257 PE            DVSA  BEIGE 

[38;5;250m16[39m  585[4m3[24m[4m9[24m[4m5[24m909  223[4m9[24m[4m8[24m[4m1[24m155 2023-03-15        [4m6[24m[4m5[24m240 SE            DVSA  BEIGE 

[38;5;250m17[39m   74[4m1[24m[4m4[24m[4m0[24m977  223[4m9[24m[4m8[24m[4m1[24m155 2023-01-14       [4m1[24m[4m4[24m[4m4[24m703 SR            DVSA  BEIGE 

[38;5;250m18[39m [4m1[24m493[4m4[24m[4m5[24m[4m6[24m623  223[4m9[24m[4m8[24m[4m1[24m155 2023-10-05        [4m4[24m[4m9[24m493 TS            DVSA  BEIGE 

[38;5;250m19[39m  378[4m2[24m[4m1[24m[4m8[24m713  223[4m9[24m[4m8[24m[4m1[24m155 2023-04-14       [4m1[24m[4m5[24m[4m6[24m723 WN            DVSA  BEIGE 

   test_result

   [3m[38;5;246m<chr>[39m[23m      

[38;5;250m 1[39m P          

[38;5;250m 2[39m P          

[38;5;250m 3[39m P          

[38;5;250m 4[39m P          

[38;5;250m 5[39m P          

[38;5;250m 6[39m P          

[38;5;250m 7[39m P          

[38;5;250m 8[39m P          

[38;5;250m 9[39m P          

[38;5;250m10[39m P          

[38;5;250m11[39m P          

[38;5;250m12[39m P          

[38;5;250m13[39m P          

[38;5;250m14[39m P          

[38;5;250m15[39m P          

[38;5;250m16[39m P          

[38;5;250m17[39m P          

[38;5;250m18[39m P          

[38;5;250m19[39m P          

```
````

````{tabs}
```{code-tab} py
#Repartition our data
sample = sample.repartition(10)
sample.rdd.getNumPartitions()
```

```{code-tab} r R


#Repartition the data
sample <- sparklyr::sdf_repartition(sample, partitions = 10)
sparklyr::sdf_num_partitions(sample) %>% print()


```
````

````{tabs}

```{code-tab} plaintext Python Output
[Stage 15:>                                                         (0 + 1) / 1]
10
```

```{code-tab} plaintext R Output
[1] 10

```
````

````{tabs}
```{code-tab} py
#Rerun drop duplicates to see discrepancy
sample.dropDuplicates(['postcode_area']).show()
```

```{code-tab} r R


#Rerun drop duplicates
sample %>%
  sparklyr::sdf_drop_duplicates(cols = "postcode_area") %>%
  collect() %>%
  print(width = Inf)
  


```
````

````{tabs}

```{code-tab} plaintext Python Output
[Stage 18:=======================================>                 (7 + 1) / 10]
+----------+----------+----------+------------+-------------+----+------+-----------+
|   test_id|vehicle_id| test_date|test_mileage|postcode_area|make|colour|test_result|
+----------+----------+----------+------------+-------------+----+------+-----------+
| 693596559| 223981155|2023-09-08|       30568|           BB|DVSA| BEIGE|          P|
|1745334919| 223981155|2023-09-12|      133314|           BS|DVSA| BEIGE|          P|
| 952073263| 223981155|2023-07-25|       43980|           CF|DVSA| BEIGE|          P|
|1595426675| 223981155|2023-09-02|      184784|           CH|DVSA| BEIGE|          P|
| 194619757| 223981155|2023-08-31|      107114|           CO|DVSA| BEIGE|          P|
| 363813511| 223981155|2023-07-24|       68944|           DH|DVSA| BEIGE|          P|
| 818121815| 223981155|2023-05-31|      139581|            E|DVSA| BEIGE|          P|
| 130590349| 223981155|2023-02-06|      125337|           EH|DVSA| BEIGE|          P|
|1621487003| 223981155|2023-04-11|       96702|           LU|DVSA| BEIGE|          P|
| 804896923| 223981155|2023-06-21|      189384|           ML|DVSA| BEIGE|          P|
| 364306397| 223981155|2023-02-23|       71745|            N|DVSA| BEIGE|          P|
|1628501733| 223981155|2023-07-13|       75452|           NN|DVSA| BEIGE|          P|
|1710297771| 223981155|2023-08-16|      141404|           NP|DVSA| BEIGE|          P|
|1356082105| 223981155|2023-06-13|      167670|           NW|DVSA| BEIGE|          P|
|1968902105| 223981155|2023-04-14|      116994|           PE|DVSA| BEIGE|          P|
|1177098561| 223981155|2023-03-15|      121700|           SE|DVSA| BEIGE|          P|
|  74140977| 223981155|2023-01-14|      144703|           SR|DVSA| BEIGE|          P|
|1493456623| 223981155|2023-10-05|       49493|           TS|DVSA| BEIGE|          P|
|1507431521| 223981155|2023-03-08|      129272|           WN|DVSA| BEIGE|          P|
+----------+----------+----------+------------+-------------+----+------+-----------+
                                                                                
```

```{code-tab} plaintext R Output
[38;5;246m# A tibble: 19 × 8[39m

      test_id vehicle_id test_date  test_mileage postcode_area make  colour

        [3m[38;5;246m<int>[39m[23m      [3m[38;5;246m<int>[39m[23m [3m[38;5;246m<date>[39m[23m            [3m[38;5;246m<int>[39m[23m [3m[38;5;246m<chr>[39m[23m         [3m[38;5;246m<chr>[39m[23m [3m[38;5;246m<chr>[39m[23m 

[38;5;250m 1[39m  693[4m5[24m[4m9[24m[4m6[24m559  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-08        [4m3[24m[4m0[24m568 BB            DVSA  BEIGE 

[38;5;250m 2[39m [4m1[24m745[4m3[24m[4m3[24m[4m4[24m919  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-12       [4m1[24m[4m3[24m[4m3[24m314 BS            DVSA  BEIGE 

[38;5;250m 3[39m  952[4m0[24m[4m7[24m[4m3[24m263  223[4m9[24m[4m8[24m[4m1[24m155 2023-07-25        [4m4[24m[4m3[24m980 CF            DVSA  BEIGE 

[38;5;250m 4[39m [4m1[24m595[4m4[24m[4m2[24m[4m6[24m675  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-02       [4m1[24m[4m8[24m[4m4[24m784 CH            DVSA  BEIGE 

[38;5;250m 5[39m  194[4m6[24m[4m1[24m[4m9[24m757  223[4m9[24m[4m8[24m[4m1[24m155 2023-08-31       [4m1[24m[4m0[24m[4m7[24m114 CO            DVSA  BEIGE 

[38;5;250m 6[39m  363[4m8[24m[4m1[24m[4m3[24m511  223[4m9[24m[4m8[24m[4m1[24m155 2023-07-24        [4m6[24m[4m8[24m944 DH            DVSA  BEIGE 

[38;5;250m 7[39m  818[4m1[24m[4m2[24m[4m1[24m815  223[4m9[24m[4m8[24m[4m1[24m155 2023-05-31       [4m1[24m[4m3[24m[4m9[24m581 E             DVSA  BEIGE 

[38;5;250m 8[39m  130[4m5[24m[4m9[24m[4m0[24m349  223[4m9[24m[4m8[24m[4m1[24m155 2023-02-06       [4m1[24m[4m2[24m[4m5[24m337 EH            DVSA  BEIGE 

[38;5;250m 9[39m [4m1[24m621[4m4[24m[4m8[24m[4m7[24m003  223[4m9[24m[4m8[24m[4m1[24m155 2023-04-11        [4m9[24m[4m6[24m702 LU            DVSA  BEIGE 

[38;5;250m10[39m  804[4m8[24m[4m9[24m[4m6[24m923  223[4m9[24m[4m8[24m[4m1[24m155 2023-06-21       [4m1[24m[4m8[24m[4m9[24m384 ML            DVSA  BEIGE 

[38;5;250m11[39m  364[4m3[24m[4m0[24m[4m6[24m397  223[4m9[24m[4m8[24m[4m1[24m155 2023-02-23        [4m7[24m[4m1[24m745 N             DVSA  BEIGE 

[38;5;250m12[39m [4m1[24m628[4m5[24m[4m0[24m[4m1[24m733  223[4m9[24m[4m8[24m[4m1[24m155 2023-07-13        [4m7[24m[4m5[24m452 NN            DVSA  BEIGE 

[38;5;250m13[39m [4m1[24m710[4m2[24m[4m9[24m[4m7[24m771  223[4m9[24m[4m8[24m[4m1[24m155 2023-08-16       [4m1[24m[4m4[24m[4m1[24m404 NP            DVSA  BEIGE 

[38;5;250m14[39m [4m1[24m356[4m0[24m[4m8[24m[4m2[24m105  223[4m9[24m[4m8[24m[4m1[24m155 2023-06-13       [4m1[24m[4m6[24m[4m7[24m670 NW            DVSA  BEIGE 

[38;5;250m15[39m [4m1[24m968[4m9[24m[4m0[24m[4m2[24m105  223[4m9[24m[4m8[24m[4m1[24m155 2023-04-14       [4m1[24m[4m1[24m[4m6[24m994 PE            DVSA  BEIGE 

[38;5;250m16[39m [4m1[24m177[4m0[24m[4m9[24m[4m8[24m561  223[4m9[24m[4m8[24m[4m1[24m155 2023-03-15       [4m1[24m[4m2[24m[4m1[24m700 SE            DVSA  BEIGE 

[38;5;250m17[39m   74[4m1[24m[4m4[24m[4m0[24m977  223[4m9[24m[4m8[24m[4m1[24m155 2023-01-14       [4m1[24m[4m4[24m[4m4[24m703 SR            DVSA  BEIGE 

[38;5;250m18[39m [4m1[24m493[4m4[24m[4m5[24m[4m6[24m623  223[4m9[24m[4m8[24m[4m1[24m155 2023-10-05        [4m4[24m[4m9[24m493 TS            DVSA  BEIGE 

[38;5;250m19[39m [4m1[24m507[4m4[24m[4m3[24m[4m1[24m521  223[4m9[24m[4m8[24m[4m1[24m155 2023-03-08       [4m1[24m[4m2[24m[4m9[24m272 WN            DVSA  BEIGE 

   test_result

   [3m[38;5;246m<chr>[39m[23m      

[38;5;250m 1[39m P          

[38;5;250m 2[39m P          

[38;5;250m 3[39m P          

[38;5;250m 4[39m P          

[38;5;250m 5[39m P          

[38;5;250m 6[39m P          

[38;5;250m 7[39m P          

[38;5;250m 8[39m P          

[38;5;250m 9[39m P          

[38;5;250m10[39m P          

[38;5;250m11[39m P          

[38;5;250m12[39m P          

[38;5;250m13[39m P          

[38;5;250m14[39m P          

[38;5;250m15[39m P          

[38;5;250m16[39m P          

[38;5;250m17[39m P          

[38;5;250m18[39m P          

[38;5;250m19[39m P          

```
````
## Window method

If you are dropping duplicates on a particular variable and want to avoid the inherent non-deterministic nature of `.dropDuplicates()` or `sparklyr::drop_duplicates()`, an alternative method to use is a Window function with `F.row_number()`.

Simply `.partitionBy()` (or `dplyr::group_by()` in sparklyr) the same column(s) you want to remove duplicates from and use `.orderBy()` (or `dplyr::arrange()` in sparklyr) to specify the rows you want. This will return the rank within the groups. Note that `F.row_number()` (`dplyr::row_number()` in sparklyr) will return unique values within each group.

Let's say we want to retain the record with the highest test mileage in each postcode area. We can use `postcode_area` to partition the data, and then order it by `test_mileage`. We can then apply the `F.row_number()` function to the window to rank each of the entries in that postcode area based on their mileage.
````{tabs}
```{code-tab} py
#create our window based on postcode_area, ordered by test_mileage.
w = Window.partitionBy('postcode_area').orderBy(F.desc('test_mileage'))

#rank the rows within each postcode_area based on mileage.
windowed_duplicates = (sample.withColumn('row_n', F.row_number().over(w)))

windowed_duplicates.show()

```

```{code-tab} r R

#create our window based on postcode_area, ordered by test_mileage.
#rank the rows within each postcode_area based on mileage.
windowed_duplicates <- sample %>%
  dplyr::group_by(postcode_area) %>%
  dplyr::arrange(desc(test_mileage), .by_group = TRUE) %>%
  dplyr::mutate(row_n = dplyr::row_number()) %>%
  dplyr::ungroup()

windowed_duplicates %>%
  collect() %>%
  print(width = Inf)


```
````

````{tabs}

```{code-tab} plaintext Python Output
[Stage 65:>                                                         (0 + 1) / 1]
+----------+----------+----------+------------+-------------+----+------+-----------+-----+
|   test_id|vehicle_id| test_date|test_mileage|postcode_area|make|colour|test_result|row_n|
+----------+----------+----------+------------+-------------+----+------+-----------+-----+
|1712482407| 223981155|2023-09-08|      315802|           BB|DVSA| BEIGE|          P|    1|
| 608299587| 223981155|2023-09-08|      129365|           BB|DVSA| BEIGE|          P|    2|
|1243548035| 223981155|2023-09-08|      118059|           BB|DVSA| BEIGE|          P|    3|
|1041508715| 223981155|2023-08-15|      111890|           BB|DVSA| BEIGE|          P|    4|
|1569660147| 223981155|2023-09-05|       68311|           BB|DVSA| BEIGE|          P|    5|
|1531481473| 223981155|2023-09-09|       37932|           BB|DVSA| BEIGE|          P|    6|
| 693596559| 223981155|2023-09-08|       30568|           BB|DVSA| BEIGE|          P|    7|
| 243918949| 223981155|2023-09-16|      218001|           BS|DVSA| BEIGE|          P|    1|
| 890662963| 223981155|2023-09-16|      197459|           BS|DVSA| BEIGE|          P|    2|
| 537036175| 223981155|2023-08-29|      188907|           BS|DVSA| BEIGE|          P|    3|
| 250905065| 223981155|2023-08-28|      186254|           BS|DVSA| BEIGE|          P|    4|
| 746058855| 223981155|2023-09-19|      185623|           BS|DVSA| BEIGE|          P|    5|
|1096998015| 223981155|2023-08-23|      180932|           BS|DVSA| BEIGE|          P|    6|
|1517807321| 223981155|2023-09-22|      156503|           BS|DVSA| BEIGE|          P|    7|
|1563278235| 223981155|2023-06-20|      155722|           BS|DVSA| BEIGE|          P|    8|
|1141589769| 223981155|2023-09-19|      155482|           BS|DVSA| BEIGE|          P|    9|
| 291027007| 223981155|2023-09-19|      149706|           BS|DVSA| BEIGE|          P|   10|
|1803896485| 223981155|2023-08-11|      143010|           BS|DVSA| BEIGE|          P|   11|
|1745334919| 223981155|2023-09-12|      133314|           BS|DVSA| BEIGE|          P|   12|
| 972921705| 223981155|2023-08-16|      127893|           BS|DVSA| BEIGE|          P|   13|
+----------+----------+----------+------------+-------------+----+------+-----------+-----+
only showing top 20 rows
                                                                                
```

```{code-tab} plaintext R Output
       

[38;5;246m# A tibble: 92 × 9[39m

      test_id vehicle_id test_date  test_mileage postcode_area make  colour

        [3m[38;5;246m<int>[39m[23m      [3m[38;5;246m<int>[39m[23m [3m[38;5;246m<date>[39m[23m            [3m[38;5;246m<int>[39m[23m [3m[38;5;246m<chr>[39m[23m         [3m[38;5;246m<chr>[39m[23m [3m[38;5;246m<chr>[39m[23m 

[38;5;250m 1[39m [4m1[24m712[4m4[24m[4m8[24m[4m2[24m407  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-08       [4m3[24m[4m1[24m[4m5[24m802 BB            DVSA  BEIGE 

[38;5;250m 2[39m  608[4m2[24m[4m9[24m[4m9[24m587  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-08       [4m1[24m[4m2[24m[4m9[24m365 BB            DVSA  BEIGE 

[38;5;250m 3[39m [4m1[24m243[4m5[24m[4m4[24m[4m8[24m035  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-08       [4m1[24m[4m1[24m[4m8[24m059 BB            DVSA  BEIGE 

[38;5;250m 4[39m [4m1[24m041[4m5[24m[4m0[24m[4m8[24m715  223[4m9[24m[4m8[24m[4m1[24m155 2023-08-15       [4m1[24m[4m1[24m[4m1[24m890 BB            DVSA  BEIGE 

[38;5;250m 5[39m [4m1[24m569[4m6[24m[4m6[24m[4m0[24m147  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-05        [4m6[24m[4m8[24m311 BB            DVSA  BEIGE 

[38;5;250m 6[39m [4m1[24m531[4m4[24m[4m8[24m[4m1[24m473  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-09        [4m3[24m[4m7[24m932 BB            DVSA  BEIGE 

[38;5;250m 7[39m  693[4m5[24m[4m9[24m[4m6[24m559  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-08        [4m3[24m[4m0[24m568 BB            DVSA  BEIGE 

[38;5;250m 8[39m  243[4m9[24m[4m1[24m[4m8[24m949  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-16       [4m2[24m[4m1[24m[4m8[24m001 BS            DVSA  BEIGE 

[38;5;250m 9[39m  890[4m6[24m[4m6[24m[4m2[24m963  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-16       [4m1[24m[4m9[24m[4m7[24m459 BS            DVSA  BEIGE 

[38;5;250m10[39m  537[4m0[24m[4m3[24m[4m6[24m175  223[4m9[24m[4m8[24m[4m1[24m155 2023-08-29       [4m1[24m[4m8[24m[4m8[24m907 BS            DVSA  BEIGE 

   test_result row_n

   [3m[38;5;246m<chr>[39m[23m       [3m[38;5;246m<int>[39m[23m

[38;5;250m 1[39m P               1

[38;5;250m 2[39m P               2

[38;5;250m 3[39m P               3

[38;5;250m 4[39m P               4

[38;5;250m 5[39m P               5

[38;5;250m 6[39m P               6

[38;5;250m 7[39m P               7

[38;5;250m 8[39m P               1

[38;5;250m 9[39m P               2

[38;5;250m10[39m P               3

[38;5;246m# ℹ 82 more rows[39m

```
````
We can then simply `.filter()` or `dplyr::filter()` the row number to get the first entry of that postcode area. This will retain the vehicle with the highest mileage.
````{tabs}
```{code-tab} py
#retain the first entry (highest mileage) in each postcode_area
highest_mileage_duplicates = windowed_duplicates.filter(F.col('row_n')==1)

highest_mileage_duplicates.show()
```

```{code-tab} r R


#retain the first entry (highest mileage) in each postcode_area
highest_mileage_duplicates <- windowed_duplicates %>%
  dplyr::filter(row_n == 1) %>%
  dplyr::select(-row_n) %>%
  dplyr::arrange(postcode_area)

highest_mileage_duplicates %>%
  collect() %>%
  print(width = Inf)


```
````

````{tabs}

```{code-tab} plaintext Python Output
+----------+----------+----------+------------+-------------+----+------+-----------+-----+
|   test_id|vehicle_id| test_date|test_mileage|postcode_area|make|colour|test_result|row_n|
+----------+----------+----------+------------+-------------+----+------+-----------+-----+
|1712482407| 223981155|2023-09-08|      315802|           BB|DVSA| BEIGE|          P|    1|
| 243918949| 223981155|2023-09-16|      218001|           BS|DVSA| BEIGE|          P|    1|
| 952073263| 223981155|2023-07-25|       43980|           CF|DVSA| BEIGE|          P|    1|
|1595426675| 223981155|2023-09-02|      184784|           CH|DVSA| BEIGE|          P|    1|
| 194619757| 223981155|2023-08-31|      107114|           CO|DVSA| BEIGE|          P|    1|
| 363813511| 223981155|2023-07-24|       68944|           DH|DVSA| BEIGE|          P|    1|
| 818121815| 223981155|2023-05-31|      139581|            E|DVSA| BEIGE|          P|    1|
| 130590349| 223981155|2023-02-06|      125337|           EH|DVSA| BEIGE|          P|    1|
|1700053881| 223981155|2023-03-02|      103661|           LU|DVSA| BEIGE|          P|    1|
| 804896923| 223981155|2023-06-21|      189384|           ML|DVSA| BEIGE|          P|    1|
| 364306397| 223981155|2023-02-23|       71745|            N|DVSA| BEIGE|          P|    1|
| 278592185| 223981155|2023-08-03|      220980|           NN|DVSA| BEIGE|          P|    1|
| 649777577| 223981155|2023-08-14|      177213|           NP|DVSA| BEIGE|          P|    1|
| 986714569| 223981155|2023-05-16|      201262|           NW|DVSA| BEIGE|          P|    1|
| 126550303| 223981155|2023-03-01|      157423|           PE|DVSA| BEIGE|          P|    1|
|1177098561| 223981155|2023-03-15|      121700|           SE|DVSA| BEIGE|          P|    1|
|  74140977| 223981155|2023-01-14|      144703|           SR|DVSA| BEIGE|          P|    1|
|1493456623| 223981155|2023-10-05|       49493|           TS|DVSA| BEIGE|          P|    1|
| 378218713| 223981155|2023-04-14|      156723|           WN|DVSA| BEIGE|          P|    1|
+----------+----------+----------+------------+-------------+----+------+-----------+-----+
```

```{code-tab} plaintext R Output
6m# A tibble: 19 × 8[39m

      test_id vehicle_id test_date  test_mileage postcode_area make  colour

        [3m[38;5;246m<int>[39m[23m      [3m[38;5;246m<int>[39m[23m [3m[38;5;246m<date>[39m[23m            [3m[38;5;246m<int>[39m[23m [3m[38;5;246m<chr>[39m[23m         [3m[38;5;246m<chr>[39m[23m [3m[38;5;246m<chr>[39m[23m 

[38;5;250m 1[39m [4m1[24m712[4m4[24m[4m8[24m[4m2[24m407  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-08       [4m3[24m[4m1[24m[4m5[24m802 BB            DVSA  BEIGE 

[38;5;250m 2[39m  243[4m9[24m[4m1[24m[4m8[24m949  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-16       [4m2[24m[4m1[24m[4m8[24m001 BS            DVSA  BEIGE 

[38;5;250m 3[39m  952[4m0[24m[4m7[24m[4m3[24m263  223[4m9[24m[4m8[24m[4m1[24m155 2023-07-25        [4m4[24m[4m3[24m980 CF            DVSA  BEIGE 

[38;5;250m 4[39m [4m1[24m595[4m4[24m[4m2[24m[4m6[24m675  223[4m9[24m[4m8[24m[4m1[24m155 2023-09-02       [4m1[24m[4m8[24m[4m4[24m784 CH            DVSA  BEIGE 

[38;5;250m 5[39m  194[4m6[24m[4m1[24m[4m9[24m757  223[4m9[24m[4m8[24m[4m1[24m155 2023-08-31       [4m1[24m[4m0[24m[4m7[24m114 CO            DVSA  BEIGE 

[38;5;250m 6[39m  363[4m8[24m[4m1[24m[4m3[24m511  223[4m9[24m[4m8[24m[4m1[24m155 2023-07-24        [4m6[24m[4m8[24m944 DH            DVSA  BEIGE 

[38;5;250m 7[39m  818[4m1[24m[4m2[24m[4m1[24m815  223[4m9[24m[4m8[24m[4m1[24m155 2023-05-31       [4m1[24m[4m3[24m[4m9[24m581 E             DVSA  BEIGE 

[38;5;250m 8[39m  130[4m5[24m[4m9[24m[4m0[24m349  223[4m9[24m[4m8[24m[4m1[24m155 2023-02-06       [4m1[24m[4m2[24m[4m5[24m337 EH            DVSA  BEIGE 

[38;5;250m 9[39m [4m1[24m700[4m0[24m[4m5[24m[4m3[24m881  223[4m9[24m[4m8[24m[4m1[24m155 2023-03-02       [4m1[24m[4m0[24m[4m3[24m661 LU            DVSA  BEIGE 

[38;5;250m10[39m  804[4m8[24m[4m9[24m[4m6[24m923  223[4m9[24m[4m8[24m[4m1[24m155 2023-06-21       [4m1[24m[4m8[24m[4m9[24m384 ML            DVSA  BEIGE 

[38;5;250m11[39m  364[4m3[24m[4m0[24m[4m6[24m397  223[4m9[24m[4m8[24m[4m1[24m155 2023-02-23        [4m7[24m[4m1[24m745 N             DVSA  BEIGE 

[38;5;250m12[39m  278[4m5[24m[4m9[24m[4m2[24m185  223[4m9[24m[4m8[24m[4m1[24m155 2023-08-03       [4m2[24m[4m2[24m[4m0[24m980 NN            DVSA  BEIGE 

[38;5;250m13[39m  649[4m7[24m[4m7[24m[4m7[24m577  223[4m9[24m[4m8[24m[4m1[24m155 2023-08-14       [4m1[24m[4m7[24m[4m7[24m213 NP            DVSA  BEIGE 

[38;5;250m14[39m  986[4m7[24m[4m1[24m[4m4[24m569  223[4m9[24m[4m8[24m[4m1[24m155 2023-05-16       [4m2[24m[4m0[24m[4m1[24m262 NW            DVSA  BEIGE 

[38;5;250m15[39m  126[4m5[24m[4m5[24m[4m0[24m303  223[4m9[24m[4m8[24m[4m1[24m155 2023-03-01       [4m1[24m[4m5[24m[4m7[24m423 PE            DVSA  BEIGE 

[38;5;250m16[39m [4m1[24m177[4m0[24m[4m9[24m[4m8[24m561  223[4m9[24m[4m8[24m[4m1[24m155 2023-03-15       [4m1[24m[4m2[24m[4m1[24m700 SE            DVSA  BEIGE 

[38;5;250m17[39m   74[4m1[24m[4m4[24m[4m0[24m977  223[4m9[24m[4m8[24m[4m1[24m155 2023-01-14       [4m1[24m[4m4[24m[4m4[24m703 SR            DVSA  BEIGE 

[38;5;250m18[39m [4m1[24m493[4m4[24m[4m5[24m[4m6[24m623  223[4m9[24m[4m8[24m[4m1[24m155 2023-10-05        [4m4[24m[4m9[24m493 TS            DVSA  BEIGE 

[38;5;250m19[39m  378[4m2[24m[4m1[24m[4m8[24m713  223[4m9[24m[4m8[24m[4m1[24m155 2023-04-14       [4m1[24m[4m5[24m[4m6[24m723 WN            DVSA  BEIGE 

   test_result

   [3m[38;5;246m<chr>[39m[23m      

[38;5;250m 1[39m P          

[38;5;250m 2[39m P          

[38;5;250m 3[39m P          

[38;5;250m 4[39m P          

[38;5;250m 5[39m P          

[38;5;250m 6[39m P          

[38;5;250m 7[39m P          

[38;5;250m 8[39m P          

[38;5;250m 9[39m P          

[38;5;250m10[39m P          

[38;5;250m11[39m P          

[38;5;250m12[39m P          

[38;5;250m13[39m P          

[38;5;250m14[39m P          

[38;5;250m15[39m P          

[38;5;250m16[39m P          

[38;5;250m17[39m P          

[38;5;250m18[39m P          

[38;5;250m19[39m P          

```
````
For further detailed information on Window functions in Spark and other use cases please see the [relevant section](https://best-practice-and-impact.github.io/ons-spark/spark-functions/window-functions.html).
````{tabs}
```{code-tab} py
#Close Spark session
spark.stop()
```

```{code-tab} r R

#Close Spark session
spark_disconnect(sc)

```
````
