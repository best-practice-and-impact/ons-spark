# Sampling Big Data for Exploratory Data Analysis

Generally, it is unnecessary and not good practice to carry out the majority of data analysis on a full dataset, especially if it is very large (~10 million rows+). In the exploratory data analysis (EDA), detailed analysis and development phases of your work it is suggested that you keep the size of your data as small as possible to conserve compute resource, time and money. Ideally, you should take a sample small enough that you can work in Python or R instead of PySpark and SparklyR - we recommend anything less than a few million rows. For more information on choosing the right tool please refer to our [guidance](https://gitlab-app-l-01/DAP_CATS/cdp-guidance-wiki/-/wikis/Choosing-the-right-tool-in-DAP).

One way to reduce your data is to take samples, especially in the EDA phase. Simply, EDA allows you to investigate your data before deep diving into further analysis, you can summarise main characteristics, assess data types, spot  anomalies and trends, test hypotheses and check assumptions; you can also determine if the techniques you want to use for further analysis are appropriate for your data. As a result, you want your sample to be as representative of the overall dataset as possible and therefore sample verification is also essential.

In this section we will cover pre-sampling, sampling methods and sample verification. The worked example we will use is the DVLA MOT dataset for 2023.

### Setting up Spark and loading your data
````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = (SparkSession.builder
          .appName('sampling_for_eda')
          .getOrCreate())

```

```{code-tab} r R

# Load in required packages
library(sparklyr)
library(dplyr)
library(pillar)

# Set up Spark session
default_config <- sparklyr::spark_config()

sc <- spark_connect(master = "local",
                  app_name = "sampling_for_eda",
                  config = default_config)


# Check Spark connection is open
  spark_connection_is_open(sc)

```
````

````{tabs}
```{code-tab} plaintext Python Output

Setting spark.hadoop.yarn.resourcemanager.principal to alexandra.snowdon

```

```{code-tab} plaintext R Output

[1] TRUE

```
````

````{tabs}
```{code-tab} py

# Read in the MOT dataset
#mot_path = "s3a://onscdp-dev-data01-5320d6ca/bat/dapcats/mot_test_results.csv
mot_path = "C:/Users/snowda/repos/data/mot_test_results.csv"

mot = (spark.read.csv(mot_path, header=True, inferSchema=True))

# Check schema and preview data
mot.printSchema()

```

```{code-tab} r R

# Read in the MOT dataset
#mot_path = "s3a://onscdp-dev-data01-5320d6ca/bat/dapcats/mot_test_results.csv"
mot_path = "C:/Users/snowda/repos/data/mot_test_results.csv"
mot <- sparklyr::spark_read_csv(
    sc,
    mot_path,
    header = TRUE,
    infer_schema = TRUE)

# Check schema and preview data
pillar::glimpse(mot)

```
````

````{tabs}
```{code-tab} plaintext Python Output

root
 |-- test_id: integer (nullable = true)
 |-- vehicle_id: integer (nullable = true)
 |-- test_date: timestamp (nullable = true)
 |-- test_class_id: integer (nullable = true)
 |-- test_type: string (nullable = true)
 |-- test_result: string (nullable = true)
 |-- test_mileage: integer (nullable = true)
 |-- postcode_area: string (nullable = true)
 |-- make: string (nullable = true)
 |-- model: string (nullable = true)
 |-- colour: string (nullable = true)
 |-- fuel_type: string (nullable = true)
 |-- cylinder_capacity: integer (nullable = true)
 |-- first_use_date: timestamp (nullable = true)

 ```

 ```{code-tab} plaintext R Output

Rows: ??
Columns: 14
Database: spark_connection
$ test_id           <int> 1687460261, 1007821341, 1268040905, 232884115, 12510…
$ vehicle_id        <int> 1131526890, 211522675, 1218838379, 159956284, 851633…
$ test_date         <date> 2023-04-28, 2023-04-28, 2023-04-28, 2023-04-28, 202…
$ test_class_id     <int> 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7, 4…
$ test_type         <chr> "NT", "NT", "RT", "NT", "NT", "NT", "NT", "NT", "NT"…
$ test_result       <chr> "F", "P", "P", "F", "P", "P", "P", "P", "P", "P", "P…
$ test_mileage      <int> 129661, 26597, 11803, 148187, 12915, 22281, 30140, 2…
$ postcode_area     <chr> "HU", "IV", "LA", "BD", "PO", "WA", "PH", "B", "S", …
$ make              <chr> "BMW", "FORD", "THE EXPLORER GROUP", "AUDI", "DACIA"…
$ model             <chr> "525", "FIESTA", "UNCLASSIFIED", "A3", "DUSTER", "CO…
$ colour            <chr> "BLACK", "BLUE", "WHITE", "WHITE", "ORANGE", "BLUE",…
$ fuel_type         <chr> "DI", "PE", "DI", "DI", "PE", "PE", "PE", "DI", "DI"…
$ cylinder_capacity <int> 2497, 998, 1997, 1968, 1330, 1398, 1199, 1500, 2993,…
$ first_use_date    <date> 2002-05-27, 2016-06-27, 2017-03-03, 2010-10-30, 201…

```
````

````{tabs}
```{code-tab} py

# Check the size of the data we are working with
mot.count()

```

```{code-tab} r R

# Check the size of the data we are working with
mot %>%
    sparklyr::sdf_nrow() %>%
    print()

```
````

````{tabs}
```{code-tab} plaintext Python Output

42216721

```

```{code-tab} plaintext R Output

[1] 42216721

```
````

It is important to consider what data is really needed for your purpose. Filter out the unnecessary data early on to reduce the size of your dataset and therefore compute resource, time and money. For example, select which columns you need to be in your dataset. Once you know which columns you want for analysis, you won't to load in the overall dataset every time you open a session, you could just use `sparklyr::select(column_name_1, column_name_2, ..., column_name_5)` at the point of reading in your data.

````{tabs}
```{code-tab} py

# Select columns we want to work with
mot = (mot.select("vehicle_id", "test_date", "test_mileage", "postcode_area", "make", "colour", "cylinder_capacity"))

# Change column data type or column name formatting. For example, change test_date to a date instead of an integer.
mot = mot.withColumn("test_date", F.to_date("test_date", "yyyy-MM-dd"))

# Re-check the schema to ensure your changes have been made to the mot dataframe
mot.printSchema()

```

```{code-tab} r R

# Select columns we want to work with
mot <- mot %>%
  sparklyr::select(vehicle_id, test_date, test_mileage, postcode_area, make, colour, cylinder_capacity)

# Change column data type or column name formatting. For example, change test_date to a date instead of an integer.
mot %>% 
    dplyr::mutate(test_date = as.date(test_date))

# Re-check the schema to ensure your changes have been made to the mot dataframe
pillar::glimpse(mot)

```
````

````{tabs}
```{code-tab} plaintext Python Output

root
 |-- vehicle_id: integer (nullable = true)
 |-- test_date: date (nullable = true)
 |-- test_mileage: integer (nullable = true)
 |-- postcode_area: string (nullable = true)
 |-- make: string (nullable = true)
 |-- colour: string (nullable = true)
 |-- cylinder_capacity: integer (nullable = true)

 ```

```{code-tab} plaintext R Output

Rows: ??
Columns: 7
Database: spark_connection
$ vehicle_id        <int> 1131526890, 211522675, 1218838379, 159956284, 851633…
$ test_date         <date> 2023-04-28, 2023-04-28, 2023-04-28, 2023-04-28, 202…
$ test_mileage      <int> 129661, 26597, 11803, 148187, 12915, 22281, 30140, 2…
$ postcode_area     <chr> "HU", "IV", "LA", "BD", "PO", "WA", "PH", "B", "S", …
$ make              <chr> "BMW", "FORD", "THE EXPLORER GROUP", "AUDI", "DACIA"…
$ colour            <chr> "BLACK", "BLUE", "WHITE", "WHITE", "ORANGE", "BLUE",…
$ cylinder_capacity <int> 2497, 998, 1997, 1968, 1330, 1398, 1199, 1500, 2993,…

```
````

## Pre-sampling

Pre-sampling is executed before taking a sample your big data, it works to clean your data and it gives a 'quick' idea of what the data looks like and helps inform decisions on what to include in your sample. Pre-sampling involves looking at nulls, duplicates, quick summary stats. If these steps are not taken then the results ouputted from analysis on your sample could be skewed and non-representative of the big data.

It is important to consider the order you execute things as this will affect your analysis. For example, if you filter out unwanted columns additional duplicates could be thrown up as you may have removed the columns with the differing data. The same goes for nulls; when you filter out unwanted columns the number of nulls could reduce as you may have removed the deciding columns and therefore when you use a function such as `na.omit()`, rows that may have been removed before filtering would actually be left in your sample. 

It is good practice to check the missing data and duplicate data first, do not just omit them. If you find unusual outputs you can investigate further to determine whether they are true nulls or duplicates. In the worked example on this page we will remove rows with null values and fully duplicated rows - note that this may not be appropriate for your data! Please refer to our accompanying pages which offer further guidance on how to handle [missing data with imputation](https://best-practice-and-impact.github.io/ons-spark/spark-analysis/interpolation.html?highlight=imputation#interpolation-in-spark) and how to work with [duplicates](https://best-practice-and-impact.github.io/ons-spark/spark-analysis/working-with-duplicates.html?highlight=duplicates#working-with-duplicates).

````{tabs}
```{code-tab} py

# Check for missing data first, do not just omit it. The example uses the test_mileage column.

from pyspark.sql.functions import col, isnan
mot.filter(col("test_mileage").isNull()).count()

```

```{code-tab} r R

# Check for missing data first, do not just omit it. The example uses the test_mileage column.

mot %>% 
    filter(is.na(test_mileage)) %>% 
    sdf_nrow() %>% 
    print()

```
````

````{tabs}
```{code-tab} plaintext Python Output

324129

```

```{code-tab} plaintext R Output

[1] 324129

```
````

````{tabs}
```{code-tab} py

# If appropriate for your data, remove any rows with missing data under ANY variable.
mot = mot.dropna()

mot.count()

```

```{code-tab} r R

# If appropriate for your data, remove any rows with missing data under ANY variable.
mot <- mot %>%
    na.omit()

 mot %>% sdf_nrow()
```
````

````{tabs}
```{code-tab} plaintext Python Output

41614543
```

```{code-tab} plaintext R

[1] 41614543

```
````

There are two ways to look at duplicates in SparklyR. You can apply `sdf_distinct()` to the entire dataframe which will remove duplicate rows based on all columns and ensures each row is unique. Alternatively, `sdf_drop_duplicates()` is more flexible as you can remove duplicate rows based on specific columns. Here, we will use the first option to remove fully duplicated rows from the filtered dataset. As with missing values, it is advised that you look at the duplicated data first before removing it; for this, please refer to our [working with duplicates guidance](https://best-practice-and-impact.github.io/ons-spark/spark-analysis/working-with-duplicates.html).

````{tabs}
```{code-tab} py

# Identify duplicate data in the mot dataframe
duplicates = (mot
            .groupBy(mot.columns)
            .count()
            .filter(F.col("count") > 1)
            .orderBy('count', ascending=False))

duplicates.count()

```

```{code-tab} r 

# Identify duplicate data in the mot dataframe
duplicates <- mot %>%
  group_by(vehicle_id, test_date, test_mileage, postcode_area, make, colour, cylinder_capacity) %>%
  summarise(count = n()) %>%
  filter(count > 1)
  arrange(desc(count))

duplicates %>%
    sdf_nrow() %>%
    print()

```
````

````{tabs}
```{code-tab} plaintext Python Output

2007820

```

```{code-tab} plaintext R Output

[1] 2007820

```
````
````{tabs}
```{code-tab} py

# If appropriate for your data, remove the duplicated rows and preview your clean dataset 
(i.e. removal of duplicates and missing values).

mot_clean = mot.dropDuplicates()

mot_clean_size <- mot_clean.count()
mot_clean_size 

mot_clean.printSchema()
mot_clean.show(5)
```

```{code-tab} r R

# If appropriate for your data, remove the duplicated rows and preview your clean dataset 
(i.e. removal of duplicates and missing values).

mot_clean <- sdf_distinct(mot)

mot_clean_size <- mot_clean %>%
                            sparklyr::sdf_nrow()

mot_clean_size %>% print()

pillar::glimpse(mot_clean)

```
````

````{tabs}
```{code-tab} plaintext Python Ouput

39601678

root
 |-- vehicle_id: integer (nullable = true)
 |-- test_date: date (nullable = true)
 |-- test_mileage: integer (nullable = true)
 |-- postcode_area: string (nullable = true)
 |-- make: string (nullable = true)
 |-- colour: string (nullable = true)
 |-- cylinder_capacity: integer (nullable = true)

[Stage 20:>                                                         (0 + 1) / 1]
+----------+----------+------------+-------------+----------+------+-----------------+
|vehicle_id| test_date|test_mileage|postcode_area|      make|colour|cylinder_capacity|
+----------+----------+------------+-------------+----------+------+-----------------+
| 956475846|2023-02-22|      160714|           SY|     HONDA| BLACK|             1246|
| 797546870|2023-02-22|       92582|            N|     HONDA|SILVER|             1799|
|1338418818|2023-02-22|      168403|           SP|     HONDA|SILVER|             1998|
| 546006004|2023-02-22|       68636|           ME|     MAZDA|  GOLD|             1598|
| 167704309|2023-02-22|       60569|           SE|VOLKSWAGEN|SILVER|             1198|
+----------+----------+------------+-------------+----------+------+-----------------+
only showing top 5 rows

```

```{code-tab} plaintext R Output

[1] 39601678

Rows: ??
Columns: 7
Database: spark_connection
$ vehicle_id        <int> 631967128, 949451480, 1095423235, 744210417, 9604893…
$ test_date         <date> 2023-04-28, 2023-04-28, 2023-04-28, 2023-04-28, 202…
$ test_mileage      <int> 16393, 67737, 66732, 53372, 60347, 28404, 83208, 427…
$ postcode_area     <chr> "PL", "LS", "ST", "AB", "NN", "BB", "CO", "AB", "PL"…
$ make              <chr> "HARLEY-DAVIDSON", "RENAULT", "AUDI", "RENAULT", "MI…
$ colour            <chr> "RED", "BLACK", "WHITE", "SILVER", "BLUE", "WHITE", …
$ cylinder_capacity <int> 1200, 1149, 1968, 1598, 1998, 1248, 1598, 2179, 998,…

```
````

````{tabs}
```{code-tab} py

# It can also be useful to view distinct groups in the categorical columns across your data frame -
# for example showing the distinct groups in the 'colour' column will show you all the different colours of cars reported in the dataframe.
# This could also help you spot anomalies or errors.

mot_clean.select("colour").distinct().show()

```

```{code-tab} r R

# It can also be useful to view distinct groups in the categorical columns across your data frame, for example showing the distinct groups in the 'colour' column will show you all the different colours of cars reported in the dataframe. This could also help you spot anomalies or errors.
colour <- mot_clean %>% 
  sparklyr::select(colour) %>% 
  sparklyr::sdf_distinct() %>%
  sparklyr::sdf_collect()

 colour 

```
````

````{tabs}
```{code-tab} plaintext Python Output

[Stage 11:======================================================> (27 + 1) / 28]
+------------+
|      colour|
+------------+
|         RED|
|       WHITE|
|       BLACK|
|       BEIGE|
|       GREEN|
|       CREAM|
|      SILVER|
|        GOLD|
|      PURPLE|
|       BROWN|
|MULTI-COLOUR|
|      YELLOW|
|      MAROON|
|  NOT STATED|
|        GREY|
|   TURQUOISE|
|      BRONZE|
|        BLUE|
|        PINK|
|      ORANGE|
+------------+
```

```{code-tab} r R

colour
# A tibble: 20 × 1
   colour
   <chr>
 1 RED
 2 CREAM
 3 GOLD
 4 BROWN
 5 YELLOW
 6 BRONZE      
 7 BLACK
 8 MULTI-COLOUR
 9 NOT STATED
10 SILVER
11 PINK
12 PURPLE
13 BLUE
14 GREY
15 BEIGE
16 MAROON
17 TURQUOISE
18 WHITE
19 ORANGE
20 GREEN

```
````
As one last check of the data before sampling it is worth checking for collinearity between the numerical variables. The outputs could induce further filtering of the data. Collinearity can cause instability within your data as it shows strong links between variables that could skew outputs of models you apply. Therefore, it is good to check this early on in your EDA; if any collinearity is revealed then you can work a solution before it causes an issue, such as removing one of the two variables that have been identified as colinear. The default of both Pyspark and SparklyR correlation functions is to use the Pearson method.

````{tabs}
```{code-tab} py

from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import pandas as pd

input_col_names = ["vehicle_id","test_mileage","cylinder_capacity"]

vector_assembler = VectorAssembler(inputCols=input_col_names, outputCol="features")
data_vector = vector_assembler.transform(mot)
features_vector  = data_vector.select("features")
#features_vector.show()
matrix = Correlation.corr(features_vector, "features").collect()[0][0]
corr_matrix = matrix.toArray().tolist()
features = input_col_names
corr_matrix_df = pd.DataFrame(data=corr_matrix, columns = features, index = features) 
corr_matrix_df

```

```{code-tab} r R

corr_matrix <- mot_distinct %>%
              ml_corr(c("vehicle_id", "test_mileage", "cylinder_capacity"))
corr_matrix

```
````

````{tabs}
```{code-tab} plaintext Python Output

	                 vehicle_id	test_mileage	cylinder_capacity
vehicle_id	        1.000000  -0.000168	    -0.00001
test_mileage	     -0.000168	 1.000000	     0.26972
cylinder_capacity	 -0.000010	 0.269720	     1.00000

```

```{code-tab} plaintext R Output

# A tibble: 3 × 3
    vehicle_id test_mileage cylinder_capacity
         <dbl>        <dbl>             <dbl>
1  1              -0.000128      -0.000000333
2 -0.000128        1              0.270      
3 -0.000000333     0.270          

```
````

