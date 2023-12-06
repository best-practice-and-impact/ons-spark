## Partitions when Unioning or Binding DataFrames (working title)

You can append two DataFrames in PySpark that have the same schema with the [`.union()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.union) operation. In sparklyr, the equivalent operation is [`sdf_bind_rows()`](https://spark.rstudio.com/reference/sdf_bind.html) and R users will often refer to appending two DataFrames as *binding*.

The `.union()` function in PySpark and the `sdf_bind_rows()` function in sparklyr are both equivalent to `UNION ALL` in SQL. A regular `UNION` operation in SQL will remove duplicates, whereas `.union()` in PySpark and `sdf_bind_rows()` in sparklyr will not.

Unlike joining two DataFrames, `.union()`/`sdf_bind_rows()`  does not involve a full shuffle, as the data does not move between partitions. Instead, the number of partitions in the unioned DataFrame is equal to the sum of the number of partitions in the two source DataFrames, i.e. if you union a DataFrame consisting of 100 partitions and one consisting of 50 partitions, your unioned DataFrame will have 150 partitions.

To avoid excessive number of partitions, you can use [`.coalesce()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.coalesce) or [`.repartition()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.repartition) to reduce the number of partitions (use [`sdf_coalesce()`](https://spark.rstudio.com/reference/sdf_coalesce.html) or [`sdf_repartition()`](https://spark.rstudio.com/reference/sdf_repartition.html)
 in sparklyr). The DataFrame will also get repartitioned when a *wide transformation* is applied to the DataFrame (also called a shuffle), e.g. with a `.groupBy()` or `.orderBy()`.
 
It is also worth being aware that storing data on HDFS as many small files is inefficient, both in terms of how the data is stored and in reading it in. Unioning many DataFrames and then writing straight to HDFS can be a cause of this issue. For more information on the small file issue, see the Section on Coalcessing partitions **NATHAN**



First we start a Spark session, read the Animal Rescue data, group by animal and year, and then count. The grouping and aggregation will cause a shuffle, meaning that the DataFrame will have 12 partitions, as we set `spark.sql.shuffle.partitions` to `12` in the `SparkSession.builder`.

````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession, functions as F
import yaml

# Create spark session
spark = (SparkSession.builder.master("local[2]")
         .appName("Partitioning-Unioning")
         .config("spark.sql.shuffle.partitions", 12)
         .getOrCreate())

# Set the data path
with open("../../../config.yaml") as f:
    config = yaml.safe_load(f)

rescue_path_csv = config["rescue_path_csv"]

# Read in and shuffle data
rescue = (spark.read.csv(rescue_path_csv, header=True, inferSchema=True)  
          .withColumnRenamed("IncidentNumber", "incident_number")
          .withColumnRenamed("AnimalGroupParent", "animal_group")
          .withColumnRenamed("CalYear", "cal_year")
          .groupBy("animal_group", "cal_year")
          .agg(F.count("incident_number").alias("animal_count")))

rescue.limit(5).toPandas()
```

```{code-tab} r R

library(sparklyr)
library(dplyr)

# Create spark session
small_config <- sparklyr::spark_config()
small_config$spark.sql.shuffle.partitions <- 12

sc <- sparklyr::spark_connect(
  master = "local",
  app_name = "Partitioning-Unioning",
  config = small_config)

sparklyr::spark_connection_is_open(sc)

# Set the data path
config <- yaml::yaml.load_file("ons-spark/config.yaml")
# Read in and shuffle data
rescue <- sparklyr::spark_read_csv(sc, config$rescue_path_csv, header=TRUE, infer_schema=TRUE)

rescue <- rescue %>%
    dplyr::rename(
        incident_number = IncidentNumber,
        animal_group = AnimalGroupParent,
        cal_year = CalYear) %>%
        dplyr::group_by(animal_group,cal_year) %>%
        dplyr::count(animal_count = count())

rescue %>% head(5)

```
````

````{tabs}

```{code-tab} plaintext Python Output
                       animal_group  cal_year  animal_count
0                              Bird      2010            99
1                           Hamster      2011             3
2  Unknown - Domestic Animal Or Pet      2012            18
3  Unknown - Heavy Livestock Animal      2012             4
4                             Sheep      2012             1
```

```{code-tab} plaintext R Output
[1] TRUE
# Source: spark<?> [?? x 4]
# Groups: animal_group, cal_year
  animal_group cal_year animal_count     n
  <chr>           <int>        <dbl> <dbl>
1 Bird             2010           99    99
2 Deer             2015            6     6
3 Deer             2016           14    14
4 Ferret           2018            1     1
5 Fox              2017           33    33
```
````
We can confirm the number of partitions of the `rescue` DataFrame using `.rdd.getNumPartitions()`:
````{tabs}
```{code-tab} py
print('Rescue partitions:',rescue.rdd.getNumPartitions())
```

```{code-tab} r R

print(paste0("Rescue partitions: ", sparklyr::sdf_num_partitions(rescue)))

```
````

````{tabs}

```{code-tab} plaintext Python Output
Rescue partitions: 12
```

```{code-tab} plaintext R Output
[1] "Rescue partitions: 12"
```
````
Now create will create some smaller DataFrames, containing different animals, by filtering the rescue data. We will preview just the `dogs` data, as all others will be of a similar format:
````{tabs}
```{code-tab} py
dogs = rescue.filter(F.col("animal_group") == "Dog")
cats = rescue.filter(F.col("animal_group") == "Cat")
hamsters = rescue.filter(F.col("animal_group") == "Hamster")

dogs.limit(5).toPandas()
```

```{code-tab} r R

dogs <- rescue %>% sparklyr::filter(animal_group == 'Dog')
cats <- rescue %>% sparklyr::filter(animal_group == 'Cat')
hamsters <- rescue %>% sparklyr::filter(animal_group == 'Hamster')

dogs %>% head(5)

```
````

````{tabs}

```{code-tab} plaintext Python Output
  animal_group  cal_year  animal_count
0          Dog      2011           103
1          Dog      2017            81
2          Dog      2009           132
3          Dog      2012           100
4          Dog      2014            90
```

```{code-tab} plaintext R Output
# Source: spark<?> [?? x 4]
# Groups: animal_group, cal_year
  animal_group cal_year animal_count     n
  <chr>           <int>        <dbl> <dbl>
1 Dog              2011          103   103
2 Dog              2017           81    81
3 Dog              2009          132   132
4 Dog              2012          100   100
5 Dog              2014           90    90
```
````
We can check that each of these DataFrames has 12 partitions:
````{tabs}
```{code-tab} py
print(' Dog partitions:', dogs.rdd.getNumPartitions(),
      '\n Cat partitions:', cats.rdd.getNumPartitions(),
      '\n Hamster partitions:', hamsters.rdd.getNumPartitions())
```

```{code-tab} r R

print(paste0("Dogs partitions: ", sparklyr::sdf_num_partitions(dogs)))
print(paste0("Cats partitions: ", sparklyr::sdf_num_partitions(cats)))
print(paste0("Hamsters partitions: ", sparklyr::sdf_num_partitions(hamsters)))

```
````

````{tabs}

```{code-tab} plaintext Python Output
 Dog partitions: 12 
 Cat partitions: 12 
 Hamster partitions: 12
```

```{code-tab} plaintext R Output
[1] "Dogs partitions: 12"
[1] "Cats partitions: 12"
[1] "Hamsters partitions: 12"
```
````
When we apply the union function, the number of partitions in the unioned DataFrame will be the sum of partitions in each DataFrame. In this case we will now have 12 + 12 = 24 partitions:
````{tabs}
```{code-tab} py
dogs_and_cats = dogs.union(cats)
print('Dogs and Cats union partitions:',dogs_and_cats.rdd.getNumPartitions())
```

```{code-tab} r R

dogs_and_cats = sparklyr::sdf_bind_rows(dogs,cats)
print(paste0("Dogs and Cats union partitions: ", sparklyr::sdf_num_partitions(dogs_and_cats)))

```
````

````{tabs}

```{code-tab} plaintext Python Output
Dogs and Cats union partitions: 24
```

```{code-tab} plaintext R Output
[1] "Dogs and Cats union partitions: 24"
```
````
Unioning another DataFrame adds another 12 partitions to make 36 (24 + 12):
````{tabs}
```{code-tab} py
dogs_cats_and_hamsters = dogs_and_cats.union(hamsters)
print('Dogs, Cats and Hamsters union partitions:',dogs_cats_and_hamsters.rdd.getNumPartitions())
```

```{code-tab} r R

dogs_cats_and_hamsters = sparklyr::sdf_bind_rows(dogs_and_cats,hamsters)
print(paste0("Dogs, Cats and Hamsters union partitions: ", sdf_num_partitions(dogs_cats_and_hamsters)))

```
````

````{tabs}

```{code-tab} plaintext Python Output
Dogs, Cats and Hamsters union partitions: 36
```

```{code-tab} plaintext R Output
[1] "Dogs, Cats and Hamsters union partitions: 36"
```
````
Although we only have 36 partitions here it is easy to see how this might get excessive with too many `.union()` statements. This can also become a bigger problem if the number of partitons is left to its default value of 200, where we would end up with 600 partitions! 

A subsequent shuffle (e.g. sorting the DataFrame) will reset the number of partitions to that specified in `spark.sql.shuffle.partitions`:
````{tabs}
```{code-tab} py
dogs_cats_and_hamsters.orderBy("animal_group", "cal_year").rdd.getNumPartitions()
```

```{code-tab} r R

sparklyr::sdf_num_partitions(dogs_cats_and_hamsters %>% sparklyr::sdf_sort(c('animal_group','cal_year')))

```
````

````{tabs}

```{code-tab} plaintext Python Output
12
```

```{code-tab} plaintext R Output
[1] 12
```
````

You can also use `.repartition()` or `.coalesce()`. `.repartition()` involves a shuffle of the DataFrame and puts the data into roughly equal partition sizes, whereas `.coalesce()` combines partitions without a full shuffle, and so is more efficient, although at the potential cost of less equal partition sizes and therefore potential skew in the data. See the see the Section on Coalcessing partitions for more information. **NATHAN**


````{tabs}
```{code-tab} py
dogs_cats_and_hamsters.repartition(20).rdd.getNumPartitions()
```

```{code-tab} r R

sparklyr::sdf_num_partitions(dogs_cats_and_hamsters %>% sparklyr::sdf_repartition(20))

```
````

````{tabs}

```{code-tab} plaintext Python Output
20
```

```{code-tab} plaintext R Output
[1] 20
```
````

### Further Resources

Spark at the ONS Articles:
- [Checkpoints and Staging Tables](../spark-concepts/checkpoint-staging.md)

PySpark Documentation:
- [`.union()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.union)
- [`.coalesce()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.coalesce)
- [`.repartition()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.repartition)

sparklyr Documentation:
- [`sdf_bind_rows()`](https://spark.rstudio.com/reference/sdf_bind.html)
- [`sdf_coalesce()`](https://spark.rstudio.com/reference/sdf_coalesce.html)
- [`sdf_repartition()`](https://spark.rstudio.com/reference/sdf_repartition.html)

Spark SQL Documentation:
- [`round`](https://spark.apache.org/docs/latest/api/sql/index.html#round)
- [`bround`](https://spark.apache.org/docs/latest/api/sql/index.html#bround)