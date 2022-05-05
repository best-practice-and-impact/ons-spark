## Exercises

### Shuffling

You have been assigned to review some code from an existing business as usual pipeline, written a couple of years ago. The code works as desired and passes all unit tests, but the business users are finding it slow to run and would like you to make it more efficient. Although the example data supplied are small, it must also be able to work efficiently for large data.

The objective of the code is to produce a DataFrame with the ten most expensive animal rescue incidents, consisting of `animal_group`, `postcode_district`, `incident_number`, `total_cost`, `population` and `animal_count`, where `animal_count` is the total number of each `animal_group` in the data. The row count should also be returned.

Any animal group with four or fewer animals needs to be filtered out for disclosure reasons.

Your task is to identify unnecessary shuffles in the code and re-write it, without affecting the output. You may want to look at the section on Optimising and Avoiding Shuffles for some ideas.

````{tabs}
```{code-tab} py
import yaml
from pyspark.sql import SparkSession, functions as F

with open("ons-spark/config.yaml") as f:
    config = yaml.safe_load(f)
    
spark = (SparkSession.builder.master("local[2]")
         .appName("exercises")
         # Disable broadcast join by default
         .config("spark.sql.autoBroadcastJoinThreshold", -1)
         .getOrCreate())

rescue_path = config["rescue_path"]
population_path = config["population_path"]

# Read source data
rescue = (spark.read.parquet(rescue_path)
          .select("incident_number", "animal_group", "total_cost", "postcode_district"))
population = spark.read.parquet(population_path)

# Sort and preview source data
rescue = rescue.orderBy("incident_number")
rescue.show()
rescue.count()
population = population.orderBy("postcode_district")
population.show()
population.count()

# Join population data to rescue data
rescue_with_pop = rescue.join(population, on="postcode_district", how="left")

# Sort and preview
rescue_with_pop.orderBy("incident_number")
rescue_with_pop.show()
rescue_with_pop.count()

# Get count of each animal group
animal_count = (rescue
                .groupBy("animal_group")
                .agg(F.count("incident_number").alias("animal_count")))

# Join this back to the rescue_with_pop DF
rescue_with_pop = rescue_with_pop.join(animal_count, on="animal_group", how="left")

# Filter out animals with small counts
rescue_with_pop = rescue_with_pop.filter(F.col("animal_count") >= 5)

# Sort the final data
rescue_with_pop = rescue_with_pop.orderBy(F.desc("total_cost"))

# Get the top 10 and row count
top_10 = rescue_with_pop.limit(10).toPandas()
row_count = rescue_with_pop.count()
```

```{code-tab} r R
library(sparklyr)
library(dplyr)

config <- yaml::yaml.load_file("ons-spark/config.yaml")

exercise_config <- sparklyr::spark_config()
# Disable broadcast join by default
exercise_config$spark.sql.autoBroadcastJoinThreshold <- -1

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "exercises",
    config = exercise_config)

# Read source data
rescue <- sparklyr::spark_read_parquet(sc, config$rescue_path) %>%
    sparklyr::select(incident_number, animal_group, total_cost, postcode_district)
population <- sparklyr::spark_read_parquet(sc, config$population_path)


# Sort and preview source data
rescue <- rescue %>% sparklyr::sdf_sort("incident_number")
rescue %>% head(5) %>% sparklyr::collect()
rescue %>% sparklyr::sdf_nrow()
population <- population %>% sparklyr::sdf_sort("postcode_district")
population %>% head(5) %>% sparklyr::collect()
population %>% sparklyr::sdf_nrow()

# Join population data to rescue data
rescue_with_pop <- rescue %>%
    sparklyr::left_join(population, by="postcode_district")

# Sort and preview
rescue_with_pop <- rescue_with_pop %>% sparklyr::sdf_sort("incident_number")
rescue_with_pop %>% head(5) %>% sparklyr::collect()
rescue_with_pop %>% sparklyr::sdf_nrow()

# Get count of each animal group
animal_count <- rescue_with_pop %>%
    dplyr::group_by(animal_group) %>%
    dplyr::summarise(animal_count = n())

# Join this back to the rescue_with_pop DF
rescue_with_pop <- rescue_with_pop %>%
    sparklyr::left_join(animal_count, by="animal_group")

# Filter out animals with small counts
rescue_with_pop <- rescue_with_pop %>%
    sparklyr::filter(animal_count >= 5)

# Sort the final data
rescue_with_pop <- rescue_with_pop %>%
    dplyr::arrange(desc(total_cost))

# Get the top 10 and row count
top_10 <- rescue_with_pop %>% head(10) %>% sparklyr::collect()
row_count <- rescue_with_pop %>% sparklyr::sdf_nrow()
```
````
<details>
<summary>Answer</summary>

Using the ideas from the Optimising and Avoiding Shuffle chapter, we can:
- Minimise actions: many of the `.show()`/`head(n) %>% collect` and `.count()`/`sdf_nrow()` actions are not needed. Just keep the ones at the end.
- Caching: The two actions at the end are needed, so to avoid full recalculation of the DF, use a cache.
- Reduce size of DataFrame: The filter and join can be moved earlier in the code without affecting the output.
- Broadcast join: Change the join from a sort merge join to a broadcast join. Here the broadcast is forced with the hint `F.broadcast()`/`sdf_broadcast()`, but you could also turn on automatic broadcasting by setting the `spark.sql.autoBroadcastJoinThreshold` in the config, or removing it altogether to use the default.
- Avoid unnecessary sorting: Many of the sorts are not needed; remove all apart from the last one.
- Window functions: Change the group by and join to use a window function.

The code is now much shorter and neater, and will run much faster. Here we have chained operations together; this will not affect the efficiency of the code, but does make it easier to read.

You could eliminate the need for Spark altogether and just use pandas or base R/dplyr, providing that the source data is small enough.

````{tabs}
```{code-tab} py
import yaml
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

with open("ons-spark/config.yaml") as f:
    config = yaml.safe_load(f)
    
spark = (SparkSession.builder.master("local[2]")
         .appName("exercises")
         # Disable broadcast join by default
         .config("spark.sql.autoBroadcastJoinThreshold", -1)
         .getOrCreate())

rescue_path = config["rescue_path"]
population_path = config["population_path"]

# Read source data
rescue = (spark.read.parquet(rescue_path)
          .select("incident_number", "animal_group", "total_cost", "postcode_district"))
population = spark.read.parquet(population_path)

rescue_with_pop = (
    rescue
    # Use a window function to get the count of each animal group
    .withColumn("animal_count",
                F.count("incident_number")
                .over(Window.partitionBy("animal_group")))

    # Filter out animals with small counts
    .filter(F.col("animal_count") >= 5)

    # Join population data to rescue data, using a broadcast join
    .join(F.broadcast(population), on="postcode_district", how="left")
    
    # Sort the final data and cache
    .orderBy(F.desc("total_cost")).cache()
)

# Get row_count first to fill the cache, then top 10
row_count = rescue_with_pop.count()
top_10 = rescue_with_pop.limit(10).toPandas()
```

```{code-tab} r R
library(sparklyr)
library(dplyr)

config <- yaml::yaml.load_file("ons-spark/config.yaml")

exercise_config <- sparklyr::spark_config()
# Disable broadcast join by default
exercise_config$spark.sql.autoBroadcastJoinThreshold <- -1

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "exercises",
    config = exercise_config)

# Read source data
rescue <- sparklyr::spark_read_parquet(sc, config$rescue_path) %>%
    sparklyr::select(incident_number, animal_group, total_cost, postcode_district)
population <- sparklyr::spark_read_parquet(sc, config$population_path)

rescue_with_pop <- rescue %>%    
    # Use a window function to get the count of each animal group
    dplyr::group_by(animal_group) %>%
    sparklyr::mutate(animal_count = n()) %>%
    dplyr::ungroup() %>%

    # Filter out animals with small counts
    sparklyr::filter(animal_count >= 5) %>%

    # Join population data to rescue data, using a broadcast join
    sparklyr::left_join(sparklyr::sdf_broadcast(population), by="postcode_district") %>%
    
    # Sort the final data
    dplyr::arrange(desc(total_cost))

# Register and cache
rescue_with_pop <- sparklyr::sdf_register(rescue_with_pop, "rescue_with_pop")
sparklyr::tbl_cache(sc, "rescue_with_pop", force=TRUE)

# Get the top 10 and row count
top_10 <- rescue_with_pop %>% head(10) %>% sparklyr::collect()
row_count <- rescue_with_pop %>% sparklyr::sdf_nrow()
```
````
</details>
