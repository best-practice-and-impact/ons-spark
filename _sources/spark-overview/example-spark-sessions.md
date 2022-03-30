## Example Spark Sessions

This document gives some example Spark sessions. For more information on Spark sessions and why you need to be careful with memory usage, please consult the [Guidance on Spark Sessions](../spark-overview/spark-session-guidance) and [Configuration Hierarchy and `spark-defaults.conf`](../spark-overview/spark-defaults).


Remember to only use a Spark session for as long as you need. It's good etiquette to use [`spark.stop()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.stop.html) (for PySpark) or [`spark_disconnect(sc)`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark-connections.html) (for sparklyr) in your scripts. Stopping the CDSW or Jupyter Notebook session will also close the Spark session if one is running.

### Default/Blank Session

As a starting point you can create a Spark session with all the default options. This is the bare minimum you need to create a Spark session and will work fine for many DAP users.

Please use this session by default or if unsure in any way about your resource requirements.

Note that for PySpark, `.config("spark.ui.showConsoleProgress", "false")` is still recommended for use with this session; this will stop the console progress in Spark, which sometimes obscures results from displaying properly.

Details:
- Will give you the default config options
    
Use case:
- When unsure of your requirements
    
Example of actual usage:
- Investigation of new or unfamiliar data sources
- Building a new pipeline where full user requirements aren't yet known

````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("default-session")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)
```
```{code-tab} r R
library(sparklyr)

default_config <- sparklyr::spark_config()

sc <- sparklyr::spark_connect(
  master = "yarn-client",
  app_name = "default-session",
  config = default_config)
```
````

### Small Session

This is the smallest session that will realistically be used in DAP. It is similar in size to that used for DAP CATS training, as the low memory and only one core means several people can run this session at the same time on Dev Test, the cluster with the smallest capacity.

Details:
- Only 1g of memory and 3 executors
- Only 1 core
- Number of partitions are limited to 12, which can improve performance with smaller data

Use case:
- Simple data exploration of small survey data
- Training and demonstrations when several people need to run Spark sessions simultaneously

Example of actual usage:
- Used for DAPCATS PySpark training, with mostly simple calculations on small data

````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("small-session")
    .config("spark.executor.memory", "1g")
    .config("spark.executor.cores", 1)
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.maxExecutors", 3)
    .config("spark.sql.shuffle.partitions", 12)
    .config("spark.shuffle.service.enabled", "true")
    .config("spark.ui.showConsoleProgress", "false")
    .enableHiveSupport()
    .getOrCreate()
)
```
```{code-tab} r R
library(sparklyr)

small_config <- sparklyr::spark_config()
small_config$spark.executor.memory <- "1g"
small_config$spark.executor.cores <- 1
small_config$spark.dynamicAllocation.enabled <- "true"
small_config$spark.dynamicAllocation.maxExecutors <- 3
small_config$spark.sql.shuffle.partitions <- 12
small_config$spark.shuffle.service.enabled <- "true"

sc <- sparklyr::spark_connect(
  master = "yarn-client",
  app_name = "small-session",
  config = small_config)
```
````
### Medium Session

A standard session used for analysing survey or synthetic datasets. Also used for some Production pipelines based on survey and/or smaller administrative data.

Details:
- 6g of memory and 3 executors
- 3 cores
- Number of partitions are limited to 18, which can improve performance with smaller data

Use case:
- Developing code in Dev Test
- Data exploration in Production
- Developing Production pipelines on a sample of data
- Running smaller Production pipelines on mostly survey data

Example of actual usage:
- Complex calculations, but on smaller synthetic data in Dev Test

````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession

spark = (
        SparkSession.builder.appName("medium-session")
        .config("spark.executor.memory", "6g")
        .config("spark.executor.cores", 3)
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.maxExecutors", 3)
        .config("spark.sql.shuffle.partitions", 18)
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .enableHiveSupport()
        .getOrCreate()
      )
 ```
 ```{code-tab} r R
 library(sparklyr)

medium_config <- sparklyr::spark_config()
medium_config$spark.executor.memory <- "6g"
medium_config$spark.executor.cores <- 3
medium_config$spark.dynamicAllocation.enabled <- "true"
medium_config$spark.dynamicAllocation.maxExecutors <- 3
medium_config$spark.sql.shuffle.partitions <- 18
medium_config$spark.shuffle.service.enabled <- "true"

sc <- sparklyr::spark_connect(
  master = "yarn-client",
  app_name = "medium-session",
  config = medium_config)
```
````

### Large Session

Session designed for running Production pipelines on large administrative data, rather than just survey data. Will often develop using a sample and a smaller session then change to this once the pipeline is complete.

Details:
- 10g of memory and 5 executors
- 1g of memory overhead
- 5 cores, which is generally optimal on larger sessions
- The default number of 200 partitions
    
Use case:
- Production pipelines on administrative data
- Cannot be used in Dev Test, as it exceeds the 9 GB limit per executor

Example of actual usage:
- One administrative dataset of 100 million rows
- Many calculations

````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("large-session")
    .config("spark.executor.memory", "10g")
    .config("spark.yarn.executor.memoryOverhead", "1g")
    .config("spark.executor.cores", 5)
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.maxExecutors", 5)
    .config("spark.sql.shuffle.partitions", 200)
    .config("spark.shuffle.service.enabled", "true")
    .config("spark.ui.showConsoleProgress", "false")
    .enableHiveSupport()
    .getOrCreate()
)
```
```{code-tab} r R
library(sparklyr)

large_config <- sparklyr::spark_config()
large_config$spark.executor.memory <- "10g"
large_config$spark.yarn.executor.memoryOverhead <- "1g"
large_config$spark.executor.cores <- 5
large_config$spark.dynamicAllocation.enabled <- "true"
large_config$spark.dynamicAllocation.maxExecutors <- 5
large_config$spark.sql.shuffle.partitions <- 200
large_config$spark.shuffle.service.enabled <- "true"

sc <- sparklyr::spark_connect(
  master = "yarn-client",
  app_name = "large-session",
  config = large_config)

```
````
### Extra Large session

Used for the most complex pipelines, with huge administrative data sources and complex calculations. This uses a large amount of resource on the cluster, so only use when running Production pipelines.

It is even more important when using more resource to close your Spark session once finished.

Details:
- 20g of memory and 12 executors
- 2g of memory overhead
- 5 cores; using too many cores can actually cause worse performance on larger sessions
- 240 partitions; not significantly higher than the default of 200, but it is best for these to be a multiple of cores and executors

Use case:
- Running large, complex pipelines in Production on mostly administrative data
- Do not use for development purposes; use a smaller session and work on a sample of data or synthetic data

Example of actual usage:
- Three administrative datasets of around 300 million rows
- Significant calculations, including joins and writing and reading to many intermediate tables

````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("xl-session")
    .config("spark.executor.memory", "20g")
    .config("spark.yarn.executor.memoryOverhead", "2g")
    .config("spark.executor.cores", 5)
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.maxExecutors", 12)
    .config("spark.sql.shuffle.partitions", 240)
    .config("spark.shuffle.service.enabled", "true")
    .config("spark.ui.showConsoleProgress", "false")
    .enableHiveSupport()
    .getOrCreate()
)
```
```{code-tab} r R
library(sparklyr)

xl_config <- sparklyr::spark_config()
xl_config$spark.executor.memory <- "20g"
xl_config$spark.yarn.executor.memoryOverhead <- "2g"
xl_config$spark.executor.cores <- 5
xl_config$spark.dynamicAllocation.enabled <- "true"
xl_config$spark.dynamicAllocation.maxExecutors <- 12
xl_config$spark.sql.shuffle.partitions <- 240
xl_config$spark.shuffle.service.enabled <- "true"

sc <- sparklyr::spark_connect(
  master = "yarn-client",
  app_name = "xl-session",
  config = xl_config)
```
````

### Closing Spark sessions

Remember to close your sessions once finished, so that the memory can be re-allocated to another user.

````{tabs}
```{code-tab} py
spark.stop()
```
```{code-tab} r R
sparklyr::spark_disconnect(sc)
```
````

### Further Resources

Spark at the ONS Articles:
- [Guidance on Spark Sessions](../spark-overview/spark-session-guidance)
- [Configuration Hierarchy and `spark-defaults.conf`](../spark-overview/spark-defaults)

PySpark Documentation:
- [`SparkSession.builder`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.html#pyspark.sql.SparkSession.builder)
- [`spark.stop()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.stop.html)

sparklyr and tidyverse Documentation:
- [`spark_connect`/`spark_disconnect()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark-connections.html)