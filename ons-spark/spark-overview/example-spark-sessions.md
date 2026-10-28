## Example Spark Sessions

This page shows the two Spark session categories most users need:

1. Local mode for prototyping, unit tests, and very small samples.
2. Default session for almost all cluster work when you are starting out.

For Spark 3.5+, the safest starting point is usually the platform default session rather than a hand-tuned session template. Many settings that used to appear in example sessions are now defaults, are inherited from the platform, or should only be changed when there is evidence from the Spark UI or failed runs. This aligns with [Guidance on Spark Sessions](../spark-overview/spark-session-guidance), [Configuration Hierarchy and `spark-defaults.conf`](../spark-overview/spark-defaults), [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html), and [Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html).

This page is intentionally platform-agnostic. If you work on a managed Spark platform (for example, an organisation-specific CML/CDP environment), use this guidance alongside your internal platform configuration guidance.

Remember to close sessions when finished so resources can be reused.

````{tabs}
```{code-tab} py
spark.stop()
```

```{code-tab} r R
sparklyr::spark_disconnect(sc)
```
````

### Local mode

Local mode runs Spark inside a single container or node and does not request a separate cluster allocation.

Use local mode for:
- Syntax checks and quick experimentation
- Unit tests
- Dummy, synthetic, or heavily sampled data
- Reproducing logic issues before scaling out

Avoid local mode for workloads that need realistic shuffle behaviour, large joins, or production-like storage and memory characteristics. See [Guidance on Spark Sessions](../spark-overview/spark-session-guidance).

````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[*]")
    .appName("local-mode")
    .getOrCreate()
)
```

```{code-tab} r R
library(sparklyr)

sc <- spark_connect(
  master = "local[*]",
  app_name = "local-mode"
)
```
````

Notes on local master settings:
- `local[*]` uses all available CPU cores/threads.
- `local` uses one worker thread.
- `local[n]` uses exactly `n` worker threads.

For laptop setup requirements, see [Getting Started with Spark](../spark-overview/spark-start).

### Default session

The default session is the right first choice for nearly all work on the cluster.

Use the default session for:
- New or unfamiliar datasets
- Exploratory analysis
- Early pipeline development
- Most jobs where there is no evidence yet that a custom setting is needed

In Spark 3.5+, this matters more than keeping a large library of named session templates. Adaptive Query Execution (AQE) is enabled in modern Spark, and several cluster-level behaviours are inherited from the platform. Copying old session settings often adds noise without improving outcomes. See [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html).

````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("default-session")
    .getOrCreate()
)
```

```{code-tab} r R
library(sparklyr)

sc <- spark_connect(
  app_name = "default-session"
)
```
````

### How to inspect the default session

The effective default session is whatever Spark and the platform inject at runtime. The most reliable method is to inspect a live session rather than copy settings from static snippets.

From a terminal or notebook session:

````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("inspect-default-session")
    .getOrCreate()
)

for key, value in sorted(spark.sparkContext.getConf().getAll()):
    print(f"{key}={value}")

spark.stop()
```

```{code-tab} r R
library(sparklyr)

sc <- spark_connect(app_name = "inspect-default-session")
conf <- spark_config(sc)

writeLines(
  paste(names(conf), conf, sep = "=")
)

spark_disconnect(sc)
```
````

From the Spark UI:
1. `Environment` tab: inspect active Spark properties.
2. `Executors` tab: inspect active executors, memory, and cores.

Reference: [Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html).

### Workload size guide

Row count is only a starting signal. Row width, joins, shuffles, window functions, Python UDFs, and skew can move a workload into a larger resource band even with moderate row counts.

| Workload band | Typical row-count starting point | Typical characteristics | Guidance |
|---|---:|---|---|
| Small-session workload | Up to ~1 million rows | Narrow tables, filters, simple aggregations, light joins | Start with the default session. Use local mode only for sampled or dummy data. |
| Medium-session workload | ~1 million to ~50 million rows | Repeated transformations, moderate joins, standard production pipelines | Stay on the default session first. Inspect the Spark UI before changing settings. |
| Large-session workload | ~50 million to ~200 million rows | Heavy shuffles, wide aggregations, complex joins, window functions | Default session may still work, but validate partitioning, skew, and storage behaviour early. |
| Extra-large-session workload | 200 million+ rows, or multiple very large inputs | Severe skew, repeated shuffle stages, large intermediate outputs | Treat as an evidence-driven tuning case. Start with defaults, then add targeted settings only for observed bottlenecks. |

### Low-risk optimisations before session tuning

Before changing Spark/session settings, prefer query and data-layout improvements first.

| Tactic | Why it is usually safer than changing the session first |
|---|---|
| Filter early and join later | Reduces shuffle volume before expensive stages. |
| Select only needed columns | Improves projection pushdown and reduces I/O for Parquet/ORC. |
| Prefer Parquet for analytical pipelines | Improves scan and compression behaviour for analytics. |
| Avoid Python UDFs unless necessary | Preserves Catalyst optimisation and avoids Python row-by-row overhead. |
| Use broadcast joins only when one side is genuinely small | Can remove a shuffle stage when safe in memory. |
| Use `coalesce()` after filtering and `repartition()` only when redistribution is needed | Avoids unnecessary full shuffles. |
| Check `explain()` and Spark UI before tuning configs | Confirms whether the problem is skew, shuffle size, scan size, or join strategy. |

References: [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html), [Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html).

### Partition count configuration

Partition count is one of the first settings to review when a default session is slow, but change it from evidence rather than habit.

Spark documents `spark.sql.shuffle.partitions` with a default of `200` for many shuffle operations such as joins and aggregations. A practical starting heuristic is to aim for shuffle partitions in the rough range of 128 MB to 1 GB each, then validate in the Spark UI.

Approximate formula:

`number_of_partitions = data_size_gb / target_partition_size_gb`

````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession

data_size_gb = 100
target_partition_size_gb = 0.5
optimal_partitions = max(1, int(data_size_gb / target_partition_size_gb))

spark = (
    SparkSession.builder
    .appName("partition-tuned-session")
    .config("spark.sql.shuffle.partitions", str(optimal_partitions))
    .getOrCreate()
)
```

```{code-tab} r R
library(sparklyr)

data_size_gb <- 100
target_partition_size_gb <- 0.5
optimal_partitions <- max(1, as.integer(data_size_gb / target_partition_size_gb))

config <- spark_config()
config$spark.sql.shuffle.partitions <- as.character(optimal_partitions)

sc <- spark_connect(
  app_name = "partition-tuned-session",
  config = config
)
```
````

Reference: [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html).

### Ad hoc settings for special cases

Add extra Spark settings only when they map to an observed problem such as skew, shuffle spill, too many small tasks, or executor local storage pressure.

| Symptom or use case | Settings to consider | Notes |
|---|---|---|
| Over-partitioned workloads with many tiny tasks | Lower `spark.sql.shuffle.partitions` from default | Confirm with Spark UI evidence first. |
| Under-partitioned large workloads with long-running tasks | Increase `spark.sql.shuffle.partitions` | Pair changes with stage/task evidence from Spark UI. |
| Executor local storage pressure during heavy shuffle | `spark.dynamicAllocation.shuffleTracking.timeout=300s` | Consider for cluster-specific troubleshooting cases. |
| Shuffle files accumulating in long-running jobs | `spark.cleaner.periodicGC.interval=5min` | Consider with platform support where relevant. |
| Heavy S3 write pressure | `spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer` | Apply selectively and validate impact. |

References: [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html), [Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html).

### Troubleshooting workflow

When a default session struggles:
1. Check Spark UI for skew, long stages, shuffle volume, failed executors, and storage growth.
2. Confirm effective configuration from Spark UI `Environment` or a live session dump.
3. Classify the issue: memory pressure, local disk pressure, skew, partition count, or Python overhead.
4. Add one or two targeted settings only for the observed symptom.
5. Re-run and confirm the effect before keeping changes.

### Further references

Internal references:
- [Guidance on Spark Sessions](../spark-overview/spark-session-guidance)
- [Configuration Hierarchy and `spark-defaults.conf`](../spark-overview/spark-defaults)
- [Getting Started with Spark](../spark-overview/spark-start)

External references:
- [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html)
- [`SparkSession.builder` documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html#pyspark.sql.SparkSession.builder)
- [`spark.stop()` documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.stop.html)
- [`spark_connect()` and `spark_disconnect()` documentation](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark-connections.html)
- [Reviewed external best-practice article: Apache Spark Performance Tuning: 20 Optimizations That Actually Work](https://medium.com/@reliabledataengineering/apache-spark-performance-tuning-20-optimizations-that-actually-work-1815e1ca98f9)