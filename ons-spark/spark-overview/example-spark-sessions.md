## Example Spark Sessions

This page shows the two Spark session categories most users need:

1. Local mode for prototyping, unit tests, and very small samples.
2. Default session for almost all cluster work when you are starting out.

For Spark 3.5+, the safest starting point is usually the platform default session rather than a hand-tuned session template. Many settings that used to appear in example sessions are now defaults, are inherited from the platform, or should only be changed when there is evidence from the Spark UI or failed runs. This aligns with [Guidance on Spark Sessions](../spark-overview/spark-session-guidance), [Configuration Hierarchy and `spark-defaults.conf`](../spark-overview/spark-defaults), [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html), and [Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html).

This page is intentionally platform-agnostic. If you work on a managed Spark platform (for example, an organisation-specific CML/CDP environment), use this guidance alongside your internal platform configuration guidance.

For Cloudera AI on Cloud users, the [Cloudera AI on Cloud documentation](https://docs.cloudera.com/machine-learning/cloud/index.html) provides platform-specific guidance. Check that the documentation applies to your deployment and runtime; it does not replace your organisation's configuration guidance.

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

(example-spark-default-session)=
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

(example-spark-inspect-session)=
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

### Optimisation and troubleshooting

If the default session struggles, use [Ideas for optimising Spark code](../spark-concepts/optimisation-tips) for workload assessment, query improvements, AQE and partitioning guidance, targeted settings, and a troubleshooting workflow. Inspect the running session's configuration using the examples above before considering an override.

### Further references

Internal references:
- [Guidance on Spark Sessions](../spark-overview/spark-session-guidance)
- [Configuration Hierarchy and `spark-defaults.conf`](../spark-overview/spark-defaults)
- [Getting Started with Spark](../spark-overview/spark-start)
- [Ideas for optimising Spark code](../spark-concepts/optimisation-tips)

External references:
- [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html)
- [`SparkSession.builder` documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html#pyspark.sql.SparkSession.builder)
- [`spark.stop()` documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.stop.html)
- [`spark_connect()` and `spark_disconnect()` documentation](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark-connections.html)
