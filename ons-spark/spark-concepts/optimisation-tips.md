## Ideas for optimising Spark code

This article was created through a combination of experience, knowledge, frustration and Spark training courses. It is not an exhaustive list of optimisation techniques. It is a summary list of ideas for an intermediate level Spark engineer, analyst or data scientist to try when developing Spark applications. 

### Assumptions 
- You have spent time getting to know your data, e.g. exploring [data quality dimensions](https://www.gov.uk/government/publications/the-government-data-quality-framework/the-government-data-quality-framework#Data-quality-dimensions), distributions within key variables, size of dataset on disk/memory etc.
- You are aware of good coding practices e.g. recommendations made in the [Quality Assurance of Code for Analysis and Research](https://best-practice-and-impact.github.io/qa-of-code-guidance/intro.html) book (also known as *the Duck Book*).

### Workload size guide

Row count is only a rough starting signal, not a rule for choosing resources. Row width, joins, shuffles, window functions, Python UDFs, and skew can make even moderate row counts expensive. Start with the default session and assess the actual workload before tuning.

| Workload band | Illustrative row count | Typical characteristics | Guidance |
|---|---:|---|---|
| Small | Up to ~1 million rows | Narrow tables, filters, simple aggregations, light joins | Start with the default session. Use local mode only for sampled or dummy data. |
| Medium | ~1 million to ~50 million rows | Repeated transformations, moderate joins, standard production pipelines | Stay on the default session first. Inspect the Spark UI before changing settings. |
| Large | ~50 million to ~200 million rows | Heavy shuffles, wide aggregations, complex joins, window functions | Default session may still work, but validate partitioning, skew, and storage behaviour early. |
| Extra-large | 200 million+ rows, or multiple very large inputs | Severe skew, repeated shuffle stages, large intermediate outputs | Start with defaults, then add targeted settings only for observed bottlenecks. |

### Get the basics right

Prefer query and data-layout improvements before changing session settings. The sections below cover reducing data volume, choosing file formats, avoiding unnecessary actions, and managing shuffles.

#### 1. Start simple
- Use a [default Spark session](../spark-overview/example-spark-sessions.html#example-spark-default-session). Do not copy Spark sessions from other projects.
- Consider taking a [sample of your data](../spark-functions/sampling) while you are developing the code for faster processing.
- Structure the code logically; not too much code in a single script.
- Check that [Adaptive Query Execution (AQE)](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution) is enabled. It is enabled by default in Apache Spark 3.2+, but platform settings can override this.

#### 2. Use the correct file formats
- Do not store intermediate data as text or CSV files. These should only be used when outputs need to be human readable. When you need to read from these file types always specify a schema.
- Use parquet files or Hive tables stored as parquet.
- Select only the columns needed; projection pushdown can reduce I/O for Parquet and ORC.
- Make sure you do not have lots of small files on HDFS (bad for Hadoop and bad for Spark).

#### 3. Get rid of actions that are not needed
- Get rid of intermediate actions put in during development (e.g. [`.show()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.show.html), [`glimpse()`](https://pillar.r-lib.org/reference/glimpse.html), [`.count()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.count.html), [`sdf_nrow()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_dim.html)); these make Spark reprocess the result.
- Is it possible to replace any remaining actions with some transformations?
- Replace loops that each trigger an action with grouped or window-based transformations where appropriate; see [Groups, not loops](../spark-concepts/groups-not-loops) for the patterns and examples.
- If there is a repeated action needed on a DataFrame consider persisting (memory or disk).

#### 4. Partitioning and shuffling
- Filter early, where the query semantics allow, to reduce data entering joins and other expensive stages.
- Shuffles are expensive; avoid unnecessary redistribution with `repartition()`.
- Consider [`.coalesce()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.coalesce.html)/[`sdf_coalesce()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_coalesce.html) after substantial filtering or to reduce output file counts, but do not apply it routinely. Too few partitions can reduce parallelism.

With both `spark.sql.adaptive.enabled` and `spark.sql.adaptive.coalescePartitions.enabled` enabled, AQE can [automatically coalesce post-shuffle partitions](https://spark.apache.org/docs/latest/sql-performance-tuning.html#coalescing-post-shuffle-partitions) using runtime statistics after wide operations such as joins and aggregations. Both settings default to `true` in Spark 3.5. This reduces the need to manually lower shuffle partition counts or add `coalesce()` after a shuffle; it does not automatically control every output file count.

Spark's default `spark.sql.shuffle.partitions` is `200`, although platforms can override it. With AQE, this supplies the initial shuffle partition count unless `spark.sql.adaptive.coalescePartitions.initialPartitionNum` is set. Start with the effective defaults and inspect task sizes and durations in the Spark UI before making changes. AQE needs a sufficiently large initial partition count to coalesce from; it does not make every initial count suitable.

##### Partition count configuration

If runtime evidence justifies a manual initial count, estimate it from the expected shuffle data volume, not simply the input file size. The following example divides an estimated 100 GB shuffle by an illustrative 0.5 GB target and rounds up. Neither this target nor the resulting count is a universal optimum or an AQE advisory-size setting; validate memory use, spill, and task duration for your workload.

````{tabs}
```{code-tab} py
from math import ceil
from pyspark.sql import SparkSession

shuffle_data_size_gb = 100
target_partition_size_gb = 0.5
initial_partitions = max(1, ceil(shuffle_data_size_gb / target_partition_size_gb))

spark = (
        SparkSession.builder
        .appName("partition-tuned-session")
        .config("spark.sql.shuffle.partitions", str(initial_partitions))
        .getOrCreate()
)
```

```{code-tab} r R
library(sparklyr)

shuffle_data_size_gb <- 100
target_partition_size_gb <- 0.5
initial_partitions <- max(1, ceiling(shuffle_data_size_gb / target_partition_size_gb))

config <- spark_config()
config$spark.sql.shuffle.partitions <- as.character(initial_partitions)

sc <- spark_connect(
    app_name = "partition-tuned-session",
    config = config
)
```
````

>See guidance on [Partitioning](../spark-concepts/partitions) and [Shuffling](../spark-concepts/shuffling) for more details.

Run code on full dataset(s). Need further optimisation? Grab a link to the Spark UI.

### Investigate issues

#### Troubleshooting workflow

When a default session struggles:
1. Check the Spark UI for skew, long stages, shuffle volume, failed executors, and storage growth.
2. Check the Spark properties shown in the Spark UI's `Environment` tab, or print the running session's configuration using [How to inspect the default session](../spark-overview/example-spark-sessions.html#example-spark-inspect-session). For SQL settings changed at runtime, also check the value through `spark.conf.get("setting.name")` in PySpark or `SET setting.name` in Spark SQL; these changes may not appear in the Environment tab.
3. Classify the issue: memory pressure, local disk pressure, skew, partition count, or Python overhead. Use the execution plan and stage/task evidence to identify the cause.
4. Prefer query or data-layout improvements first. If a configuration change is needed, change one setting at a time for the observed symptom.
5. Re-run a comparable workload and confirm the effect before keeping changes.

#### 5. Investigate wide operations in the Spark UI
- Set the [job description](../spark-concepts/partitions.html#run-the-jobs-and-investigate-ui) in the code to help identify actions that relate to jobs in the UI.
- Look for task skew and spill to disk.
- Do a group by and count of join keys or window keys to look for the skew in the data.

>See articles on [Spark Application and UI](../spark-concepts/spark-application-and-ui.html#spark-application-overview) and [Partitions](../spark-concepts/partitions.html#intermediate-partitions-in-wide-operations) for more details.

#### 6. How is Spark processing the data?
- Check the execution plan using [`.explain()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html)/[`explain()`](https://dplyr.tidyverse.org/reference/explain.html).
- Find the relevant SQL DAG in the UI.
- Complex plans can be simplified using persistence 

> See articles on [Caching](../spark-concepts/cache), [Persistence](../spark-concepts/persistence), and [Checkpointing and staging](../spark-concepts/checkpoint-staging) for the appropriate way to break or reuse lineage.

### Optimise

#### 7. Optimise joins
- Consider [replacing a join with narrow transformation](../spark-concepts/join-concepts.html#replacing-a-join-with-a-narrow-transformation)
- [Use a broadcast join](../spark-concepts/join-concepts.html#broadcast-join) when one side is small enough to fit safely in each executor's memory; inspect the plan first, as Spark may already choose a broadcast join.
- Consider a [salted join](../spark-concepts/salted-joins) if skew is a real issue. Alternatively, if there is just one over-represented key, use a separate join for this key and union later.

#### 8. Avoid UDFs in PySpark
- Use `spark.sql.functions` by default.
- Built-in functions preserve Catalyst optimisation and avoid Python row-by-row overhead.
- Use vectorised pandas/numpy UDFs when functionality is not available in `spark.sql.functions`.
- If you cannot avoid scalar UDFs at all consider increasing the proportion of off-head memory allocated to the executors.

#### 9. Caching
- Persist when a repeated calculations are made on a DataFrame.
- Use memory for less frequent caching; use disk for frequent persisting.
- Keep track of executor storage and empty when a cache is not needed.
- Warning: caching an object can force the entire object to be materialised, even if only parts of it are used in your code (e.g. if you filter a persisted object). This can increase run-time over not using persist.

> See article on [Persistence in Spark](../spark-concepts/persistence) for more details.

#### 10. Spark session parameters

Add extra settings only when they address an observed problem, rather than copying a session template. Review the [configuration hierarchy](../spark-overview/spark-defaults) and follow platform guidance for settings controlled by the cluster.

>[!TIP]
>When overriding a default, add a comment beside the setting explaining the observed problem, why the chosen value helps, and any platform-specific assumptions. Revisit the override when the workload or platform changes.


##### Ad hoc settings for special cases

| Symptom or use case | Settings to consider | Checks before changing |
|---|---|---|
| Many tiny shuffle tasks | AQE coalescing settings, then `spark.sql.shuffle.partitions` if needed | Check that AQE coalescing is active before manually lowering the initial count. |
| Large shuffle partitions with long-running tasks or spill | Increase `spark.sql.shuffle.partitions` where appropriate | Distinguish an insufficient initial count from skew using stage/task evidence. |
| Executors retained because shuffle data is still tracked | `spark.dynamicAllocation.shuffleTracking.timeout` | Applies when dynamic allocation and shuffle tracking are enabled. A finite timeout can cause shuffle recomputation; consult platform support rather than treating it as a general disk-pressure fix. |
| Unused shuffle files retained in long-running applications | `spark.cleaner.periodicGC.interval` | With platform support, check whether delayed garbage collection is preventing cleanup. This does not remove shuffle data still needed by the application. |
| S3A upload buffering puts pressure on local disk | `spark.hadoop.fs.s3a.fast.upload.buffer` | The `bytebuffer` option uses off-heap memory instead of disk. Check the Hadoop version, concurrent uploads, and memory capacity with platform support before changing it. |

Do not treat these as universal values or guaranteed fixes. Validate each change against the same workload and keep only changes with a demonstrated benefit.

- Explore some [Spark session parameters](../spark-overview/spark-session-guidance) to help tune the session to the particular job at hand.
- For troublesome joins, grouping or windows with a small number of larger keys try a smaller number of larger executors.
- For troublesome joins, grouping or windows with a large number of smaller keys try a larger number of smaller executors.

> See [Partitioning](../spark-concepts/partitions) for partition sizing and [Garbage collection](../spark-concepts/garbage-collection) for the executor-sizing trade-offs.

### Further Resources

Spark at the ONS Articles:
- [Example Spark Sessions](../spark-overview/example-spark-sessions)
    - [Default session](../spark-overview/example-spark-sessions.html#example-spark-default-session).
- [Sampling](../spark-functions/sampling)
- [Managing Partitions](../spark-concepts/partitions)
    - [Intermediate partitions in wide operations](../spark-concepts/partitions.html#intermediate-partitions-in-wide-operations)
- [Shuffling](../spark-concepts/shuffling)
- [Spark Application and UI](../spark-concepts/spark-application-and-ui)
    - [Spark Application Overview](../spark-concepts/spark-application-and-ui.html#spark-application-overview)
- [Caching](../spark-concepts/cache)
- [Optimising Joins](../spark-concepts/join-concepts)
    - [Replacing a join with a narrow transformation](../spark-concepts/join-concepts.html#replacing-a-join-with-a-narrow-transformation)
    - [Broadcast Join](../spark-concepts/join-concepts)
- [Salted Joins](../spark-concepts/salted-joins)
- [Persisting in Spark](../spark-concepts/persistence)
- [Checkpointing and staging](../spark-concepts/checkpoint-staging)
- [Groups, not loops](../spark-concepts/groups-not-loops)
- [Garbage collection](../spark-concepts/garbage-collection)
- [Guidance on Spark Sessions](../spark-overview/spark-session-guidance)

PySpark Documentation:
- [`.show()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.show.html)
- [`.count()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.count.html)
- [`.coalesce()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.coalesce.html)
- [`.explain()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html)

sparklyr and tidyverse Documentation:
- [`glimpse()`](https://pillar.r-lib.org/reference/glimpse.html)
- [`sdf_nrow()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_dim.html)
- [`sdf_coalesce()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_coalesce.html)
- [`explain()`](https://dplyr.tidyverse.org/reference/explain.html)

Spark Documentation:
- [Adaptive Query Execution (AQE)](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution) 
- [Coalescing post-shuffle partitions](https://spark.apache.org/docs/latest/sql-performance-tuning.html#coalescing-post-shuffle-partitions)
- [Spark configuration](https://spark.apache.org/docs/latest/configuration.html)
- [Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html)

Hadoop Documentation:
- [S3A upload buffering](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/performance.html#How_S3A_writes_data_to_S3)

Other Links:
- [The Government Data Quality Framework: Data quality dimensions](https://www.gov.uk/government/publications/the-government-data-quality-framework/the-government-data-quality-framework#Data-quality-dimensions)
- [Quality Assurance of Code for Analysis and Research](https://best-practice-and-impact.github.io/qa-of-code-guidance/intro.html): also known as *the Duck Book*

#### Acknowledgements

Many thanks to Andy Sutton and David Foster for contributing and reviewing this material.
