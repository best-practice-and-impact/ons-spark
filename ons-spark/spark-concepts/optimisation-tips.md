## Ideas for optimising Spark code

This article was created through a combination of experience, knowledge, frustration and Spark training courses. It is not an exhaustive list of optimisation techniques. It is a summary list of ideas for an intermediate level Spark engineer, analyst or data scientist to try when developing Spark applications. 

### Assumptions 
- You have spent time getting to know your data, e.g. exploring [data quality dimensions](https://www.gov.uk/government/publications/the-government-data-quality-framework/the-government-data-quality-framework#Data-quality-dimensions), distributions within key variables, size of dataset on disk/memory etc.
- You are aware of good coding practices e.g. recommendations made in the [Quality Assurance of Code for Analysis and Research](https://best-practice-and-impact.github.io/qa-of-code-guidance/intro.html) book (also known as *the Duck Book*).

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
- AQE can combine small partitions after a shuffle, reducing the need for manual changes. See [Automatic partition sizing with AQE](../spark-concepts/partitions.html#automatic-partition-sizing-with-aqe) and [Partition count configuration](../spark-concepts/partitions.html#partition-count-configuration) for details and examples.

> [!TIP]
> See guidance on [Partitioning](../spark-concepts/partitions) and [Shuffling](../spark-concepts/shuffling) for more details.

Run code on full dataset(s). Need further optimisation? Grab a link to the Spark UI.

### Investigate issues

#### 5. Investigate and characterise the issue
- Check the Spark UI for uneven task sizes (skew), data spilling from memory to disk, long stages, large shuffles, failed executors, and growing storage use. Use the task details to check whether the problem is related to memory, disk space, partition count, or time spent running Python code.
- Check the Spark properties shown in the Spark UI's `Environment` tab, or use [How to inspect the default session](../spark-overview/example-spark-sessions.html#example-spark-inspect-session). For SQL settings changed while the session is running, use `spark.conf.get("setting.name")` in PySpark or `SET setting.name` in Spark SQL; these changes may not appear in the Environment tab.
- Set the [job description](../spark-concepts/partitions.html#run-the-jobs-and-investigate-ui) in the code to help identify actions that relate to jobs in the UI.
- For skew, compare task sizes in the Spark UI or group by the join or window keys and count the rows for each key.
- Try improving the query or how the data is stored before changing session settings. If a setting needs changing, change one at a time and re-run the same workload to check whether it helps.

> [!TIP]
> See articles on [Spark Application and UI](../spark-concepts/spark-application-and-ui.html#spark-application-overview) and [Partitions](../spark-concepts/partitions.html#intermediate-partitions-in-wide-operations) for more details.

#### 6. How is Spark processing the data?
- Check the execution plan using [`.explain()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html)/[`explain()`](https://dplyr.tidyverse.org/reference/explain.html).
- Find the relevant SQL DAG in the UI.
- Complex plans can be simplified using persistence 

> [!TIP]
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

> [!TIP]
> See article on [Persistence in Spark](../spark-concepts/persistence) for more details.

#### 10. Spark session parameters

Add extra settings only when they address an observed problem, rather than copying a session template. Review the [configuration hierarchy](../spark-overview/spark-defaults) and follow platform guidance for settings controlled by the cluster.

>[!NOTE]
>When overriding a default, add a comment beside the setting explaining the observed problem, why the chosen value helps, and any platform-specific assumptions. Revisit the override when the workload or platform changes.

##### Workload size guide

Row count is only a rough guide, not a rule for choosing resources. The number of columns, joins, shuffles, window functions, Python UDFs, and uneven group sizes can make even smaller datasets slow to process. Start with the default session and check how it handles your work before changing settings.

| Workload band | Illustrative row count | Typical characteristics | Guidance |
|---|---:|---|---|
| Small | Up to ~1 million rows | Narrow tables, filters, simple aggregations, light joins | Start with the default session. Use local mode only for sampled or dummy data. |
| Medium | ~1 million to ~50 million rows | Repeated transformations, moderate joins, standard production pipelines | Stay on the default session first. Inspect the Spark UI before changing settings. |
| Large | ~50 million to ~200 million rows | Heavy shuffles, wide aggregations, complex joins, window functions | Default session may still work, but check partitioning, uneven group sizes, and storage use early. |
| Extra-large | 200 million+ rows, or multiple very large inputs | Very uneven group sizes, repeated shuffle stages, large intermediate outputs | Start with defaults, then change settings only for problems you have observed. |

##### Ad hoc settings for special cases

| Symptom or use case | Settings to consider | Checks before changing |
|---|---|---|
| Many tiny shuffle tasks | AQE coalescing settings, then `spark.sql.shuffle.partitions` if needed | Check that AQE coalescing is active before manually lowering the initial count. |
| Large shuffle partitions with long-running tasks or spill | Increase `spark.sql.shuffle.partitions` where appropriate | Distinguish an insufficient initial count from skew using stage/task evidence. |
| Executors retained because shuffle data is still tracked | `spark.dynamicAllocation.shuffleTracking.timeout` | Applies when dynamic allocation and shuffle tracking are enabled. A finite timeout can cause shuffle recomputation; consult platform support rather than treating it as a general disk-pressure fix. |
| Unused shuffle files retained in long-running applications | `spark.cleaner.periodicGC.interval` | With platform support, check whether delayed garbage collection is preventing cleanup. This does not remove shuffle data still needed by the application. |
| S3A upload buffering puts pressure on local disk | `spark.hadoop.fs.s3a.fast.upload.buffer` | The `bytebuffer` option uses off-heap memory instead of disk. Check the Hadoop version, concurrent uploads, and memory capacity with platform support before changing it. |

Do not treat these as universal values or guaranteed fixes. Validate each change against the same workload and keep only changes with a demonstrated benefit.

>[!TIP]
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
