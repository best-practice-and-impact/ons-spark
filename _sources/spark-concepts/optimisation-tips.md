## Ideas for optimising Spark code

This article was created through a combination of experience, knowledge, frustration and Spark training courses. It is not an exhaustive list of optimisation techniques. It is a summary list of ideas for an intermediate level Spark engineer, analyst or data scientist to try when developing Spark applications. 

### Assumptions 
- You have spent time getting to know your data, e.g. exploring [data quality dimensions](https://www.gov.uk/government/publications/the-government-data-quality-framework/the-government-data-quality-framework#Data-quality-dimensions), distributions within key variables, size of dataset on disk/memory etc.
- You are aware of good coding practices e.g. recommendations made in the [Quality Assurance of Code for Analysis and Research](https://best-practice-and-impact.github.io/qa-of-code-guidance/intro.html) book (also known as *the Duck Book*).

### Get the basics right

#### 1. Start simple
- Use a [default Spark session](../spark-overview/example-spark-sessions.html#default-blank-session). Do not copy Spark sessions from other projects.
- Consider taking a [sample of your data](../spark-functions/sampling) while you are developing the code for faster processing.
- Structure the code logically; not too much code in a single script.
- Use [Adaptive Query Execution (AQE)](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution) if available (Spark 3.x).

#### 2. Use the correct file formats
- Do not store intermediate data as text or CSV files. These should only be used when outputs need to be human readable. When you need to read from these file types always specify a schema.
- Use parquet files or Hive tables stored as parquet.
- Make sure you do not have lots of small files on HDFS (bad for Hadoop and bad for Spark).

#### 3. Get rid of actions that are not needed
- Get rid of intermediate actions put in during development (e.g. [`.show()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.show.html), [`glimpse()`](https://pillar.r-lib.org/reference/glimpse.html), [`.count()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.count.html), [`sdf_nrow()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_dim.html)); these make Spark reprocess the result.
- Is it possible to replace any remaining actions with some transformations?
- If there is a repeated action needed on a DataFrame consider persisting (memory or disk).

#### 4. Partitioning and shuffling
- If the amount of data you are processing is small for Spark, reduce the `spark.sql.shuffle.partitions` parameter.
- Use [`.coalesce()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.coalesce.html)/[`sdf_coalesce()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_coalesce.html) for small DataFrames, especially before writing them to disk.
- Shuffles are expensive, remove unnecessary shuffles.

*See guidance on [Partitioning](../spark-concepts/partitions) and [Shuffling](../spark-concepts/shuffling) for more details.*

Run code on full dataset(s). Need further optimisation? Grab a link to the Spark UI.

### Investigate issues

#### 5. Investigate wide operations in the Spark UI
- Set the [job description](../spark-concepts/partitions.html#run-the-jobs-and-investigate-ui) in the code to help identify actions that relate to jobs in the UI.
- Look for task skew and spill to disk.
- Do a group by and count of join keys or window keys to look for the skew in the data.

*See articles on [Spark Application and UI](../spark-concepts/spark-application-and-ui.html#spark-application-overview) and [Partitions](../spark-concepts/partitions.html#intermediate-partitions-in-wide-operations) for more details.*

#### 6. How is Spark processing the data?
- Check the execution plan using [`.explain()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.explain.html)/[`explain()`](https://dplyr.tidyverse.org/reference/explain.html).
- Find the relevant SQL DAG in the UI.
- Complex plans can be simplified using persistence 

*See article on [Cache](../spark-concepts/cache) for more details*.

### Optimise

#### 7. Optimise joins
- Consider [replacing a join with narrow transformation](../spark-concepts/join-concepts.html#replacing-a-join-with-a-narrow-transformation)
- [Use a broadcast join](../spark-concepts/join-concepts.html#broadcast-join) when joining with small DataFrame.
- Consider a [salted join](../spark-concepts/salted-joins) if skew is a real issue. Alternatively, if there is just one over-represented key, use a separate join for this key and union later.

#### 8. Avoid UDFs in PySpark
- Use `spark.sql.functions` by default.
- Use vectorised pandas/numpy UDFs when functionality is not available in `spark.sql.functions`.
- If you cannot avoid scalar UDFs at all consider increasing the proportion of off-head memory allocated to the executors.

#### 9. Caching
- Persist when a repeated calculations are made on a DataFrame.
- Use memory for less frequent caching; use disk for frequent persisting.
- Keep track of executor storage and empty when a cache is not needed.
- Warning: caching an object can force the entire object to be materialised, even if only parts of it are used in your code (e.g. if you filter a persisted object). This can increase run-time over not using persist.

*See article on [Persistence in Spark](../spark-concepts/persistence) for more details.*

#### 10. Spark session parameters
- Explore some [Spark session parameters](../spark-overview/spark-session-guidance) to help tune the session to the particular job at hand.
- For troublesome joins, grouping or windows with a small number of larger keys try a smaller number of larger executors.
- For troublesome joins, grouping or windows with a large number of smaller keys try a larger number of smaller executors.

*See section on [Intermediate partitions in wide operations](../spark-concepts/partitions.html#intermediate-partitions-in-wide-operations) for more details.*

### Further Resources

Spark at the ONS Articles:
- [Example Spark Sessions](../spark-overview/example-spark-sessions)
    - [Default/Blank Session](../spark-overview/example-spark-sessions.html#default-blank-session).
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
- [Guidance on Spark Sessions](../spark-overview/spark-session-guidance)

PySpark Documentation:
- [`.show()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.show.html)
- [`.count()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.count.html)
- [`.coalesce()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.coalesce.html)
- [`.explain()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.explain.html)

sparklyr and tidyverse Documentation:
- [`glimpse()`](https://pillar.r-lib.org/reference/glimpse.html)
- [`sdf_nrow()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_dim.html)
- [`sdf_coalesce()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_coalesce.html)
- [`explain()`](https://dplyr.tidyverse.org/reference/explain.html)

Spark Documentation:
- [Adaptive Query Execution (AQE)](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution) 

Other Links:
- [The Government Data Quality Framework: Data quality dimensions](https://www.gov.uk/government/publications/the-government-data-quality-framework/the-government-data-quality-framework#Data-quality-dimensions)
- [Quality Assurance of Code for Analysis and Research](https://best-practice-and-impact.github.io/qa-of-code-guidance/intro.html): also known as *the Duck Book*
