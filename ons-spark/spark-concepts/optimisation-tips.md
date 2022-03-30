# Ideas for optimising Spark code

This article was created through a combination of experience, knowledge, frustration and Spark training courses. It is not an exhaustive list of optimisation techniques. It is a summary list of ideas for an intermediate level developer/analyst to try when developing Spark applications. 

## Assumptions 
- You have spent time geting to know your data; both in terms of data quality and various distributions you will use during processing with Spark.
- You are aware of good coding practices e.g. recommendations made in [the Duck Book](https://best-practice-and-impact.github.io/qa-of-code-guidance/intro.html).

## Get the basics right

### 1. Start simple
- Use a [default Spark session](https://best-practice-and-impact.github.io/ons-spark/spark-overview/example-spark-sessions.html#default-blank-session).
- Consider taking a [sample of your data](https://best-practice-and-impact.github.io/ons-spark/spark-functions/sampling.html) while you're developing the code for faster processing.
- Structure the code logically; not too much code in a single script.
- Use [Adaptive Query Execution (AQE)](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution) if available (Spark 3.x).

### 2. Use the correct file formats
- Don't store intermediate data as text or csv files. When you need to read from these file types always specify a schema.
- Use parquet files or Hive tables stored as parquet.
- Make sure you don't have lots of small files on HDFS (bad for Hadoop and bad for Spark).

### 3. Get rid of actions that aren't needed
- Get rid of `.count()` and `.show()` put in during development; these make Spark reprocess the result.
- Is it possible to replace any remaining actions with some transformations?
- If there is a repeated action needed on a dataframe consider persisting (memory or disk).

### 4. Partitioning and shuffling
- If the amount of data you are processing is small for Spark, reduce the `spark.sql.shuffle.partitions` parameter.
- Use `coalesce()` for small DataFrames, especially before writing them to disk.
- Shuffles are expensive, remove unnecessary shuffles.

*See guidance on [Partitioning](https://best-practice-and-impact.github.io/ons-spark/spark-concepts/partitions.html) and [Shuffling](https://best-practice-and-impact.github.io/ons-spark/spark-concepts/shuffling.html) for more details.*

Run code on full dataset(s). Need further optimisation? Grab a link to the Spark UI.

## Investigate issues

### 5. Investigate wide operations in the Spark UI
- Use `spark.SparkContext.setJobGroup()` in the code to help identify actions in the UI
- Look for task skew and spill to disk.
- Do a `.groupBy().agg(F.count())` of join keys or window keys to look for the skew in the data.

*See articles on [Spark Application and UI](https://best-practice-and-impact.github.io/ons-spark/spark-concepts/spark_application_and_ui.html#spark-application-overview) and [Partitions](https://best-practice-and-impact.github.io/ons-spark/spark-concepts/partitions.html#intermediate-partitions-in-wide-operations) for more details.*

### 6. How is Spark processing the data?
- Check the execution plan using `explain()`.
- Find the relevant SQL DAG in the UI.
- Complex plans can be simplified using persistance 

*See article on [Cache](https://best-practice-and-impact.github.io/ons-spark/spark-concepts/cache.html) for more details*.

## Optimise

### 7. Optimise joins
- Consider [replacing a join with narrow transformation](https://best-practice-and-impact.github.io/ons-spark/spark-concepts/partitions.html#intermediate-partitions-in-wide-operations)
- [Use a broadcast join](https://best-practice-and-impact.github.io/ons-spark/spark-concepts/join_concepts.html#broadcast-join) when joining with small DataFrame.
- Consider [salted join](https://best-practice-and-impact.github.io/ons-spark/spark-concepts/salted_joins.html) if skew is a real issue. Alternatively, if there is just one over-represented key, use a separate join for this key and union later.

### 8. Avoid UDFs
- Use `spark.sql.functions` by default.
- Use vectorised pandas/numpy UDFs when functionality isn't available in `spark.sql.functions`.
- If you can't avoid scalar UDFs at all consider increasing the proportion of off-head memory allocated to the executors.

### 9. Caching
- Persist when a repeated calculation is done on a DataFrame.
- Use memory for less frequent caching; use disk for frequent persisting.
- Keep track of executor storage and empty when a cache isn't needed.
- Warning: caching an object can force the entire object to be materialised, even if only parts of it are used in your code (e.g. if you filter a persisted object). This can increase run-time over not using persist.

*See article on [Persistance in Spark](https://best-practice-and-impact.github.io/ons-spark/spark-concepts/persistence.html) for more details.*

### 10. Spark session parameters
- How big are the data? Use [suggested Spark sessions](https://best-practice-and-impact.github.io/ons-spark/spark-overview/example-spark-sessions.html).
- For troublesome joins, groupbys or windows with a small number of larger keys try a smaller number of larger executors.
- For troublesome joins, groupbys or windows with a large number of smaller keys try a larger number of smaller executors.

*See section on [Intermediate partitions in wide operations](https://best-practice-and-impact.github.io/ons-spark/spark-concepts/partitions.html#intermediate-partitions-in-wide-operations) for more details.*
