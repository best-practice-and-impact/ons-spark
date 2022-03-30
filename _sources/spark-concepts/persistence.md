<!-- #region -->
## Persisting in Spark

Persisting Spark DataFrames is done for a number of reasons, a common reason is creating intermediate outputs in a pipeline for quality assurance purposes. In this article we are mainly interested in using [persistence](https://en.wikipedia.org/wiki/Persistence_(computer_science)) to improve performance, i.e. reduce processing time. There are two different cases where persisting DataFrames can be useful in this context:

1. To remove unnecessary repetitions of the same processing
2. If the DataFrame lineage is long and complex

In this article we discuss why persisting is useful with Spark, introduce the different methods of persisting data and discuss their use cases. Examples are given in the [Caching](../spark-concepts/cache) and [Checkpoint and Staging Tables](../raw-notebooks/checkpoint-staging/checkpoint-staging) articles.

### Theory: lineage, execution plan and the catalyst optimiser

Before we discuss persistence we should discuss lineage, the execution plan and the catalyst optimiser.

We know that Spark uses lazy evaluation, meaning it doesn't process data until it has to e.g. inferring the schema of a file, a row count, returning some data using [`.show()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.show.html)/`head() %>%` [`collect()`](https://dplyr.tidyverse.org/reference/compute.html) or writing data to disk. As we execute DataFrame transformations Spark tracks the lineage of the DataFrame and creates an execution plan. When we execute an action Spark executes the plan. This is quite different to how regular Python, pandas or R works. Why does Spark work in this way? One reason is that Spark wont have to store intermediate objects in memory, which makes it more efficient for processing big data. Another reason is that Spark will find more efficient ways to process the data than just following our commands one after the other, and again, this is really useful when processing big data. 

The way Spark optimises our jobs is using the [catalyst optimiser](https://databricks.com/glossary/catalyst-optimizer) and the [tungsten optimiser](https://databricks.com/glossary/tungsten). The former uses a list of rules to change our code into a more efficient strategy, whereas the latter performs optimisations on the hardware. If we want to see how Spark will or has processed a DataFrame we can look at the execution plan. For a quick view you can apply the [`.explain()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.explain.html)/[`explain()`](https://dplyr.tidyverse.org/reference/explain.html) functions to DataFrames in PySpark/sparklyr. The `full` argument can be set to `true` to see how the catalyst optimiser has taken the original Spark code and optimised it to reach the final version, called the Physical Plan. Examples are given in the [Checkpoint and Staging Tables](../raw-notebooks/checkpoint-staging/checkpoint-staging) article on how to read the output of the `explain` function. The execution plan is also given in the form of a DAG diagram within the SQL tab in the Spark UI. Examples are given in the [Caching](../spark-concepts/cache) article on understanding these diagrams.

### Remove repeated processing

The default behaviour of Spark when it encounters an action is to execute the full execution plan from when the data was read in from disk to the output of the action which was called. There are some optimisations provided by the tungsten optimiser that diverge from this default behaviour, but we will ignore these for now.

Now imagine we read in a dataset and perform some cleaning transformations to get the data ready for some analysis. We then perform three different aggregations on the cleansed data and write these summary tables to disk. If we consider how Spark executes the full execution plan when it gets to an action, we see that Spark will repeat the cleaning transformations for each summary table. Surely it would be more efficient to clean the data once and not three times (one for each aggregation)? This where persisting can help. A possible solution here would be to persist the cleansed data into memory using a cache before performing the aggregations. This means that the first time Spark executes the full plan it will create a copy of the cleansed data in the executor memory, so next time the DataFrame is used it can be accessed quickly and efficiently.

An example of using cache to remove repeated processing is given in the [Caching](../spark-concepts/cache) article.

### Breaking DataFrame lineage

As we apply more transformations to our DataFrames the lineage grows and so does the execution plan. If the lineage is long and complex Spark will struggle to optimise the plan and take a long time to process the DataFrame. Hence, in a data pipeline we might write intermediate tables at sensible points that can be used for quality assurance purposes, but also this process breaks the DataFrame lineage.

Let's say we are executing an iterative algorithm on a DataFrame, for example we apply some calculation to a column to create a new column and use this new column as the input to the calculation in the next iteration. We notice that Spark struggles to execute the code after many iterations. What we can try to solve this issue is every few iterations we write the DataFrame out to disk and read it back in for the next iteration. Writing the data out to disk is a form of persistence, and so this is an example where persistence is used in Spark to break the lineage of a DataFrame.

An example of persistence in an iterative process is given in the article on [Checkpoint and Staging Tables](../raw-notebooks/checkpoint-staging/checkpoint-staging).


### Different types of persisting

There are multiple ways of persisting data with Spark, they are:

- Caching a DataFrame into the executor memory using [`.cache()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.cache.html)/[`tbl_cache()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/tbl_cache.html) for PySpark/sparklyr. This forces Spark to compute the DataFrame and store it in the memory of the executors.
- Persisting using the [`.persist()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.persist.html)/[`sdf_persist()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_persist.html) functions in PySpark/sparklyr. This is similar to the above but has more options for storing data in the executor memory or disk.
- Writing a DataFrame to disk as a parquet file and reading the file back in. Again, this forces Spark to compute the DataFrame but makes use of the file system instead of the executor memory/disk.
- Checkpointing is essentially a convenient shortcut to a write/read process to the file system.
- Staging tables is another form of a write/read process using a Hive table.

Please see the Cache article for a more detailed explanation of the first two options above. Examples of checkpointing and staging tables are given in the [Checkpoint and Staging Tables](../raw-notebooks/checkpoint-staging/checkpoint-staging).

### Why should we not persist?

Persisting data in Spark is no *silver bullet*. Some data platform support teams say that over half of their issue tickets are to do with poor use of caching. Make sure you understand the pros *and cons* before you use it to improve performance.

#### Predicate pushdown

As explained above, Spark uses the catalyst optimiser to improve performance by optimising the whole execution plan. However, when we persist the data the plan is made shorter. This is sometimes a good thing if the plan is too complex, but in general we want the catalyst optimiser to take all our transformations to find the optimum execution plan. For example, in the Cache article we saw that the catalyst optimiser will move a filter as early as possible in the execution plan so Spark has to process fewer rows of data later on; this is called a predicate pushdown. If we cached at the wrong point in the code, the optimiser would not be able to push the filter to the beginning of the execution plan.

#### Writing and reading is not free

The process of writing and reading data for checkpoints and staging tables, as well as general write/read operations, takes some amount of time. Perhaps the time it takes to do this process is greater than the gains from breaking the lineage. As we often say, it makes sense to get the code working first and try to improve performance where needed. When experimenting on improving performance record the evidence used to inform decisions so that others can clearly understand why a decision to persist was made.

#### Filling up cache/file system with persisted data

There is a limited amount of memory on the executors of the Spark cluster and if you fill it up with cached data Spark will start to spill data onto disk or return an out of memory error. Spark will prioritise recent caches and run more expensive processes to manage the low priority data. Remember to empty the cache when you've stopped using a DataFrame and don't cache too many DataFrames at one time. Memory management in a Spark cluster is a complex topic, a good introduction is given in the [Spark documentation](https://spark.apache.org/docs/latest/tuning.html#memory-management-overview).

The same is true for the file system, although there is much more space to store data of course. It's good practice to delete a checkpoint directory after use within a Spark script. The same is true for staging tables, unless you want to view the data at a later date.


### Use cases for persisting

- It usually makes sense to cache data in memory before repeated actions are applied to the DataFrame, like the example in the [Caching](../spark-concepts/cache) article. 
- Another use case for caching in memory is just before running an iterative algorithm, where you make use of the cached DataFrame in each iteration.
- Also, when running a model on a DataFrame multiple times whilst trying out different inputs or parameters. 
- As shown in the [checkpoint](../raw-notebooks/checkpoint-staging/checkpoint-staging) article, when a DataFrame's lineage gets long or complex breaking up the lineage by persisting can improve performance.
- Caching is also a great tool whilst developing your code. For example, perhaps you have just linked two data sets and want to do some checks to make sure the result makes sense. Perhaps the checks would involve running multiple actions e.g. `.show()`, [`.count()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.count.html), [`.describe()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.describe.html), but without caching after the join Spark would repeat the join process for each action. Once you're happy with the result you might want to empty the cache and possibly even replace the cache with a staging table so that users can quality assure the result of the join after each run of the pipeline. 

### Checkpoint or staging tables?

**Checkpoint**

- Checkpointing is more convenient than staging tables in terms of writing the code
- Overall it can be more effort to maintain staging tables, especially if using the option to insert into a table, as you will have to alter the table if the DataFrame structure changes

**Staging tables**

- The same table can be overwritten, meaning there is no need to clean up old checkpointed directories
- It is stored in a location that is easier to access, rather than the checkpointing folder, which can help with debugging and testing changes to the code
- They can be re-used elsewhere, whereas checkpoint files are given arbitrary names
- If inserting into an existing table, you can take advantage of the table schema, as an exception will be raised if the DataFrame and table schemas do not match
- It is more efficient for Spark to read Hive tables than text/CSV files as the underlying format is Parquet, so if your data are delivered as text/CSV files you may want to stage them as Hive tables first.

### Further Resources

Spark at the ONS Articles:
- [Caching](../spark-concepts/cache)
- [Checkpoint and Staging Tables](../raw-notebooks/checkpoint-staging/checkpoint-staging)

PySpark Documentation:
- [`.count()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.count.html)
- [`.show()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.show.html)
- [`.explain()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.explain.html)
- [`.cache()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.cache.html)
- [`.persist()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.persist.html)
- [`.describe()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.describe.html)

sparklyr and tidyverse Documentation:
- [`collect()`](https://dplyr.tidyverse.org/reference/compute.html)
- [`explain()`](https://dplyr.tidyverse.org/reference/explain.html)
- [`tbl_cache()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/tbl_cache.html)
- [`sdf_persist()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/sdf_persist.html)

Spark Documentation:
- [Memory Management Overview](https://spark.apache.org/docs/latest/tuning.html#memory-management-overview)
<!-- #endregion -->
