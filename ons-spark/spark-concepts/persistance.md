<!-- #region -->
# Persisting in Spark

Persisting Spark DataFrames is done to improve efficiency. There are two different cases where persisting can be useful:

1. Lineage of the DataFrame is long and complex
2. Stop unnecessary repetitions of processes

In this article we discuss the different methods of persisting data with Spark, their advantages and disadvantages.

## Lineage, execution plan and the catalyst optimiser

Before we discuss persistance we should discuss lineage, the execution plan and the catalyst optimiser.

We know that Spark uses lazy evaluation, meaning it doesn't process data until it has to e.g. infering the schema of a file, a row count, returning some data using `.show()`/`head() %>% collect()` or writing data to disk. As we execute DataFrame transformations Spark tracks the lineage of the DataFrame and creates an execution plan. When we execute an action Spark executes the plan. This is quite different to how regular Python, pandas or R works. Why does Spark work in this way?  Because Spark will find more efficient ways to process the data than just following our commands one after the other, and when it comes to processing big data, this is really useful. 

The way Spark optimises our jobs is using the [catalyst optimiser](https://databricks.com/glossary/catalyst-optimizer) and the [tungsten optimiser](https://databricks.com/glossary/tungsten). The former uses a list of rules to change our code into a more efficient strategy, whereas the latter performs optimisations on the hardware. If we want to see how Spark will or has processed a DataFrame we can look at the execution plan. For a quick view you can apply the `.explain()`/`explain` functions to DataFrames in PySpark/sparklyr. The `full` argument can be set to true to see how the catalyst optimiser has taken the original Spark code and optimised it to reach the final version, called the Physical Plan. Examples are given in the **Checkpoint and Staging table** article on how to read the output of the `explain` function. The execution plan is also given in the form of a DAG diagram within the SQL tab in the Spark UI. Examples are given in the **Cache** article on understanding these diagrams.

## Breaking DataFrame lineage

As we apply more transformations to our DataFrames the lineage grows and so does the execution plan. If the lineage is long and complex Spark will struggle to optimise the plan and take a long time to process the DataFrame. 

Let's say we are executing an interative algorithm on a DataFrame, for example we apply some calculation to a column to create a new column and use this new column as the input to the calculation in the next iteration. We notice that Spark struggles to execute the code after 10 iterations. What we can try to solve this issue is every few iterations we write the DataFrame out to disk and read it back in for the next iteration. Writing the data out to disk is a form of [persistance](https://en.wikipedia.org/wiki/Persistence_(computer_science)), and so this is an example where persistance is used in Spark to break the lineage of a DataFrame.

An example of persistance in an interative process is given in the article on checkpoints and staging tables.

There are other forms of persistance, we'll introduce a different type in an example below.

## Remove repeated processing

The default behaviour of Spark when it encounters an action is to execute the full execution plan from when the data was read in from disk to the output of the action which was called. There are some optimisations provided by the tungsten optimiser that diverge from this default behaviour, but we will ignore these for now.

Now imagine we read in a dataset and perform some cleaning transformations to get the data ready for some analysis. We then perform three different aggregations on the cleansed data and write these summary tables to disk. If we consider how Spark executes the full execution plan when it gets to an action, we see that Spark will repeat the cleaning transformations for each summary table. Surely it would be more efficient to clean the data once and not three times (one for each aggregation)? This is another case where persisting can help. A possible solution here would be to persist the cleansed data into memory using a cache before performing the aggregations. This means that the first time Spark executes the full plan it will create a copy of the cleansed data in the executor memory, so next time the DataFrame is used it can be accessed quickly and efficiently.

An example of using cache to remove repeated processing is given in the cache article.

## Why should we not persist?

Persisting data in Spark is no *silver bullet*. Some data platform support teams say that over half of their issue tickets are to do with poor use of caching. Make sure you understand the pros *and cons* before you use it to improve performance.

### Predicate pushdown

As explained above, Spark uses the catalyst optimiser to improve performance by optimising the whole execution plan. However, when we persist the data the plan is made shorter. This is sometimes a good thing if the plan is too complex, but in general we want the catalyst optimiser to take all our transformations to find the optimum execution plan. For example, in the Cache article we saw that the catalyst optimiser will move a filter as early as possible in the execution plan so Spark has to process fewer rows of data later on, this is called a predicate pushdown. If we cached at the wrong point in the code, the optimiser would not be able to push the filter to the beginning of the execution plan.

### Writing and reading can be expensive

The process of writing and reading data for checkpoints and staging tables takes some amount of time. Perhaps the time it takes to do this process is greater than the gains from breaking the lineage. As we often say, it makes sense to get the code working first and try to improve performance where needed.

### Filling up cache/file system with persisted data

There is a limited amount of memory on the executors of the Spark cluster and if you fill it up with cached data Spark will start to spill data onto disk or return an out of memory error. Remember to empty the cache when you've stopped using a DataFrame and don't cache too many DataFrames at one time. Memory management in a Spark cluster is a complex topic, a good introduction is given in [this blog](https://0x0fff.com/spark-memory-management).

The same is true for the file system, although there is much more space to store data of course. It's good practice to delete a checkpoint directory after use within a Spark script. The same is true for staging tables, unless you want to view the data at a later date.


## Summary of persistance in Spark

<!-- #endregion -->
