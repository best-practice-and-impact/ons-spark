TODO:

Currently:
- Article is about caching as opposed to persisting in general
- Show how to view memory usage due to cache
- Show some analysis where caching is appropriate
- Touch on execution plan and catalyst optimiser
- Summary and tips on when to cache and when not to cache

Proposed structure:
- Article is about persisting
- Mention execution plan and catalyst optimiser (link to dedicated article?)
- Discussion on persisting strategy
- Link to caching article
- Link to dedicated article on checkpointing/staging tables



- persisting strategy and mention execution plan and cat optimiser
- checkpointing and staging tables
- caching


# Persisting in Spark

Persisting Spark DataFrames is done to improve efficiency. There are two different cases where persisting can be useful:

1. Lineage of the DataFrame is long and complex
2. Stop unnecessary repetitions of processes

In this article we discuss the different methods of persisting data with Spark, their advantages and disadvantages.

## Lineage, execution plan and the catalyst optimiser

Before we discuss persistance we should discuss lineage, the execution plan and the catalyst optimiser.

We know that Spark uses lazy evaluation, meaning it doesn't process data until it has to e.g. infering the schema of a file, a row count, returning some data using `.show()`/`head() %>% collect()` or writing data to disk. As we execute DataFrame transformations Spark tracks the lineage of the DataFrame and creates an execution plan. When we execute an action Spark executes the plan. This is quite different to how regular Python, pandas or R works. Why does Spark work in this way?  Because Spark will find more efficient ways to process the data than just following our commands one after the other, and when it comes to processing big data, this is really useful. 

The way Spark optimises our jobs is using the [catalyst optimiser](https://databricks.com/glossary/catalyst-optimizer) and the [tungsten optimiser](https://databricks.com/glossary/tungsten). The former uses a list of rules to change our code into a more efficient strategy, whereas the latter performs optimisations on the hardware. If we want to see how Spark will or has processed a DataFrame we can look at the execution plan. For a quick view you can apply the `.explain()`/`explain` functions to DataFrames in PySpark/sparklyr. The `full` argument can be set to true to see how the catalyst optimiser has taken the original Spark code and optimised it to reach the final version, called the Physical Plan. Examples are given in the **Joins** article on how to read the output of the `explain` function. The execution plan is also given in the form of a DAG diagram within the SQL tab in the Spark UI. Examples are given in the **Cache** article on understanding these diagrams.

## Breaking DataFrame lineage

As we apply more transformations to our DataFrames the lineage grows and so does the execution plan. If the lineage is long and complex Spark will struggle to optimise the plan and take a long time to process the DataFrame. 

Let's say we are executing an interative algorithm on a DataFrame, for example we apply some calculation to a column to create a new column and use this new column as the input to the calculation in the next iteration. We notice that Spark struggles to execute the code after 10 iterations. What we can try to solve this issue is every 5 iterations we write the DataFrame out to disk and read it back in for the next iteration. Writing the data out to disk is a form of [persistance](https://en.wikipedia.org/wiki/Persistence_(computer_science)), and so this is an example where persistance is used in Spark to break the lineage of a DataFrame.

An example of persistance in an interative process is given in the article on checkpoints and staging tables.

There are other forms of persistance, we'll use a different example below.

## Remove repeated processing

The default behaviour of Spark when it encounters an action is to execute the full execution plan from when the data was read in from disk to the output of the action which was called. There are some optimisations provided by the tungsten optimiser that diverge from this default behaviour, but we will ignore these for now.

Now imagine we read in a dataset and perform some cleaning transformations to get the data ready for some analysis. We then perform three different aggregations on the cleansed data and write these summary tables to disk. If we consider how Spark executes the full execution plan when it gets to an action, we see that Spark will repeat the cleaning transformations for each summary table. Surely it would be more efficient to clean the data once and not three times (one for each aggregation)? This is another case where persisting can help. A possible solution here would be to persist the cleansed data into memory using a cache before performing the aggregations. This means that the first tiome Spark executes the full plan it will create a copy of the cleansed data in the executor memory, so next time the DataFrame is used it can be accessed quickly and efficiently.

An example of using cache to remove repeated processing is given in the cache article.

## Different types of persistance


- On disk
- In memory

- Checkpoint/staging tables
- Cache (persist)


## Cache gone wrong

## Persisting strategy
# Summary

- Spark tracks the lineage (history) of a DataFrame from its origin (often when it's read in from disk)
- Spark will change the order of the execution plan without changing the result. 
    - It does this because it's more efficient to do it Spark's way, e.g. move a filter as early as possible so we're processing less data in the rest of the query. This is done by the **catalyst optimiser**.
- Spark will execute the **whole plan** as opposed to executing the new operations, why? 
    - To allow the catalyst optimiser to work out the best way of executing the job.
- We can get around executing the full plan by **persisting** the data at the appropriate place.
    - Beware, the optimiser is there for a reason and `.cache()` restricts the reach of the optimiser. 
- Caching can increase efficiency by reducing the runtime if used *properly*
    - databricks claim that over 70% or their issue tickets are related to poor use of `.cache()`
    
## When to `.cache`?

- it makes sense to use `.cache()` in the example above because we want to create three new tables from a cleaned data set. However, the processing time is only consistently faster the second time code is run after a `.cache()`. These results will vary depending on the size of the data, type of processing etc..
- a good example is just before running an iterative algorithm, where we make use of the cached DataFrame many times.
- another example is running a model on a DataFrame multiple times trying out different parameters. 
- also, when a DataFrame's lineage is very long, breaking up the lineage by persisting can increase efficiency. There is an alternative that might be more appropriate here, which is `.checkpoint()`, especially when adding 10s or 100s of columns. See the exercises for an example.
- whilst developing your code. For example, perhaps you have just linked two data sets and want to do some checks to make sure the result makes sense. Perhaps the checks would involve running multiple actions (e.g. `.show()`, `.count()`, `.describe()`), but without doing a `.cache()` after the `.join()`, Spark would repeat the join process for each action. Once you're happy with the result you might want to empty the cache and replace `.cache()` with writing the DataFrame to HDFS. 

## When not to `.cache`?

- in an arbitrary manner
- just because someone said it makes your code run faster
- beware when you `.cache()` a large DataFrame without setting the `spark.dynamicAllocation.maxExecutors` config, especially on a small cluster. Spark will assign as many executors as needed to fit the DataFrame in memory, which might be more resource than you intended on using.
- do not over-use `.cache()` in one Spark session. It is worth checking the *Storage* tab in the Spark UI occasionally to make sure there aren't any unwanted caches lying around. Use `spark.catalog.clearCache()` to clear the cache.

Experiment! (...with caution) Try it out and have a look at the SQL DAGs to see what works well for your processing.