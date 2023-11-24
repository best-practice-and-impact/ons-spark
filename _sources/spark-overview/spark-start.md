## Getting Started with Spark

### What is Spark?

Spark is a distributed computing engine for processing big data. Spark has interfaces for Python (PySpark) and R (sparklyr), examples of which are given throughout this book. 

Here is how Spark is defined on the Apache Spark website and Wikipedia:

> Apache Spark™ is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters.
>
> *[Apache Spark](https://spark.apache.org/) (2022)*

> Apache Spark is an open-source unified analytics engine for large-scale data processing. Spark provides an interface for programming clusters with implicit data parallelism and fault tolerance.
>
> *[Wikipedia (Apache Spark)](https://en.wikipedia.org/wiki/Apache_Spark) (2022)*

### Basic principles

Spark is efficient for processing big data for a number of reasons. One reason is that it chunks up data to be [processed in parallel](https://en.wikipedia.org/wiki/Parallel_computing) (where possible). Another reason is it has some built in optimisations that make processing large amounts of data much quicker and using fewer resources.

As analysts we do not necessarily need to know exactly how Spark processes data, but when something goes wrong it is useful to understand a little more about what Spark is doing "under the hood". Debugging Spark issues often requires some basic understanding of how Spark works, and we hope to cover these details in this book without getting too technical. In this section there is enough to get started. There are links below to other parts of the book that cover these topics in more detail.

#### Spark DataFrames

If you look online or in text books you might read that the basic data structure in Spark is Resilient Distributed Dataset (RDD), but these are combined with [data types](data-types) to create **DataFrames** meaning we can often ignore the low-level RDD. If you have used Python (specifically pandas) or R before you should be familiar with DataFrames, but there are some key differences between DataFrames in Python/R and Spark, here are the important ones: 

- Spark DataFrames are chunked up into **partitions**. This means that Spark can process data in parallel; sometimes distributed over many nodes within the cluster.
- Although Spark DataFrames are processed in memory, they do not exist in memory by default. If we create a DataFrame in Python/R the data will be **persisted** in memory as we run the code. This is not the case in Spark where operations are classed as either **transformations** or **actions**. An **execution plan** is created as we run transformations and this plan is executed when we run an action. This is called **lazy evaluation** and allows Spark to apply optimisations to our code for more efficient processing. 
- Spark DataFrames are not ordered and do not have an index. The reason for this is that Spark DataFrames are distributed across partitions. Some operations, such as joining, grouping or sorting, cause data to be moved between partitions, known as a **shuffle**. Running the exact same Spark code can result in different data being on different partitions, as well as a different order within each partition.

Some links to more advanced reading on the above topics:
- [Spark application and UI](../spark-concepts/spark-application-and-ui)
- [Managing partitions](../spark-concepts/partitions)
- [Persisting in Spark](../spark-concepts/persistence)
- [Shuffling](../spark-concepts/shuffling)
- [Spark DataFrames are not ordered](../spark-concepts/df-order)
- [Catalyst optimiser](https://databricks.com/glossary/catalyst-optimizer)


### Setting up resources

If you want to try using PySpark or sparklyr and have not got access to a data platform that supports Spark, you can try one of the following options:

- [Databricks community edition](https://databricks.com/try-databricks) to use a small cluster on databricks infrastructure
- [Install PySpark on your laptop](https://www.datacamp.com/community/tutorials/installation-of-pyspark) to run PySpark in local mode
- [Install sparklyr on your laptop](https://therinspark.com/starting.html) to run sparklyr in local mode

The code in this book was executed using local mode so it will work on a laptop, Databricks or another platform that supports Spark. More details on local vs cluster mode in the [Guidance on Spark Sessions](spark-session-guidance).

### Further Resources

External links:
- [Apache Spark](https://spark.apache.org/)
- [Databricks community edition](https://databricks.com/try-databricks)
- [Databricks glossary](https://databricks.com/glossary)
- [Datacamp - Install PySpark for local mode](https://www.datacamp.com/community/tutorials/installation-of-pyspark)
- [Mastering Spark with R - Install sparklyr for local mode](https://therinspark.com/starting.html)
- [Wikipedia (Apache Spark)](https://en.wikipedia.org/wiki/Apache_Spark)
- [Wikipedia (Parallel computing)](https://en.wikipedia.org/wiki/Parallel_computing)
 
Links within this book:
- [Data types](data-types)
- [Guidance on Spark Sessions](spark-session-guidance)
- [Managing partitions](../spark-concepts/partitions)
- [Persisting in Spark](../spark-concepts/persistence)
- [Shuffling](../spark-concepts/shuffling)
- [Spark application and UI](../spark-concepts/spark-application-and-ui)
- [Spark DataFrames are not ordered](../spark-concepts/df-order)
