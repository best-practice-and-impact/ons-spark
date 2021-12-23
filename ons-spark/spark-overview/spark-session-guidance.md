# Spark Sessions

## Architecture

There are many versions of the diagram below on the internet. This version illustrates how we interact with data on the Cloudera cluster within the Data Access Plaform using Spark. If ONS staff are looking for more information online, note that our cluster runs on YARN.

![Diagram that shows a CDSW program on an edge node linking to a resource manager then multiple worker nodes containing executors.](./images/spark_driver_executors.PNG "processing diagram")

Each machine in a cluster is referred to as a node. We start with the edge node, which is sometimes called a gateway node because it’s our gateway or interface to the cluster. There is also a resource manager and some worker nodes. 

1. Our first step is to launch a Python or R CDSW session, which runs on the edge node. 

2. Next from within the CDSW session we create a Spark session. Spark will communicate with the resource manager and allocate some resource to our Spark session in the form of executors.  

3. We then write some Spark instructions in our CDSW session to process some data. Use the sparklyr package in R or the PySpark package for Python. These instructions form Spark jobs which are executed on worker nodes on the cluster by executors. 

4. Finally, any results we have requested are moved back to the edge node for us to view because we can't view data while it's on an executor.

One of the great features of Spark is that we’re able to scale up our resources to complete more expensive tasks, such as processing lots of data or running some complex algorithm. When we send a complex Spark job to the cluster the resource manager will automatically allocate more executors to help with the 
job. It will also shut them down when they are not needed. This process of switching executors on and off is called dynamic allocation.

Remember we should stop the Spark session when we are finished to free up the cluster resource for others. This is done automatically when we stop our CDSW
session, or you can also do it by running `spark.stop()`.

## Creating a Spark Session

There are many options when it comes to creating a Spark session. We recommend using the default parameters when getting started, you can then start adding other parameters when you learn more about how Spark works and how to tune your session to your specific needs.

### Using default parameters

As a starting point you can create a Spark session with all the defaults. This is the bare minimum you need to create a Spark session and will work fine in most cases. Note you should give your session a sensible name, here we have called it `default-session`.

```{code-tab} py
from pyspark.sql import SparkSession
spark = (
    SparkSession.builder.appName("default-session")
    .getOrCreate()
)
```
```{code-tab} r R
library(sparklyr)

default_config <- spark_config()

sc <- spark_connect(
    master = "yarn-client",
    app_name = "default-session",
    config = default_config)
```

### Specifying configs

When you have a good grasp of how Spark works, you understand what processing needs doing and most importantly the data, you can look to introduce some customised configurations to your Spark session. The session builder will look something like this,

```{code-tab} py
spark = (
    SparkSession.builder.appName("custom-session")
    .config("<property-name>", <property-value>)
    .getOrCreate()
)
```
```{code-tab} r R
config <- spark_config()
config$<property-name> <- <property-value>

sc <- spark_connect(
    master = "yarn-client",
    app_name = "custom-session",
    config = config)
```

The most popular configs to set are listed and explained in the [Config glossary](#config-glossary) section below. Some example Spark configurations are given in [example Spark configurations] section. 

See the page on `spark-defaults.conf` for setting these properties in other ways.

The [Configuring Spark Connections](https://spark.rstudio.com/guides/connections/) page of the RStudio documentation gives a good introduction on this topic that builds on the guidance here. 

## Config Glossary

Some commonly used Spark and YARN properties are listed in this section.  

For more details look at the official documentation for Spark configuration:
https://spark.apache.org/docs/2.4.0/configuration.html 

ONS runs Spark on YARN, that entails another set of configuration options documented here:
https://spark.apache.org/docs/2.4.0/running-on-yarn.html

To view all of the parameters, both default and custom, associated with your Spark session use
```
spark.sparkContext.getConf().getAll()
```
Note that `sparkContext` is created automatically when calling `SparkSession`


*Note that the values shown in brackets are the default values* 

**App Name:**  
`.appName()`  
The Spark session is often referred to as a Spark application. This option gives your Spark session a meaningful name, which can be used to help others identify the purpose of your app.  
You should enter your app name as a string, so within 'single' or "double" inverted commas.  

**Executor Memory:**  
`.config("spark.executor.memory", 1g)`  
This is the amount of memory per Spark executor. Can be given in mebibytes (m) or gibibytes (g).

**Cores**  
`.config("spark.executor.cores", 1)`  
This is the number of cores each executor can use.

**Executor Memory Overhead**  
`.config("spark.yarn.executor.memoryOverhead", max(0.1 * spark.executor.memory, 348m))`  
This sets the amount of additional memory allocated per executor for overheads.  
The default value is either 0.1 times the executor memory size, or 348MiB, whichever is greater.  
You might consider increasing the memory overhead, for example, if you are using User Defined Functions (UDFs) in your PySpark code.

**Dynamic Allocation**  
`.config("spark.dynamicAllocation.enabled", "true")`  
This setting allows your Spark session to scale up or down by automatically adding and removing executors depending on your workload.  
In the DAP environments, dynamic allocation is enabled by default.

**Max Executors**  
`.config("spark.dynamicAllocation.maxExecutors", )`  
This sets the maximum number of executors that can be used in dynamic allocation. Spark has no default value for maxExecutors.  
Within DAP, minExecutors is set to 3, so maxExecutors cannot be set to less than 3. 

Also note when specifying this config, Spark will reserve the maxExecutors for you so others cannot access these resources until you 
stop your session- even if you don't need them. See the [reserving](#reserving) section of the calculations below for more details.

**Shuffle Service**  
`.config("spark.shuffle.service.enabled", "true")`  
This ensures executors can be safely removed and is required for dynamic allocation. It is enabled by default in DAP.

**Hive Support**  
`.enableHiveSupport()`  
This setting should be used if you want to interact with data stored in Hive tables.

**Shuffle Partitions**  
`.config("spark.sql.shuffle.partitions", 200)`  
Sets the number of partitions to use for a DataFrame after a shuffle. The default is 200, which is too high for most use cases.

**Console Progress Bars**  
`.config("spark.ui.showConsoleProgress", "true")`  
This setting is set to true by default in DAP, but should be set to false in order to correctly display output in CDSW.

## Calculating Resource Allocation

Let's take the large session from the [Example Spark Sessions](http://np2rvlapxx507/DAP_CATS/guidance/-/blob/master/spark_session_sizes.ipynb) and explain the allocation of resource in more detail.

```
spark = (
    SparkSession.builder.appName("large-session")
    .config("spark.executor.memory", "10g")
    .config("spark.yarn.executor.memoryOverhead", "1g")
    .config("spark.executor.cores", 5)
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.maxExecutors", 5)
    .config("spark.shuffle.service.enabled", "true")
    .config("spark.ui.showConsoleProgress", "false")
    .enableHiveSupport()
    .getOrCreate()
)
```

### Resource allocation
How much resource is allocated to this application? Here we are asking how many cores and memory is this application using?

Cores - We have requested 5 cores per executor using `spark.executor.cores` and 5 executors with `spark.dynamicAllocation.maxExecutors`. The sum is then:

5 cores per executor x 5 executors = 25 cores

Memory - Using `spark.executor.memory` we requested 10 GiB of memory (also called  *on-heap* memory) per executor. Using `spark.yarn.executor.memoryOverhead` we
requested 1 GiB of overhead memory (also called *off-heap* memory). As before, we requested 5 executors with `spark.dynamicAllocation.maxExecutors`. Now the sum is:

( 10 GiB + 1 GiB ) per executor x 5 executors = 55 GiB of memory

### Reserving resource
An important point to remember about setting the `spark.dynamicAllocation.maxExecutors` option is that *although* Spark will increase and decrease the number of executors
we use at any point, the *maximum resource requested* will be reserved for the application. 

That means that in the above example Spark will reserve 25 cores and 55 GiB 
of memory for this application for the entirety of its duration, no matter if we are only utilising a single executor. This is not an issue here as 25 cores and 55 GiB isn't
excessive for processing a large DataFrame. However, if `spark.dynamicAllocation.maxExecutors` was set to 500 requests from other DAP users might be rejected and the cluster
would be under significant strain.