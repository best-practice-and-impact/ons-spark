## Configuration Hierarchy and `spark-defaults.conf`

Every Spark session has a *configuration*, where important settings such as the amount of memory, number of executors and cores are defined. There are three places that can contain configuration settings for a Spark session:
- Directly in [`SparkSession.builder`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.html#pyspark.sql.SparkSession.builder) (PySpark) or [`spark_connect()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark-connections.html) (sparklyr)
- In a `spark-defaults.conf` configuration file
- The default global setting

These work on a hierarchy, first looking at the value in `SparkSession.builder` (for PySpark) or `spark_connect()` (for sparklyr), if this is not defined then the value in `spark-defaults.conf`, and finally the default setting for that environment if not defined in either.

Remember that when using the Spark cluster to only use the resource that you need. Depending on your organisations architecture, using excessive memory could cause cloud computing bills to be higher, or prevent other users from running Spark jobs. This is the most important point to remember, regardless of where the Spark session is defined.

### Default global settings

If no value is supplied in `SparkSession.builder`/`spark_connect()` or `spark-defaults.conf`, then the default global settings will be used. These are ultimately derived either from the [Spark default value](https://spark.apache.org/docs/latest/configuration.html#available-properties), or in some cases the global setting specific to the environment. For instance, at the ONS, the `spark.dynamicAllocation.maxExecutors` is set to `3` in the Dev Test environment.

To use the global default settings in Spark you use the Spark session builder with no config options apart from the application name:

````{tabs}
```{code-tab} py
spark = (
    SparkSession.builder.appName("default-session")
    .getOrCreate()
)
```

```{code-tab} r R
default_config <- spark_config()

sc <- spark_connect(
  master = "yarn-client",
  app_name = "default-session",
  config = default_config)
```
````

It is advised to use the default settings when you are not yet sure of your requirements, such as in the initial investigation of new data sources. See the section *Default/Blank Session* in the Sample Spark Session Sizes guidance for more details.

### `spark-defaults.conf` configuration file

`spark-defaults.conf` is an optional configuration file, located in the home directory of the project. If it exists, then it sits in the middle of the hierarchy; any values not specified in the `SparkSession.builder` will instead use the values in `spark-defaults.conf`. If they are not defined in either then the default global settings will apply.

The format of `spark-defaults.conf` is one property per line, with the setting name and values separated by whitespace; these should **not** be given as strings. As an example, we can give the example small session in the `spark-defaults.conf` format:

````{tabs}
```{code-tab} plaintext spark-defaults.conf
spark.executor.memory                1g
spark.executor.cores                 1
spark.dynamicAllocation.enabled      true
spark.dynamicAllocation.maxExecutors 3
spark.sql.shuffle.partitions         12
spark.shuffle.service.enabled        true
spark.ui.showConsoleProgress         false
```

There are advantages and disadvantages to using a `spark-defaults.conf` file; they can be useful in some circumstances but often users prefer to define their Spark sessions from within the script. As with any coding practice, try and be consistent within your team.

Advantages:
- `spark-defaults.conf` is a specific example of a configuration file; one particular advantage is allowing editing of the configuration without altering the main script, which can be useful when running the code in different environments or when the size of the source data varies considerably.
- It is language agnostic and the same configuration file can be used with Python and R (and also Scala).
- One use case for `spark-defaults.conf` if using CDSW is setting `spark.ui.showConsoleProgress` to `false`; this disables the progress bar from obscuring some outputs in CDSW and is almost always a desired setting in any project.
- If the Git repo is cloned to the home directory rather than a subdirectory then it is easy to track it with Git.

Disadvantages:
- The Spark session settings become less visible as they are contained in a different file. It is sensible to document in a docstring or comment that you are using the settings in `spark-defaults.conf`.
- Depending on how the Git repositories in your container are set up, you may not be able to track `spark-defaults.conf` with Git. If you use one repository per container, then it can be tracked; if several repositories are cloned in the same container as subdirectories then the `spark-defaults.conf` will be outside the tracked repository.

### Using `SparkSession.builder`/`spark_connect()`

The values in `SparkSession.builder`/`spark_connect()` will override any values set either globally or in
 `spark-defaults.conf`. This is the most commonly used option at the ONS.
 
When starting a new project which requires Spark, think about your requirements and use the appropriate amount of memory; the Guidance on Spark Sessions and Sample Spark Session Sizes articles are a good starting point. If unsure, use a blank session which will use the global defaults. It is important not to copy and paste Spark session builders between projects as you may use far more memory than needed, or too little, which may case the code to run slowly or not at all.

### Legacy configuration options

The values in `spark-defaults.conf` are read once when the Python or R kernel is initiated, so if these values are altered you will need to restart the kernel to use them.

The hierarchy of Spark session settings is more complicated once a Spark session has been started, as in the absence of a value specifically defined in `SparkSession.builder`/`spark_connect()`, the value from the previous Spark session will be used. For example, assume that you need to join two small DataFrames and so start a Spark session with `spark.sql.shuffle.partitions` set to `12`. You then want to process much larger data and so stop the current Spark session and create a new one with the default value for `spark.sql.shuffle.partitions`, which is `200`, and so do not include this in the builder. However, it will still start a session with the previous value, `12`. To avoid this issue, either explicitly specify any setting that has been previously defined, or stop and restart the kernel.

### Scala and CDSW

If using [Scala](https://www.scala-lang.org/) in CDSW, a Spark session will be automatically started when loading a CDSW session, so you will have to use `spark-defaults.conf`, unless the default settings are appropriate for your project.

At the ONS, we recommend that Spark code is written with Python or R, using the PySpark or sparklyr packages. Most users find writing Spark code with Python or R much easier than learning Scala. There are also more users of Python and R at the ONS, meaning it is easier to find people to peer review and maintain your code.

### Further Resources

PySpark Documentation:
- [`SparkSession.builder`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.html#pyspark.sql.SparkSession.builder)

sparklyr and tidyverse Documentation:
- [`spark_connect()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark-connections.html)

Spark Documentation:
- [Spark Properties and Defaults](https://spark.apache.org/docs/latest/configuration.html#available-properties)

Other Links:
- [Scala](https://www.scala-lang.org/)