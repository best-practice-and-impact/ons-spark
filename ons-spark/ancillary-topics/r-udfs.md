## R UDFs

When coding with Spark, you will generally want to try and use sparklyr or Spark SQL functions wherever possible. However, there are instances where there may not be a Spark function available to do what you need. In cases such as this, one option is to use an R user-defined function (UDF).

The sparklyr package comes with the `spark_apply()` function, which is designed for this purpose. It allows you to apply a function written in R to a Spark object (e.g. a Spark DataFrame). In order for this to work, you need to ensure that your cluster administrator, cloud provider, or you (in the case of running Spark locally) has configured your cluster by installing either:

- R on every node
- Apache Arrow on every node (requires Spark 2.3 or later)

Although only one of these is required to make use of `spark_apply()`, in practice, it is recommended to use `spark_apply()` with Apache Arrow, as it provides significant performance improvements. In general, R UDFs should be considered a 'last resort', where there is no equivalent functionality compatible with Spark and an R function needs to be run on a large Spark DataFrame. They are generally very inefficient due to the need to serialise and deserialise the data between Spark and R. Apache Arrow speeds this up and allows data to be transferred more efficiently between Spark and R, but even with these improvements `spark_apply()` is still best avoided if an alternative can be found. For a detailed explanation of why `spark_apply()` is more efficient with Apache Arrow, see the [Distributed R chapter of Mastering Spark with R](https://therinspark.com/distributed.html#cluster-requirements). 

However, it is sometimes not possible to avoid using a UDF. For example, if we want to use a specialised statistical R package on our data that has no Spark equivalent. The examples below demonstrate how to use `spark_apply()`. They are written assuming that the user has Apache Arrow available on every node (and has installed the `arrow` package using `install.packages("arrow")`), but they should work even if your cluster configuration does not have Apache Arrow installed. Any code to omit in this case is indicated in the comments.  

### Example 1: Simple UDF and loading packages onto worker nodes

This example represents a simple UDF just to demonstrate how `spark_apply()` can be used. In practice, it is far too simple to necessitate using a UDF - we could definitely do this with sparklyr code instead of R code!

The example UDF below will make use of the `mutate()` function from he `dplyr` package. In order for Spark to run our UDF, we need to make sure that the packages we need are installed on each worker node in our Spark cluster once we have set up our session. To do this, we need to pass the location of our R package library to the Spark cluster by including the `spark.r.libpaths` setting as shown below:

```{code-tab} R
library(dplyr)
library(sparklyr)
# Load the 'arrow' library - omit if you are unable to use Apache Arrow
library(arrow)

# Set up our Spark configuration - including our library path
default_config <- spark_config()
default_config["spark.r.libpaths"] <- .libPaths()[1]

sc <- spark_connect(
  master = "local[2]",
  app_name = "r-udfs",
  config = default_config)

```

You can replace `.libPaths()[1]` with the full path to your R package library in quotation marks if you need to. If you are unsure where this is, running `.libPaths()` in your session console will show you a list of library locations that R searches for installed packages.

We have told Spark where to look for R packages, but they are not yet installed on the worker nodes. The first time we run `spark_apply()` in our Spark session, any packages found in the specified library path will be copied over to the Spark cluster. As a result, the most efficient way to load packages onto the cluster is to run a 'dummy' UDF on a small dataset before running our actual function. This ensures that the packages are loaded and ready for use before Spark attempts to perform more complex operations on our data. We can define and run our dummy UDF as shown:

````{tabs}
```{code-tab} R
# Define dummy UDF 'libload' which loads our required function libraries
# You could just define an empty function here, but this forces Spark to output 
# a list of libraries loaded on the cluster so we can see it has worked 

libload <- function() {
  library(dplyr)
}

# All packages will be loaded on to worker nodes during first call to spark_apply in session
# so it is more efficient to do this 'on' a minimal sdf (of length 1) first

sdf_len(sc, 1) |> sparklyr::spark_apply(f = libload,
                packages = FALSE)

```
``` plaintext
# Source:   table<`sparklyr_tmp__78b5fc02_a3fa_40b2_98d8_a156efa9cf69`> [8 x 1]
# Database: spark_connection
  result   
  <chr>    
1 dplyr    
2 stats    
3 graphics 
4 grDevices
5 utils    
6 datasets 
7 methods  
8 base
```
````    

Confusingly, to load packages using this method, note that we have had to set the `packages` argument in `spark_apply()` to `FALSE`. This is because there is another method to load packages on to the cluster, using [`spark_apply_bundle()`](https://www.rdocumentation.org/packages/sparklyr/versions/1.8.5/topics/spark_apply_bundle). This function is designed to bundle all packages into a .tar file ready to pass to the Spark cluster. Setting the `packages` argument to `TRUE` prompts Spark to search for this .tar file and extract the relevant packages. However, this method is not easily generalisable to different system setups and may not always be possible using a given configuration (e.g. it does not seem to work with S3 bucket storage). For this reason, this guide demonstrates using the alternative approach outlined above to use packages with `spark_apply()`.  



