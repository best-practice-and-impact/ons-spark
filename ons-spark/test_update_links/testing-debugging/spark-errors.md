## Understanding and Handling Spark Errors

Spark errors can be very long, often with redundant information and can appear intimidating at first. However, if you know which parts of the error message to look at you will often be able to resolve it. Sometimes you may want to handle errors programmatically, enabling you to simplify the output of an error message, or to continue the code execution in some circumstances.

Errors can be rendered differently depending on the software you are using to write code, e.g. CDSW will generally give you long passages of red text whereas Jupyter notebooks have code highlighting. This example uses the CDSW error messages as this is the most commonly used tool to write code at the ONS. The general principles are the same regardless of IDE used to write code.

There are some examples of errors given here but the intention of this article is to help you debug errors for yourself rather than being a list of all potential problems that you may encounter. We focus on error messages that are caused by Spark code.

### Practical tips for error handling in Spark

Spark error messages can be long, but the most important principle is that **the first line returned is the most important**. This first line gives a description of the error, put there by the package developers. The output when you get an error will often be larger than the length of the screen and so you may have to scroll up to find this. The examples in the next sections show some PySpark and sparklyr errors.

The most likely cause of an error is your code being incorrect in some way. Use the information given on the first line of the error message to try and resolve it. In many cases this will give you enough information to help diagnose and attempt to resolve the situation. If you are still struggling, try using a search engine; [Stack Overflow](https://stackoverflow.com/) will often be the first result and whatever error you have you are very unlikely to be the first person to have encountered it. If you are still stuck, then consulting your colleagues is often a good next step.

Remember that Spark uses the concept of lazy evaluation, which means that your error might be elsewhere in the code to where you think it is, since the plan will only be executed upon calling an action. If you suspect this is the case, try and put an action earlier in the code and see if it runs. Repeat this process until you have found the line of code which causes the error. With more experience of coding in Spark you will come to know which areas of your code could cause potential issues

Errors which appear to be related to memory are important to mention here. The first solution should **not** be just to increase the amount of memory; instead see if other solutions can work, for instance breaking the lineage with [checkpointing or staging tables](../raw-notebooks/checkpoint-staging/checkpoint-staging). See the [Ideas for optimising Spark code](../spark-concepts/optimisation-tips) in the first instance. Increasing the memory should be the last resort.

Occasionally your error may be because of a software or hardware issue with the Spark cluster rather than your code. It is worth resetting as much as possible, e.g. if you are using a Docker container then close and reopen a session. If there are still issues then raise a ticket with your organisations IT support department.

If you are struggling to get started with Spark then ensure that you have read the [Getting Started with Spark](../spark-overview/spark-start) article; in particular, ensure that your environment variables are set correctly.

#### A recap of Python and R errors

<details>
<summary><b>Python Errors</b></summary>

PySpark errors are just a variation of Python errors and are structured the same way, so it is worth looking at the documentation for [errors](https://docs.python.org/3/tutorial/errors.html) and the [base exceptions](https://docs.python.org/3/library/exceptions.html#bltin-exceptions).

Some PySpark errors are fundamentally Python coding issues, not PySpark. An example is where you try and use a variable that you have not defined, for instance, when creating a new DataFrame without a valid Spark session:

````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession, functions as F

data = [["Cat", 10], ["Dog", 5]]
columns = ["animal", "count"]

animal_df = spark.createDataFrame(data, columns)
```
````

````{tabs}
```{code-tab} plaintext Python Output
NameError: name 'spark' is not defined
NameError                                 Traceback (most recent call last)
in engine
----> 1 animal_df = spark.createDataFrame(data, columns)

NameError: name 'spark' is not defined
```
````

The error message on the first line here is clear: `name 'spark' is not defined`, which is enough information to resolve the problem: we need to start a Spark session.

This error has two parts, the error message and the stack trace. The stack trace tells us the specific line where the error occurred, but this can be long when using nested functions and packages. Generally you will only want to look at the stack trace if you cannot understand the error from the error message or want to locate the line of code which needs changing.

</details>


<details>
<summary><b>R Errors</b></summary>

sparklyr errors are just a variation of base R errors and are structured the same way. Some sparklyr errors are fundamentally R coding issues, not sparklyr. An example is where you try and use a variable that you have not defined, for instance, when creating a new sparklyr DataFrame without first setting `sc` to be the Spark session:

````{tabs}
```{code-tab} r R
library(sparklyr)
library(dplyr)

# Create a base R DataFrame
animal_df <- data.frame(
        animal = c("Cat", "Dog"),
        count = c(10, 5),
        stringsAsFactors = FALSE)

# Copy base R DataFrame to the Spark cluster
animal_sdf <- sparklyr::sdf_copy_to(sc, animal_df)
```
````

````{tabs}
```{code-tab} r R Output
Error in sdf_copy_to(sc, animal_df) : object 'sc' not found
```
````

The error message here is easy to understand: `sc`, the Spark connection object, has not been defined. To resolve this, we just have to start a Spark session. Not all base R errors are as easy to debug as this, but they will generally be much shorter than Spark specific errors.

</details>

#### Example Spark error: missing file

When using Spark, sometimes errors from other languages that the code is compiled into can be raised. You may see messages about Scala and Java errors. Do not be overwhelmed, just locate the error message on the first line rather than being distracted. An example is reading a file that does not exist.

<details>
<summary><b>Python Example</b></summary>

For more details on why Python error messages can be so long, especially with Spark, you may want to read the documentation on [Exception Chaining](https://docs.python.org/3/tutorial/errors.html#exception-chaining).

Try using [`spark.read.parquet()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.parquet.html) with an incorrect file path:

````{tabs}
```{code-tab} py
spark = (SparkSession.builder.master("local[2]")
         .appName("errors")
         .getOrCreate())

file_path = "this/is_not/a/file_path.parquet"

no_df = spark.read.parquet(file_path)
```
````

````{tabs}
```{code-tab} plaintext Truncated Python Output
AnalysisException: 'Path does not exist: hdfs://.../this/is_not/a/file_path.parquet;'
Py4JJavaError                             Traceback (most recent call last)
...
```
````

The full error message is not given here as it is very long and some of it is platform specific, so try running this code in your own Spark session. You will see a long error message that has raised both a [`Py4JJavaError`](https://www.py4j.org/py4j_java_protocol.html) and an [`AnalysisException`](https://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/utils.html). The `Py4JJavaError` is caused by Spark and has become an `AnalysisException` in Python.

We can ignore everything else apart from the first line as this contains enough information to resolve the error:

`AnalysisException: 'Path does not exist: hdfs://.../this/is_not/a/file_path.parquet;'`

The code will work if the `file_path` is correct; this can be confirmed with `.show()`:

````{tabs}
```{code-tab} py
import yaml
with open("ons-spark/config.yaml") as f:
    config = yaml.safe_load(f)
    
file_path = config["rescue_path"]

rescue = spark.read.parquet(file_path)
rescue.select("incident_number", "animal_group").show(3)
```
````

````{tabs}
```{code-tab} plaintext Python Output
+---------------+------------+
|incident_number|animal_group|
+---------------+------------+
|       80771131|         Cat|
|      141817141|       Horse|
|143166-22102016|        Bird|
+---------------+------------+
only showing top 3 rows
```
````

</details>

<details>
<summary><b>R Example</b></summary>

Try using [`spark_read_parquet()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark_read_parquet.html) with an incorrect file path:

````{tabs}
```{code-tab} r R
sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "errors",
    config = sparklyr::spark_config())

file_path <- "this/is_not/a/file_path.parquet"
no_df <- sparklyr::spark_read_parquet(sc, path=file_path)
```
````

````{tabs}
```{code-tab} plaintext Truncated R Output
Error: org.apache.spark.sql.AnalysisException: Path does not exist: hdfs://.../this/is_not/a/file_path.parquet;
...
```
````

The full error message is not given here as it is very long and some of it is platform specific, so try running this code in your own Spark session. Although both `java` and `scala` are mentioned in the error, ignore this and look at the first line as this contains enough information to resolve the error:

`Error: org.apache.spark.sql.AnalysisException: Path does not exist: hdfs://.../this/is_not/a/file_path.parquet;`

The code will work if the `file_path` is correct; this can be confirmed with `glimpse()`:

````{tabs}
```{code-tab} r R
rescue <- sparklyr::spark_read_parquet(sc, path=config$rescue_path)

rescue %>%
    sparklyr::select(incident_number, animal_group) %>%
    pillar::glimpse()
```
````

````{tabs}
```{code-tab} r R Output
Rows: ??
Columns: 2
Database: spark_connection
$ incident_number <chr> "80771131", "141817141", "143166-22102016", "43051141"…
$ animal_group    <chr> "Cat", "Horse", "Bird", "Cat", "Dog", "Deer", "Deer", …
```
````

</details>

#### Understanding Errors: Summary of key points

- Spark error messages can be long, but most of the output can be ignored
- **Look at the first line**; this is the error message and will often give you all the information you need
- The stack trace tells you where the error occurred but can be very long and can be misleading in some circumstances
- Error messages can contain information about errors in other languages such as Java and Scala, but these can mostly be ignored

### Handling exceptions in Spark

When there is an error with Spark code, the code execution will be interrupted and will display an error message. In many cases this will be desirable, giving you chance to fix the error and then restart the script. You can however use error handling to print out a more useful error message. Sometimes you may want to handle the error and then let the code continue.

It is recommend to read the sections above on understanding errors first, especially if you are new to error handling in Python or base R.

The most important principle for handling errors is to look at the first line of the code. This will tell you the exception type and it is this that needs to be handled. The examples here use error outputs from CDSW; they may look different in other editors.

Error handling can be a tricky concept and can actually make understanding errors more difficult if implemented incorrectly, so you may want to get more experience before trying some of the ideas in this section.

#### What is error handling and why use it?

You will often have lots of errors when developing your code and these can be put in two categories: *syntax errors* and *runtime errors*. A *syntax error* is where the code has been written incorrectly, e.g. a missing comma, and has to be fixed before the code will compile. A *runtime error* is where the code compiles and starts running, but then gets interrupted and an error message is displayed, e.g. trying to divide by zero or non-existent file trying to be read in. We saw some examples in the the section above. Only *runtime errors* can be handled.

We saw that Spark errors are often long and hard to read. You can use error handling to test if a block of code returns a certain type of error and instead return a clearer error message. This can save time when debugging.

You can also set the code to continue after an error, rather than being interrupted. You may want to do this if the error is not critical to the end result. If you do this it is a good idea to print a warning with the `print()` statement or use logging, e.g. using the [Python logger](https://docs.python.org/3/library/logging.html).

```{warning}
- Just because the code runs does not mean it gives the desired results, so make sure you always test your code!
- It is useful to know how to handle errors, but do not overuse it. Remember that errors do occur for a reason and you do **not** usually need to try and catch every circumstance where the code might fail.
```

#### How to handle Spark errors

<details>
<summary><b>Handling Errors in PySpark</b></summary>

PySpark errors can be handled in the usual Python way, with a `try`/`except` block. Python contains some base exceptions that do not need to be imported, e.g. `NameError` and `ZeroDivisionError`. Package authors sometimes create custom exceptions which need to be imported to be handled; for PySpark errors you will likely need to import `AnalysisException` from `pyspark.sql.utils` and potentially `Py4JJavaError` from `py4j.protocol`:

````{tabs}
```{code-tab} py
from py4j.protocol import Py4JJavaError
from pyspark.sql.utils import AnalysisException
```
````

</details>

<details>
<summary><b>Handling Errors in sparklyr</b></summary>
    
Unlike Python (and many other languages), R uses a function for error handling, `tryCatch()`. sparklyr errors are still R errors, and so can be handled with `tryCatch()`. Error handling functionality is contained in base R, so there is no need to reference other packages. [Advanced R](https://adv-r.hadley.nz/) has more details on [`tryCatch()`](https://adv-r.hadley.nz/conditions.html#handling-conditions)

Although error handling in this way is unconventional if you are used to other languages, one advantage is that you will often use functions when coding anyway and it becomes natural to assign `tryCatch()` to a custom function.

</details>

### Error Handling Examples

#### Example 1: No Spark session

A simple example of error handling is ensuring that we have a running Spark session. If want to run this code yourself, restart your container or console entirely before looking at this section.

<details>
<summary><b>Python Example</b></summary>
    
Recall the `NameError` from earlier:

````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession, functions as F

data = [["Cat", 10], ["Dog", 5]]
columns = ["animal", "count"]

animal_df = spark.createDataFrame(data, columns)
```
````

````{tabs}
```{code-tab} plaintext Python Output
NameError: name 'spark' is not defined
NameError                                 Traceback (most recent call last)
in engine
----> 1 animal_df = spark.createDataFrame(data, columns)

NameError: name 'spark' is not defined
```
````

We can handle this exception and give a more useful error message.

In Python you can test for specific error types and the content of the error message. This ensures that we capture only the error which we want and others can be raised as usual.

In this example, first test for `NameError` and then check that the error message is `"name 'spark' is not defined"`.

The syntax here is worth explaining:
- The code within the `try:` block has active error handing. Code outside this will not have any errors handled.
- If a `NameError` is raised, it will be handled. Other errors will be raised as usual.
- `e` is the error message object; to test the content of the message convert it to a string with `str(e)`
- Within the `except:` block `str(e)` is tested and if it is `"name 'spark' is not defined"`, a `NameError` is raised but with a custom error message that is more useful than the default
- Raising the error `from None` prevents [exception chaining](https://docs.python.org/3/tutorial/errors.html#exception-chaining) and reduces the amount of output
- If the error message is not `"name 'spark' is not defined"` then the exception is raised as usual

````{tabs}
```{code-tab} py
try:
    animal_df = spark.createDataFrame(data, columns)
    animal_df.show()
except NameError as e:
    if str(e) == "name 'spark' is not defined":
        raise NameError("No running Spark session. Start one before creating a DataFrame") from None
    else:
        raise
```
````

````{tabs}
```{code-tab} plaintext Python Output
NameError: No running Spark session. Start one before creating a DataFrame
NameError                Traceback (most recent call last)
in engine
      4 except NameError as e:
      5     if str(e) == "name 'spark' is not defined":
----> 6         raise NameError("No running Spark session. Start one before creating a DataFrame") from None
      7     else:
      8         raise

NameError: No running Spark session. Start one before creating a DataFrame
```
````

This error message is more useful than the previous one as we know exactly what to do to get the code to run correctly: start a Spark session and run the code again:


````{tabs}
```{code-tab} py
spark = (SparkSession.builder.master("local[2]")
         .appName("errors")
         .getOrCreate())

try:
    animal_df = spark.createDataFrame(data, columns)
    animal_df.show()
except NameError as e:
    if str(e) == "name 'spark' is not defined":
        raise NameError("No running Spark session. Start one before creating a DataFrame") from None
    else:
        raise
```
````

````{tabs}
```{code-tab} plaintext Python Output
+------+-----+
|animal|count|
+------+-----+
|   Cat|   10|
|   Dog|    5|
+------+-----+
```
````

As there are no errors in the `try` block the `except` block is ignored here and the desired result is displayed.

</details>
    
<details>
<summary><b>R Example</b></summary>
    
Recall the `object 'sc' not found` error from earlier:

````{tabs}
```{code-tab} r R
library(sparklyr)
library(dplyr)

# Create a base R DataFrame
animal_df <- data.frame(
        animal = c("Cat", "Dog"),
        count = c(10, 5),
        stringsAsFactors = FALSE)

# Copy base R DataFrame to the Spark cluster
animal_sdf <- sdf_copy_to(sc, animal_df)
```
````

````{tabs}
```{code-tab} r R Output
Error in sdf_copy_to(sc, animal_df) : object 'sc' not found
```
````

We can handle this exception and give a more useful error message.

In R you can test for the content of the error message. This ensures that we capture only the specific error which we want and others can be raised as usual. In this example, see if the error message contains `object 'sc' not found`.

The syntax here is worth explaining:
- The expression to test and the error handling code are both contained within the `tryCatch()` statement; code outside this will not have any errors handled.
- Code assigned to `expr` will be attempted to run
- If there is no error, the rest of the code continues as usual
- If an error is raised, the `error` function is called, with the error message `e` as an input
- `grepl()` is used to test if `"AnalysisException: Path does not exist"` is within `e`; if it is, then an error is raised with a custom error message that is more useful than the default
- If the message is anything else, `stop(e)` will be called, which raises an error with `e` as the message

````{tabs}
```{code-tab} r R
tryCatch(
    expr = {
        # Copy base R DataFrame to the Spark cluster
        animal_sdf <- sparklyr::sdf_copy_to(sc, animal_df)
        # Preview data
        pillar::glimpse(animal_sdf)
    },
    error = function(e){
        # Test to see if the error message contains `object 'sc' not found`
        if(grepl("object 'sc' not found", e, fixed=TRUE)){
            # Raise error with custom message if true
            stop("No running Spark session. Start one before creating a sparklyr DataFrame")            
        }else{
            # Raise error without modification
            stop(e)
        }
    })
```
````

````{tabs}
```{code-tab} r R Output
Error in value[[3L]](cond) : 
  No running Spark session. Start one before creating a sparklyr DataFrame
```
````

This error message is more useful than the previous one as we know exactly what to do to get the code to run correctly: start a Spark session and run the code again:

````{tabs}
```{code-tab} r R

# Start Spark session
sc <- sparklyr::spark_connect(
  master = "local[2]",
  app_name = "errors",
  config = sparklyr::spark_config())

tryCatch(
    expr = {
        # Copy base R DataFrame to the Spark cluster
        animal_sdf <- sparklyr::sdf_copy_to(sc, animal_df)
        # Preview data
        pillar::glimpse(animal_sdf)
    },
    error = function(e){
        # Test to see if the error message contains `object 'sc' not found`
        if(grepl("object 'sc' not found", e, fixed=TRUE)){
            # Raise error with custom message if true
            stop("No running Spark session. Start one before creating a sparklyr DataFrame")            
        }else{
            # Raise error without modification
            stop(e)
        }
    })
```
````

````{tabs}
```{code-tab} r R Output
Rows: ??
Columns: 2
Database: spark_connection
$ animal <chr> "Cat", "Dog"
$ count  <dbl> 10, 5
```
````

As there are no errors in `expr` the `error` statement is ignored here and the desired result is displayed.


</details>

#### Example 2: Handle multiple errors in a function

This example shows how functions can be used to handle errors.

<details>
<summary><b>Python Example</b></summary>

We have started to see how useful `try`/`except` blocks can be, but it adds extra lines of code which interrupt the flow for the reader. As such it is a good idea to wrap error handling in functions. You should document why you are choosing to handle the error and the docstring of a function is a natural place to do this.

As an example, define a wrapper function for [`spark.read.csv`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.csv.html) which reads a CSV file from HDFS. This can handle two types of errors:
- If the Spark context has been stopped, it will return a custom error message that is much shorter and descriptive
- If the path does not exist the same error message will be returned but raised `from None` to shorten the stack trace

Only the first error which is hit at runtime will be returned. Logically this makes sense: the code could logically have multiple problems but the execution will halt at the first, meaning the rest can go undetected until the first is fixed.

This function uses some Python string methods to test for error message equality: [`str.find()`](https://docs.python.org/3/library/stdtypes.html#str.find) and slicing strings with `[:]`.

````{tabs}
```{code-tab} py
from py4j.protocol import Py4JJavaError
from pyspark.sql.utils import AnalysisException

def read_csv_handle_exceptions(file_path):
    """
    Read a CSV from HDFS and return a Spark DF
    
    Custom exceptions will be raised for trying to read the CSV from a stopped
        Spark context and if the path does not exist.
    
    Args:
        file_path (string): path of CSV on HDFS
        
    Returns:
        spark DataFrame
    """
    try:
        return spark.read.csv(file_path, header=True, inferSchema=True)
    except Py4JJavaError as e:
        # Uses str(e).find() to search for specific text within the error
        if str(e).find("java.lang.IllegalStateException: Cannot call methods on a stopped SparkContext") > -1:
            # Use ... from None to ignore the stack trace in the output
            raise Exception("Spark session has been stopped. Please start a new Spark session.") from None
        else:
            # Raise an exception if the error message is anything else
            raise
    except AnalysisException as e:
        # See if the first 21 characters are the error we want to capture
        if str(e)[:21] == "'Path does not exist:":
            raise Exception(e) from None
        else:
            raise
```
````

Stop the Spark session and try to read in a CSV:

````{tabs}
```{code-tab} py
spark.stop()
no_df = read_csv_handle_exceptions("this/is_not/a/file_path.csv")
```
````

````{tabs}
```{code-tab} plaintext Python Output
Exception: 'Path does not exist: hdfs://.../this/is_not/a/file_path.csv;'
Exception                                 Traceback (most recent call last)
in engine
----> 1 df = read_csv_handle_exceptions("this/is_not/a/file_path.csv")

<ipython-input-1-394f508cffc3> in read_csv_handle_exceptions(file_path)
     13         # See if the first 21 characters are the error we want to capture
     14         if str(e)[:21] == "'Path does not exist:":
---> 15             raise Exception(e) from None
     16         else:
     17             raise

Exception: 'Path does not exist: hdfs://.../this/is_not/a/file_path.csv;'
```
````

Fix the path; this will give the other error:

````{tabs}
```{code-tab} py
import yaml
with open("ons-spark/config.yaml") as f:
    config = yaml.safe_load(f)
    
rescue_path_csv = config["rescue_path_csv"]
rescue = read_csv_handle_exceptions(rescue_path_csv)
```
````

````{tabs}
```{code-tab} plaintext Python Output
Exception: Spark session has been stopped. Please start a new Spark session.
Exception                                 Traceback (most recent call last)
in engine
----> 1 rescue = read_csv_handle_exceptions(rescue_path_csv)

<ipython-input-1-de3ee93967c9> in read_csv_handle_exceptions(file_path)
     17         if str(e).find("java.lang.IllegalStateException: Cannot call methods on a stopped SparkContext") > -1:
     18             # Use ... from None to ignore the stack trace in the output
---> 19             raise Exception("Spark session has been stopped. Please start a new Spark session.") from None
     20         else:
     21             # Raise an exception if the error message is anything else

Exception: Spark session has been stopped. Please start a new Spark session.
```
````

Correct both errors by starting a Spark session and reading the correct path:

````{tabs}
```{code-tab} py
spark = (SparkSession.builder.master("local[2]")
         .appName("errors")
         .getOrCreate())

rescue = read_csv_handle_exceptions(rescue_path_csv)
rescue.select("IncidentNumber", "AnimalGroupParent").show(3)
```
````

````{tabs}
```{code-tab} plaintext Python Output
+--------------+-----------------+
|IncidentNumber|AnimalGroupParent|
+--------------+-----------------+
|        139091|              Dog|
|        275091|              Fox|
|       2075091|              Dog|
+--------------+-----------------+
only showing top 3 rows

```
````
A better way of writing this function would be to add `spark` as a parameter to the function:

`def read_csv_handle_exceptions(spark, file_path):`

Writing the code in this way prompts for a Spark session and so should lead to fewer user errors when writing the code.

</details>

<details>
<summary><b>R Example</b></summary>

We have started to see how useful the `tryCatch()` function is, but it adds extra lines of code which interrupt the flow for the reader. It is easy to assign a `tryCatch()` function to a custom function and this will make your code neater. You should document why you are choosing to handle the error in your code.

As an example, define a wrapper function for [`spark_read_csv()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark_read_csv.html) which reads a CSV file from HDFS. This can handle two types of errors:
- If the Spark context has been stopped, it will return a custom error message that is much shorter and descriptive
- If the path does not exist the default error message will be returned

Only the first error which is hit at runtime will be returned. Logically
this makes sense: the code could logically have multiple problems but
the execution will halt at the first, meaning the rest can go undetected
until the first is fixed.

This function uses `grepl()` to test if the error message contains a
specific string:

````{tabs}
```{code-tab} r R
read_csv_handle_exceptions <- function(file_path){
    tryCatch(
        expr = {
            # Read a CSV file from HDFS
            sdf <- sparklyr::spark_read_csv(sc,
                                            file_path,
                                            header=TRUE,
                                            infer_schema=TRUE)
            return(sdf)
        },
        error = function(e){
            # See if the error is invalid connection and return custom error message if true
            if(grepl("invalid connection", e, fixed=TRUE)){
                stop("No running Spark session. Start one before creating a sparklyr DataFrame")
            # See if the file path is valid; if not, return custom error message
            }else if(grepl("AnalysisException: Path does not exist", e, fixed=TRUE)){
                stop(paste("File path:",
                           file_path,
                           "does not exist. Please supply a valid file path.",
                           sep=" "))
            # If the error message is neither of these, return the original error
            }else{stop(e)}
        })
}
```
````

Stop the Spark session and try to read in a CSV:

````{tabs}
```{code-tab} r R
sparklyr::spark_disconnect(sc)
file_path <- "this/is_not/a/file_path.csv"
no_sdf <- read_csv_handle_exceptions(file_path)

```
````

````{tabs}
```{code-tab} plaintext R Output
Error in value[[3L]](cond) : 
  No running Spark session. Start one before creating a sparklyr DataFrame
```
````

Start a Spark session and try the function again; this will give the
other error:

````{tabs}
```{code-tab} r R
sc <- sparklyr::spark_connect(
  master = "local[2]",
  app_name = "errors",
  config = sparklyr::spark_config())
        
no_sdf <- read_csv_handle_exceptions(file_path)
```
````

````{tabs}
```{code-tab} plaintext R Output
Error in value[[3L]](cond) : 
  File path: this/is_not/a/file_path.csv does not exist. Please supply a valid file path
```
````

Run without errors by supplying a correct path:

````{tabs}
```{code-tab} r R
config <- yaml::yaml.load_file("ons-spark/config.yaml")

rescue <- read_csv_handle_exceptions(config$rescue_path_csv)
rescue %>%
    sparklyr::select(IncidentNumber, AnimalGroupParent) %>%
    pillar::glimpse()
```
````

````{tabs}
```{code-tab} plaintext R Output
Rows: ??
Columns: 2
Database: spark_connection
$ IncidentNumber    <chr> "139091", "275091", "2075091", "2872091", "3553091",…
$ AnimalGroupParent <chr> "Dog", "Fox", "Dog", "Horse", "Rabbit", "Unknown - H…
```
````

A better way of writing this function would be to add `sc` as a
parameter to the function:

`read_csv_handle_exceptions <- function(sc, file_path)`

Writing the code in this way prompts for a Spark session and so should
lead to fewer user errors when writing the code.

</details>

#### Example 3: Capture and ignore the error

Another option is to capture the error and ignore it. Generally you will only want to do this in limited circumstances when you are ignoring errors that you expect, and even then it is better to anticipate them using logic. After all, the code returned an error for a reason!

This example counts the number of distinct values in a column, returning `0` and printing a message if the column does not exist.


<details>
<summary><b>Python Example</b></summary>

Define a Python function in the usual way:

````{tabs}
```{code-tab} py
def distinct_count(df, input_column):
    """
    Returns the number of unique values of a specified column in a Spark DF.
    
    Will return an error if input_column is not in df
    
    Args:
        df (spark DataFrame): input DataFrame
        input_column (string): name of a column in df for which the distinct count is required
        
    Returns:
        int: Count of unique values in input_column
    """
    try:
        return df.select(input_column).distinct().count()
    except AnalysisException as e:
        # Derive an expected error
        expected_error_str = f"cannot resolve '`{input_column}`' given input columns"
        # Test if the error contains the expected_error_str
        if str(e).find(expected_error_str) > -1:
            # Print a message and continue
            print(f"Column `{input_column}` does not exist. Returning `0`")
            return 0
        else:
            # Raise an error otherwise
            raise
```
````
Try one column which exists and one which does not:

````{tabs}
```{code-tab} py
rescue_path = config["rescue_path"]
rescue = spark.read.parquet(rescue_path)

distinct_count(rescue, "incident_number")
```
````

````{tabs}
```{code-tab} plaintext Python Output
5898
```
````

````{tabs}
```{code-tab} py
distinct_count(rescue, "column_that_does_not_exist")
```
````

````{tabs}
```{code-tab} plaintext Python Output
Column `column_that_does_not_exist` does not exist. Returning `0`
0
```
````

A better way would be to avoid the error in the first place by checking if the column exists before the [`.distinct()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.distinct.html):

````{tabs}
```{code-tab} py
def distinct_count(df, input_column):
    # Test if column exists
    if input_column in df.columns:
        return df.select(input_column).distinct().count()
    # Return 0 and print message if it does not exist
    else:
        print(f"Column `{input_column}` does not exist. Returning `0`")
        return 0
        
distinct_count(rescue, "column_that_does_not_exist")
```
````

````{tabs}
```{code-tab} plaintext Python Output
Column `column_that_does_not_exist` does not exist. Returning `0`
0
```
````
</details>


<details>
<summary><b>R Example</b></summary>

Define an R function in the usual way:

````{tabs}
```{code-tab} r R
distinct_count <- function(sdf, input_column){
    tryCatch(
        expr = {
            # Get the distinct count of input_column
            return(
                sdf %>%
                    sparklyr::select(all_of(input_column)) %>%
                    sparklyr::distinct() %>%
                    sparklyr::sdf_nrow()
            )},
        error = function(e){
            # If the column does not exist, return 0 and print out a message
            #    rather than raise an exception
            if(grepl("Can't subset columns that don't exist", e, fixed=TRUE)){
                print(paste("Column",
                            input_column,
                            "does not exist. Returning `0`",
                           sep = " "))
                return(0)
            # If the error is anything else, return the original error message
            }else{stop(e)}            
        }
    )
}
```
````

Try one column which exists and one which does not:

````{tabs}
```{code-tab} r R
incident_count <- distinct_count(rescue, "IncidentNumber")
incident_count
```
````

````{tabs}
```{code-tab} plaintext R Output
[1] 5898
```
````

````{tabs}
```{code-tab} r R
zero_count <- distinct_count(rescue, "column_that_does_not_exist")
zero_count  
```
````

````{tabs}
```{code-tab} plaintext R Output
[1] "Column column_that_does_not_exist does not exist. Returning `0`"
[1] 0
```
````

A better way would be to avoid the error in the first place by checking if the column exists:

````{tabs}
```{code-tab} r R
distinct_count <- function(sdf, input_column){
    col_names <- sdf %>% colnames()
    # See if the column exists
    if(input_column %in% col_names){
        return(
                # Get the distinct count of input_column
                sdf %>%
                    sparklyr::select(all_of(input_column)) %>%
                    sparklyr::distinct() %>%
                    sparklyr::sdf_nrow()
            )
    }else{
        # If the column does not exist, return 0 and print out a message
        print(paste("Column",
                    input_column,
                    "does not exist. Returning `0`",
                    sep = " "))
        return(0)
    }
}

zero_count <- distinct_count(rescue, "column_that_does_not_exist")
zero_count
```
````

````{tabs}
```{code-tab} plaintext R Output
[1] "Column column_that_does_not_exist does not exist. Returning `0`"
[1] 0
```
````

</details>

### Clean up options

It is worth briefly mentioning the `finally` clause which exists in both Python and R.

In Python, `finally` is added at the end of a `try`/`except` block. This is where clean up code which will always be ran regardless of the outcome of the `try`/`except`. See [Defining Clean Up Action](https://docs.python.org/3/tutorial/errors.html#defining-clean-up-actions) for more information.

The `tryCatch()` function in R has two other options:
- `warning`: Used to handle warnings; the usage is the same as `error`
- `finally`: This is code that will be ran regardless of any errors, often used for clean up if needed

### Further Resources

Spark at the ONS Articles:
- [Checkpointing and Staging Tables](../raw-notebooks/checkpoint-staging/checkpoint-staging)
- [Ideas for optimising Spark code](../spark-concepts/optimisation-tips)
- [Getting Started with Spark](../spark-overview/spark-start)

PySpark Documentation:
- [`spark.read.parquet()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.parquet.html)
- [`spark.read.csv`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.csv.html)
- [`pyspark.sql.utils`](https://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/utils.html): source code for `AnalysisException`
- [`.distinct()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.distinct.html)

Python Documentation:
- [Errors](https://docs.python.org/3/tutorial/errors.html)
- [Base Exceptions](https://docs.python.org/3/library/exceptions.html#bltin-exceptions)
- [Exception Chaining](https://docs.python.org/3/tutorial/errors.html#exception-chaining)
- [Logging](https://docs.python.org/3/library/logging.html)
- [`str.find()`](https://docs.python.org/3/library/stdtypes.html#str.find)

sparklyr and tidyverse Documentation:
- [`spark_read_parquet()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark_read_parquet.html)
- [`spark_read_csv()`](https://spark.rstudio.com/packages/sparklyr/latest/reference/spark_read_csv.html)
- [Advanced R](https://adv-r.hadley.nz/)
    - [Handling Conditions](https://adv-r.hadley.nz/conditions.html#handling-conditions)

Other Links:
- [Stack Overflow](https://stackoverflow.com/)
- [Py4J Protocol](https://www.py4j.org/py4j_java_protocol.html): Details of Py4J Protocal errors
