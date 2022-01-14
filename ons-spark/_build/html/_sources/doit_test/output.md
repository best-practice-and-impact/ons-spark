## Sampling (code tabs version)

Sampling: `.sample()` and `sdf_sample()`

You can take a sample of a DataFrame with [`.sample()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.sample) in PySpark or [`sdf_sample()`](https://spark.rstudio.com/reference/sdf_sample.html) in sparklyr. This is something that you may want to do during development or initial analysis of data, as with a smaller amount of data your code will run faster and requires less memory to process.

It is important to note that sampling in Spark returns an approximate fraction of the data, rather than an exact one. The reason for this is explained in the [Returning an exact sample](#returning-an-exact-sample) section.

### Example: `.sample()` and `sdf_sample()`

First, set up the Spark session, read the Animal Rescue data, and then get the row count:

````{tabs}

```{code-tab} py
x = 10
abc = 200000


```
```{code-tab} r R
a <- 5


```
````
  
### This is a test header

some test text
some more test text
even more test text
  
````{tabs}

```{code-tab} py
y = 20
```
```{code-tab} r R
b <- 6
c <- 7
```
````
  
end of text