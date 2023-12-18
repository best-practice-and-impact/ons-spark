## Calculating Cramér's V from a Spark DataFrame

### What is Cramér's V?
Cramér's V is a statistical measure of an association between two nominal variables, giving a value between 0 and 1 inclusive. Here 0 would indicate no association and 1 indicates a strong association between the two variables. It is based on Pearson's chi-square statistics.

We calculate Cramér's V as follows:

$$ \text{Cramer's V} = \sqrt{\dfrac{\dfrac{\chi^2}{n}}{\min (c-1,r-1)}}, $$ 
where:
- $\chi^2$ is the Chi-squared statistic,
- $n$ is the number of samples,
- $r$ is the number of rows,
- $c$ is the number of columns.

In some literature you may see the Phi coefficient used ($\phi$), where $\phi^2 = \chi^2/n$.

### Cramér's V in Spark:
Although there is not an in built method for calculating this statistic in base python, is it reasonably straightforward using `numpy` and `scipy.stats` packages. An example of this can be found [online here](https://www.statology.org/cramers-v-in-python/).
A similar example for R is linked [here](https://www.statology.org/cramers-v-in-r/).

To calculate the Cramér V statistic, we will need to first calculate the $\chi^2$ statistic. In python we will utilise `scipy.stats.chi2_contingency` and `chisq.test` in R. Both these functions will take a matrix like input of a contingency table / pair-wise frequency table. Both Pyspark and SparklyR have inbuilt functions which can produce these tables (`crosstab`/`sdf_crosstab`) as we will see shortly.

Due to Pyspark and SparklyR's differences to classical python and R, we need to consider how we can calculate Cramér's V when using Spark DataFrames.
First we will import the needed packages, start a spark session and load the rescue data. 
````{tabs}
```{code-tab} py
import yaml
import numpy as np
from pyspark.sql import SparkSession, functions as F
import scipy.stats as stats


spark = (SparkSession.builder.master("local[2]")
         .appName("cramer-v")
         .getOrCreate())


with open("../../../config.yaml") as f:
    config = yaml.safe_load(f)
    

rescue_path = config["rescue_clean_path"]
rescue = spark.read.parquet(rescue_path)

rescue = (rescue.withColumnRenamed('animal_group','animal_type')
                .withColumnRenamed('postcodedistrict','postcode_district')
          )

```

```{code-tab} r R

library(sparklyr)
library(dplyr)

default_config <- sparklyr::spark_config()

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "cramer-v",
    config = default_config)

config <- yaml::yaml.load_file("ons-spark/config.yaml")
rescue <- sparklyr::spark_read_parquet(sc, config$rescue_clean_path, header=TRUE, infer_schema=TRUE)
rescue <- rescue %>% dplyr::rename(
                    animal_type = animal_group,
                    postcode_district = postcodedistrict)


```
````
As Cramér V is a measure of how two variables are associated, it makes sense for us to select two variables which we either believe will or will not have some level of association. For our first example we will select the `cal_year` and `animal_type` columns. following this we will compare `postcode_district` and `animal_type`.

#### Cramér's V Example 1: `cal_year` and `animal_type`

Using either `.crosstab()` or `sdf_crosstab()`, we can calculate a pair-wise frequency table of the `id` and `value` columns (a.k.a. contingency table). We will generate this table and convert it to a pandas DataFrame.
````{tabs}
```{code-tab} py
freq_spark = rescue.crosstab('cal_year','animal_type')
freq_pandas = freq_spark.toPandas()
freq_pandas.head()
```

```{code-tab} r R

freq_spark <- sparklyr::sdf_crosstab(rescue, 'cal_year', 'animal_type') 
glimpse(freq_spark)

```
````

````{tabs}

```{code-tab} plaintext Python Output
  cal_year_animal_type  Bird  Budgie  Bull  Cat  Cow  Deer  Dog  Ferret  Fish  \
0                 2014   110       0     0  298    1     5   90       0     0   
1                 2013    85       0     0  313    0     7   93       0     0   
2                 2018   126       1     0  305    0    16   91       1     0   
3                 2015   106       0     0  263    0     6   88       1     0   
4                 2011   120       1     0  309    0     9  103       2     0   

           ...            Pigeon  Rabbit  Sheep  Snake  Squirrel  Tortoise  \
0          ...                 1       3      0      0         2         0   
1          ...                 1       0      0      2         7         0   
2          ...                 1       2      0      1         5         0   
3          ...                 0       0      1      0         5         1   
4          ...                 0       0      0      0         4         0   

   Unknown - Animal Rescue From Water - Farm Animal  \
0                                                 1   
1                                                 0   
2                                                 0   
3                                                 0   
4                                                 0   

   Unknown - Domestic Animal Or Pet  Unknown - Heavy Livestock Animal  \
0                                29                                 0   
1                                19                                 2   
2                                 7                                 2   
3                                29                                 0   
4                                13                                 8   

   Unknown - Wild Animal  
0                      7  
1                     12  
2                      5  
3                      7  
4                      0  

[5 rows x 27 columns]
```

```{code-tab} plaintext R Output
Rows: ??
Columns: 27
Database: spark_connection
$ cal_year_animal_type                               <chr> "2014", "2013", "20…
$ Bird                                               <dbl> 110, 85, 126, 106, …
$ Budgie                                             <dbl> 0, 0, 1, 0, 1, 0, 0…
$ Bull                                               <dbl> 0, 0, 0, 0, 0, 0, 0…
$ Cat                                                <dbl> 298, 313, 305, 263,…
$ Cow                                                <dbl> 1, 0, 0, 0, 0, 3, 0…
$ Deer                                               <dbl> 5, 7, 16, 6, 9, 7, …
$ Dog                                                <dbl> 90, 93, 91, 88, 103…
$ Ferret                                             <dbl> 0, 0, 1, 1, 2, 1, 0…
$ Fish                                               <dbl> 0, 0, 0, 0, 0, 0, 0…
$ Fox                                                <dbl> 22, 25, 33, 21, 26,…
$ Goat                                               <dbl> 1, 0, 0, 0, 0, 0, 0…
$ Hamster                                            <dbl> 1, 3, 0, 0, 3, 0, 0…
$ Hedgehog                                           <dbl> 0, 0, 0, 0, 0, 0, 0…
$ Horse                                              <dbl> 12, 16, 12, 12, 22,…
$ Lamb                                               <dbl> 0, 0, 0, 0, 0, 1, 0…
$ Lizard                                             <dbl> 0, 0, 1, 0, 0, 0, 0…
$ Pigeon                                             <dbl> 1, 1, 1, 0, 0, 0, 0…
$ Rabbit                                             <dbl> 3, 0, 2, 0, 0, 0, 0…
$ Sheep                                              <dbl> 0, 0, 0, 1, 0, 1, 0…
$ Snake                                              <dbl> 0, 2, 1, 0, 0, 0, 0…
$ Squirrel                                           <dbl> 2, 7, 5, 5, 4, 4, 1…
$ Tortoise                                           <dbl> 0, 0, 0, 1, 0, 0, 0…
$ `Unknown - Animal Rescue From Water - Farm Animal` <dbl> 1, 0, 0, 0, 0, 1, 1…
$ `Unknown - Domestic Animal Or Pet`                 <dbl> 29, 19, 7, 29, 13, …
$ `Unknown - Heavy Livestock Animal`                 <dbl> 0, 2, 2, 0, 8, 4, 0…
$ `Unknown - Wild Animal`                            <dbl> 7, 12, 5, 7, 0, 4, …
```
````
Now that we have converted out data into a contingency table, we need to be careful about converting this into a matrix or array type variable. If we do this without considering the `CalYear_animal_type` column, we will end up with some string within our matrix. We would spot this issue when moving forward to use `.chi2_contingency()`, as we would get a `TypeError` raised. We need to find some way of dealing with our column, such that it is not converted when changing our pandas DataFrame into a numpy array. We do this by setting the `cal_year_animal_type` column as the index of our pandas dataframe, as neither the index or column headers are converted into a numpy array, just the internal values are extracted.

A type issue is also raised in R, however we will deal with this slightly differently. Instead of changing the index of our DataFrame, we will simply drop the `cal_year_animal_type` column. This allows us to pass the frequency table directly into the `chisq.test()` function.
````{tabs}
```{code-tab} py
freq_pandas_reindexed = freq_pandas.set_index('cal_year_animal_type')
freq_numpy = np.array(freq_pandas_reindexed)

```

```{code-tab} r R

freq_r <- freq_spark %>% collect() 
freq_r <- subset(freq_r, select = -cal_year_animal_type)


```
````
As we are wanting to calculate Cramér's V for two examples, we will define a function to perform the $\chi^2$ test and calculate the statistic. We only need the $\chi^2$ statistic, as such we extract this from the `chi2_contingency`/`chisq.test` functions using `[0]` in python and `$statistic` in R. `chisq.test` has additional text included in the output, to remove this we cast this output as a double, keeping only the numerical value.
````{tabs}
```{code-tab} py
def get_cramer_v(freq_numpy):
    """
    Returns the Cramer's V Statistic for the given pair-wise frequency table

    Parameters
    ----------
    freq_numpy : np.array
        pair-wise frequency table / contingency table as a numpy array. 

    Returns
    -------
    cramer_v_statistic :  float
        the Cramer's V statistic for the given contingency table
    """
    # Chi-squared test statistic, sample size, and minimum of rows and columns
    chi_sqrd = stats.chi2_contingency(freq_numpy, correction=False)[0]
    n = np.sum(freq_numpy)
    min_dim = min(freq_numpy.shape)-1

    #calculate Cramer's V 
    cramer_v_statistic = np.sqrt((chi_sqrd/n) / min_dim)
    return cramer_v_statistic
```

```{code-tab} r R

get_cramer_v <- function(freq_r){
  # Get cramer V statistic
  # Returns the Cramer's V Statistic for the given
  # pair-wise frequency table
  # 
  # Param: 
  # freq_r: dataframe. pair-wise frequency / contingency table not containing strings
  #
  # Return:
  # cramer_v_statistic: numeric. The Cramer's V statistic 
  # 

  # Chi-squared test statistic, sample size, and minimum of rows and columns
  chi_sqrd <- chisq.test(freq_r, correct = FALSE)$statistic
  chi_sqrd <- as.double(chi_sqrd)
  n <- sum(freq_r)
  min_dim <- min(dim(freq_r)) - 1

  # calculate Cramer's V 
  cramer_v_statistic <- sqrt((chi_sqrd/n) / min_dim)
  return(cramer_v_statistic)
}

```
````
Following the preprocessing and consideration of column names, we can now apply the `get_cramer_v()` function to our arrays 
````{tabs}
```{code-tab} py
get_cramer_v(freq_numpy)
```

```{code-tab} r R

get_cramer_v(freq_r)

```
````

````{tabs}

```{code-tab} plaintext Python Output
0.086908868435997877
```

```{code-tab} plaintext R Output
[1] 0.08690887
```
````
We now get an Cramér V of 0.0869, telling us that there is little to no association between `cal_year` and `animal_type`.
This example also validates both Python and R methods against each other, as we get identical outputs.

#### Cramér's V Example 2: `postcode_district` and `animal_type`

Using the earlier defined function, just need to repeat the cross tabbing exercise before applying our function. This time we will select `postcode_district` instead of `cal_year`.
````{tabs}
```{code-tab} py
freq_spark = rescue.crosstab('postcode_district','animal_type')
freq_pandas = freq_spark.toPandas()
freq_pandas_reindexed = freq_pandas.set_index('postcode_district_animal_type')
freq_numpy = np.array(freq_pandas_reindexed)
get_cramer_v(freq_numpy)
```

```{code-tab} r R

freq_spark <- sparklyr::sdf_crosstab(rescue, 'postcode_district', 'animal_type') 
freq_r <- freq_spark %>% collect() 
freq_r <- subset(freq_r, select = -postcode_district_animal_type)
get_cramer_v(freq_r)


```
````

````{tabs}

```{code-tab} plaintext Python Output
0.29083878195595769
```

```{code-tab} plaintext R Output
[1] 0.2908388
```
````
This time we get a cramér V value of 0.29, suggesting a slight association between `postcode_district` and `animal_type`.

### Potential Issue with `.crosstab` or `sdf_crosstab()`
During the testing for this page, we noted a few common issues which may arise when attempting to calculate Cramér's V statistic. Specifically this issue is linked to `.crosstab` and `sdf_crosstab()` functions for data which has not been fully pre-processed.

For this example we take the unprocessed rescue dataset. Here in `animal_type`, we have values for cat with both a upper and lower case first letter. Here we will use the `tryCatch()` function to handle the error message, this is similar to pythons `try: except:` statements (For more info see [`Error handing in R`](https://cran.r-project.org/web/packages/tryCatchLog/vignettes/tryCatchLog-intro.html) or [`Errors and Exceptions (Python)`](https://docs.python.org/3/tutorial/errors.html)).


````{tabs}
```{code-tab} py
import traceback

rescue_path_csv = config["rescue_path_csv"]
rescue_raw = spark.read.csv(rescue_path_csv, header=True, inferSchema=True)

rescue_raw = (rescue_raw.withColumnRenamed('AnimalGroupParent','animal_type')
                .withColumnRenamed('IncidentNumber','incident_number')
                .withColumnRenamed('CalYear','cal_year')
          )

try:
      rescue_raw.crosstab('cal_year','animal_type')
except Exception:
  print(traceback.format_exc())
```

```{code-tab} r R

rescue_raw <- sparklyr::spark_read_csv(sc, config$rescue_path_csv, header=TRUE, infer_schema=TRUE)
rescue_raw <- rescue_raw %>%
    dplyr::rename(
        incident_number = IncidentNumber,
        animal_type = AnimalGroupParent,
        cal_year = CalYear)

tryCatch(
  {
    sparklyr::sdf_crosstab(rescue_raw, 'cal_year', 'animal_type')
  },
  error = function(e){
    message('Error message from Spark')
    print(e)
  }
)

```
````

````{tabs}

```{code-tab} plaintext Python Output
Traceback (most recent call last):
  File "/opt/cloudera/parcels/CDH/lib/spark/python/pyspark/sql/utils.py", line 63, in deco
    return f(*a, **kw)
  File "/usr/local/lib/python3.6/dist-packages/py4j/protocol.py", line 328, in get_return_value
    format(target_id, ".", name), value)
py4j.protocol.Py4JJavaError: An error occurred while calling o479.crosstab.
: org.apache.spark.sql.AnalysisException: Reference 'Cat' is ambiguous, could be: Cat, Cat.;
	at org.apache.spark.sql.catalyst.expressions.package$AttributeSeq.resolve(package.scala:259)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.resolveQuoted(LogicalPlan.scala:121)
	at org.apache.spark.sql.Dataset.resolve(Dataset.scala:221)
	at org.apache.spark.sql.Dataset.col(Dataset.scala:1268)
	at org.apache.spark.sql.DataFrameNaFunctions.org$apache$spark$sql$DataFrameNaFunctions$$fillCol(DataFrameNaFunctions.scala:443)
	at org.apache.spark.sql.DataFrameNaFunctions$$anonfun$7.apply(DataFrameNaFunctions.scala:502)
	at org.apache.spark.sql.DataFrameNaFunctions$$anonfun$7.apply(DataFrameNaFunctions.scala:492)
	at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
	at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
	at scala.collection.IndexedSeqOptimized$class.foreach(IndexedSeqOptimized.scala:33)
	at scala.collection.mutable.ArrayOps$ofRef.foreach(ArrayOps.scala:186)
	at scala.collection.TraversableLike$class.map(TraversableLike.scala:234)
	at scala.collection.mutable.ArrayOps$ofRef.map(ArrayOps.scala:186)
	at org.apache.spark.sql.DataFrameNaFunctions.fillValue(DataFrameNaFunctions.scala:492)
	at org.apache.spark.sql.DataFrameNaFunctions.fill(DataFrameNaFunctions.scala:179)
	at org.apache.spark.sql.DataFrameNaFunctions.fill(DataFrameNaFunctions.scala:163)
	at org.apache.spark.sql.DataFrameNaFunctions.fill(DataFrameNaFunctions.scala:140)
	at org.apache.spark.sql.execution.stat.StatFunctions$.crossTabulate(StatFunctions.scala:225)
	at org.apache.spark.sql.DataFrameStatFunctions.crosstab(DataFrameStatFunctions.scala:215)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
	at py4j.Gateway.invoke(Gateway.java:282)
	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
	at py4j.commands.CallCommand.execute(CallCommand.java:79)
	at py4j.GatewayConnection.run(GatewayConnection.java:238)
	at java.lang.Thread.run(Thread.java:745)


During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "<ipython-input-13-6eb2220a558e>", line 12, in <module>
    rescue_raw.crosstab('cal_year','animal_type')
  File "/opt/cloudera/parcels/CDH/lib/spark/python/pyspark/sql/dataframe.py", line 1945, in crosstab
    return DataFrame(self._jdf.stat().crosstab(col1, col2), self.sql_ctx)
  File "/usr/local/lib/python3.6/dist-packages/py4j/java_gateway.py", line 1257, in __call__
    answer, self.gateway_client, self.target_id, self.name)
  File "/opt/cloudera/parcels/CDH/lib/spark/python/pyspark/sql/utils.py", line 69, in deco
    raise AnalysisException(s.split(': ', 1)[1], stackTrace)
pyspark.sql.utils.AnalysisException: "Reference 'Cat' is ambiguous, could be: Cat, Cat.;"
```

```{code-tab} plaintext R Output
<simpleError: org.apache.spark.sql.AnalysisException: Reference 'Cat' is ambiguous, could be: Cat, Cat.;
	at org.apache.spark.sql.catalyst.expressions.package$AttributeSeq.resolve(package.scala:259)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.resolveQuoted(LogicalPlan.scala:121)
	at org.apache.spark.sql.Dataset.resolve(Dataset.scala:221)
	at org.apache.spark.sql.Dataset.col(Dataset.scala:1268)
	at org.apache.spark.sql.DataFrameNaFunctions.org$apache$spark$sql$DataFrameNaFunctions$$fillCol(DataFrameNaFunctions.scala:443)
	at org.apache.spark.sql.DataFrameNaFunctions$$anonfun$7.apply(DataFrameNaFunctions.scala:502)
	at org.apache.spark.sql.DataFrameNaFunctions$$anonfun$7.apply(DataFrameNaFunctions.scala:492)
	at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
	at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
	at scala.collection.IndexedSeqOptimized$class.foreach(IndexedSeqOptimized.scala:33)
	at scala.collection.mutable.ArrayOps$ofRef.foreach(ArrayOps.scala:186)
	at scala.collection.TraversableLike$class.map(TraversableLike.scala:234)
	at scala.collection.mutable.ArrayOps$ofRef.map(ArrayOps.scala:186)
	at org.apache.spark.sql.DataFrameNaFunctions.fillValue(DataFrameNaFunctions.scala:492)
	at org.apache.spark.sql.DataFrameNaFunctions.fill(DataFrameNaFunctions.scala:179)
	at org.apache.spark.sql.DataFrameNaFunctions.fill(DataFrameNaFunctions.scala:163)
	at org.apache.spark.sql.DataFrameNaFunctions.fill(DataFrameNaFunctions.scala:140)
	at org.apache.spark.sql.execution.stat.StatFunctions$.crossTabulate(StatFunctions.scala:225)
	at org.apache.spark.sql.DataFrameStatFunctions.crosstab(DataFrameStatFunctions.scala:215)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at sparklyr.Invoke.invoke(invoke.scala:161)
	at sparklyr.StreamHandler$$anonfun$handleMethodCall$1.apply$mcVI$sp(stream.scala:130)
	at sparklyr.StreamHandler$$anonfun$handleMethodCall$1.apply(stream.scala:128)
	at sparklyr.StreamHandler$$anonfun$handleMethodCall$1.apply(stream.scala:128)
	at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
	at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
	at scala.collection.immutable.Range.foreach(Range.scala:160)
	at scala.collection.TraversableLike$class.map(TraversableLike.scala:234)
	at scala.collection.AbstractTraversable.map(Traversable.scala:104)
	at sparklyr.StreamHandler.handleMethodCall(stream.scala:128)
	at sparklyr.StreamHandler.read(stream.scala:62)
	at sparklyr.BackendHandler$$anonfun$channelRead0$1.apply$mcV$sp(handler.scala:60)
	at scala.util.control.Breaks.breakable(Breaks.scala:38)
	at sparklyr.BackendHandler.channelRead0(handler.scala:40)
	at sparklyr.BackendHandler.channelRead0(handler.scala:14)
	at io.netty.channel.SimpleChannelInboundHandler.channelRead(SimpleChannelInboundHandler.java:105)
	at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:374)
	at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:360)
	at io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:352)
	at io.netty.handler.codec.MessageToMessageDecoder.channelRead(MessageToMessageDecoder.java:102)
	at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:374)
	at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:360)
	at io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:352)
	at io.netty.handler.codec.ByteToMessageDecoder.fireChannelRead(ByteToMessageDecoder.java:328)
	at io.netty.handler.codec.ByteToMessageDecoder.channelRead(ByteToMessageDecoder.java:302)
	at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:374)
	at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:360)
	at io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:352)
	at io.netty.channel.DefaultChannelPipeline$HeadContext.channelRead(DefaultChannelPipeline.java:1422)
	at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:374)
	at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:360)
	at io.netty.channel.DefaultChannelPipeline.fireChannelRead(DefaultChannelPipeline.java:931)
	at io.netty.channel.nio.AbstractNioByteChannel$NioByteUnsafe.read(AbstractNioByteChannel.java:163)
	at io.netty.channel.nio.NioEventLoop.processSelectedKey(NioEventLoop.java:700)
	at io.netty.channel.nio.NioEventLoop.processSelectedKeysOptimized(NioEventLoop.java:635)
	at io.netty.channel.nio.NioEventLoop.processSelectedKeys(NioEventLoop.java:552)
	at io.netty.channel.nio.NioEventLoop.run(NioEventLoop.java:514)
	at io.netty.util.concurrent.SingleThreadEventExecutor$6.run(SingleThreadEventExecutor.java:1044)
	at io.netty.util.internal.ThreadExecutorMap$2.run(ThreadExecutorMap.java:74)
	at io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)
	at java.lang.Thread.run(Thread.java:745)
>
```
````
From the error message, we can see that `Cat` is ambigious. When we look closer at the distinct values within the `animal_type` column, we will see there is both `cat` and `Cat` present. This is something to be aware of if you wish to use either crosstab function in the future.

*Note* the exact error message you recive may depend on the version of Pyspark or sparklyR you are using, but this still relates to values within the `animal_type` column as mentioned above.
````{tabs}
```{code-tab} py
rescue_raw.select('animal_type').distinct().orderBy('animal_type',ascending = True).show(27)
```

```{code-tab} r R

rescue_raw %>% sparklyr::select(animal_type) %>% distinct() %>% print(n=27)

```
````

````{tabs}

```{code-tab} plaintext Python Output
+--------------------+
|         animal_type|
+--------------------+
|                Bird|
|              Budgie|
|                Bull|
|                 Cat|
|                 Cow|
|                Deer|
|                 Dog|
|              Ferret|
|                Fish|
|                 Fox|
|                Goat|
|             Hamster|
|            Hedgehog|
|               Horse|
|                Lamb|
|              Lizard|
|              Pigeon|
|              Rabbit|
|               Sheep|
|               Snake|
|            Squirrel|
|            Tortoise|
|Unknown - Animal ...|
|Unknown - Domesti...|
|Unknown - Heavy L...|
|Unknown - Wild An...|
|                 cat|
+--------------------+
```

```{code-tab} plaintext R Output
# Source: spark<?> [?? x 1]
   animal_type                                     
   <chr>                                           
 1 Bird                                            
 2 Fish                                            
 3 Lizard                                          
 4 Snake                                           
 5 Unknown - Heavy Livestock Animal                
 6 Lamb                                            
 7 Unknown - Animal rescue from water - Farm animal
 8 Ferret                                          
 9 Horse                                           
10 Fox                                             
11 Unknown - Domestic Animal Or Pet                
12 Bull                                            
13 Hamster                                         
14 Sheep                                           
15 cat                                             
16 Budgie                                          
17 Deer                                            
18 Cat                                             
19 Cow                                             
20 Dog                                             
21 Tortoise                                        
22 Goat                                            
23 Squirrel                                        
24 Unknown - Wild Animal                           
25 Rabbit                                          
26 Pigeon                                          
27 Hedgehog                                        
```
````
This is something to consider when attempting to calculate Cramér's V or a $\chi^2$ statistic from a spark dataframe.

### Further resources

PySpark Documentation:
- [`.crosstab()`](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.crosstab.html)

Python Documentation:
- [`chi2_contingency()`](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.chi2_contingency.html)
- [`array()`](https://numpy.org/doc/stable/reference/generated/numpy.array.html)
- [`Errors and Exceptions`](https://docs.python.org/3/tutorial/errors.html)

sparklyr Documentation:
- [`sdf_crosstab()`](https://www.rdocumentation.org/packages/sparklyr/versions/1.3.1/topics/sdf_crosstab)

R Documention:
- [`chisq.test()`](https://www.rdocumentation.org/packages/stats/versions/3.6.2/topics/chisq.test)
- [`Error handing in R`](https://cran.r-project.org/web/packages/tryCatchLog/vignettes/tryCatchLog-intro.html)
