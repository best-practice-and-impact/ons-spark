## Date functions

Handling dates is tricky in most programming languages, and Spark is no exception. This article gives examples of a few date functions, including interval, which is not well documented in PySpark. This article was written in December and so we have decided to make our examples festive!Note that this is far from exhaustive; see the links in Further Resources for more information which explain dates in more detail. To demonstrate the date functions we first need to start a Spark session and read in the Animal Rescue data:

````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[2]").appName("date-functions").getOrCreate()

rescue = (spark.read.csv("/training/animal_rescue.csv", header=True, inferSchema=True)
          .withColumnRenamed("IncidentNumber", "incident_number")
          .withColumnRenamed("DateTimeOfCall", "date_time_of_call")
          .select("incident_number", "date_time_of_call"))

rescue.limit(5).toPandas()
```
```{code-tab} r R

library(sparklyr)
library(dplyr)

default_config <- sparklyr::spark_config()

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "date-functions",
    config = default_config)

rescue <- sparklyr::spark_read_csv(
    sc,
    path="/training/animal_rescue.csv", 
    header=TRUE,
    infer_schema=TRUE) %>%
    rename(
        incident_number = IncidentNumber,
        date_time_of_call = DateTimeOfCall,
       )  %>%
    select(incident_number, date_time_of_call)

rescue
```
````
````{tabs}
```{code-tab} plaintext Python Output
  incident_number date_time_of_call
0          139091  01/01/2009 03:01
1          275091  01/01/2009 08:51
2         2075091  04/01/2009 10:07
3         2872091  05/01/2009 12:27
4         3553091  06/01/2009 15:23
```

```{code-tab} plaintext R Output
# Source: spark<?> [?? x 2]
  incident_number date_time_of_call
  <chr>           <chr>            
1 139091          01/01/2009 03:01 
2 275091          01/01/2009 08:51 
3 2075091         04/01/2009 10:07 
4 2872091         05/01/2009 12:27 
5 3553091         06/01/2009 15:23 
```
````
The `date_time_of_call` column contains dates and times, but is actually a string. From the R output we can check that under the column name the data type is `char`,
which stands for `character`  and is used for storing string or character values in a variable. To get the same insights in `PySpark` we need to print the schema:

````{tabs}
```{code-tab} py
rescue.printSchema()
```
````

````{tabs}
```{code-tab} plaintext Python Output
root
 |-- incident_number: string (nullable = true)
 |-- date_of_call: date (nullable = true)
```
````

### Convert string to date with `to_date()`

Before using any date functions we want to change the `date_time_of_call` column to a `date` type, omitting the time, using [`F.to_date()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.functions.to_date).

 This has two arguments, the name of the column and the date format, `dd/MM/yyyy` in this example:

````{tabs}
```{code-tab} py
rescue = rescue.withColumn("date_of_call", F.to_date("date_time_of_call", "dd/MM/yyyy")).drop("date_time_of_call")
rescue.limit(5).toPandas()
```
```{code-tab} r R
rescue <- rescue %>% mutate(date_of_call = to_date(date_time_of_call, "dd/MM/yyyy"))  %>% 
      select(-c(date_time_of_call))

print(rescue, n = 5)
```
````
````{tabs}
```{code-tab} plaintext Python Output
  incident_number date_of_call
0          139091   2009-01-01
1          275091   2009-01-01
2         2075091   2009-01-04
3         2872091   2009-01-05
4         3553091   2009-01-06
```
```{code-tab} plaintext R Output
  incident_number date_of_call
  <chr>           <date>      
1 139091          2009-01-01  
2 275091          2009-01-01  
3 2075091         2009-01-04  
4 2872091         2009-01-05  
5 3553091         2009-01-06  
```
````
From the R output we can confirm that indeed `date_of_call` column is a `date` data type. For python we must print again the schema to confirm that.

````{tabs}
```{code-tab} py
rescue.printSchema()
```
````
````{tabs}
```{code-tab} plaintext Python Output
root
 |-- incident_number: string (nullable = true)
 |-- date_of_call: date (nullable = true)

```
````
### Format dates with `F.date_format()`


````{tabs}
```{code-tab} py
rescue = rescue.withColumn("call_month_day", F.date_format("date_of_call", "MMM-dd"))

rescue.limit(5).toPandas()
```
```{code-tab} r R
rescue <- rescue %>%
  mutate(call_month_day = date_format(date_of_call, "MMM-dd"))

print(rescue, n = 5)
```
````

````{tabs}
```{code-tab} plaintext Python Output
  incident_number date_of_call call_month_day
0          139091   2009-01-01         Jan-01
1          275091   2009-01-01         Jan-01
2         2075091   2009-01-04         Jan-04
3         2872091   2009-01-05         Jan-05
4         3553091   2009-01-06         Jan-06
```
```{code-tab} plaintext R Output
 incident_number date_of_call call_month_day
  <chr>           <date>       <chr>         
1 139091          2009-01-01   Jan-01        
2 275091          2009-01-01   Jan-01        
3 2075091         2009-01-04   Jan-04        
4 2872091         2009-01-05   Jan-05        
5 3553091         2009-01-06   Jan-06        
```
````
 If only one component of the date is required then there are specific functions: `F.year()`, `F.month()` and `F.dayofmonth()`; in theory these should be more efficient than `F.date_format()`, although less elegant to write if being combined. For `sparklyr` we can achieve the same result by applying `year()`, `month()`, `dayofmonth()` with `mutate()`.
 Now create a new DataFrame containing some festive days:

````{tabs}
```{code-tab} py
festive_days = spark.createDataFrame([
    ["Dec-24", "Christmas Eve"],
    ["Dec-25", "Christmas Day"],
    ["Dec-26", "Boxing Day"],
    ["Jan-01", "New Years Day"]],
    ["month_day", "festive_day"])

festive_days.toPandas()
```
```{code-tab} r R

festive_days <- data.frame(
  "month_day" = c("Dec-24","Dec-25","Dec-26","Jan-01"),
  "festive_day" =c("Christmas Eve","Christmas Day","Boxing Day","New Years Day"))

festive_days <- copy_to(sc,festive_days)

festive_days
```
````

````{tabs}
```{code-tab} plaintext Python Output
  month_day    festive_day
0    Dec-24  Christmas Eve
1    Dec-25  Christmas Day
2    Dec-26     Boxing Day
3    Jan-01  New Years Day
```
```{code-tab} plaintext R Output
  month_day festive_day  
  <chr>     <chr>        
1 Dec-24    Christmas Eve
2 Dec-25    Christmas Day
3 Dec-26    Boxing Day   
4 Jan-01    New Years Day
```
````

Use an inner join to return a list of rescue incidents on these festive days:

````{tabs}
```{code-tab} py
 festive_rescues = (rescue
                   .join(festive_days, on=rescue.call_month_day == festive_days.month_day, how="inner")
                   .drop("call_month_day", "month_day"))

festive_rescues.limit(5).toPandas()

```
```{code-tab} r R
festive_rescues <- inner_join(rescue, festive_days,by = c("call_month_day" = "month_day"))  %>% 
  select(-c(call_month_day,call_month_day))
```
````

````{tabs}
```{code-tab} plaintext Python Output
   incident_number date_of_call    festive_day
0  179370-24122018   2018-12-24  Christmas Eve
1  179212-24122018   2018-12-24  Christmas Eve
2  173626-24122016   2016-12-24  Christmas Eve
3        171323141   2014-12-24  Christmas Eve
4        171286141   2014-12-24  Christmas Eve
```
```{code-tab} plaintext R Output
  incident_number date_of_call festive_day  
  <chr>           <date>       <chr>        
1 179370-24122018 2018-12-24   Christmas Eve
2 179212-24122018 2018-12-24   Christmas Eve
3 173626-24122016 2016-12-24   Christmas Eve
4 171323141       2014-12-24   Christmas Eve
5 171286141       2014-12-24   Christmas Eve
```
````
We can now get the number of animal rescue incidents on each festive day:
````{tabs}
```{code-tab} py
festive_rescues.groupBy("date_of_call", "festive_day").count().orderBy("date_of_call").limit(10).toPandas()

```
```{code-tab} r R
festive_rescues %>% 
  group_by(date_of_call,festive_day) %>%
  summarize(count = n()) %>% 
  arrange(date_of_call) %>%
  print(n = 10)
```
````

````{tabs}
```{code-tab} plaintext Python Output
 date_of_call    festive_day  count
0   2009-01-01  New Years Day      2
1   2009-12-24  Christmas Eve      1
2   2009-12-25  Christmas Day      1
3   2009-12-26     Boxing Day      2
4   2010-01-01  New Years Day      2
5   2010-12-24  Christmas Eve      3
6   2010-12-25  Christmas Day      1
7   2010-12-26     Boxing Day      3
8   2011-01-01  New Years Day      2
9   2011-12-24  Christmas Eve      2
```
```{code-tab} plaintext R Output
 date_of_call festive_day   count
   <date>       <chr>         <dbl>
 1 2009-01-01   New Years Day     2
 2 2009-12-24   Christmas Eve     1
 3 2009-12-25   Christmas Day     1
 4 2009-12-26   Boxing Day        2
 5 2010-01-01   New Years Day     2
 6 2010-12-24   Christmas Eve     3
 7 2010-12-25   Christmas Day     1
 8 2010-12-26   Boxing Day        3
 9 2011-01-01   New Years Day     2
10 2011-12-24   Christmas Eve     2
```
````

### Add and subtract dates with `interval`

The `interval` function adds or subtracts days, months and years to a date. Despite being a very useful function it is unfortunately poorly documented within PySpark. `interval` is not contained in the PySpark functions module, and needs to be wrapped in [`F.expr()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.functions.expr). The most common format is:

`F.expr("column_name + interval y years m months d days")`

We can apply the same expression in sparklyr by leveraging the [`sql()`](https://dplyr.tidyverse.org/reference/sql.html#:~:text=These%20functions%20are%20critical%20when,and%20return%20an%20sql%20object.) function.

The PySpark functions module contains `F.date_add()`, `F.date_sub()` and `F.add_months()`. These functions are not the easiest to remember as they are inconsistently named, and there is no function for adding years; you can create your own by multiplying `F.add_months()` by `12`. `interval` is easier to use and a better choice in most circumstances.

For instance, if we assume that a report has to be produced for each animal rescue incident within a year and three months, then we can use `interval 1 year 3 months`. The singular `year` is used here rather than `years`; both work correctly regardless of the integer value used.

````{tabs}
```{code-tab} py
festive_rescues.withColumn("report_date", F.expr("date_of_call + interval 1 year 3 months")).limit(5).toPandas()
```
```{code-tab} r R
rescue %>%
  mutate(report_date = sql("date_of_call + interval 1 year 3 months")) %>%
  print(n = 5)
```
````

````{tabs}
```{code-tab} plaintext Python Output
 incident_number date_of_call    festive_day report_date
0  179370-24122018   2018-12-24  Christmas Eve  2020-03-24
1  179212-24122018   2018-12-24  Christmas Eve  2020-03-24
2  173626-24122016   2016-12-24  Christmas Eve  2018-03-24
3        171323141   2014-12-24  Christmas Eve  2016-03-24
4        171286141   2014-12-24  Christmas Eve  2016-03-24
```
```{code-tab} plaintext R Output
  incident_number date_of_call festive_day   report_date
  <chr>           <date>       <chr>         <date>     
1 179370-24122018 2018-12-24   Christmas Eve 2020-03-24 
2 179212-24122018 2018-12-24   Christmas Eve 2020-03-24 
3 173626-24122016 2016-12-24   Christmas Eve 2018-03-24 
4 171323141       2014-12-24   Christmas Eve 2016-03-24 
5 171286141       2014-12-24   Christmas Eve 2016-03-24 
```
````
You can also use negative numbers with interval. As a festive example, in some Orthodox churches in central and eastern Europe Christmas is celebrated 13 days later than in Britain, with Christmas Day on the 7th of January. To calculate how many animals were rescued on the Orthodox festive days without redefining the festive_days DataFrame we can use - interval with 13 days, then join festive days. We do not need to specify months or years as we are only using days. Boxing Day is also being filtered out as it is not widely celebrated outside of the Commonwealth.

````{tabs}
```{code-tab} py
rescue = rescue.withColumn("adjusted_date", F.expr("date_of_call - interval 13 days"))
rescue.limit(5).toPandas()
```
```{code-tab} r R
rescue <- rescue %>%
  mutate(adjusted_date = sql("date_of_call - interval 13 days"))
  
print(rescue, n = 5 )
```
````

````{tabs}
```{code-tab} plaintext Python Output
  incident_number date_of_call call_month_day adjusted_date
0          139091   2009-01-01         Jan-01    2008-12-19
1          275091   2009-01-01         Jan-01    2008-12-19
2         2075091   2009-01-04         Jan-04    2008-12-22
3         2872091   2009-01-05         Jan-05    2008-12-23
4         3553091   2009-01-06         Jan-06    2008-12-24
```
```{code-tab} plaintext R Output
  incident_number date_of_call call_month_day adjusted_date
  <chr>           <date>       <chr>          <date>       
1 139091          2009-01-01   Jan-01         2008-12-19   
2 275091          2009-01-01   Jan-01         2008-12-19   
3 2075091         2009-01-04   Jan-04         2008-12-22   
4 2872091         2009-01-05   Jan-05         2008-12-23   
5 3553091         2009-01-06   Jan-06         2008-12-24   
```
````

````{tabs}
```{code-tab} py
rescue = rescue.withColumn("adjusted_month_day", F.date_format("adjusted_date", "MMM-dd"))

orthodox_festive_rescues = (rescue
                   .join(festive_days, on=rescue.adjusted_month_day == festive_days.month_day, how="inner")
                   .withColumnRenamed("festive_day", "orthodox_festive_day")
                   .filter(F.col("orthodox_festive_day") != "Boxing Day")
                   .drop("call_month_day", "month_day", "adjusted_date", "adjusted_month_day"))

orthodox_festive_rescues.limit(5).toPandas()

```
```{code-tab} r R
rescue <- rescue %>%
  mutate(adjusted_month_day = date_format(adjusted_date, "MMM-dd"))

orthodox_festive_rescues <- inner_join(rescue, festive_days,by = c("adjusted_month_day" = "month_day"))  %>% 
  dplyr::rename(orthodox_festive_day = festive_day)  %>%
  filter(orthodox_festive_day != "Boxing Day") %>%
  select(incident_number ,date_of_call ,orthodox_festive_day)
    
print(orthodox_festive_rescues, n = 5)
```
````

````{tabs}
```{code-tab} plaintext Python Output
   incident_number date_of_call orthodox_festive_day
0  002638-06012019   2019-01-06        Christmas Eve
1  002419-06012019   2019-01-06        Christmas Eve
2  002369-06012018   2018-01-06        Christmas Eve
3  002204-06012018   2018-01-06        Christmas Eve
4  001927-06012016   2016-01-06        Christmas Eve
```
```{code-tab} plaintext R Output
 incident_number date_of_call orthodox_festive_day
  <chr>           <date>       <chr>               
1 002638-06012019 2019-01-06   Christmas Eve       
2 002419-06012019 2019-01-06   Christmas Eve       
3 002369-06012018 2018-01-06   Christmas Eve       
4 002204-06012018 2018-01-06   Christmas Eve       
5 001927-06012016 2016-01-06   Christmas Eve       
```
````
As before, we can get the counts:

````{tabs}
```{code-tab} py
(orthodox_festive_rescues
     .groupBy("date_of_call", "orthodox_festive_day")
     .count()
     .orderBy("date_of_call")
     .limit(10)
     .toPandas())
```
```{code-tab} r R
orthodox_festive_rescues %>% 
  group_by(date_of_call,orthodox_festive_day) %>%
  summarize(count = n()) %>% 
  arrange(date_of_call) %>%
  print(n = 10)
```
````

````{tabs}
```{code-tab} plaintext Python Output
date_of_call orthodox_festive_day  count
0   2009-01-06        Christmas Eve      2
1   2009-01-07        Christmas Day      4
2   2009-01-14        New Years Day      1
3   2010-01-06        Christmas Eve      2
4   2010-01-07        Christmas Day      2
5   2010-01-14        New Years Day      2
6   2011-01-06        Christmas Eve      1
7   2012-01-06        Christmas Eve      1
8   2012-01-07        Christmas Day      2
9   2012-01-14        New Years Day      1
```
```{code-tab} plaintext R Output
   date_of_call orthodox_festive_day count
   <date>       <chr>                <dbl>
 1 2009-01-06   Christmas Eve            2
 2 2009-01-07   Christmas Day            4
 3 2009-01-14   New Years Day            1
 4 2010-01-06   Christmas Eve            2
 5 2010-01-07   Christmas Day            2
 6 2010-01-14   New Years Day            2
 7 2011-01-06   Christmas Eve            1
 8 2012-01-06   Christmas Eve            1
 9 2012-01-07   Christmas Day            2
10 2012-01-14   New Years Day            1
```
````
### `interval` with times and weeks
weeks can be used; note that there is no native PySpark function for adding weeks.Time periods, e.g. hours, can also be used with interval, which may be useful when working with timestamps.As with any function, it is recommended to use unit tests when working with dates. The Pytest for PySpark repository gives some examples of how to unit test when working with PySpark.


### Documentation:
#### PySpark
- [`F.date_format()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.functions.date_format)
- [`F.year()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.functions.year)
- [`F.month()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.functions.month)
- [`F.dayofmonth()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.functions.dayofmonth)
- [`F.expr()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.functions.expr)
- [`F.date_add()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.functions.date_add)
- [`F.date_sub()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.functions.date_sub)
- [`F.add_months()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.functions.add_months)

#### sparklyr

- [`sql()`](https://dplyr.tidyverse.org/reference/sql.html#:~:text=These%20functions%20are%20critical%20when,and%20return%20an%20sql%20object.)
- [sparklyr-functions](../sparklyr-intro/sparklyr-functions)

### Other links:
- [Yammer discussion of `interval`](https://www.yammer.com/ons.gov.uk/#/threads/show?threadId=934654814199808): Diogo Marques gives an explanation and examples of `interval`
- [Java `SimpleDateFormat` class](https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html): details of valid date formats for `F.to_date()` and `F.date_format()`- [`F.to_date()`](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.functions.to_date)
      
### Acknowledgments
  
Special thanks to Diogo Marques for sharing his knowledge of the `interval` function, and to Vicky Pickering for inspiring this festive tip!