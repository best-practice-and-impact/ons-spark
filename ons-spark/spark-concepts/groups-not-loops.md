<div align="center">
  <img src="https://officenationalstatistics.sharepoint.com/:i:/r/sites/itoDSTPMO/DAPCATS/04.%20Technical/02.%20Development_Test/images_for_gitlab/dap-cats-ds-logo.png"/>
</div>

## Groups not Loops

Most of the time, Spark is better at processing one big job than lots of smaller ones. This is because one large Spark job can be processed in parallel, whereas lots of small ones have to be processed in serial; the second job can only start once the first is completely finished. This means that as a general principle you will want to try and avoid using loops in your code if they contain an action.

To refactor your code to avoid loops, you can often group the data instead. This will mean you are calling one action on a larger DataFrame which will almost certainly be quicker than calling several actions on smaller DataFrames.

Let's look at two examples, one simple, using `groupBy()`, and one a little more complex using a window function.

### Replace a loop with `groupBy()`

First, import all the modules needed and create a Spark session:
````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import pandas as pd

spark = (SparkSession.builder.master("local[2]")
         .appName("groups-not-loops")
         .getOrCreate())
```

```{code-tab} r R

library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
  master = "local[2]",
  app_name = "groups-not-loops",
  config = sparklyr::spark_config(),
  )
  

```
````
Read in the data and select and rename the columns used in this example:
````{tabs}
```{code-tab} py
rescue = (spark
          .read.csv("/training/animal_rescue.csv", header=True, inferSchema=True)
          .withColumnRenamed("AnimalGroupParent", "AnimalGroup")
          .withColumnRenamed("IncidentNotionalCost(£)", "TotalCost")
          .withColumnRenamed("FinalDescription", "Description")
          .select("IncidentNumber", "AnimalGroup", "CalYear", "TotalCost", "Description"))

rescue.limit(5).toPandas()
```

```{code-tab} r R

rescue <- sparklyr::spark_read_csv(
  sc,
  path = "/training/animal_rescue.csv",
  header = TRUE,
  infer_schema = TRUE) %>% 
  sparklyr::select(
    AnimalGroup = AnimalGroupParent,
    TotalCost = IncidentNotionalCostGBP,
    Description = FinalDescription,
    CalYear = CalYear,
    IncidentNumber = IncidentNumber)


head(rescue) %>% 
  sparklyr::collect() %>%
  print()
  

```
````

````{tabs}

```{code-tab} plaintext Python Output
  IncidentNumber AnimalGroup  CalYear  TotalCost  \
0         139091         Dog     2009      510.0   
1         275091         Fox     2009      255.0   
2        2075091         Dog     2009      255.0   
3        2872091       Horse     2009      255.0   
4        3553091      Rabbit     2009      255.0   

                                 Description  
0  DOG WITH JAW TRAPPED IN MAGAZINE RACK,B15  
1          ASSIST RSPCA WITH FOX TRAPPED,B15  
2                    DOG CAUGHT IN DRAIN,B15  
3                  HORSE TRAPPED IN LAKE,J17  
4              RABBIT TRAPPED UNDER SOFA,B15  
```

```{code-tab} plaintext R Output
# A tibble: 6 × 5
  AnimalGroup                      TotalCost Description  CalYear IncidentNumber
  <chr>                                <dbl> <chr>          <int> <chr>         
1 Dog                                    510 DOG WITH JA…    2009 139091        
2 Fox                                    255 ASSIST RSPC…    2009 275091        
3 Dog                                    255 DOG CAUGHT …    2009 2075091       
4 Horse                                  255 HORSE TRAPP…    2009 2872091       
5 Rabbit                                 255 RABBIT TRAP…    2009 3553091       
6 Unknown - Heavy Livestock Animal       255 ANIMAL TRAP…    2009 3742091       
```
````
First, we want to get the total number of cats rescued. This is simple to do with `.filter()` and `count()`:
````{tabs}
```{code-tab} py
rescue.filter(F.col("AnimalGroup") == "Cat").count()
```

```{code-tab} r R

rescue %>% 
  sparklyr::filter(AnimalGroup == "Cat") %>%
  count() %>%
  print()


```
````

````{tabs}

```{code-tab} plaintext Python Output
2909
```

```{code-tab} plaintext R Output
# Source: spark<?> [?? x 1]
      n
  <dbl>
1  2909
```
````
Now let's assume that we want to count some other animals: dogs, hamsters and deer.

The temptation here is to look at our existing code, see that we can easily adapt it to include any animal, and put it in a loop, appending the result to a list each time.

In practice, this is a common way that loops end up being written in Spark code; you have some code which works and a loop is seen as the easiest way to generalise it.
````{tabs}
```{code-tab} py
animals = ["Cat", "Dog", "Hamster", "Deer"]

result = []

for animal in animals:
    result.append(rescue.filter(F.col("AnimalGroup") == animal).count())

pdf = pd.DataFrame({"AnimalGroup": animals, "count": result}).sort_values(by=["count"], ascending=False)
pdf
```

```{code-tab} r R

animals = c("Cat", "Dog", "Hamster", "Deer")

result = list()

for (animal in animals) {
    count <- rescue %>%
      sparklyr::filter(AnimalGroup == animal) %>%
      count() %>%
      sparklyr::collect() %>%
      as.numeric()
  
    result <- append(result, count)
}


animal_counts <- data.frame(AnimalGroup = animals, count = unlist(result))

head(animal_counts)


```
````

````{tabs}

```{code-tab} plaintext Python Output
  AnimalGroup  count
0         Cat   2909
1         Dog   1008
3        Deer     94
2     Hamster     14
```

```{code-tab} plaintext R Output
  AnimalGroup count
1         Cat  2909
2         Dog  1008
3     Hamster    14
4        Deer    94
```
````
The issue with this code is the efficiency. `.count()` is being called four times, meaning that Spark will execute the plan for each animal four separate times. Ideally, we want to call as few actions as possible, as this will make our code much more efficient.

Rather than use a loop, we can instead use filter the data using `.isin()`, group the data by `AnimalGroup`, and then count. This will give the same result as above, but by grouping, we are only submitting one action to the cluster, rather than four. This will make the code run much faster, as well as being easier to read.
````{tabs}
```{code-tab} py
(rescue
    .filter(F.col("AnimalGroup").isin(animals))
    .groupBy("AnimalGroup")
    .count()
    .orderBy("count", ascending=False)
    .toPandas()
)
```

```{code-tab} r R

rescue %>%
  sparklyr::filter(AnimalGroup %in% animals) %>%
  dplyr::group_by(AnimalGroup) %>%
  dplyr::summarise(count = n()) %>%
  dplyr::arrange(desc(count)) %>%
  sparklyr::collect() %>%
  print()


```
````

````{tabs}

```{code-tab} plaintext Python Output
  AnimalGroup  count
0         Cat   2909
1         Dog   1008
2        Deer     94
3     Hamster     14
```

```{code-tab} plaintext R Output
# A tibble: 4 × 2
  AnimalGroup count
  <chr>       <dbl>
1 Cat          2909
2 Dog          1008
3 Deer           94
4 Hamster        14
```
````
### Replace a loop with a window function

The example above was relatively simple. Sometimes it's not as straightforward to refactor your loop into a group.

Let's get the three most expensive cats, dogs, hamsters and deer to rescue. We can write a function which does this, and then call it within a loop:
````{tabs}
```{code-tab} py
def get_expensive_incidents(df, animal):
    return(df
        .filter(F.col("AnimalGroup") == animal)
        .select("IncidentNumber", "CalYear", "TotalCost", "Description")
        .orderBy(F.desc("TotalCost"), "IncidentNumber") # use IncidentNumber as a tie-break to ensure determinism
        .limit(3) # just get the top 3
          )
```

```{code-tab} r R

get_expensive_incidents <- function(df, animal) {
  df %>%
    sparklyr::filter(AnimalGroup == animal) %>%
    sparklyr::select(IncidentNumber, CalYear, TotalCost, Description, AnimalGroup) %>%
    dplyr::arrange(desc(TotalCost), IncidentNumber) %>%
    head(3)

}


```
````

````{tabs}
```{code-tab} py
result = []

for animal in animals:
    pdf = get_expensive_incidents(rescue, animal).toPandas()
    pdf["AnimalGroup"] = animal
    result.append(pdf)

pd.concat(result).sort_values(by=["AnimalGroup", "TotalCost"], ascending=[True, False]).reset_index()
```

```{code-tab} r R

result <- list() #create an empty list to store out results from the loop

for (animal in animals) {
  expensive_animal <- get_expensive_incidents(rescue, animal) #apply our function to each animal
  expensive_animal$AnimalGroup <- animal
  result <- append(result, list(expensive_animal)) #add result to list
}

expensive_rescues <- do.call(rbind, result) %>% #convert list into dataframe
  dplyr::arrange(AnimalGroup, desc(TotalCost))

expensive_rescues %>% 
  collect() %>%
  print()


```
````

````{tabs}

```{code-tab} plaintext Python Output
    index   IncidentNumber  CalYear  TotalCost  \
0       0  098141-28072016     2016     3912.0   
1       1         49076141     2014     2655.0   
2       2  028258-08032017     2017     2282.0   
3       0        101755111     2011     2340.0   
4       1  144664-11102018     2018     1998.0   
5       2  174801-27122016     2016     1304.0   
6       0        154461131     2013     2030.0   
7       1  098400-17072018     2018     1665.0   
8       2  033179-19032017     2017     1630.0   
9       0  040568-06042016     2016      652.0   
10      1  050586-29042016     2016      326.0   
11      2  099706-31072016     2016      326.0   

                                          Description AnimalGroup  
0    CAT STUCK WITHIN WALL SPACE  RSPCA IN ATTENDANCE         Cat  
1                           KITTEN TRAPPED IN CHIMNEY         Cat  
2                    ASSIST RSPCA WITH CAT IN CHIMNEY         Cat  
3                DEER IN CANAL - WATER RESCUE LEVEL 2        Deer  
4         DEER ENTANGLED - RSPCA ON SCENE 07969305977        Deer  
5   TWO DEER STUCK IN FENCING RVP OUTSIDE PORTLAND...        Deer  
6                       DOG TRAPPED BETWEEN TWO HOUSE         Dog  
7   PUPPY TRAPPED BETWEEN TWO HOUSES  FRU REQ FROM...         Dog  
8   DOG STUCK ON ISLAND IN LARGE POND WATER RESCUE...         Dog  
9                     HAMSTER STUCK UNDER FLOORBOARDS     Hamster  
10                       HAMSTER STUCK IN CAVITY WALL     Hamster  
11                  HAMSTER TRAPPED UNDER FLOORBOARDS     Hamster  
```

```{code-tab} plaintext R Output
# A tibble: 12 × 5
   IncidentNumber  CalYear TotalCost Description                     AnimalGroup
   <chr>             <int>     <dbl> <chr>                           <chr>      
 1 098141-28072016    2016      3912 CAT STUCK WITHIN WALL SPACE  R… Cat        
 2 49076141           2014      2655 KITTEN TRAPPED IN CHIMNEY       Cat        
 3 028258-08032017    2017      2282 ASSIST RSPCA WITH CAT IN CHIMN… Cat        
 4 101755111          2011      2340 DEER IN CANAL - WATER RESCUE L… Deer       
 5 144664-11102018    2018      1998 DEER ENTANGLED - RSPCA ON SCEN… Deer       
 6 174801-27122016    2016      1304 TWO DEER STUCK IN FENCING RVP … Deer       
 7 154461131          2013      2030 DOG TRAPPED BETWEEN TWO HOUSE   Dog        
 8 098400-17072018    2018      1665 PUPPY TRAPPED BETWEEN TWO HOUS… Dog        
 9 033179-19032017    2017      1630 DOG STUCK ON ISLAND IN LARGE P… Dog        
10 040568-06042016    2016       652 HAMSTER STUCK UNDER FLOORBOARDS Hamster    
11 050586-29042016    2016       326 HAMSTER STUCK IN CAVITY WALL    Hamster    
12 099706-31072016    2016       326 HAMSTER TRAPPED UNDER FLOORBOA… Hamster    
```
````
Like in the previous example, this is calling four actions. To re-write this, we can use a window function. The comments in the function should help explain the process, but for a full understanding please look at [Using Window Functions for Ranking](https://best-practice-and-impact.github.io/ons-spark/spark-functions/window-functions.html#using-window-functions-for-ranking).
````{tabs}
```{code-tab} py
def get_expensive_incidents_grouped(df, animals):
    return(df
        .filter(F.col("AnimalGroup").isin(animals)) # Only include specified animals
        .select("IncidentNumber", "CalYear", "TotalCost", "Description", "AnimalGroup") # Add AnimalGroup to the select
        .withColumn("row_no", F.row_number().over(Window # Rank the animals
                                                .partitionBy("AnimalGroup") # Group by AnimalGroup
                                                .orderBy(F.desc("TotalCost"), "IncidentNumber"))) # Order as before
        .filter(F.col("row_no") <= 3) # Only return the top 3 per group
        .drop("row_no") # Remove the column used in the calculation
          )

(get_expensive_incidents_grouped(rescue, animals)
    .orderBy(["AnimalGroup", "TotalCost"], ascending=[True, False])
    .toPandas()
)
```

```{code-tab} r R

get_expensive_incidents_grouped <- function(df, animals){
  df %>%
    sparklyr::filter(AnimalGroup %in% animals) %>%
    sparklyr::select(IncidentNumber, CalYear, TotalCost, Description, AnimalGroup) %>%
    dplyr::group_by(AnimalGroup) %>%
    dplyr::arrange(desc(TotalCost)) %>%
    sparklyr::filter(row_number() <= 3) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(AnimalGroup, desc(TotalCost), IncidentNumber)
}

expensive_rescues <- get_expensive_incidents_grouped(rescue, animals)

expensive_rescues %>% 
  collect() %>%
  print()


```
````

````{tabs}

```{code-tab} plaintext Python Output
     IncidentNumber  CalYear  TotalCost  \
0   098141-28072016     2016     3912.0   
1          49076141     2014     2655.0   
2   028258-08032017     2017     2282.0   
3         101755111     2011     2340.0   
4   144664-11102018     2018     1998.0   
5   174801-27122016     2016     1304.0   
6         154461131     2013     2030.0   
7   098400-17072018     2018     1665.0   
8   033179-19032017     2017     1630.0   
9   040568-06042016     2016      652.0   
10  050586-29042016     2016      326.0   
11  099706-31072016     2016      326.0   

                                          Description AnimalGroup  
0    CAT STUCK WITHIN WALL SPACE  RSPCA IN ATTENDANCE         Cat  
1                           KITTEN TRAPPED IN CHIMNEY         Cat  
2                    ASSIST RSPCA WITH CAT IN CHIMNEY         Cat  
3                DEER IN CANAL - WATER RESCUE LEVEL 2        Deer  
4         DEER ENTANGLED - RSPCA ON SCENE 07969305977        Deer  
5   TWO DEER STUCK IN FENCING RVP OUTSIDE PORTLAND...        Deer  
6                       DOG TRAPPED BETWEEN TWO HOUSE         Dog  
7   PUPPY TRAPPED BETWEEN TWO HOUSES  FRU REQ FROM...         Dog  
8   DOG STUCK ON ISLAND IN LARGE POND WATER RESCUE...         Dog  
9                     HAMSTER STUCK UNDER FLOORBOARDS     Hamster  
10                       HAMSTER STUCK IN CAVITY WALL     Hamster  
11                  HAMSTER TRAPPED UNDER FLOORBOARDS     Hamster  
```

```{code-tab} plaintext R Output
# A tibble: 12 × 5
   IncidentNumber  CalYear TotalCost Description                     AnimalGroup
   <chr>             <int>     <dbl> <chr>                           <chr>      
 1 098141-28072016    2016      3912 CAT STUCK WITHIN WALL SPACE  R… Cat        
 2 49076141           2014      2655 KITTEN TRAPPED IN CHIMNEY       Cat        
 3 028258-08032017    2017      2282 ASSIST RSPCA WITH CAT IN CHIMN… Cat        
 4 101755111          2011      2340 DEER IN CANAL - WATER RESCUE L… Deer       
 5 144664-11102018    2018      1998 DEER ENTANGLED - RSPCA ON SCEN… Deer       
 6 174801-27122016    2016      1304 TWO DEER STUCK IN FENCING RVP … Deer       
 7 154461131          2013      2030 DOG TRAPPED BETWEEN TWO HOUSE   Dog        
 8 098400-17072018    2018      1665 PUPPY TRAPPED BETWEEN TWO HOUS… Dog        
 9 033179-19032017    2017      1630 DOG STUCK ON ISLAND IN LARGE P… Dog        
10 040568-06042016    2016       652 HAMSTER STUCK UNDER FLOORBOARDS Hamster    
11 050586-29042016    2016       326 HAMSTER STUCK IN CAVITY WALL    Hamster    
12 099706-31072016    2016       326 HAMSTER TRAPPED UNDER FLOORBOA… Hamster    
```
````
We have the same result, but this will run much quicker due to only calling one action.

### Where loops aren't a problem

If you're not calling an action, then your loop will just add an extra step to the plan. In this example we are creating a new column each time, which is a *transformation*, not an *action*. The single action is `.toPandas()`, called outside of the loop.
````{tabs}
```{code-tab} py
for animal in animals:
    rescue = rescue.withColumn(animal, F.when(F.col("AnimalGroup") == animal, True).otherwise(False))

rescue.orderBy("IncidentNumber").limit(5).toPandas()
```

```{code-tab} r R

for (animal in animals) {
  rescue <- rescue %>%
    sparklyr::mutate(!!animal := ifelse(
      AnimalGroup == animal, TRUE, FALSE))
  
}

rescue %>%
  dplyr::arrange(IncidentNumber) %>%
  head(5) %>%
  collect() %>%
  print()


```
````

````{tabs}

```{code-tab} plaintext Python Output
     IncidentNumber                       AnimalGroup  CalYear  TotalCost  \
0  000014-03092018M  Unknown - Heavy Livestock Animal     2018      999.0   
1   000099-01012017                               Dog     2017      652.0   
2   000260-01012017                              Bird     2017      326.0   
3   000375-01012017                               Dog     2017      652.0   
4   000477-01012017                              Deer     2017      326.0   

                                         Description    Cat    Dog  Hamster  \
0                                               None  False  False    False   
1    DOG WITH HEAD STUCK IN RAILINGS CALLED BY OWNER  False   True    False   
2  BIRD TRAPPED IN NETTING BY THE 02 SHOP AND NEA...  False  False    False   
3  DOG STUCK IN HULL OF DERELICT BOAT - WATER RES...  False   True    False   
4  DEER TRAPPED IN RAILINGS JUNCTION WITH DENNIS ...  False  False    False   

    Deer  
0  False  
1  False  
2  False  
3  False  
4   True  
```

```{code-tab} plaintext R Output
# A tibble: 5 × 9
  AnimalGroup   TotalCost Description CalYear IncidentNumber Cat   Dog   Hamster
  <chr>             <dbl> <chr>         <int> <chr>          <lgl> <lgl> <lgl>  
1 Unknown - He…       999 <NA>           2018 000014-030920… FALSE FALSE FALSE  
2 Dog                 652 DOG WITH H…    2017 000099-010120… FALSE TRUE  FALSE  
3 Bird                326 BIRD TRAPP…    2017 000260-010120… FALSE FALSE FALSE  
4 Dog                 652 DOG STUCK …    2017 000375-010120… FALSE TRUE  FALSE  
5 Deer                326 DEER TRAPP…    2017 000477-010120… FALSE FALSE FALSE  
# ℹ 1 more variable: Deer <lgl>
```
````
If you can find a more Pythonic way to write your loops that don't call actions then that is recommended, but it is less of an issue for efficiency.

### Finally: testing!

When refactoring your code always make sure you use unit tests or a regression test to make sure that the functionality of the code is preserved. It's far better to have slow code which works than quick code which doesn't!

### Further Resources

Documentation:
- [isin()](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.Column.isin)
- [Window](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.Window)
- [F.row_number()](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.functions.row_number)


