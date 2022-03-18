## Using Spark functions in sparklyr

The sparklyr package allows you to use the dplyr style functions when working on the cluster with sparklyr DataFrames. The key difference to working with tibbles or base R DataFrames is that the Spark cluster will be used for processing, rather than the CDSW session. This means that you can handle much larger data.

You can also make use of [Spark functions](https://spark.apache.org/docs/latest/api/sql/index.html) directly when using sparklyr. To do this, wrap them in a relevant `dplyr` command, for instance, `mutate()` or `filter()`. Note that these functions are not part of an actual R package and so you can't prefix them with the package name with `::`.

There are a large number of Spark functions and the authors of this article have not verified them all; versioning and implementation differences mean that not all might be available.

Remember: you can't use these functions on a tibble or base R DataFrame as R cannot interpret them. They can only be processed on the Spark cluster.

### Selected practical examples

Set up a Spark session and read the Animal Rescue data:
````{tabs}

```{code-tab} r R

library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "sparklyr-functions",
    config = sparklyr::spark_config())
        
config <- yaml::yaml.load_file("ons-spark/config.yaml")

rescue <- sparklyr::spark_read_parquet(sc, config$rescue_path) %>%
    sparklyr::select(date_time_of_call, animal_group, property_category)
    
pillar::glimpse(rescue)

```
````

```plaintext
Rows: ??
Columns: 3
Database: spark_connection
$ date_time_of_call <chr> "25/06/2013 07:47", "22/10/2014 17:39", "22/10/2016 …
$ animal_group      <chr> "Cat", "Horse", "Bird", "Cat", "Dog", "Deer", "Deer"…
$ property_category <chr> "Dwelling", "Outdoor", "Outdoor Structure", "Dwellin…
```
#### Cast to date: `to_date()`

`to_date()` changes the column type to date with the chosen format. This must be wrapped in a valid `dplyr` command, such as `mutate()`:
````{tabs}

```{code-tab} r R

rescue <- rescue %>% 
    sparklyr::mutate(date_of_call = to_date(date_time_of_call, "dd/MM/yyyy"))

rescue %>%
    sparklyr::select(date_time_of_call, date_of_call) %>%
    head(5) %>%
    sparklyr::collect()

```
````

```plaintext
# A tibble: 5 × 2
  date_time_of_call date_of_call
  <chr>             <date>      
1 25/06/2013 07:47  2013-06-25  
2 22/10/2014 17:39  2014-10-22  
3 22/10/2016 12:44  2016-10-22  
4 09/04/2014 13:37  2014-04-09  
5 22/01/2013 19:16  2013-01-22  
```
#### Capitalise first letter of each word: `initcap()`

`initcap()` capitalises the first letter of each word, and can be useful when data cleansing.

In the Animal Rescue data the values in the `animal_group` column do not always begin with a capital letter. In this example, `initcap()` can be combined with `filter()` to return all cats, regardless of case:
````{tabs}

```{code-tab} r R

cats <- rescue %>% sparklyr::filter(initcap(animal_group) == "Cat")

```
````
Show that both `"cat"` and `"Cat"` are included in this DataFrame:
````{tabs}

```{code-tab} r R

cats %>%
    dplyr::group_by(animal_group) %>%
    dplyr::summarise(n())

```
````

```plaintext
# Source: spark<?> [?? x 2]
  animal_group `n()`
  <chr>        <dbl>
1 cat             15
2 Cat           2909
```
#### `concat_ws()`: a Spark version of `paste()`

`concat_ws()` works in a similar way to the base R function `paste()`; the separator is the first argument:
````{tabs}

```{code-tab} r R

rescue <- rescue %>% sparklyr::mutate(animal_property = concat_ws(": ", animal_group, property_category))

rescue %>%
    sparklyr::select(animal_group, property_category, animal_property) %>%
    head(5) %>%
    sparklyr::collect()

```
````

```plaintext
# A tibble: 5 × 3
  animal_group property_category animal_property        
  <chr>        <chr>             <chr>                  
1 Cat          Dwelling          Cat: Dwelling          
2 Horse        Outdoor           Horse: Outdoor         
3 Bird         Outdoor Structure Bird: Outdoor Structure
4 Cat          Dwelling          Cat: Dwelling          
5 Dog          Outdoor           Dog: Outdoor           
```
## Further Resources

Spark SQL Documentation:
- [`to_date`](https://spark.apache.org/docs/latest/api/sql/index.html#to_date)
- [`initcap`](https://spark.apache.org/docs/latest/api/sql/index.html#initcap)
- [`concat_ws`](https://spark.apache.org/docs/latest/api/sql/index.html#concat_ws)