# Missing values and imputation page outline

### Introduction - needs editing

Need a way of checking for and dealing with missing values in data
Spark ML functions won't work with nulls/NAs present
Dealing with missing values is complex and the best strategy will depend on individual datasets/models etc.
This guide is not intended to comprehensively cover strategies for dealing with missing values, but instead to show how various methods can be applied to large datasets in PySpark/SparklyR
Will cover: how to identify missing values, mean and median imputation, group imputation and last value carried forward.

### Setting up Spark and loading your data

````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = (SparkSession.builder
          .appName('missing-data')
          .getOrCreate())
```

```{code-tab} r R
# Load in required packages
library(sparklyr)
library(dplyr)
library(pillar)

# Set up Spark session
default_config <- sparklyr::spark_config()

sc <- spark_connect(master = "local",
                  app_name = "missing_data",
                  config = default_config)


# Check Spark connection is open
  spark_connection_is_open(sc)
```
````
````{tabs}
```{code-tab} plaintext Python output

Setting spark.hadoop.yarn.resourcemanager.principal to alexandra.snowdon

```

```{code-tab} plaintext R output

[1] TRUE

```
````
````{tabs}
```{code-tab} py

# Read in the MOT dataset
#mot_path = "s3a://onscdp-dev-data01-5320d6ca/bat/dapcats/mot_test_results.csv
mot_path = "C:/Users/snowda/repos/data/mot_test_results.csv"

mot = (spark.read.csv(mot_path, header=True, inferSchema=True))

# Check schema and preview data
mot.printSchema()

```

```{code-tab} r R

# Read in the MOT dataset
#mot_path = "s3a://onscdp-dev-data01-5320d6ca/bat/dapcats/mot_test_results.csv"
mot_path = "C:/Users/snowda/repos/data/mot_test_results.csv"
mot <- sparklyr::spark_read_csv(
    sc,
    mot_path,
    header = TRUE,
    infer_schema = TRUE)

# Check schema and preview data
pillar::glimpse(mot)

```
````

````{tabs}
```{code-tab} plaintext Python output

root
 |-- test_id: integer (nullable = true)
 |-- vehicle_id: integer (nullable = true)
 |-- test_date: timestamp (nullable = true)
 |-- test_class_id: integer (nullable = true)
 |-- test_type: string (nullable = true)
 |-- test_result: string (nullable = true)
 |-- test_mileage: integer (nullable = true)
 |-- postcode_area: string (nullable = true)
 |-- make: string (nullable = true)
 |-- model: string (nullable = true)
 |-- colour: string (nullable = true)
 |-- fuel_type: string (nullable = true)
 |-- cylinder_capacity: integer (nullable = true)
 |-- first_use_date: timestamp (nullable = true)

```

```{code-tab} plaintext R output

Rows: ??
Columns: 14
Database: spark_connection
$ test_id           <int> 1687460261, 1007821341, 1268040905, 232884115, 12510…
$ vehicle_id        <int> 1131526890, 211522675, 1218838379, 159956284, 851633…
$ test_date         <date> 2023-04-28, 2023-04-28, 2023-04-28, 2023-04-28, 202…
$ test_class_id     <int> 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7, 4…
$ test_type         <chr> "NT", "NT", "RT", "NT", "NT", "NT", "NT", "NT", "NT"…
$ test_result       <chr> "F", "P", "P", "F", "P", "P", "P", "P", "P", "P", "P…
$ test_mileage      <int> 129661, 26597, 11803, 148187, 12915, 22281, 30140, 2…
$ postcode_area     <chr> "HU", "IV", "LA", "BD", "PO", "WA", "PH", "B", "S", …
$ make              <chr> "BMW", "FORD", "THE EXPLORER GROUP", "AUDI", "DACIA"…
$ model             <chr> "525", "FIESTA", "UNCLASSIFIED", "A3", "DUSTER", "CO…
$ colour            <chr> "BLACK", "BLUE", "WHITE", "WHITE", "ORANGE", "BLUE",…
$ fuel_type         <chr> "DI", "PE", "DI", "DI", "PE", "PE", "PE", "DI", "DI"…
$ cylinder_capacity <int> 2497, 998, 1997, 1968, 1330, 1398, 1199, 1500, 2993,…
$ first_use_date    <date> 2002-05-27, 2016-06-27, 2017-03-03, 2010-10-30, 201…

```
````
### Identifying missing values 

````{tabs}
```{code-tab} py

```

```{code-tab} r R

# Get a summary of NAs for the entire dataset
results_nas <- mot %>%
  mutate(across(c(test_date, first_use_date), ~to_date(.))) %>%
  dplyr::summarise_all(~sum(as.integer(is.na(.)))) 

results_nas %>% 
  print(width = Inf)

```
````

````{tabs}
```{code-tab} plaintext Python output

```

```{code-tab} plaintext R output

# Source:   SQL [1 x 14]
# Database: spark_connection
  test_id vehicle_id test_date test_class_id test_type test_result test_mileage
    <dbl>      <dbl>     <dbl>         <dbl>     <dbl>       <dbl>        <dbl>
1       0          0         0             0         0           0       324129
  postcode_area  make model colour fuel_type cylinder_capacity first_use_date
          <dbl> <dbl> <dbl>  <dbl>     <dbl>             <dbl>          <dbl>
1             0     0     0      0         0            279982              0

```
````
It is important to consider what data is really needed for your purpose. Filter out the unnecessary data early on to reduce the size of your dataset as well as compute resource, time and money. For consistency across additional guidance we have that uses the MOT dataset we will pre-process and filter the data now.

````{tabs}
```{code-tab} py

```

```{code-tab} r R

# Select for certain columns and correct data types

cleaned_results <- test_result %>%
    sparklyr::select(vehicle_id, test_date, test_mileage, postcode_area, make, model, colour, cylinder_capacity) %>%
    dplyr::mutate(test_date = as.date(test_date))

results_nas <- cleaned_results %>%
  dplyr::summarise_all(~sum(as.integer(is.na(.))))

# Check the NA data again
results_nas %>% 
  print(width = Inf)

```
````

````{tabs}
```{code-tab} plaintext Python output

```

```{code-tab} plaintext R output

# Source:   SQL [1 x 8]
# Database: spark_connection
  vehicle_id test_date test_mileage postcode_area  make model colour
       <dbl>     <dbl>        <dbl>         <dbl> <dbl> <dbl>  <dbl>
1          0         0       324129             0     0     0      0
  cylinder_capacity
              <dbl>
1            279982

```
````

We can now see there are lots of missing values in test_mileage and cylinder_capacity.

Now we have identified missing values, one option to deal with them would be to exclude these columns from our analysis, or simply to filter out rows with missing values. However, filtering out missing values might skew the distributions of values in our dataset and make our analysis unreliable. Similarly, we may not want to discard these columns from our analysis because they might still be important despite being incomplete eg. we might expect test_mileage to be an important predictor of whether a car is likely to pass an MOT test or not, so excluding this column would not be ideal. Think of what you are wanting to use the data for!

### Common imputation methods for continuous variables 

#### Mean or median imputation

Imputing missing values with the mean or the median value for a given variable is a simple way of dealing with missing values in a dataset. In Spark, this type of imputation is easy to achieve using feature transformers. In SparklyR you would use  `ft_imputer` and in Pyspark it would be `Imputer()`.

Before we impute the missing values, we will do some data manipulation. To simplify the data we will drop the full date and use just the year. We will also encode the missing values - add an indicator column for each variable will be added which be filled with 1 for non-missing values and 0 if a value is missing for a particular record.

````{tabs}
```{code-tab} py

```

```{code-tab} r R

results <- mot %>%
  mutate(year_test = year(test_date)) %>%
  # drop date cols
  select(-test_date) %>%
  # encode missing values
   mutate(missing_cyl = ifelse(is.na(cylinder_capacity), 0, 1), 
         missing_mileage = ifelse(is.na(test_mileage), 0, 1))

results %>% 
  print(width = Inf)

```
````

````{tabs}
```{code-tab} plaintext Python output

```

```{code-tab} plaintext R output

# Source:   SQL [?? x 10]
# Database: spark_connection
   vehicle_id test_mileage postcode_area make               model        colour
        <int>        <int> <chr>         <chr>              <chr>        <chr> 
 1 1131526890       129661 HU            BMW                525          BLACK 
 2  211522675        26597 IV            FORD               FIESTA       BLUE  
 3 1218838379        11803 LA            THE EXPLORER GROUP UNCLASSIFIED WHITE 
 4  159956284       148187 BD            AUDI               A3           WHITE 
 5  851633977        12915 PO            DACIA              DUSTER       ORANGE
 6 1236900649        22281 WA            VAUXHALL           CORSA        BLUE  
 7 1343158317        30140 PH            CITROEN            C3           GREY  
 8 1221072863        23462 B             PEUGEOT            3008         GREY  
 9  249708845        26122 S             BMW                X5           WHITE 
10  480476389        22007 PA            FORD               C-MAX        BLUE  
   cylinder_capacity year_test missing_cyl missing_mileage
               <int>     <int>       <dbl>           <dbl>
 1              2497      2023           1               1
 2               998      2023           1               1
 3              1997      2023           1               1
 4              1968      2023           1               1
 5              1330      2023           1               1
 6              1398      2023           1               1
 7              1199      2023           1               1
 8              1500      2023           1               1
 9              2993      2023           1               1
10              1596      2023           1               1

```
````
Next we can apply mean imputation by setting the strategy argument to "mean". But first, we need to enforce new column types as double or float so that we can include a mutate statement first to initialise. Note that you could also use "median" as the strategy argument in the below code. 

````{tabs}
```{code-tab} py

```

```{code-tab} r R

# Specify columns to impute
impute_cols <-  c("cylinder_capacity", "test_mileage")

# Actually is very quick to do on full dataset
mean_imputed <- results %>%
  mutate(across(all_of(impute_cols), ~as.double(.))) %>%
  ft_imputer(input_cols = impute_cols,
             output_cols = c("cyl_imputed", "mileage_imputed"), 
             strategy = "mean")

mean_imputed %>% 
  arrange(missing_cyl, missing_mileage) %>% 
  glimpse()

```
````

````{tabs}
```{code-tab} plaintext Python output

```

```{code-tab} plaintext R output

Rows: ??
Columns: 12
Database: spark_connection
Ordered by: missing_cyl, missing_mileage
$ vehicle_id        <int> 1025464220, 347027954, 145727633, 250280838, 9537842…
$ test_mileage      <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, …
$ postcode_area     <chr> "E", "B", "G", "CH", "TN", "SN", "ME", "KT", "W", "W…
$ make              <chr> "TOYOTA", "HUMMER", "RENAULT", "FORD", "S5 E2", "TES…
$ model             <chr> "PRIUS", "UNCLASSIFIED", "KANGOO", "TRANSIT", "UNCLA…
$ colour            <chr> "SILVER", "WHITE", "WHITE", "WHITE", "GREY", "BLUE",…
$ cylinder_capacity <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, …
$ year_test         <int> 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023…
$ missing_cyl       <dbl> 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0…
$ missing_mileage   <dbl> 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0…
$ cyl_imputed       <dbl> 1694.371, 1694.371, 1694.371, 1694.371, 1694.371, 16…
$ mileage_imputed   <dbl> 75507.65, 75507.65, 75507.65, 75507.65, 75507.65, 75…

```
````

````{tabs}
```{code-tab} py

```

```{code-tab} r R

```
````

````{tabs}
```{code-tab} plaintext Python output

```

```{code-tab} plaintext R output

```
````
````{tabs}
```{code-tab} py

```

```{code-tab} r R

```
````

````{tabs}
```{code-tab} plaintext Python output

```

```{code-tab} plaintext R output

```
````
````{tabs}
```{code-tab} py

```

```{code-tab} r R

```
````

````{tabs}
```{code-tab} plaintext Python output

```

```{code-tab} plaintext R output

```
````
