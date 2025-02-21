# Hive Partitions and Buckets

Partitions and buckets can be used in both Hive and Spark to aid optimization. 
However, there are differences between Hive and Spark in terms of how these operations work and when to use them.
This guide with cover partitioning and bucketing in Hive. How Hive partitions differ from Spark partitions will also be covered, and when is best to use each. 

## Hive Partitions

Hive partitions are static in nature and tied to a table schema, and optimized for running SQL queries on. Hive tables can be split into multiple partitions to make storage of data more efficient. This means that Hive internally divides the data based on a partition key, with data for each key being stored in a sub-directory in the underlying Hive parquet file on S3. Hive partitions function differently from [Spark partitions](#spark-partitions).

### When to use

Hive partitions are used to distribute data load horizontally. This is especially useful for low cardinality columns (e.g. where you do not have many unique values).
Examples of this could include country or year of birth columns.

You can read in Hive partitions individually, allowing you to perform operations on smaller subsets of data. They are especially useful for batch-processing as opposed to real-time processing where Spark partitions are preferred.

However, unlike Spark where partitioning can be applied dynamically to dataframes, you can't apply new partitioning to an existing Hive table - you have to create a new one. 

For example, say I want to partion the `animal_rescue` table within our dapcats database. I can check the current partitioning by going into Hue, navigating to the dapcats database and right clicking the animal_rescue table and selecting "show in browser".

From here I can look at the stats and see the current number of files the data is stored on, which in this case is only 1.

```{figure} ../images/hive_partitions/hive_unpartitioned_rescue.png
---
width: 100%
name: hive_unpartitioned
alt: Unpartitioned animal rescue Hive table showing table existing on 1 partition.
---
Unpartitioned animal rescue table
```


If I want to partition the table by year (`calyear`), I can save as new table with this partitioning scheme and then insert my data into it. 

To ensure Hive partitioning is enabled when using the Hive Query Editor (this does not need to be done if using PySpark/SparklyR), the settings `hive.exec.dynamic.partition` needs to be set to "true" and `hive.exec.dynamic.partition.mode` needs to be set to "nonstrict" to allow multiple partitions to be added at once.

This can be done directly within the Hive query editor as such:

```sql
DROP TABLE IF EXISTS dapcats.partitioned_rescue;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE EXTERNAL TABLE dapcats.partitioned_rescue(
    incidentnumber string,
    borough string,
    typeofincident string
)
PARTITIONED BY (calyear int)
STORED AS PARQUET; 

INSERT OVERWRITE TABLE dapcats.partitioned_rescue PARTITION (calyear)
SELECT incidentnumber, borough, typeofincident, calyear
FROM dapcats.rescue_lite;

```

You can also achieve the same thing using PySpark with the below code:

```py
from pyspark.sql import SparkSession

spark = (SparkSession.builder
          .appName('hive_partitions')
          .getOrCreate())

animal_rescue = spark.sql('select * from dapcats.rescue_lite')

(animal_rescue.write.mode('overwrite')
    .partitionBy('calyear')
    .saveAsTable('dapcats.partitioned_rescue'))

```

And in sparklyR:

```r
library(dplyr)
library(sparklyr)

sc <- sparklyr::spark_connect(
      master = "local", 
      app_name = "hive_partitions_R")


animal_rescue <- dplyr::tbl(sc, sql("SELECT * FROM dapcats.rescue_lite"))


animal_rescue %>%
  sparklyr::spark_write_table(name = "dapcats.partitioned_rescue_R",
                              partition_by = "calyear",
                              mode = "overwrite")
```

You can then go to S3 and see that the table partitioned_rescue is stored in 11 files - one for each year. Also note, that there is a key symbol next to the column(s) which partitioning is applied to.

```{figure} ../images/hive_partitions/hive_partitioned_rescue.png
---
width: 100%
name: hive_partitioned
alt: Partitioned animal rescue Hive table showing table existing on 11 partition.
---
Partitioned animal rescue table
```


You can view the records stored in each partition by clicking directly on the partition value:


```{figure} ../images/hive_partitions/rescue_2019_partition.png
---
width: 100%
name: rescue_2019_partition
alt: Image showing inside the partioned table with 2019 selected.
---
Partition for 2019
```

```{figure} ../images/hive_partitions/rescue_2019_records.png
---
width: 100%
name: rescue_2019_partition
alt: Image showing inside the partioned table, showing records inside partition 2019.
---
Records for 2019
```

You can also run the `SHOW PARTITIONS <table>` command to view how the table is partitioned:

```{figure} ../images/hive_partitions/show_hive_partitions.png
---
width: 100%
name: show_hive_partitions
alt: SQL query in Hive Query editor showing partitions of the rescue table.
---
SQL query to show partitions in Hive query editor
```




## Hive Buckets

Similiar to partitioning, bucketing can be used as a way to optimize data by segregating it into different files and hence preventing the shuffle operation. With bucketing, data is split into a fixed number of buckets according to a hash function performed on the bucketing column. The result is then used to determine which bucket the row should go into.

In the below diagram, we have a small dataset containing rescue data from the years 2022 to 2024. We have first partioned on year as it is a low cardinality column (only has 3 values), thus creating 3 partitions. We have then created 3 buckets on incidentnumber which is high cardinality as it contains many unique values. This means 3 buckets are created on each of the partitions. Records are assigned to each bucket based on their hash. For simple illustration purposes a single number has been used to represent the hash, but in reality this would be a long string of random characters. 

```{figure} ../images/hive_partitions/partitioning_bucketing_diagram.png
---
width: 100%
name: partitioning_bucketing_diagram
alt: Daigram to show how partitioning and bucketing works in Hive tables. Example shows rescue table over 3 partitions based on a subset of calyear, with 3 buckets based on incidentnumber in each partition.  
---
Diagram of Hive table partitioning and bucketing
```



### When to use

Bucketing is useful for high cardinality columns (i.e. columns where there are many unique values). 
Examples of this could include unique id or primary key columns. You can't add buckets to a pre-existing Hive table - a new one has to be created (or use overwrite in Spark).

Bucketing can be used on both partitioned and non-partitioned Hive tables. If using on partioned tables, Hive will seperate each partion into the number of buckets specified. E.g. if I had 2 partitions based on column x and I had 5 buckets based on column y, I would have 10 files in total (5 sub-directories for each partition).

By combining bucketing with partitioning, you can organise your data based on a low cardinality column (e.g. year), and organise the data within that based on a high cardinality column (e.g. unique id). This combination is particularly useful for optimising join operations on large datasets.

The example code below illustrates this by partitioning the rescue_lite dataframe on calyear (which as we saw previously was 11 partions), and bucketing based on incidentnumber. This gives us 55 files which we can see in the Hue browser.

```sql
DROP TABLE IF EXISTS dapcats.bucketed_rescue;

CREATE EXTERNAL TABLE dapcats.bucketed_rescue(
    incidentnumber string,
    borough string,
    typeofincident string
)
PARTITIONED BY (calyear int)
CLUSTERED BY (incidentnumber) INTO 5 BUCKETS
STORED AS PARQUET; 

INSERT OVERWRITE TABLE dapcats.bucketed_rescue PARTITION (calyear)
SELECT incidentnumber, borough, typeofincident, calyear
FROM dapcats.rescue_lite;

```

You can also achieve the same thing using PySpark with the below code:

```py
from pyspark.sql import SparkSession

spark = (SparkSession.builder
          .appName('hive_partitions')
          .getOrCreate())

animal_rescue = spark.sql('select * from dapcats.rescue_lite')

(animal_rescue.write.mode('overwrite')
    .partitionBy('calyear')
    .bucketBy(5, "incidentnumber") 
    .saveAsTable("dapcats.bucketed_rescue"))
```

Unfortunately sparklyr currently doesn't support bucketing, so if you are working in this language it is advised to use the Hive query editor and to create your table in SQL before inserting data into it.

```{figure} ../images/hive_partitions/hive_bucketed_rescue.png
---
width: 100%
name: hive_bucketed_rescue
alt: Diagram to show rescue table with 5 buckets in each partition. 
---
Diagram of bucketed rescue table
```


You can now go into each partition of the table to view the buckets (by clicking on "Files" on the left of the partition number). We can see the 5 buckets for the year 2019:

```{figure} ../images/hive_partitions/hive_partitions/rescue_buckets2.png
---
width: 100%
name: rescue_buckets2
alt: Inside folder for partitioned and bucketed rescue table. Year 2019 is selected. 
---
Inside bucketed table
```


```{figure} ../images/hive_partitions/hive_partitions/rescue_buckets.png
---
width: 100%
name: rescue_buckets
alt: Inside partition 2019 showing 5 sub-folders for the buckets. 
---
Inside partition 2019 showing 5 buckets
```


## Spark Partitions

Spark partitions are logical divisions of data within a DataFrame (as opposed to within the schema in a Hive table). Spark partitions are optimised for being distributed across executors and processed in parralel. 

Unlike in Hive, where partitions are tied to physical storage locations, in Spark partitions are in-memory representations, meaning they can be used for real-time processing. Spark partitions do not create physical directories in the file system, unless explicitly written out.

### When to use

Spark partitions are best to use when working with large datasets in real-time and in-memory that require high-performance parralel computation. They can also be used for temporary paritioning of data prior to wide operations such as joins and aggregations.

For more information an examples on Spark partitioning please see the Spark at the ONS section on [Managing Partitions](https://best-practice-and-impact.github.io/ons-spark/spark-concepts/partitions.html).

