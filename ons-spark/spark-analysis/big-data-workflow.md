# **Big data workflow**
Working with big data efficiently presents a major challenge to analysts. In particular, working with large datasets can use large amounts of computational resource, which can be very expensive. Inefficient code or working practices can unnecessarily increase the costs of running your analysis, as well as the time it takes you to run your code. However, there are good practices that can be employed to help minimise costs without sacrificing the quality of your analysis. We have produced this page as a general guide to best practice in working with big data, including tips for general workflow and managing pipeline development, working with samples and subsets of your data, and optimising analysis with Spark.


## **General workflow and project planning**

### 1. Plan your analysis
- A simplified version of the process of developing a new pipeline might look something like this:


```{figure} ../images/big_data_pipeline_stages.png
---
width: 100%
name: BigDataPipelineStages
alt: Diagram showing potentials steps in the process of developing a new big data pipeline.
---
Big data pipeline development stages
```

- Splitting your development into clearly defined stages like this will help with applying some of the tips we have below for keeping your resource use to a minimum e.g. working with small samples or subsets of your data in the EDA phase.

### 2. [Version control](https://gitlab-app-l-01/DAP_CATS/guidance/-/wikis/Git-and-GitLab) your code and apply [RAP good practice](https://best-practice-and-impact.github.io/qa-of-code-guidance/intro.html) 

- This will make it easier to track what has and hasn't already been done and for others to understand what your code is doing.

- It is a good idea to agree on a coding plan including code style for the project and Git branch strategies in advance.

- Ensure code is readable, well documented, and follows a logical structure.

### 3. Use [code reviews](https://best-practice-and-impact.github.io/qa-of-code-guidance/peer_review.html) 
- Peer review of code should be used to quality assure and improve your code. A fresh pair of eyes may be able to spot places where things could be done more efficiently to save computing resource.

- This also helps other members of your team understand the code in detail and can help build capability in the team if necessary. This ensures that a single developer is not relied upon to write and maintain all code.

## **Data exploration and quality**

### 4. Be aware of the size of your dataset
- How many rows and columns does your dataset have?
- If the dataset is relatively small (less than a few million rows) use Python or R for analysis instead of PySpark or SparklyR.
- Consider also switching to using Python or R at points in your pipeline where data becomes small due to filtering or aggregation to save resource.
- Is the size of the data likely to grow over time? If this is the case you should consider how you will minimise resource use as the amount of data to process increases.

### 5. Reduce the size of your dataset to use fewer compute resources
- Generally, it is unnecessary and not good practice to carry out the majority of the analysis stages outlined above on a full dataset, especially if it is very large. 
- Keep the size of your data in the EDA, detailed analysis and development stages as small as possible to conserve resource. 
- Consider [taking samples of your data](../spark-functions/sampling), especially in the EDA phase
    - Use a simple sample size calculator to estimate how large a sample to take.
    - Ideally, you should take a sample small enough that you can use Python or R for analysis instead of PySpark/SparklyR.
    - You can validate your sample by comparing descriptive statistics to the full dataset.
- Aggregate data where possible.
    - This can be done in any stage of analysis and may even form part of your regular production pipeline.
    - You can also use aggregation to help find edge cases that might not be captured by sampling your data. This can be particularly useful in the EDA phase to ensure that these extreme cases are not missed in the development of your pipeline.
- Consider what data you really need and filter this out as early as possible to reduce the size of your data. 

### 6. Consider creating synthetic data to work on
- Create minimal size dummy data containing edge cases to work with in the development phase.
- Synthetic datasets can also be used for testing or peer reviewing code to help QA either code or analysis.


The diagram below has suggestions for where taking samples, subsets or creating synthetic data may be appropriate at each stage in your pipeline development.


```{figure} ../images/big_data_pipeline_datasize.png
---
width: 100%
name: BigDataPipelineDataSize
alt: A diagram showing rough guidelines for the size of data to use at each phase of development for your pipeline.
---
Using samples/subsets or synthetic data in your pipeline development
```

### 7. Ensure that duplicates are removed early on in the pipeline
- This can help reduce your data size early on.
- Make sure your code does not create any duplicates e.g. when joining data sources together.

### 8. Use big data profiling tools to check [data quality dimensions](https://www.gov.uk/government/publications/the-government-data-quality-framework/the-government-data-quality-framework#Data-quality-dimensions) where possible
- [PyDeequ](https://aws.amazon.com/blogs/big-data/testing-data-quality-at-scale-with-pydeequ/) or [Great Expectations](https://greatexpectations.io/) are useful profiling tools that can be used with PySpark.
- We are not currently aware of any similar profiling packages that are compatible with Sparklyr. As a result, if you are an R user, you will need to create code to examine your data quality dimensions yourself. We recommend that you consider the other tips in this guide to help minimise your resource use when doing so.


## **Spark optimisation**

The following section contains some more advanced suggestions you can consider applying to your pipeline to further optimise your code and minimise resource use. Our general tips for Spark optimisation can be found on the [Ideas for optimising Spark code](../optimisation-tips.html) page of the book.

### 9. Use Spark functions where possible
- `Spark ML` functions ([MLlib](https://spark.apache.org/docs/latest/ml-guide.html)) contain a range of tools for wrangling and analysing big data efficiently.
- See the [Logistic regression](./logistic-regression.html) page of this book for some examples of applying `Spark ML` functions and setting up a `Spark ML` pipeline. 

### 10. Optimise joins
- Be aware of how the size of your data changes when joining other sources at various points in your pipeline.
- Follow the guidance on the [optimising joins](../spark-concepts/join-concepts.html) page of this book.

### 11. Consider data [partitioning](../spark-concepts/partitions.html)
- Spark splits data into partitions to perform parallel processing. 
- Having too many or too few partitions may cause performance issues in your code. 
- There is no set rule for how many partitions your data should have. As a general guide, the optimum size for a partition is often suggested to be 100-200MB and the number of partitions should be a multiple of the number of nodes in your [Spark session](../spark-overview/spark-session-guidance.html). 
- It may be beneficial to change how your data is partitioned at various points in your pipeline. For example if your full dataset contains 200 million rows, it may be reasonable to have 200 partitions. However, if later on in your pipeline this data is aggregated so that there are now only around 20 million rows, repartitioning your data on to a smaller number of partitions could improve performance and reduce the compute resource used. 
- Be aware of the difference between [*narrow* and *wide* transformations](../spark-concepts/shuffling.html) in Spark and how *wide* transformations can increase the number of partitions in your data. 
    - If you are using Spark 2.X, you may need to repartition after performing such operations or consider changing the `spark.sql.shuffle.partitions` from the default (200) in your [Spark config](../spark-overview/spark-defaults.html).
    - If you are using Spark 3.X, ensure [Adaptive Query Excution (ADE)](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution) is switched on by setting `spark.sql.adaptive.enabled` to `true` in your [Spark config](../spark-overview/spark-defaults.html). This will automatically coalesce post-shuffle partitions.
    
### 12. Become familiar with the Spark UI    
- The [Spark UI](../spark-concepts/spark-application-and-ui.html) is used to monitor the status and resource consumption of your Spark cluster and can be a useful tool for troubleshooting slow Spark code.
- Check the size of the execution plan regularly
    - Do this either by examining the DAG (Directed Acyclic Graph) in the Spark UI or by using `df.explain()`/`explain(df)` in PySpark/SparklyR.
    - Overly long execution plans can cause performance issues.
    - Consider using [checkpoints and staging tables](https://best-practice-and-impact.github.io/ons-spark/spark-concepts/checkpoint-staging.html?highlight=df+explain) at appropriate points in your code to break the lineage in these cases.
    
    





