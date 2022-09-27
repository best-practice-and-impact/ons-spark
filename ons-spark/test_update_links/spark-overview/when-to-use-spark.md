## When To Use Spark

Some points to consider when making a decision on whether to use Spark for analysis in government are:

- Good practices for research and analysis in government
- The amount of data you need to process
- Current skills within the project team
- What do you want to do with the data?
- Using a mix of tools that work for you and are available to you

In this article we will give some comments on each of the above points. These comments are the authors' opinions and by no means a comprehensive discussion on tooling for analysis. 

### Good practices

Analysts in government are increasingly being asked to make use of open-source coding languages for research and analysis. Gov.uk guidance on [Choosing tools and infrastructure to make better use of your data](https://www.gov.uk/guidance/choose-tools-and-infrastructure-to-make-better-use-of-your-data) says,

>Data scientists and analysts often use common open source languages such as Python and R. The benefits of using these languages include:
>
> - good support and open training - which means reduced training costs
> - new data analytics methods
> - the ability to create your own methods using open source languages


Therefore, from a good practice point of view, the preferred tools to use are Python or R. 

For more information about good practices in using Python and R see the [QA of analysis and research in government](https://best-practice-and-impact.github.io/qa-of-code-guidance/intro.html).

### Amount of data to process

Python and R have their limits, specifically when it comes to big data. The main issue is that as datasets get larger, the data are eventually too big to fit into a computer's RAM (or memory). Another issue is that processing large amounts of data using Python or R can become very slow. This is where Spark comes in.

As repeatedly mentioned in the [Getting Started](spark-start) guide, Spark is a big data solution. It allows us to process large amounts of data efficiently by making use of a cluster of computers and parallel processing. Can we use Spark to process smaller amounts of data? Yes, but it is not advised to do so unless for a specific reason like developing code using a [sample of the data](../spark-functions/sampling), writing [unit tests](../testing-debugging/unit-testing) or doing some training. There are easier ways to process smaller datasets, like using pandas or R. 

When deciding whether to use Spark think about how big the data is; anything bigger than a few GB and it's likely Spark will be more efficient at processing the data. It is also worth considering whether the dataset will be joined or linked to another dataset, because both datasets will need to fit into memory to complete the join in Python or R.

Another consideration is whether the data will grow with time. For example, a data processing pipeline written in R takes a 2GB dataset as an input, but if the dataset grows by 50MB per week the pipeline will eventually face some out of memory issues. Perhaps only one week's worth of data could be processed at a time, but if not it may be safer to opt for Spark.

### Skills within the team

What if nobody within the project team has experience in using Python or R, let alone Spark?

It is well worth getting started with learning Python or R because these languages will be used by analysts in government, as well as outside government, for a long time. 

In the meantime, an alternative option for processing data on most platforms is SQL. This is particularly useful if someone in the team already knows SQL, but it can also be quicker and easier to pick up than Python or R. Hive and Impala SQL make use of parallel processing to speed up the queries. 

Excel is also often available to government analysts. However, note that Excel is not suitable for processing large amounts of data, neither is it a suitable tool for complex processes. There is a relatively large amount of risk involved with processing data using Excel compared with Python or R, therefore, use with caution.

### What do you want to do with the data?

For this section we will loosely define typical reasons to process data.

**Data engineering** generally refers to preparing data ready for analysis. Sometimes it involves simple operations done to a high standard, but it can also include more complex processes. Data engineers are also typically associated with big data. Therefore modern engineers might use PySpark and SQL.

Data science can be quite a broad term so we will use **machine learning** to be more specific. There are many packages available in Python and R that make machine learning more accessible to analysts. Therefore as long as the datasets are not too large Python or R would be first choice. Failing that, Spark does have machine learning libraries that are better suited for machine learning on big data.

**General analysis** can be anything else. If it is a quick answer you are after, consider Impala SQL to run simple queries on tables if this is available. For anything more complex the rich packages available with Python and R make these the best options. Again, if the data are too big for Python and R, go for the big data versions, PySpark or sparklyr.

### Combination of tools that work for you

In practice, within a modern data platform environment people will probably use a combination of these tools to complete some analysis. Below are some examples of how this might work, particularly in government.

Typical usage:

- A project team could use PySpark to process a large admin dataset through various stages that make up a data pipeline.
- Throughout the various stages of the pipeline the data are written out to the file system or database in the form of tables, these are called intermediate outputs.
- Some simple quality assurance checks are carried out on the intermediate outputs using Impala SQL to make sure the pipeline is producing the desired result.
- Towards the latter stages of the pipeline the data are aggregated and therefore small enough to use the pandas library in Python instead of PySpark.
- The final outputs are quality assured and disclosure checked using Excel before being exported out of the secure environment.

Alternatives to the example above might be using sparklyr and R instead of PySpark and Python. Or perhaps use Impala/Hive SQL to do all of the heavy lifting (processing large datasets) and use Python or R once the data has been aggregated to an appropriate size.

### Further Resources

External links:
- [Choosing tools and infrastructure to make better use of your data](https://www.gov.uk/guidance/choose-tools-and-infrastructure-to-make-better-use-of-your-data)
- [QA of analysis and research in government](https://best-practice-and-impact.github.io/qa-of-code-guidance/intro.html)

Links within this book:
- [Sampling](../spark-functions/sampling)
- [Unit tests](../testing-debugging/unit-testing)