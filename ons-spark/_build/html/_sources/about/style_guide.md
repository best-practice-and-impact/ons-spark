## Style Guide

This book was originally written at the Office for National Statistics. Contributions are welcome from anyone with an interest in Spark and big data. Ensure that all contributions adhere to the [Civil Service Code](https://www.gov.uk/government/publications/civil-service-code/the-civil-service-code); this includes non-Civil Servants.

This style guide is not overly prescriptive, but gives some general principles that are used throughout the book.

Useful general resources include:
- [Style.ONS](https://style.ons.gov.uk)
- [Government Digital Service: A to Z Style Guide](https://www.gov.uk/guidance/style-guide/a-to-z-of-gov-uk-style)
- [PEP 8 (Python style guide)](https://www.python.org/dev/peps/pep-0008)
- [Tidyverse style guide](https://style.tidyverse.org)
- [Wikipedia: Manual of Style](https://en.wikipedia.org/wiki/Wikipedia:Manual_of_Style)

### General

Individual articles may have a different tone depending on the lead author. Try and keep to the general tone when contributing to an exisiting article.

### Writing

When mentioning DataFrames, use **DataFrame** in full on the first reference, then **DF** in subsequent mentions.

### Coding

#### Python

Use `F.col("column_name")` rather than `df.column_name` when referencing a column.

#### R

It is recommended to reference packages directly when using functions, e.g. `sparklyr::read_spark_parquet()`. Note that `dplyr` will often be imported directly with `library(dplyr)`, due to the `%>%` operator.

```python

```
