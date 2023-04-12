# Contributing

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given. You can contribute in the ways listed below.

## Report Bugs

Report bugs using GitHub issues.

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

## Fix Bugs

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

## Implement Features

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

## Write Documentation

Spark at the ONS could always use more documentation, whether as part of the
official Spark at the ONS docs, in docstrings, or even on the web in blog posts,
articles, and such.

## Submit Feedback

The best way to send feedback is to file an issue on GitHub.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

## Get Started

Ready to contribute? Here's how to set up `Spark at the ONS` for local development.

1. Fork the repo on GitHub.
2. Clone your fork locally.
3. Install your local copy into a virtualenv, e.g., using `conda`.
4. Create a branch for local development and make changes locally.
5. Commit your changes and push your branch to GitHub.
6. Submit a pull request through the GitHub website.


## Environment Set-up

This repository is built using a relatively complex set of dependencies, including both Python and R programming languages and Spark. A list of environment requirements is below:
- Python >= 3.6 
- R >=3.5,<4
- git
- Java Development Kit (for Spark) >=1.8.0_121
- PySpark >= 2.4.0
- sparklyr >= 1.7.5

other versions of Spark 2 may be compatible but have not been tested. 

## Building the Book

As mentioned in the README in the root of this repo. To build this book you'll need python installed. Once Python installed, then install its Python dependencies like so:

```bash 
git clone https://github.com/best-practice-and-impact/ons-spark.git
cd ons-spark 
pip install -r requirements.txt
```

Now all the appropriate dependencies should be installed we can now build the book locally. This is done by running the following command within the root of the repository. 

```bash
jb build ons-spark
```

## Contributing to Notebooks to contain both Python and R code

The conversion of notebook files into markdown files that have code tabs to display both Python and R code requires the use of some of the functionality contained in the utilities folder of this repo. 
`notebook_converter.py` contains the function markdown_from_notebook that (as the name suggests) will:

- convert a Jupyter Notebook into a Markdown file with appropriate code tabs
- extract and run the R code
- store both python and R outputs and put them in appropriate tabs in the notebook. 

This function takes as an argument the notebook that is to be converted and the output location of where you would like the resulting markdown file. 


N.B. it is not neccessary to convert ALL notebooks, only ones that you would like to show code examples of both Python and R code. For example notebooks in the PySpark specific section (i.e. not relevant to Sparklyr and therefore not containing any R code) can remain as notebooks and JupyterBook will include them in the book without any issue. 

Pages that contain code examples in both Python and R have been converted using the above mentioned function in the utilities folder of this repo. And as a result the notebooks must be correctly formatted in order for the converter to work correctly. For any code that you wish to include in both languages, place the Python code in a code cell in the notebook as normal. Place the R code in a markdown cell directly below the Python code cell contained between `` ```r and ``` ``.  The notebook converter function uses these symbols as a marker to produce the R code tabs and R output tabs.

For example, if you like the code to start a local spark session to be displayed in both Python and R, you would place the following in a code cell of a jupyter notebook:
```python 
from pyspark.sql import SparkSession

spark = (SparkSession.builder.master("local[2]")
         .getOrCreate())

```
An in a markdown cell right below you would have the following code:

~~~
```r
library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    config = sparklyr::spark_config())
``` 
~~~




Once you have correctly formatted the notebook it can then be converted into a markdown file by the converter. 

An example of the Python code required to accomplish this conversion can be seen in [convert](ons-spark\utilities\convert.py) but will be shown here. 

In this example we would like to convert `checkpoint-staging.ipynb`. 

```python
group = "spark-concepts"
folder = "checkpoint-staging"
page = "checkpoint-staging"
base_path = "/home/cdsw/ons-spark/ons-spark/"
out_path = base_path + group

in_path = base_path+"raw-notebooks/"+folder

nb_maker = (markdown_from_notebook(in_path + "/" + page + ".ipynb",
                                   out_path + "/" + page + ".md",
                                   in_path + "/r_input.R",
                                   in_path + "/outputs.csv",
                                   show_warnings=False,
                                   output_type="tabs")
)

```

Once you have converted the notebook and the resulting files are stored in the right locations, you can now build the book with your new changes. 

In the case that you are adding R code to a page in the book that is currently a Jupyter Notebook you will need to change the table of contents to point to the markdown file created by the conversion. To do this, inside the ons-spark folder modify the ```_toc.yml``` file such that the newly modified markdown file is included correctly. To learn a little more about Table of Contents in JupyterBooks see the Jupyter documentation [here](https://jupyterbook.org/en/stable/structure/toc.html). 


## Publishing changes

Internal contributors can trigger a new release of the book.

### Preparation

To create a new release and publish the `main` branch, you will need to install the dependencies:

```
pip install -r requirements.txt
```

### Releasing

To create a new release, use the command line tool `bump2version`, which will be installed with the dev dependencies.
The version number references the current `year` and an incremental `build` count.

For a the first release of a year, provide the `year` as the command argument, otherwise provide `build`.

```
bump2version build
```

`bumpversion` will create a new Git `tag` and `commit`.
If you're happy with the version increase, `push` these to the remote to trigger the publication, by running both:

```
git push
git push --tags
```



## Code of Conduct

Please note that the Spark at the ONS project is released with a [Contributor Code of Conduct](CONDUCT.md). By contributing to this project you agree to abide by its terms.
