# Contributing

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given. To contribute, you will first need to raise the change you wish to make or suggest as an [Issue](https://github.com/best-practice-and-impact/ons-spark/issues).  

## Raising an issue
We have made use of GitHub issue forms which we hope will make suggesting content or changes even easier!
you want to suggest a new page, please use [this link for the issue form](https://github.com/best-practice-and-impact/ons-spark/issues/new?assignees=&labels=New+page&projects=&template=new-page-form.yml&title=%5BNew+page%5D%3A+)
For bug issues please use [this link for the issue form](https://github.com/best-practice-and-impact/ons-spark/issues/new?assignees=&labels=bug&projects=&template=bug-report-form.yml&title=%5BBug%5D%3A+)
To create any other type of issue, select the [blank issue form](https://github.com/best-practice-and-impact/ons-spark/issues/new/choose) and label your issue with any of the following tags:

| label    | description |
| - | - |
| Bug | Used for general bug reports |
| Documentation | Improvements or additions to documentation |
| Duplicate | This issue or pull request already exists |
| Help wanted | External help or guidance (outside of DAPCATS) is needed before this can be implemented |
| Question | Further information is needed or something needs clarification |
| New page | Suggestions for new topics and pages |
| Addition to existing page | Suggestions to add information to existing pages e.g. Python/R equivalent code, new subsection etc. |
| Enhancement | Suggestion for change not relating to content eg. improving the usability or appearance of the book |
| Other | An issue that is not covered by any of the other labels |

## Working on an issue
If you wish to address an outstanding issue yourself (whether that be a bug, a new page, or anything else) you should follow this process:

1. **Create a new branch from the issue page:** On the right hand side of the issue page, there should be a link to 'Create a branch' from the issue under the development section. We recommend that you do this so that branches can be easily linked to issues.

2. **Make your changes on the newly created branch**: We recommend that contributions to the book be made locally, by cloning the repository on your local machine. Details of how to do this are given in the [Cloning this repository](https://github.com/best-practice-and-impact/ons-spark/edit/contributing-improvements/CONTRIBUTING.md#cloning-this-repository) section below.
  
   You may choose to write or edit book pages in either Jupyter notebook format (.ipynb) or markdown (.md). However, we require that finalised book content is saved into the book repository in **both** formats and that raw Python and R scripts are also available. You can generate all of the required files either from a .ipynb file by running the notebook converter utility or from the .md file by running the reverse converter     utility. See the relevant sections of this guidance ([Converting .ipynb files](https://github.com/best-practice-and-impact/ons-spark/edit/contributing-improvements/CONTRIBUTING.md#converting-ipynb-files) or [Converting .md files](https://github.com/best-practice-and-impact/ons-spark/edit/contributing-improvements/CONTRIBUTING.md#converting-md-files)) for more details of how to do this. 

3. **Commit your changes. Check that you can build the book locally and that pages display correctly:** Check the [Building the book](https://github.com/best-practice-and-impact/ons-spark/edit/contributing-improvements/CONTRIBUTING.md#building-the-book) section for instructions on how to do this. When you are happy with your local changes, you can **push your branch to Github**.
   
4. **Open a pull request and request a review:** The main branch of this repository is protected, so an approving review is required before changes can be merged in. Please fill out the pull request template with as much detail as possible so that your request can be reviewed properly. 
   
5. **Merge in your branch and delete it once changes have been approved:** We also recommend using the 'squash commits' option to keep the main branch commit history as clean and easy to read as possible.

## Cloning this repository

## Converting .ipynb files

## Converting .md files

## Building the book

1. Ensure that python is installed and you have cloned the repository as described above.
2. Run `pip install -r requirements.txt` (it is recommended you do this within a virtual environment)
3. Run `jupyter-book clean ons-spark/` to remove any existing builds
4. Run `jupyter-book build ons-spark/`
5. Check the built HTML pages are correct (these will be in ons-spark/_build/html)

## Environment Set-up

This repository is built using a relatively complex set of dependencies, including both Python and R programming languages and Spark. A list of environment requirements is below:
- Python >= 3.6 
- R >=3.5,<4
- git
- Java Development Kit (for Spark) >=1.8.0_121
- PySpark >= 2.4.0
- sparklyr >= 1.7.5

other versions of Spark 2 may be compatible but have not been tested. 


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
