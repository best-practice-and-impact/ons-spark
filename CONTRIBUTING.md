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
If you wish to address an outstanding issue yourself (whether that be a bug, a new page, or anything else) you should follow one of the processes outlined below. Which one you should follow depends on whether you are an [internal contributor](CONTRIBUTING.md#internal-contributors) with permissions to create new branches in this repository (e.g. a DAPCATS team member) or whether you are an [external contributor](CONTRIBUTING.md#external-contributors).

### Internal contributors

1. **Create a new branch from the issue page:** On the right hand side of the issue page, there should be a link to 'Create a branch' from the issue under the development section. We recommend that you do this so that branches can be easily linked to issues.

2. **Make your changes on the newly created branch**: We recommend that contributions to the book be made locally, by cloning the repository on your local machine.
  
   You may choose to write or edit book pages in either Jupyter notebook format (.ipynb) or markdown (.md). However, we require that finalised book content is saved into the book repository in **both** formats and that raw Python and R scripts are also available. You can generate all of the required files either from a .ipynb file by running the notebook converter utility or from the .md file by running the reverse converter     utility. See the relevant sections of this guidance ([Converting .ipynb files](CONTRIBUTING.md#converting-ipynb-files) or [Converting .md files](CONTRIBUTING.md#converting-md-files)) for more details of how to do this. 

3. **Commit your changes. Check that you can build the book locally and that pages display correctly:** Check the [Building the book](CONTRIBUTING.md#building-the-book) section for instructions on how to do this. When you are happy with your local changes, you can **push your branch to Github**.
   
4. **Open a pull request and request a review:** The main branch of this repository is protected, so an approving review is required before changes can be merged in. Please fill out the pull request template with as much detail as possible so that your request can be reviewed properly. 
   
5. **Merge in your branch and delete it once changes have been approved:** We also recommend using the 'squash commits' option to keep the main branch commit history as clean and easy to read as possible.

### External contributors

1. **Fork this repository:** Since you will not have permissions to create a new branch to work on in this repository, you will first need to fork your own copy the repo. Click the 'Fork' button near the top of the page on the front page of this repository and make sure you select 'Copy the main branch only'.

2. **Create a new branch using the correct naming convention:** You will not have a copy of the repository issues in your new fork, so it is important that when you create your new branch, you name it clearly so it can be easily matched back to an issue. The preferred naming convention for this repository is to use <issue_number>-<title>-<of>-<issue>. So for example if you wanted to work on issue number 1 and it was titled "Contributing Guidance Needs Testing" in the issues list, your new branch would be called 1-contributing-guidance-needs-testing.

3. **Make your changes on the newly created branch**: We recommend that contributions to the book be made locally, by cloning the repository on your local machine.
  
   You may choose to write or edit book pages in either Jupyter notebook format (.ipynb) or markdown (.md). However, we require that finalised book content is saved into the book repository in **both** formats and that raw Python and R scripts are also available. You can generate all of the required files either from a .ipynb file by running the notebook converter utility or from the .md file by running the reverse converter     utility. See the relevant sections of this guidance ([Converting .ipynb files](CONTRIBUTING.md#converting-ipynb-files) or [Converting .md files](CONTRIBUTING.md#converting-md-files)) for more details of how to do this. 

4. **Commit your changes. Check that you can build the book locally and that pages display correctly:** Check the [Building the book](CONTRIBUTING.md#building-the-book) section for instructions on how to do this. When you are happy with your local changes, you can **push your branch to Github**.

5. **Open a pull request for your forked branch and request a review:** The main branch of this repository is protected, approving review from a DAPCATS team member is required before changes can be merged in. Please fill out the pull request template with as much detail as possible so that your request can be reviewed properly. It would be best to assign a reviewer to your pull request (it doesn't matter who, you can just use the suggested person Github recommends) so that we will receive an email alert that the is a pull request waiting for review.

    Please keep an eye on your pull request as we may ask for additional changes to be made before your changes are suitable for merging in. Once they are approved, a DAPCATS team member will merge your branch in. You may delete the branch on your forked repo after this is complete if you wish. 

## Converting `.ipynb` files

If you choose to develop pages in jupyter notebooks, these needs to also include R code (if appropriate) and converted using the `convert.py` script found in the utilities folder.
**Notebooks containing only Python code do not need to be converted and can be rendered into the book as .ipynb files**.

### R setup
As a quality assurance step the R code is executed during the conversion, therefore we need to have R and Spark setup in our local environment.
Information on spark setup can be found on the [sparklyR documentation](https://spark.posit.co/get-started/). **Note** some organisations (like the ONS) will have different setup instructions and those should be followed over external guidance. 

The notebook converter uses the subprocess package, as such we will need to have all the required packages installed in our R environment prior to converting. 

### Converting
To convert `ipynb` pages into a markdown page we will run the `convert.py` script.
This can either be run using VScode and the run Python script button, or in a terminal by running `python utilities/convert.py` assuming you are in the root directory of this repo.

The following code will convert the `ons-spark/raw-notebooks/groups-not-loops/groups-not-loops.ipynb` file into a markdown page named `groups-not-loops.md` located in `ons-spark/spark-concepts/` folder
```python
from notebook_converter import markdown_from_notebook
# Group: Which chapter does this work belong in once converted 
group = "spark-concepts"

# What is the name of the folder the .ipynb is located in 
folder = "groups-not-loops"

# What is the name of the .ipynb folder
page = "groups-not-loops"

# The path to the folder named above (can be relative or absolute path)
base_path = "ons-spark/"

# No changes are needed below this comment 
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

### Troubleshooting

If you are having issues with converting `.ipynb` files, you might have been getting an error raised in python explaining `unable to find Rscript, please set the path to this in your environment variables`.
To do this you will need to set the value `Rscript` in your windows environment variables for your account. 
Create a new variable with `variable name` as `Rscript` and `variable value` as the path towards your `Rscript.exe`, if you are using Rstudio this can be found in `C:/My_Rstudio/<r-version>/bin/x64/Rscript.exe`. You may need to restart your terminal or device after changing your environment variables. 

## Converting `.md` files

If you prefer, you can write up your contribution directly using markdown. You might choose to do this if you are simply editing text (rather than code) in the book. If the edits you make to the text are significant (e.g. some information was previously incorrect, you have added a lot of text etc.), you should follow this guide to convert your `.md` files to updated `.ipynb` and `.R` scripts to make sure the information is up to date in the notebook as well. 

You might also choose to use markdown if you are developing Python and/or R code as well as writing text. In this case, you should be aware that we use tabbed code chunks to write our pages, so you will need to match this formatting in your file. An example of how to do this can be found below:

``` 
        ````{tabs}
        ```{code-tab} py
        # Write your python code here
        ```
        ```{code-tab} r R 
        # Write your R code here
        ```
        ````
```

If you are still unsure, you can check another markdown file for a book page in this repository. Most book pages contain both Python and R code in this format, so you should be able to find example of the formatting to copy. 

### Converting

To convert `.md` pages into a `.ipynb` file and a `.R` file, we will run the `reverse_convert.py` script.

Before running the conversion, make sure you edit lines 14 and 17 in the code to specify the name of the page you are adding (`bpname`) and the relevant book folder or section your page belongs in (`fname`). You may need to edit the `base_path` in line 11 if you are external to ONS. The `base_path` variable should point to the `ons-spark` folder in the root of this repo.

The script can then either be run using VScode and the run Python script button, or in a terminal by running `python utilities/reverse_convert.py` assuming you are in the root directory of this repo.

The `reverse_convert.py` script does not run the `.R` or `.ipynb` files outputted automatically on conversion. We suggest that you run these files individually to check the functionality of the outputted scripts. The outputs can be found by navigating to the `ons-spark/raw-notebooks/<your_page_name>/` folder from the root of this repo.


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

To create a new release, use the command line tool [bump2version](https://pypi.org/project/bump2version/), which will be installed with the dev dependencies.
We use a year.build version tag for the release of the book, so the first release of 2024 would be `2024.0`.

For a the first release of a year, use the following bump version command.
Note you should be on the main branch when creating new versions.
```
 bump2version year
```

For any subsequent releases during the year, please bump the version with following command:

```
bump2version build
```

`bump2version` will create a new Git `tag` and `commit`. 
These are only created locally, and will need to be pushed.

```
git push
git push --tags
```

If you wish to view the current tags on a repo, run `git tag`.
If a tag has been produced a new tag/version by accident, you can delete this locally by running:
```
git tag --delete tagname
```

If this has already been pushed you can delete this from the remote repository by 
adding an additional origin and push to the above line as shown below:
```
git push --delete origin tagname
```


## Code of Conduct

Please note that the Spark at the ONS project is released with a [Contributor Code of Conduct](CONDUCT.md). By contributing to this project you agree to abide by its terms.
