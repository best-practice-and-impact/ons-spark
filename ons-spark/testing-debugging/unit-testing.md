## Unit Testing in Spark

This article gives an overview of the [Pytest for PySpark](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/) and [testthat for sparklyr](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/) repositories, which give examples of unit testing in Spark. These are stored in the same [GitHub repository](https://github.com/best-practice-and-impact/ons-spark/) as the raw files for this book.

### What is unit testing?

Unit testing is where individual functions are tested to ensure that example inputs match example outputs. For example, if writing a function that returns the square of a number, you may check some small examples covering different scenarios, e.g. that $f(4) = 16$, $f(-1) = 1$, etc. If the function returns the expected outputs then the unit tests pass and you can be confident that your function works correctly. Ease of testing is one of many good reasons to write your code as [functions](https://best-practice-and-impact.github.io/qa-of-code-guidance/core_programming.html#functions) where possible.

Pytest and testthat both allow automation of unit tests, so rather than having to run testing functions manually you can set up testing functions in code files and run all the tests with a simple command. It is also possible to automate unit testing as part of a [CI/CD](https://www.atlassian.com/continuous-delivery/principles/continuous-integration-vs-delivery-vs-deployment) workflow; the tests will get automatically ran when pushed and the updated software can be delivered to the customer automatically.

Many programmers already informally test their functions when coding. For instance, you may work with a small sample of the data and check that the outcome is as expected at different stages. Unit testing just adds a formal structure around this to assist you and your colleagues.

A full explanation of unit testing is not given in this book, although the examples given should be enough for a Spark programmer to get started even if they have not written a unit test before. Instead, the repositories focus on practical usage and assumes that you are aware of the basic concept of unit testing. If you are new to unit testing, the following are recommended, which includes links from the [QA of Code for Analysis and Research: Unit Testing](https://best-practice-and-impact.github.io/qa-of-code-guidance/testing_code.html#unit-testing):
- [Introduction to Unit Testing](https://learninghub.ons.gov.uk/enrol/index.php?id=539): available to UK Civil Servants only; your organisation may have similar introductory training
- [Writing good unit tests in Python with ease](https://mitches-got-glitches.medium.com/writing-good-unit-tests-in-python-with-ease-5fb6d7aa2b77) by [Mitch Edmunds](https://github.com/mitches-got-glitches). Although this uses Python, R users may also find this useful as the general principles apply to unit tests in any programming language. There is an accompanying [repository](https://github.com/mitches-got-glitches/testing-tips).
- [testthat: getting started with testing](https://vita.had.co.nz/papers/testthat.pdf) by Hadley Wickham
- [R Packages: Testing](https://r-pkgs.org/tests.html): overview of testthat
- [Pytest: Get Started](https://docs.pytest.org/en/latest/getting-started.html)
- [Real Python: Getting Started With Testing in Python](https://realpython.com/python-testing/)

Some developers take the concept of unit testing a stage further, and develop their code without the actual source data and instead write a series of functions that are unit tested. This is referred to as [*test driven development*](https://www.agilealliance.org/glossary/tdd).

### Unit Testing in PySpark

The following section is for PySpark users and explains how to use [Pytest for PySpark](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/).

<details>

<summary><b>Unit Testing in PySpark</b></summary>

<br>

#### Why use Pytest?

[Pytest](https://docs.pytest.org/en/stable/) is easier to use than Pythons default [`unittest`](https://docs.python.org/3/library/unittest.html) module. The issue with unit testing PySpark code is that you need to set up a Spark session; Pytest lets you easily do this with a [*fixture*](https://docs.pytest.org/en/6.2.x/fixture.html).

Pytest can be installed in the usual way. If you are using CDSW at the ONS, ensure that you are installing with Python 3 with `pip3 install pytest`. You will also want to ensure that [`pytest-mock`](https://pypi.org/project/pytest-mock/) and [`mock`](https://docs.python.org/3/library/unittest.mock.html) are installed for mocking, and [`pytest-cov`](https://pytest-cov.readthedocs.io/en/latest/) to look at code coverage. All four of these can be installed with the [requirements](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/requirements.txt) file within this repository using `pip install -r requirements.txt` (replace `pip` with `pip3` if using CDSW).

#### Test Structure

This example has four modules, stored in a directory named [`functions`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/functions/). Each has a suite of tests, stored in a [`tests`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/) directory, as well as configuration files and a [`README`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/README.md) at the top level:

- [`functions`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/functions/):
    - [`basic_functions.py`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/functions/basic_functions.py)
    - [`dataframe_functions.py`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/functions/dataframe_functions.py)
    - [`mock_methods.py`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/functions/mock_methods.py)
    - [`more_functions.py`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/functions/more_functions.py)
- [`tests`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/):
    - [`__init__.py`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/__init__.py)
    - [`conftest.py`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/conftest.py)
    - [`helpers.py`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/helpers.py)
    - [`test_basic.py`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/test_basic.py)
    - [`test_dataframe.py`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/test_dataframe.py)
    - [`test_mock_methods.py`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/test_mock_methods.py)
    - [`test_more.py`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/test_more.py)
- [`pytest.ini`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/pytest.ini)
- [`README.md`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/README.md)
- [`requirements.txt`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/requirements.txt)

Note that all the test modules begin with `test_*`; Pytest will also discover them if they end in `*_test`. The tests in [`test_mock_methods.py`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/test_mock_methods.py) are organised into *classes*; this is optional in Pytest but can make your code easier to read.

If you are using this structure it is essential that you include an [`__init__.py`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/__init__.py) file in the tests directory. This can be blank. Without it, Pytest will not be able to correctly import modules and your tests will not even compile.

Fixtures contained in [`conftest.py`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/conftest.py) can be used in any of the testing modules, without having to be specifically imported. This is the most logical place to put fixtures which have a `session` scope, including the fixture which defines the Spark session. As we want to store the test data as close to the test as possible, if your fixtures are not used in more than one module store them in that module rather than `conftest.py`.

Custom functions are stored in [`helpers.py`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/helpers.py). The key difference between this and `conftest.py` is that these are functions, not fixtures, and are imported in the usual way, e.g. `from tests.helpers import assert_pyspark_df_equal`.

#### Writing Tests

Writing unit tests for PySpark with Pytest is the same as writing a normal unit test, just with the additional challenge that a Spark session is needed to run the tests. To adapt Pytest for PySpark, a *fixture* needs to be added with scope `session` in [`conftest.py`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/conftest.py) that defines the Spark session. You can then pass this into each test. See the example code for more information on this.

#### Running Tests

To run the unit tests when in a container, open a terminal window and run `pytest`. This will automatically discover the tests and run them. You can also run them through the Python console with `!pytest`.

You can run a single module of tests with `pytest test_module_name.py` and run an individual test with `pytest test_module_name.py::test_name`. This can be useful when you have a large test suite and are only changing one module or function.

There are several options that you can specify when using Pytest. `pytest -v` will list the full names of the tests and if they passed or not and `pytest -vv` will give you the full output. You can find a full list of options with `pytest -h`.

#### Example Tests

The example tests cover several common scenarios, although they are far from exhaustive. Note that these unit tests use [manually created DataFrames](../spark-overview/creating-dataframes).

For Pytest to discover tests, they must begin with `test_`. Optionally they can be grouped into parent classes, which are in `CamelCase` and begin `Test`.

[Basic Tests](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/test_basic.py): gives some simple examples:
- `test_count_animal`: simple scalar equality
- `test_format_columns`: checks that the output columns have the correct name and order
- `test_format_columns_unordered`: as above, but columns can be in any order

[DataFrame Tests](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/test_dataframe.py): shows three different ways to test PySpark DF equality:
- `test_group_animal_collect`: tests DF equality using `.collect()`
- `test_group_animal_toPandas`: tests DF equality by using `.toPandas()` then `assert_frame_equal()`
- `test_group_animal_pyspark`: tests DF equality with a function that can be customised

You may want to investigate the [`chispa`](https://github.com/MrPowers/chispa) package for another way to check DataFrame equality if using Spark 3.0 or above.

[Mocking Tests](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/test_mock_methods.py): provides examples using the `mock` module; these are grouped into classes:
- `TestCheckIsFirstOfMonth`:
    - `test_check_if_first_of_month`: mocks `datetime` to return `True`
    - `test_check_if_not_first_of_month`: mocks `datetime` to return `False`
- `TestReadCsvFromCdsw`:
    - `test_read_csv_from_cdsw`: uses `assert_called_with` a mocked file name, to verify that the function is called
- `TestReadCsvFromHdfs`:
    - `test_read_csv_from_hdfs`: as above, but uses Spark rather than pandas
- `TestOpenJson`:
    - `test_open_json`: as above, but reading a JSON file
- `TestJsonToDictionary`:
    - `test_json_to_dictionary`: mocks the reading of the dictionary with the one specified in `keywords`

[More Tests](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/test_more.py): covers errors, mocking, data types, parametrisation and an example of Test Driven Development:
- `test_analysis_exception`: tests that the code raises an error
- `test_read_and_format_rescue`: uses mocking instead of reading from HDFS and tests data types
- `test_count_animal_parametrise`: example of parametrisation; generalised version of `test_count_animal`
- `TestAddSquareColumn`: this class contains six tests, all of the same format, which were written using [*test driven development*](https://www.agilealliance.org/glossary/tdd), where the tests are written *before* the code:
    - `test_add_square_column_small`
    - `test_add_square_column_null_identity`
    - `test_add_square_column_large`
    - `test_add_square_column_decimal`
    - `test_add_square_column_negative`

#### Ignoring Warnings

Your tests will return either `passed` or `failed` for each test. In addition, you may get warnings. Sometimes these contain useful information about the code, that prompts you to correct some potential problems. They warnings can however be superfluous, for instance, if the warning is not relevant to you or is from another package. You can create a [`pytest.ini`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/pytest.ini) file in the parent directory, with instructions to ignore certain types of warnings. If running this example at the ONS with CDSW, there is a `DeprecationWarning` from a built in Cloudera package, plus a `RuntimeWarning`; both are ignored through providing a partial string match. Do not just ignore entire classes of errors; warnings exist for a reason!

#### Mocking

Unit testing functions which take an input and produce an output, without any side effects, are relatively straightforward and when developing with unit tests in mind it is useful to try and write functions in this way. Some functions do have side effects; for instance, reading from a file on HDFS or another data source, or writing out to a log file. When unit testing we do not want to read or write files; instead, we can use the concept of [*mocking*](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/tests/test_mock_methods.py).

Mocking enables you to alter the behaviour of objects in your code. So for instance, rather than read a file which returns a PySpark DataFrame, you can specify a different DataFrame which will be returned instead. See the example in `test_read_and_format_rescue`. Another use of mocking is to replace calls to the current date and time with a fixed value (see `test_check_if_first_of_month`).

The example in `test_read_and_format_rescue` uses the `pytest-mock` module which can be installed with pip. This works essentially as a wrapper to the `mock` module used in `test_mock_methods.py`; which one to use is personal preference.

This only scratches the surface of what mocking can do. The documentation for [pytest-mock](https://pypi.org/project/pytest-mock/) and [mock](https://docs.python.org/3/library/unittest.mock.html) give more detail on this.

#### Parametrisation

`test_count_animal_parametrise` is an example of parametrisation. Whereas `test_count_animal` only checked that `"Cat"` was 3, here the test is generalised to check other animals too in a succinct manner.

#### Coverage

Ideally unit tests should cover as much of your code as possible. There is an automated way to check what percentage of each module is covered, using the [`pytest-cov`](https://pytest-cov.readthedocs.io/en/latest/) module.

To run, open a terminal window and run `pytest --cov functions`, where `functions` is the name of the directory where your modules are stored.

This will return a report showing what percentage of each module is covered by unit tests. Obviously, the higher the percentage the better, but there is no standard percentage to aim for: each project is different and some will have more coverage than others. For instance, a module which covers reading and writing data from HDFS or another data source will often have less coverage than one with pure statistical functions.

#### `chispa`: Checking DataFrame Equality

The [`chispa`](https://github.com/MrPowers/chispa) package contains methods that can be used to test PySpark DataFrame equality. However, as a dependency it will install Spark 3, so if you are using Spark 2.4 or earlier you will have to manually uninstall this and revert back. If you are an experienced user or are using Spark 3 you may want to investigate this package further.

</details>

### sparklyr Unit Testing

The following section is for sparklyr users and explains how to use [testthat for sparklyr](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/).

<details>

<summary><b>Unit Testing in sparklyr</b></summary>

<br>

There is only a small amount of information available online for best practice when unit testing sparklyr code. As such this repository is in development and we encourage any contributions or suggestions; please do this by raising an [issue](https://github.com/best-practice-and-impact/ons-spark/issues) or [pull request](https://github.com/best-practice-and-impact/ons-spark/pulls) on [GitHub](https://github.com/best-practice-and-impact/ons-spark).

#### Why use testthat?

[`testthat`](https://testthat.r-lib.org/) is the most popular unit testing package available for R. It is connected to the [`tidyverse`](https://www.tidyverse.org/) suite of packages, along with [`dplyr`](https://dplyr.tidyverse.org/) and [`sparklyr`](https://spark.rstudio.com/). `testthat` can be used with sparklyr code by setting up a local Spark connection in a setup file.

You can install `testthat` in the usual way with `install.packages("testthat")`. It is recommended to install `tidyverse` first; also ensure that you have installed `sparklyr`. You can also install [`covr`](https://github.com/r-lib/covr#readme) if you want to check [code coverage](#Coverage).

#### Test Structure

This example has three modules, stored in a [`functions`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/functions/) directory. Each has a suite of tests, stored in a [`tests`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/tests/) directory:

- [`functions`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/functions/)
    - [`basic_functions.R`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/functions/basic_functions.R)
    - [`dataframe_functions.R`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/functions/dataframe_functions.R)
    - [`more_functions.R`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/functions/more_functions.R)
- [`tests`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/tests/)
    - [`setup_spark.R`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/tests/setup_spark.R)
    - [`test_basic.R`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/tests/test_basic.R)
    - [`test_dataframe.R`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/tests/test_dataframe.R)
    - [`test_more.R`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/tests/test_more.R)
- [`coverage.R`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/coverage.R)
- [`README.md`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/README.md)
- [`run_tests.R`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/run_tests.R)

Note that all the test modules begin with `test_*`. `testthat` will discover any file that begins `test*`; adding the underscore makes the context clearer (more detail in [Running Tests](running-tests-sparklyr)). Unlike unit testing in Python, the files will not be discovered if they end solely in `*test`.

`testthat` will import functions in any file beginning `setup_*`, without having to individually source them in the test modules. This is the most logical place to put a function that contains the local Spark setup. As we want to store the test data as close to the test as possible, if the intention of your functions is to only use them in one module store them in that module rather than in `setup_*`; `test_sum_animal()` is therefore contained only in `test_more`, whereas `expect_sdf_equal()` is in `setup_spark` as this is a wrapper for testing DataFrame equality in Spark and therefore it is desirable to have this available globally.

[`coverage.R`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/coverage.R) can be ran to check code coverage. See the section on [Coverage](coverage-sparklyr) for more details.

#### Writing Tests

The [R Packages](https://r-pkgs.org/) chapter on [testing](https://r-pkgs.org/tests.html#test-tests) covers how to write a unit test using testthat. To adapt this to sparklyr, a local Spark session needs to be created for each test, using `testthat_spark_connection()` in [`setup_spark.R`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/tests/setup_spark.R). See the example code for more information on this.

(running-tests-sparklyr)=
#### Running Tests

To run the unit tests, ensure that your working directory is set correctly (if cloning the `ons-spark` repository, that will be `setwd("./ons-spark/testthat-for-sparklyr")`, then type `testthat::test_dir("./tests")` into the R console (if using a container ensure that you are not using the terminal window). Referencing the package directly with `::` means it does not have to be imported using `library()` or `require()` and can therefore be ran with one simple command.

The wrapper code in [`run_tests.R`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/run_tests.R) script will automatically set the working directory and run the tests.

The output lists the number of tests that passed, failed, warned and skipped for each file in turn, plus the duration:
```
testthat::test_dir("./tests")
✔ |  OK F W S | Context
✔ |   3       | basic [22.9 s]
✔ |   2       | dataframe [6.0 s]
✔ |   7       | more [7.6 s]

══ Results ═════════════════════════════════════════════════════════════════════
Duration: 36.5 s

[ FAIL 0 | WARN 0 | SKIP 0 | PASS 12 ]
```

The full name of the module will be listed under `Context`, unless it begins with `test_`, e.g. `test_basic.R` becomes `basic`. The duration of the first test took longer, this is due to the setting up of the Spark session, `sc`. Future tests are able to reuse the same session and so run faster.

To run a single test module, use `testthat::test_file()`.  This can be useful when you have a large test suite and are only changing one module or function.

#### Example Tests

The example tests cover several common scenarios, although they are far from exhaustive.  Note that these unit tests use [manually created DataFrames](../spark-overview/creating-dataframes).

[Basic Tests](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/tests/test_basic.R): gives some simple examples
- `test count_animal`: simple scalar equality
- `test format_columns`: checks that the output columns have the correct name and order
- `test format_columns unordered`: as above, but columns can be in any order

[DataFrame Tests](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/tests/test_dataframe.R): shows two different ways to test sparklyr DF equality
- `test group_animal`: tests DF equality using [`collect()`](https://dplyr.tidyverse.org/reference/compute.html) and [`arrange()`](https://dplyr.tidyverse.org/reference/arrange.html), to ensure the DFs are sorted identically
- `test group_animal using function`: uses a wrapper for [`expect_equal()`](https://testthat.r-lib.org/reference/equality-expectations.html), `expect_sdf_equal()`, which takes sparklyr DFs as an input and will collect and arrange

[More Tests](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/tests/test_more.R): covers errors and parameterisation
- `test analysis exception`: tests that the code raises an error
- `test sum_animal with multiple expectations`: example of multiple `expect_equal()` in the same test, including comparing `NA` values
- `test sum_animal parameterised`: example of parameterisation

#### Parametrisation

`test sum_animal parameterised` is an example of parametrisation. You can use the `apply` family of functions from base R, or make use of the [`purrr`](https://purrr.tidyverse.org/index.html) package depending on your preference. This example uses [`mapply()`](https://stat.ethz.ch/R-manual/R-devel/library/base/html/mapply.html) with two input vectors; [`walk2()`](https://purrr.tidyverse.org/reference/map2.html) is the `purrr` equivalent.

(coverage-sparklyr)=
#### Coverage

Ideally unit tests should cover as much of your code as possible. There is an automated way to check what percentage of each module is covered, using the [`covr`](https://github.com/r-lib/covr#readme) package.

`covr` is designed for full packages but can be adapted for files using `covr::file_coverage` with `mapply()` or [`purrr::pmap()`](https://purrr.tidyverse.org/reference/map.html), with a list of files and tests as inputs. This is contained in the [`coverage.R`](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/coverage.R) script. It will return a report for each file, showing what percentage of each module is covered by unit tests. Obviously, the higher the percentage the better, but there is no standard percentage to aim for: each project is different and some will have more coverage than others. For instance, a module which covers reading and writing data from HDFS or another data source will often have less coverage than one with pure statistical functions.

</details>

### Other Tests

Your code should also be tested in other ways. This is important as your individual functions could all be fully unit tested, but do not work when ran together; you would perform an integration test to check for this. Other common tests include user acceptance testing (UAT), where users of the system verify that it fits their requirements, and regression testing, where you compare outputs between existing and new code. Some tests are very simple: checking that the outputs are sensible is called a sanity test.

The [QA of Code for Analysis and Research](https://best-practice-and-impact.github.io/qa-of-code-guidance) has a good [overview of testing code](https://best-practice-and-impact.github.io/qa-of-code-guidance/testing_code.html) which gives an introduction to the different types of tests.

### Further Resources

Spark at the ONS Articles:
- [Creating DataFrames Manually](../spark-overview/creating-dataframes)

[Spark at the ONS GitHub repository](https://github.com/best-practice-and-impact/ons-spark):
- [Pytest for PySpark](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/pytest-for-pyspark/)
- [testthat for sparklyr](https://github.com/best-practice-and-impact/ons-spark/blob/main/ons-spark/testthat-for-sparklyr/)
- [Issues](https://github.com/best-practice-and-impact/ons-spark/issues): if you notice something that you believe needs improving then submit an issue
- [Pull Requests](https://github.com/best-practice-and-impact/ons-spark/pulls): submit a pull request to contribute your own code to the repository

[QA of Code for Analysis and Research](https://best-practice-and-impact.github.io/qa-of-code-guidance/):
- [Unit Testing](https://best-practice-and-impact.github.io/qa-of-code-guidance/testing_code.html#unit-testing)
- [Functions](https://best-practice-and-impact.github.io/qa-of-code-guidance/core_programming.html#functions)

Unit Testing Background Reading:
- [Atlassian: CI/CD](https://www.atlassian.com/continuous-delivery/principles/continuous-integration-vs-delivery-vs-deployment)
- [Writing good unit tests in Python with ease](https://mitches-got-glitches.medium.com/writing-good-unit-tests-in-python-with-ease-5fb6d7aa2b77)
- [Testing Tips repository](https://github.com/mitches-got-glitches/testing-tips)
- [testthat: getting started with testing](https://vita.had.co.nz/papers/testthat.pdf) by Hadley Wickham
- [R Packages: Testing](https://r-pkgs.org/tests.html): overview of testthat
- [Pytest: Get Started](https://docs.pytest.org/en/latest/getting-started.html)
- [Real Python: Getting Started With Testing in Python](https://realpython.com/python-testing/)
- [Agile Alliance: TDD](https://www.agilealliance.org/glossary/tdd): linked through from the [Government Data Service blog on pair programming](https://gds.blog.gov.uk/2018/02/06/how-to-pair-program-effectively-in-6-steps/)

Python Packages Documentation:
- [Pytest](https://docs.pytest.org/en/stable/):
    - [Fixtures](https://docs.pytest.org/en/6.2.x/fixture.html)
- [`unittest`](https://docs.python.org/3/library/unittest.html)
- [`pytest-mock`](https://pypi.org/project/pytest-mock/)
- [`mock`](https://docs.python.org/3/library/unittest.mock.html)
- [`pytest-cov`](https://pytest-cov.readthedocs.io/en/latest/)
- [`chispa`](https://github.com/MrPowers/chispa)

R Package Documentation:
- [`testthat`](https://testthat.r-lib.org/) 
- [`tidyverse`](https://www.tidyverse.org/)
- [`dplyr`](https://dplyr.tidyverse.org/)
- [`sparklyr`](https://spark.rstudio.com/).
- [`covr`](https://github.com/r-lib/covr#readme)

sparklyr and tidyverse documentation:
- [`expect_equal()`](https://testthat.r-lib.org/reference/equality-expectations.html)
- [`collect()`](https://dplyr.tidyverse.org/reference/compute.html)
- [`arrange()`](https://dplyr.tidyverse.org/reference/arrange.html)
- [`pmap()`](https://purrr.tidyverse.org/reference/map.html)

UK Civil Service Learning:
- [Introduction to Unit Testing](https://learninghub.ons.gov.uk/enrol/index.php?id=539): available to UK Civil Servants only

### Acknowledgements

Special thanks to:
- Peter Derrick for contributing the `test_mock_methods.py` and `mock_methods.py` modules
- [Mitch Edmunds](https://github.com/mitches-got-glitches) for sharing his unit testing [articles](https://mitches-got-glitches.medium.com/writing-good-unit-tests-in-python-with-ease-5fb6d7aa2b77) and [repository](https://github.com/mitches-got-glitches/testing-tips)
- [Neil Currie](https://github.com/neilcuz) for his suggestions on using testthat for sparklyr
- [Ian Banda](https://github.com/bandaian) for his suggestions on using testthat for sparklyr
