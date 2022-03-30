# covr is intended to be used on packages but can be
#   adapted to be used for projects in development.
# This script will run a coverage report for each file in
#   the functions directory using mapply

# Set the parent working directory if not already set
test_wd <- "/ons-spark/testthat-for-sparklyr"

if(stringr::str_sub(getwd(), -nchar(test_wd)) != test_wd){
    setwd(paste0("./", test_wd))
}

# Temporarily set working directory to be tests, to match the run_tests.R
withr::with_dir("./tests", {
    # Load the user defined global functions
    source("setup_spark.R")

    # Files and tests in a vector
    files <- c("../functions/basic_functions.R",
               "../functions/dataframe_functions.R",
               "../functions/more_functions.R")
    tests <- c("test_basic.R",
               "test_dataframe.R",
               "test_more.R")

    # Run file_coverage for each file
    # Can alternatively use purrr::pmap() if you prefer the syntax
    mapply(covr::file_coverage, files, tests)
})