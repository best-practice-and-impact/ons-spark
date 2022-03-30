library(sparklyr)
library(dplyr)

#'testthat Spark Connection
#'
#'Gets or creates a local Spark connection
#'
#'@returns sc, the Spark connection object
testthat_spark_connection <- function(){

    sc <- sparklyr::spark_connect(
        master = "local[2]",
        app_name = "testthat-sparklyr",
        config = sparklyr::spark_config())
    
    return(sc)
}

#'sparklyr DataFrames Comparison
#'
#'Wrapper to use expect_equal with sparklyr DataFrames
#'    Uses collect() to bring the DF into the CDSW session
#'    sparklyr DataFrames aren't ordered, so are non-deterministic;
#'    arrange(across()) ensures that they are sorted by every
#'    column before doing the comparison with expect_equal
#'
#'@section Correct Usage:
#'
#'Use this function in place of expect_equal for sparklyr DataFrames.
#'This function is not intended to have a return value
#'
#'@param actual_sdf Actual sparklyr DataFrame
#'@param expected_sdf Expected sparklyr DataFrame
expect_sdf_equal <- function(actual_sdf, expected_sdf){
    actual <- actual_sdf %>%
        sparklyr::collect() %>%
        dplyr::arrange(dplyr::across())
                
    expected <- expected_sdf %>%
        sparklyr::collect() %>%
        dplyr::arrange(dplyr::across())
                
    expect_equal(actual, expected)
}