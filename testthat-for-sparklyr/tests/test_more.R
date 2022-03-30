# Source the functions; note that testthat will change the working directory
#   to `/tests` and therefore paths are relative to this
source("../functions/more_functions.R")

library(sparklyr)
library(dplyr)
library(testthat)

test_that("test analysis exception",{
    # Tests that an error is raised
    
    # This supplies a DataFrame with an incorrect column, which causes
    #     an AnalysisException. expect_error() can be used to test for
    #     this; the test passes if the specific error is raised,
    #     otherwise it fails.
    
    # You can test other errors in a similar way

    # Arrange
    # Get or create Spark session
    sc <- testthat_spark_connection()
    
    # Set test_df to be a base R DataFrame; can also use a tibble
    # This will later be converted to a sparklyr DataFrame
    test_df <- data.frame(
                id = 1:4,
                wrong_column = c("cat", "Cat", "CAT", "dog"),
                total_cost = c(1, 2, 4, 8),
                stringsAsFactors = FALSE)
    
    # Copy test_df to the Spark cluster as a sparklyr DataFrame
    sdf <- sparklyr::sdf_copy_to(sc, test_df, overwrite = TRUE)
    
    # Assert
    # Spark errors are long, so just the start of the error message
    #    is supplied with the wildcard * for the remainder
    expect_error(animal_cost(sdf, "Cat") %>% sparklyr::collect(),
                 "org.apache.spark.sql.AnalysisException: cannot resolve '`animal_group`*")    
})

test_that("test sum_animal with multiple expectations",{
    # You can have multiple expect_equal statements in the same test
    # This example calls sum_animal three times, with the same
    #     input (test_df), but different animals to aggregate
    # Includes an example of comparing NA values
    
    # Arrange
    # Get or create Spark session
    sc <- testthat_spark_connection()
    
    # Set test_df to be a base R DataFrame; can also use a tibble
    # This will later be converted to a sparklyr DataFrame
    # The same test_df is used as the input for every test
    test_df <- data.frame(
        id = 1:4,
        animal_group = c("cat", "Cat", "CAT", "dog"),
        total_cost = c(1, 2, 4, 8),
        stringsAsFactors = FALSE)
    
    # Copy test_df to the Spark cluster as a sparklyr DataFrame
    sdf <- sparklyr::sdf_copy_to(sc, test_df, overwrite = TRUE)
    
    # Act
    # The function being tested is ran three times with different inputs
    # We use the sdf as an input; we cannot use a base R DF or a tibble
    #     as the function contains Spark functionality, initcap()
    # pull() will return the scalar value from the cluster
    result_cat <- animal_cost(sdf, "Cat") %>% dplyr::pull()
    result_dog <- animal_cost(sdf, "Dog") %>% dplyr::pull()
    result_hamster <- animal_cost(sdf, "Hamster") %>% dplyr::pull()
    
    # Assert
    # Run expect_equal three times for different expected results
    # This is now just comparing scalar values in the CDSW session memory
    expect_equal(result_cat, 7)
    expect_equal(result_dog, 8)
    # Use is.na() to test for NA equality
    expect_equal(is.na(result_hamster), TRUE)
})

#'Test Sum Animal
#
#'Wrapper for testing sum_animal
#
#'Calls sum_animal and tests that value returned is equal to expected_sum
#'        
#'@param animal name of the animal to sum TotalCost for
#'@param expected_sum 
test_sum_animal <- function(animal, expected_sum) {
    
    # Arrange
    # Get or create Spark session
    sc <- testthat_spark_connection()
    
    # Set test_df to be a base R DataFrame; can also use a tibble
    # This will later be converted to a sparklyr DataFrame
    test_df <- data.frame(
        id = 1:4,
        animal_group = c("cat", "Cat", "CAT", "dog"),
        total_cost = c(1, 2, 4, 8),
        stringsAsFactors = FALSE)
    
    # Copy test_df to the Spark cluster as a sparklyr DataFrame
    sdf <- sparklyr::sdf_copy_to(sc, test_df, overwrite = TRUE)
    
    # Act
    # We use the sdf as an input; we cannot use a base R DF or a tibble
    #     as the function contains Spark functionality, initcap()
    # pull() will return the scalar value from the cluster
    # This uses the animal input
    result <- animal_cost(sdf, animal) %>% dplyr::pull()
    
    # Assert
    # This is now just comparing scalar values in the CDSW session memory
    # Uses the expected_count input
    expect_equal(result, expected_sum)

}

test_that("test sum_animal parameterised",{
    # This uses the test_sum_animal function to parameterise the tests,
    #   so it is easy to test for different animals and sums
    # mapply is used here, which applies a function over more than one
    #   input vector, in this case animals and expected_sums
    # In this example you could use walk2() from the purr package
    
    # Set up two vectors for animal and expected sum
    # NA values can be compared here as it is in a numeric vector
    animals <- c("Cat", "Dog", "Hamster")
    expected_sums <- c(7, 8, NA)

    # Use mapply on the test function to test each scenario
    # Can alternatively use purr::walk2(), depending on your preference
    mapply(test_sum_animal, animals, expected_sums)
})