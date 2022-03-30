# Source the functions; note that testthat will change the working directory
#   to `/tests` and therefore paths are relative to this
source("../functions/dataframe_functions.R")

library(sparklyr)
library(dplyr)
library(testthat)

# These tests show how to compare sparklyr DataFrames
# Note that in Spark, DataFrames are not ordered in the same way
#   as base R DataFrames or tibbles, so a sorting step is needed
#   before comparison

test_that("test group_animal",{
    # Tests DataFrame equality, sorting the DFs within
    #   expect_equal.
    
    # Arrange
    # Get or create Spark session
    sc <- testthat_spark_connection()
    
    # Set input_df and output_df to be base R DataFrame
    # This will later be converted to sparklyr DataFrame    
    input_df <- data.frame(id = 1:7,
                    animal_group = c("Cat", "CAT", "Hamster",
                                "dog", "Cat", "cat", "DoG"),
                    stringsAsFactors = FALSE)

    output_df <- data.frame(animal_group = c("Cat", "Hamster", "Dog"),
                            count = c(4, 1, 2),
                            stringsAsFactors = FALSE)
    
    # Copy these to the Spark cluster as sparklyr DataFrames
    sdf <- sparklyr::sdf_copy_to(sc, input_df, overwrite = TRUE)
    expected <- sparklyr::sdf_copy_to(sc, output_df, overwrite = TRUE)
    
    # Act
    result <- group_animal(sdf)
    
    # Assert
    # Use collect() to bring the DF into the driver from the cluster
    # sparklyr DataFrames are not ordered, so are non-deterministic
    # Use arrange(across()) to ensure that they are sorted by every
    #    column before doing the comparison with expect_equal
    expect_equal(result %>%
                    sparklyr::collect() %>%
                    dplyr::arrange(dplyr::across()),
                 expected %>%
                    sparklyr::collect() %>%
                    dplyr::arrange(dplyr::across()))
})

           
test_that("test group_animal using function",{
    # This test is almost identical to test group_animal above, but
    #   uses the function defined in setup_spark. When unit testing
    #   sparklyr code you will frequently check DF equality and so
    #   it is a good idea to use a function for this.
    
    # Arrange
    # Get or create Spark session
    sc <- testthat_spark_connection()
    
    # Set input_df and output_df to be base R DataFrame
    # This will later be converted to sparklyr DataFrame    
    input_df <- data.frame(id = 1:7,
                    animal_group = c("Cat", "CAT", "Hamster",
                                "dog", "Cat", "cat", "DoG"),
                    stringsAsFactors = FALSE)

    output_df <- data.frame(animal_group = c("Cat", "Hamster", "Dog"),
                            count = c(4, 1, 2),
                            stringsAsFactors = FALSE)
    
    # Copy these to the Spark cluster as sparklyr DataFrames
    sdf <- sparklyr::sdf_copy_to(sc, input_df, overwrite = TRUE)
    expected <- sparklyr::sdf_copy_to(sc, output_df, overwrite = TRUE)
    
    # Act
    result <- group_animal(sdf)
    
    # Assert
    # Uses the function defined in setup_spark
    expect_sdf_equal(result, expected)
    
})