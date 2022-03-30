# Source the functions; note that testthat will change the working directory
#   to `/tests` and therefore paths are relative to this
source("../functions/basic_functions.R")

library(sparklyr)
library(dplyr)
library(testthat)

test_that("test count_animal",{
    # The simplest example is an expect_equal statement for checking
    #     scalar values, e.g. a row count or a sum
    #    
    # The function being tested counts the number of animals after first
    #     capitalising the first letter, so the input DF tests some common
    #     scenarios
    #    
    # Structure here follows the Arrange, Act, Assert pattern:
    #     - Arrange: set up your inputs and expected outputs
    #     - Act: call the function and return the result
    #     - Assert: Check that the actual result is as expected
    
    # Arrange
    # Get or create Spark session
    sc <- testthat_spark_connection()
    
    # Set test_df to be a base R DataFrame; can also use a tibble
    # This will later be converted to a sparklyr DataFrame
    test_df <- data.frame(
        id = 1:4,
        animal_group = c("cat", "Cat", "CAT", "dog"),
        stringsAsFactors = FALSE)
    
    # Copy test_df to the Spark cluster as a sparklyr DataFrame
    sdf <- sparklyr::sdf_copy_to(sc, test_df, overwrite = TRUE)
    
    # Act
    # We use the sdf as an input; we can't use a base R DF or a tibble
    #     as the function contains Spark functionality, initcap
    # pull() will return the scalar value from the cluster
    result <- count_animal(sdf, "Cat") %>% dplyr::pull()
    
    # Assert
    # This is now just comparing scalar values in the CDSW session memory
    expect_equal(result, 3)
})

test_that("test format columns",{
    # A simple assert statement can also be used for checking the names
    #     of the columns, using the colnames() function
        
    # The DataFrame created here is just one row, as it is only the column
    #     names which matter
        
    # Note that we have defined the expected_columns as a vector, which has
    #     an order and index. If the column order does not matter see the
    #     test below, test format columns unordered.
    
    # Arrange
    # Get or create Spark session
    sc <- testthat_spark_connection()
    
    # Set expected_columns to be a vector of column names
    expected_columns = c("incident_number", "animal_group",
                         "engine_count", "description")
    
    # Set test_df to be a base R DataFrame
    test_df <- data.frame(
        IncidentNumber = 1,
        AnimalGroupParent = "Cat",
        PumpCount = 1,
        FinalDescription = "CAT STUCK IN TREE",
        stringsAsFactors = FALSE)
    
    # Copy test_df to Spark cluster
    sdf <- sparklyr::sdf_copy_to(sc, test_df, overwrite = TRUE)
    
    # Act
    # colnames() will be returned as a character vector in the driver
    result <- sdf %>% format_columns() %>% colnames()
    
    # Assert
    # This is comparing two character vectors in the driver
    expect_equal(result, expected_columns)
})


test_that("test format columns unordered",{
    # This works in the same was as test format_columns, but the column
    #     order does not matter. This is achieved by sorting both DataFrames
    #     before comparison for equality.
    
    # Arrange
    # Get or create Spark session
    sc <- testthat_spark_connection()
    
    # Set expected_columns to be a vector of column names in any order
    expected_columns = c("animal_group", "engine_count",
                         "description", "incident_number")
    
    # Set test_df to be a base R DataFrame
    test_df <- data.frame(
        IncidentNumber = 1,
        AnimalGroupParent = "Cat",
        PumpCount = 1,
        FinalDescription = "CAT STUCK IN TREE",
        stringsAsFactors = FALSE)
    
    # Copy test_df to Spark cluster
    sdf <- sparklyr::sdf_copy_to(sc, test_df, overwrite = TRUE)
    
    # Act
    result <- sdf %>% format_columns() %>% colnames()
    
    # Assert
    # Use sort() so the input order of the columns does not matter
    # As above, this is in the driver memory
    expect_equal(sort(result), sort(expected_columns))
})