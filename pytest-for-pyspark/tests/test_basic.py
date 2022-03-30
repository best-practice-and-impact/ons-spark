import pytest

from functions import basic_functions

def test_count_animal(spark):
    """
    The simplest example is an assert statement
    
    This can be used for checking scalar values, e.g. a row count
        or a sum
        
    The function being tested counts the number of animals after first
        capitalising the first letter, so the input DF tests some common
        scenarios
        
    Structure here follows the Arrange, Act, Assert pattern:
        - Arrange: set up your inputs and expected outputs
        - Act: call the function and return the result
        - Assert: Check that the actual result is as expected
    """
    # Arrange

    df = spark.createDataFrame([
        # Test lowercase
        [1, "cat"],
        # Test first letter capitalised
        [2, "Cat"],
        # Test uppercase
        [3, "CAT"],
        # Check that non cats are not included in the count
        [4, "dog"],
    ], ["id", "animal_group"])
    
    expected_count = 3
    
    # Act
    actual_count = basic_functions.count_animal(df, "Cat")
    
    # Assert
    assert actual_count == expected_count
    
def test_format_columns(spark):
    """
    A simple assert statements can also be used for checking the names
        of the columns, using the .columns property of the DataFrame
        
    The DataFrame created here is just one row, as it is only the column
        names which matter
        
    Note that we have defined the expected_columns as a list, which has
        an order. If the column order doesn't matter see the test below,
        test_format_columns_unordered.
    """
    # Arrange
    df = spark.createDataFrame([
        [1, "Cat", 1, "CAT STUCK IN TREE"],
        ], ["IncidentNumber", "AnimalGroupParent", "PumpCount", "FinalDescription"])
    
    expected_columns = ["incident_number", "animal_group", "engine_count", "description"]
    
    # Act
    actual_df = basic_functions.format_columns(df)
    
    # Assert
    assert actual_df.columns == expected_columns


def test_format_columns_unordered(spark):
    """
    This works in the same was as test_format_columns, but the column
        order does not matter. This is achieved by defining the expected
        column names as a set, which is unordered.
        
    Note that the input columns are in a differentorder to
        test_format_columns() above. 
    """
    
    # Arrange
    df = spark.createDataFrame([
        [1, "Cat", 1, "CAT STUCK IN TREE"],
        ], ["AnimalGroupParent", "PumpCount", "IncidentNumber", "FinalDescription"])
    
    # Define as a Python set, which is unordered
    expected_columns = {"incident_number", "animal_group", "engine_count", "description"}
    
    # Act
    actual_df = basic_functions.format_columns(df)

    # Assert
    # Both results are now sets, which means the order does not matter
    assert set(actual_df.columns) == expected_columns