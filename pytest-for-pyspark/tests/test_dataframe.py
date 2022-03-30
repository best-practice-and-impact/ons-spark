import pytest
from pandas.testing import assert_frame_equal

from functions import dataframe_functions
from tests.helpers import assert_pyspark_df_equal

@pytest.fixture(scope="module")
def input_df(spark):
    """
    The same input DF is being used in several tests, so we have
        defined this as a fixture
        
    The scope is set to module, as unlike the spark fixture in
        confest.py, it is only used within this module
    """
    return (spark.createDataFrame([
        [1, "Cat"],
        [2, "CAT"],
        [3, "Hamster"],
        [4, "dog"],
        [5, "Cat"],
        [6, "cat"],
        [7, "DoG"],
        ], ["id", "animal_group"]))

@pytest.fixture(scope="module")
def output_df(spark):
    """
    This is the expected result of the function to be tested.
    
    Defined as a fixture, in the same way as the input_df
    """
    return (spark.createDataFrame([
        ["Cat", 4],
        ["Hamster", 1],
        ["Dog", 2],
        ], ["animal_group", "count"]))

def test_group_animal_collect(spark, input_df, output_df):
    """
    All the tests in this module will test the output of group_animal.
        This performs a simple groupBy on animal, which is not case
        sensitive, and then counts.
    
    One method of comparing DataFrames is to collect them and then
        compare with assert
        
    It is important to order the DFs first before comparing, as otherwise
        the order of PySpark DataFrames are generally non-deterministic due
        to the way that they are distributed on the cluster.
    """
    # Arrange
    df = input_df
    expected_df = output_df
    
    # Act
    actual_df = dataframe_functions.group_animal(df)
    
    # Assert
    # DF are both sorted in column order
    # Note that if the column order is not the same in both DFs, this will
    #   fail. 
    assert (actual_df.orderBy(*actual_df.columns).collect() ==
            expected_df.orderBy(*expected_df.columns).collect())
    
def test_group_animal_toPandas(spark, input_df, output_df):
    """
    Converts PySpark DFs to pandas DFs, then uses assert_frame_equal from
        pandas.testing. The advantage of this method over using .collect()
        is that it will print out more information about the assertion
        error.
    
    As before, the PySpark DFs are sorted first, by every column.
    """
    # Arrange
    df = input_df
    expected_df = output_df
    
    # Act
    actual_df = dataframe_functions.group_animal(df)
    
    # Assert
    assert_frame_equal(actual_df.orderBy(*actual_df.columns).toPandas(),
                       expected_df.orderBy(*expected_df.columns).toPandas())


def test_group_animal_pyspark(spark, input_df, output_df):
    """
    Runs the same test as before, but using the assert_pyspark_df_equal
        function defined in helpers.py. The advantage of this method is that
        you can customise the function to include more information about
        test failures or make it less strict.
    """
    # Arrange
    df = input_df
    expected_df = output_df
    
    # Act
    actual_df = dataframe_functions.group_animal(df)
    
    # Assert
    # Uses the function defined above
    assert_pyspark_df_equal(actual_df, expected_df)