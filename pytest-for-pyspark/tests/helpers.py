"""
This module contains shared functions for testing

As it does not begin with "test", it will not be detected by the
    auto-discovery of Pytest.
"""

def assert_pyspark_df_equal(actual_df, expected_df):
    """
    Tests if two DataFrames are equal.
    
    First tests that the columns are equal, including the column order. If
        not an AssertionError is raised.
    
    Then uses .exceptAll() twice to find rows that are in the expected_df 
        but not in the actual_df, and vice versa. If they are identical
        then the count of both of these should be zero; if not, an 
        assertionError is raised.
        
    This function can be expanded to print out more information about any
        test failure, e.g. diff_ab.union(diff_ba).show() if the count is
        greater than zero. You can also modify it to account for columns
        having a different order by testing set equality and then setting
        expected_df = expected_df.select(actual_df.columns). For this
        example however it is left as simple as possible.
        
    Pytest knows this function is not to be ran as a test as it doesn't
        start or end with the word "test" and is contained in a non-test
        module.

    Args:
        actual_df (spark DataFrame): DF to be tested
        expected_df (spark DataFrame): expected result
    """
        
    if actual_df.columns != expected_df.columns:
        raise AssertionError(
            f"DataFrames have different columns"
        )

    diff_ab = actual_df.exceptAll(expected_df)
    diff_ba = expected_df.exceptAll(actual_df)
    
    if diff_ab.union(diff_ba).count() > 0:
        raise AssertionError(
            f"Dataframes are not equal."
        )