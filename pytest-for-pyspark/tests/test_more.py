import pytest
from pyspark.sql.utils import AnalysisException

from functions import more_functions
from tests.helpers import assert_pyspark_df_equal

def test_analysis_exception(spark):
    """
    Tests that an error is raised
    
    This supplies a DataFrame with an incorrect column, which causes
        an AnalysisException. If this is wrapped with pytest.raises,
        then the test passes if the specific error is raised,
        otherwise it fails. You can think of pytest.raises as
        working in the same way as a not statement.
        
    Note that AnalysisException needs to be imported from
        pyspark.sql.utils
        
    You can test other errors in a similar way
    """
    
    # Arrange
    df = spark.createDataFrame([
        [1, "Cat"],
        [2, "Cat"],
        [3, "Hamster"],
        [4, "Dog"],
        [5, "Cat"],
        [6, "Cat"],
        [7, "Dog"],
        ],
        # WrongColumn is not expected, so will raise an error
        ["incident_number", "wrong_column"])
    
    # Act and Assert
    with pytest.raises(AnalysisException):
        more_functions.group_animal(df)
        
def test_read_and_format_rescue(spark, mocker):
    """
    Test that gives an example of mocker and shows how to test
        data types.
    
    Functions that read or write data can be tested by mocking the
        methods which read or write data.
        
    In this example, we can specify a DF to be used instead of 
        reading a CSV from a path on HDFS.
        
    mocker needs to be specified as an input to the test function
    
    Data types can be compared with the assert statement, using the
        .dtypes property of the PySpark DataFrame
    """
    
    # Arrange
    # To create an empty DF, first create a schema    
    input_schema = """
        IncidentNumber string,
        PumpCount double,
        FinalDescription string,
        AnimalGroupParent string
        """
    
    # Use an empty RDD with the schema to create an empty DF
    mock_df = spark.createDataFrame(
        spark.sparkContext.emptyRDD(),
        input_schema)

    # Change the result of read_rescue() to be mock_df
    mocker.patch('functions.more_functions.read_rescue',
                 return_value=mock_df)
    
    # Define the expected result in the same way as before
    expected_schema = """             
        incident_number string,
        animal_group string,
        engine_count double,
        description string
        """

    expected_df = spark.createDataFrame(
        spark.sparkContext.emptyRDD(),
        expected_schema)
    
    # Act
    actual_df = more_functions.read_and_format_rescue(spark)
    
    # Assert
    # Check that the columns and types are equal
    # This will also test the order - so will sometimes be too strict
    assert actual_df.dtypes == expected_df.dtypes

@pytest.mark.parametrize("animal, expected_count", [
    ("Cat", 3), ("Dog", 1), ("Hamster", 0)])
def test_count_animal_parametrise(spark, animal, expected_count):
    """
    You can easily test different parameters for the same test by adding
        parametrisation. Here we are generalising the test_count_animal
        from test_basic by adding an argument for animal and
        expected_count.
        
    Be careful with the syntax here: the first argument is one string:
        "animal, expected_count" not "animal", "expected_count"
    It is then passed through to the test was an argument:
        test_count_animal_parametrize(spark, animal, expected_count)
    animal and expected_count can then be used as variables in the test
    
    Also ensure you use the American spelling (with a z) for
        @pytest.mark.parametrize
        
    This is only a simple example; for more detail on different ways to
        parametrise tests, see Mitch Edmunds testing tips repository:
        https://github.com/mitches-got-glitches/testing-tips
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
    
    # Act
    actual_count = more_functions.count_animal(df, animal)
    
    #Assert
    assert actual_count == expected_count

class TestAddSquareColumn:
    """
    These tests were written using Test Driven Development (TDD).
    
    With TDD, we write the tests first, then write the function
        afterwards. This means you have to consider the requirements
        more carefully and can lead to code with fewer bugs. Of course,
        always test the code with the real data before deploying to
        production.
        
    All these tests follow the same format: define expected and
        input DFs, run the add_square_column() function and check the
        result with assert_pyspark_df_equal(). Classes are optional in
        Pytest but are useful for grouping similar tests together.
    
    These tests could be parametrized, in a similar way to the
        example above.
    """
    cols = ["input_no", "squared"]
    
    def test_add_square_column_small(self, spark):
        """Test small values"""
        expected_df = spark.createDataFrame([
        [4.0, 16.0],
        [7.0, 49.0],
        [10.0, 100.0],
        ], self.cols)

        input_df = expected_df.drop("squared")

        actual_df = more_functions.add_square_column(
            input_df, "input_no", "squared")

        assert_pyspark_df_equal(actual_df, expected_df)
        
    def test_add_square_column_null_identity(self, spark):
        """Test null, 0 and 1"""
        expected_df = spark.createDataFrame([
        [None, None],
        [0.0, 0.0],
        [1.0, 1.0],
        ], self.cols)

        input_df = expected_df.drop("squared")

        actual_df = more_functions.add_square_column(
            input_df, "input_no", "squared")

        assert_pyspark_df_equal(actual_df, expected_df)
        
    def test_add_square_column_large(self, spark):
        """Test some larger values"""
        expected_df = spark.createDataFrame([
        [10000.0, 100000000.0],
        [20001.0, 400040001.0],
        ], self.cols)

        input_df = expected_df.drop("squared")

        actual_df = more_functions.add_square_column(
            input_df, "input_no", "squared")

        assert_pyspark_df_equal(actual_df, expected_df)
        
    def test_add_square_column_decimal(self, spark):
        """Test non-integer values"""
        expected_df = spark.createDataFrame([
        [0.5, 0.25],
        [9.9, 98.01],
        ], self.cols)

        input_df = expected_df.drop("squared")

        actual_df = more_functions.add_square_column(
            input_df, "input_no", "squared")

        assert_pyspark_df_equal(actual_df, expected_df)
        
    def test_add_square_column_negative(self, spark):
        """Test negative values"""
        expected_df = spark.createDataFrame([
        [-1.0, 1.0],
        [-7.0, 49.0],
        [-20001.0, 400040001.0]
        ], self.cols)

        input_df = expected_df.drop("squared")

        actual_df = more_functions.add_square_column(
            input_df, "input_no", "squared")

        assert_pyspark_df_equal(actual_df, expected_df)