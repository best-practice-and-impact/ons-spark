"""
Thanks to Peter Derrick (Peter.Derrick@ons.gov.uk) for contributing this section

The examples here are grouped together with classes. In Pytest, classes
    are optional but can be useful for organisation, e.g. TestJsonToDictionary
    has functions for the input and output data
"""

# Import testing packages
import pytest
import mock

from datetime import datetime

# Import module to test
from functions import mock_methods


class TestCheckIsFirstOfMonth(object):
    """Tests for check_if_first_of_month function."""
    
    @mock.patch("pandas.Timestamp")
    def test_check_if_first_of_month(self, mock_Timestamp):
        """Test expected functionality"""
        
        # Arrange
        # Set the today value to be 1st Jan 2021, rather than the current date
        mock_Timestamp.today.return_value = datetime.strptime("2021-01-01", "%Y-%m-%d")
        
        # Act
        result = mock_methods.check_if_first_of_month()
      
        # Assert
        assert result
        
    @mock.patch("pandas.Timestamp")
    def test_check_if_not_first_of_month(self, mock_Timestamp):
        """Test expected functionality"""

        # Arrange
        # Set the today value to be 2nd Jan 2021, rather than the current date
        mock_Timestamp.today.return_value = datetime.strptime("2021-01-02", "%Y-%m-%d")
        
        # Act
        result = mock_methods.check_if_first_of_month()
        
        # Assert
        # assert not means this will pass if result == False
        assert not result
        
        
class TestReadCsvFromCdsw(object):
    """Tests for read_csv_from_cdsw function."""
    
    # Use a patch to mock the result of pandas.read_csv
    # This means that a CSV will not be read in from CDSW
    # This test is used to ensure that read_csv is called, with the options specified
    @mock.patch("pandas.read_csv")
    def test_read_csv_from_cdsw(self, mock_read):
        """Test the expected functionality."""
        
        # Arrange and Act
        result = mock_methods.read_csv_from_cdsw("cdsw_path/file.csv")
        
        # Assert
        mock_read.assert_called_with("cdsw_path/file.csv",
                                     index_col=0,
                                     encoding="latin-1")

class TestReadCsvFromHdfs(object):
    """Tests for read_csv_from_hdfs function."""
    
    # Use a patch to mock the result of spark.read
    # This uses the same concept as test_read_csv_from_cdsw, just with Spark instead
    # Note the order of the parameters here: self, mock_read, spark
    # The fixtures are listed at the end, and any mocks before this
    # Note that multiple mock decorators work in reverse order - the one at the top
    #   is the last listed in the function and vice versa
    @mock.patch("pyspark.sql.SparkSession.read")
    def test_read_csv_from_hdfs(self, mock_read, spark):
        """Test the expected functionality."""
        
        # Arrange and Act
        sdf = mock_methods.read_csv_from_hdfs(spark, "file/path/filename.csv")
        
        # Assert
        mock_read.csv.assert_called_with("file/path/filename.csv",
                                         header=True,
                                         inferSchema=True)
        
class TestOpenJson(object):
    """Test for open_json function."""
    
    # Use a patch to mock the result of spark.read, similar to the above
    @mock.patch("pyspark.sql.SparkSession.read")
    def test_open_json(self, mock_read, spark):        
        """Test the expected functionality"""
        
        # Arrange and Act
        sdf = mock_methods.open_json(spark, "file/path/filename.json")
        
        # Assert
        mock_read.json.assert_called_with("file/path/filename.json",
                                          encoding='ISO-8859-1',
                                          multiLine = True)
        
        
class TestJsonToDictionary(object):
    """Test tests for json_to_dictionary function."""
    
    def keywords(self):
        """Input data for unit test."""
        
        keywords = [{"keywords":["A"],
                     "attributes_A":["1"],
                     "attributes_B":["X"],
                     "attributes_C":["A"],
                     "labels":"X"}]
        
        return keywords
    
    def expout_data(self):
        """Expected output data for unit test."""
        
        dictionary = {"keywords":["A"],
                     "attributes_A":["1"],
                     "attributes_B":["X"],
                     "attributes_C":["A"],
                     "labels":"X"}
        
        return dictionary
    
    # Use a patch to mock the result of open_json
    # This is a function that we defined, which reads a JSON file from
    #   HDFS as a pandas DF. Here the result of this function is replaced
    #   with the result of keywords(), defined in this class
    @mock.patch("functions.mock_methods.open_json")
    def test_json_to_dictionary(self, mock_open_json, spark):
        """Test the expected functionality."""
        
        # Arrange
        # Set the expected result
        expout = self.expout_data()
        
        # Set the source data to be user defined, not actually read in from HDFS
        mock_open_json.return_value.to_dict.return_value = self.keywords()
        
        # Act
        result = mock_methods.json_to_dictionary(spark, "json_path/file.json")
        
        # Assert
        assert result == expout
