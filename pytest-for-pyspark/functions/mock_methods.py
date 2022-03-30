"""
Thanks to Peter Derrick for contributing this section
"""

# Import third party libraries.
import pandas as pd

# Import PySpark libraries.
from pyspark.sql import SparkSession


def check_if_first_of_month():
    """Returns true if today's date is the first of the month.

    Parameters
    ----------
    None

    Returns
    -------
    boolean
        True if today's date is the first of the month
        False otherwise
    """
    
    date_today = pd.Timestamp.today().date()
  
    day = str(date_today)[8:10]
  
    if day == "01":
    
        return True
  
    return False


def read_csv_from_cdsw(filename):
    """Reads a csv file from CDSW.

    Parameters
    ----------
    filename
      filepath and name of csv file.

    Returns
    -------
    Pandas DataFrame
    """
    whole_file = pd.read_csv(filename,
                             index_col=0,
                             encoding="latin-1")
    
    return whole_file

def read_csv_from_hdfs(spark, filename):
    """Reads a csv file from HDFS.

    Parameters
    ----------
    filename
      filepath and name of csv file.

    Returns
    -------
    PySpark DataFrame
    """
    sdf = spark.read.csv(filename,
                         header=True,
                         inferSchema=True)
    
    return sdf

def open_json(spark, json_path):
    """Opens a json file as a pandas DataFrame.

    Parameters
    ----------
    spark
      Spark session.
    json_path
      a HDFS file path and file name ending in .json

    Returns
    -------
    Pandas DataFrame
    """
    sdf = spark.read.json(
        json_path,
        encoding='ISO-8859-1',
        multiLine = True,
    )
    
    df = sdf.toPandas()

    return df

  
def json_to_dictionary(spark, json_path):
    """Converts a json file to a dictionary.

    Parameters
    ----------
    spark
      Spark session.
    json_path
      a HDFS file path and file name ending in .json

    Returns
    -------
    Spark DataFrame
    """
    
    keyword_dict = (open_json(spark, json_path)
                    .to_dict(orient='records'))
    
    return keyword_dict[0]
