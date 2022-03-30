from pyspark.sql import functions as F

from functions.basic_functions import format_columns, count_animal
from functions.dataframe_functions import group_animal

def read_rescue(spark):
    """
    Reads animal rescue CSV data from HDFS
    
    Args:
        spark (SparkSession): active Spark session
    
    Returns:
        spark DataFrame: animal rescue data
    
    """
    return spark.read.csv("/training/animal_rescue.csv",
                          header=True, inferSchema=True)

def read_and_format_rescue(spark):
    """
    Reads in the animal rescue data, then formats the columns
    
    Args:
        spark (SparkSession): active Spark session
    
    Returns:
        spark DataFrame: formatted animal rescue data     
    """
    
    return format_columns(read_rescue(spark))

def add_square_column(df, source_column_name, new_column_name):
    """
    Adds a column of square numbers
    
    Args:
        df (spark DataFrame): a Spark DF
        source_column_name (string): values to be squared
        new_column_name(string): column to write squared values to
    
    Returns:
        spark DataFrame:  DF with a column for squared values
    """
    return df.withColumn(new_column_name, F.pow(F.col(source_column_name), 2))