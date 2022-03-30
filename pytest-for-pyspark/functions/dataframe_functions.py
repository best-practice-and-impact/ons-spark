from pyspark.sql import functions as F

def group_animal(df):
    """
    Performs a simple groupBy and count on the DF

    Uses F.initcap() on animal_group, which capitalises the first
        letter of each word, to remove data input errors. Note
        that this does not happen to the input animal, so "cat"
        and "CAT" will always return a count of 0.
        
    Args:
        df (spark DataFrame): input DF, with a column named "animal_group"
    
    Returns:
        spark DataFrame: DF with animal_group and count
    
    """
    return (df
            .withColumn("animal_group", F.initcap("animal_group"))
            .groupBy("animal_group")
            .count())