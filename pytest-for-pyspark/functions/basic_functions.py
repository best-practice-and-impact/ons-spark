from pyspark.sql import functions as F

def count_animal(df, animal):
    """
    Counts the number of "animal" in the animal_group column
    
    Uses F.initcap() on animal_group, which capitalises the first
        letter of each word, to remove data input errors. Note
        that this does not happen to the input animal, so "cat"
        and "CAT" will always return a count of 0.
        
    Args:
        df (spark DataFrame): input DF
        animal (string): name of the animal to count
        
    Returns:
        int: count of animal in animal_group
    """
    
    return (df
            .withColumn("animal_group", F.initcap("animal_group"))
            .filter(F.col("animal_group") == animal)
            .count())

def format_columns(df):
    """
    Changes the names of some columns to be briefer or more descriptive.
    
    The data in the AnimalGroupParent has the first letter of each
        word capitalised and is renamed animal_group.
    
    Args:
        df (spark DataFrame): input DF
    
    Returns:
        spark DataFrame: input DF with columns renamed and reordered
    """
    
    df = (df
          .withColumnRenamed("IncidentNumber", "incident_number")
          .withColumn("animal_group", F.initcap("AnimalGroupParent"))
          .withColumnRenamed("PumpCount", "engine_count")
          .withColumnRenamed("FinalDescription", "description"))
    
    return df.select("incident_number", "animal_group", "engine_count", "description")
