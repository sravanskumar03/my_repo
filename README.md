from pyspark.sql.functions import regexp_replace

def replace_substrings(df, column, replacements):
    """
    Replace multiple substrings in a PySpark DataFrame column.
    
    :param df: The input DataFrame
    :param column: The name of the column to replace substrings in
    :param replacements: A dictionary of substring replacements, where the keys are the substrings to be replaced and the values are the replacement strings
    :return: A new DataFrame with the specified substrings replaced
    """
    new_df = df
    for old, new in replacements.items():
        new_df = new_df.withColumn(column, regexp_replace(column, old, new))
    return new_df
