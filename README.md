from pyspark.sql.functions import split, size

def transform_string_col(df, string_col_name):
    # transform string column into array column
    df = df.withColumn("array_col", split(df[string_col_name], ","))
    
    # add new column with length of array
    df = df.withColumn("array_length", size(df["array_col"]))
    
    return df
