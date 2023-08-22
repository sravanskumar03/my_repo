from pyspark.sql.functions import array_remove

def remove_elements_from_array_column(df, column_name, elements_to_remove):
    return df.withColumn(column_name, array_remove(column_name, *elements_to_remove))
