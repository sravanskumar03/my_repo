from pyspark.sql.functions import when, length

def remove_initials(df, column_name):
    return df.withColumn(column_name, when(length(column_name.split()[-1]) == 2, column_name[:-2]).otherwise(column_name))
