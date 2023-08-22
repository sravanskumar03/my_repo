from pyspark.sql.functions import when, length

def remove_initials(df, column_name):
    return df.withColumn(column_name, when(length(df[column_name].split()[-1]) == 2, df[column_name].substr(1, length(df[column_name]) - 2)).otherwise(df[column_name]))
