from pyspark.sql.functions import regexp_replace

def remove_initials(df, column_name):
    return df.withColumn(column_name, regexp_replace(column_name, ",[A-Za-z]{1}$", ""))
