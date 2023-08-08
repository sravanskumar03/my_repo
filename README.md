from pyspark.sql.functions import col, regexp_replace

def remove_non_ascii(df):
    for column in df.columns:
        df = df.withColumn(column, regexp_replace(col(column), '[^ -~]', ''))
    return df
