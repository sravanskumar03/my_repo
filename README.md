import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def remove_one_letter_initials(df: DataFrame, col_name: str) -> DataFrame:
    return df.withColumn(col_name, F.regexp_replace(F.col(col_name), r'\b\w\b', ''))
