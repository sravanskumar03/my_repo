from pyspark.sql.functions import split, sort_array, concat_ws

def update_column(df, column_to_update):
    df2 = df.select(
        "*",
        concat_ws(",", sort_array(split(column_to_update, ";"))).alias("updated_column")
    )
    return df2
