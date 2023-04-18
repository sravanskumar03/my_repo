column_name = "your_column_name"
column_data = df.select(column_name).rdd.flatMap(lambda x: x).collect()
