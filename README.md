column_name = "your_column_name"
column_data = df.select(column_name).rdd.flatMap(lambda x: x).collect()
column_data_str = ','.join([str(elem) for elem in column_data])
