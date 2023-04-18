column_data = [df.select(c).rdd.flatMap(lambda x: x).collect() for c in df.columns]
