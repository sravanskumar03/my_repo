from pyspark.sql.functions import col

df_filtered = df.filter(col("import_date") > "2022-12-22")
