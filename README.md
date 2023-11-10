from pyspark.sql.functions import col, concat, lit, to_date, date_format, when

# Create a list of all Monday dates and month end dates from Jan 2021 to current day
date_list = spark.range("2021-01-01", "2023-11-10", "1 day").select(to_date(col("id")).alias("date"))
monday_dates = date_list.filter(date_format(col("date"), "E") == lit("Mon"))
month_end_dates = date_list.filter(date_format(col("date"), "d") == date_format(date_add(date_add(last_day(col("date")), 1), -1), "d"))

# Combine the two lists and create a DataFrame
df = monday_dates.union(month_end_dates).distinct().select(col("date").alias("import_dt"))

# Add the Freq column
df = df.withColumn("Freq", when(date_format(col("import_dt"), "E") == lit("Mon"), lit("W")).otherwise(lit("M")))

# Show the DataFrame
df.show()
