from pyspark.sql.functions import array, explode, expr

# Define start and end dates
start_date = "2020-01-01"
end_date = "2020-12-31"

# Create DataFrame with single row containing array of dates
df = spark.createDataFrame([(array([start_date] + [expr(f"date_add('{start_date}', {i})") for i in range(1, (to_date(end_date) - to_date(start_date)).days + 1)]))], ["date_range"])

# Explode array of dates into individual rows
df = df.select(explode("date_range").alias("date"))

# Show the first 10 rows of the DataFrame
df.show(10)
