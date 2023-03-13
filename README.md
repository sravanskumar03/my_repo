from pyspark.sql.functions import date_add, to_date
from pyspark.sql.types import IntegerType

# Define start and end dates
start_date = "2020-01-01"
end_date = "2020-12-31"

# Calculate number of days between start and end dates
num_days = (to_date(end_date) - to_date(start_date) + 1).cast(IntegerType())

# Create DataFrame with date column
df = spark.range(num_days).withColumn("date", date_add(to_date(start_date), expr("id")))

# Show the first 10 rows of the DataFrame
df.show(10)
