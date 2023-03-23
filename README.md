from pyspark.sql.functions import count

# Assume df is your DataFrame
duplicate_count = df.groupBy(df.columns).count().filter("count > 1").count()

print("Number of duplicate records:", duplicate_count)
