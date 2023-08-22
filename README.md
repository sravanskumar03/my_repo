from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def remove_initials(df, column_name):
    def remove_initial(name):
        if len(name.split()[-1]) == 2:
            return name[:-2]
        else:
            return name

    remove_initial_udf = udf(remove_initial, StringType())
    return df.withColumn(column_name, remove_initial_udf(column_name))
