
from pyspark.sql import SparkSession
import boto3

def get_size_s3(s3_path):
    spark = SparkSession.builder.appName("S3Size").getOrCreate()
    sc = spark.sparkContext
    s3 = boto3.resource('s3')
    bucket_name = s3_path.split('/')[2]
    prefix = '/'.join(s3_path.split('/')[3:])
    bucket = s3.Bucket(bucket_name)
    total_size = 0
    for obj in bucket.objects.filter(Prefix=prefix):
        total_size += obj.size
    return total_size

s3_path = "s3://your-bucket/your-prefix/"
total_size = get_size_s3(s3_path)
print(f"Total size of data in {s3_path}: {total_size} bytes")
