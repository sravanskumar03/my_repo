
from pyspark.sql import SparkSession
import boto3

spark = SparkSession.builder.appName("S3FolderSize").getOrCreate()
sc = spark.sparkContext
s3 = boto3.resource('s3')

def get_folder_size_s3(s3_path):
    bucket_name = s3_path.split('/')[2]
    prefix = '/'.join(s3_path.split('/')[3:])
    bucket = s3.Bucket(bucket_name)
    total_size = 0
    for obj in bucket.objects.filter(Prefix=prefix):
        total_size += obj.size
    return total_size

s3_path = "s3://your-bucket/your-prefix/your-folder/"
total_size = get_folder_size_s3(s3_path)
print(f"Total size of data in folder: {total_size} bytes")
