
from pyspark.sql import SparkSession
import boto3

# create a spark session
spark = SparkSession.builder.appName("S3DataSize").getOrCreate()

# set up the S3 client
s3 = boto3.client('s3')

# specify the bucket and prefix (folder) for the data
bucket = 'your-bucket-name'
prefix = 'your-prefix'

# get the list of objects in the specified location
response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

# calculate the total size of the data
total_size = sum(obj['Size'] for obj in response['Contents'])

# print the result
print(f'Total size of data in {bucket}/{prefix}: {total_size} bytes')
