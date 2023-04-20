
from pyspark.sql import SparkSession
import boto3

# create a spark session
spark = SparkSession.builder.appName("S3FolderDataSize").getOrCreate()

# set up the S3 client
s3 = boto3.client('s3')

# specify the bucket and prefix (folder) for the data
bucket = 'your-bucket-name'
prefix = 'your-prefix'

# get the list of objects in the specified location
response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')

# get the list of folder names
folders = [folder['Prefix'] for folder in response['CommonPrefixes']]

# calculate the size of data in each folder
folder_sizes = {}
for folder in folders:
    response = s3.list_objects_v2(Bucket=bucket, Prefix=folder)
    total_size = sum(obj['Size'] for obj in response['Contents'])
    folder_sizes[folder] = total_size

# print the result
print(f'Size of data in each folder in {bucket}/{prefix}:')
for folder, size in folder_sizes.items():
    print(f'- {folder}: {size} bytes')
