
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
import boto3

# create a spark session
spark = SparkSession.builder.appName("S3FolderDataSizeDF").getOrCreate()

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
folder_sizes = []
for folder in folders:
    response = s3.list_objects_v2(Bucket=bucket, Prefix=folder)
    total_size = sum(obj['Size'] for obj in response['Contents'])
    folder_sizes.append((folder, total_size))

# create a DataFrame from the data
schema = StructType([
    StructField('Folder', StringType(), True),
    StructField('Size', LongType(), True)
])
df = spark.createDataFrame(folder_sizes, schema)

# show the result
df.display()
