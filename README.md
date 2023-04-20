
from pyspark.sql import SparkSession
import boto3
from collections import defaultdict

spark = SparkSession.builder.appName("S3Size").getOrCreate()
sc = spark.sparkContext
s3 = boto3.resource('s3')

def get_size_s3(s3_path):
    bucket_name = s3_path.split('/')[2]
    prefix = '/'.join(s3_path.split('/')[3:])
    bucket = s3.Bucket(bucket_name)
    sizes = defaultdict(int)
    for obj in bucket.objects.filter(Prefix=prefix):
        folder_name = obj.key.split('/')[0]
        sizes[folder_name] += obj.size
    return sizes

def get_folder_size(folder_name):
    s3_path = f"s3://your-bucket/your-prefix/{folder_name}/"
    sizes = get_size_s3(s3_path)
    return (folder_name, sizes[folder_name])

folders = ["folder1", "folder2", "folder3"]
folder_sizes_rdd = sc.parallelize(folders).map(get_folder_size)
folder_sizes = folder_sizes_rdd.collect()

for folder_name, size in folder_sizes:
    print(f"Size of data in {folder_name}: {size} bytes")
