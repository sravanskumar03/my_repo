
from pyspark.sql import SparkSession
import boto3

spark = SparkSession.builder.appName("S3LeafFolder").getOrCreate()
sc = spark.sparkContext
s3 = boto3.resource('s3')

def get_leaf_folder_s3(s3_path):
    bucket_name = s3_path.split('/')[2]
    prefix = '/'.join(s3_path.split('/')[3:])
    bucket = s3.Bucket(bucket_name)
    leaf_folder = None
    for obj in bucket.objects.filter(Prefix=prefix):
        folder_name = obj.key.split('/')[-2]
        leaf_folder = folder_name
    return leaf_folder

s3_path = "s3://your-bucket/your-prefix/"
leaf_folder = get_leaf_folder_s3(s3_path)
print(leaf_folder)
