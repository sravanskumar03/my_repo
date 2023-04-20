import boto3

s3 = boto3.resource('s3')
bucket = s3.Bucket('your-bucket')
prefix = 'your-prefix/'

result = [o.key for o in bucket.objects.filter(Prefix=prefix) if o.key.endswith('/')]
print(result)
