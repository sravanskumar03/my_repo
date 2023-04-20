
s3_path = "s3a://your-bucket/your-prefix/"
result = [file.name for file in dbutils.fs.ls(s3_path)]
print(result)
