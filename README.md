s3_path = "s3a://your-bucket/your-prefix/"
size = sum([file.size for file in dbutils.fs.ls(s3_path)])
print(size)
