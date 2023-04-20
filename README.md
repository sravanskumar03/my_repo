from pyspark.sql.functions import col

def get_size(s3_path):
    files = dbutils.fs.ls(s3_path)
    size = sum([file.size for file in files])
    for file in files:
        if file.isDir():
            size += get_size(file.path)
    return size

s3_path = "s3a://your-bucket/your-prefix/"
size = get_size(s3_path)
print(size)
