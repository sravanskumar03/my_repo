from multiprocessing.pool import ThreadPool

# Source and destination S3 paths
src_s3_path = "s3://my-source-bucket/path/to/files/"
dst_s3_path = "s3://my-destination-bucket/path/to/files/"

# Function to copy files using dbutils.fs.cp
def copy_file(src_path, dst_path):
    dbutils.fs.cp(src_path, dst_path)

# Function to copy files in parallel using multiprocessing
def parallel_copy_file(src_file_path):
    # Generate destination S3 path for the file
    dst_file_path = dst_s3_path + src_file_path[len(src_s3_path):]

    # Copy the file using multiprocessing
    pool = ThreadPool(processes=4) # Change number of processes as per your requirement
    pool.apply_async(copy_file, args=(src_file_path, dst_file_path))
    pool.close()
    pool.join()

# Get a list of all files in the source S3 path
s3_files = [obj.key for obj in dbutils.fs.ls(src_s3_path)]

# Copy files in parallel
for s3_file in s3_files:
    parallel_copy_file(s3_file.path)
