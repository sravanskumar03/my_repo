def bytes_to_mb(bytes):
    return round(bytes / (1024 * 1024), 2)

# example usage
size_in_bytes = 123456789
size_in_mb = bytes_to_mb(size_in_bytes)
print(f'{size_in_bytes} bytes = {size_in_mb} MB')
