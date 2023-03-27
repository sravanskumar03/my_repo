# Assume 'row_object' is a row object that we want to convert to a Python list
row_dict = row_object.asDict()

# Convert the dictionary to a list of values
row_list = list(row_dict.values())

# Now 'row_list' is a Python list containing the values of the columns in 'row_object'
