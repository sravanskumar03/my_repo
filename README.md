import re

# Assume 'my_list' is the Python list that we want to extract elements from
my_list = ["apple", "'banana'", "cherry", "'date'"]

# Use a regular expression to extract elements in the list that are enclosed in single quotes
pattern = re.compile(r"'(.*?)'")  # Matches any sequence of characters enclosed in single quotes
result = [element for element in my_list if pattern.match(element)]

# Now 'result' contains only the elements in 'my_list' that are enclosed in single quotes
