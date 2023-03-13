from datetime import timedelta, datetime

# Define start and end dates
start_date = datetime(2020, 1, 1)
end_date = datetime(2020, 12, 31)

# Generate sequence of dates as list
date_list = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

# Print the list of dates
print(date_list)
