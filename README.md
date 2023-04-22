from datetime import date

# Create a list of date objects
dates = [date(2022, 4, 5), date(2022, 4, 6), date(2022, 4, 7)]

# Convert the list of date objects to a list of strings
date_strings = [d.strftime("%Y-%m-%d") for d in dates]

print(date_strings) # ['2022-04-05', '2022-04-06', '2022-04-07']
