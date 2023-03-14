from datetime import datetime

date_string = '12/31/2020'
date_object = datetime.strptime(date_string, '%m/%d/%Y')
new_date_string = date_object.strftime('%Y-%m-%d')

print(new_date_string)
