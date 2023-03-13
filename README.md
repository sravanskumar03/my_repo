from datetime import datetime

date_string = '2020-01-31'
date_object = datetime.strptime(date_string, '%Y-%m-%d').date()

print(date_object)
