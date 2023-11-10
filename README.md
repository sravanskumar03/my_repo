from datetime import datetime

date_string = "2020-01-01"
date_object = datetime.strptime(date_string, "%Y-%m-%d")
integer_value = int(date_object.timestamp())
