from datetime import date, timedelta

start_date = date(2020, 1, 1)
end_date = date(2020, 1, 31)

delta = timedelta(days=1)
date_list = []
while start_date <= end_date:
    date_list.append(start_date.strftime('%m/%d/%Y'))
    start_date += delta

print(date_list)
