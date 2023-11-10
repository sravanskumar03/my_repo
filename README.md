import datetime

def get_month_end_dates(year):
    month_end_dates = []
    for month in range(1, 13):
        end_of_month = datetime.date(year, month, 1) + datetime.timedelta(days=32)
        end_of_month = end_of_month.replace(day=1) - datetime.timedelta(days=1)
        month_end_dates.append(end_of_month.strftime('%Y-%m-%d'))
    return month_end_dates
