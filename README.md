import datetime

def get_month_end_dates():
    today = datetime.date.today()
    end_of_month = datetime.date(today.year, today.month, 1) + datetime.timedelta(days=32)
    end_of_month = end_of_month.replace(day=1) - datetime.timedelta(days=1)
    month_end_dates = []
    while end_of_month >= today:
        month_end_dates.append(end_of_month)
        end_of_month -= datetime.timedelta(days=1)
    return month_end_dates
