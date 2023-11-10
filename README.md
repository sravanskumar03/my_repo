import datetime

def get_all_mondays(year):
    """
    Returns a list of all Mondays in the given year.
    """
    d = datetime.date(year, 1, 1)
    days_to_monday = 7 - d.weekday()
    if days_to_monday == 7:
        days_to_monday = 0
    d += datetime.timedelta(days_to_monday)
    mondays = []
    while d.year == year:
        mondays.append(d)
        d += datetime.timedelta(days=7)
    return mondays
