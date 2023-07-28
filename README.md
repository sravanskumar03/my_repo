from datetime import datetime, timedelta

def generate_dates(start_date: str, end_date: str) -> list:
    return [(datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=i)).strftime('%Y-%m-%d') for i in range((datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days + 1)]
