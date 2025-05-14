from datetime import datetime, timedelta

try:
    DATE_EXECUTION = event["DATE_EXECUTION"]
except Exception as e:
    DATE_EXECUTION = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

year = DATE_EXECUTION.split('-')[0]
month = DATE_EXECUTION.split('-')[1]
day = DATE_EXECUTION.split('-')[2]

print(year)
print(month)
print(day)