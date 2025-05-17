from datetime import datetime, timedelta
import pandas as pd

df = pd.DataFrame({
    'id': [1, 2, 3],
    'user_id': [101, 102, 103],
    'order_date': ['2023-10-01', '2023-10-02', '2023-10-03'],
    'total_price': [100.0, 200.0, 300.0]
})

print(df)