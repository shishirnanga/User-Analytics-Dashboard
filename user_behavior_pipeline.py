from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta

faker = Faker()
num_users = 200
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 3, 1)
event_types = ['login', 'view_item', 'add_to_cart', 'purchase', 'logout']
devices = ['mobile', 'desktop', 'tablet']
regions = ['NA', 'EU', 'APAC']

users = []
for i in range(1, num_users + 1):
    users.append({
        'user_id': i,
        'name': faker.name(),
        'email': faker.email(),
        'signup_date': start_date + timedelta(days=random.randint(0, 30)),
        'region': random.choice(regions)
    })
df_users = pd.DataFrame(users)


events = []
current = start_date
user_ids = [u['user_id'] for u in users]

while current <= end_date:
    active_users = random.sample(user_ids, random.randint(30, 80))
    for user_id in active_users:
        for _ in range(random.randint(1, 5)):
            events.append({
                'user_id': user_id,
                'event_type': random.choice(event_types),
                'timestamp': current + timedelta(minutes=random.randint(0, 1439)),
                'device': random.choice(devices)
            })
    current += timedelta(days=1)
df_events = pd.DataFrame(events)

df_events['event_date'] = df_events['timestamp'].dt.date

dim_user = df_users.copy()
dim_user['user_sk'] = dim_user['user_id']

unique_events = df_events['event_type'].unique()
dim_event = pd.DataFrame({
    'event_id': range(1, len(unique_events) + 1),
    'event_type': unique_events
})

dim_time = pd.DataFrame(df_events['event_date'].unique(), columns=['date'])
dim_time['time_id'] = range(1, len(dim_time) + 1)
dim_time = dim_time[['time_id', 'date']]

fact_user_event = df_events.merge(dim_event, on='event_type', how='left')
fact_user_event = fact_user_event.merge(
    dim_time, left_on=df_events['timestamp'].dt.date, right_on='date', how='left'
)
fact_user_event.rename(columns={
    'user_id': 'user_sk',
    'event_id': 'event_sk',
    'time_id': 'time_sk'
}, inplace=True)
fact_user_event = fact_user_event[['user_sk', 'event_sk', 'time_sk', 'timestamp', 'device']]

df_users.to_csv("users.csv", index=False)
df_events.to_csv("events.csv", index=False)
dim_user.to_csv("dim_user.csv", index=False)
dim_event.to_csv("dim_event.csv", index=False)
dim_time.to_csv("dim_time.csv", index=False)
fact_user_event.to_csv("fact_user_event.csv", index=False)

print("All CSVs generated: users.csv, events.csv, dim_user.csv, dim_event.csv, dim_time.csv, fact_user_event.csv")
