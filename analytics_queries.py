import pandas as pd

fact = pd.read_csv("fact_user_event.csv", parse_dates=['timestamp'])
dim_user = pd.read_csv("dim_user.csv")
dim_time = pd.read_csv("dim_time.csv")
dim_event = pd.read_csv("dim_event.csv")

fact = fact.merge(dim_time, left_on='time_sk', right_on='time_id', how='left')
fact = fact.merge(dim_user[['user_sk', 'signup_date']], on='user_sk', how='left')

fact['date'] = pd.to_datetime(fact['date'])
fact['week'] = fact['date'].dt.isocalendar().week
fact['year'] = fact['date'].dt.year

dau = (
    fact.groupby('date')['user_sk']
    .nunique()
    .reset_index(name='daily_active_users')
)

dau.to_csv("daily_active_users.csv", index=False)

wau = (
    fact.groupby(['year', 'week'])['user_sk']
    .nunique()
    .reset_index(name='weekly_active_users')
)

wau.to_csv("weekly_active_users.csv", index=False)


fact_retention = fact.copy()
fact_retention['activity_date'] = pd.to_datetime(fact_retention['date'])
fact_retention['signup_date'] = pd.to_datetime(fact_retention['signup_date'])

fact_retention = fact_retention[fact_retention['activity_date'] >= fact_retention['signup_date']]

fact_retention['days_since_signup'] = (fact_retention['activity_date'] - fact_retention['signup_date']).dt.days

retention = fact_retention.groupby(['signup_date', 'days_since_signup'])['user_sk'].nunique().reset_index()

retention_pivot = retention.pivot(index='signup_date', columns='days_since_signup', values='user_sk').fillna(0).astype(int)

retention_percent = retention_pivot.div(retention_pivot[0], axis=0).round(2)

retention_pivot.to_csv("retention_matrix_counts.csv")
retention_percent.to_csv("retention_matrix_percent.csv")

funnel_events = ['login', 'view_item', 'add_to_cart', 'purchase']
funnel_df = fact.merge(dim_event, left_on='event_sk', right_on='event_id')
funnel_df = funnel_df[funnel_df['event_type'].isin(funnel_events)]

funnel_counts = (
    funnel_df.groupby('event_type')['user_sk']
    .nunique()
    .reindex(funnel_events)  
    .reset_index(name='unique_users')
)

funnel_counts['conversion_rate'] = (funnel_counts['unique_users'] / funnel_counts.loc[0, 'unique_users']).round(2)

funnel_counts.to_csv("funnel_analysis.csv", index=False)
