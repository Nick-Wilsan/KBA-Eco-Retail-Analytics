import pandas as pd

print('Checking IoT CSV date range...')
df = pd.read_csv('data/iot_telemetry_cleaned.csv', usecols=['timestamp'])
df['ts'] = pd.to_datetime(df['timestamp'], utc=True)

print(f'Total rows : {len(df):,}')
print(f'Min date   : {df["ts"].min()}')
print(f'Max date   : {df["ts"].max()}')
days = (df['ts'].max() - df['ts'].min()).days
print(f'Total days : {days} hari ({days//365} tahun)')

df['year'] = df['ts'].dt.year
print()
print('Rows per tahun:')
print(df['year'].value_counts().sort_index().to_string())
