import pandas as pd

df = pd.read_parquet('~/data/parquet')
df.head()

fdf = df[df['dt']=='2024-07-12']
fdf.head()

sdf = fdf.sort_values(by='cnt', ascending=False).head(10)
sdf

ddf = sdf.drop(columns=['dt'])
ddf


