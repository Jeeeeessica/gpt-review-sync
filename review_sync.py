"""Install packages"""

!pip install -q google-play-scraper snowflake-connector-python pandas tqdm

"""Import libraries"""

from google_play_scraper import reviews, Sort
import pandas as pd
import snowflake.connector
from datetime import datetime
from tqdm import tqdm
import time

"""Define Snowflake connection parameters"""

conn_params = {
    "user": "USER1204",
    "password": "Review20251018@",
    "account": "XUZXIIE-EAC06737",
    "warehouse": "COMPUTE_WH",
    "database": "GPT_REVIEWS_DB",
    "schema": "PUBLIC",
    "role": "ACCOUNTADMIN"
}

"""Fetch reviews from Google Play"""

print("Fetching reviews from Google Play...")

app_id = "com.openai.chatgpt"
buf, token, first = [], None, True
pbar = tqdm(desc="Fetching", unit="reviews")

while True:
    if first:
        res, token = reviews(app_id, lang="en", country="us", sort=Sort.NEWEST, count=200)
        first = False
    else:
        if token is None:
            break
        res, token = reviews(app_id, continuation_token=token)

    if not res:
        break

    buf.extend(res)
    pbar.update(len(res))

    time.sleep(0.2)

pbar.close()
df = pd.DataFrame(buf)
print(f"Total reviews fetched: {len(df):,}")

"""Clean and format data"""

# Convert 'at' column to datetime
df['at'] = pd.to_datetime(df['at'], errors='coerce')

# Rename columns for clarity
df.rename(columns={
    "reviewId": "review_id",
    "userName": "user_name",
    "content": "content",
    "score": "score",
    "at": "created_at",
    "appVersion": "app_version"
}, inplace=True)

# Keep only necessary fields
df = df[[
    "review_id", "user_name", "content",
    "score", "created_at", "app_version"
]]

"""Upload to Snowflake"""

# Commented out IPython magic to ensure Python compatibility.
print("Uploading data to Snowflake...")

conn = snowflake.connector.connect(**conn_params)
cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS reviews (
    review_id STRING PRIMARY KEY,
    user_name STRING,
    content TEXT,
    score INT,
    created_at TIMESTAMP,
    app_version STRING
)
""")

# Insert or update each row
for _, row in df.iterrows():
    row["created_at"] = row["created_at"].strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(row["created_at"]) else None

    cursor.execute("""
        MERGE INTO reviews AS target
        USING (SELECT %s AS review_id, %s AS user_name, %s AS content,
#                       %s AS score, %s AS created_at, %s AS app_version) AS source
        ON target.review_id = source.review_id
        WHEN MATCHED THEN UPDATE SET
            content = source.content,
            score = source.score,
            created_at = source.created_at,
            app_version = source.app_version
        WHEN NOT MATCHED THEN INSERT (
            review_id, user_name, content,
            score, created_at, app_version
        ) VALUES (
            source.review_id, source.user_name, source.content,
            source.score, source.created_at, source.app_version
        )
    """, tuple(row))

cursor.close()
conn.close()
print("Data upload completed.")
