# review_update.py

from google_play_scraper import reviews, Sort, app
import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta, timezone
from tqdm import tqdm
import time
import os
import sys
import traceback


def main():
    """Fetch new Google Play reviews and upload them to Snowflake."""
    rows_loaded = 0  # Default in case no new data
    try:
        print("Connecting to Snowflake...")

        conn_params = {
            "user": os.getenv("SNOWFLAKE_USER", "USER1204"),
            "password": os.environ["SNOWFLAKE_PASSWORD"],
            "account": os.getenv("SNOWFLAKE_ACCOUNT", "XUZXIIE-EAC06737"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": os.getenv("SNOWFLAKE_DATABASE", "GPT_REVIEWS_DB"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
            "role": "ACCOUNTADMIN"
        }

        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor()

        # Ensure target table exists
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

        cursor.execute("SELECT MAX(created_at) FROM reviews")
        last_uploaded = cursor.fetchone()[0]
        if last_uploaded is None:
            last_uploaded = datetime.utcnow() - timedelta(days=30)

        print(f"Last review timestamp: {last_uploaded}")

        # Fetch reviews
        print("Fetching new reviews from Google Play...")
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

            new_data = [r for r in res if r["at"] > last_uploaded]
            if not new_data:
                break

            buf.extend(new_data)
            pbar.update(len(new_data))
            time.sleep(0.2)

        pbar.close()

        if not buf:
            print("No new reviews to upload.")
            rows_loaded = 0
        else:
            df = pd.DataFrame(buf)
            rows_loaded = len(df)
            print(f"Fetched {rows_loaded:,} new reviews.")

            # Clean
            df['at'] = pd.to_datetime(df['at'], errors='coerce')
            df.rename(columns={
                "reviewId": "review_id",
                "userName": "user_name",
                "content": "content",
                "score": "score",
                "at": "created_at",
                "appVersion": "app_version"
            }, inplace=True)

            df = df[[
                "review_id", "user_name", "content",
                "score", "created_at", "app_version"
            ]]

            # Create staging table
            cursor.execute("CREATE OR REPLACE TEMPORARY TABLE reviews_staging LIKE reviews;")

            records = []
            for _, row in df.iterrows():
                created_at = row["created_at"].strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(row["created_at"]) else None
                records.append((
                    row["review_id"],
                    row["user_name"],
                    row["content"],
                    int(row["score"]) if pd.notnull(row["score"]) else None,
                    created_at,
                    row["app_version"]
                ))

            print(f"Total records to insert: {len(records):,}")
            BATCH_SIZE = 200000

            insert_sql = """
            INSERT INTO reviews_staging (
                review_id, user_name, content, score, created_at, app_version
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """

            for i in range(0, len(records), BATCH_SIZE):
                batch = records[i:i + BATCH_SIZE]
                print(f"Inserting batch {i // BATCH_SIZE + 1}: {len(batch):,} records...")
                cursor.executemany(insert_sql, batch)

            print("Merging into reviews table...")
            merge_sql = """
            MERGE INTO reviews AS target
            USING reviews_staging AS source
            ON target.review_id = source.review_id
            WHEN MATCHED THEN UPDATE SET
                content = source.content,
                score = source.score,
                created_at = source.created_at,
                app_version = source.app_version
            WHEN NOT MATCHED THEN INSERT (
                review_id, user_name, content, score, created_at, app_version
            ) VALUES (
                source.review_id, source.user_name, source.content,
                source.score, source.created_at, source.app_version
            )
            """
            cursor.execute(merge_sql)
            conn.commit()
            print("Reviews updated successfully.")

        # Insert app metadata
        print("\nFetching app metadata...")
        metadata = app(app_id, lang="en", country="us")
        fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        metadata_row = {
            "APP_VERSION": metadata.get("version"),
            "TITLE": metadata.get("title"),
            "DEVELOPER": metadata.get("developer", "OpenAI"),
            "GENRE": metadata.get("genre", "Productivity"),
            "SCORE": metadata.get("score"),
            "RATINGS_COUNT": metadata.get("ratings"),
            "REVIEWS_COUNT": metadata.get("reviews"),
            "INSTALLS": metadata.get("installs"),
            "REAL_INSTALLS": metadata.get("realInstalls"),
            "IS_FREE": metadata.get("free"),
            "PRICE": metadata.get("price"),
            "CURRENCY": metadata.get("currency"),
            "SALE": metadata.get("sale", False),
            "OFFERS_IAP": metadata.get("offersIAP"),
            "IAP_PRICE_RANGE": metadata.get("inAppProductPrice"),
            "URL": metadata.get("url", f"https://play.google.com/store/apps/details?id={app_id}"),
            "FETCHED_AT": fetched_at
        }

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS APP_METADATA (
            APP_VERSION STRING,
            TITLE STRING,
            DEVELOPER STRING,
            GENRE STRING,
            SCORE FLOAT,
            RATINGS_COUNT INT,
            REVIEWS_COUNT INT,
            INSTALLS STRING,
            REAL_INSTALLS INT,
            IS_FREE BOOLEAN,
            PRICE FLOAT,
            CURRENCY STRING,
            SALE BOOLEAN,
            OFFERS_IAP BOOLEAN,
            IAP_PRICE_RANGE STRING,
            URL STRING,
            FETCHED_AT TIMESTAMP
        )
        """)

        cursor.execute("""
        INSERT INTO APP_METADATA (
            APP_VERSION, TITLE, DEVELOPER, GENRE, SCORE, RATINGS_COUNT, REVIEWS_COUNT,
            INSTALLS, REAL_INSTALLS, IS_FREE, PRICE, CURRENCY, SALE, OFFERS_IAP,
            IAP_PRICE_RANGE, URL, FETCHED_AT
        ) VALUES (
            %(APP_VERSION)s, %(TITLE)s, %(DEVELOPER)s, %(GENRE)s, %(SCORE)s, %(RATINGS_COUNT)s, %(REVIEWS_COUNT)s,
            %(INSTALLS)s, %(REAL_INSTALLS)s, %(IS_FREE)s, %(PRICE)s, %(CURRENCY)s, %(SALE)s, %(OFFERS_IAP)s,
            %(IAP_PRICE_RANGE)s, %(URL)s, %(FETCHED_AT)s
        )
        """, metadata_row)

        conn.commit()
        cursor.close()
        conn.close()

        print("App metadata inserted successfully.")
        print(f"ROWS_LOADED={rows_loaded}")
        return rows_loaded

    except Exception:
        print("Script failed with error:")
        traceback.print_exc()
        print("ROWS_LOADED=0")
        sys.exit(1)


if __name__ == "__main__":
    main()


