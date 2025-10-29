# monitor_pipeline.py
# Unified Eastern Time version with anomaly alert and Snowflake logging

import os
import time
import traceback
import smtplib
from email.mime.text import MIMEText
import snowflake.connector
from datetime import datetime
import pytz

# Define Eastern Time (America/New_York)
ET = pytz.timezone("America/New_York")


def log_to_snowflake(status, rows_loaded, error_message, duration, anomaly_flag=None):
    """Write pipeline run log into Snowflake table PIPELINE_MONITORING (ET timezone)."""
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )
        cur = conn.cursor()

        # Create table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS PIPELINE_MONITORING (
                DATE DATE,
                TASK_NAME STRING,
                STATUS STRING,
                ROWS_LOADED INT,
                ERROR_MESSAGE STRING,
                DURATION_SEC FLOAT,
                ANOMALY_FLAG STRING,
                TIMESTAMP TIMESTAMP_NTZ,
                LOGGED_AT STRING
            );
        """)

        # Truncate long error messages
        if error_message:
            error_message = error_message[:800] + " ..." if len(error_message) > 800 else error_message

        # Get current ET time
        now_et = datetime.now(ET)
        date_et = now_et.date()
        timestamp_str = now_et.strftime("%Y-%m-%d %H:%M:%S")

        # Insert log
        cur.execute("""
            INSERT INTO PIPELINE_MONITORING
            (DATE, TASK_NAME, STATUS, ROWS_LOADED, ERROR_MESSAGE, DURATION_SEC, ANOMALY_FLAG, TIMESTAMP, LOGGED_AT)
            VALUES (%s, 'review_update', %s, %s, %s, %s, %s, %s, %s)
        """, (date_et, status, rows_loaded, error_message, duration, anomaly_flag, timestamp_str, timestamp_str))

        conn.commit()
        print(f"Pipeline status logged successfully at {timestamp_str} ET.")

    except Exception as e:
        print("Failed to log to Snowflake:", e)

    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def get_last_run_rows():
    """Fetch the ROWS_LOADED value from the last successful run."""
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT ROWS_LOADED
            FROM PIPELINE_MONITORING
            WHERE STATUS = 'SUCCESS'
            ORDER BY TIMESTAMP DESC
            LIMIT 1 OFFSET 1
        """)
        row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        print("Could not fetch previous ROWS_LOADED:", e)
        return None
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def send_email(subject, body):
    """Send email alert when anomaly or failure occurs."""
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "465"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")
    sender = os.getenv("ALERT_FROM")
    recipient = os.getenv("ALERT_TO")

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    try:
        with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
            server.login(smtp_user, smtp_pass)
            server.sendmail(sender, [recipient], msg.as_string())
        print("Alert email sent successfully.")
    except Exception as e:
        print("Failed to send alert email:", e)


def main():
    """Run review_update.py and log result with anomaly detection."""
    import review_update
    start_time = time.time()
    status = "SUCCESS"
    rows_loaded = 0
    error_message = None
    anomaly_flag = None

    try:
        print("Running monthly review update...")
        if hasattr(review_update, "main"):
            rows_loaded = review_update.main()
            print(f"review_update.main() completed. Rows loaded: {rows_loaded}")
        else:
            raise RuntimeError("review_update.main() not found")

        # Simple anomaly detection
        last_rows = get_last_run_rows()
        if last_rows and last_rows > 0:
            diff_ratio = rows_loaded / last_rows
            if diff_ratio < 0.2:
                anomaly_flag = f"WARNING: Only {rows_loaded} rows vs {last_rows} last month"
                print(anomaly_flag)
            else:
                anomaly_flag = "OK"
        else:
            anomaly_flag = "FIRST_RUN"

    except Exception:
        status = "FAILURE"
        error_message = traceback.format_exc()
        print("review_update.py failed:\n", error_message)

    finally:
        duration = round(time.time() - start_time, 2)
        print(f"Pipeline finished in {duration} seconds with status: {status}")
        log_to_snowflake(status, rows_loaded, error_message, duration, anomaly_flag)

        # Send alert email if failure or anomaly detected
        if status == "FAILURE" or (anomaly_flag and "WARNING" in anomaly_flag):
            subject = f"Pipeline anomaly detected: {status}"
            body = f"""
Pipeline status: {status}
Rows loaded: {rows_loaded}
Anomaly flag: {anomaly_flag}
Duration: {duration} seconds
Error: {error_message or 'None'}
Time (ET): {datetime.now(ET).strftime('%Y-%m-%d %H:%M:%S')}
"""
            send_email(subject, body)


if __name__ == "__main__":
    main()


