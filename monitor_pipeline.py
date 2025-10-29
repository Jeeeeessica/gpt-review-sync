import os
import time
import traceback
import snowflake.connector
import smtplib
from email.mime.text import MIMEText


def send_email(subject, body):
    """Send a simple email alert via SMTP."""
    try:
        smtp_server = os.getenv("SMTP_SERVER")
        smtp_port = int(os.getenv("SMTP_PORT", 587))
        smtp_user = os.getenv("SMTP_USER")
        smtp_password = os.getenv("SMTP_PASSWORD")
        recipients = os.getenv("ALERT_EMAILS", "").split(",")

        if not recipients or recipients == [""]:
            print("No ALERT_EMAILS configured. Skipping email alert.")
            return

        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = smtp_user
        msg["To"] = ", ".join(recipients)

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, recipients, msg.as_string())

        print(f"Email alert sent to: {recipients}")

    except Exception as e:
        print("Failed to send email:", e)


def log_to_snowflake(status, rows_loaded, error_message, duration, anomaly_flag=None):
    """Write pipeline run log into Snowflake table PIPELINE_MONITORING."""
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
            CREATE TABLE IF NOT EXISTS PIPELINE_MONITORING (
                DATE DATE,
                TASK_NAME STRING,
                STATUS STRING,
                ROWS_LOADED INT,
                ERROR_MESSAGE STRING,
                DURATION_SEC FLOAT,
                ANOMALY_FLAG STRING,
                TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)

        if error_message:
            error_message = error_message[:800] + " ..." if len(error_message) > 800 else error_message

        cur.execute("""
            INSERT INTO PIPELINE_MONITORING
            (DATE, TASK_NAME, STATUS, ROWS_LOADED, ERROR_MESSAGE, DURATION_SEC, ANOMALY_FLAG)
            VALUES (CURRENT_DATE(), 'review_update', %s, %s, %s, %s, %s)
        """, (status, rows_loaded, error_message, duration, anomaly_flag))

        conn.commit()
        print("Pipeline status logged successfully in Snowflake.")

    except Exception as e:
        print("Failed to log to Snowflake:", e)

    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def get_last_run_rows():
    """Fetch the ROWS_LOADED value from the last successful run for comparison."""
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


def main():
    """Run review_update.py and log the result with anomaly detection."""
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

        last_rows = get_last_run_rows()
        if last_rows and last_rows > 0:
            diff_ratio = rows_loaded / last_rows if last_rows else 1
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

        # Send alert only if failure or anomaly
        if status == "FAILURE" or (anomaly_flag and anomaly_flag.startswith("WARNING")):
            subject = f"[Pipeline Alert] review_update.py {status}"
            body = f"Status: {status}\nRows Loaded: {rows_loaded}\nDuration: {duration}s\n\n{anomaly_flag or ''}\n\n{error_message or ''}"
            send_email(subject, body)


if __name__ == "__main__":
    main()


