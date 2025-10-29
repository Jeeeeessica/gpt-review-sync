"""
monitor_pipeline.py
---------------------------------------
Monitors the execution of review_update.py and logs run results to Snowflake.
Logs include:
- run date
- status (SUCCESS / FAILURE)
- number of rows loaded (optional)
- error message (if any)
- duration (seconds)
---------------------------------------
"""

import os
import time
import traceback
import snowflake.connector
import subprocess
import re


def log_to_snowflake(status, rows_loaded, error_message, duration):
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

        # Create table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS PIPELINE_MONITORING (
                DATE DATE,
                TASK_NAME STRING,
                STATUS STRING,
                ROWS_LOADED INT,
                ERROR_MESSAGE STRING,
                DURATION_SEC FLOAT,
                TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Truncate long error message
        if error_message:
            error_message = error_message[:800] + " ..." if len(error_message) > 800 else error_message

        # Insert one log record
        cur.execute("""
            INSERT INTO PIPELINE_MONITORING
            (DATE, TASK_NAME, STATUS, ROWS_LOADED, ERROR_MESSAGE, DURATION_SEC)
            VALUES (CURRENT_DATE(), 'review_update', %s, %s, %s, %s)
        """, (status, rows_loaded, error_message, duration))

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


def main():
    """Run review_update.py and log the result."""
    start_time = time.time()
    status = "SUCCESS"
    rows_loaded = 0
    error_message = None

    try:
        print("Running main task: review_update.py ...")

        # Run the script and capture stdout/stderr
        result = subprocess.run(
            ["python", "review_update.py"],
            capture_output=True,
            text=True
        )

        # Print script output
        print(result.stdout)
        if result.returncode != 0:
            status = "FAILURE"
            error_message = result.stderr
        else:
            # Extract "ROWS_LOADED" from output if present
            match = re.search(r"ROWS_LOADED=(\d+)", result.stdout)
            if match:
                rows_loaded = int(match.group(1))
            print(f"review_update.py completed successfully. Rows loaded: {rows_loaded}")

    except Exception:
        status = "FAILURE"
        error_message = traceback.format_exc()
        print("review_update.py failed:\n", error_message)

    finally:
        duration = round(time.time() - start_time, 2)
        print(f"Pipeline finished in {duration} seconds with status: {status}")
        log_to_snowflake(status, rows_loaded, error_message, duration)

        if status == "FAILURE":
            print("Check PIPELINE_MONITORING table in Snowflake for error details.")


if __name__ == "__main__":
    main()
