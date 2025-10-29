USE DATABASE GPT_REVIEWS_DB;
USE SCHEMA PUBLIC;

CREATE OR REPLACE PROCEDURE DASHBOARD_REFRESH_PROC()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    CREATE OR REPLACE VIEW GPT_REVIEWS_DB.PUBLIC.PIPELINE_MONITORING_WITH_ANOMALY AS
    WITH base AS (
        SELECT 
            DATE AS RUN_DATE,
            TASK_NAME,
            STATUS,
            ROWS_LOADED,
            DURATION_SEC,
            ERROR_MESSAGE,
            CASE 
                WHEN ROWS_LOADED = 0 THEN 'NO_DATA'
                WHEN ERROR_MESSAGE IS NOT NULL THEN 'PIPELINE_ERROR'
                ELSE NULL
            END AS pipeline_flag
        FROM GPT_REVIEWS_DB.PUBLIC.PIPELINE_MONITORING
    ),
    field_check AS (
        SELECT 
            MAX(DATE_TRUNC('MONTH', CURRENT_DATE())) AS RUN_MONTH,
            'review_update' AS TASK_NAME,
            COUNT_IF(REVIEW_ID IS NULL) AS missing_review_id,
            COUNT_IF(CONTENT IS NULL OR TRIM(CONTENT) = '') AS missing_content,
            COUNT_IF(SCORE IS NULL) AS missing_score,
            COUNT(*) AS total_rows,
            ROUND(100 * COUNT_IF(CONTENT IS NULL OR TRIM(CONTENT) = '') / NULLIF(COUNT(*), 0), 2) AS missing_content_pct
        FROM GPT_REVIEWS_DB.PUBLIC.REVIEWS
    )
    SELECT 
        b.RUN_DATE,
        b.TASK_NAME,
        b.STATUS,
        b.ROWS_LOADED,
        b.DURATION_SEC,
        b.ERROR_MESSAGE,
        COALESCE(
            b.pipeline_flag,
            CASE WHEN f.missing_content_pct > 10 THEN 'MISSING_FIELDS' END
        ) AS FINAL_ANOMALY_FLAG,
        f.missing_review_id,
        f.missing_content,
        f.missing_score,
        f.missing_content_pct
    FROM base b
    LEFT JOIN field_check f
      ON b.TASK_NAME = f.TASK_NAME
    ORDER BY b.RUN_DATE DESC;

    RETURN 'Dashboard view recreated successfully';
END;
$$;

CREATE OR REPLACE TASK DASHBOARD_MONTHLY_REFRESH_TASK
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 9 1 * * UTC'
AS
CALL DASHBOARD_REFRESH_PROC();



-- Test the refresh 

EXECUTE TASK DASHBOARD_MONTHLY_REFRESH_TASK;

SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'DASHBOARD_MONTHLY_REFRESH_TASK'
ORDER BY COMPLETED_TIME DESC;







