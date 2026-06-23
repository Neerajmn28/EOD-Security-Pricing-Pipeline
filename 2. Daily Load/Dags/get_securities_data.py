from airflow import DAG
import pendulum
from airflow.models import Variable
from airflow.exceptions import AirflowFailException  # To raise exceptions in case of failures
from airflow.providers.standard.operators.python import PythonOperator  # To execute Python functions as tasks
import os
import logging
from lib.eod_data_downloader import download_polygon_eod_data_to_csv  # Custom function to download EOD data
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator  # To upload files to S3
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator  # To execute SQL queries in Snowflake
from airflow.utils.task_group import TaskGroup  # To group related tasks together

from lib.slack_utils import slack_post, on_task_failure  # Custom Slack utility functions for notifications
import logging

POLYGON_API_KEY = Variable.get('POLYGON_API_KEY')
POLYGON_MAX_LOOKBACK_DAYS = int(Variable.get('LOOKBACK_DAYS', default_var = '10'))
S3_BUCKET = Variable.get('S3_BUCKET')  # Ensure this variable is set in Airflow Variables
TEMPLATE_SEARCHPATH = [os.path.join(os.path.dirname(__file__), "sql")]  # Directory where your SQL files are located


log = logging.getLogger(__name__)

DEFAULT_ARGS = {'owner': 'data-eng',
                'retries': 3,
                'retry_delay': pendulum.duration(minutes = 5)
                }


with DAG(
    dag_id ='polygon_eod_data_downloader_final_v2',
    start_date = pendulum.datetime(2025, 1, 1),
    schedule = '5 21 * * 1-5',
    catchup = False,
    max_active_runs = 1,
    default_args = DEFAULT_ARGS,
    tags = ['securities', 'batch', 'polygon'],
    description = 'Polygon-only batch EOD: Download and process the latest available trading day',
    template_searchpath = TEMPLATE_SEARCHPATH,  # Set the search path for SQL files
)as dag:
    
    def download_trading_day_csv(**ctx):
        '''
        This function downloads the polygon EOD data 
        and stores it as a csv file in the specified location
        '''
        
        trading_date = download_polygon_eod_data_to_csv(POLYGON_API_KEY, POLYGON_MAX_LOOKBACK_DAYS)
        
        
        # Push the trading day to XCOM for further tasks if needed
        ctx['ti'].xcom_push(key='trading_date', value = trading_date)
        
        log.info(f'Download EOD data for {trading_date}')
        
    download = PythonOperator(
        task_id = 't01_download_to_csv',
        python_callable = download_trading_day_csv,
    )
    
    
    
        # Step : Verify local file
    def verify_file_exists(**ctx):
        """
        This function checks if the expected CSV file exists at the given local path.
        If not, it raises an AirflowFailException.
        """

        # Get the trading date from XCom (from the previous task)
        trading_date = ctx["ti"].xcom_pull(task_ids="t01_download_to_csv", key="trading_date")  # Ensure
        path = f"/tmp/eod_{trading_date}.csv"  # Construct the path of the file
        log.info("[verify] expecting file at: %s", path)

        # Check if the file exists locally
        if not os.path.exists(path):
            raise AirflowFailException(f"Expected file not found: {path}")

        # Log the file size if it exists
        log.info("[verify] file exists at %s (size=%s bytes)", path, os.path.getsize(path))
        
    verify_file = PythonOperator(
            task_id="t02_verify_local_file",
            python_callable=verify_file_exists)
   
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="t03_upload_to_s3",
        filename="/tmp/eod_{{ti.xcom_pull(task_ids='t01_download_to_csv', key='trading_date')}}.csv",
        dest_bucket=S3_BUCKET,
        dest_key=(
            "market/bronze/eod/"
            "eod_prices_{{ti.xcom_pull(task_ids='t01_download_to_csv', key='trading_date')}}.csv"
        ),
        aws_conn_id="aws_default",
        replace=True,
    )
    
    
    
    
    # Step : Snowflake load
    
    with TaskGroup(group_id = "t04_snowflake_load") as snowflake_load:
        params_common = {"trading_ds_task_is": "t01_download_to_csv"}
        copy_to_raw = SQLExecuteQueryOperator(
            task_id = "S01_copy_to_raw",
            conn_id = "snowflake_default",
            sql = "1. copy_to_raw.sql",
            params = params_common,
        )
    
    check_loaded = SQLExecuteQueryOperator(
        task_id = "check_eod_prices_exist",
        sql = "2. check_loaded.sql",
        conn_id = "snowflake_default",
        params = params_common,
    )
    
    premerge_metrics = SQLExecuteQueryOperator(
        task_id = "S03_compute_premerge_metrics",
        conn_id = "snowflake_default",
        sql = "3. premerge_metrics.sql",
        params = params_common,
    )    
    
    merge_core = SQLExecuteQueryOperator(
        task_id = "S04_merge_core_eod",
        conn_id = "snowflake_default",
        sql = "4. merge_core.sql",
        params = params_common,
    )
    
    
    merge_dim_security = SQLExecuteQueryOperator(
        task_id = "S05_merge_dim_security",
        conn_id = "snowflake_default",
        sql = "5. merge_dim_security.sql",
        params = params_common,
    )
    
    postmerge = SQLExecuteQueryOperator(
        task_id = "S08_compute_postmerge_updates",
        conn_id = "snowflake_default",
        sql = "8. postmerge.sql",
        params = params_common,
    )
    
    copy_to_raw >> check_loaded >> premerge_metrics
        
    download >> verify_file >> upload_to_s3 >> snowflake_load
    
    
    
    # Step: Slack summary
    def notify_slack_summary(**ctx):
        """
        Sends a summary message to Slack at the end of the DAG.
        Pulls metrics from pre/post merge tasks and posts a compact summary.
        """
        trading_date = ctx["ti"].xcom_pull(task_ids="t01_download_to_csv", key="trading_date")
        pre = ctx["ti"].xcom_pull(task_ids="t04_snowflake_load.s03_compute_premerge_metrics") or []
        post = ctx["ti"].xcom_pull(task_ids="t04_snowflake_load.s08_compute_postmerge_metrics") or []

        raw_cnt = ins_est = upd_est = core_ds = fact_ds = 0

        # pre = [(raw_cnt, core_existing_cnt, ins_est, upd_est)]
        if pre and len(pre[0]) >= 4:
            raw_cnt, reject_cnt, ins_est, upd_est = pre[0]

        # post = [(core_rows, fact_rows)]
        if post and len(post[0]) >= 2:
            core_ds, fact_ds = post[0]

        msg = (
                ":white_check_mark: *EOD Summary*\n"
                f"• Trading Date: `{trading_date}`\n"
                f"• RAW rows: `{int(raw_cnt):,}`\n"
                f"• Reject rows: `{int(reject_cnt):,}`\n"
                f"• Estimated CORE inserts: `{int(ins_est):,}`\n"
                f"• Estimated CORE updates: `{int(upd_est):,}`\n"
                f"• CORE rows after merge: `{int(core_ds):,}`\n"
                f"• FACT rows after merge: `{int(fact_ds):,}`"
            )
        slack_post(msg)


    slack_summary = PythonOperator(
        task_id="t05_notify_slack_summary",
        python_callable=notify_slack_summary,
        trigger_rule="all_done",   # ensure Slack fires even if an upstream task failed/skipped
    )


    snowflake_load >> slack_summary