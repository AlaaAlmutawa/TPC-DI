from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from constants import CSV_EXTENSION, GCS_BUCKET, BIG_QUERY_CONN_ID, GOOGLE_CLOUD_DEFAULT
from utils import construct_gcs_to_bq_operator, get_file_path, insert_overwrite, reset_table, insert_if_empty, \
    execute_sql

AIRFLOW = 'airflow'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG('historical_load_test', schedule_interval=None, default_args=default_args) as dag:

    create_staging_tables = execute_sql(task_id='create_staging_tables',
                                                sql_file_path='queries_test/data_sources_schema_dag.sql')
    # create_staging_references_tables = execute_sql(task_id='create_staging_references_tables',
    #                                             sql_file_path='queries_test/staging_reference_tables_schema_dag.sql')
    create_master_tables = execute_sql(task_id='create_master_tables',
                                                sql_file_path='queries_test/master_schema_dag.sql')
    load_hr_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_hr_to_staging',
        bucket=GCS_BUCKET,
        source_objects=['sf3/Batch1/HR.csv'],
        field_delimiter='|',
        destination_project_dataset_table='staging1.hr',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_batch_date_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_batch_date_to_staging',
        bucket=GCS_BUCKET,
        source_objects=['sf3/Batch1/BatchDate.txt'],
        field_delimiter='|',
        destination_project_dataset_table='staging1.batch_date',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_trade_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_trade_to_staging',
        bucket=GCS_BUCKET,
        source_objects=['sf3/Batch1/Trade.txt'],
        field_delimiter='|',
        destination_project_dataset_table='staging1.trade',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_cash_transaction_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_cash_transaction_to_staging',
        bucket=GCS_BUCKET,
        source_objects=['sf3/Batch1/CashTransaction.txt'],
        field_delimiter='|',
        destination_project_dataset_table='staging1.cash_transaction',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_holding_history_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_holding_history_to_staging',
        bucket=GCS_BUCKET,
        source_objects=['sf3/Batch1/HoldingHistory.txt'],
        field_delimiter='|',
        destination_project_dataset_table='staging1.holding_history',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_daily_market_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_daily_market_to_staging',
        bucket=GCS_BUCKET,
        source_objects=['sf3/Batch1/DailyMarket.txt'],
        field_delimiter='|',
        destination_project_dataset_table='staging1.daily_market',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_trade_history_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_trade_history_to_staging',
        bucket=GCS_BUCKET,
        source_objects=['sf3/Batch1/TradeHistory.txt'],
        field_delimiter='|',
        destination_project_dataset_table='staging1.trade_history',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_watch_history_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_watch_history_to_staging',
        bucket=GCS_BUCKET,
        source_objects=['sf3/Batch1/WatchHistory.txt'],
        field_delimiter='|',
        destination_project_dataset_table='staging1.watch_history',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )
    load_prospect_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_prospect_to_staging',
        bucket=GCS_BUCKET,
        source_objects=['sf3/Batch1/Prospect.csv'],
        field_delimiter='|',
        destination_project_dataset_table='staging1.prospect',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_finwire_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_finwire_to_staging',
        bucket=GCS_BUCKET,
        source_objects=['sf3/Batch1/FINWIRE*'],
        field_delimiter='|',
        destination_project_dataset_table='staging1.finwire',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_date_to_master = GoogleCloudStorageToBigQueryOperator(
        task_id='load_date_to_master',
        bucket=GCS_BUCKET,
        source_objects=['sf3/Batch1/Date.txt'],
        field_delimiter='|',
        destination_project_dataset_table='master1.DimDate',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_time_to_master = GoogleCloudStorageToBigQueryOperator(
        task_id='load_time_to_master',
        bucket=GCS_BUCKET,
        source_objects=['sf3/Batch1/Time.txt'],
        field_delimiter='|',
        destination_project_dataset_table='master1.DimTime',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_trade_type_to_master = GoogleCloudStorageToBigQueryOperator(
        task_id='load_trade_type_to_master',
        bucket=GCS_BUCKET,
        source_objects=['sf3/Batch1/TradeType.txt'],
        field_delimiter='|',
        destination_project_dataset_table='master1.TradeType',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_industry_to_master = GoogleCloudStorageToBigQueryOperator(
        task_id='load_industry_to_master',
        bucket=GCS_BUCKET,
        source_objects=['sf3/Batch1/Industry.txt'],
        field_delimiter='|',
        destination_project_dataset_table='master1.Industry',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_status_type_to_master = GoogleCloudStorageToBigQueryOperator(
        task_id='load_status_type_to_master',
        bucket=GCS_BUCKET,
        source_objects=['sf3/Batch1/StatusType.txt'],
        field_delimiter='|',
        destination_project_dataset_table='master1.StatusType',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_tax_rate_to_master = GoogleCloudStorageToBigQueryOperator(
        task_id='load_tax_rate_to_master',
        bucket=GCS_BUCKET,
        source_objects=['sf3/Batch1/TaxRate.txt'],
        field_delimiter='|',
        destination_project_dataset_table='master1.TaxRate',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

## load source files into staging
create_staging_tables >> load_hr_to_staging >> load_batch_date_to_staging >> load_trade_to_staging >> load_cash_transaction_to_staging >> load_holding_history_to_staging >> load_daily_market_to_staging >> load_trade_history_to_staging >> load_watch_history_to_staging >> load_prospect_to_staging >> load_finwire_to_staging
## load references into master 'as-is'
create_master_tables >> load_date_to_master >> load_time_to_master >> load_trade_type_to_master >> load_industry_to_master >> load_status_type_to_master >> load_tax_rate_to_master



    
