## Reference to idea: https://github.com/snithish/tpc-di_benchmark
## utils.py script was adjusted for our usecase. 

from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from constants import *

def execute_sql(task_id: str, sql_file_path: str) -> BigQueryOperator:
    return BigQueryOperator(
        task_id=task_id,
        sql=sql_file_path,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        write_disposition='WRITE_APPEND',
        use_legacy_sql=False,
        location='US'
    )

def insert_overwrite(task_id: str, sql_file_path: str, destination_table: str) -> BigQueryOperator:
    return BigQueryOperator(
        task_id=task_id,
        sql=sql_file_path,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table=destination_table,
        use_legacy_sql=False,
        location='US'
    )

