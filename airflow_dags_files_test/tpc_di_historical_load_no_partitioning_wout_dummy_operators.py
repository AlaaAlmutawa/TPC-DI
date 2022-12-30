from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from constants import CSV_EXTENSION, GCS_BUCKET, BIG_QUERY_CONN_ID, GOOGLE_CLOUD_DEFAULT, SF
from utils import construct_gcs_to_bq_operator, get_file_path, insert_overwrite, reset_table, insert_if_empty, \
    execute_sql
from customer_mgmt_helpers import get_customer_mgmt_xml, push_customer_mgmt_json, convert_xml_to_json


AIRFLOW = 'airflow'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG('tpc_di_historical_load_v2.3', schedule_interval=None, default_args=default_args) as dag:

    create_staging_tables = execute_sql(task_id='create_staging_tables',
                                                sql_file_path='queries_test/data_sources_schema_dag.sql')
    create_master_tables = execute_sql(task_id='create_master_tables',
                                                sql_file_path='queries_test/master_schema_dag.sql')
    load_hr_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_hr_to_staging',
        bucket=GCS_BUCKET,
        source_objects=[f'{SF}/Batch1/HR.csv'],
        field_delimiter=',',
        #skip_leading_rows=1,
        destination_project_dataset_table='staging1.hr',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[{"name": "EmployeeID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "ManagerID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "EmployeeFirstName", "type": "STRING", "mode": "REQUIRED"},
        {"name": "EmployeeLastName", "type": "STRING", "mode": "REQUIRED"},
        {"name": "EmployeeMI", "type": "STRING", "mode": "NULLABLE"},
        {"name": "EmployeeJobCode", "type": "INT64", "mode": "NULLABLE"},
        {"name": "EmployeeBranch", "type": "STRING", "mode": "NULLABLE"},
        {"name": "EmployeeOffice", "type": "STRING", "mode": "NULLABLE"},
        {"name": "EmployeePhone", "type": "STRING", "mode": "NULLABLE"}],
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_batch_date_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_batch_date_to_staging',
        bucket=GCS_BUCKET,
        source_objects=[f'{SF}/Batch1/BatchDate.txt'],
        field_delimiter='|',
        destination_project_dataset_table='staging1.batch_date',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[{"name": "BatchDate", "type": "DATE", "mode": "REQUIRED"}],
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_trade_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_trade_to_staging',
        bucket=GCS_BUCKET,
        source_objects=[f'{SF}/Batch1/Trade.txt'],
        field_delimiter='|',
        destination_project_dataset_table='staging1.trade',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[{"name": "T_ID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "T_DTS", "type": "DATETIME", "mode": "REQUIRED"},
        {"name": "T_ST_ID", "type": "STRING", "mode": "REQUIRED"},
        {"name": "T_TT_ID", "type": "STRING", "mode": "REQUIRED"},
        {"name": "T_IS_CASH", "type": "BOOLEAN", "mode": "REQUIRED"},
        {"name": "T_S_SYMB", "type": "STRING", "mode": "REQUIRED"},
        {"name": "T_QTY", "type": "INT64", "mode": "REQUIRED"},
        {"name": "T_BID_PRICE", "type": "NUMERIC", "mode": "REQUIRED"},
        {"name": "T_CA_ID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "T_EXEC_NAME", "type": "STRING", "mode": "REQUIRED"},
        {"name": "T_TRADE_PRICE", "type": "NUMERIC", "mode": "NULLABLE"},
        {"name": "T_CHRG", "type": "NUMERIC", "mode": "NULLABLE"},
        {"name": "T_COMM", "type": "NUMERIC", "mode": "NULLABLE"},
        {"name": "T_TAX", "type": "NUMERIC", "mode": "NULLABLE"}
        ],
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_cash_transaction_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_cash_transaction_to_staging',
        bucket=GCS_BUCKET,
        source_objects=[f'{SF}/Batch1/CashTransaction.txt'],
        field_delimiter='|',
        destination_project_dataset_table='staging1.cash_transaction',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[{"name": "CT_CA_ID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "CT_DTS", "type": "DATETIME", "mode": "REQUIRED"},
        {"name": "CT_AMT", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "CT_NAME", "type": "STRING", "mode": "REQUIRED"}
        ],
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_holding_history_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_holding_history_to_staging',
        bucket=GCS_BUCKET,
        source_objects=[f'{SF}/Batch1/HoldingHistory.txt'],
        field_delimiter='|',
        destination_project_dataset_table='staging1.holding_history',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[{"name": "HH_H_T_ID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "HH_T_ID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "HH_BEFORE_QTY", "type": "INT64", "mode": "REQUIRED"},
        {"name": "HH_AFTER_QTY", "type": "INT64", "mode": "REQUIRED"}
        ],
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_daily_market_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_daily_market_to_staging',
        bucket=GCS_BUCKET,
        source_objects=[f'{SF}/Batch1/DailyMarket.txt'],
        field_delimiter='|',
        destination_project_dataset_table='staging1.daily_market',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[{"name": "DM_DATE", "type": "DATE", "mode": "REQUIRED"},
        {"name": "DM_S_SYMB", "type": "STRING", "mode": "REQUIRED"},
        {"name": "DM_CLOSE", "type": "NUMERIC", "mode": "REQUIRED"},
        {"name": "DM_HIGH", "type": "NUMERIC", "mode": "REQUIRED"},
        {"name": "DM_LOW", "type": "NUMERIC", "mode": "REQUIRED"},
        {"name": "DM_VOL", "type": "INT64", "mode": "REQUIRED"}
        ],
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_trade_history_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_trade_history_to_staging',
        bucket=GCS_BUCKET,
        source_objects=[f'{SF}/Batch1/TradeHistory.txt'],
        field_delimiter='|',
        destination_project_dataset_table='staging1.trade_history',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[{"name": "TH_T_ID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "TRADE_T", "type": "DATETIME", "mode": "REQUIRED"},
        {"name": "TH_ST_ID", "type": "STRING", "mode": "REQUIRED"}
        ],
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_watch_history_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_watch_history_to_staging',
        bucket=GCS_BUCKET,
        source_objects=[f'{SF}/Batch1/WatchHistory.txt'],
        field_delimiter='|',
        destination_project_dataset_table='staging1.watch_history',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[{"name": "W_C_ID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "W_S_SYMB", "type": "STRING", "mode": "REQUIRED"},
        {"name": "W_DTS", "type": "DATETIME", "mode": "REQUIRED"},
        {"name": "W_ACTION", "type": "STRING", "mode": "REQUIRED"}
        ],
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )
    load_prospect_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_prospect_to_staging',
        bucket=GCS_BUCKET,
        source_objects=[f'{SF}/Batch1/Prospect.csv'],
        field_delimiter=',',
        destination_project_dataset_table='staging1.prospect',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[{"name": "AgencyID", "type": "STRING", "mode": "REQUIRED"},
        {"name": "LastName", "type": "STRING", "mode": "REQUIRED"},
        {"name": "FirstName", "type": "STRING", "mode": "REQUIRED"},
        {"name": "MiddleInitial", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Gender", "type": "STRING", "mode": "NULLABLE"},
        {"name": "AddressLine1", "type": "STRING", "mode": "NULLABLE"},
        {"name": "AddressLine2", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PostalCode", "type": "STRING", "mode": "NULLABLE"},
        {"name": "City", "type": "STRING", "mode": "REQUIRED"},
        {"name": "State", "type": "STRING", "mode": "REQUIRED"},
        {"name": "Country", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Phone", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Income", "type": "NUMERIC", "mode": "NULLABLE"},
        {"name": "NumberCars", "type": "INT64", "mode": "NULLABLE"},
        {"name": "NumberChildern", "type": "INT64", "mode": "NULLABLE"},
        {"name": "MaritalStatus", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Age", "type": "INT64", "mode": "NULLABLE"},
        {"name": "CreditRating", "type": "NUMERIC", "mode": "NULLABLE"},
        {"name": "OwnOrRentFlag", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Employer", "type": "STRING", "mode": "NULLABLE"},
        {"name": "NumberCreditCards", "type": "INT64", "mode": "NULLABLE"},
        {"name": "NetWorth", "type": "INT64", "mode": "NULLABLE"}
        ],
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_finwire_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_finwire_to_staging',
        bucket=GCS_BUCKET,
        source_objects=[f'{SF}/Batch1/FINWIRE*'],
        field_delimiter='|',
        destination_project_dataset_table='staging1.finwire',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[{"name": "RECORD", "type": "STRING", "mode": "NULLABLE"}],
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    ## below you can find the task to do the json conversion 
    load_customer_management_json_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_customer_management_json_to_staging',
        bucket=GCS_BUCKET,
        source_objects=[f'{SF}/Batch1/CustomerMgmt.json'],
        source_format='NEWLINE_DELIMITED_JSON',
        destination_project_dataset_table='staging1.customer_management',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_date_to_master = GoogleCloudStorageToBigQueryOperator(
        task_id='load_date_to_master',
        bucket=GCS_BUCKET,
        source_objects=[f'{SF}/Batch1/Date.txt'],
        field_delimiter='|',
        destination_project_dataset_table='master1.DimDate',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[{"name": "SK_DateID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "DateValue", "type": "DATE", "mode": "REQUIRED"},
        {"name": "DateDesc", "type": "STRING", "mode": "REQUIRED"},
        {"name": "CalendarYearID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "CalendarYearDesc", "type": "STRING", "mode": "REQUIRED"},
        {"name": "CalendarQtrID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "CalendarQtrDesc", "type": "STRING", "mode": "REQUIRED"},
        {"name": "CalendarMonthID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "CalendarMonthDesc", "type": "STRING", "mode": "REQUIRED"},
        {"name": "CalendarWeekID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "CalendarWeekDesc", "type": "STRING", "mode": "REQUIRED"},
        {"name": "DayOfWeekNum", "type": "INT64", "mode": "REQUIRED"},
        {"name": "DayOfWeekDesc", "type": "STRING", "mode": "REQUIRED"},
        {"name": "FiscalYearID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "FiscalYearDesc", "type": "STRING", "mode": "REQUIRED"},
        {"name": "FiscalQtrID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "FiscalQtrDesc", "type": "STRING", "mode": "REQUIRED"},
        {"name": "HolidayFlag", "type": "BOOLEAN", "mode": "NULLABLE"},
        ],
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_time_to_master = GoogleCloudStorageToBigQueryOperator(
        task_id='load_time_to_master',
        bucket=GCS_BUCKET,
        source_objects=[f'{SF}/Batch1/Time.txt'],
        field_delimiter='|',
        destination_project_dataset_table='master1.DimTime',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[{"name": "SK_TimeID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "TimeValue", "type": "TIME", "mode": "REQUIRED"},
        {"name": "HourID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "HourDesc", "type": "STRING", "mode": "REQUIRED"},
        {"name": "MinuteID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "MinuteDesc", "type": "STRING", "mode": "REQUIRED"},
        {"name": "SecondID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "SecondDesc", "type": "STRING", "mode": "REQUIRED"},
        {"name": "MarketHoursFlag", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "OfficeHoursFlag", "type": "BOOLEAN", "mode": "NULLABLE"}
        ],
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_trade_type_to_master = GoogleCloudStorageToBigQueryOperator(
        task_id='load_trade_type_to_master',
        bucket=GCS_BUCKET,
        source_objects=[f'{SF}/Batch1/TradeType.txt'],
        field_delimiter='|',
        destination_project_dataset_table='master1.TradeType',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[{"name": "TT_ID", "type": "STRING", "mode": "REQUIRED"},
        {"name": "TT_NAME", "type": "STRING", "mode": "REQUIRED"},
        {"name": "TT_IS_SELL", "type": "INT64", "mode": "REQUIRED"},
        {"name": "TT_IS_MRKT", "type": "INT64", "mode": "REQUIRED"}
        ],
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_industry_to_master = GoogleCloudStorageToBigQueryOperator(
        task_id='load_industry_to_master',
        bucket=GCS_BUCKET,
        source_objects=[f'{SF}/Batch1/Industry.txt'],
        field_delimiter='|',
        destination_project_dataset_table='master1.Industry',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[{"name": "IN_ID", "type": "STRING", "mode": "REQUIRED"},
        {"name": "IN_NAME", "type": "STRING", "mode": "REQUIRED"},
        {"name": "IN_SC_ID", "type": "STRING", "mode": "REQUIRED"}
        ],
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_status_type_to_master = GoogleCloudStorageToBigQueryOperator(
        task_id='load_status_type_to_master',
        bucket=GCS_BUCKET,
        source_objects=[f'{SF}/Batch1/StatusType.txt'],
        field_delimiter='|',
        destination_project_dataset_table='master1.StatusType',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[{"name": "ST_ID", "type": "STRING", "mode": "REQUIRED"},
        {"name": "ST_NAME", "type": "STRING", "mode": "REQUIRED"}
        ],
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_tax_rate_to_master = GoogleCloudStorageToBigQueryOperator(
        task_id='load_tax_rate_to_master',
        bucket=GCS_BUCKET,
        source_objects=[f'{SF}/Batch1/TaxRate.txt'],
        field_delimiter='|',
        destination_project_dataset_table='master1.TaxRate',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[{"name": "TX_ID", "type": "STRING", "mode": "REQUIRED"},
        {"name": "TX_NAME", "type": "STRING", "mode": "REQUIRED"},
        {"name": "TX_RATE", "type": "NUMERIC", "mode": "REQUIRED"}
        ],
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )
    transform_hr_to_dimbroker = insert_overwrite(task_id='transform_hr_to_dimbroker',
                                                sql_file_path='queries_test/master_populate_dimbroker_dag.sql',
                                                destination_table='master1.DimBroker')
    transform_finwire_to_cmp = insert_overwrite(task_id='transform_finwire_to_cmp',
                                                sql_file_path='queries_test/staging_finwire_cmp_record_table_dag.sql',
                                                destination_table='staging1.finwire_cmp_record')
    transform_finwire_to_fin = insert_overwrite(task_id='transform_finwire_to_fin',
                                                sql_file_path='queries_test/staging_finwire_fin_table_dag.sql',
                                                destination_table='staging1.finwire_fin_record')
    transform_finwire_to_sec = insert_overwrite(task_id='transform_finwire_to_sec',
                                                sql_file_path='queries_test/staging_finwire_sec_table_dag.sql',
                                                destination_table='staging1.finwire_sec_record')
    financial_to_master = insert_overwrite(task_id='financial_to_master',
                                                sql_file_path='queries_test/master_populate_financial_dag.sql',
                                                destination_table='master1.Financial')
    transform_DimCompany_to_master = insert_overwrite(task_id='transform_DimCompany_to_master',
                                                sql_file_path='queries_test/master_populate_dimcompany_dag.sql',
                                                destination_table='master1.DimCompany')
    transform_dimsecurity_to_master = insert_overwrite(task_id='transform_dimsecurity_to_master',
                                                sql_file_path='queries_test/master_populate_dimsecurity_dag.sql',
                                                destination_table='master1.DimSecurity')
    load_customer_historical_from_customer_management_json_to_staging = insert_overwrite(task_id='get_customer_historical_from_customer_management_json_to_staging',
                                                sql_file_path='queries_test/staging_customer_historical_table_dag.sql',
                                                destination_table='staging1.customer_historical')
    transform_dimcustomer_to_master = insert_overwrite(task_id='transform_dimcustomer_to_master',
                                                sql_file_path='queries_test/master_populate_dimcustomer_dag.sql',
                                                destination_table='master1.DimCustomer')
    transform_dimaccount_to_master = insert_overwrite(task_id='transform_dimaccount_to_master',
                                                sql_file_path='queries_test/master_populate_dimaccount_dag.sql',
                                                destination_table='master1.DimAccount')
    transform_dimtrade_to_master = insert_overwrite(task_id='transform_dimtrade_to_master',
                                                sql_file_path='queries_test/master_populate_dimtrade_dag.sql',
                                                destination_table='master1.DimTrade')
    transform_propspect_to_master = insert_overwrite(task_id='transform_propspect_to_master',
                                                sql_file_path='queries_test/master_populate_prospect_dag.sql',
                                                destination_table='master1.Prospect')
    transform_fastcasbalances_to_master = insert_overwrite(task_id='transform_fastcasbalances_to_master',
                                                sql_file_path='queries_test/master_populate_factcashbalances_dag.sql',
                                                destination_table='master1.FactCashBalances')
    transform_factwatches_to_master = insert_overwrite(task_id='transform_factwatches_to_master',
                                                sql_file_path='queries_test/master_populate_factwatches_dag.sql',
                                                destination_table='master1.FactWatches')
    # prep_partition_daily_market_staging = execute_sql(task_id='prep_partition_daily_market_staging',
    #                                             sql_file_path='queries_test/staging_cluster_daily_marke_dag.sql')
    transform_factmarkethistory_to_master = insert_overwrite(task_id='transform_factmarkethistory_to_master',
                                                sql_file_path='queries_test/master_populate_factmarkethistory_without_partition_dag.sql',
                                                destination_table='master1.FactMarketHistory')
    transform_factholdings_to_master = insert_overwrite(task_id='transform_factholdings_to_master',
                                                sql_file_path='queries_test/master_populate_factsholding_dag.sql',
                                                destination_table='master1.FactHoldings')
    populate_dimessages_in_master = insert_overwrite(task_id='populate_dimessages_in_master',
                                                sql_file_path='queries_test/master_populate_dimessages_dag.sql',
                                                destination_table='master1.DiMessages')
    ## handle customer mgmt xml tasks 
    ## delete any existing xml file 
    clear_directory_xml = BashOperator(task_id='clear_directory_xml',
                                        bash_command='rm -rf dags/tpc-di_resources/gcs/data/CustomerMgmt.xml')
    ## delete any existing json file 
    clear_directory_json = BashOperator(task_id='clear_directory_json',
                                        bash_command='rm -rf dags/tpc-di_resources/gcs/data/transform/CustomerMgmt.json')

    ## load customer mgmt from gcs 
    get_customer_mgmt_xml = PythonOperator(
        task_id='get_customer_mgmt_xml',
        python_callable=get_customer_mgmt_xml
    )

    ## convert xml to json 
    convert_xml_to_json = PythonOperator(
        task_id='convert_xml_to_json',
        python_callable=convert_xml_to_json
    )

    ## push json converted file 
    push_customer_mgmt_json = PythonOperator(
        task_id='push_customer_mgmt_json',
        python_callable=push_customer_mgmt_json
    )

    # staging_holding_history_table_creation_complete = DummyOperator(task_id='staging_holding_history_table_creation_complete')
    # staging_hr_table_creation_complete = DummyOperator(task_id='staging_hr_table_creation_complete')
    # tax_rate_data_loading_complete = DummyOperator(task_id='tax_rate_data_loading_complete')
    # reference_date_data_loading_complete = DummyOperator(task_id='reference_date_data_loading_complete')
    # reference_industry_status_data_loading_complete = DummyOperator(task_id='reference_industry_status_data_loading_complete')
    # finwire_load_complete = DummyOperator(task_id='finwire_load_complete')
    # finwire_cmp_transform_complete = DummyOperator(task_id='finwire_cmp_transform_complete')
    # finwire_fin_transform_complete = DummyOperator(task_id='finwire_fin_transform_complete')
    # finwire_sec_transform_complete = DummyOperator(task_id='finwire_sec_transform_complete')
    # financial_to_master_complete = DummyOperator(task_id='financial_to_master_complete')
    # dimcompany_to_master_complete = DummyOperator(task_id='dimcompany_to_master_complete')
    # dimsecurity_to_master_complete = DummyOperator(task_id='dimsecurity_to_master_complete')
    # customer_historical_complete = DummyOperator(task_id='customer_historical_complete')
    # staging_prospect_table_creation_complete = DummyOperator(task_id='staging_prospect_table_creation_complete')
    # dimcustomer_to_master_complete = DummyOperator(task_id='dimcustomer_to_master_complete')
    # dimbroker_to_master_complete = DummyOperator(task_id='dimbroker_to_master_complete')
    # dimaccount_to_master_complete = DummyOperator(task_id='dimaccount_to_master_complete')
    # reference_time_trade_type_data_loading_complete = DummyOperator(task_id='reference_time_trade_type_data_loading_complete')
    # dimtrade_to_master_complete = DummyOperator(task_id='dimtrade_to_master_complete')
    # staging_trade_table_creation_complete = DummyOperator(task_id='staging_trade_table_creation_complete')
    # staging_batch_date_table_creation_complete = DummyOperator(task_id='staging_batch_date_table_creation_complete')
    # prospect_to_master_complete = DummyOperator(task_id='prospect_to_master_complete')
    # staging_cash_transaction_table_creation_complete = DummyOperator(task_id='staging_cash_transaction_table_creation_complete')
    # fastcashbalances_to_master_complete = DummyOperator(task_id='fastcashbalances_to_master_complete')
    # staging_watch_history_table_creation_complete = DummyOperator(task_id='staging_watch_history_table_creation_complete')
    # fastwatches_to_master_complete = DummyOperator(task_id='fastwatches_to_master_complete')
    # staging_daily_market_v2_table_creation_complete = DummyOperator(task_id='staging_daily_market_v2_table_creation_complete')
    # factmarkethistory_to_master_complete = DummyOperator(task_id='factmarkethistory_to_master_complete')
    # factholdings_to_master_complete = DummyOperator(task_id='factholdings_to_master_complete')
    # dimessages_to_master_complete = DummyOperator(task_id='dimessages_to_master_complete')

## load source files into staging
create_staging_tables >> load_hr_to_staging #>> staging_hr_table_creation_complete
create_staging_tables >> load_finwire_to_staging #>> finwire_load_complete
create_staging_tables >> load_trade_to_staging >> load_trade_history_to_staging #>> staging_trade_table_creation_complete
create_staging_tables >> load_holding_history_to_staging #>> staging_holding_history_table_creation_complete
create_staging_tables >> load_daily_market_to_staging #>> prep_partition_daily_market_staging #>> staging_daily_market_v2_table_creation_complete
create_staging_tables >> load_watch_history_to_staging #>> staging_watch_history_table_creation_complete
create_staging_tables >> load_batch_date_to_staging #>> staging_batch_date_table_creation_complete
create_staging_tables >> clear_directory_xml >> clear_directory_json >> get_customer_mgmt_xml >> convert_xml_to_json >> push_customer_mgmt_json >> load_customer_management_json_to_staging >> load_customer_historical_from_customer_management_json_to_staging #>> customer_historical_complete
create_staging_tables >> load_prospect_to_staging #>> staging_prospect_table_creation_complete
create_staging_tables >> load_cash_transaction_to_staging #>> staging_cash_transaction_table_creation_complete
## load references into master 'as-is'
create_master_tables >> load_date_to_master #>> reference_date_data_loading_complete
create_master_tables >> load_time_to_master >> load_trade_type_to_master #>> reference_time_trade_type_data_loading_complete
create_master_tables >> load_tax_rate_to_master #>> tax_rate_data_loading_complete
create_master_tables >> load_industry_to_master >> load_status_type_to_master #>> reference_industry_status_data_loading_complete

## populate dimbroker 
[load_hr_to_staging, load_date_to_master] >> transform_hr_to_dimbroker #>> dimbroker_to_master_complete

## create cmp, fin, sec 
load_finwire_to_staging >> transform_finwire_to_cmp #>> finwire_cmp_transform_complete
load_finwire_to_staging >> transform_finwire_to_fin #>> finwire_fin_transform_complete
load_finwire_to_staging >> transform_finwire_to_sec #>> finwire_sec_transform_complete

# build DimCompany 
[load_status_type_to_master, transform_finwire_to_cmp] >> transform_DimCompany_to_master #>> dimcompany_to_master_complete

# build finanical ref table
[transform_DimCompany_to_master, transform_finwire_to_fin] >> financial_to_master #>> financial_to_master_complete

# build dimsecurity 
[transform_finwire_to_sec, transform_DimCompany_to_master] >> transform_dimsecurity_to_master #>> dimsecurity_to_master_complete

# build dimcustomer 
[load_prospect_to_staging, load_customer_historical_from_customer_management_json_to_staging, load_tax_rate_to_master] >> transform_dimcustomer_to_master #>> dimcustomer_to_master_complete

# build dimaccount
[transform_dimcustomer_to_master, transform_hr_to_dimbroker] >> transform_dimaccount_to_master #>> dimaccount_to_master_complete

#build dimtrade 
[load_trade_history_to_staging, load_trade_type_to_master, load_date_to_master, transform_dimaccount_to_master, transform_dimsecurity_to_master] >> transform_dimtrade_to_master #>> dimtrade_to_master_complete

# build prospect 
[load_prospect_to_staging, load_customer_historical_from_customer_management_json_to_staging, load_date_to_master, load_batch_date_to_staging] >>  transform_propspect_to_master #>> prospect_to_master_complete

# build fact tables 

# build FastCashBalances 
[load_cash_transaction_to_staging, transform_dimaccount_to_master] >> transform_fastcasbalances_to_master #>> fastcashbalances_to_master_complete

# build factwatches 
[load_watch_history_to_staging, transform_dimcustomer_to_master, transform_dimsecurity_to_master, load_date_to_master] >> transform_factwatches_to_master #>> fastwatches_to_master_complete

# build FactDailyMarket 
[load_daily_market_to_staging,load_date_to_master,financial_to_master,transform_dimsecurity_to_master] >> transform_factmarkethistory_to_master #>> factmarkethistory_to_master_complete

# build FactHolding 
[load_holding_history_to_staging, transform_dimtrade_to_master] >> transform_factholdings_to_master #>> factholdings_to_master_complete 

# Populating DiMessages 
[transform_factholdings_to_master, transform_factmarkethistory_to_master] >> populate_dimessages_in_master #>> dimessages_to_master_complete
