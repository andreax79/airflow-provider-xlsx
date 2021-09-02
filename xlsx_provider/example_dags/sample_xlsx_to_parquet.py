#!/usr/bin/env python

from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator, SFTPOperation
from xlsx_provider.operators.from_xlsx_operator import FromXLSXOperator

# SFTP Connection ID
SFTP_CONNECTION_ID = 'sftp_catalog'
# SFTP source path
SFTP_SOURCE_PATH = '/data/catalog.xlsx'
# Temporary xlsx local filename
TMP_XLSX_CATALOG = '/tmp/catalog/catalog_{{ ds_nodash}}.xlsx'
# Temporary parquet local filename
TMP_PARQUET_CATALOG = '/tmp/catalog/catalog_{{ ds_nodash}}.parquet'
# S3 Target path
S3_TARGET = 's3://example/catalog/{{ ds_nodash }}/catalog.parquet'

default_args = {
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    'dummy',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    concurrency=3,
    max_active_runs=1,
) as dag:
    """
    ### Sample DAG

    - Download an XLSX from an SFTP server
    - Convert to Parquet
    - Copy to S3
    - Add datalake's partition
    - Delete temporary files
    """

    start = DummyOperator(task_id='start')

    # Download an XLSX from an SFTP server
    fetch_catalog_from_sftp = SFTPOperator(
        task_id='fetch_catalog_from_sftp',
        ssh_conn_id=SFTP_CONNECTION_ID,
        local_filepath=TMP_XLSX_CATALOG,
        remote_filepath=SFTP_SOURCE_PATH,
        operation=SFTPOperation.GET,
        create_intermediate_dirs=True,
        dag=dag,
    )

    # Convert XLSX to Parquet
    catalog_xlsx_to_parquet = FromXLSXOperator(
        task_id='catalog_xlsx_to_parquet',
        source=TMP_XLSX_CATALOG,
        target=TMP_PARQUET_CATALOG,
        file_format='parquet',
        dag=dag,
    )

    # Copy to S3
    copy_to_s3 = BashOperator(
        task_id='copy_to_s3',
        bash_command='aws s3 cp {0} {1}'.format(TMP_PARQUET_CATALOG, S3_TARGET),
    )

    # Add datalake table's partition
    add_datalake_partition = BashOperator(
        task_id='add_datalake_partition',
        bash_command='gluettalax addp datalake catalog --partition_0={{ ds_nodash }}',
    )

    # Cleanup
    end = BashOperator(
        task_id='end',
        trigger_rule='none_failed',
        bash_command='rm -f {0} {1}'.format(TMP_XLSX_CATALOG, TMP_PARQUET_CATALOG),
    )

    # Defining the task dependencies
    (
        start
        >> fetch_catalog_from_sftp
        >> catalog_xlsx_to_parquet
        >> copy_to_s3
        >> add_datalake_partition
        >> end
    )
