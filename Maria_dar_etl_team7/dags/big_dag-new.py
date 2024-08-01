import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import datetime
import psycopg
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scripts import source_to_ods
from scripts import lgc_dds_with_cleaning
from scripts import DM_tables

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 30)
}

dag = DAG(
    'source_ods_via_lgs_dds_dm',
    default_args=default_args,
    description='Transfer data from source to ods',
    schedule_interval=None,  
    catchup=False,           
)

start_step = EmptyOperator(
    task_id='start_step',
    dag=dag
)


copy_step = PythonOperator(
        task_id='copy_step',
        python_callable=source_to_ods.move_data,
        dag=dag,
    )

lgc_to_dds_step = PythonOperator(
        task_id='lgc_to_dds_step',
        python_callable=lgc_dds_with_cleaning.clean_data,
        dag=dag,
    )

dm_step = PythonOperator(
        task_id='dm_step',
        python_callable=DM_tables.dm_data,
        dag=dag,
    )

end_step = EmptyOperator(
    task_id='end_step',
    dag=dag
)

start_step >> copy_step >> lgc_to_dds_step >> dm_step >> end_step