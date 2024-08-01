import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import tables, dds_table_sql, pre_dm
from etls.copying_tables import copying_tables
from etls.employee_table import transform_employee_table
from sql.g_dm_dep.insertion_g_dm_dep import insertion_g_dm_dep
from sql.g_dm.insertion_g_dm import insertion_g_dm

default_args = {
    'owner': 'gena',
    'start_date': datetime(2024, 7, 1)
}

dag = DAG(
    'etl_DAR_intership',
    default_args=default_args,
    description='Transfer data from source to ods',
    schedule_interval=None,
    catchup=False,
)

start_dag = EmptyOperator(
    task_id='start_dag',
    dag=dag,
)

end_ods = EmptyOperator(
    task_id='end_ods',
    dag=dag,
)

end_dds = EmptyOperator(
    task_id='end_dds',
    dag=dag,
)

end_pre_dm = EmptyOperator(
    task_id='end_pre_dm',
    dag=dag,
)

end_dag = EmptyOperator(
    task_id='end_dag',
    dag=dag,
)


insertion_ods_tasks = []
for table in tables:
    copying_task = PythonOperator(
        task_id=f'copying_{table}',
        python_callable=copying_tables,
        op_kwargs={
            'src_schema': 'source_data',
            'dest_schema': 'ods',
            'table': table,
        },
        dag=dag
    )
    insertion_ods_tasks.append(copying_task)

insertion_dds_tasks = []
for key in dds_table_sql:
    clean_task = PostgresOperator(
        task_id=f'insertion_dds_{key}',
        postgres_conn_id='etl_db_7',
        sql=dds_table_sql[key],
        dag=dag
    )
    insertion_dds_tasks.append(clean_task)

employee_table_task = PythonOperator(
    task_id='employee_table_task',
    python_callable=transform_employee_table,
    dag=dag
)
insertion_dds_tasks.append(employee_table_task)

insertion_pre_dm_tasks = []
for key in pre_dm:
    insertion_pre_dm = PostgresOperator(
        task_id=f'insertion_pre_dm_{key}',
        postgres_conn_id='etl_db_7',
        sql=pre_dm[key],
        dag=dag
    )
    insertion_pre_dm_tasks.append(insertion_pre_dm)

insertion_dm_dep = PostgresOperator(
    task_id='insertion_g_dm_dep',
    postgres_conn_id='etl_db_7',
    sql=insertion_g_dm_dep,
    dag=dag
)

insertion_dm = PostgresOperator(
    task_id='insertion_g_dm',
    postgres_conn_id='etl_db_7',
    sql=insertion_g_dm,
    dag=dag
)

start_dag >> insertion_ods_tasks >> end_ods
end_ods >> insertion_dds_tasks >> end_dds
end_dds >> insertion_pre_dm_tasks >> end_pre_dm
end_pre_dm >> insertion_dm_dep >> insertion_dm >> end_dag