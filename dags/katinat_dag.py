from datetime import datetime, timedelta
from config import CET
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from katinat_ETL import *

default_args = {
    'owner': 'trist',
    'start_date': CET,
    'email': ['tristanhuyanhngo@gmail.com'],
    'email_on_failure': ['tristanhuyanhngo@gmail.com'],
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False
}

with DAG(
    dag_id='katinat_airflow_dag',
    default_args=default_args,
    # schedule_interval='0 22 * * *',
    schedule_interval=None
) as dag:
    # Task start & end
    task_start = DummyOperator(
        task_id="start",
        dag=dag,
    )
    
    task_end = DummyOperator(
        task_id="end",
        dag=dag,
    )
    
    # Get data from sources
    task_source_1 = PythonOperator(
        task_id="get_data_source_1",
        python_callable=get_data_from_source_1,
        do_xcom_push=True
    )
    
    task_source_2 = PythonOperator(
        task_id="get_data_source_2",
        python_callable=get_data_from_source_2,
        do_xcom_push=True
    )
    
    task_source_3 = PythonOperator(
        task_id="get_data_source_3",
        python_callable=get_data_from_source_3,
        do_xcom_push=True
    )
    
    task_source_4 = PythonOperator(
        task_id="get_data_source_4",
        python_callable=get_data_from_source_4,
        do_xcom_push=True
    )
    
    # Processing data from sources
    task_process_data_source_1 = PythonOperator(
	    task_id='process_data_source_1',
	    python_callable=process_data_from_source_1
    )
    
    task_process_data_source_2 = PythonOperator(
	    task_id='process_data_source_2',
	    python_callable=process_data_from_source_2
    )
    
    task_process_data_source_3 = PythonOperator(
	    task_id='process_data_source_3',
	    python_callable=process_data_from_source_3
    )
    
    task_process_data_source_4 = PythonOperator(
	    task_id='process_data_source_4',
	    python_callable=process_data_from_source_4
    )
    
    # Truncate Stage    
    task_truncate_stage = PostgresOperator(
        task_id='truncate_stage_table',
        postgres_conn_id='postgres_stage',
        sql="""
            truncate table katinat_customer_rainbow_drink
        """
    )
    
    # Save to Postgres
    sql_query_1 = Variable.get("sql_stage_01")
    sql_query_2 = Variable.get("sql_stage_02")
    sql_query_3 = Variable.get("sql_stage_03")
    sql_query_4 = Variable.get("sql_stage_04")
    
    task_save_to_stage_1 = PostgresOperator(
        task_id='load_to_stage_table_1',
        postgres_conn_id='postgres_stage',
        sql=f"""
            {sql_query_1}
        """
    )
    
    task_save_to_stage_2 = PostgresOperator(
        task_id='load_to_stage_table_2',
        postgres_conn_id='postgres_stage',
        sql=f"""
            {sql_query_2}
        """
    )
    
    task_save_to_stage_3 = PostgresOperator(
        task_id='load_to_stage_table_3',
        postgres_conn_id='postgres_stage',
        sql=f"""
            {sql_query_3}
        """
    )
    
    task_save_to_stage_4 = PostgresOperator(
        task_id='load_to_stage_table_4',
        postgres_conn_id='postgres_stage',
        sql=f"""
            {sql_query_4}
        """
    )
    
    
# ------------------------------------------------------------------------
task_start >> [task_source_1, task_source_2, task_source_3, task_source_4] 

task_source_1 >> task_process_data_source_1
task_source_2 >> task_process_data_source_2
task_source_3 >> task_process_data_source_3
task_source_4 >> task_process_data_source_4

[task_process_data_source_1, task_process_data_source_2, task_process_data_source_3, task_process_data_source_4] >> task_truncate_stage 

task_truncate_stage >> task_save_to_stage_1 >> task_save_to_stage_2 >> task_save_to_stage_3 >> task_save_to_stage_4 >> task_end