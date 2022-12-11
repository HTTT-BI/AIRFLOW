import pandas as pd
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

def get_data_from_source_1():
    sql_stmt = 'SELECT * FROM katinat_customer_rainbow_drink'
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_source_system_1',
        database='katinat_01'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    
    return cursor.fetchall()

def get_data_from_source_2():
    sql_stmt = 'SELECT * FROM katinat_customer_rainbow_drink'
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_source_system_2',
        database='katinat_02'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    
    return cursor.fetchall()

def get_data_from_source_3():
    sql_stmt = 'SELECT * FROM katinat_customer_rainbow_drink'
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_source_system_3',
        database='katinat_03'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    
    return cursor.fetchall()

def get_data_from_source_4():
    sql_stmt = 'SELECT * FROM katinat_customer_rainbow_drink'
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_source_system_4',
        database='katinat_04'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    
    return cursor.fetchall()

def get_data_from_source_5():
    sql_stmt = 'SELECT * FROM katinat_customer_rainbow_drink'
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_source_system_5',
        database='katinat_05'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    
    return cursor.fetchall()

def process_data_from_source_1(ti):
    source_data = ti.xcom_pull(task_ids=['get_data_source_1'])
    if not source_data:
        raise Exception('No data.')

    source_data = pd.DataFrame(
        data=source_data[0],
        columns=['id','ID','Name', 'Gender','TimeArrived','TimeAway']
    )
    source_data = source_data.drop('ID', axis=1)
    source_data = source_data.drop('id', axis=1)
    
    sql_insert = ""
    
    for index, row in source_data.iterrows():
        query = f"""INSERT INTO public.katinat_customer_rainbow_drink ("Name", "Gender", "TimeArrived", "TimeAway") VALUES ('{row["Name"]}', '{row["Gender"]}', '{row["TimeArrived"]}', '{row["TimeAway"]}'); """
        sql_insert = sql_insert + query
    
    Variable.set("sql_stage_01", sql_insert)
    
def process_data_from_source_2(ti):
    source_data = ti.xcom_pull(task_ids=['get_data_source_2'])
    if not source_data:
        raise Exception('No data.')

    source_data = pd.DataFrame(
        data=source_data[0],
        columns=['id','ID','Name', 'Gender','TimeArrived','TimeAway']
    )
    source_data = source_data.drop('ID', axis=1)
    source_data = source_data.drop('id', axis=1)
    
    sql_insert = ""
    
    for index, row in source_data.iterrows():
        query = f"""INSERT INTO public.katinat_customer_rainbow_drink ("Name", "Gender", "TimeArrived", "TimeAway") VALUES ('{row["Name"]}', '{row["Gender"]}', '{row["TimeArrived"]}', '{row["TimeAway"]}'); """
        sql_insert = sql_insert + query
    
    Variable.set("sql_stage_02", sql_insert)
    
def process_data_from_source_3(ti):
    source_data = ti.xcom_pull(task_ids=['get_data_source_3'])
    if not source_data:
        raise Exception('No data.')

    source_data = pd.DataFrame(
        data=source_data[0],
        columns=['id','ID','Name', 'Gender','TimeArrived','TimeAway']
    )
    source_data = source_data.drop('ID', axis=1)
    source_data = source_data.drop('id', axis=1)
    
    sql_insert = ""
    
    for index, row in source_data.iterrows():
        query = f"""INSERT INTO public.katinat_customer_rainbow_drink ("Name", "Gender", "TimeArrived", "TimeAway") VALUES ('{row["Name"]}', '{row["Gender"]}', '{row["TimeArrived"]}', '{row["TimeAway"]}'); """
        sql_insert = sql_insert + query
    
    Variable.set("sql_stage_03", sql_insert)
    
def process_data_from_source_4(ti):
    source_data = ti.xcom_pull(task_ids=['get_data_source_4'])
    if not source_data:
        raise Exception('No data.')

    source_data = pd.DataFrame(
        data=source_data[0],
        columns=['id','ID','Name', 'Gender','TimeArrived','TimeAway']
    )
    source_data = source_data.drop('ID', axis=1)
    source_data = source_data.drop('id', axis=1)
    
    sql_insert = ""
    
    for index, row in source_data.iterrows():
        query = f"""INSERT INTO public.katinat_customer_rainbow_drink ("Name", "Gender", "TimeArrived", "TimeAway") VALUES ('{row["Name"]}', '{row["Gender"]}', '{row["TimeArrived"]}', '{row["TimeAway"]}'); """
        sql_insert = sql_insert + query
    
    Variable.set("sql_stage_04", sql_insert)