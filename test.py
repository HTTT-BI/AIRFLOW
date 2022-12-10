import psycopg2
import pandas as pd
from sqlalchemy import create_engine
  
# establish connections
conn_string = 'postgresql://airflow:airflow@127.0.0.1/katinat_01'
  
db = create_engine(conn_string)
conn = db.connect()
conn1 = psycopg2.connect(
  database="katinat_04",
  user='airflow', 
  password='airflow', 
  host='127.0.0.1', 
  port= '5432'
)

conn1.autocommit = True
cursor = conn1.cursor()

csv_path = './source_system/' + "20221205_katinat_01.csv"
df = pd.read_csv(csv_path)

df.to_csv("./stage/test.csv", index=False)