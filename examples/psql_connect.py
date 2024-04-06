ip = 'localhost'
port = 5432
user = 'postgres'
pw = 'postgres'
db = 'pg_database'
table = 'pg_table'

import psycopg2
from sqlalchemy import create_engine ,text
import pandas as pd
#___pg_server___
#bash
#PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres 
#CREATE DATABASE pg_database;
#CREATE TABLE pg_table (
#   msg_id      BIGSERIAL     PRIMARY KEY,
#   msg_date    TIMESTAMP WITH TIME ZONE,
#   msg_topic   varchar(100),
#   msg_value   varchar(10)
#   );


engine = create_engine(f'postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}') 

connection = engine.connect() 

result = connection.execute(text(f"SELECT * FROM {table}"))
df = pd.DataFrame(result.all())
print(df.to_string())
#for res in result.all():
#    print(f"PrimaryKey: {res[0]} | Time: {res[1]} | Topic: {res[2]} Value: {res[3]}")


connection.close()

