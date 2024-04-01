ip = '192.168.178.25'
port = 5433
user = 'postgres'
pw = 'tryit1ce'
db = 'mqttdump'
table = 'dump'

import psycopg2
from sqlalchemy import create_engine ,text


engine = create_engine(f'postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}') 

connection = engine.connect() 

result = connection.execute(text("SELECT * FROM tbl_dump"))
print(result.all())

connection.close()