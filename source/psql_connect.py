#IMportszzz
from paho.mqtt import client as mqtt_client
import time
from datetime import timedelta,datetime

import yaml
import psycopg2
from sqlalchemy import create_engine, MetaData,VARCHAR,DATETIME
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import inspect,text,Table
import pandas as pd
import numpy 

def load_config_file(file_name:str):
    """
    """
    file = open(file=file_name,mode='r')
    conf = yaml.safe_load(file)
    return conf

def create_engine_from_conf(conf:dict):
    """
    """
    engine = create_engine('{db_language}+{db_adapter}://{user}:{pw}@{ip}:{port}/{db_name}'.
                            format(db_language=conf['db_server']['database']['db_language'],
                                    db_adapter=conf['db_server']['database']['db_adapter'],
                                    user=conf['db_server']['connection']['user'],
                                    pw=conf['db_server']['connection']['pw'],
                                    ip=conf['db_server']['ip'],
                                    port=conf['db_server']['port'], 
                                    db_name=conf['db_server']['database']['db_name']
                                    )
                             ) 
    return engine
def connect_engine(engine):
    return engine.connect()

def create_broker_from_conf(conf:dict):

    print("___LOAD_CONFIG___")
broker = '192.168.178.25'
port = 1883
topics =['shellies/shellies/shellyplug001/energy/relay/0',
         'shellies/shellies/shellyplug001/energy/relay/0/power',
         'shellies/shellies/shellyplug001/energy/relay/0/energy']
user_data = dict(username="mqtt-user",password="tryit1ce")

# https://www.emqx.com/en/blog/how-to-use-mqtt-in-python 
# python 3.12

print("___IMPORT___")



print("___LOAD_CONFIG___")
broker = '192.168.178.25'
port = 1883
topics =['shellies/shellies/shellyplug001/energy/relay/0',
         'shellies/shellies/shellyplug001/energy/relay/0/power',
         'shellies/shellies/shellyplug001/energy/relay/0/energy']
user_data = dict(username="mqtt-user",password="tryit1ce")


print("___REFERENCE_TIME___")
ref_time = dict(t_mono=time.monotonic(), t_dt=datetime.datetime.now())
print(f"Reference Time [Monotonic]: {ref_time['t_mono']} Reference Time [Datetime]: {ref_time['t_dt']}")

print ("___CUSTOM_FUNCTIONS___")

def t_mono2t_datetime(ref_time:dict, msg_timestamp:int) -> dict:
    dt = timedelta(seconds=msg_timestamp-ref_time['t_mono'])
    msg_dt = ref_time['t_dt']+dt 
    return msg_dt, msg_timestamp

def connect_mqtt(client:mqtt_client,broker, port) -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    _client = client
    # client.username_pw_set(username, password)
    _client.on_connect = on_connect
    _client.connect(host=broker, port=port)
    return _client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        _vals = *(t_mono2t_datetime(ref_time=ref_time,msg_timestamp=msg.timestamp)), msg.payload.decode(), msg.topic
        print(f"Time [Date]: {_vals[0]} | Time Monotonic]: {_vals[1]} | Message: {_vals[2]} | Topic: {_vals[3]}")

    for topic in topics:
        client.subscribe(topic)
    client.on_message = on_message

def run():
    try:
        client = mqtt_client.Client(clean_session=True)
        client.username_pw_set(username=user_data['username'],password=user_data['password'])
    except ConnectionError as ex:
        print(ex)
        
    client.connect(host=broker,port=port)
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()
    #terminal PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres
    #postgres=#\c pg_database
    #pg_database=# CREATE TABLE pg_table(
    #msg_id BIGSERIAL PRIMARY KEY,
    #msg_date TIMESTAMP WITH TIME ZONE,
    #msg_topic varchar(100),
    #msg_value varchar(10));


    file_name=r'config/config.yaml'
    conf = load_config_file(file_name=file_name)

    for num,topic in enumerate(conf['mqtt_server']['topics']):
        print(num,topic)
    
    engine = create_engine_from_conf(conf=conf)
    connection = connect_engine(engine=engine)
    metadata = MetaData()
    metadata.reflect(bind=engine)

    pg_table = Table('pg_table', metadata, autoload = True)

    for topic in conf['mqtt_server']['topics']:

        query = insert(pg_table).values(msg_date=datetime.now(), msg_topic=topic, msg_value=numpy.random.randint(0,50))
        print(query)
        connection.execute(query)
        connection.commit()
        del query
    #f"INSERT INTO pg_table VALUES ({datetime.datetime.now()},{topic},{numpy.random.randint(0,50)})"))
    # result = connection.execute(sa.text(f"SELECT * FROM {conf['db_server']['database']['db_table']}"))
    connection.close()
    connection2=connect_engine(engine=engine)

    result = connection2.execute(text(f"SELECT * FROM pg_table"))
    
    connection2.close()
    df = pd.DataFrame(result.all())
    print(df.to_string())
    