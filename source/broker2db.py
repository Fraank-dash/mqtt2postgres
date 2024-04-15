#IMPORTS
##GENERAL
import yaml
##BROKER
from paho.mqtt import client as mqtt_client
import time
from datetime import timedelta,datetime
##POSTGRES
import psycopg2
from sqlalchemy import create_engine, MetaData,VARCHAR,DATETIME
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import inspect,text,Table
import pandas as pd
import numpy 


class broker:
    def __init__(self, config_file_name:str,msg_config:str='print'):
        self.conf = self.load_config_file(config_file_name)
        #db
        self._engine = self.create_engine_from_conf(conf=self.conf)
        self.db_connection = self._engine.connect()
        self.metadata = MetaData()
        self.metadata.reflect(bind=self._engine)
        self.pg_table = Table('pg_table', self.metadata, autoload = True)
        #broker
        self.ref_time = self.get_reference_time()
        self.client = mqtt_client.Client(clean_session=True)
        try:
            self.client.username_pw_set(username=self.conf['mqtt_server']['connection']['user'],
                                        password=self.conf['mqtt_server']['connection']['pw'],)
        except ConnectionError as ex:
            print(ex)
        self.client.connect(host=self.conf['mqtt_server']['ip'],
                            port=self.conf['mqtt_server']['port'])
        for topic in self.conf['mqtt_server']['topics']:
            self.client.subscribe(topic)

        self.client.on_connect = self.on_connect()

        self.client.on_message = self.on_message(msg_config)

    def load_config_file(self,file_name:str):
        """
        """
        file = open(file=file_name,mode='r')
        conf = yaml.safe_load(file)
        return conf
    
    def on_connect(self):
        def on_connect(client,userdata,flags,rc):
            if rc == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", rc)
        return on_connect
    def on_message(self,msg_config:str):
        if msg_config=='print':
            def on_message(client,userdata,msg):
                _vals = *(self.t_mono2t_datetime(ref_time=self.ref_time,msg_timestamp=msg.timestamp)), msg.payload.decode(), msg.topic
                print(f"Time [Date]: {_vals[0]} | Time Monotonic]: {_vals[1]} | Message: {_vals[2]} | Topic: {_vals[3]}")
        elif msg_config=='db':
            def on_message(client,userdata,msg:mqtt_client.MQTTMessage):
                query = insert(self.pg_table).values(msg_date=self.t_mono2t_datetime(ref_time=self.ref_time,msg_timestamp=msg.timestamp)[0], 
                                                     msg_topic=msg.topic, 
                                                     msg_value=msg.payload.decode()[0:10])
                print(query)
                self.db_connection.execute(query)
                self.db_connection.commit()
                print(f"MsgFromBroker2DB sent at {datetime.now()}")
                del query
        else:
            def on_message(client,userdata,msg):
                _vals = *(self.t_mono2t_datetime(ref_time=self.ref_time,msg_timestamp=msg.timestamp)), msg.payload.decode(), msg.topic
                print(f"Time [Date]: {_vals[0]} | Time Monotonic]: {_vals[1]} | Message: {_vals[2]} | Topic: {_vals[3]}")
        return on_message
    
    def get_reference_time(self):
        """"""
        return dict(t_mono=time.monotonic(), t_dt=datetime.now())

    def t_mono2t_datetime(self,ref_time:dict, msg_timestamp:int) -> dict:
        dt = timedelta(seconds=msg_timestamp-ref_time['t_mono'])
        msg_dt = ref_time['t_dt']+dt 
        return msg_dt, msg_timestamp
    ##POSTGRES-DATABASE
    def create_engine_from_conf(self,conf:dict):
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

    #run
    def run(self):
        self.client.loop_forever()


if __name__ == '__main__':
    
    #terminal PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres
    #postgres=#\c pg_database
    #pg_database=# CREATE TABLE pg_table(
    #msg_id BIGSERIAL PRIMARY KEY,
    #msg_date TIMESTAMP WITH TIME ZONE,
    #msg_topic varchar(100),
    #msg_value varchar(10));


    file_name=r'config/config.yaml'
    x = broker(config_file_name=file_name,msg_config="db")
    x.run()