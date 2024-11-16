#IMPORTS
##GENERAL
import yaml
##BROKER
from paho.mqtt import client as mqtt_client
import time
from datetime import timedelta,datetime
##POSTGRES
from sqlalchemy import create_engine, MetaData
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import inspect,text,Table


class broker:
    def __init__(self, config_file_name:str,msg_config:str='print'):
        """
        I am not happy with the class name 'broker' since it is more like a one-directional interface 
        
        I did not care about security like ssl, password security or anything else since this is more like a proof of concept! 
        Therefore, i assume, that, if you can read and UNDERSTAND this code, it is obvious to you, that storing ip adresses, 
        usernames and passwords within a single 'config.yaml' file is a stupid idea! 
        (And uploading it as a public repository to the internet is worse!)
        So, if you use this code, be aware of the risks. 
        but what it basically does is: 
        a) load a yaml.config based on the "config/config.yaml"-File 
        b) the database connection is established based on the in a) loaded config file
        c) an  mqtt-client is created which connects via the in a) mentioned broker-settings


        """
        #loading config
        self.conf = self.load_config_file(config_file_name)
        #db-connection
        self._engine = self.create_engine_from_conf(conf=self.conf)
        try:
            self.db_connection = self._engine.connect()
        except ConnectionError as ex:
            print(ex)
        self.metadata = MetaData()
        self.metadata.reflect(bind=self._engine)
        self.pg_table = Table(self.conf['db_server']['database']['db_table'], self.metadata, autoload = True)
        #broker-connection
        self.ref_time = self.get_reference_time()
        self.client = mqtt_client.Client(clean_session=True)
        try:
            self.client.username_pw_set(username=self.conf['mqtt_server']['connection']['user'],
                                        password=self.conf['mqtt_server']['connection']['pw'],)
        except ConnectionError as ex:
            print(ex)
        self.client.connect(host=self.conf['mqtt_server']['ip'],
                            port=self.conf['mqtt_server']['port'])
        ##subscriptions
        for topic in self.conf['mqtt_server']['topics']:
            self.client.subscribe(topic)
        ##other whacky stuff
        self.client.on_connect = self.on_connect()
        self.client.on_message = self.on_message(msg_config)

    def load_config_file(self,file_name:str):
        """func to load configfile
        """
        file = open(file=file_name,mode='r')
        conf = yaml.safe_load(file)
        return conf
    
    def on_connect(self):
        """
        """
        def on_connect(client,userdata,flags,rc):
            if rc == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", rc)
        return on_connect
    def on_message(self,msg_config:str):
        """3 different options for 'on message'
        well, actually 2 different options...
        i thought about writing into a csv-file or other stuff as well for testing, 
        but writing to a database worked so well that i havent added this option
        """
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
        """
        lorem ipsum
        """
        return dict(t_mono=time.monotonic(), t_dt=datetime.now())

    def t_mono2t_datetime(self,ref_time:dict, msg_timestamp:int) -> dict:
        dt = timedelta(seconds=msg_timestamp-ref_time['t_mono'])
        msg_dt = ref_time['t_dt']+dt 
        return msg_dt, msg_timestamp
    ##POSTGRES-DATABASE
    def create_engine_from_conf(self,conf:dict):
        """
        lorem ipsum
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
        """lorem ipsum
        the only way to stop is Ctrl+C or Alt+F4 :D
        """
        self.client.loop_forever()


if __name__ == '__main__':
    # STEPS TO DO iN ORDER TO SET EVERYTHING UP (but not yet an automated part of the setup
    # ___Python_ENV___
    #conda create --name mqtt2pg python=3.13
    #conda activate mqtt2pg
    #conda install pyyaml conda-forge::paho-mqtt psycopg2 sqlalchemy pandas numpy networkx

    # ___DOCKER___
    # 1. MQTT-Broker
        # -> basic installation... https://hub.docker.com/_/eclipse-mosquitto/
        # docker pull eclipse-mosquitto
        # docker run -it -p  1883:1883 ...

    # 2. Postgres (Timescale(!))
        # -> basic installation... https://docs.timescale.com/self-hosted/latest/install/installation-docker/
        # Timescale because handling timeseries with hypertables, pre written timeseries aggregation etc.
        # docker pull timescale/timescaledb-ha:pg16
        # docker run -d --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=postgres timescale/timescaledb-ha:pg16

    # ___Postgres-Terminal or via PGAdmin4___
    # 1. create new db
        #CREATE DATABASE staging_mqtt
        #    WITH
        #    OWNER = postgres
        #    ENCODING = 'UTF8'
        #    LC_COLLATE = 'C.UTF-8'
        #    LC_CTYPE = 'C.UTF-8'
        #    LOCALE_PROVIDER = 'libc'
        #    TABLESPACE = pg_default
        #    CONNECTION LIMIT = -1
        #    IS_TEMPLATE = False;
        #
        #COMMENT ON DATABASE staging_mqtt
        #    IS 'Staging für den mqtt_client';

    # 2. create table
        # see https://docs.timescale.com/use-timescale/latest/hypertables/create/

        # CREATE TABLE tbl_staging_mqtt (
        #    msg_date   TIMESTAMPTZ NOT NULL,
        #    msg_topic  TEXT        NOT NULL,
        #    msg_value  TEXT        NOT NULL
        #    );
        # SELECT create_hypertable('tbl_staging_mqtt', by_range('msg_date'));


    file_name=r'config/config.yaml'
    x = broker(config_file_name=file_name,msg_config="db")
    x.run()