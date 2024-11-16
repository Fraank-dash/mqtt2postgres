# most important headline 
'source/broker2db.py' is the only 'working' py-script so far (16.11.2024)
* it is still more like a "proof-of-concept" or "prototype"
* the code should be executed via (your own) main.py or take a look at main_test.py

# STEPS TO DO, IN ORDER TO SET EVERYTHING UP (that's not yet an automated part of the setup)
1. Python_ENV
* conda create --name mqtt2pg python=3.13
* conda activate mqtt2pg
* conda install pyyaml conda-forge::paho-mqtt psycopg2 sqlalchemy pandas numpy networkx
2. ___DOCKER___
* MQTT-Broker -> basic installation... https://hub.docker.com/_/eclipse-mosquitto/
  -   docker pull eclipse-mosquitto
  -   docker run -it -p  1883:1883 ...
* Postgres (Timescale(!)) -> basic installation... https://docs.timescale.com/self-hosted/latest/install/installation-docker/
  Timescale because handling timeseries with hypertables, pre written timeseries aggregation etc.
  - docker pull timescale/timescaledb-ha:pg16
  - docker run -d --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=postgres timescale/timescaledb-ha:pg16
  - ___Postgres-Terminal or via PGAdmin4___
    + create new db

      CREATE DATABASE staging_mqtt
      WITH
      OWNER = postgres
      ENCODING = 'UTF8'
      LC_COLLATE = 'C.UTF-8'
      LC_CTYPE = 'C.UTF-8'
      LOCALE_PROVIDER = 'libc'
      TABLESPACE = pg_default
      CONNECTION LIMIT = -1
      IS_TEMPLATE = False;
        
      COMMENT ON DATABASE staging_mqtt
      IS 'Staging für den mqtt_client';
    + create table -> see https://docs.timescale.com/use-timescale/latest/hypertables/create/
   
      CREATE TABLE tbl_staging_mqtt (
      msg_date   TIMESTAMPTZ NOT NULL,
      msg_topic  TEXT        NOT NULL,
      msg_value  TEXT        NOT NULL
      );
      
      SELECT create_hypertable('tbl_staging_mqtt', by_range('msg_date'));
