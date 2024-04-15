source/broker2db.py is the only 'working' py-script sof ar (15.04.2024)
it is more like a "proof-of-concept" or "prototype" so far...

1. you need a mqtt-broker and a database-server
- in this case 2 local-hosted docker containers (15.04.2024)
- settings can be found in 'config/config.yaml' for now. (15.04.2024)
-- going to be replaced with datacontracts.yaml later on...
  
1.1. mqtt-broker
eclipse-mqtt:latest with bare minimum config, w/o security like ssl/tls/etc.

1.2. postgresdb-server
postgres:latest with bare minimum config, w/o security like ssl/tls/etc.
- going to be replaced with timeseriesDB (soon^TM - some sort of postgres fork specialized on temporal data)
- surprisingly simple table where anything is written into
-- going to be replaced with timeseriesDB/rolling-hypertable
