# https://www.emqx.com/en/blog/how-to-use-mqtt-in-python 
# python 3.12

print("___IMPORT___")
from paho.mqtt import client as mqtt_client
import datetime
import time
from datetime import timedelta


print("___LOAD_CONFIG___")
broker = 'localhost'
port = 1883
topics =['$SYS/#']
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
    def on_message(client, userdata,msg):
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