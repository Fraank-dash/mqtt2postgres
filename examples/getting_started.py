
auth = dict(username="mqtt-user", password="tryit1ce")
broker = ""
broker_address = "192.168.178.25"
broker_port:int = 1883
broker_timeout:int = 60
# https://eclipse.dev/paho/files/paho.mqtt.python/html/index.html
import paho.mqtt.client as mqtt

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("$SYS/#")
    
# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

mqttc = mqtt.Client(userdata= auth)
mqttc.on_connect = on_connect
mqttc.on_message = on_message

mqttc.connect(host=broker_address, 
              port=broker_port, 
              keepalive=broker_timeout,
              properties=auth)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
mqttc.loop_forever()