import paho.mqtt.client as cli
import paho.mqtt.subscribe as sub
auth = dict(username="mqtt-user",password="tryit1ce")

msg = sub.simple("shellies/shellies/#", 
                 hostname="192.168.178.25",
                 port=1883,
                 auth=auth
                 )

print(msg)
print(msg.topic)