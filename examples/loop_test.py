import paho.mqtt.subscribe as subscribe
import pandas as pd

topics =['shellies/shellies/shellyplug001/energy/relay/0',
         'shellies/shellies/shellyplug001/energy/relay/0/power',
         'shellies/shellies/shellyplug001/energy/relay/0/energy']
hostname = '192.168.178.25'
port=1883
auth = dict(username="mqtt-user",password="tryit1ce")
subs = subscribe.simple(topics, 
                     hostname=hostname,
                     port=port,
                     auth=auth, 
                     retained=False, 
                     msg_count=30)
arr = []
for sub in subs:
    arr.append((sub.timestamp, sub.topic, sub.payload))


df = pd.DataFrame(arr,columns=["Timestamp","Topic","Payload"])

df.to_csv("Testdump.csv",sep=";")