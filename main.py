#__CONFIG__
topics="shellies/shellies/#"
auth = dict(username="mqtt-user",password="tryit1ce")
hostname="192.168.178.25"
#__INIT__
import paho.mqtt.subscribe as sub
from paho.mqtt import __version__ as vsn
import time
from datetime import timedelta
import datetime
print(vsn)
ref_time = time.monotonic(), datetime.datetime.now()
print(ref_time)

#__FI..__ (First In)
msg = sub.simple(topics=topics, #from __CONFIG__
                 hostname=hostname, #from __CONFIG__
                 port=1883,
                 auth=auth, #from __CONFIG__
                 msg_count=10)
#__..FO__ (First Out)
#or toilet-flush-out once in a while -> via numpy array func?
dt0 = ref_time[0]
for m in msg: # do something
    dt = timedelta(seconds=m.timestamp-ref_time[0])
    dt2 = timedelta(seconds=m.timestamp-dt0) #only makes sense if the topic is the same, else close to 0 -> see results
    msg_time=ref_time[1]+dt
    print("Ref_Time: ",ref_time[1],"\tMsg_Time: ",msg_time.time(),"\tdt_ref: ",dt, "\tdt_msg",dt2)
    dt0 = m.timestamp