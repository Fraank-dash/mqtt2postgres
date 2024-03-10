hostname= "192.168.178.25"
topics =["shellies/#"]
auth = dict(username="mqtt-user",password="tryit1ce")
#https://github.com/eclipse/paho.mqtt.python/blob/master/examples/subscribe_simple.py

#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Copyright (c) 2016 Roger Light <roger@atchoo.org>
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Distribution License v1.0
# which accompanies this distribution.
#
# The Eclipse Distribution License is available at
#   http://www.eclipse.org/org/documents/edl-v10.php.
#
# Contributors:
#    Roger Light - initial implementation

# This shows an example of using the subscribe.simple helper function.

#import context  # Ensures paho is in PYTHONPATH

import paho.mqtt.subscribe as subscribe

#topics = ['#']

m = subscribe.simple(topics, 
                     hostname=hostname,
                     auth=auth, 
                     retained=False, 
                     msg_count=10)
for a in m:
    print(a.timestamp,a.topic,a.payload)