# -*- coding: utf-8 -*-
"""
Created on Fri Aug 31 23:34:18 2018

@author: kiruparan
"""

from kafka import KafkaConsumer

consumer = KafkaConsumer('output_event', bootstrap_servers=['abc.def.com:9092'])
for msg in consumer:
    print(msg.value)