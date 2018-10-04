# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 14:54:52 2018

@author: kiruparan
"""

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='abc.def.com:9092', value_serializer=str.encode, key_serializer=str.encode)
event_stream_key = 'product_list'
event_stream_value = 'product1 product2 product3 product1'
producer.send('input_event', key = event_stream_key, value = event_stream_value)