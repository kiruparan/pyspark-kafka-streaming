# -*- coding: utf-8 -*-
"""
Created on Mon Oct  1 13:26:06 2018

@author: kiruparan
"""

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from collections import Counter

producer = KafkaProducer(bootstrap_servers='abc.def.com:9092', value_serializer=str.encode, key_serializer=str.encode)

#create SC with the specified configuration
def spark_context_creator():
    conf = SparkConf()
    #set name for our app
    conf.setAppName("ConnectingDotsSparkKafkaStreaming")
    #The master URL to connect
    conf.setMaster('spark://abc.def.ghi.jkl:7077')
    sc = None
    try:
        sc.stop()
        sc = SparkContext(conf=conf)
    except:
        sc = SparkContext(conf=conf)
    return sc 

#push the processed event to Kafka
def push_back_to_kafka(processed_events):
    list_of_processed_events = processed_events.collect()
    producer.send('output_event', value = str(list_of_processed_events))

#processing each micro batch
def process_events(event):
    return (event[0], Counter(event[1].split(" ")).most_common(3))
    
sc = spark_context_creator()
#To avoid unncessary logs
sc.setLogLevel("WARN")
#batch duration, here i process for each second
ssc = StreamingContext(sc,1)
kafkaStream = KafkaUtils.createStream(ssc, 'abc.def.ghi.jkl:2181', 'test-consumer-group', {'input_event':1})
lines = kafkaStream.map(lambda x : process_events(x))

lines.foreachRDD(push_back_to_kafka)

ssc.start()
ssc.awaitTermination()