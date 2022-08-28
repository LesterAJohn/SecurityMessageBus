from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract, ContractDetails
from ibapi.order import Order
from ibapi.common import SetOfString
from ibapi.common import SetOfFloat
from ibapi.common import BarData
from threading import Timer, Event, activeCount
from numpy import double, str0
from datetime import date, datetime, timedelta
from random import randrange
from logging.handlers import RotatingFileHandler
from ib_insync.order import Trade, OrderStatus
from ib_insync.objects import Position
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

import numpy as np
import queue
import math
import string
import time
import datetime
import random
import threading
import multiprocessing as mp
import os
import numpy as np
import logging
import sys
import getopt
import configparser
import pymongo
import asyncio
import motor.motor_asyncio
import talib
import pika

random.seed(a=None, version=2)
event = threading.Event()

configParser = configparser.RawConfigParser()
configFilePath = 'PATH/config.conf'
configParser.read(configFilePath)

dbPath = configParser.get('DIRECTORY', 'DB')
logsPath = configParser.get('DIRECTORY', 'LOGS')
mongodbConn = configParser.get('DIRECTORY', 'MONGODBCONN')
mongoDB = configParser.get('DIRECTORY', 'MONGODB')

influxdbToken = configParser.get('DIRECTORY', 'INFLUXDBTOKEN')
influxdbOrg = configParser.get('DIRECTORY', 'INFLUXDBORG')
influxdbBucket = configParser.get('DIRECTORY', 'INFLUXDBBUCKET')
influxdbURL = configParser.get('DIRECTORY', 'INFLUXDBURL')

rabbitMQHost = configParser.get('DIRECTORY', 'RABBITMQHOST')
rabbitMQuid = configParser.get('DIRECTORY', 'RABBITMQUID')
rabbitMQupw = configParser.get('DIRECTORY', 'RABBITMQUPW')
rabbitMQttl = configParser.get('DIRECTORY', 'RABBITMQTTL')


class rabbitMQ():
    def __init__(self):
        self.credentials = pika.PlainCredentials(rabbitMQuid, rabbitMQupw)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitMQHost, 5672, '/', self.credentials))
        self.channel = self.connection.channel()

    def declareQueue(self, queue):
        self.channel.queue_declare(queue = queue)

    def sendMessage(self, exchange, queue, message, ttl):
        properties = pika.BasicProperties(expiration=ttl)
        self.declareQueue(queue)
        rabbitMQManagement.resAddQueueRecord(queue, 'publisher')
        
        self.channel.basic_publish(exchange = exchange, 
                                   routing_key = queue,
                                   body = message, properties = properties)
        self.channel.close()
    
    def subReadQueue(self, queue, callback):
        self.declareQueue(queue)
        self.channel.basic_consume(queue =  queue,
                                   auto_ack = False,
                                   on_message_callback = callback)
        
        self.channel.start_consuming()
        

class rabbitMQManagement():      
    def resAddQueueRecord(queueName, pub_sub):
        db = Mongodb()
        query = { 'queueName' : queueName}
        data = { 'queueName' : queueName, 'publish' : False, 'subscribe' : False}
        if (pub_sub == 'publisher'):
            update_data = {"publish" : True}
        if (pub_sub == 'subscriber'):
            update_data = {"subscribe" : True}
        db.addUniqueRecord('QueueInfo', query, data, update_data)

    def clearqueueInfo():
        db = Mongodb()
        query = {"$or":[{"subscribe" : True},{"publish" : True}]}
        data = {'subscribe' : False, 'publish' : False}
        db.recordUpdates('QueueInfo', query, data)
        
    def reqQueueRecordCount():
        db = Mongodb()
        query = {"$or": [{'publish' : True}]}
        
        return db.recordCount('QueueInfo', query)


class Mongodb():
    def __init__(self):
        self.client = pymongo.MongoClient(mongodbConn, connect=False, maxIdleTimeMS=5000)
        self.db = self.client[mongoDB]

    # General Functions
    def showCollections(self):
        print(self.db.list_collection_names())
        
    def insertDocument(self, collection, data):
        activeCol = self.db[collection]     
        activeCol.insert_one(data)
               
    def recordQuery(self, collection, query):
        record = []
        activeCol = self.db[collection]
        record = activeCol.find_one(query)
        return (record)
    
    def recordQueries(self, collection, query):
        record = []
        activeCol = self.db[collection]
        record = activeCol.find(query)
        return (record)
    
    def recordCount(self, collection, query):
        count = 0
        activeCol = self.db[collection]
        count = activeCol.count_documents(query)
        return count
    
    def recordUpdate(self, collection, query, data):
        activeCol = self.db[collection]
        activeCol.update_one(query, { "$set" : data })

    def recordUpdates(self, collection, query, data):
        activeCol = self.db[collection]
        activeCol.update_many( query, { "$set" : data })
        
    def updateAcctRecord(self, collection, query, data, update_data):
        activeCol = self.db[collection]
        update_data = {"$set": update_data}
        activeCol.update_one(query, update_data)
        
    def delAcctRecords(self, collection, query):
        activeCol = self.db[collection]
        activeCol.delete_many(query)

    def delAcctRecord(self, collection, query):
        activeCol = self.db[collection]
        activeCol.delete_one(query)
        
    def updateDocumentField(self, collection, query, data, update_data):
        log.info("Add a Document Field " + str(data) + " to Database " + collection)
        activeCol = self.db[collection]
        update_data = {"$set": update_data}
        activeCol.update_many(query, update_data)
    
    def addUniqueRecord(self, collection, query, data, update_data):
        activeCol = self.db[collection]
        if(self.recordCount(collection, query) == 0):
            activeCol.insert_one(data)
        else:
            update_data = {"$set": update_data}
            activeCol.update_one(query, update_data)


class influxdb():
    def __init__(self):
        self.influxClient = InfluxDBClient(url=influxdbURL, token=influxdbToken, org=influxdbOrg)
        self.write_api = self.influxClient.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.influxClient.query_api()
        
    def pointWrite(self, measurement, tags, fieldName, field, barTime):
        point = Point(measurement) \
            .tag(tags[0],tags[1]) \
            .field(fieldName, field) \
            .time(barTime, WritePrecision.NS)
        self.write_api.write(influxdbBucket, influxdbOrg, point)
        
    def pointQuery(self, query):
        return self.query_api.query(org=influxdbOrg, query=query)


class logicFunctions():        
    def logic_postionAverage(avgCost, positionPrice, secType, direction):
        if (direction == 'none'):
            pass
        if (direction == 'C'):
            pass
        if (direction == 'P'):
            pass
        
        