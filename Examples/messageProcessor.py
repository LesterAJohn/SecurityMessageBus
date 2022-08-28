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
import os
import numpy as np
import logging
import sys
import getopt
import configparser
import pymongo
import asyncio
import motor.motor_asyncio
import pika

sys.path.insert(1, os.path.abspath("PATH"))
from commonLib import rabbitMQ, rabbitMQManagement, Mongodb, influxdb, logicFunctions

random.seed(a=None, version=2)
event = threading.Event()

configParser = configparser.RawConfigParser()
configFilePath = os.path.abspath('PATH messageProcessor.conf')
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

# Global Variables
messageDict = []

class DBApp():
    def messageProcessorDecision(account, messageType, field, fieldData):
                
        logic = LogicFunctions()
        
        query = { 'account' : account }
        data = { 'account' : account,  'netLiq' : 0, 'buyPower' : 0, 'acctUnRealizedPnL' : 0, 'acctRealizedPnL' : 0, 'cashBalance' : 0, 'stockMarketValue' : 0, 'optionMarketValue' : 0, 'acctRealizedLossLimit' : 0}
        update_data = {"account" : data['account']}
        if (messageType == 'messageType'):
            if (field == 'fieldDecription'):
                x = fieldData.split(",")
                messageDict = {
                            'item' : x[0]
                             }
        if (messageType == 'messageType'):
            if (field == 'fieldDecription'):
                x = fieldData.split(",")
                messageDict = {
                            'item' : x[0]
                             }



class rabbitMQlocal():
    def __init__(self):
        self.credentials = pika.PlainCredentials(rabbitMQuid, rabbitMQupw)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitMQHost, 5672, '/', self.credentials))
        self.channel = self.connection.channel()

    def declareQueue(self, queue):
        self.channel.queue_declare(queue = queue)

    def sendMessage(self, exchange, queue, message, ttl):
        properties = pika.BasicProperties(expiration=ttl)
        self.declareQueue(queue)
        self.channel.basic_publish(exchange = exchange, 
                                   routing_key = queue,
                                   body = message, properties = properties)
        self.channel.close()
    
    def subReadQueue(self, queue, callback):
        self.declareQueue(queue)
        self.channel.basic_consume(queue =  queue,
                                   auto_ack = False,
                                   on_message_callback = messages.callBackRead)
        
        self.channel.start_consuming()


class messages():
    def sendMessage(self, queue, message, ttl):
        sm = rabbitMQ()
        sm.sendMessage('', queue, message, ttl)
        log.info ("Message sent to Queue: " + queue + " Message: " + message + " ttl: " +str(ttl))
        
    def readMessage(self, queue, callback):
        sm = rabbitMQlocal()
        sm.subReadQueue(queue, callback)
    
    def callBackRead(ch, method, properties, body):
        log.info(" [x] Received %r" % body)
        message = str(body)
        x = message.split(":")
        log.info (message)
        DBApp.resStkInfoRecord(x[1], x[3], x[4], x[5])
        ch.basic_ack(delivery_tag = method.delivery_tag)


class messageApp():
    def __init__(self):
        rabbitMQlocal.__init__(self)
        
    def start(self):
        global queueName
        try:
            if (ActiveFunction == "ActiveFunction"):
                rabbitMQManagement.resAddQueueRecord(queueName,'subscriber')
                messages.readMessage(self, queueName, "messages.callBackARead")
        except Exception as e:
            log.info("Initiation Startup Functions " + str(e))


def loop(time):
    timeDelay = datetime.datetime.now() + datetime.timedelta(seconds=time)
    while (datetime.datetime.now() < timeDelay):
        pass

def main(argv):
    global runActive
    global Container
    global ConCount
    global ActiveFunction
    global queueName
    
    if os.environ.get('CONTAINER') != None:
        pass
    else:
        os.environ['CONTAINER'] = 'NO'
    
    if (os.environ.get('CONTAINER') == 'YES'):
        log.info("Using Environment Variables")
        Container = True
        ConCount = os.environ['CONCOUNT']
        ActiveFunction = os.environ['ACTIVEFUNCTION']
    else:
        log.info("Using Command Line Variables")
        try:
            opts, args = getopt.getopt(argv, "f:q:", ["ActiveFunction=", "queueName="])
        except getopt.GetoptError:
            log.info("Missing Option -f value  or --ActvieFunction=[balance]")
            log.info("Missing Option -f value  or --queueName=[queue name]")
    
        for opt, arg in opts:
            print(opt, arg)
            if (opt == '-f'):
                ActiveFunction = arg
            if (opt == '-q'):
                queueName = arg
    
    while True:
        messageCore_Load() 
        while (runActive == True):
            loop(2)
        else:
            log.info("Starting Thread Termination and Reconnection Process")
            loop(60)
            runActive = True
            loop(60)
            log.info("Completing Thread Restart and Reconnection Process")


def messageCore_Load():
    app = messageApp()
    try:
        loop_core = threading.Thread(target=app.start)
        loop_core.daemon = True
        loop_core.start()
    except Exception as e:
        log.info("core initial run ERROR Captured")

    
if __name__ == "__main__":
    Rotation = RotatingFileHandler(filename=os.path.abspath('PATH/messageProcessor.log'), mode='a', maxBytes=20*1024*1024, backupCount=3, encoding=None, delay=0)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)-25s %(levelname)-8s %(message)s', datefmt='%y-%m-%d %H:%M:%S', filename=os.path.abspath('PATH/messageProcessor.log'))
    log = logging.getLogger(__name__)
    log.addHandler(Rotation)
    log.addHandler(logging.NullHandler())
    log.addHandler(logging.StreamHandler(sys.stdout))
    
    log.info('messageProcessor Startup')
    
    try:
        main(sys.argv[1:])
    except Exception as e:
        log.info("main ERROR Captured " + str(e))
