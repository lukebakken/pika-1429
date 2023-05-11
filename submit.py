# -*- coding: utf-8 -*-
import json
import logging
import os
import pika
import sys
import traceback
from loguru import logger as lg

cleanupfiles = os.getenv("CLEANUP", "True")
app_env = os.getenv("APP_ENV", "Dev")
rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
rabbitmq_port = int(os.getenv("RABBITMQ_PORT", "5672"))
rabbitmq_user = os.getenv("RABBITMQ_USER", "guest")
rabbitmq_pass = os.getenv("RABBITMQ_PASS", "guest")


def exception_handler(exctype, value, tb):
    lg.add("file_{time}.log")
    lg.error(exctype)
    lg.error(value)
    lg.error(traceback.extract_tb(tb))


sys.excepthook = exception_handler


LOG_FORMAT = (
    "%(levelname) -10s %(asctime)s %(name) -30s %(funcName) "
    "-35s %(lineno) -5d: %(message)s"
)
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)


def rabbit_init():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port)
    )
    channel = connection.channel()
    return connection, channel


def post_to_queue(jsondata, queuename):
    connection, channel = rabbit_init()
    channel.queue_declare(queue=queuename, auto_delete=False)
    channel.basic_publish(exchange="", routing_key=queuename, body=json.dumps(jsondata))
    connection.close()
    return {"status": "sent"}


jsondata01 = {"request": {"params": {"jobbatchid": 1}}}
jsondata02 = {"request": {"params": {"jobbatchid": 2}}}
jsondata03 = {"request": {"params": {"jobbatchid": 3}}}
jsondata04 = {"request": {"params": {"jobbatchid": 4}}}
jsondata05 = {"request": {"params": {"jobbatchid": 5}}}
jsondata06 = {"request": {"params": {"jobbatchid": 6}}}
jsondata07 = {"request": {"params": {"jobbatchid": 7}}}
jsondata08 = {"request": {"params": {"jobbatchid": 8}}}
jsondata09 = {"request": {"params": {"jobbatchid": 9}}}
jsondata10 = {"request": {"params": {"jobbatchid": 10}}}

post_to_queue(jsondata01, "start")
post_to_queue(jsondata02, "start")
post_to_queue(jsondata03, "start")
post_to_queue(jsondata04, "start")
post_to_queue(jsondata05, "start")
post_to_queue(jsondata06, "start")
post_to_queue(jsondata07, "start")
post_to_queue(jsondata08, "start")
post_to_queue(jsondata09, "start")
post_to_queue(jsondata10, "start")
