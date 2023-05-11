# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205
import functools
import logging
import threading
import time,os,json,pathlib,base64,sys,traceback
from random import randint
from time import sleep

import pika,pandas as pd, sqlalchemy as sal
from pika.exchange_type import ExchangeType
from loguru import logger as lg

cleanupfiles = os.getenv('cleanup','True')
app_env = os.getenv('app_env','Dev')
#rabbitmq_host = os.getenv('rabbitmq_host','localhost')
rabbitmq_host = os.getenv('rabbitmq_host','cluster.rke.natimark.com')
#rabbitmq_port = int(os.getenv('rabbitmq_port','5672'))
rabbitmq_port = int(os.getenv('rabbitmq_port','32304'))
rabbitmq_user = os.getenv('rabbitmq_user','guest')
rabbitmq_pass = os.getenv('rabbitmq_pass','guest')
worker_threads = 4 ##this is the var i want to set for how many worker threads run

# if os.name == 'posix':
#     rabbitmq_watchqueue = os.getenv('rabbitmq_watchqueue','finish')
#     rabbitmq_resultqueue = os.getenv('rabbitmq_resultqueue','')
# else:
#     rabbitmq_watchqueue = os.getenv('rabbitmq_watchqueue','start')
#     rabbitmq_resultqueue = os.getenv('rabbitmq_resultqueue','finish')
rabbitmq_watchqueue = os.getenv('rabbitmq_watchqueue','start')
rabbitmq_resultqueue = os.getenv('rabbitmq_resultqueue','finish')

def exception_handler(exctype, value, tb):
    lg.add("file_{time}.log")
    lg.error(exctype)
    lg.error(value)
    lg.error(traceback.extract_tb(tb))

sys.excepthook = exception_handler


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)


def ack_message(ch, delivery_tag):
    """Note that `ch` must be the same pika channel instance via which
    the message being ACKed was retrieved (AMQP protocol constraint).
    """
    if ch.is_open:
        ch.basic_ack(delivery_tag)
    else:
        # Channel is already closed, so we can't ACK this message;
        # log and/or do something that makes sense for your app in this case.
        pass

def do_work(ch, delivery_tag, body):
    
    thread_id = threading.get_ident()
    jsondata = json.loads(body)
    
    LOGGER.info('Thread id: %s Delivery tag: %s jobbatchid: %s', thread_id,
                delivery_tag, jsondata['request']['params']['jobbatchid'])
    entry_func = globals()[rabbitmq_watchqueue]
    try:
        entry_func(jsondata)
    except:
        import sys
        exc_info = sys.exc_info()
        print("""############
    ############
    ############
        
        Error running job: """ + exc_info + """

    ############
    ############
    ############""")
        #if we error. stop everything (bounce container)
        channel.stop_consuming()
        pass
    finally:
        cb = functools.partial(ack_message, ch, delivery_tag)
        ch.connection.add_callback_threadsafe(cb)
    
def on_message(ch, method_frame, _header_frame, body, args):
    thrds = args
    delivery_tag = method_frame.delivery_tag
    t = threading.Thread(target=do_work, args=(ch, delivery_tag, body))
    t.start()
    thrds.append(t)

def start(jsondata):
    print("""############
############
############
    
    starting job

############
############
############""")
    jsondata = runjob(jsondata)
    putcompleted(jsondata)
    return jsondata

def runjob(jsondata):
    # this runs on windows node
    # random sleep
    sleepint=randint(10,100)
    sleep(sleepint)
    jsondata['response'] = {}
    jsondata['response']['result'] = 'windows slept for '+str(sleepint)
    lg.debug(json.dumps(jsondata))
    return jsondata

def putcompleted(jsondata):
    r = post_to_queue(jsondata,rabbitmq_resultqueue)
    return r

def finish(jsondata):
    # this runs on linux node
    # random sleep    
    sleepint=randint(10,100)
    sleep(sleepint)
    jsondata['response'] = {}
    jsondata['response']['result'] = 'Linux slept for '+str(sleepint)
    lg.debug(json.dumps(jsondata))
    return #all done

def rabbit_init():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=rabbitmq_host, 
        port=rabbitmq_port)
    )
    channel = connection.channel()
    return connection,channel

def post_to_queue(jsondata,queuename):
    connection,channel = rabbit_init()
    channel.queue_declare(queue=queuename,auto_delete=False)
    channel.basic_publish(exchange='', routing_key=queuename, body=json.dumps(jsondata))
    connection.close()
    return({'status':'sent'})

credentials = pika.PlainCredentials(username=rabbitmq_user,password=rabbitmq_pass)
# Note: sending a short heartbeat to prove that heartbeats are still
# sent even though the worker simulates long-running work
parameters = pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=credentials, heartbeat=30)
connection = pika.BlockingConnection(parameters)

channel = connection.channel()
channel.exchange_declare(
    exchange=rabbitmq_watchqueue,
    exchange_type=ExchangeType.direct,
    passive=False,
    durable=True,
    auto_delete=False)
channel.queue_declare(queue=rabbitmq_watchqueue, auto_delete=False)
channel.queue_bind(
    queue=rabbitmq_watchqueue, exchange=rabbitmq_watchqueue, routing_key=rabbitmq_watchqueue)
# Note: prefetch is set to 1 here as an example only and to keep the number of threads created
# to a reasonable amount. In production you will want to test with different prefetch values
# to find which one provides the best performance and usability for your solution
channel.basic_qos(prefetch_count=1)

threads = []
on_message_callback = functools.partial(on_message, args=(threads))
channel.basic_consume(on_message_callback=on_message_callback, queue=rabbitmq_watchqueue)


try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()

# Wait for all to complete
for thread in threads:
    thread.join()

connection.close()  