import pika
from pika.adapters.blocking_connection import BlockingChannel
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.queue_name:str = queue_name

        self.connection:pika.BlockingConnection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel:BlockingChannel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.exchange_name:str = exchange_name
        self.routing_keys = routing_keys
        self.connection:pika.BlockingConnection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel:BlockingChannel = self.connection.channel()
        #Direct porque hay tests con casos "direct messaging" (1 key -> 1 consumidor) y otros
        #tipo "broadcas" (1 key -> muchos consumidores). Permite ambos casos. fanout ignoraria la key
        #y topic no es necesaria para los casos de uso
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')
        declared_queue = self.channel.queue_declare(queue='', exclusive=True)
        self.queue = declared_queue.method.queue

        for routing_key in self.routing_keys:
            self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue, routing_key=routing_key)
        

        pass
