import pika
from pika.adapters.blocking_connection import BlockingChannel
import pika.exceptions as PikaExceptions
import random
import string
from .middleware import MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.queue_name:str = queue_name

        self.connection:pika.BlockingConnection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel:BlockingChannel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

    def send(self, message):
        try:

            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)

        #Capturo errores por desconeccion especificos de RabbitMQ, no se tiene en cuenta
        #desconeccion por problemas de socket, por ejemplo
        except (PikaExceptions.AMQPConnectionError,
                PikaExceptions.ChannelWrongStateError) as e:
            raise MessageMiddlewareDisconnectedError (str(e)) from e
        
        except PikaExceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(str(e)) from e 
        #Diferencio desconceccion por problema de Rabbit que por problema de network
        except (ConnectionError, OSError) as e:
            raise MessageMiddlewareDisconnectedError (str(e)) from e 
        

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
        

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)

        #Capturo errores por desconeccion especificos de RabbitMQ, no se tiene en cuenta
        #desconeccion por problemas de socket, por ejemplo
        except (PikaExceptions.AMQPConnectionError,
                PikaExceptions.ChannelWrongStateError) as e:
            raise MessageMiddlewareDisconnectedError (str(e)) from e
        
        except PikaExceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(str(e)) from e 
        
        #Diferencio desconceccion por problema de Rabbit que por problema de network
        except (ConnectionError, OSError) as e:
            raise MessageMiddlewareDisconnectedError (str(e)) from e 
