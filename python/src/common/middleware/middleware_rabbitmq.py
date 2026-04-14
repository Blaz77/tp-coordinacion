import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic
from pika.frame import Method
from pika.exceptions import AMQPConnectionError
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

# Exceptions doc
# https://pika.readthedocs.io/en/stable/_modules/pika/exceptions.html

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def _on_message_received(self, ch: BlockingChannel, method: Basic.Deliver, properties, body: bytes):
        ack_func = lambda: ch.basic_ack(delivery_tag = method.delivery_tag)
        nack_func = lambda: ch.basic_nack(delivery_tag = method.delivery_tag)
        self.on_message_callback(body, ack_func, nack_func)

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.queue_name = queue_name

        self.channel.queue_declare(queue=self.queue_name, 
                                   durable=True,
                                   arguments={'x-queue-type': 'quorum'})
    
    def start_consuming(self, on_message_callback):
        self.on_message_callback = on_message_callback
        try:
            self.channel.basic_consume(queue=self.queue_name, 
                                       on_message_callback=self._on_message_received, 
                                       auto_ack=False)
            self.channel.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e
	
    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e

    def send(self, message):
        try:
            self.channel.basic_publish(exchange='',
                                       routing_key=self.queue_name,
                                       body=message)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def close(self):
        try:
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def _on_message_received(self, ch: BlockingChannel, method: Basic.Deliver, properties, body: bytes):
        ack_func = lambda: ch.basic_ack(delivery_tag = method.delivery_tag)
        nack_func = lambda: ch.basic_nack(delivery_tag = method.delivery_tag)
        self.on_message_callback(body, ack_func, nack_func)
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

    def start_consuming(self, on_message_callback):
        self.on_message_callback = on_message_callback
        try:
            result: Method = self.channel.queue_declare(queue='', exclusive=True)
            queue_name: str = result.method.queue

            for key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name,
                                        queue=queue_name,
                                        routing_key=key)
            
            self.channel.basic_consume(queue=queue_name, 
                                       on_message_callback=self._on_message_received, 
                                       auto_ack=False)
            self.channel.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e

    def send(self, message):
        try:
            # Should publishers with multiple routing keys be supported?
            for key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name,
                                           routing_key=key,
                                           body=message)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def close(self):
        try:
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e
