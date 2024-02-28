from consumer_interface import mqConsumerInterface
import pika
import os
import sys
import json

class mqConsumer(mqConsumerInterface):
    def _init_(self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name

        self.setupRMQConnection()
        
    def on_message_callback(self, channel, method_frame, header_frame, body) -> None:
        message = json.loads(body)
        self.channel.start_consuming(message)
        self.connection.close()

    def setupRMQConnection(self) -> None:
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        self.connection = pika.BlockingConnection(parameters=conParams)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exhange = 'Test Exchange')
        self.channel.queue_declare(queue ="Test queue")
        self.channel.queue_bind(
            queue= self.queue_name,
            routing_key = self.binding_key,
            exchange = self.exchange_name,
        )
        self.channel.basic_consume(
            self.queue_name, on_message_callback=self.on_message_callback, auto_ack=False
        )

    def startConsuming(self) -> None:
        self.channel.basic_ack(self.delivery_tag, False)
    
    def __del__(self) -> None:
        self.channel.close()
        self.connection.close()
    
   

