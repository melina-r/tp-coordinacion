import pika

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)


class _MessageMiddlewareRabbitMQBase:
    def __init__(self, host):
        self.host = host
        self.queue_name = None
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self.channel = self.connection.channel()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def start_consuming(self, on_message_callback):
        def callback(channel, method, properties, body):
            ack = lambda: channel.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: channel.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body, ack, nack)

        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(
                queue=self.queue_name, on_message_callback=callback
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def close(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e


class MessageMiddlewareQueueRabbitMQ(_MessageMiddlewareRabbitMQBase, MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        super().__init__(host)
        self.queue_name = queue_name
        try:
            self.channel.queue_declare(queue=queue_name, durable=True)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e


class MessageMiddlewareExchangeRabbitMQ(_MessageMiddlewareRabbitMQBase, MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        super().__init__(host)
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        try:
            self.channel.exchange_declare(
                exchange=self.exchange_name, exchange_type="topic"
            )

            result = self.channel.queue_declare("", exclusive=True)
            self.queue_name = result.method.queue

            for routing_key in self.routing_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=self.queue_name,
                    routing_key=routing_key,
                )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name, routing_key=routing_key, body=message
                )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e
