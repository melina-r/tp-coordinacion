import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_client = {}
        self.sum_eof_received = {}
        self.shutdown = False

    def _process_data(self, uuid, fruit, amount):
        logging.info("Processing data message")
        client_fruit_top = self.fruit_top_by_client.get(uuid, {})
        client_fruit_top[fruit] = client_fruit_top.get(fruit, 0) + amount
        self.fruit_top_by_client[uuid] = client_fruit_top

    def _process_eof(self, uuid):
        logging.info(f"Received EOF for uuid: {uuid}")
        client_fruit_top = self.fruit_top_by_client.setdefault(uuid, {})
        self.sum_eof_received[uuid] = self.sum_eof_received.get(uuid, 0) + 1
        if self.sum_eof_received[uuid] < SUM_AMOUNT:
            logging.info(f"Waiting for more EOFs for uuid: {uuid}. Received {self.sum_eof_received[uuid]} out of {SUM_AMOUNT}")
            return

        fruit_items = [fruit_item.FruitItem(item[0], item[1]) for item in client_fruit_top.items()]
        fruit_items.sort(reverse=True)
        top = fruit_items[:TOP_SIZE]
        self.output_queue.send(
            message_protocol.internal.serialize_top_message([uuid, top])
        )
        del self.fruit_top_by_client[uuid]
        del self.sum_eof_received[uuid]

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        try:
            fields = message_protocol.internal.deserialize(message)
        except Exception as e:
            logging.error(f"Error deserializing message: {e}")
            nack()
            return
        if len(fields) >= 2:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)

    def stop(self):
        if self.shutdown:
            return
        self.shutdown = True
        self.input_exchange.stop_consuming()
        self.input_exchange.close()
        self.output_queue.close()

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.input_exchange.stop_consuming()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    signal.signal(signal.SIGTERM, aggregation_filter.handle_sigterm)
    try:
        aggregation_filter.start()
    except KeyboardInterrupt:
        logging.info("Aggregation filter stopped by user")
    except Exception as e:
        logging.error(f"Error starting aggregation filter: {e}")
    finally:
        aggregation_filter.stop()
    return 0


if __name__ == "__main__":
    main()
