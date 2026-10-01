import logging
import os
import threading
import signal
from hashlib import sha256

from common import fruit_item, message_protocol, middleware

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.control_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_CONTROL_EXCHANGE}_{(ID + 1) % SUM_AMOUNT}"],
        )
        self.control_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_CONTROL_EXCHANGE}_{ID}"],
        )
        self.threads = []
        self.amount_by_client = {}
        self.completed_clients = set()
        self.data_output_lock = threading.Lock()
        self.shutdown = False

    def _process_data(self, uuid, fruit, amount):
        logging.info(f"Process data")
        with self.data_output_lock:
            self.amount_by_client[uuid] = self.amount_by_client.get(uuid, {})
            self.amount_by_client[uuid][fruit] = self.amount_by_client[uuid].get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, amount)

    def _process_eof(self, uuid, propagate=True):

        logging.info(f"Broadcasting data messages")

        with self.data_output_lock:
            if uuid in self.completed_clients:
                return
            self.completed_clients.add(uuid)

            for final_fruit_item in self.amount_by_client.get(uuid, {}).values():
                routing_key = f"{uuid}{final_fruit_item.fruit}".encode("utf-8")
                exchange_index = (
                    int.from_bytes(sha256(routing_key).digest(), "big")
                    % AGGREGATION_AMOUNT
                )
                self.data_output_exchanges[exchange_index].send(
                    message_protocol.internal.serialize(
                        [uuid, final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )

            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(message_protocol.internal.serialize([uuid]))

            if propagate:
                self.control_output_exchange.send(
                    message_protocol.internal.serialize([uuid])
                )

            if uuid in self.amount_by_client:
                del self.amount_by_client[uuid]

    def process_control_messsage(self, message, ack, nack):
        logging.info("Process control message")
        try:
            fields = message_protocol.internal.deserialize(message)
        except Exception as e:
            logging.error(f"Error deserializing control message: {e}")
            nack()
            return

        if len(fields) == 1:
            self._process_eof(*fields)
        ack()

    def process_data_messsage(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
        except Exception as e:
            logging.error(f"Error deserializing data message: {e}")
            nack()
            return
        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 1:
            self._process_eof(*fields)
        else:
            logging.error(f"Unexpected number of fields in data message: {len(fields)}")
            nack()
            return
        ack()

    def start(self):
        thread = threading.Thread(
            target=self.control_input_exchange.start_consuming,
            args=(self.process_control_messsage,),
        )
        thread.start()
        self.threads.append(thread)
        self.input_queue.start_consuming(self.process_data_messsage)

    def stop(self):
        if self.shutdown:
            return
        self.shutdown = True
        self.input_queue.stop_consuming()
        self.control_input_exchange.stop_consuming()

        for t in self.threads:
            t.join()

        self.input_queue.close()
        self.control_input_exchange.close()
        self.control_output_exchange.close()
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.close()

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.input_queue.stop_consuming()
        self.control_input_exchange.stop_consuming()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    signal.signal(signal.SIGTERM, sum_filter.handle_sigterm)
    try:
        sum_filter.start()
    except KeyboardInterrupt:
        logging.info("Sum filter stopped by user")
    except Exception as e:
        logging.error(f"Error starting sum filter: {e}")
    finally:
        sum_filter.stop()
    return 0


if __name__ == "__main__":
    main()
