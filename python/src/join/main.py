import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.top_by_client = {}
        self.aggregation_eof_received = {}
        self.shutdown = False

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        try:
            uuid, fruit_top = message_protocol.internal.deserialize_top_message(message)
        except Exception as e:
            logging.error(f"Error deserializing top message: {e}")
            nack()
            return

        self.top_by_client.setdefault(uuid, {})
        for fruit, amount in fruit_top:
            self.top_by_client[uuid][fruit] = self.top_by_client[uuid].get(fruit, 0) + amount

        self.aggregation_eof_received[uuid] = self.aggregation_eof_received.get(uuid, 0) + 1
        if self.aggregation_eof_received[uuid] == AGGREGATION_AMOUNT:
            logging.info(f"Received all EOFs for uuid: {uuid}, sending to output queue")
            top_list = [fruit_item.FruitItem(fruit, amount) for fruit, amount in self.top_by_client[uuid].items()]
            top_list.sort(reverse=True)
            self.output_queue.send(message_protocol.internal.serialize_top_message([uuid, top_list[:TOP_SIZE]]))
            del self.top_by_client[uuid]
            del self.aggregation_eof_received[uuid]
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

    def stop(self):
        if self.shutdown:
            return
        self.shutdown = True
        self.input_queue.stop_consuming()
        self.input_queue.close()
        self.output_queue.close()

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.input_queue.stop_consuming()

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    signal.signal(signal.SIGTERM, join_filter.handle_sigterm)
    try:
        join_filter.start()
    except KeyboardInterrupt:
        logging.info("Join filter stopped by user")
    except Exception as e:
        logging.error(f"Error occurred while starting join filter: {e}")
    finally:
        join_filter.stop()

    return 0


if __name__ == "__main__":
    main()
