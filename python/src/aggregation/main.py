import os
import logging
import bisect

from common import middleware, fruit_item
import common.message_protocol.internal as protocol

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
        self.fruit_top_by_client: dict[str, list[fruit_item.FruitItem]] = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        if not client_id in self.fruit_top_by_client:
            self.fruit_top_by_client[client_id] = []
        
        client_fruit_top = self.fruit_top_by_client[client_id]
        for i in range(len(client_fruit_top)):
            if client_fruit_top[i].fruit == fruit:
                client_fruit_top[i] = client_fruit_top[i] + fruit_item.FruitItem(
                    fruit, amount
                )
                return
        bisect.insort(client_fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        logging.info("Received EOF")
        client_fruit_top = self.fruit_top_by_client[client_id]
        fruit_chunk = list(client_fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        out_top_msg = protocol.FruMessage(client_id, protocol.MsgType.FRUIT_TOP, fruit_top)
        self.output_queue.send(out_top_msg.serialize())
        del self.fruit_top_by_client[client_id]

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fru_msg = protocol.FruMessage.deserialize(message)
        if fru_msg.msg_type == protocol.MsgType.FRUIT_RECORD:
            self._process_data(fru_msg.client_id, *fru_msg.data)
        elif fru_msg.msg_type == protocol.MsgType.END_OF_RECODS:
            self._process_eof(fru_msg.client_id)
        else:
            ack()
            raise RuntimeError(f"Unsupported message type {fru_msg.msg_type}")
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
