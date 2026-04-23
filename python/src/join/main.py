import bisect
import os
import logging

from common import middleware, fruit_item
import common.message_protocol.internal as protocol

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
        self.merged_tops_by_client: dict[str, list[fruit_item.FruitItem]] = {}
        self.agg_ready_by_client: dict[str, int] = {}

    def _process_top(self, client_id, partial_top: list[fruit_item.FruitItem]):
        logging.info(f"Received top for {client_id}")
        if not client_id in self.merged_tops_by_client:
            self.merged_tops_by_client[client_id] = []
            self.agg_ready_by_client[client_id] = 0

        self.agg_ready_by_client[client_id] += 1
        # Assuming there is no fruit repetition from different sources
        merged_top = self.merged_tops_by_client[client_id]
        for fruit, amount in partial_top:
            bisect.insort(merged_top, fruit_item.FruitItem(fruit, amount))

        if self.agg_ready_by_client[client_id] < AGGREGATION_AMOUNT:
            return
        
        logging.info(f"Sending merged top for {client_id}")
        fruit_chunk = list(merged_top[-TOP_SIZE:])
        fruit_chunk.reverse()

        fruit_top = [(f.fruit, f.amount) for f in fruit_chunk]
        out_top_msg = protocol.FruMessage(client_id, protocol.MsgType.FRUIT_TOP, fruit_top)
        self.output_queue.send(protocol.FruMessage.serialize(out_top_msg))
        del self.merged_tops_by_client[client_id]
        del self.agg_ready_by_client[client_id]

    def process_messsage(self, message, ack, nack):
        fruit_top_msg = protocol.FruMessage.deserialize(message)
        assert(fruit_top_msg.msg_type == protocol.MsgType.FRUIT_TOP)
        self._process_top(fruit_top_msg.client_id, fruit_top_msg.data)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
