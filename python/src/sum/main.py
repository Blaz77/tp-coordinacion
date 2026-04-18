import os
import logging
import threading

from common import middleware, fruit_item
import common.message_protocol.internal as protocol

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
        self.sums_by_client: dict[str, dict[str, fruit_item.FruitItem]] = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        if not client_id in self.sums_by_client:
            self.sums_by_client[client_id] = {}
        
        client_amount_by_fruit = self.sums_by_client[client_id]
        client_amount_by_fruit[fruit] = client_amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting data messages")
        for final_fruit_item in self.sums_by_client[client_id].values():
            out_fru_msg = protocol.FruMessage(
                client_id,
                protocol.MsgType.FRUIT_RECORD, 
                [final_fruit_item.fruit, final_fruit_item.amount]
            )
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(out_fru_msg.serialize())

        logging.info(f"Broadcasting EOF message")
        out_end_fru_msg = protocol.FruMessage(client_id, protocol.MsgType.END_OF_RECODS, [])
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(out_end_fru_msg.serialize())


    def process_data_messsage(self, message, ack, nack):
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
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
