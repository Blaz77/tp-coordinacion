import os
import logging
import signal
import threading
import time

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
        self.control_exchange_out = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_{ID}"],
        )
        self.sums_by_client: dict[str, dict[str, fruit_item.FruitItem]] = {}
        self.eof_by_client: dict[str, bool]
        # This lock prevents handling an EOF_NOTIFY while an input data is being processed
        self.flying_input_lock = threading.Lock()
        self.notify_listener: threading.Thread = None

    def _hash_fruit(self, fruit: str):
        # Prime numbers should work well for the amplifier (Neither too big or too small)
        AMPLIFIER = 31
        hash = 0
        for c in fruit:
            hash = hash * AMPLIFIER + ord(c)
        return hash % AGGREGATION_AMOUNT
    
    def _generate_agg_distribution(self, sums: dict[str, fruit_item.FruitItem]) -> list[list[fruit_item.FruitItem]]:
        sums_by_agg_instance = [[] for _ in range(AGGREGATION_AMOUNT)]
        for final_fruit_item in sums.values():
            dst_instance = self._hash_fruit(final_fruit_item.fruit)
            sums_by_agg_instance[dst_instance].append(final_fruit_item)
        return sums_by_agg_instance

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data for {client_id}")
        if not client_id in self.sums_by_client:
            self.sums_by_client[client_id] = {}
        
        #Uncomment to trigger issues (without qos=1)
        #time.sleep(0.04)
        client_amount_by_fruit = self.sums_by_client[client_id]
        client_amount_by_fruit[fruit] = client_amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting EOF_NOTIFY message for {client_id}")
        out_notify_fru_msg = protocol.FruMessage(client_id, protocol.MsgType.END_OF_RECODS_NOTIFY, [])
        self.control_exchange_out.send(out_notify_fru_msg.serialize())

    def _process_eof_notify(self, client_id):
        logging.info(f"Sending data messages for {client_id}")
        sums_by_agg_instance = self._generate_agg_distribution(self.sums_by_client.pop(client_id))
        affected_aggregators = []
        for agg_idx in range(len(sums_by_agg_instance)):
            if len(sums_by_agg_instance[agg_idx]) > 0:
                affected_aggregators.append(agg_idx)

            for final_fruit_item in sums_by_agg_instance[agg_idx]:
                out_fru_msg = protocol.FruMessage(
                    client_id,
                    protocol.MsgType.FRUIT_RECORD, 
                    [final_fruit_item.fruit, final_fruit_item.amount]
                )
                self.data_output_exchanges[agg_idx].send(out_fru_msg.serialize())

        logging.info(f"Broadcasting EOF message for {client_id}")
        out_end_fru_msg = protocol.FruMessage(client_id, protocol.MsgType.END_OF_RECODS, [])
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(out_end_fru_msg.serialize())
    
    def process_control_message(self, message, ack, nack):
        with self.flying_input_lock:
            fru_msg = protocol.FruMessage.deserialize(message)
            if fru_msg.msg_type == protocol.MsgType.END_OF_RECODS_NOTIFY:
                self._process_eof_notify(fru_msg.client_id)
            else:
                logging.error(f"Unsupported control message type {fru_msg.msg_type}")
            ack()
            
    def process_data_messsage(self, message, ack, nack):
        with self.flying_input_lock:
            fru_msg = protocol.FruMessage.deserialize(message)
            if fru_msg.msg_type == protocol.MsgType.FRUIT_RECORD:
                self._process_data(fru_msg.client_id, *fru_msg.data)
            elif fru_msg.msg_type == protocol.MsgType.END_OF_RECODS:
                self._process_eof(fru_msg.client_id)
            else:
                logging.error(f"Unsupported data message type {fru_msg.msg_type}")
            ack()
    
    def _control_worker(self):
        self.control_exchange_in = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_{i}" for i in range(SUM_AMOUNT)],
        )
        self.data_output_exchanges: list[middleware.MessageMiddlewareExchangeRabbitMQ] = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.control_exchange_in.start_consuming(self.process_control_message)

        self.control_exchange_in.close()
        for exchange in self.data_output_exchanges:
            exchange.close()

    def start(self):
        self.notify_listener = threading.Thread(target=self._control_worker,)
        self.notify_listener.start()
        self.input_queue.start_consuming(self.process_data_messsage)
        
        self.stop()

    def stop(self):
        logging.info("Stopping SumFilter...")

        if self.notify_listener and self.notify_listener.is_alive():
            try:
                self.control_exchange_in.stop_consuming()
            except Exception as e:
                logging.error(e)
            self.notify_listener.join()

        self.control_exchange_out.close()
        self.input_queue.close()
    
def handle_sigterm(sum_filter: SumFilter):
    logging.info("SIGTERM received")
    try:
        sum_filter.input_queue.stop_consuming()
    except Exception as e:
        logging.error(e)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    signal.signal(signal.SIGTERM, lambda s, f: handle_sigterm(sum_filter))
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
