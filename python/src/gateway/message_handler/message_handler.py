import uuid
from common.message_protocol.internal import FruMessage, MsgType

class MessageHandler:

    def __init__(self):
        self.client_id = uuid.uuid4().hex
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        fru_msg = FruMessage(self.client_id, MsgType.FRUIT_RECORD, [fruit, amount])
        return fru_msg.serialize()

    def serialize_eof_message(self, message):
        fru_msg = FruMessage(self.client_id, MsgType.END_OF_RECODS, [])
        return fru_msg.serialize()

    def deserialize_result_message(self, message):
        fru_msg = FruMessage.deserialize(message)
        if fru_msg.client_id == self.client_id:
            return fru_msg.data
        return None
