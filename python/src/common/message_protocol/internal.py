import json

class MsgType:
    FRUIT_RECORD = 1
    FRUIT_TOP = 2
    ACK = 3
    END_OF_RECODS = 4

class FruMessage:
    def __init__(self, client_id: str, msg_type: MsgType, data):
        self.client_id = client_id
        self.msg_type = msg_type
        self.data = data

    def to_dict(self):
        return {
            "client_id": self.client_id,
            "msg_type": self.msg_type,
            "data": self.data
        }
    
    @staticmethod
    def from_dict(fields):
        return FruMessage(fields["client_id"], fields["msg_type"], fields["data"])
    
    def serialize(self):
        return serialize(self.to_dict())
    
    @staticmethod
    def deserialize(message):
        return FruMessage.from_dict(deserialize(message))

def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))
