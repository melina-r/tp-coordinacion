from uuid import uuid4

from common import message_protocol


class MessageHandler:

    def __init__(self):
        self.id = str(uuid4())

    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.id])

    def deserialize_result_message(self, message):
        uuid, top = message_protocol.internal.deserialize_top_message(message)

        if uuid != self.id:
            return None

        return top
