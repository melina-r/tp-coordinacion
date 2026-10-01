from enum import Enum

AMOUNT_SIZE = 4
LENGTH_SIZE = 2
TYPE_SIZE = 1


class FieldType(Enum):
    EndOfMessage = 0
    FruitName = 1
    FruitAmount = 2
    Uuid = 3

    def to_bytes(self):
        return self.value.to_bytes(TYPE_SIZE, "big")


def serialize_fruit_name(fruit_name: str):
    """ Serialize a fruit name into bytes with a type and length prefix. """
    bytes_value = fruit_name.encode("utf-8")
    length = len(bytes_value)
    data = b""
    data += FieldType.FruitName.to_bytes()
    data += length.to_bytes(LENGTH_SIZE, "big")
    data += bytes_value
    return data


def serialize_fruit_amount(fruit_amount: int):
    """ Serialize a fruit amount into bytes with a type and length prefix. """
    bytes_value = fruit_amount.to_bytes(AMOUNT_SIZE, "big")
    length = len(bytes_value)
    data = b""
    data += FieldType.FruitAmount.to_bytes()
    data += length.to_bytes(LENGTH_SIZE, "big")
    data += bytes_value
    return data


def serialize_uuid(uuid: str):
    """ Serialize a UUID into bytes with a type and length prefix. """
    bytes_value = uuid.encode("utf-8")
    length = len(bytes_value)
    data = b""
    data += FieldType.Uuid.to_bytes()
    data += length.to_bytes(LENGTH_SIZE, "big")
    data += bytes_value
    return data


def serialize_eof_message(message):
    """ Serialize an EOF message into bytes with a type and length prefix. """
    uuid_data = serialize_uuid(message[0])
    eof_data = FieldType.EndOfMessage.to_bytes() + (0).to_bytes(LENGTH_SIZE, "big")
    total_length = len(uuid_data) + len(eof_data)
    data = b""
    data += total_length.to_bytes(LENGTH_SIZE, "big")
    data += uuid_data + eof_data
    return data


def serialize(message):
    """ Serialize a message into bytes. 
    The message can be either a data message (uuid, fruit, amount) or an EOF message (uuid).
    
    It returns the serialized bytes representation of the message with a total length prefix.
    """
    if len(message) == 1:
        return serialize_eof_message(message)

    uuid, fruit, amount = message
    uuid_data = serialize_uuid(uuid)
    fruit_data = serialize_fruit_name(fruit)
    amount_data = serialize_fruit_amount(amount)
    total_length = len(fruit_data) + len(amount_data) + len(uuid_data)
    data = b""
    data += total_length.to_bytes(LENGTH_SIZE, "big")
    data += uuid_data
    data += fruit_data
    data += amount_data
    return data


def serialize_top_message(message):
    """ Serialize a top message into bytes.
    The message is a tuple containing a UUID and a list of tuples, where each tuple contains a FruitItem.

    It returns the serialized bytes representation of the top message with a total length prefix.
    """
    uuid, top_list = message
    uuid_data = serialize_uuid(uuid)
    data = b""
    data += uuid_data
    for fruit in top_list:
        fruit_data = serialize_fruit_name(fruit.fruit)
        amount_data = serialize_fruit_amount(fruit.amount)
        data += fruit_data
        data += amount_data
    total_length = len(data)
    return b"" + total_length.to_bytes(LENGTH_SIZE, "big") + data


def deserialize_fruit_name(data):
    """ Deserialize a fruit name from bytes with a type and length prefix.

    It returns a tuple containing the fruit name and the total number of bytes consumed.
    """
    type_value = data[0]
    if type_value != FieldType.FruitName.value:
        raise ValueError(f"Expected FruitName type, got {type_value}")

    length = int.from_bytes(data[TYPE_SIZE : TYPE_SIZE + LENGTH_SIZE], "big")
    fruit_name = data[
        TYPE_SIZE + LENGTH_SIZE : TYPE_SIZE + LENGTH_SIZE + length
    ].decode("utf-8")
    return fruit_name, TYPE_SIZE + LENGTH_SIZE + length


def deserialize_fruit_amount(data):
    """ Deserialize a fruit amount from bytes with a type and length prefix.

    It returns a tuple containing the fruit amount and the total number of bytes consumed.
    """
    type_value = data[0]
    if type_value != FieldType.FruitAmount.value:
        raise ValueError(f"Expected FruitAmount type, got {type_value}")
    length = int.from_bytes(data[TYPE_SIZE : TYPE_SIZE + LENGTH_SIZE], "big")
    fruit_amount = int.from_bytes(
        data[TYPE_SIZE + LENGTH_SIZE : TYPE_SIZE + LENGTH_SIZE + length], "big"
    )
    return fruit_amount, TYPE_SIZE + LENGTH_SIZE + length


def deserialize_uuid(data):
    """ Deserialize a UUID from bytes with a type and length prefix.

    It returns a tuple containing the UUID and the total number of bytes consumed.
    """
    type_value = data[0]
    if type_value != FieldType.Uuid.value:
        raise ValueError(f"Expected Uuid type, got {type_value}")
    length = int.from_bytes(data[TYPE_SIZE : TYPE_SIZE + LENGTH_SIZE], "big")
    uuid = data[TYPE_SIZE + LENGTH_SIZE : TYPE_SIZE + LENGTH_SIZE + length].decode(
        "utf-8"
    )
    return uuid, TYPE_SIZE + LENGTH_SIZE + length


def deserialize(message):
    """ Deserialize a message from bytes.
    The message can be either a data message (uuid, fruit, amount) or an EOF message (uuid).

    It returns a list containing the deserialized fields of the message.
    """
    total_length = int.from_bytes(message[:LENGTH_SIZE], "big")
    data = message[LENGTH_SIZE : LENGTH_SIZE + total_length]
    uuid, offset = deserialize_uuid(data)
    if offset == len(data):
        return [uuid]

    if (
        data[offset] == FieldType.EndOfMessage.value
        and int.from_bytes(data[offset + TYPE_SIZE :], "big") == 0
    ):
        return [uuid]

    fruit_name, fruit_offset = deserialize_fruit_name(data[offset:])
    offset += fruit_offset
    fruit_amount, _ = deserialize_fruit_amount(data[offset:])
    return [uuid, fruit_name, fruit_amount]


def deserialize_top_message(message):
    """ Deserialize a top message from bytes.
    The message is a tuple containing a UUID and a list of tuples, where each tuple contains a FruitItem.

    It returns a tuple containing the UUID and a list of tuples, where each tuple contains a fruit name and amount.
    """
    total_length = int.from_bytes(message[:LENGTH_SIZE], "big")
    data = message[LENGTH_SIZE : LENGTH_SIZE + total_length]
    uuid, offset = deserialize_uuid(data)
    top_list = []
    while offset < len(data):
        fruit_name, fruit_offset = deserialize_fruit_name(data[offset:])
        offset += fruit_offset
        fruit_amount, amount_offset = deserialize_fruit_amount(data[offset:])
        offset += amount_offset
        top_list.append((fruit_name, fruit_amount))
    return uuid, top_list
