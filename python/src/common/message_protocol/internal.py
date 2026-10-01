from enum import Enum

AMOUNT_SIZE = 4
LENGTH_SIZE = 2
TYPE_SIZE = 1

class FieldType(Enum):
    EndOfMessage = 0
    FruitName = 1
    FruitAmount = 2

    def to_bytes(self):
        return self.value.to_bytes(TYPE_SIZE, "big")

def serialize_fruit_name(fruit_name):
    bytes_value = fruit_name.encode("utf-8")
    length = len(bytes_value)
    data = b""
    data += FieldType.FruitName.to_bytes()
    data += length.to_bytes(LENGTH_SIZE, "big")
    data += bytes_value
    return data

def serialize_fruit_amount(fruit_amount):
    bytes_value = fruit_amount.to_bytes(AMOUNT_SIZE, "big")
    length = len(bytes_value)
    data = b""
    data += FieldType.FruitAmount.to_bytes()
    data += length.to_bytes(LENGTH_SIZE, "big")
    data += bytes_value
    return data


def serialize_uuid(uuid):
    bytes_value = uuid.encode("utf-8")
    length = len(bytes_value)
    data = b""
    data += FieldType.FruitName.to_bytes()
    data += length.to_bytes(LENGTH_SIZE, "big")
    data += bytes_value
    return data


def serialize_eof_message(message):
    uuid_data = serialize_uuid(message[0])
    total_length = len(uuid_data)
    data = b""
    data += total_length.to_bytes(LENGTH_SIZE, "big")
    data += uuid_data
    return data


def serialize(message):
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
    uuid, top_list = message
    uuid_data = serialize_uuid(uuid)
    data = b""
    data += uuid_data
    for fruit, amount in top_list:
        fruit_data = serialize_fruit_name(fruit)
        amount_data = serialize_fruit_amount(amount)
        data += fruit_data
        data += amount_data
    return data

def deserialize_fruit_name(data):
    length = int.from_bytes(data[TYPE_SIZE:TYPE_SIZE + LENGTH_SIZE], "big")
    fruit_name = data[TYPE_SIZE + LENGTH_SIZE : TYPE_SIZE + LENGTH_SIZE + length].decode("utf-8")
    return fruit_name, TYPE_SIZE + LENGTH_SIZE + length

def deserialize_fruit_amount(data):
    length = int.from_bytes(data[TYPE_SIZE:TYPE_SIZE + LENGTH_SIZE], "big")
    fruit_amount = int.from_bytes(data[TYPE_SIZE + LENGTH_SIZE : TYPE_SIZE + LENGTH_SIZE + length], "big")
    return fruit_amount, TYPE_SIZE + LENGTH_SIZE + length

def deserialize_uuid(data):
    length = int.from_bytes(data[TYPE_SIZE:TYPE_SIZE + LENGTH_SIZE], "big")
    uuid = data[TYPE_SIZE + LENGTH_SIZE : TYPE_SIZE + LENGTH_SIZE + length].decode("utf-8")
    return uuid, TYPE_SIZE + LENGTH_SIZE + length

def deserialize(message):
    total_length = int.from_bytes(message[:LENGTH_SIZE], "big")
    data = message[LENGTH_SIZE:LENGTH_SIZE + total_length]
    uuid, offset = deserialize_fruit_name(data)
    if offset >= len(data):
        return [uuid]

    fruit_name, fruit_offset = deserialize_fruit_name(data[offset:])
    offset += fruit_offset
    fruit_amount, _ = deserialize_fruit_amount(data[offset:])
    return uuid, fruit_name, fruit_amount

def deserialize_top_message(message):
    uuid, offset = deserialize_uuid(message)
    top_list = []
    while offset < len(message):
        fruit_name, fruit_offset = deserialize_fruit_name(message[offset:])
        offset += fruit_offset
        fruit_amount, amount_offset = deserialize_fruit_amount(message[offset:])
        offset += amount_offset
        top_list.append((fruit_name, fruit_amount))
    return uuid, top_list
