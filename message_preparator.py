import struct # для перевода типа BLOB
import json
from json import JSONEncoder


class Message: # обертка класса DataDB
    def __init__(self, field, bush, well, brigade, params):
        self.field = field
        self.bush = bush
        self.well = well
        self.brigade = brigade
        self.params = params


class DataDB: # класс для данных, получаемых из БД.
    def __init__(self, data_tuple, query_columns_list):
        data_list = list(data_tuple)
        if "nodeid" in query_columns_list:
            index = query_columns_list.index("nodeid")
            self.nodeid = data_list[index]
        if "sourcetime" in query_columns_list:
            index = query_columns_list.index("sourcetime")
            self.sourcetime = data_list[index]
        if "servertime" in query_columns_list:
            index = query_columns_list.index("servertime")
            self.servertime = data_list[index]
        if "value" in query_columns_list:
            index = query_columns_list.index("value")
            self.value, self.type = self.parse_value(data_list[index])
        if "status" in query_columns_list:
            index = query_columns_list.index("status")
            self.status = data_list[index]
        if "id" in query_columns_list:
            index = query_columns_list.index("id")
            self.id = data_list[index]
        if "type" in query_columns_list:
            index = query_columns_list.index("type")
            self.type = data_list[index]
        if "ns" in query_columns_list:
            index = query_columns_list.index("ns")
            self.ns = data_list[index]
        if "identifier" in query_columns_list:
            index = query_columns_list.index("identifier")
            self.identifier = data_list[index]


    def parse_value(self, value):
        hex_value = value.hex()
        num_type = hex_value[:2]
        num_value = hex_value[2:]
        if num_type == "01":
            return struct.unpack('<?', bytes.fromhex(num_value))[0], "Boolean"
        if num_type == "03":
            return struct.unpack('<c', bytes.fromhex(num_value))[0], "Byte"
        if num_type == "04":
            return struct.unpack('<h', bytes.fromhex(num_value))[0], "Int16"
        if num_type == "05":
            return struct.unpack('<H', bytes.fromhex(num_value))[0], "UInt16"
        if num_type == "06":
            return struct.unpack('<i', bytes.fromhex(num_value))[0], "Int32"
        if num_type == "07":
            return struct.unpack('<I', bytes.fromhex(num_value))[0], "UInt32"
        if num_type == "08":
            return struct.unpack('<q', bytes.fromhex(num_value))[0], "Int64"
        if num_type == "09":
            return struct.unpack('<Q', bytes.fromhex(num_value))[0], "UInt64"
        if num_type == "0a":
            return struct.unpack('<f', bytes.fromhex(num_value))[0], "Float"
        if num_type == "0c":
            return bytes.fromhex(num_value).decode("utf-8"), "String"
        else:
            return "", "" # TO-DO


class ObjectEncoder(JSONEncoder): # класс для сериализации любых объектов
    def default(self, o):
        return o.__dict__

def chunks(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]


def prepare_data(max_values_per_request, values, query_columns_list, field, bush, well, brigade): # конвертер данных в строки JSON
    values_list = []
    if values is not None:
        for index in values:
            object_value = DataDB(index, query_columns_list)
            values_list.append(object_value)

        values_list = list(chunks(values_list, max_values_per_request))
        messages_list = []
        for value in values_list:
            message = Message(field, bush, well, brigade, value)
            json_data = ObjectEncoder().encode(message)
            messages_list.append(json_data)
        return messages_list
    else:
        return [""]