from config import STANDART_CORD_SIZE, STANDART_LEN_SIZE, TYPES_INFO, BASED_RESERV_SIZE, STANDART_CORD_VALS, BYTES_TO_TYPE, NAME_TO_TYPE
import collections
import struct
import pickle
import array
import datetime
import decimal
import uuid
import re
import io
import pathlib
import enum
import fractions
import weakref
import types
def create_cord_block(cord, cord_size=STANDART_CORD_SIZE):
    return b''.join([i.to_bytes(cord_size, byteorder='big', signed=True) for i in cord])
def serialize_data(data, LEN_SIZE = STANDART_LEN_SIZE):
    # Определяем тип данных
    data_type = type(data)
    
    if data_type not in TYPES_INFO:
        raise TypeError(f"Unsupported type: {data_type}")
    
    type_code, type_byte = TYPES_INFO[data_type]
    
    # Для простых типов
    if data_type is str:
        encoded = data.encode('utf-8')
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    elif data_type is int:
        byte_repr = data.to_bytes((data.bit_length() + 7) // 8, 'big', signed=True)
        return type_byte + len(byte_repr).to_bytes(LEN_SIZE, 'big') + byte_repr
    elif data_type is float:
        encoded = struct.pack('!d', data)
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    elif data_type is bool:
        encoded = b'\x01' if data else b'\x00'
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    elif data_type is bytes:
        return type_byte + len(data).to_bytes(LEN_SIZE, 'big') + data
    elif data_type is bytearray:
        byte_repr = bytes(data)
        return type_byte + len(byte_repr).to_bytes(LEN_SIZE, 'big') + byte_repr
    elif data_type is complex:
        encoded = struct.pack('!dd', data.real, data.imag)
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    elif data is None:
        return type_byte + (0).to_bytes(LEN_SIZE, 'big')
    
    # Для коллекций
    elif data_type in (list, tuple, set, frozenset, collections.deque):
        items = b''.join(serialize_data(item) for item in data)
        length = len(data).to_bytes(LEN_SIZE, 'big')
        return type_byte + len(length + items).to_bytes(LEN_SIZE, 'big') + length + items
    elif data_type in (dict, collections.defaultdict, collections.OrderedDict, collections.Counter):
        items = b''.join(serialize_data(k) + serialize_data(v) for k, v in data.items())
        length = len(data).to_bytes(LEN_SIZE, 'big')
        return type_byte + len(length + items).to_bytes(LEN_SIZE, 'big') + length + items
    elif data_type is collections.ChainMap:
        encoded = serialize_data(list(data.maps))
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    
    # Специальные типы
    elif data_type is array.array:
        byte_repr = data.tobytes()
        return type_byte + len(byte_repr).to_bytes(LEN_SIZE, 'big') + byte_repr
    elif data_type in (datetime.date, datetime.datetime, datetime.time):
        encoded = data.isoformat().encode('utf-8')
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    elif data_type is datetime.timedelta:
        encoded = struct.pack('!d', data.total_seconds())
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    elif data_type is decimal.Decimal:
        encoded = str(data).encode('utf-8')
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    elif data_type is uuid.UUID:
        byte_repr = data.bytes
        return type_byte + len(byte_repr).to_bytes(LEN_SIZE, 'big') + byte_repr
    elif data_type is re.Pattern:
        encoded = pickle.dumps(data)
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    elif data_type is re.Match:
        encoded = pickle.dumps(data)
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    elif data_type in (io.StringIO, io.BytesIO):
        encoded = serialize_data(data.getvalue())
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    elif data_type is pathlib.Path:
        encoded = str(data).encode('utf-8')
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    elif isinstance(data, (enum.Enum, enum.Flag)):
        encoded = serialize_data(data.value)
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    elif data_type is fractions.Fraction:
        encoded = serialize_data(data.numerator) + serialize_data(data.denominator)
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    elif data_type is memoryview:
        byte_repr = data.tobytes()
        return type_byte + len(byte_repr).to_bytes(LEN_SIZE, 'big') + byte_repr
    elif data_type is weakref.ref:
        encoded = serialize_data(data())
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    elif data_type is weakref.ProxyType:
        encoded = serialize_data(data)
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    elif data_type is types.FunctionType:
        encoded = pickle.dumps(data)
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    elif data_type is types.GeneratorType:
        encoded = pickle.dumps(data)
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    elif data_type is types.CoroutineType:
        encoded = pickle.dumps(data)
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    elif data_type is types.ModuleType:
        encoded = data.__name__.encode('utf-8')
        return type_byte + len(encoded).to_bytes(LEN_SIZE, 'big') + encoded
    
    # Если тип не обработан
    raise TypeError(f"Serialization not implemented for type: {data_type}")


def create_case(cords, data, reserved_size=BASED_RESERV_SIZE):
    return b'\xf8'+create_cord_block(cords)+serialize_data(data)+b'\x00'*reserved_size

def unpuk_cords(data, cord_size=STANDART_CORD_SIZE, cord_val=STANDART_CORD_VALS):
    if data[0] == 248:
        data= data[1:]
    cord_block = []
    for i in range(cord_val):
        cord_block.append(int.from_bytes(data[:cord_size],byteorder='big', signed=True))
        data=data[cord_size:]
    return  cord_block, cord_size*cord_val
def unpuk_data(data, data_type):
    # Для простых типов
    if data_type is str:
        return data.decode('utf-8')
    elif data_type is int:
        return int.from_bytes(data, byteorder='big', signed=True)
    elif data_type is float:
        return struct.unpack('!d', data)[0]
    elif data_type is bool:
        return data == b'\x01'
    elif data_type is bytes:
        return data
    elif data_type is bytearray:
        return bytearray(data)
    elif data_type is complex:
        real, imag = struct.unpack('!dd', data)
        return complex(real, imag)
    elif data_type is None.__class__:
        return None
    
    # Для коллекций
    elif data_type in (list, tuple, set, frozenset, collections.deque):
        length = int.from_bytes(data[:STANDART_LEN_SIZE], 'big')
        data = data[STANDART_LEN_SIZE:]
        items = []
        while len(data) > 0:
            item_type_byte = data[:1]
            item_type = BYTES_TO_TYPE[item_type_byte][0]
            item_len = int.from_bytes(data[1:1+STANDART_LEN_SIZE], 'big')
            item_data = data[1+STANDART_LEN_SIZE:1+STANDART_LEN_SIZE+item_len]
            items.append(unpuk_data(item_data, item_type))
            data = data[1+STANDART_LEN_SIZE+item_len:]
        
        if data_type is list:
            return items
        elif data_type is tuple:
            return tuple(items)
        elif data_type is set:
            return set(items)
        elif data_type is frozenset:
            return frozenset(items)
        elif data_type is collections.deque:
            return collections.deque(items)
    
    elif data_type in (dict, collections.defaultdict, collections.OrderedDict, collections.Counter):
        length = int.from_bytes(data[:STANDART_LEN_SIZE], 'big')
        data = data[STANDART_LEN_SIZE:]
        result = {}
        while len(data) > 0:
            # Десериализация ключа
            key_type_byte = data[:1]
            key_type = BYTES_TO_TYPE[key_type_byte][0]
            key_len = int.from_bytes(data[1:1+STANDART_LEN_SIZE], 'big')
            key_data = data[1+STANDART_LEN_SIZE:1+STANDART_LEN_SIZE+key_len]
            key = unpuk_data(key_data, key_type)
            data = data[1+STANDART_LEN_SIZE+key_len:]
            
            # Десериализация значения
            val_type_byte = data[:1]
            val_type = BYTES_TO_TYPE[val_type_byte][0]
            val_len = int.from_bytes(data[1:1+STANDART_LEN_SIZE], 'big')
            val_data = data[1+STANDART_LEN_SIZE:1+STANDART_LEN_SIZE+val_len]
            value = unpuk_data(val_data, val_type)
            data = data[1+STANDART_LEN_SIZE+val_len:]
            
            result[key] = value
        
        if data_type is dict:
            return result
        elif data_type is collections.defaultdict:
            return collections.defaultdict(None, result)
        elif data_type is collections.OrderedDict:
            return collections.OrderedDict(result)
        elif data_type is collections.Counter:
            return collections.Counter(result)
    
    elif data_type is collections.ChainMap:
        maps_data = unpuk_data(data, list)
        return collections.ChainMap(*maps_data)
    
    # Специальные типы
    elif data_type is array.array:
        # Для array.array нужно знать тип, но в сериализации эта информация не сохраняется
        # Здесь предполагается, что это массив байтов (тип 'B')
        return array.array('B', data)
    elif data_type is datetime.date:
        return datetime.date.fromisoformat(data.decode('utf-8'))
    elif data_type is datetime.datetime:
        return datetime.datetime.fromisoformat(data.decode('utf-8'))
    elif data_type is datetime.time:
        return datetime.time.fromisoformat(data.decode('utf-8'))
    elif data_type is datetime.timedelta:
        seconds = struct.unpack('!d', data)[0]
        return datetime.timedelta(seconds=seconds)
    elif data_type is decimal.Decimal:
        return decimal.Decimal(data.decode('utf-8'))
    elif data_type is uuid.UUID:
        return uuid.UUID(bytes=data)
    elif data_type is re.Pattern:
        return pickle.loads(data)
    elif data_type is re.Match:
        return pickle.loads(data)
    elif data_type is io.StringIO:
        return io.StringIO(unpuk_data(data, str))
    elif data_type is io.BytesIO:
        return io.BytesIO(unpuk_data(data, bytes))
    elif data_type is pathlib.Path:
        return pathlib.Path(data.decode('utf-8'))
    elif data_type in (enum.Enum, enum.Flag):
        # Для enum нужно знать конкретный класс enum, что сложно без дополнительной информации
        # Здесь просто возвращаем значение
        return unpuk_data(data, type(unpuk_data(data, object)))
    elif data_type is fractions.Fraction:
        # Данные должны быть последовательностью из двух элементов: числитель и знаменатель
        numerator_data = data[:len(data)//2]
        denominator_data = data[len(data)//2:]
        numerator = unpuk_data(numerator_data, int)
        denominator = unpuk_data(denominator_data, int)
        return fractions.Fraction(numerator, denominator)
    elif data_type is memoryview:
        return memoryview(data)
    elif data_type is weakref.ref:
        obj = unpuk_data(data, object)
        return weakref.ref(obj)
    elif data_type is weakref.ProxyType:
        obj = unpuk_data(data, object)
        return weakref.proxy(obj)
    elif data_type is types.FunctionType:
        return pickle.loads(data)
    elif data_type is types.GeneratorType:
        return pickle.loads(data)
    elif data_type is types.CoroutineType:
        return pickle.loads(data)
    elif data_type is types.ModuleType:
        module_name = data.decode('utf-8')
        return types.ModuleType(module_name)
    
    # Если тип не обработан
    raise TypeError(f"Deserialization not implemented for type: {data_type}")
def un_puck_case(case_inp, len_b = STANDART_LEN_SIZE, cords_val=STANDART_CORD_VALS):
    if case_inp[0] == 248:
        case_inp= case_inp[1:]
    f = unpuk_cords(case_inp, cord_val=cords_val)
    case_inp=case_inp[f[1]:]
    type_data = BYTES_TO_TYPE[case_inp[:1]][0]
    case_inp= case_inp[1:]
    data_len = int.from_bytes(case_inp[:len_b],byteorder='big', signed=False)
    case_inp = case_inp[len_b:]
    data = unpuk_data(case_inp[:data_len], type_data)
    case_inp = case_inp[data_len:]
    reversed_size = 0
    for i in case_inp:
        if i == 0:
            reversed_size+= 1
        else:
            break
    
    return f, type_data, data_len, data, reversed_size

