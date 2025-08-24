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
from config import STANDART_LEN_SIZE

# Словарь соответствия типов Python и их байтовых идентификаторов
PYTHON_TYPE_TO_BYTE = {
    str: b'\x01',
    int: b'\x02',
    float: b'\x03',
    bool: b'\x04',
    dict: b'\x05',
    list: b'\x06',
    tuple: b'\x07',
    set: b'\x08',
    frozenset: b'\x09',
    bytes: b'\x0A',
    bytearray: b'\x0B',
    complex: b'\x0C',
    type(None): b'\x0D',
    collections.deque: b'\x0E',
    collections.defaultdict: b'\x0F',
    collections.OrderedDict: b'\x10',
    collections.Counter: b'\x11',
    collections.ChainMap: b'\x12',
    array.array: b'\x13',
    datetime.date: b'\x14',
    datetime.datetime: b'\x15',
    datetime.time: b'\x16',
    datetime.timedelta: b'\x17',
    decimal.Decimal: b'\x18',
    uuid.UUID: b'\x19',
    re.Pattern: b'\x1A',
    re.Match: b'\x1B',
    io.StringIO: b'\x1C',
    io.BytesIO: b'\x1D',
    pathlib.Path: b'\x1E',
    enum.Enum: b'\x1F',
    enum.Flag: b'\x20',
    fractions.Fraction: b'\x21',
    memoryview: b'\x22',
    weakref.ref: b'\x23',
    weakref.ProxyType: b'\x24',
    types.FunctionType: b'\x25',
    types.GeneratorType: b'\x26',
    types.CoroutineType: b'\x27',
    types.ModuleType: b'\x28'
}

# Обратный словарь для десериализации
BYTE_TO_PYTHON_TYPE = {v: k for k, v in PYTHON_TYPE_TO_BYTE.items()}

def create_cord_block(cords, cord_size):
    """Создает блок координат из списка значений"""
    return b''.join(i.to_bytes(cord_size, byteorder='big', signed=True) for i in cords)

def serialize_data(data):
    """Сериализует данные любого поддерживаемого типа"""
    data_type = type(data)
    
    if data_type not in PYTHON_TYPE_TO_BYTE:
        raise TypeError(f"Неподдерживаемый тип: {data_type}")
    
    type_byte = PYTHON_TYPE_TO_BYTE[data_type]
    
    # Простые типы данных
    if data_type is str:
        encoded = data.encode('utf-8')
        return type_byte + len(encoded).to_bytes(STANDART_LEN_SIZE, 'big') + encoded
    elif data_type is int:
        # Для нуля используем 1 байт
        if data == 0:
            byte_repr = b'\x00'
        else:
            byte_repr = data.to_bytes((data.bit_length() + 7) // 8, 'big', signed=True)
        return type_byte + len(byte_repr).to_bytes(STANDART_LEN_SIZE, 'big') + byte_repr
    elif data_type is float:
        encoded = struct.pack('!d', data)
        return type_byte + len(encoded).to_bytes(STANDART_LEN_SIZE, 'big') + encoded
    elif data_type is bool:
        encoded = b'\x01' if data else b'\x00'
        return type_byte + len(encoded).to_bytes(STANDART_LEN_SIZE, 'big') + encoded
    elif data_type is bytes:
        return type_byte + len(data).to_bytes(STANDART_LEN_SIZE, 'big') + data
    elif data_type is bytearray:
        return type_byte + len(data).to_bytes(STANDART_LEN_SIZE, 'big') + bytes(data)
    elif data_type is complex:
        encoded = struct.pack('!dd', data.real, data.imag)
        return type_byte + len(encoded).to_bytes(STANDART_LEN_SIZE, 'big') + encoded
    elif data is None:
        return type_byte + b'\x00' * STANDART_LEN_SIZE
    
    # Коллекции
    elif data_type in (list, tuple, set, frozenset, collections.deque):
        items = b''.join(serialize_data(item) for item in data)
        return type_byte + len(items).to_bytes(STANDART_LEN_SIZE, 'big') + items
    elif data_type in (dict, collections.defaultdict, collections.OrderedDict, collections.Counter):
        items = b''.join(serialize_data(k) + serialize_data(v) for k, v in data.items())
        return type_byte + len(items).to_bytes(STANDART_LEN_SIZE, 'big') + items
    elif data_type is collections.ChainMap:
        items = serialize_data(list(data.maps))
        return type_byte + len(items).to_bytes(STANDART_LEN_SIZE, 'big') + items
    
    # Специальные типы
    elif data_type is array.array:
        return type_byte + len(data).to_bytes(STANDART_LEN_SIZE, 'big') + data.tobytes()
    elif data_type in (datetime.date, datetime.datetime, datetime.time):
        encoded = data.isoformat().encode('utf-8')
        return type_byte + len(encoded).to_bytes(STANDART_LEN_SIZE, 'big') + encoded
    elif data_type is datetime.timedelta:
        encoded = struct.pack('!d', data.total_seconds())
        return type_byte + len(encoded).to_bytes(STANDART_LEN_SIZE, 'big') + encoded
    elif data_type is decimal.Decimal:
        encoded = str(data).encode('utf-8')
        return type_byte + len(encoded).to_bytes(STANDART_LEN_SIZE, 'big') + encoded
    elif data_type is uuid.UUID:
        return type_byte + len(data.bytes).to_bytes(STANDART_LEN_SIZE, 'big') + data.bytes
    elif data_type in (re.Pattern, re.Match, types.FunctionType, types.GeneratorType, types.CoroutineType):
        encoded = pickle.dumps(data)
        return type_byte + len(encoded).to_bytes(STANDART_LEN_SIZE, 'big') + encoded
    elif data_type in (io.StringIO, io.BytesIO):
        encoded = serialize_data(data.getvalue())
        return type_byte + len(encoded).to_bytes(STANDART_LEN_SIZE, 'big') + encoded
    elif data_type is pathlib.Path:
        encoded = str(data).encode('utf-8')
        return type_byte + len(encoded).to_bytes(STANDART_LEN_SIZE, 'big') + encoded
    elif isinstance(data, (enum.Enum, enum.Flag)):
        encoded = serialize_data(data.value)
        return type_byte + len(encoded).to_bytes(STANDART_LEN_SIZE, 'big') + encoded
    elif data_type is fractions.Fraction:
        encoded = serialize_data(data.numerator) + serialize_data(data.denominator)
        return type_byte + len(encoded).to_bytes(STANDART_LEN_SIZE, 'big') + encoded
    elif data_type is memoryview:
        return type_byte + len(data).to_bytes(STANDART_LEN_SIZE, 'big') + data.tobytes()
    elif data_type is weakref.ref:
        encoded = serialize_data(data())
        return type_byte + len(encoded).to_bytes(STANDART_LEN_SIZE, 'big') + encoded
    elif data_type is weakref.ProxyType:
        encoded = serialize_data(data)
        return type_byte + len(encoded).to_bytes(STANDART_LEN_SIZE, 'big') + encoded
    elif data_type is types.ModuleType:
        encoded = data.__name__.encode('utf-8')
        return type_byte + len(encoded).to_bytes(STANDART_LEN_SIZE, 'big') + encoded
    
    raise TypeError(f"Сериализация не реализована для типа: {data_type}")

def deserialize_data(data, data_type):
    """Десериализует данные из байтового представления"""
    # Простые типы данных
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
    elif data_type is type(None):
        return None
    
    # Коллекции
    elif data_type in (list, tuple, set, frozenset, collections.deque):
        items = []
        offset = 0
        while offset < len(data):
            item_type_byte = data[offset:offset+1]
            item_type = BYTE_TO_PYTHON_TYPE[item_type_byte]
            item_len = int.from_bytes(data[offset+1:offset+1+STANDART_LEN_SIZE], 'big')
            item_data = data[offset+1+STANDART_LEN_SIZE:offset+1+STANDART_LEN_SIZE+item_len]
            items.append(deserialize_data(item_data, item_type))
            offset += 1 + STANDART_LEN_SIZE + item_len
        
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
        result = {}
        offset = 0
        while offset < len(data):
            # Ключ
            key_type_byte = data[offset:offset+1]
            key_type = BYTE_TO_PYTHON_TYPE[key_type_byte]
            key_len = int.from_bytes(data[offset+1:offset+1+STANDART_LEN_SIZE], 'big')
            key_data = data[offset+1+STANDART_LEN_SIZE:offset+1+STANDART_LEN_SIZE+key_len]
            key = deserialize_data(key_data, key_type)
            offset += 1 + STANDART_LEN_SIZE + key_len
            
            # Значение
            val_type_byte = data[offset:offset+1]
            val_type = BYTE_TO_PYTHON_TYPE[val_type_byte]
            val_len = int.from_bytes(data[offset+1:offset+1+STANDART_LEN_SIZE], 'big')
            val_data = data[offset+1+STANDART_LEN_SIZE:offset+1+STANDART_LEN_SIZE+val_len]
            value = deserialize_data(val_data, val_type)
            offset += 1 + STANDART_LEN_SIZE + val_len
            
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
        maps_data = deserialize_data(data, list)
        return collections.ChainMap(*maps_data)
    
    # Специальные типы
    elif data_type is array.array:
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
    elif data_type in (re.Pattern, re.Match, types.FunctionType, types.GeneratorType, types.CoroutineType):
        return pickle.loads(data)
    elif data_type is io.StringIO:
        return io.StringIO(deserialize_data(data, str))
    elif data_type is io.BytesIO:
        return io.BytesIO(deserialize_data(data, bytes))
    elif data_type is pathlib.Path:
        return pathlib.Path(data.decode('utf-8'))
    elif data_type in (enum.Enum, enum.Flag):
        # Для enum требуется дополнительная информация о классе
        return deserialize_data(data, int)  # Упрощенная версия
    elif data_type is fractions.Fraction:
        # Данные должны быть разделены на числитель и знаменатель
        half_len = len(data) // 2
        numerator = deserialize_data(data[:half_len], int)
        denominator = deserialize_data(data[half_len:], int)
        return fractions.Fraction(numerator, denominator)
    elif data_type is memoryview:
        return memoryview(data)
    elif data_type is weakref.ref:
        obj = deserialize_data(data, object)
        return weakref.ref(obj)
    elif data_type is weakref.ProxyType:
        obj = deserialize_data(data, object)
        return weakref.proxy(obj)
    elif data_type is types.ModuleType:
        module_name = data.decode('utf-8')
        return types.ModuleType(module_name)
    
    raise TypeError(f"Десериализация не реализована для типа: {data_type}")

def create_case(cords, data, cord_size, reserved_size):
    """Создает запись с координатами и данными"""
    return b'\xf8' + create_cord_block(cords, cord_size) + serialize_data(data) + b'\x00' * reserved_size

def unpack_case(case_data, cord_size, cord_vals):
    """Распаковывает запись на составляющие"""
    if case_data[0] == 0xf8:
        case_data = case_data[1:]
    
    # Извлекаем координаты
    cords = []
    for i in range(cord_vals):
        cord = int.from_bytes(case_data[:cord_size], byteorder='big', signed=True)
        cords.append(cord)
        case_data = case_data[cord_size:]
    
    # Извлекаем тип данных
    type_byte = case_data[:1]
    data_type = BYTE_TO_PYTHON_TYPE[type_byte]
    case_data = case_data[1:]
    
    # Извлекаем длину данных
    data_len = int.from_bytes(case_data[:STANDART_LEN_SIZE], 'big')
    case_data = case_data[STANDART_LEN_SIZE:]
    
    # Извлекаем и десериализуем данные
    data = deserialize_data(case_data[:data_len], data_type)
    case_data = case_data[data_len:]
    
    # Вычисляем размер резервного пространства
    reserved_size = 0
    for byte in case_data:
        if byte == 0:
            reserved_size += 1
        else:
            break
    
    return cords, data_type, data_len, data, reserved_size