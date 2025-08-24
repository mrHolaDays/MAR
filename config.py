DB_VERSION = 1
DB_VERSION_BYTES = 3
STANDART_CORD_SIZE = 2
STANDART_LEN_SIZE = 2
STANDART_CORD_VALS = 3
CASES_IN_FILE = 10
MAX_TABLES_IN_BD_B = 2
MAX_CASES_IN_TABLE_B = 2
BYTES_PLASE_IN_FILE = 5
BASED_RESERV_SIZE = 10
CASES_DIR = "cases/"
import collections
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

TYPES_INFO = {
    # Стандартные типы
    str: ("STRS", b'\x01'),
    int: ("INTS", b'\x02'),
    float: ("FLOS", b'\x03'),
    bool: ("BOLS", b'\x04'),
    dict: ("DCTS", b'\x05'),
    list: ("LSTS", b'\x06'),
    tuple: ("TPLS", b'\x07'),
    set: ("SETS", b'\x08'),
    frozenset: ("FROS", b'\x09'),
    bytes: ("BYTS", b'\x0A'),
    bytearray: ("BARS", b'\x0B'),
    complex: ("CMPS", b'\x0C'),
    None.__class__: ("NONS", b'\x0D'),  # NoneType
    
    # collections
    collections.deque: ("DEQS", b'\x0E'),
    collections.defaultdict: ("DFDS", b'\x0F'),
    collections.OrderedDict: ("ORDS", b'\x10'),
    collections.Counter: ("CNTS", b'\x11'),
    collections.ChainMap: ("CHMS", b'\x12'),
    
    # array
    array.array: ("ARRS", b'\x13'),
    
    # datetime
    datetime.date: ("DATS", b'\x14'),
    datetime.datetime: ("DTMS", b'\x15'),
    datetime.time: ("TIMS", b'\x16'),
    datetime.timedelta: ("TDLS", b'\x17'),
    
    # decimal
    decimal.Decimal: ("DECS", b'\x18'),
    
    # uuid
    uuid.UUID: ("UUIS", b'\x19'),
    
    # re
    re.Pattern: ("REPS", b'\x1A'),
    re.Match: ("REMS", b'\x1B'),
    
    # io
    io.StringIO: ("STIO", b'\x1C'),
    io.BytesIO: ("BTIO", b'\x1D'),
    
    # pathlib
    pathlib.Path: ("PATS", b'\x1E'),
    
    # enum
    enum.Enum: ("ENMS", b'\x1F'),
    enum.Flag: ("FLGS", b'\x20'),
    
    # fractions
    fractions.Fraction: ("FRAS", b'\x21'),
    
    # memoryview
    memoryview: ("MEVS", b'\x22'),
    
    # weakref
    weakref.ref: ("REFS", b'\x23'),
    weakref.ProxyType: ("PRXS", b'\x24'),
    
    # types
    types.FunctionType: ("FUNS", b'\x25'),
    types.GeneratorType: ("GENS", b'\x26'),
    types.CoroutineType: ("CORS", b'\x27'),
    types.ModuleType: ("MODS", b'\x28'),
}


NAME_TO_TYPE = {
    # Стандартные типы
    "STRS": (str, b'\x01'),
    "INTS": (int, b'\x02'),
    "FLOS": (float, b'\x03'),
    "BOLS": (bool, b'\x04'),
    "DCTS": (dict, b'\x05'),
    "LSTS": (list, b'\x06'),
    "TPLS": (tuple, b'\x07'),
    "SETS": (set, b'\x08'),
    "FROS": (frozenset, b'\x09'),
    "BYTS": (bytes, b'\x0A'),
    "BARS": (bytearray, b'\x0B'),
    "CMPS": (complex, b'\x0C'),
    "NONS": (None.__class__, b'\x0D'),  # NoneType
    
    # collections
    "DEQS": (collections.deque, b'\x0E'),
    "DFDS": (collections.defaultdict, b'\x0F'),
    "ORDS": (collections.OrderedDict, b'\x10'),
    "CNTS": (collections.Counter, b'\x11'),
    "CHMS": (collections.ChainMap, b'\x12'),
    
    # array
    "ARRS": (array.array, b'\x13'),
    
    # datetime
    "DATS": (datetime.date, b'\x14'),
    "DTMS": (datetime.datetime, b'\x15'),
    "TIMS": (datetime.time, b'\x16'),
    "TDLS": (datetime.timedelta, b'\x17'),
    
    # decimal
    "DECS": (decimal.Decimal, b'\x18'),
    
    # uuid
    "UUIS": (uuid.UUID, b'\x19'),
    
    # re
    "REPS": (re.Pattern, b'\x1A'),
    "REMS": (re.Match, b'\x1B'),
    
    # io
    "STIO": (io.StringIO, b'\x1C'),
    "BTIO": (io.BytesIO, b'\x1D'),
    
    # pathlib
    "PATS": (pathlib.Path, b'\x1E'),
    
    # enum
    "ENMS": (enum.Enum, b'\x1F'),
    "FLGS": (enum.Flag, b'\x20'),
    
    # fractions
    "FRAS": (fractions.Fraction, b'\x21'),
    
    # memoryview
    "MEVS": (memoryview, b'\x22'),
    
    # weakref
    "REFS": (weakref.ref, b'\x23'),
    "PRXS": (weakref.ProxyType, b'\x24'),
    
    # types
    "FUNS": (types.FunctionType, b'\x25'),
    "GENS": (types.GeneratorType, b'\x26'),
    "CORS": (types.CoroutineType, b'\x27'),
    "MODS": (types.ModuleType, b'\x28'),
}

BYTES_TO_TYPE = {
    # Стандартные типы
    b'\x01': (str, "STRS"),
    b'\x02': (int, "INTS"),
    b'\x03': (float, "FLOS"),
    b'\x04': (bool, "BOLS"),
    b'\x05': (dict, "DCTS"),
    b'\x06': (list, "LSTS"),
    b'\x07': (tuple, "TPLS"),
    b'\x08': (set, "SETS"),
    b'\x09': (frozenset, "FROS"),
    b'\x0A': (bytes, "BYTS"),
    b'\x0B': (bytearray, "BARS"),
    b'\x0C': (complex, "CMPS"),
    b'\x0D': (None.__class__, "NONS"),  # NoneType
    
    # collections
    b'\x0E': (collections.deque, "DEQS"),
    b'\x0F': (collections.defaultdict, "DFDS"),
    b'\x10': (collections.OrderedDict, "ORDS"),
    b'\x11': (collections.Counter, "CNTS"),
    b'\x12': (collections.ChainMap, "CHMS"),
    
    # array
    b'\x13': (array.array, "ARRS"),
    
    # datetime
    b'\x14': (datetime.date, "DATS"),
    b'\x15': (datetime.datetime, "DTMS"),
    b'\x16': (datetime.time, "TIMS"),
    b'\x17': (datetime.timedelta, "TDLS"),
    
    # decimal
    b'\x18': (decimal.Decimal, "DECS"),
    
    # uuid
    b'\x19': (uuid.UUID, "UUIS"),
    
    # re
    b'\x1A': (re.Pattern, "REPS"),
    b'\x1B': (re.Match, "REMS"),
    
    # io
    b'\x1C': (io.StringIO, "STIO"),
    b'\x1D': (io.BytesIO, "BTIO"),
    
    # pathlib
    b'\x1E': (pathlib.Path, "PATS"),
    
    # enum
    b'\x1F': (enum.Enum, "ENMS"),
    b'\x20': (enum.Flag, "FLGS"),
    
    # fractions
    b'\x21': (fractions.Fraction, "FRAS"),
    
    # memoryview
    b'\x22': (memoryview, "MEVS"),
    
    # weakref
    b'\x23': (weakref.ref, "REFS"),
    b'\x24': (weakref.ProxyType, "PRXS"),
    
    # types
    b'\x25': (types.FunctionType, "FUNS"),
    b'\x26': (types.GeneratorType, "GENS"),
    b'\x27': (types.CoroutineType, "CORS"),
    b'\x28': (types.ModuleType, "MODS"),
}

