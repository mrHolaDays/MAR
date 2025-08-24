import os
import struct
from config import *
from file_operations import create_cases_file, write_case_to_file, find_case_in_file, read_all_cases

def create_database(db_name):
    """Создает новую базу данных"""
    with open(db_name, "wb") as f:
        f.write(DB_VERSION.to_bytes(DB_VERSION_BYTES, 'big'))
        f.write(b'\xf8')
        f.write(CASES_DIR.encode('utf-8'))
        f.write(b'\xfa')
        f.write(b'\x00' * 24)  # Резервное пространство для конфигурации
        f.write(b'\xfa')
        f.write(b'\x00' * MAX_TABLES_IN_BD_B)  # Место для количества таблиц

def create_table(db_name, table_name, columns):
    """Создает новую таблицу в базе данных"""
    # Читаем текущее количество таблиц
    with open(db_name, "rb+") as f:
        f.seek(DB_VERSION_BYTES + 1)
        
        # Пропускаем путь к директории
        while f.read(1) != b'\xfa':
            pass
        
        # Пропускаем конфигурацию
        f.seek(24, 1)
        f.read(1)  # Пропускаем разделитель
        
        # Читаем количество таблиц
        table_count = int.from_bytes(f.read(MAX_TABLES_IN_BD_B), 'big')
        new_table_count = table_count + 1
        
        # Обновляем количество таблиц
        f.seek(-MAX_TABLES_IN_BD_B, 1)
        f.write(new_table_count.to_bytes(MAX_TABLES_IN_BD_B, 'big'))
        
        # Добавляем информацию о новой таблице
        f.seek(0, 2)
        f.write(new_table_count.to_bytes(MAX_TABLES_IN_BD_B, 'big'))
        f.write(table_name.encode('utf-8'))
        f.write(b'\xfa')
        f.write(len(columns).to_bytes(MAX_CASES_IN_TABLE_B, 'big'))
        
        for i, column in enumerate(columns):
            f.write(i.to_bytes(MAX_CASES_IN_TABLE_B, 'big'))
            f.write(column.encode('utf-8'))
            f.write(b'\xfa')
    
    # Создаем файл для хранения данных таблицы
    if not os.path.exists('config'):
        os.makedirs('config')
    
    table_file = f"{table_name}_1.marc"
    create_cases_file(table_file, new_table_count, len(columns))
    
    # Создаем конфигурационный файл таблицы
    with open(f"config/{table_name}.mart", "wb") as f:
        f.write(new_table_count.to_bytes(MAX_TABLES_IN_BD_B, 'big'))
        f.write(table_file.encode('utf-8'))
        f.write(b'\xfa')

def parse_database(db_name):
    """Парсит основную информацию о базе данных"""
    with open(db_name, "rb") as f:
        version = int.from_bytes(f.read(DB_VERSION_BYTES), 'big')
        
        # Читаем путь к директории с данными
        f.read(1)  # Пропускаем разделитель
        cases_dir = b''
        while True:
            byte = f.read(1)
            if byte == b'\xfa':
                break
            cases_dir += byte
        
        # Читаем конфигурацию
        config = f.read(24)
        f.read(1)  # Пропускаем разделитель
        
        # Читаем количество таблиц
        table_count = int.from_bytes(f.read(MAX_TABLES_IN_BD_B), 'big')
        tables_pos = f.tell()
        
        return {
            'version': version,
            'cases_dir': cases_dir.decode('utf-8'),
            'config': config,
            'table_count': table_count,
            'tables_pos': tables_pos
        }

def get_tables(db_name):
    """Возвращает информацию о всех таблицах в базе данных"""
    db_info = parse_database(db_name)
    tables = {}
    
    with open(db_name, "rb") as f:
        f.seek(db_info['tables_pos'])
        
        for i in range(db_info['table_count']):
            table_id = int.from_bytes(f.read(MAX_TABLES_IN_BD_B), 'big')
            
            # Читаем название таблицы
            table_name = b''
            while True:
                byte = f.read(1)
                if byte in (b'\xfa', b'\xf8'):
                    break
                table_name += byte
            
            # Читаем количество колонок
            columns_count = int.from_bytes(f.read(MAX_CASES_IN_TABLE_B), 'big')
            columns = {}
            
            for j in range(columns_count):
                col_id = int.from_bytes(f.read(MAX_CASES_IN_TABLE_B), 'big')
                
                # Читаем название колонки
                col_name = b''
                while True:
                    byte = f.read(1)
                    if byte in (b'\xfa', b'\xf8'):
                        break
                    col_name += byte
                
                columns[col_id] = col_name.decode('utf-8')
            
            tables[table_id] = {
                'name': table_name.decode('utf-8'),
                'columns_count': columns_count,
                'columns': columns
            }
    
    return tables

def get_table_files(db_name, table_name=None):
    """Возвращает файлы, связанные с таблицей/таблицами"""
    tables = get_tables(db_name)
    result = {}
    
    for table_id, table_info in tables.items():
        if table_name is None or table_info['name'] == table_name:
            config_file = f"config/{table_info['name']}.mart"
            
            if os.path.exists(config_file):
                with open(config_file, "rb") as f:
                    f.read(MAX_TABLES_IN_BD_B)  # Пропускаем ID таблицы
                    
                    files = []
                    file_name = b''
                    while True:
                        byte = f.read(1)
                        if byte == b'\xfa':
                            files.append(file_name.decode('utf-8'))
                            file_name = b''
                        elif not byte:
                            break
                        else:
                            file_name += byte
                    
                    result[table_id] = files
    
    return result

def find_in_table(db_name, table_name, cords):
    """Ищет запись в указанной таблице"""
    table_files = get_table_files(db_name, table_name)
    
    for files in table_files.values():
        for file in files:
            result = find_case_in_file(file, cords)
            if result:
                return result
    
    return None

def insert_into_table(db_name, table_name, cords, data):
    """Вставляет запись в таблицу"""
    table_files = get_table_files(db_name, table_name)
    
    for files in table_files.values():
        for file in files:
            if write_case_to_file(file, cords, data):
                return True
    
    return False

def select_from_table(db_name, table_name):
    """Возвращает все записи из таблицы"""
    table_files = get_table_files(db_name, table_name)
    results = []
    
    for files in table_files.values():
        for file in files:
            results.extend(read_all_cases(file))
    
    return results