import os
import time
from .config import *
from .serialization import create_case, unpack_case, create_cord_block
# Глобальная переменная для отслеживания свободного пространства
FREE_SPACE = {}

def create_cases_file(file_name, table_id, cords, cases_dir=CASES_DIR):
    """Создает новый файл для хранения записей"""
    if not os.path.exists(cases_dir[:-1]):
        os.makedirs(cases_dir[:-1])
    
    file_path = os.path.join(cases_dir, file_name)
    with open(file_path, "wb") as f:
        f.write(table_id.to_bytes(MAX_TABLES_IN_BD_B, 'big'))
        f.write(cords.to_bytes(MAX_TABLES_IN_BD_B, 'big'))
        f.write(CASES_IN_FILE.to_bytes(MAX_CASES_IN_TABLE_B, 'big'))
        f.write(b'\x00' * MAX_CASES_IN_TABLE_B)  # Место для счетчика записей
        
        # Записываем пустые слоты для записей
        for _ in range(CASES_IN_FILE):
            f.write(b'\x00' * (cords * STANDART_CORD_SIZE) + 
                   b'\x00' * BYTES_PLASE_IN_FILE + 
                   b'\x00' * (STANDART_LEN_SIZE + 1))

def get_table_id(file_name, cases_dir=CASES_DIR):
    """Возвращает ID таблицы из файла"""
    file_path = os.path.join(cases_dir, file_name)
    with open(file_path, "rb") as f:
        return int.from_bytes(f.read(MAX_TABLES_IN_BD_B), 'big')

def get_table_info(file_name, cases_dir=CASES_DIR):
    """Возвращает информацию о таблице"""
    file_path = os.path.join(cases_dir, file_name)
    with open(file_path, "rb") as f:
        f.seek(MAX_TABLES_IN_BD_B)
        cords = int.from_bytes(f.read(MAX_TABLES_IN_BD_B), 'big')
        max_cases = int.from_bytes(f.read(MAX_CASES_IN_TABLE_B), 'big')
        case_count = int.from_bytes(f.read(MAX_CASES_IN_TABLE_B), 'big')
        return cords, max_cases, case_count, max_cases - case_count

def get_cases_info(file_name, cases_dir=CASES_DIR):
    """Возвращает информацию о записях в файле"""
    cords, max_cases, case_count, free_slots = get_table_info(file_name, cases_dir)
    
    file_path = os.path.join(cases_dir, file_name)
    with open(file_path, "rb") as f:
        # Пропускаем заголовок
        f.seek(MAX_TABLES_IN_BD_B * 2 + MAX_CASES_IN_TABLE_B * 2)
        
        cases = []
        for _ in range(case_count):
            cord_data = f.read(cords * STANDART_CORD_SIZE)
            position = int.from_bytes(f.read(BYTES_PLASE_IN_FILE), 'big')
            data_len = int.from_bytes(f.read(STANDART_LEN_SIZE + 1), 'big')
            
            # Извлекаем координаты
            case_cords = []
            for i in range(cords):
                start = i * STANDART_CORD_SIZE
                end = start + STANDART_CORD_SIZE
                cord_val = int.from_bytes(cord_data[start:end], 'big', signed=True)
                case_cords.append(cord_val)
            
            cases.append({
                'cords': case_cords,
                'position': position,
                'length': data_len,
                'current_pos': f.tell() - (BYTES_PLASE_IN_FILE + STANDART_LEN_SIZE + 1)
            })
        
        return cases, cords

def write_case_to_file(file_name, cords, data, cases_dir=CASES_DIR):
    """Записывает запись в файл"""
    cases_info, cords_count = get_cases_info(file_name, cases_dir)
    file_path = os.path.join(cases_dir, file_name)
    
    # Проверяем, существует ли уже запись с такими координатами
    existing_case = None
    for case in cases_info:
        if case['cords'] == cords:
            existing_case = case
            break
    
    # Сериализуем данные
    serialized_data = create_case(cords, data, STANDART_CORD_SIZE, BASED_RESERV_SIZE)
    
    with open(file_path, "rb+") as f:
        if existing_case:
            # Если запись существует, проверяем, достаточно ли места
            if existing_case['length'] >= len(serialized_data):
                # Перезаписываем существующую запись
                f.seek(existing_case['position'])
                f.write(serialized_data)
                return True
            else:
                # Помечаем старое место как свободное
                if existing_case['length'] not in FREE_SPACE:
                    FREE_SPACE[existing_case['length']] = []
                FREE_SPACE[existing_case['length']].append(existing_case['position'])
        else:
            # Ищем свободное место
            free_position = None
            for size in sorted(FREE_SPACE.keys()):
                if size >= len(serialized_data) and FREE_SPACE[size]:
                    free_position = FREE_SPACE[size].pop(0)
                    if not FREE_SPACE[size]:
                        del FREE_SPACE[size]
                    break
            
            if free_position is None:
                # Если свободного места нет, записываем в конец файла
                f.seek(0, 2)
                free_position = f.tell()
            
            # Записываем данные
            f.seek(free_position)
            f.write(serialized_data)
            
            # Обновляем информацию о записи
            f.seek(MAX_TABLES_IN_BD_B * 2 + MAX_CASES_IN_TABLE_B)
            case_count = int.from_bytes(f.read(MAX_CASES_IN_TABLE_B), 'big')
            f.seek(MAX_TABLES_IN_BD_B * 2 + MAX_CASES_IN_TABLE_B)
            f.write((case_count + 1).to_bytes(MAX_CASES_IN_TABLE_B, 'big'))
            
            # Записываем метаданные записи
            metadata_pos = MAX_TABLES_IN_BD_B * 2 + MAX_CASES_IN_TABLE_B * 2 + case_count * (cords_count * STANDART_CORD_SIZE + BYTES_PLASE_IN_FILE + STANDART_LEN_SIZE + 1)
            f.seek(metadata_pos)
            f.write(create_cord_block(cords, STANDART_CORD_SIZE))
            f.write(free_position.to_bytes(BYTES_PLASE_IN_FILE, 'big'))
            f.write(len(serialized_data).to_bytes(STANDART_LEN_SIZE + 1, 'big'))
            
            return True
    
    return False

def find_case_in_file(file_name, cords, cases_dir=CASES_DIR):
    """Ищет запись по координатам"""
    cases_info, _ = get_cases_info(file_name, cases_dir)
    
    for case in cases_info:
        if case['cords'] == cords:
            file_path = os.path.join(cases_dir, file_name)
            with open(file_path, "rb") as f:
                f.seek(case['position'])
                case_data = f.read(case['length'])
                return unpack_case(case_data, STANDART_CORD_SIZE, len(cords))
    
    return None

def read_all_cases(file_name, cases_dir=CASES_DIR):
    """Читает все записи из файла"""
    cases_info, cords_count = get_cases_info(file_name, cases_dir)
    file_path = os.path.join(cases_dir, file_name)
    
    cases = []
    with open(file_path, "rb") as f:
        for case in cases_info:
            f.seek(case['position'])
            case_data = f.read(case['length'])
            unpacked = unpack_case(case_data, STANDART_CORD_SIZE, cords_count)
            cases.append(unpacked)
    
    return cases

def defragment_file(file_name, cases_dir=CASES_DIR):
    """Дефрагментирует файл, удаляя пустые пространства"""
    temp_file = f"temp_{file_name}"
    create_cases_file(temp_file, get_table_id(file_name), get_table_info(file_name)[0])
    
    cases = read_all_cases(file_name, cases_dir)
    for cords, _, _, data, _ in cases:
        write_case_to_file(temp_file, cords, data)
    
    os.replace(os.path.join(cases_dir, temp_file), os.path.join(cases_dir, file_name))
    FREE_SPACE.clear()