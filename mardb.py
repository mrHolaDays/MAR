import socket
import json
import os
from .config import *
from . import database
from . import file_operations
from . import serialization

class MARDB:
    def __init__(self, mode='local', host='localhost', port=9999):
        """
        Инициализация клиента базы данных
        
        :param mode: Режим работы ('local' или 'server')
        :param host: Хост сервера (только для режима 'server')
        :param port: Порт сервера (только для режима 'server')
        """
        self.mode = mode
        self.host = host
        self.port = port
        self.socket = None
        
        if mode == 'server':
            self._connect_to_server()
            
    def _connect_to_server(self):
        """Устанавливает соединение с сервером"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
        except Exception as e:
            raise ConnectionError(f"Failed to connect to server: {e}")
            
    def _send_request(self, command, args):
        """
        Отправляет запрос на сервер и возвращает ответ
        
        :param command: Команда для выполнения
        :param args: Аргументы команды
        :return: Ответ от сервера
        """
        if not self.socket:
            self._connect_to_server()
            
        request = {
            'command': command,
            'args': args
        }
        
        request_data = json.dumps(request).encode('utf-8')
        
        try:
            # Отправляем длину сообщения
            self.socket.send(len(request_data).to_bytes(4, 'big'))
            # Отправляем само сообщение
            self.socket.send(request_data)
            
            # Получаем длину ответа
            length_bytes = self.socket.recv(4)
            if not length_bytes:
                raise ConnectionError("Connection closed by server")
                
            length = int.from_bytes(length_bytes, 'big')
            
            # Получаем сам ответ
            data = b''
            while len(data) < length:
                packet = self.socket.recv(length - len(data))
                if not packet:
                    raise ConnectionError("Connection closed by server")
                data += packet
                
            response = json.loads(data.decode('utf-8'))
            
            if response['status'] == 'error':
                raise Exception(response.get('message', 'Unknown error'))
                
            return response.get('data')
            
        except Exception as e:
            # При ошибке соединения пытаемся переподключиться
            if isinstance(e, (ConnectionError, BrokenPipeError)):
                self._connect_to_server()
                return self._send_request(command, args)
            else:
                raise e
                
    def create_database(self, db_name):
        """Создает новую базу данных"""
        if self.mode == 'local':
            return database.create_database(db_name)
        else:
            return self._send_request('create_database', {'db_name': db_name})
            
    def create_table(self, db_name, table_name, columns):
        """Создает новую таблицу в базе данных"""
        if self.mode == 'local':
            return database.create_table(db_name, table_name, columns)
        else:
            return self._send_request('create_table', {
                'db_name': db_name,
                'table_name': table_name,
                'columns': columns
            })
            
    def get_tables(self, db_name):
        """Возвращает информацию о всех таблицах в базе данных"""
        if self.mode == 'local':
            return database.get_tables(db_name)
        else:
            return self._send_request('get_tables', {'db_name': db_name})
            
    def get_table_files(self, db_name, table_name=None):
        """Возвращает файлы, связанные с таблицей/таблицами"""
        if self.mode == 'local':
            return database.get_table_files(db_name, table_name)
        else:
            return self._send_request('get_table_files', {
                'db_name': db_name,
                'table_name': table_name
            })
            
    def find_in_table(self, db_name, table_name, cords):
        """Ищет запись в указанной таблице"""
        if self.mode == 'local':
            result = database.find_in_table(db_name, table_name, cords)
            if result:
                cords, data_type, data_len, data, reversed_size = result
                return {
                    'cords': cords,
                    'data_type': data_type.__name__,
                    'data_len': data_len,
                    'data': data,
                    'reversed_size': reversed_size
                }
            else:
                return None
        else:
            return self._send_request('find_in_table', {
                'db_name': db_name,
                'table_name': table_name,
                'cords': cords
            })
            
    def insert_into_table(self, db_name, table_name, cords, data):
        """Вставляет запись в таблицу"""
        if self.mode == 'local':
            return database.insert_into_table(db_name, table_name, cords, data)
        else:
            return self._send_request('insert_into_table', {
                'db_name': db_name,
                'table_name': table_name,
                'cords': cords,
                'data': data
            })
            
    def select_from_table(self, db_name, table_name):
        """Возвращает все записи из таблицы"""
        if self.mode == 'local':
            results = database.select_from_table(db_name, table_name)
            # Сериализуем результаты для единообразия с серверным режимом
            serialized_results = []
            for result in results:
                cords, data_type, data_len, data, reversed_size = result
                serialized_results.append({
                    'cords': cords,
                    'data_type': data_type.__name__,
                    'data_len': data_len,
                    'data': data,
                    'reversed_size': reversed_size
                })
            return serialized_results
        else:
            return self._send_request('select_from_table', {
                'db_name': db_name,
                'table_name': table_name
            })
            
    def defragment_database(self, db_name):
        """Дефрагментирует базу данных"""
        if self.mode == 'local':
            # Получаем все файлы таблиц и дефрагментируем их
            table_files = database.get_table_files(db_name)
            for files in table_files.values():
                for file_name in files:
                    file_operations.defragment_file(file_name)
            return True
        else:
            return self._send_request('defragment_database', {'db_name': db_name})
            
    def load_database(self, db_name, mode='fast'):
        """Загружает базу данных на сервере"""
        if self.mode == 'server':
            return self._send_request('load_database', {
                'db_name': db_name,
                'mode': mode
            })
        else:
            raise Exception("Load database is only available in server mode")
            
    def unload_database(self, db_name):
        """Выгружает базу данных с сервера"""
        if self.mode == 'server':
            return self._send_request('unload_database', {'db_name': db_name})
        else:
            raise Exception("Unload database is only available in server mode")
            
    def close(self):
        """Закрывает соединение с сервером (только для серверного режима)"""
        if self.mode == 'server' and self.socket:
            self.socket.close()
            self.socket = None


# Функция для инициализации базы данных
def init_db(mode='local', host='localhost', port=9999):
    """
    Инициализирует клиент базы данных
    
    :param mode: Режим работы ('local' или 'server')
    :param host: Хост сервера (только для режима 'server')
    :param port: Порт сервера (только для режима 'server')
    :return: Объект MARDB
    """
    return MARDB(mode, host, port)


# Пример использования
# if __name__ == '__main__':
#     # Локальный режим
#     db_local = init_db('local')
#     db_local.create_database('test.marm')
#     db_local.create_table('test.marm', 'users', ['id', 'name', 'age'])
#     db_local.insert_into_table('test.marm', 'users', [1, 2, 3], 'John Doe')
#     results = db_local.select_from_table('test.marm', 'users')
#     print(results)
#     db_local.close()
    
#     # Серверный режим (предполагая, что сервер запущен)
#     db_server = init_db('server', 'localhost', 9999)
#     db_server.load_database('test_server.marm', 'full')
#     db_server.create_table('test_server.marm', 'products', ['id', 'name', 'price'])
#     db_server.insert_into_table('test_server.marm', 'products', [1, 2, 3], 'Laptop')
#     results = db_server.select_from_table('test_server.marm', 'products')
#     print(results)
#     db_server.close()