import socket
import threading
import json
import sys
import os
import traceback
import logging
import argparse
import time
import pickle
from datetime import datetime
from collections import deque
from config import *
import database
import file_operations
import serialization

class MARDatabaseServer:
    def __init__(self, host='localhost', port=9999, log_level='INFO', 
                 console_log=True, file_log=False, log_file='mardb_server.log',
                 sync_interval=30, load_mode='fast'):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.running = False
        self.sync_interval = sync_interval  # Интервал синхронизации в секундах
        self.load_mode = load_mode  # Режим загрузки: full, part, fast
        
        # Структуры для хранения данных в памяти
        self.active_databases = {}  # Активные базы данных
        self.cached_data = {}       # Кэшированные данные: {db_name: {table_name: {tuple(cords): data}}}
        self.modified_cells = {}    # Измененные ячейки: {db_name: {table_name: set(tuple(cords))}}
        self.accessed_cells = {}    # Доступные ячейки: {db_name: {table_name: set(tuple(cords))}}
        self.operation_queue = deque()  # Очередь операций для синхронизации
        
        # Настройка логирования
        self.setup_logging(log_level, console_log, file_log, log_file)
        
        # Запуск потока синхронизации
        self.sync_thread = threading.Thread(target=self.sync_worker, daemon=True)
        
    def setup_logging(self, log_level, console_log, file_log, log_file):
        """Настраивает логирование"""
        # Создаем логгер
        self.logger = logging.getLogger('MARDatabaseServer')
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Очищаем существующие обработчики
        self.logger.handlers.clear()
        
        # Создаем форматтер
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Добавляем обработчик для консоли, если включен
        if console_log:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        
        # Добавляем обработчик для файла, если включен
        if file_log:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            
        # Если оба типа логирования отключены, добавляем NullHandler
        if not console_log and not file_log:
            self.logger.addHandler(logging.NullHandler())
            
        self.logger.info(f"Logging initialized (level: {log_level}, console: {console_log}, file: {file_log})")
        
    def load_database(self, db_name, mode=None):
        """Загружает базу данных в память в соответствии с выбранным режимом"""
        if mode is None:
            mode = self.load_mode
            
        if db_name not in self.active_databases:
            self.active_databases[db_name] = {
                'tables': {},
                'files': {},
                'loaded_at': datetime.now(),
                'load_mode': mode
            }
            
        # Получаем информацию о таблицах
        tables = database.get_tables(db_name)
        self.active_databases[db_name]['tables'] = tables
        
        # Получаем информацию о файлах
        table_files = database.get_table_files(db_name)
        self.active_databases[db_name]['files'] = table_files
        
        # Инициализируем структуры данных для кэширования
        if db_name not in self.cached_data:
            self.cached_data[db_name] = {}
            self.modified_cells[db_name] = {}
            self.accessed_cells[db_name] = {}
            
        # Загружаем данные в соответствии с режимом
        if mode == 'full':
            self.logger.info(f"Full-loading database: {db_name}")
            for table_id, table_info in tables.items():
                table_name = table_info['name']
                self.load_table_data(db_name, table_name, full=True)
                
        elif mode == 'part':
            self.logger.info(f"Part-loading database: {db_name}")
            # Загружаем данные из .marl файла, если он существует
            marl_file = f"{db_name}.marl"
            if os.path.exists(marl_file):
                self.load_marl_file(db_name, marl_file)
                
        elif mode == 'fast':
            self.logger.info(f"Fast-loading database: {db_name}")
            # В этом режиме данные загружаются по мере обращения
            pass
            
        self.logger.info(f"Database loaded: {db_name} in {mode} mode")
        
    def load_marl_file(self, db_name, marl_file):
        """Загружает данные из .marl файла"""
        try:
            with open(marl_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                        
                    if line.startswith('load_table:'):
                        table_name = line.split(':', 1)[1].strip()
                        self.load_table_data(db_name, table_name, full=True)
                        
                    elif line.startswith('load_case:'):
                        parts = line.split(':', 1)[1].split(',')
                        if len(parts) >= 2:
                            table_name = parts[0].strip()
                            cords = [int(cord.strip()) for cord in parts[1:]]
                            self.load_case_data(db_name, table_name, cords)
                            
        except Exception as e:
            self.logger.error(f"Error loading MARL file {marl_file}: {e}")
            
    def load_table_data(self, db_name, table_name, full=False):
        """Загружает данные таблицы в память"""
        if db_name not in self.cached_data:
            self.cached_data[db_name] = {}
            
        if table_name not in self.cached_data[db_name]:
            self.cached_data[db_name][table_name] = {}
            
        if table_name not in self.modified_cells[db_name]:
            self.modified_cells[db_name][table_name] = set()
            
        if table_name not in self.accessed_cells[db_name]:
            self.accessed_cells[db_name][table_name] = set()
            
        if full:
            # Загружаем все данные таблицы
            try:
                records = database.select_from_table(db_name, table_name)
                for record in records:
                    cords, data_type, data_len, data, reversed_size = record
                    cord_key = tuple(cords)
                    self.cached_data[db_name][table_name][cord_key] = data
                    self.accessed_cells[db_name][table_name].add(cord_key)
                    
                self.logger.info(f"Loaded table {table_name} from {db_name}, records: {len(records)}")
            except Exception as e:
                self.logger.error(f"Error loading table {table_name} from {db_name}: {e}")
                
    def load_case_data(self, db_name, table_name, cords):
        """Загружает конкретную ячейку в память"""
        if db_name not in self.cached_data:
            self.cached_data[db_name] = {}
            
        if table_name not in self.cached_data[db_name]:
            self.cached_data[db_name][table_name] = {}
            
        if table_name not in self.modified_cells[db_name]:
            self.modified_cells[db_name][table_name] = set()
            
        if table_name not in self.accessed_cells[db_name]:
            self.accessed_cells[db_name][table_name] = set()
            
        cord_key = tuple(cords)
        
        # Если данные уже в кэше, пропускаем загрузку
        if cord_key in self.cached_data[db_name][table_name]:
            return
            
        # Загружаем данные из базы
        try:
            result = database.find_in_table(db_name, table_name, cords)
            if result:
                cords, data_type, data_len, data, reversed_size = result
                self.cached_data[db_name][table_name][cord_key] = data
                self.accessed_cells[db_name][table_name].add(cord_key)
                self.logger.debug(f"Loaded case {cords} from {table_name} in {db_name}")
        except Exception as e:
            self.logger.error(f"Error loading case {cords} from {table_name} in {db_name}: {e}")
            
    def sync_worker(self):
        """Фоновая задача для синхронизации данных с базой"""
        while self.running:
            time.sleep(self.sync_interval)
            self.sync_to_database()
            
    def sync_to_database(self):
        """Синхронизирует измененные данные с базой"""
        if not self.modified_cells:
            return
            
        self.logger.info("Starting database synchronization")
        
        for db_name, tables in self.modified_cells.items():
            for table_name, cells in tables.items():
                if not cells:
                    continue
                    
                for cord_key in cells:
                    if (db_name in self.cached_data and 
                        table_name in self.cached_data[db_name] and 
                        cord_key in self.cached_data[db_name][table_name]):
                        
                        data = self.cached_data[db_name][table_name][cord_key]
                        cords = list(cord_key)
                        
                        try:
                            # Обновляем данные в базе
                            success = database.insert_into_table(db_name, table_name, cords, data)
                            if success:
                                self.logger.debug(f"Synced case {cords} to {table_name} in {db_name}")
                            else:
                                self.logger.error(f"Failed to sync case {cords} to {table_name} in {db_name}")
                        except Exception as e:
                            self.logger.error(f"Error syncing case {cords} to {table_name} in {db_name}: {e}")
                
                # Очищаем множество измененных ячеек после синхронизации
                cells.clear()
                
        self.logger.info("Database synchronization completed")
        
    def start(self):
        """Запускает сервер базы данных"""
        try:
            self.socket.bind((self.host, self.port))
            self.socket.listen(5)
            self.running = True
            
            # Запускаем поток синхронизации
            self.sync_thread.start()
            
            self.logger.info(f"MAR Database Server started on {self.host}:{self.port}")
            self.logger.info(f"Sync interval: {self.sync_interval} seconds")
            self.logger.info(f"Load mode: {self.load_mode}")
            
            while self.running:
                client_socket, addr = self.socket.accept()
                self.logger.info(f"Connection from {addr}")
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, addr)
                )
                client_thread.daemon = True
                client_thread.start()
                
        except Exception as e:
            self.logger.error(f"Server error: {e}")
            self.logger.error(traceback.format_exc())
            self.stop()
            
    def stop(self):
        """Останавливает сервер"""
        self.running = False
        
        # Синхронизируем все данные перед остановкой
        self.sync_to_database()
        
        self.socket.close()
        self.logger.info("MAR Database Server stopped")
        
    def handle_client(self, client_socket, addr):
        """Обрабатывает запросы от клиента"""
        try:
            while self.running:
                # Получаем длину сообщения
                length_bytes = client_socket.recv(4)
                if not length_bytes:
                    break
                    
                length = int.from_bytes(length_bytes, 'big')
                
                # Получаем само сообщение
                data = b''
                while len(data) < length:
                    packet = client_socket.recv(length - len(data))
                    if not packet:
                        break
                    data += packet
                
                if not data:
                    break
                    
                # Десериализуем запрос
                request = json.loads(data.decode('utf-8'))
                self.logger.debug(f"Received request from {addr}: {request}")
                
                response = self.process_request(request)
                
                # Отправляем ответ
                response_data = json.dumps(response).encode('utf-8')
                client_socket.send(len(response_data).to_bytes(4, 'big'))
                client_socket.send(response_data)
                
                self.logger.debug(f"Sent response to {addr}: {response}")
                
        except Exception as e:
            self.logger.error(f"Error handling client {addr}: {e}")
            self.logger.error(traceback.format_exc())
        finally:
            client_socket.close()
            self.logger.info(f"Connection closed with {addr}")
            
    def process_request(self, request):
        """Обрабатывает запрос и возвращает ответ"""
        try:
            command = request.get('command')
            args = request.get('args', {})
            db_name = args.get('db_name')
            
            self.logger.info(f"Processing command: {command} for database: {db_name}")
            
            # Если база данных не загружена, загружаем ее
            if db_name and db_name not in self.active_databases:
                self.load_database(db_name)
            
            if command == 'create_database':
                database.create_database(args['db_name'])
                self.load_database(args['db_name'])
                self.logger.info(f"Database created: {args['db_name']}")
                return {'status': 'success', 'data': None}
                
            elif command == 'create_table':
                database.create_table(args['db_name'], args['table_name'], args['columns'])
                # Обновляем информацию о базе данных
                if args['db_name'] in self.active_databases:
                    tables = database.get_tables(args['db_name'])
                    self.active_databases[args['db_name']]['tables'] = tables
                self.logger.info(f"Table created: {args['table_name']} in {args['db_name']}")
                return {'status': 'success', 'data': None}
                
            elif command == 'get_tables':
                if db_name in self.active_databases:
                    tables = self.active_databases[db_name]['tables']
                else:
                    tables = database.get_tables(db_name)
                self.logger.debug(f"Retrieved tables from {db_name}: {tables}")
                return {'status': 'success', 'data': tables}
                
            elif command == 'get_table_files':
                if db_name in self.active_databases:
                    files = self.active_databases[db_name]['files']
                else:
                    files = database.get_table_files(db_name, args.get('table_name'))
                self.logger.debug(f"Retrieved table files from {db_name}: {files}")
                return {'status': 'success', 'data': files}
                
            elif command == 'find_in_table':
                table_name = args['table_name']
                cords = args['cords']
                cord_key = tuple(cords)
                
                # Проверяем, есть ли данные в кэше
                if (db_name in self.cached_data and 
                    table_name in self.cached_data[db_name] and 
                    cord_key in self.cached_data[db_name][table_name]):
                    
                    data = self.cached_data[db_name][table_name][cord_key]
                    self.accessed_cells[db_name][table_name].add(cord_key)
                    self.logger.debug(f"Found record in cache: {table_name}: {data}")
                    
                    return {
                        'status': 'success', 
                        'data': {
                            'cords': cords,
                            'data_type': type(data).__name__,
                            'data_len': len(str(data)),
                            'data': data,
                            'reversed_size': 0
                        }
                    }
                
                # Если данных нет в кэше, ищем в базе
                result = database.find_in_table(db_name, table_name, cords)
                if result:
                    # Сохраняем в кэш для будущих обращений
                    cords, data_type, data_len, data, reversed_size = result
                    cord_key = tuple(cords)
                    
                    if db_name not in self.cached_data:
                        self.cached_data[db_name] = {}
                    if table_name not in self.cached_data[db_name]:
                        self.cached_data[db_name][table_name] = {}
                    
                    self.cached_data[db_name][table_name][cord_key] = data
                    self.accessed_cells[db_name][table_name].add(cord_key)
                    
                    self.logger.debug(f"Found record in database: {table_name}: {data}")
                    return {
                        'status': 'success', 
                        'data': {
                            'cords': cords,
                            'data_type': data_type.__name__,
                            'data_len': data_len,
                            'data': data,
                            'reversed_size': reversed_size
                        }
                    }
                else:
                    self.logger.debug(f"Record not found in {table_name} with cords {cords}")
                    return {'status': 'success', 'data': None}
                    
            elif command == 'insert_into_table':
                db_name = args['db_name']
                table_name = args['table_name']
                cords = args['cords']
                data = args['data']
                cord_key = tuple(cords)
                
                # Сохраняем в кэш
                if db_name not in self.cached_data:
                    self.cached_data[db_name] = {}
                if table_name not in self.cached_data[db_name]:
                    self.cached_data[db_name][table_name] = {}
                    
                self.cached_data[db_name][table_name][cord_key] = data
                
                # Помечаем как измененную ячейку
                if db_name not in self.modified_cells:
                    self.modified_cells[db_name] = {}
                if table_name not in self.modified_cells[db_name]:
                    self.modified_cells[db_name][table_name] = set()
                    
                self.modified_cells[db_name][table_name].add(cord_key)
                
                # Помечаем как доступную ячейку
                if db_name not in self.accessed_cells:
                    self.accessed_cells[db_name] = {}
                if table_name not in self.accessed_cells[db_name]:
                    self.accessed_cells[db_name][table_name] = set()
                    
                self.accessed_cells[db_name][table_name].add(cord_key)
                
                # Добавляем операцию в очередь для синхронизации
                self.operation_queue.append({
                    'type': 'insert',
                    'db_name': db_name,
                    'table_name': table_name,
                    'cords': cords,
                    'data': data
                })
                
                self.logger.info(f"Record inserted into cache: {table_name} in {db_name}")
                return {'status': 'success', 'data': True}
                
            elif command == 'select_from_table':
                table_name = args['table_name']
                
                # Проверяем, есть ли вся таблица в кэше
                if (db_name in self.cached_data and 
                    table_name in self.cached_data[db_name] and 
                    len(self.cached_data[db_name][table_name]) > 0):
                    
                    # Возвращаем данные из кэша
                    results = []
                    for cord_key, data in self.cached_data[db_name][table_name].items():
                        results.append({
                            'cords': list(cord_key),
                            'data_type': type(data).__name__,
                            'data_len': len(str(data)),
                            'data': data,
                            'reversed_size': 0
                        })
                    
                    self.logger.debug(f"Selected {len(results)} records from cache: {table_name}")
                    return {'status': 'success', 'data': results}
                
                # Если таблицы нет в кэше, загружаем из базы
                results = database.select_from_table(db_name, table_name)
                # Сериализуем результаты для передачи
                serialized_results = []
                for result in results:
                    cords, data_type, data_len, data, reversed_size = result
                    cord_key = tuple(cords)
                    
                    # Сохраняем в кэш
                    if db_name not in self.cached_data:
                        self.cached_data[db_name] = {}
                    if table_name not in self.cached_data[db_name]:
                        self.cached_data[db_name][table_name] = {}
                    
                    self.cached_data[db_name][table_name][cord_key] = data
                    self.accessed_cells[db_name][table_name].add(cord_key)
                    
                    serialized_results.append({
                        'cords': cords,
                        'data_type': data_type.__name__,
                        'data_len': data_len,
                        'data': data,
                        'reversed_size': reversed_size
                    })
                
                self.logger.debug(f"Selected {len(results)} records from database: {table_name}")
                return {'status': 'success', 'data': serialized_results}
                
            elif command == 'defragment_database':
                # Получаем все файлы таблиц и дефрагментируем их
                table_files = database.get_table_files(db_name)
                for files in table_files.values():
                    for file_name in files:
                        file_operations.defragment_file(file_name)
                self.logger.info(f"Database defragmented: {db_name}")
                return {'status': 'success', 'data': None}
                
            elif command == 'load_database':
                # Явная команда для загрузки базы данных
                mode = args.get('mode', self.load_mode)
                self.load_database(db_name, mode)
                return {'status': 'success', 'data': None}
                
            elif command == 'unload_database':
                # Выгружаем базу данных из памяти
                if db_name in self.active_databases:
                    # Синхронизируем перед выгрузкой
                    if db_name in self.modified_cells:
                        for table_name, cells in self.modified_cells[db_name].items():
                            if cells:
                                self.sync_to_database()
                                break
                    
                    # Удаляем из памяти
                    if db_name in self.cached_data:
                        del self.cached_data[db_name]
                    if db_name in self.modified_cells:
                        del self.modified_cells[db_name]
                    if db_name in self.accessed_cells:
                        del self.accessed_cells[db_name]
                    if db_name in self.active_databases:
                        del self.active_databases[db_name]
                        
                    self.logger.info(f"Database unloaded: {db_name}")
                    return {'status': 'success', 'data': None}
                else:
                    return {'status': 'error', 'message': f'Database not loaded: {db_name}'}
                
            else:
                self.logger.warning(f"Unknown command: {command}")
                return {'status': 'error', 'message': f'Unknown command: {command}'}
                
        except Exception as e:
            self.logger.error(f"Error processing request: {e}")
            self.logger.error(traceback.format_exc())
            return {'status': 'error', 'message': str(e), 'traceback': traceback.format_exc()}


def main():
    """Основная функция для запуска сервера"""
    parser = argparse.ArgumentParser(description='MAR Database Server')
    parser.add_argument('--host', default='localhost', help='Host to bind to')
    parser.add_argument('--port', type=int, default=9999, help='Port to bind to')
    parser.add_argument('--log-level', default='INFO', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       help='Logging level')
    parser.add_argument('--no-console-log', action='store_true', 
                       help='Disable console logging')
    parser.add_argument('--file-log', action='store_true', 
                       help='Enable file logging')
    parser.add_argument('--log-file', default='mardb_server.log', 
                       help='Log file name (only used with --file-log)')
    parser.add_argument('--sync-interval', type=int, default=30,
                       help='Database synchronization interval in seconds')
    parser.add_argument('--load-mode', default='fast',
                       choices=['full', 'part', 'fast'],
                       help='Data loading mode: full, part, fast')
    parser.add_argument('--preload', nargs='+',
                       help='Preload these databases at startup')
    
    args = parser.parse_args()
    
    server = MARDatabaseServer(
        host=args.host,
        port=args.port,
        log_level=args.log_level,
        console_log=not args.no_console_log,
        file_log=args.file_log,
        log_file=args.log_file,
        sync_interval=args.sync_interval,
        load_mode=args.load_mode
    )
    
    # Предзагрузка баз данных, если указано
    if args.preload:
        for db_name in args.preload:
            server.load_database(db_name, args.load_mode)
    
    try:
        server.start()
    except KeyboardInterrupt:
        server.logger.info("\nShutting down server...")
        server.stop()
    except Exception as e:
        server.logger.error(f"Server error: {e}")
        server.stop()


if __name__ == '__main__':
    main()