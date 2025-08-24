import sys
import os
import json
from marlib import database, file_operations
from simple_term_menu import TerminalMenu
from colorama import init, Fore, Back, Style
from pyfiglet import Figlet
import pickle

# Инициализация colorama для цветного вывода
init(autoreset=True)

class ConsoleDatabaseApp:
    def __init__(self):
        self.current_db = None
        self.current_table = None
        self.language = "en"  # По умолчанию английский
        self.settings_file = "console_settings.json"
        self.load_settings()
        
    def load_settings(self):
        """Загружает настройки из файла"""
        if os.path.exists(self.settings_file):
            try:
                with open(self.settings_file, 'r') as f:
                    settings = json.load(f)
                    self.language = settings.get("language", "en")
            except:
                pass  # Если файл поврежден, используем настройки по умолчанию
    
    def save_settings(self):
        """Сохраняет настройки в файл"""
        settings = {
            "language": self.language
        }
        with open(self.settings_file, 'w') as f:
            json.dump(settings, f)
    
    def translate(self, key):
        """Возвращает переведенный текст в зависимости от выбранного языка"""
        translations = {
            "en": {
                "welcome": "MAR Database Console Manager",
                "main_menu": "Main Menu",
                "create_db": "Create Database",
                "open_db": "Open Database",
                "close_db": "Close Database",
                "create_table": "Create Table",
                "delete_table": "Delete Table",
                "add_record": "Add Record",
                "search_record": "Search Record",
                "view_records": "View Records",
                "defragment": "Defragment Database",
                "settings": "Settings",
                "instructions": "Instructions",
                "exit": "Exit",
                "back": "Back",
                "select_db": "Select Database",
                "select_table": "Select Table",
                "enter_db_name": "Enter database name: ",
                "enter_table_name": "Enter table name: ",
                "enter_columns": "Enter column names (comma separated): ",
                "enter_coords": "Enter coordinates (comma separated): ",
                "enter_data": "Enter data: ",
                "db_created": "Database created successfully!",
                "db_opened": "Database opened successfully!",
                "db_closed": "Database closed successfully!",
                "table_created": "Table created successfully!",
                "table_deleted": "Table deleted successfully!",
                "record_added": "Record added successfully!",
                "record_found": "Record found: ",
                "record_not_found": "Record not found!",
                "defrag_complete": "Defragmentation completed successfully!",
                "language_set": "Language changed to English",
                "press_enter": "Press Enter to continue...",
                "no_db_open": "No database is currently open!",
                "no_table_selected": "No table is currently selected!",
                "error": "Error: ",
                "settings_menu": "Settings Menu",
                "change_language": "Change Language",
                "current_language": "Current Language: English",
                "select_language": "Select Language:",
                "english": "English",
                "russian": "Russian",
                "instructions_text": "MAR Database Console Manager Instructions\n\n" +
                                     "This application allows you to manage MAR databases from the console.\n\n" +
                                     "Basic Operations:\n" +
                                     "1. Create Database - Create a new database file\n" +
                                     "2. Open Database - Open an existing database\n" +
                                     "3. Create Table - Create a new table in the database\n" +
                                     "4. Add Record - Add a new record to a table\n" +
                                     "5. Search Record - Search for a record by coordinates\n" +
                                     "6. View Records - View all records in a table\n" +
                                     "7. Defragment - Optimize database storage\n\n" +
                                     "Navigation:\n" +
                                     "- Use arrow keys to navigate menus\n" +
                                     "- Press Enter to select an option\n" +
                                     "- Press Esc to go back\n"
            },
            "ru": {
                "welcome": "Консольный менеджер баз данных MAR",
                "main_menu": "Главное меню",
                "create_db": "Создать базу данных",
                "open_db": "Открыть базу данных",
                "close_db": "Закрыть базу данных",
                "create_table": "Создать таблицу",
                "delete_table": "Удалить таблицу",
                "add_record": "Добавить запись",
                "search_record": "Поиск записи",
                "view_records": "Просмотр записей",
                "defragment": "Дефрагментировать базу",
                "settings": "Настройки",
                "instructions": "Инструкция",
                "exit": "Выход",
                "back": "Назад",
                "select_db": "Выбрать базу данных",
                "select_table": "Выбрать таблицу",
                "enter_db_name": "Введите имя базы данных: ",
                "enter_table_name": "Введите имя таблицы: ",
                "enter_columns": "Введите названия колонок (через запятую): ",
                "enter_coords": "Введите координаты (через запятую): ",
                "enter_data": "Введите данные: ",
                "db_created": "База данных успешно создана!",
                "db_opened": "База данных успешно открыта!",
                "db_closed": "База данных успешно закрыта!",
                "table_created": "Таблица успешно создана!",
                "table_deleted": "Таблица успешно удалена!",
                "record_added": "Запись успешно добавлена!",
                "record_found": "Запись найдена: ",
                "record_not_found": "Запись не найдена!",
                "defrag_complete": "Дефрагментация успешно завершена!",
                "language_set": "Язык изменен на Русский",
                "press_enter": "Нажмите Enter для продолжения...",
                "no_db_open": "База данных не открыта!",
                "no_table_selected": "Таблица не выбрана!",
                "error": "Ошибка: ",
                "settings_menu": "Меню настроек",
                "change_language": "Изменить язык",
                "current_language": "Текущий язык: Русский",
                "select_language": "Выберите язык:",
                "english": "Английский",
                "russian": "Русский",
                "instructions_text": "Инструкция по использованию консольного менеджера баз данных MAR\n\n" +
                                     "Это приложение позволяет управлять базами данных MAR из консоли.\n\n" +
                                     "Основные операции:\n" +
                                     "1. Создать базу данных - Создать новый файл базы данных\n" +
                                     "2. Открыть базу данных - Открыть существующую базу данных\n" +
                                     "3. Создать таблицу - Создать новую таблицу в базе данных\n" +
                                     "4. Добавить запись - Добавить новую запись в таблицу\n" +
                                     "5. Поиск записи - Найти запись по координатам\n" +
                                     "6. Просмотр записей - Просмотреть все записи в таблице\n" +
                                     "7. Дефрагментировать - Оптимизировать хранение базы данных\n\n" +
                                     "Навигация:\n" +
                                     "- Используйте стрелки для навигации по меню\n" +
                                     "- Нажмите Enter для выбора опции\n" +
                                     "- Нажмите Esc для возврата назад\n"
            }
        }
        
        return translations[self.language].get(key, key)
    
    def clear_screen(self):
        """Очищает экран консоли"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def show_header(self):
        """Показывает заголовок приложения"""
        self.clear_screen()
        f = Figlet(font='slant')
        print(Fore.CYAN + f.renderText('MAR DB'))
        print(Fore.YELLOW + self.translate("welcome"))
        print()
    
    def wait_for_enter(self):
        """Ждет нажатия Enter"""
        input(Fore.GREEN + self.translate("press_enter"))
    
    def show_error(self, message):
        """Показывает сообщение об ошибке"""
        print(Fore.RED + self.translate("error") + message)
        self.wait_for_enter()
    
    def show_success(self, message):
        """Показывает сообщение об успехе"""
        print(Fore.GREEN + message)
        self.wait_for_enter()
    
    def create_database(self):
        """Создает новую базу данных"""
        self.show_header()
        db_name = input(Fore.WHITE + self.translate("enter_db_name"))
        
        if not db_name.endswith('.marm'):
            db_name += '.marm'
        
        try:
            database.create_database(db_name)
            self.show_success(self.translate("db_created"))
        except Exception as e:
            self.show_error(str(e))
    
    def open_database(self):
        """Открывает существующую базу данных"""
        self.show_header()
        
        # Получаем список файлов базы данных в текущей директории
        db_files = [f for f in os.listdir('.') if f.endswith('.marm')]
        
        if not db_files:
            print(Fore.YELLOW + "No database files found in current directory!")
            self.wait_for_enter()
            return
        
        options = db_files + [self.translate("back")]
        menu = TerminalMenu(options, title=self.translate("select_db"))
        selected_index = menu.show()
        
        if selected_index < len(db_files):
            try:
                self.current_db = db_files[selected_index]
                self.show_success(self.translate("db_opened"))
            except Exception as e:
                self.show_error(str(e))
    
    def close_database(self):
        """Закрывает текущую базу данных"""
        self.current_db = None
        self.current_table = None
        self.show_success(self.translate("db_closed"))
    
    def create_table(self):
        """Создает новую таблицу в базе данных"""
        if not self.current_db:
            self.show_error(self.translate("no_db_open"))
            return
        
        self.show_header()
        table_name = input(Fore.WHITE + self.translate("enter_table_name"))
        columns_input = input(Fore.WHITE + self.translate("enter_columns"))
        
        columns = [col.strip() for col in columns_input.split(',')]
        
        try:
            database.create_table(self.current_db, table_name, columns)
            self.show_success(self.translate("table_created"))
        except Exception as e:
            self.show_error(str(e))
    
    def select_table(self):
        """Выбирает таблицу для работы"""
        if not self.current_db:
            self.show_error(self.translate("no_db_open"))
            return None
        
        try:
            tables = database.get_tables(self.current_db)
            if not tables:
                print(Fore.YELLOW + "No tables found in database!")
                self.wait_for_enter()
                return None
            
            table_names = [tables[table_id]['name'] for table_id in tables]
            options = table_names + [self.translate("back")]
            
            menu = TerminalMenu(options, title=self.translate("select_table"))
            selected_index = menu.show()
            
            if selected_index < len(table_names):
                return table_names[selected_index]
            else:
                return None
                
        except Exception as e:
            self.show_error(str(e))
            return None
    
    def add_record(self):
        """Добавляет запись в таблицу"""
        if not self.current_db:
            self.show_error(self.translate("no_db_open"))
            return
        
        if not self.current_table:
            self.current_table = self.select_table()
            if not self.current_table:
                return
        
        self.show_header()
        print(Fore.YELLOW + f"Table: {self.current_table}")
        
        # Получаем информацию о таблице для определения количества координат
        tables = database.get_tables(self.current_db)
        table_info = None
        
        for table_id, info in tables.items():
            if info['name'] == self.current_table:
                table_info = info
                break
        
        if not table_info:
            self.show_error("Table information not found!")
            return
        
        coords_input = input(Fore.WHITE + self.translate("enter_coords"))
        data = input(Fore.WHITE + self.translate("enter_data"))
        
        try:
            coords = [int(coord.strip()) for coord in coords_input.split(',')]
            
            # Проверяем, что количество координат соответствует таблице
            if len(coords) != len(table_info['columns']):
                self.show_error(f"Expected {len(table_info['columns'])} coordinates, got {len(coords)}")
                return
            
            success = database.insert_into_table(self.current_db, self.current_table, coords, data)
            
            if success:
                self.show_success(self.translate("record_added"))
            else:
                self.show_error("Failed to add record!")
                
        except ValueError:
            self.show_error("Coordinates must be integers!")
        except Exception as e:
            self.show_error(str(e))
    
    def search_record(self):
        """Ищет запись по координатам"""
        if not self.current_db:
            self.show_error(self.translate("no_db_open"))
            return
        
        if not self.current_table:
            self.current_table = self.select_table()
            if not self.current_table:
                return
        
        self.show_header()
        print(Fore.YELLOW + f"Table: {self.current_table}")
        
        # Получаем информацию о таблице для определения количества координат
        tables = database.get_tables(self.current_db)
        table_info = None
        
        for table_id, info in tables.items():
            if info['name'] == self.current_table:
                table_info = info
                break
        
        if not table_info:
            self.show_error("Table information not found!")
            return
        
        coords_input = input(Fore.WHITE + self.translate("enter_coords"))
        
        try:
            coords = [int(coord.strip()) for coord in coords_input.split(',')]
            
            # Проверяем, что количество координат соответствует таблице
            if len(coords) != len(table_info['columns']):
                self.show_error(f"Expected {len(table_info['columns'])} coordinates, got {len(coords)}")
                return
            
            result = database.find_in_table(self.current_db, self.current_table, coords)
            
            if result:
                cords, data_type, data_len, data, reversed_size = result
                print(Fore.GREEN + self.translate("record_found"))
                print(Fore.WHITE + f"Coordinates: {cords}")
                print(Fore.WHITE + f"Data: {data}")
            else:
                print(Fore.YELLOW + self.translate("record_not_found"))
                
            self.wait_for_enter()
                
        except ValueError:
            self.show_error("Coordinates must be integers!")
        except Exception as e:
            self.show_error(str(e))
    
    def view_records(self):
        """Показывает все записи в таблице"""
        if not self.current_db:
            self.show_error(self.translate("no_db_open"))
            return
        
        if not self.current_table:
            self.current_table = self.select_table()
            if not self.current_table:
                return
        
        try:
            records = database.select_from_table(self.current_db, self.current_table)
            
            self.show_header()
            print(Fore.YELLOW + f"Table: {self.current_table}")
            print()
            
            if not records:
                print(Fore.YELLOW + "No records found in table!")
                self.wait_for_enter()
                return
            
            # Получаем информацию о таблице для отображения заголовков
            tables = database.get_tables(self.current_db)
            table_info = None
            
            for table_id, info in tables.items():
                if info['name'] == self.current_table:
                    table_info = info
                    break
            
            if table_info:
                # Выводим заголовки
                headers = []
                for i in range(len(table_info['columns'])):
                    headers.append(table_info['columns'].get(i, f"Column {i}"))
                headers.append("Data")
                
                print(Fore.CYAN + " | ".join(headers))
                print(Fore.CYAN + "-" * (len(" | ".join(headers)) + 10))
            
            # Выводим записи
            for record in records:
                cords, data_type, data_len, data, reversed_size = record
                row = [str(coord) for coord in cords] + [str(data)]
                print(Fore.WHITE + " | ".join(row))
            
            self.wait_for_enter()
                
        except Exception as e:
            self.show_error(str(e))
    
    def defragment_database(self):
        """Дефрагментирует базу данных"""
        if not self.current_db:
            self.show_error(self.translate("no_db_open"))
            return
        
        try:
            # Получаем все файлы таблиц
            table_files = database.get_table_files(self.current_db)
            
            for table_id, files in table_files.items():
                for file_name in files:
                    file_operations.defragment_file(file_name)
            
            self.show_success(self.translate("defrag_complete"))
                
        except Exception as e:
            self.show_error(str(e))
    
    def change_language(self):
        """Изменяет язык интерфейса"""
        options = [
            f"English {'(current)' if self.language == 'en' else ''}",
            f"Russian {'(current)' if self.language == 'ru' else ''}",
            self.translate("back")
        ]
        
        menu = TerminalMenu(options, title=self.translate("select_language"))
        selected_index = menu.show()
        
        if selected_index == 0 and self.language != 'en':
            self.language = 'en'
            self.save_settings()
            self.show_success(self.translate("language_set"))
        elif selected_index == 1 and self.language != 'ru':
            self.language = 'ru'
            self.save_settings()
            self.show_success(self.translate("language_set"))
    
    def show_instructions(self):
        """Показывает инструкцию по использованию"""
        self.show_header()
        print(Fore.WHITE + self.translate("instructions_text"))
        self.wait_for_enter()
    
    def show_settings(self):
        """Показывает меню настроек"""
        while True:
            self.show_header()
            options = [
                self.translate("change_language"),
                f"{self.translate('current_language')} ({self.language})",
                self.translate("back")
            ]
            
            menu = TerminalMenu(options, title=self.translate("settings_menu"))
            selected_index = menu.show()
            
            if selected_index == 0:
                self.change_language()
            elif selected_index == 2 or selected_index is None:
                break
    
    def main_menu(self):
        """Главное меню приложения"""
        while True:
            self.show_header()
            
            # Информация о текущей базе данных и таблице
            if self.current_db:
                print(Fore.GREEN + f"Database: {self.current_db}")
            else:
                print(Fore.YELLOW + self.translate("no_db_open"))
            
            if self.current_table:
                print(Fore.GREEN + f"Table: {self.current_table}")
            else:
                print(Fore.YELLOW + self.translate("no_table_selected"))
            
            print()
            
            # Опции меню
            options = [
                self.translate("create_db"),
                self.translate("open_db"),
                self.translate("close_db"),
                self.translate("create_table"),
                self.translate("add_record"),
                self.translate("search_record"),
                self.translate("view_records"),
                self.translate("defragment"),
                self.translate("settings"),
                self.translate("instructions"),
                self.translate("exit")
            ]
            
            menu = TerminalMenu(options, title=self.translate("main_menu"))
            selected_index = menu.show()
            
            if selected_index == 0:
                self.create_database()
            elif selected_index == 1:
                self.open_database()
            elif selected_index == 2:
                self.close_database()
            elif selected_index == 3:
                self.create_table()
            elif selected_index == 4:
                self.add_record()
            elif selected_index == 5:
                self.search_record()
            elif selected_index == 6:
                self.view_records()
            elif selected_index == 7:
                self.defragment_database()
            elif selected_index == 8:
                self.show_settings()
            elif selected_index == 9:
                self.show_instructions()
            elif selected_index == 10 or selected_index is None:
                print(Fore.YELLOW + "Goodbye!")
                break

def main():
    """Основная функция приложения"""
    app = ConsoleDatabaseApp()
    app.main_menu()

if __name__ == '__main__':
    main()