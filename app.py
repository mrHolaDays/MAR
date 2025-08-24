import sys
import os
import traceback
import socket
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QLabel, QLineEdit, QPushButton, 
                             QTableWidget, QTableWidgetItem, QTreeWidget, 
                             QTreeWidgetItem, QTabWidget, QMessageBox, 
                             QFileDialog, QSplitter, QComboBox, QTextEdit,
                             QGroupBox, QFormLayout, QSpinBox, QListWidget,
                             QHeaderView, QDialog, QDialogButtonBox, QInputDialog,
                             QAction, QMenu, QToolBar, QStatusBar, QCheckBox,
                             QGridLayout, QProgressBar)
from PyQt5.QtCore import Qt, QSettings, QTranslator, QLocale, QThread, pyqtSignal
from PyQt5.QtGui import QIcon, QPalette, QColor
import config
import database
import file_operations
import serialization
import mardb

class ServerConnectionThread(QThread):
    """Поток для проверки соединения с сервером"""
    connection_result = pyqtSignal(bool, str)

    def __init__(self, host, port):
        super().__init__()
        self.host = host
        self.port = port

    def run(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((self.host, self.port))
            sock.close()
            self.connection_result.emit(result == 0, f"{self.host}:{self.port}")
        except Exception as e:
            self.connection_result.emit(False, str(e))

class ServerDiscoveryThread(QThread):
    """Поток для поиска локальных серверов"""
    server_found = pyqtSignal(str, int)
    finished = pyqtSignal()

    def __init__(self, network_prefix="192.168.1"):
        super().__init__()
        self.network_prefix = network_prefix

    def run(self):
        # Проверяем локальный хост
        self.check_server("localhost", 9999)
        
        # Сканируем локальную сеть (упрощенная версия)
        for i in range(1, 255):
            ip = f"{self.network_prefix}.{i}"
            self.check_server(ip, 9999)
        
        self.finished.emit()

    def check_server(self, host, port):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(0.5)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                self.server_found.emit(host, port)
        except:
            pass

class ConnectionDialog(QDialog):
    """Диалог подключения к серверу"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(parent.tr("Подключение к серверу"))
        self.setModal(True)
        self.setup_ui()
        
    def setup_ui(self):
        layout = QVBoxLayout(self)
        
        # Форма ввода
        form_layout = QFormLayout()
        
        self.host_input = QLineEdit("localhost")
        self.port_input = QSpinBox()
        self.port_input.setRange(1, 65535)
        self.port_input.setValue(9999)
        
        form_layout.addRow(self.tr("Хост:"), self.host_input)
        form_layout.addRow(self.tr("Порт:"), self.port_input)
        
        layout.addLayout(form_layout)
        
        # Кнопки
        buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        
        layout.addWidget(buttons)
        
    def get_connection_params(self):
        return self.host_input.text(), self.port_input.value()

class DatabaseApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.current_db = None
        self.current_table = None
        self.db_client = None  # Клиент для работы с базой
        self.connection_mode = "local"  # Режим подключения
        self.settings = QSettings("MARSoft", "DatabaseManager")
        self.translator = QTranslator()
        self.initUI()
        
    def initUI(self):
        self.setWindowTitle('Database Manager')
        self.setGeometry(100, 100, 1200, 800)
        
        # Применяем сохраненные настройки
        self.apply_settings()
        
        # Создаем меню
        self.create_menus()
        
        # Центральный виджет и основной layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)
        
        # Левая панель для выбора базы данных и таблиц
        left_panel = QWidget()
        left_panel.setMaximumWidth(300)
        left_layout = QVBoxLayout(left_panel)
        
        # Информация о подключении
        self.connection_info = QLabel(self.tr("Локальный режим"))
        left_layout.addWidget(self.connection_info)
        
        # Операции с базой данных
        db_group = QGroupBox(self.tr("Операции с базой данных"))
        db_layout = QVBoxLayout(db_group)
        
        self.btn_create_db = QPushButton(self.tr("Создать базу данных"))
        self.btn_open_db = QPushButton(self.tr("Открыть базу данных"))
        self.btn_close_db = QPushButton(self.tr("Закрыть базу данных"))
        self.btn_defragment = QPushButton(self.tr("Дефрагментировать базу"))
        
        db_layout.addWidget(self.btn_create_db)
        db_layout.addWidget(self.btn_open_db)
        db_layout.addWidget(self.btn_close_db)
        db_layout.addWidget(self.btn_defragment)
        
        # Список таблиц
        tables_group = QGroupBox(self.tr("Таблицы"))
        tables_layout = QVBoxLayout(tables_group)
        
        self.tables_tree = QTreeWidget()
        self.tables_tree.setHeaderLabels([self.tr("Таблицы"), self.tr("Колонки")])
        tables_layout.addWidget(self.tables_tree)
        
        self.btn_create_table = QPushButton(self.tr("Создать таблицу"))
        self.btn_delete_table = QPushButton(self.tr("Удалить таблицу"))
        
        tables_layout.addWidget(self.btn_create_table)
        tables_layout.addWidget(self.btn_delete_table)
        
        # Добавляем группы на левую панель
        left_layout.addWidget(db_group)
        left_layout.addWidget(tables_group)
        
        # Правая панель для отображения данных и операций
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        
        # Вкладки для различных операций
        self.tabs = QTabWidget()
        
        # Вкладка просмотра
        self.browse_tab = QWidget()
        browse_layout = QVBoxLayout(self.browse_tab)
        
        self.records_table = QTableWidget()
        browse_layout.addWidget(self.records_table)
        
        # Вкладка добавления записи
        self.add_tab = QWidget()
        add_layout = QVBoxLayout(self.add_tab)
        
        self.coord_inputs = []
        self.data_input = QLineEdit()
        
        self.form_widget = QWidget()
        self.form_layout = QFormLayout(self.form_widget)
        add_layout.addWidget(self.form_widget)
        
        self.btn_add_record = QPushButton(self.tr("Добавить запись"))
        add_layout.addWidget(self.btn_add_record)
        
        # Вкладка поиска
        self.search_tab = QWidget()
        search_layout = QVBoxLayout(self.search_tab)
        
        self.search_coord_inputs = []
        self.search_form = QWidget()
        self.search_form_layout = QFormLayout(self.search_form)
        search_layout.addWidget(self.search_form)
        
        self.btn_search = QPushButton(self.tr("Поиск"))
        search_layout.addWidget(self.btn_search)
        
        self.search_result = QTextEdit()
        self.search_result.setReadOnly(True)
        search_layout.addWidget(self.search_result)
        
        # Добавляем вкладки
        self.tabs.addTab(self.browse_tab, self.tr("Просмотр"))
        self.tabs.addTab(self.add_tab, self.tr("Добавить запись"))
        self.tabs.addTab(self.search_tab, self.tr("Поиск"))
        
        right_layout.addWidget(self.tabs)
        
        # Добавляем панели в основной layout
        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)
        splitter.setSizes([300, 900])
        main_layout.addWidget(splitter)
        
        # Подключаем сигналы
        self.btn_create_db.clicked.connect(self.create_database)
        self.btn_open_db.clicked.connect(self.open_database)
        self.btn_close_db.clicked.connect(self.close_database)
        self.btn_defragment.clicked.connect(self.defragment_database)
        self.btn_create_table.clicked.connect(self.create_table)
        self.btn_delete_table.clicked.connect(self.delete_table)
        self.tables_tree.itemClicked.connect(self.table_selected)
        self.btn_add_record.clicked.connect(self.add_record)
        self.btn_search.clicked.connect(self.search_record)
        
        # Инициализируем состояние UI
        self.update_ui_state()
        
    def create_menus(self):
        # Меню Файл
        file_menu = self.menuBar().addMenu(self.tr("Файл"))
        
        new_db_action = QAction(self.tr("Новая база данных"), self)
        new_db_action.setShortcut("Ctrl+N")
        new_db_action.triggered.connect(self.create_database)
        file_menu.addAction(new_db_action)
        
        open_db_action = QAction(self.tr("Открыть базу данных"), self)
        open_db_action.setShortcut("Ctrl+O")
        open_db_action.triggered.connect(self.open_database)
        file_menu.addAction(open_db_action)
        
        close_db_action = QAction(self.tr("Закрыть базу данных"), self)
        close_db_action.setShortcut("Ctrl+W")
        close_db_action.triggered.connect(self.close_database)
        file_menu.addAction(close_db_action)
        
        file_menu.addSeparator()
        
        exit_action = QAction(self.tr("Выход"), self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # Меню Подключение
        connection_menu = self.menuBar().addMenu(self.tr("Подключение"))
        
        local_mode_action = QAction(self.tr("Локальный режим"), self)
        local_mode_action.triggered.connect(lambda: self.set_connection_mode("local"))
        connection_menu.addAction(local_mode_action)
        
        server_mode_action = QAction(self.tr("Подключиться к серверу"), self)
        server_mode_action.triggered.connect(self.connect_to_server)
        connection_menu.addAction(server_mode_action)
        
        discover_servers_action = QAction(self.tr("Найти серверы в сети"), self)
        discover_servers_action.triggered.connect(self.discover_servers)
        connection_menu.addAction(discover_servers_action)
        
        # Меню Настройки
        settings_menu = self.menuBar().addMenu(self.tr("Настройки"))
        
        # Подменю Язык
        language_menu = settings_menu.addMenu(self.tr("Язык"))
        
        english_action = QAction("English", self)
        english_action.triggered.connect(lambda: self.change_language("en"))
        language_menu.addAction(english_action)
        
        russian_action = QAction("Русский", self)
        russian_action.triggered.connect(lambda: self.change_language("ru"))
        language_menu.addAction(russian_action)
        
        # Подменю Тема
        theme_menu = settings_menu.addMenu(self.tr("Тема"))
        
        light_action = QAction(self.tr("Светлая"), self)
        light_action.triggered.connect(lambda: self.change_theme("light"))
        theme_menu.addAction(light_action)
        
        dark_action = QAction(self.tr("Темная"), self)
        dark_action.triggered.connect(lambda: self.change_theme("dark"))
        theme_menu.addAction(dark_action)
        
        # Меню Помощь
        help_menu = self.menuBar().addMenu(self.tr("Помощь"))
        
        instruction_action = QAction(self.tr("Инструкции"), self)
        instruction_action.setShortcut("F1")
        instruction_action.triggered.connect(self.show_instructions)
        help_menu.addAction(instruction_action)
        
        about_action = QAction(self.tr("О программе"), self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)
        
    def apply_settings(self):
        # Применяем язык
        lang = self.settings.value("language", "en")
        self.change_language(lang, init=True)
        
        # Применяем тему
        theme = self.settings.value("theme", "light")
        self.change_theme(theme, init=True)
        
    def set_connection_mode(self, mode, host=None, port=None):
        """Устанавливает режим подключения (локальный/серверный)"""
        self.connection_mode = mode
        
        if mode == "local":
            self.db_client = None
            self.connection_info.setText(self.tr("Локальный режим"))
            self.statusBar().showMessage(self.tr("Переключен в локальный режим"))
        elif mode == "server":
            try:
                self.db_client = mardb.MARDB(mode='server', host=host, port=port)
                self.connection_info.setText(self.tr("Сервер: {0}:{1}").format(host, port))
                self.statusBar().showMessage(self.tr("Подключено к серверу {0}:{1}").format(host, port))
            except Exception as e:
                QMessageBox.critical(self, self.tr("Ошибка подключения"), 
                                   self.tr("Не удалось подключиться к серверу: {0}").format(str(e)))
                self.set_connection_mode("local")
        
    def connect_to_server(self):
        """Подключается к серверу базы данных"""
        dialog = ConnectionDialog(self)
        if dialog.exec_() == QDialog.Accepted:
            host, port = dialog.get_connection_params()
            self.set_connection_mode("server", host, port)
            
    def discover_servers(self):
        """Ищет серверы в локальной сети"""
        self.statusBar().showMessage(self.tr("Поиск серверов..."))
        
        # Создаем диалог с прогрессом
        progress_dialog = QDialog(self)
        progress_dialog.setWindowTitle(self.tr("Поиск серверов"))
        progress_dialog.setModal(True)
        progress_layout = QVBoxLayout(progress_dialog)
        
        progress_label = QLabel(self.tr("Сканирование сети..."))
        progress_bar = QProgressBar()
        progress_bar.setRange(0, 0)  # Неопределенный прогресс
        
        progress_layout.addWidget(progress_label)
        progress_layout.addWidget(progress_bar)
        progress_dialog.show()
        
        # Запускаем поиск в отдельном потоке
        self.discovery_thread = ServerDiscoveryThread()
        self.discovery_thread.server_found.connect(self.on_server_found)
        self.discovery_thread.finished.connect(progress_dialog.accept)
        self.discovery_thread.start()
        
    def on_server_found(self, host, port):
        """Обрабатывает найденный сервер"""
        # Предлагаем подключиться к найденному серверу
        reply = QMessageBox.question(self, self.tr("Сервер найден"),
                                  self.tr("Найден сервер {0}:{1}. Подключиться?").format(host, port),
                                  QMessageBox.Yes | QMessageBox.No)
        
        if reply == QMessageBox.Yes:
            self.set_connection_mode("server", host, port)
        
    def change_language(self, language, init=False):
        # Изменение языка интерфейса
        if language == "ru":
            self.translator.load("database_ru", ".")
            QApplication.instance().installTranslator(self.translator)
        else:
            QApplication.instance().removeTranslator(self.translator)
            
        if not init:
            self.settings.setValue("language", language)
            QMessageBox.information(self, self.tr("Язык изменен"), 
                                  self.tr("Перезапустите приложение для полного применения изменений."))
        
    def change_theme(self, theme, init=False):
        # Изменение темы интерфейса
        if theme == "dark":
            self.apply_dark_theme()
        else:
            self.apply_light_theme()
            
        if not init:
            self.settings.setValue("theme", theme)
        
    def apply_dark_theme(self):
        # Применение темной темы
        dark_palette = QPalette()
        dark_palette.setColor(QPalette.Window, QColor(53, 53, 53))
        dark_palette.setColor(QPalette.WindowText, Qt.white)
        dark_palette.setColor(QPalette.Base, QColor(25, 25, 25))
        dark_palette.setColor(QPalette.AlternateBase, QColor(53, 53, 53))
        dark_palette.setColor(QPalette.ToolTipBase, Qt.white)
        dark_palette.setColor(QPalette.ToolTipText, Qt.white)
        dark_palette.setColor(QPalette.Text, Qt.white)
        dark_palette.setColor(QPalette.Button, QColor(53, 53, 53))
        dark_palette.setColor(QPalette.ButtonText, Qt.white)
        dark_palette.setColor(QPalette.BrightText, Qt.red)
        dark_palette.setColor(QPalette.Link, QColor(42, 130, 218))
        dark_palette.setColor(QPalette.Highlight, QColor(42, 130, 218))
        dark_palette.setColor(QPalette.HighlightedText, Qt.black)
        
        QApplication.setPalette(dark_palette)
        
    def apply_light_theme(self):
        # Применение светлой темы
        QApplication.setPalette(QApplication.style().standardPalette())
        
    def show_instructions(self):
        # Показать инструкции
        try:
            if os.path.exists("MAR.info"):
                with open("MAR.info", "r", encoding="utf-8") as f:
                    instructions = f.read()
                    
                dialog = QDialog(self)
                dialog.setWindowTitle(self.tr("Инструкции"))
                dialog.setModal(True)
                dialog.setMinimumSize(600, 400)
                
                layout = QVBoxLayout(dialog)
                
                text_edit = QTextEdit()
                text_edit.setReadOnly(True)
                text_edit.setPlainText(instructions)
                layout.addWidget(text_edit)
                
                buttons = QDialogButtonBox(QDialogButtonBox.Ok)
                buttons.accepted.connect(dialog.accept)
                layout.addWidget(buttons)
                
                dialog.exec_()
            else:
                QMessageBox.information(self, self.tr("Инструкции"), 
                                      self.tr("Файл инструкций MAR.info не найден."))
        except Exception as e:
            QMessageBox.critical(self, self.tr("Ошибка"), 
                               self.tr("Не удалось загрузить инструкции: {0}").format(str(e)))
            
    def show_about(self):
        # Показать информацию о программе
        about_text = self.tr(
            "Database Manager\n\n"
            "Версия 1.0\n\n"
            "Современное приложение для работы с базами данных формата MAR.\n\n"
            "Возможности:\n"
            "- Создание и управление базами данных\n"
            "- Создание и изменение таблиц\n"
            "- Добавление, поиск и просмотр записей\n"
            "- Поддержка нескольких языков\n"
            "- Светлая и темная темы\n"
            "- Работа в локальном и сетевом режиме\n\n"
        )
        
        QMessageBox.about(self, self.tr("О программе Database Manager"), about_text)
        
    def update_ui_state(self):
        # Обновление состояния интерфейса
        db_open = self.current_db is not None
        table_selected = self.current_table is not None
        
        self.btn_close_db.setEnabled(db_open)
        self.btn_defragment.setEnabled(db_open)
        self.btn_create_table.setEnabled(db_open)
        self.btn_delete_table.setEnabled(db_open and table_selected)
        self.tabs.setEnabled(db_open and table_selected)
        
    def create_database(self):
        # Создание новой базы данных
        try:
            file_path, _ = QFileDialog.getSaveFileName(
                self, self.tr("Создать базу данных"), "", self.tr("Файлы баз данных (*.marm)")
            )
            if file_path:
                if self.connection_mode == "local":
                    database.create_database(file_path)
                else:
                    self.db_client.create_database(file_path)
                self.statusBar().showMessage(self.tr("База данных создана: {0}").format(file_path))
        except Exception as e:
            QMessageBox.critical(self, self.tr("Ошибка"), self.tr("Не удалось создать базу данных: {0}").format(str(e)))
            
    def open_database(self):
        # Открытие базы данных
        try:
            if self.connection_mode == "local":
                file_path, _ = QFileDialog.getOpenFileName(
                    self, self.tr("Открыть базу данных"), "", self.tr("Файлы баз данных (*.marm)")
                )
                if file_path:
                    self.current_db = file_path
                    self.load_tables()
                    self.statusBar().showMessage(self.tr("База данных открыта: {0}").format(file_path))
            else:
                # В серверном режиме получаем список доступных баз
                db_name, ok = QInputDialog.getText(
                    self, self.tr("Открыть базу данных"), 
                    self.tr("Введите имя базы данных:")
                )
                if ok and db_name:
                    self.current_db = db_name
                    self.load_tables()
                    self.statusBar().showMessage(self.tr("База данных открыта: {0}").format(db_name))
                    
            self.update_ui_state()
        except Exception as e:
            QMessageBox.critical(self, self.tr("Ошибка"), self.tr("Не удалось открыть базу данных: {0}").format(str(e)))
            
    def close_database(self):
        # Закрытие базы данных
        self.current_db = None
        self.current_table = None
        self.tables_tree.clear()
        self.records_table.setRowCount(0)
        self.records_table.setColumnCount(0)
        self.statusBar().showMessage(self.tr("База данных закрыта"))
        self.update_ui_state()
        
    def defragment_database(self):
        # Дефрагментация базы данных
        try:
            if not self.current_db:
                QMessageBox.warning(self, self.tr("Предупреждение"), self.tr("Сначала откройте базу данных"))
                return
                
            if self.connection_mode == "local":
                # Локальная дефрагментация
                table_files = database.get_table_files(self.current_db)
                for table_id, files in table_files.items():
                    for file_name in files:
                        file_operations.defragment_file(file_name)
            else:
                # Серверная дефрагментация
                self.db_client.defragment_database(self.current_db)
            
            QMessageBox.information(self, self.tr("Успех"), self.tr("База данных дефрагментирована"))
            self.statusBar().showMessage(self.tr("База данных дефрагментирована"))
            
        except Exception as e:
            QMessageBox.critical(self, self.tr("Ошибка"), self.tr("Не удалось дефрагментировать базу данных: {0}").format(str(e)))
            
    def load_tables(self):
        # Загрузка списка таблиц
        try:
            self.tables_tree.clear()
            
            if self.connection_mode == "local":
                tables = database.get_tables(self.current_db)
            else:
                tables = self.db_client.get_tables(self.current_db)
            
            for table_id, table_info in tables.items():
                table_name = table_info['name']
                columns = table_info['columns']
                
                table_item = QTreeWidgetItem(self.tables_tree, [table_name])
                table_item.table_id = table_id
                table_item.table_name = table_name
                
                for col_id, col_name in columns.items():
                    column_item = QTreeWidgetItem(table_item, [self.tr("Колонка {0}").format(col_id), col_name])
                    
            self.tables_tree.expandAll()
        except Exception as e:
            QMessageBox.critical(self, self.tr("Ошибка"), self.tr("Не удалось загрузить таблицы: {0}").format(str(e)))
            
    def create_table(self):
        # Создание новой таблицы
        try:
            dialog = CreateTableDialog(self)
            if dialog.exec_() == QDialog.Accepted:
                table_name, columns = dialog.get_data()
                if table_name and columns:
                    if self.connection_mode == "local":
                        database.create_table(self.current_db, table_name, columns)
                    else:
                        self.db_client.create_table(self.current_db, table_name, columns)
                    self.load_tables()
                    self.statusBar().showMessage(self.tr("Таблица создана: {0}").format(table_name))
                    
        except Exception as e:
            QMessageBox.critical(self, self.tr("Ошибка"), self.tr("Не удалось создать таблицу: {0}").format(str(e)))
            
    def delete_table(self):
        # Удаление таблицы
        try:
            if not self.current_table:
                QMessageBox.warning(self, self.tr("Предупреждение"), self.tr("Сначала выберите таблицу"))
                return
                
            if self.connection_mode == "local":
                # Локальное удаление таблицы
                table_files = database.get_table_files(self.current_db, self.current_table)
                for files in table_files.values():
                    for file_name in files:
                        if os.path.exists(file_name):
                            os.remove(file_name)
                
                config_file = f"config/{self.current_table}.mart"
                if os.path.exists(config_file):
                    os.remove(config_file)
            else:
                # Серверное удаление таблицы (требует реализации на сервере)
                QMessageBox.warning(self, self.tr("Предупреждение"), 
                                  self.tr("Удаление таблиц в серверном режиме не реализовано"))
                return
            
            QMessageBox.information(self, self.tr("Успех"), self.tr("Таблица {0} удалена").format(self.current_table))
            self.current_table = None
            self.load_tables()
            self.update_ui_state()
            
        except Exception as e:
            QMessageBox.critical(self, self.tr("Ошибка"), self.tr("Не удалось удалить таблицу: {0}").format(str(e)))
            
    def table_selected(self, item, column):
        # Обработка выбора таблицы
        try:
            if hasattr(item, 'table_id'):
                self.current_table = item.table_name
                self.load_table_data()
                self.setup_add_tab(item.table_id)
                self.setup_search_tab(item.table_id)
                self.update_ui_state()
        except Exception as e:
            QMessageBox.critical(self, self.tr("Ошибка"), self.tr("Не удалось выбрать таблицу: {0}").format(str(e)))
            
    def load_table_data(self):
        # Загрузка данных таблицы
        try:
            if self.connection_mode == "local":
                records = database.select_from_table(self.current_db, self.current_table)
            else:
                records = self.db_client.select_from_table(self.current_db, self.current_table)
            
            if records:
                # Получаем информацию о таблице
                if self.connection_mode == "local":
                    tables = database.get_tables(self.current_db)
                else:
                    tables = self.db_client.get_tables(self.current_db)
                
                table_info = None
                for table_id, info in tables.items():
                    if info['name'] == self.current_table:
                        table_info = info
                        break
                
                if table_info:
                    # Настраиваем таблицу
                    self.records_table.setRowCount(len(records))
                    self.records_table.setColumnCount(len(table_info['columns']) + 1)
                    
                    # Устанавливаем заголовки
                    headers = []
                    for i in range(len(table_info['columns'])):
                        headers.append(table_info['columns'].get(i, self.tr("Колонка {0}").format(i)))
                    headers.append(self.tr("Данные"))
                    self.records_table.setHorizontalHeaderLabels(headers)
                    
                    # Заполняем данными
                    for row, record in enumerate(records):
                        if self.connection_mode == "local":
                            cords, data_type, data_len, data, reversed_size = record
                        else:
                            # В серверном режиме данные уже сериализованы
                            cords = record['cords']
                            data = record['data']
                        
                        # Добавляем значения координат
                        for col, coord_value in enumerate(cords):
                            if col < self.records_table.columnCount() - 1:
                                self.records_table.setItem(row, col, QTableWidgetItem(str(coord_value)))
                        
                        # Добавляем значение данных
                        data_col = self.records_table.columnCount() - 1
                        self.records_table.setItem(row, data_col, QTableWidgetItem(str(data)))
                    
                    # Настраиваем размеры колонок
                    self.records_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
                    self.records_table.horizontalHeader().setStretchLastSection(True)
                    
        except Exception as e:
            QMessageBox.critical(self, self.tr("Ошибка"), self.tr("Не удалось загрузить данные таблицы: {0}\n{1}").format(str(e), traceback.format_exc()))
            
    def setup_add_tab(self, table_id):
        # Настройка вкладки добавления записи
        # Очищаем предыдущие поля ввода
        for i in reversed(range(self.form_layout.count())): 
            widget = self.form_layout.itemAt(i).widget()
            if widget:
                widget.setParent(None)
        
        self.coord_inputs = []
        
        # Получаем структуру таблицы
        if self.connection_mode == "local":
            tables = database.get_tables(self.current_db)
        else:
            tables = self.db_client.get_tables(self.current_db)
        
        table_info = tables[table_id]
        columns = table_info['columns']
        
        # Создаем поля ввода для координат
        for col_id in sorted(columns.keys()):
            col_name = columns[col_id]
            input_field = QSpinBox()
            input_field.setRange(-1000000, 1000000)
            self.form_layout.addRow("{0}:".format(col_name), input_field)
            self.coord_inputs.append(input_field)
            
        # Создаем поле ввода для данных
        self.data_input = QLineEdit()
        self.form_layout.addRow(self.tr("Данные:"), self.data_input)
        
    def setup_search_tab(self, table_id):
        # Настройка вкладки поиска
        # Очищаем предыдущие поля ввода
        for i in reversed(range(self.search_form_layout.count())): 
            widget = self.search_form_layout.itemAt(i).widget()
            if widget:
                widget.setParent(None)
        
        self.search_coord_inputs = []
        
        # Получаем структуру таблицы
        if self.connection_mode == "local":
            tables = database.get_tables(self.current_db)
        else:
            tables = self.db_client.get_tables(self.current_db)
        
        table_info = tables[table_id]
        columns = table_info['columns']
        
        # Создаем поля ввода для координат
        for col_id in sorted(columns.keys()):
            col_name = columns[col_id]
            input_field = QSpinBox()
            input_field.setRange(-1000000, 1000000)
            self.search_form_layout.addRow("{0}:".format(col_name), input_field)
            self.search_coord_inputs.append(input_field)
            
    def add_record(self):
        # Добавление новой записи
        try:
            if not self.current_db or not self.current_table:
                QMessageBox.warning(self, self.tr("Предупреждение"), self.tr("Сначала выберите таблицу"))
                return
                
            # Получаем координаты
            coords = [input.value() for input in self.coord_inputs]
            
            # Получаем данные
            data = self.data_input.text()
            
            # Добавляем запись
            if self.connection_mode == "local":
                success = database.insert_into_table(self.current_db, self.current_table, coords, data)
            else:
                success = self.db_client.insert_into_table(self.current_db, self.current_table, coords, data)
            
            if success:
                QMessageBox.information(self, self.tr("Успех"), self.tr("Запись добавлена"))
                self.data_input.clear()
                self.load_table_data()
            else:
                QMessageBox.warning(self, self.tr("Предупреждение"), self.tr("Не удалось добавить запись"))
                
        except Exception as e:
            QMessageBox.critical(self, self.tr("Ошибка"), self.tr("Не удалось добавить запись: {0}").format(str(e)))
            
    def search_record(self):
        # Поиск записи
        try:
            if not self.current_db or not self.current_table:
                QMessageBox.warning(self, self.tr("Предупреждение"), self.tr("Сначала выберите таблицу"))
                return
                
            # Получаем координаты
            coords = [input.value() for input in self.search_coord_inputs]
            
            # Ищем запись
            if self.connection_mode == "local":
                result = database.find_in_table(self.current_db, self.current_table, coords)
            else:
                result = self.db_client.find_in_table(self.current_db, self.current_table, coords)
            
            # Отображаем результат
            if result:
                if self.connection_mode == "local":
                    cords, data_type, data_len, data, reversed_size = result
                    result_text = self.tr("Найдена запись:\nКоординаты: {0}\nТип данных: {1}\nДанные: {2}").format(
                        cords, data_type.__name__, data)
                else:
                    result_text = self.tr("Найдена запись:\nКоординаты: {0}\nТип данных: {1}\nДанные: {2}").format(
                        result['cords'], result['data_type'], result['data'])
                self.search_result.setText(result_text)
            else:
                self.search_result.setText(self.tr("Запись с указанными координатами не найдена"))
                
        except Exception as e:
            QMessageBox.critical(self, self.tr("Ошибка"), self.tr("Не удалось выполнить поиск: {0}").format(str(e)))


class CreateTableDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(parent.tr("Создать таблицу"))
        self.setModal(True)
        self.setup_ui()
        
    def setup_ui(self):
        layout = QVBoxLayout(self)
        
        # Название таблицы
        name_layout = QHBoxLayout()
        name_layout.addWidget(QLabel(self.tr("Название таблицы:")))
        self.name_input = QLineEdit()
        name_layout.addWidget(self.name_input)
        layout.addLayout(name_layout)
        
        # Колонки
        layout.addWidget(QLabel(self.tr("Колонки:")))
        self.columns_list = QListWidget()
        layout.addWidget(self.columns_list)
        
        # Кнопки для добавления/удаления колонок
        btn_layout = QHBoxLayout()
        self.add_btn = QPushButton(self.tr("Добавить колонку"))
        self.remove_btn = QPushButton(self.tr("Удалить выбранную"))
        btn_layout.addWidget(self.add_btn)
        btn_layout.addWidget(self.remove_btn)
        layout.addLayout(btn_layout)
        
        # Кнопки диалога
        self.buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        layout.addWidget(self.buttons)
        
        # Подключаем сигналы
        self.add_btn.clicked.connect(self.add_column)
        self.remove_btn.clicked.connect(self.remove_column)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        
    def add_column(self):
        # Добавление новой колонки
        name, ok = QInputDialog.getText(
            self, self.tr("Название колонки"), self.tr("Введите название колонки:")
        )
        if ok and name:
            self.columns_list.addItem(name)
            
    def remove_column(self):
        # Удаление выбранной колонки
        row = self.columns_list.currentRow()
        if row >= 0:
            self.columns_list.takeItem(row)
            
    def get_data(self):
        # Получение данных из диалога
        columns = []
        for i in range(self.columns_list.count()):
            columns.append(self.columns_list.item(i).text())
        return self.name_input.text(), columns


if __name__ == '__main__':
    app = QApplication(sys.argv)
    
    # Устанавливаем настройки для QSettings
    QApplication.setOrganizationName("MARSoft")
    QApplication.setApplicationName("DatabaseManager")
    
    window = DatabaseApp()
    window.show()
    sys.exit(app.exec_())