import sys
import os
import traceback
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QLabel, QLineEdit, QPushButton, 
                             QTableWidget, QTableWidgetItem, QTreeWidget, 
                             QTreeWidgetItem, QTabWidget, QMessageBox, 
                             QFileDialog, QSplitter, QComboBox, QTextEdit,
                             QGroupBox, QFormLayout, QSpinBox, QListWidget,
                             QHeaderView, QDialog, QDialogButtonBox, QInputDialog,
                             QAction, QMenu, QToolBar, QStatusBar, QCheckBox)
from PyQt5.QtCore import Qt, QSettings, QTranslator, QLocale
from PyQt5.QtGui import QIcon, QPalette, QColor
import config
import database
import file_operations
import serialization

class DatabaseApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.current_db = None
        self.current_table = None
        self.settings = QSettings("MARSoft", "DatabaseManager")
        self.translator = QTranslator()
        self.initUI()
        
    def initUI(self):
        self.setWindowTitle('Database Manager')
        self.setGeometry(100, 100, 1200, 800)
        
        # Apply saved settings
        self.apply_settings()
        
        # Create menus
        self.create_menus()
        
        # Central widget and main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)
        
        # Left panel for database and table selection
        left_panel = QWidget()
        left_panel.setMaximumWidth(300)
        left_layout = QVBoxLayout(left_panel)
        
        # Database operations
        db_group = QGroupBox(self.tr("Database Operations"))
        db_layout = QVBoxLayout(db_group)
        
        self.btn_create_db = QPushButton(self.tr("Create Database"))
        self.btn_open_db = QPushButton(self.tr("Open Database"))
        self.btn_close_db = QPushButton(self.tr("Close Database"))
        self.btn_defragment = QPushButton(self.tr("Defragment Database"))
        
        db_layout.addWidget(self.btn_create_db)
        db_layout.addWidget(self.btn_open_db)
        db_layout.addWidget(self.btn_close_db)
        db_layout.addWidget(self.btn_defragment)
        
        # Tables list
        tables_group = QGroupBox(self.tr("Tables"))
        tables_layout = QVBoxLayout(tables_group)
        
        self.tables_tree = QTreeWidget()
        self.tables_tree.setHeaderLabels([self.tr("Tables"), self.tr("Columns")])
        tables_layout.addWidget(self.tables_tree)
        
        self.btn_create_table = QPushButton(self.tr("Create Table"))
        self.btn_delete_table = QPushButton(self.tr("Delete Table"))
        
        tables_layout.addWidget(self.btn_create_table)
        tables_layout.addWidget(self.btn_delete_table)
        
        # Add groups to left layout
        left_layout.addWidget(db_group)
        left_layout.addWidget(tables_group)
        
        # Right panel for data display and operations
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        
        # Tabs for different operations
        self.tabs = QTabWidget()
        
        # Browse tab
        self.browse_tab = QWidget()
        browse_layout = QVBoxLayout(self.browse_tab)
        
        self.records_table = QTableWidget()
        browse_layout.addWidget(self.records_table)
        
        # Add record tab
        self.add_tab = QWidget()
        add_layout = QVBoxLayout(self.add_tab)
        
        self.coord_inputs = []
        self.data_input = QLineEdit()
        
        self.form_widget = QWidget()
        self.form_layout = QFormLayout(self.form_widget)
        add_layout.addWidget(self.form_widget)
        
        self.btn_add_record = QPushButton(self.tr("Add Record"))
        add_layout.addWidget(self.btn_add_record)
        
        # Search tab
        self.search_tab = QWidget()
        search_layout = QVBoxLayout(self.search_tab)
        
        self.search_coord_inputs = []
        self.search_form = QWidget()
        self.search_form_layout = QFormLayout(self.search_form)
        search_layout.addWidget(self.search_form)
        
        self.btn_search = QPushButton(self.tr("Search"))
        search_layout.addWidget(self.btn_search)
        
        self.search_result = QTextEdit()
        self.search_result.setReadOnly(True)
        search_layout.addWidget(self.search_result)
        
        # Add tabs
        self.tabs.addTab(self.browse_tab, self.tr("Browse"))
        self.tabs.addTab(self.add_tab, self.tr("Add Record"))
        self.tabs.addTab(self.search_tab, self.tr("Search"))
        
        right_layout.addWidget(self.tabs)
        
        # Add panels to main layout
        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)
        splitter.setSizes([300, 900])
        main_layout.addWidget(splitter)
        
        # Connect signals
        self.btn_create_db.clicked.connect(self.create_database)
        self.btn_open_db.clicked.connect(self.open_database)
        self.btn_close_db.clicked.connect(self.close_database)
        self.btn_defragment.clicked.connect(self.defragment_database)
        self.btn_create_table.clicked.connect(self.create_table)
        self.btn_delete_table.clicked.connect(self.delete_table)
        self.tables_tree.itemClicked.connect(self.table_selected)
        self.btn_add_record.clicked.connect(self.add_record)
        self.btn_search.clicked.connect(self.search_record)
        
        # Initialize UI state
        self.update_ui_state()
        
    def create_menus(self):
        # File menu
        file_menu = self.menuBar().addMenu(self.tr("File"))
        
        new_db_action = QAction(self.tr("New Database"), self)
        new_db_action.setShortcut("Ctrl+N")
        new_db_action.triggered.connect(self.create_database)
        file_menu.addAction(new_db_action)
        
        open_db_action = QAction(self.tr("Open Database"), self)
        open_db_action.setShortcut("Ctrl+O")
        open_db_action.triggered.connect(self.open_database)
        file_menu.addAction(open_db_action)
        
        close_db_action = QAction(self.tr("Close Database"), self)
        close_db_action.setShortcut("Ctrl+W")
        close_db_action.triggered.connect(self.close_database)
        file_menu.addAction(close_db_action)
        
        file_menu.addSeparator()
        
        exit_action = QAction(self.tr("Exit"), self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # Settings menu
        settings_menu = self.menuBar().addMenu(self.tr("Settings"))
        
        # Language submenu
        language_menu = settings_menu.addMenu(self.tr("Language"))
        
        english_action = QAction("English", self)
        english_action.triggered.connect(lambda: self.change_language("en"))
        language_menu.addAction(english_action)
        
        russian_action = QAction("Русский", self)
        russian_action.triggered.connect(lambda: self.change_language("ru"))
        language_menu.addAction(russian_action)
        
        # Theme submenu
        theme_menu = settings_menu.addMenu(self.tr("Theme"))
        
        light_action = QAction(self.tr("Light"), self)
        light_action.triggered.connect(lambda: self.change_theme("light"))
        theme_menu.addAction(light_action)
        
        dark_action = QAction(self.tr("Dark"), self)
        dark_action.triggered.connect(lambda: self.change_theme("dark"))
        theme_menu.addAction(dark_action)
        
        # Help menu
        help_menu = self.menuBar().addMenu(self.tr("Help"))
        
        instruction_action = QAction(self.tr("Instructions"), self)
        instruction_action.setShortcut("F1")
        instruction_action.triggered.connect(self.show_instructions)
        help_menu.addAction(instruction_action)
        
        about_action = QAction(self.tr("About"), self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)
        
    def apply_settings(self):
        # Apply language
        lang = self.settings.value("language", "en")
        self.change_language(lang, init=True)
        
        # Apply theme
        theme = self.settings.value("theme", "light")
        self.change_theme(theme, init=True)
        
    def change_language(self, language, init=False):
        if language == "ru":
            self.translator.load("database_ru", ".")
            QApplication.instance().installTranslator(self.translator)
        else:
            QApplication.instance().removeTranslator(self.translator)
            
        if not init:
            self.settings.setValue("language", language)
            # Show message that restart is needed for full translation
            QMessageBox.information(self, self.tr("Language Changed"), 
                                  self.tr("Please restart the application for the language change to take full effect."))
        
    def change_theme(self, theme, init=False):
        if theme == "dark":
            self.apply_dark_theme()
        else:
            self.apply_light_theme()
            
        if not init:
            self.settings.setValue("theme", theme)
        
    def apply_dark_theme(self):
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
        QApplication.setPalette(QApplication.style().standardPalette())
        
    def show_instructions(self):
        try:
            if os.path.exists("MAR.info"):
                with open("MAR.info", "r", encoding="utf-8") as f:
                    instructions = f.read()
                    
                dialog = QDialog(self)
                dialog.setWindowTitle(self.tr("Instructions"))
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
                QMessageBox.information(self, self.tr("Instructions"), 
                                      self.tr("Instruction file MAR.info not found. Please create this file with application instructions."))
        except Exception as e:
            QMessageBox.critical(self, self.tr("Error"), 
                               self.tr("Failed to load instructions: {0}").format(str(e)))
            
    def show_about(self):
        about_text = self.tr(
            "Database Manager\n\n"
            "Version 1.0\n\n"
            "A modern application for working with MAR database format.\n\n"
            "Features:\n"
            "- Create and manage databases\n"
            "- Create and modify tables\n"
            "- Add, search and view records\n"
            "- Multiple language support\n"
            "- Light and dark themes\n\n"
            "© 2023 MARSoft. All rights reserved."
        )
        
        QMessageBox.about(self, self.tr("About Database Manager"), about_text)
        
    def update_ui_state(self):
        db_open = self.current_db is not None
        table_selected = self.current_table is not None
        
        self.btn_close_db.setEnabled(db_open)
        self.btn_defragment.setEnabled(db_open)
        self.btn_create_table.setEnabled(db_open)
        self.btn_delete_table.setEnabled(db_open and table_selected)
        self.tabs.setEnabled(db_open and table_selected)
        
    def create_database(self):
        try:
            file_path, _ = QFileDialog.getSaveFileName(
                self, self.tr("Create Database"), "", self.tr("Database Files (*.marm)")
            )
            if file_path:
                database.create_database(file_path)
                self.statusBar().showMessage(self.tr("Database created: {0}").format(file_path))
        except Exception as e:
            QMessageBox.critical(self, self.tr("Error"), self.tr("Failed to create database: {0}").format(str(e)))
            
    def open_database(self):
        try:
            file_path, _ = QFileDialog.getOpenFileName(
                self, self.tr("Open Database"), "", self.tr("Database Files (*.marm)")
            )
            if file_path:
                self.current_db = file_path
                self.load_tables()
                self.statusBar().showMessage(self.tr("Database opened: {0}").format(file_path))
                self.update_ui_state()
        except Exception as e:
            QMessageBox.critical(self, self.tr("Error"), self.tr("Failed to open database: {0}").format(str(e)))
            
    def close_database(self):
        self.current_db = None
        self.current_table = None
        self.tables_tree.clear()
        self.records_table.setRowCount(0)
        self.records_table.setColumnCount(0)
        self.statusBar().showMessage(self.tr("Database closed"))
        self.update_ui_state()
        
    def defragment_database(self):
        try:
            if not self.current_db:
                QMessageBox.warning(self, self.tr("Warning"), self.tr("Please open a database first"))
                return
                
            # Get all table files
            tables = database.get_tables(self.current_db)
            table_files = database.get_table_files(self.current_db)
            
            for table_id, files in table_files.items():
                for file_name in files:
                    file_operations.defragment_file(file_name)
            
            QMessageBox.information(self, self.tr("Success"), self.tr("Database defragmented successfully"))
            self.statusBar().showMessage(self.tr("Database defragmented"))
            
        except Exception as e:
            QMessageBox.critical(self, self.tr("Error"), self.tr("Failed to defragment database: {0}").format(str(e)))
            
    def load_tables(self):
        try:
            self.tables_tree.clear()
            tables = database.get_tables(self.current_db)
            
            for table_id, table_info in tables.items():
                table_name = table_info['name']
                columns = table_info['columns']
                
                table_item = QTreeWidgetItem(self.tables_tree, [table_name])
                table_item.table_id = table_id
                table_item.table_name = table_name
                
                for col_id, col_name in columns.items():
                    column_item = QTreeWidgetItem(table_item, [self.tr("Column {0}").format(col_id), col_name])
                    
            self.tables_tree.expandAll()
        except Exception as e:
            QMessageBox.critical(self, self.tr("Error"), self.tr("Failed to load tables: {0}").format(str(e)))
            
    def create_table(self):
        try:
            dialog = CreateTableDialog(self)
            if dialog.exec_() == QDialog.Accepted:
                table_name, columns = dialog.get_data()
                if table_name and columns:
                    database.create_table(self.current_db, table_name, columns)
                    self.load_tables()
                    self.statusBar().showMessage(self.tr("Table created: {0}").format(table_name))
                    
        except Exception as e:
            QMessageBox.critical(self, self.tr("Error"), self.tr("Failed to create table: {0}").format(str(e)))
            
    def delete_table(self):
        try:
            if not self.current_table:
                QMessageBox.warning(self, self.tr("Warning"), self.tr("Please select a table first"))
                return
                
            # Get table files
            table_files = database.get_table_files(self.current_db, self.current_table)
            
            # Delete table files
            for files in table_files.values():
                for file_name in files:
                    if os.path.exists(file_name):
                        os.remove(file_name)
            
            # Delete table config
            config_file = f"config/{self.current_table}.mart"
            if os.path.exists(config_file):
                os.remove(config_file)
            
            # TODO: Remove table from database file (this would require modifying the database file)
            
            QMessageBox.information(self, self.tr("Success"), self.tr("Table {0} deleted").format(self.current_table))
            self.current_table = None
            self.load_tables()
            self.update_ui_state()
            
        except Exception as e:
            QMessageBox.critical(self, self.tr("Error"), self.tr("Failed to delete table: {0}").format(str(e)))
            
    def table_selected(self, item, column):
        try:
            if hasattr(item, 'table_id'):
                self.current_table = item.table_name
                self.load_table_data()
                self.setup_add_tab(item.table_id)
                self.setup_search_tab(item.table_id)
                self.update_ui_state()
        except Exception as e:
            QMessageBox.critical(self, self.tr("Error"), self.tr("Failed to select table: {0}").format(str(e)))
            
    def load_table_data(self):
        try:
            records = database.select_from_table(self.current_db, self.current_table)
            
            if records:
                # Get columns from table info
                tables = database.get_tables(self.current_db)
                table_info = None
                
                for table_id, info in tables.items():
                    if info['name'] == self.current_table:
                        table_info = info
                        break
                
                if table_info:
                    # Setup table
                    self.records_table.setRowCount(len(records))
                    # +1 for data column
                    self.records_table.setColumnCount(len(table_info['columns']) + 1)
                    
                    # Set headers
                    headers = []
                    for i in range(len(table_info['columns'])):
                        headers.append(table_info['columns'].get(i, self.tr("Column {0}").format(i)))
                    headers.append(self.tr("Data"))
                    self.records_table.setHorizontalHeaderLabels(headers)
                    
                    # Populate data
                    for row, record in enumerate(records):
                        cords, data_type, data_len, data, reversed_size = record
                        
                        # Add coordinate values
                        for col, coord_value in enumerate(cords):
                            if col < self.records_table.columnCount() - 1:  # -1 to exclude data column
                                self.records_table.setItem(row, col, QTableWidgetItem(str(coord_value)))
                        
                        # Add data value
                        data_col = self.records_table.columnCount() - 1
                        self.records_table.setItem(row, data_col, QTableWidgetItem(str(data)))
                    
                    # Resize columns to fit content
                    self.records_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
                    self.records_table.horizontalHeader().setStretchLastSection(True)
                    
        except Exception as e:
            QMessageBox.critical(self, self.tr("Error"), self.tr("Failed to load table data: {0}\n{1}").format(str(e), traceback.format_exc()))
            
    def setup_add_tab(self, table_id):
        # Clear previous inputs
        for i in reversed(range(self.form_layout.count())): 
            widget = self.form_layout.itemAt(i).widget()
            if widget:
                widget.setParent(None)
        
        self.coord_inputs = []
        
        # Get table structure
        tables = database.get_tables(self.current_db)
        table_info = tables[table_id]
        columns = table_info['columns']
        
        # Create coordinate inputs
        for col_id in sorted(columns.keys()):
            col_name = columns[col_id]
            input_field = QSpinBox()
            input_field.setRange(-1000000, 1000000)
            self.form_layout.addRow("{0}:".format(col_name), input_field)
            self.coord_inputs.append(input_field)
            
        # Create data input
        self.data_input = QLineEdit()
        self.form_layout.addRow(self.tr("Data:"), self.data_input)
        
    def setup_search_tab(self, table_id):
        # Clear previous inputs
        for i in reversed(range(self.search_form_layout.count())): 
            widget = self.search_form_layout.itemAt(i).widget()
            if widget:
                widget.setParent(None)
        
        self.search_coord_inputs = []
        
        # Get table structure
        tables = database.get_tables(self.current_db)
        table_info = tables[table_id]
        columns = table_info['columns']
        
        # Create coordinate inputs
        for col_id in sorted(columns.keys()):
            col_name = columns[col_id]
            input_field = QSpinBox()
            input_field.setRange(-1000000, 1000000)
            self.search_form_layout.addRow("{0}:".format(col_name), input_field)
            self.search_coord_inputs.append(input_field)
            
    def add_record(self):
        try:
            if not self.current_db or not self.current_table:
                QMessageBox.warning(self, self.tr("Warning"), self.tr("Please select a table first"))
                return
                
            # Get coordinates
            coords = [input.value() for input in self.coord_inputs]
            
            # Get data
            data = self.data_input.text()
            
            # Add record
            success = database.insert_into_table(self.current_db, self.current_table, coords, data)
            
            if success:
                QMessageBox.information(self, self.tr("Success"), self.tr("Record added successfully"))
                self.data_input.clear()
                self.load_table_data()
            else:
                QMessageBox.warning(self, self.tr("Warning"), self.tr("Failed to add record"))
                
        except Exception as e:
            QMessageBox.critical(self, self.tr("Error"), self.tr("Failed to add record: {0}").format(str(e)))
            
    def search_record(self):
        try:
            if not self.current_db or not self.current_table:
                QMessageBox.warning(self, self.tr("Warning"), self.tr("Please select a table first"))
                return
                
            # Get coordinates
            coords = [input.value() for input in self.search_coord_inputs]
            
            # Search for record
            result = database.find_in_table(self.current_db, self.current_table, coords)
            
            # Display result
            if result:
                cords, data_type, data_len, data, reversed_size = result
                result_text = self.tr("Found record:\nCoordinates: {0}\nData Type: {1}\nData: {2}").format(
                    cords, data_type.__name__, data)
                self.search_result.setText(result_text)
            else:
                self.search_result.setText(self.tr("No record found with the specified coordinates"))
                
        except Exception as e:
            QMessageBox.critical(self, self.tr("Error"), self.tr("Failed to search record: {0}").format(str(e)))


class CreateTableDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(parent.tr("Create Table"))
        self.setModal(True)
        self.setup_ui()
        
    def setup_ui(self):
        layout = QVBoxLayout(self)
        
        # Table name
        name_layout = QHBoxLayout()
        name_layout.addWidget(QLabel(self.tr("Table Name:")))
        self.name_input = QLineEdit()
        name_layout.addWidget(self.name_input)
        layout.addLayout(name_layout)
        
        # Columns
        layout.addWidget(QLabel(self.tr("Columns:")))
        self.columns_list = QListWidget()
        layout.addWidget(self.columns_list)
        
        # Buttons for adding/removing columns
        btn_layout = QHBoxLayout()
        self.add_btn = QPushButton(self.tr("Add Column"))
        self.remove_btn = QPushButton(self.tr("Remove Selected"))
        btn_layout.addWidget(self.add_btn)
        btn_layout.addWidget(self.remove_btn)
        layout.addLayout(btn_layout)
        
        # Dialog buttons
        self.buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        layout.addWidget(self.buttons)
        
        # Connect signals
        self.add_btn.clicked.connect(self.add_column)
        self.remove_btn.clicked.connect(self.remove_column)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        
    def add_column(self):
        name, ok = QInputDialog.getText(
            self, self.tr("Column Name"), self.tr("Enter column name:")
        )
        if ok and name:
            self.columns_list.addItem(name)
            
    def remove_column(self):
        row = self.columns_list.currentRow()
        if row >= 0:
            self.columns_list.takeItem(row)
            
    def get_data(self):
        columns = []
        for i in range(self.columns_list.count()):
            columns.append(self.columns_list.item(i).text())
        return self.name_input.text(), columns


if __name__ == '__main__':
    app = QApplication(sys.argv)
    
    # Set organization and application name for QSettings
    QApplication.setOrganizationName("MARSoft")
    QApplication.setApplicationName("DatabaseManager")
    
    window = DatabaseApp()
    window.show()
    sys.exit(app.exec_())