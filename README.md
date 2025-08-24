# MAR Database Library (marlib)

Библиотека для работы с базами данных в формате MAR. Предоставляет API для создания, управления и взаимодействия с базами данных, поддерживает как локальную работу, так и клиент-серверную архитектуру.

## Возможности

- Создание и управление базами данных в формате MAR
- Работа с таблицами и записями через программный интерфейс
- Клиент-серверная архитектура с кэшированием и синхронизацией
- Поддержка множества типов данных Python
- Сериализация и десериализация сложных структур данных
- Оптимизация хранения данных через дефрагментацию

## Установка

Установите библиотеку через pip:

```bash
pip install marlib
```

Или установите из исходного кода:

```bash
git clone https://github.com/your-username/marlib.git
cd marlib
pip install -e .
```

## Быстрый старт

### Базовые операции

```python
import marlib

# Инициализация в локальном режиме
db = marlib.MARDB(mode='local')

# Создание базы данных
db.create_database('test.marm')

# Создание таблицы
db.create_table('test.marm', 'users', ['id', 'name', 'age'])

# Добавление записи
db.insert_into_table('test.marm', 'users', [1, 2, 3], 'John Doe')

# Поиск записи
result = db.find_in_table('test.marm', 'users', [1, 2, 3])
print(result)

# Выборка всех записей
records = db.select_from_table('test.marm', 'users')
print(records)

# Дефрагментация базы данных
db.defragment_database('test.marm')
```

### Работа с сервером

```python
import marlib

# Подключение к серверу
db = marlib.MARDB(mode='server', host='localhost', port=9999)

# Загрузка базы данных на сервере
db.load_database('test.marm', mode='fast')

# Далее операции аналогичны локальному режиму
```

### Запуск сервера

```python
from marlib import MARDatabaseServer

# Создание и запуск сервера
server = MARDatabaseServer(host='localhost', port=9999, log_level='INFO')
server.start()
```

Или через командную строку:

```bash
python -m marlib.mardb_server --host localhost --port 9999 --log-level INFO
```

## Структура проекта

```
marlib/
├── __init__.py         # Основной файл пакета
├── config.py           # Конфигурационные константы
├── database.py         # Функции работы с БД
├── file_operations.py  # Операции с файлами
├── mardb.py           # Основной класс для работы с БД
├── mardb_server.py    # Серверная реализация
└── serialization.py   # Сериализация данных
```

## Формат данных MAR

### Структура базы данных

- Файл базы данных имеет расширение `.marm`
- Данные таблиц хранятся в отдельных файлах с расширением `.marc`
- Конфигурация таблиц хранится в файлах с расширением `.mart`

### Координатная адресация

Каждая запись в таблице идентифицируется набором координат (целых чисел), которые используются для поиска и доступа к записям.

### Поддерживаемые типы данных

Библиотека поддерживает сериализацию множества типов данных Python:

- Базовые типы: str, int, float, bool, None
- Коллекции: list, tuple, set, dict, deque
- Специальные типы: datetime, UUID, Decimal, Path
- И многие другие

## Документация

Полная документация доступна в файле [MAR.md](MAR.md), который содержит:

- Подробное описание формата MAR
- API reference библиотеки
- Примеры использования
- Описание внутренней структуры данных

---

# MAR Format Documentation

## Общее описание

Формат MAR (Multidimensional Array Record) - это специализированный формат для хранения структурированных данных с поддержкой многомерной адресации записей.

## Структура файлов

### Файл базы данных (.marm)

1. **Заголовок**:
   - Версия базы данных (3 байта)
   - Разделитель (0xF8)
   - Путь к директории данных
   - Разделитель (0xFA)
   - Резервное пространство (24 байта)
   - Разделитель (0xFA)
   - Количество таблиц (2 байта)

2. **Информация о таблицах**:
   - ID таблицы (2 байта)
   - Имя таблицы (строка)
   - Разделитель (0xFA)
   - Количество колонок (2 байта)
   - Для каждой колонки:
     - ID колонки (2 байта)
     - Имя колонки (строка)
     - Разделитель (0xFA)

### Файлы данных таблиц (.marc)

1. **Заголовок**:
   - ID таблицы (2 байта)
   - Количество координат (2 байта)
   - Максимальное количество записей (2 байта)
   - Текущее количество записей (2 байта)

2. **Метаданные записей**:
   - Для каждой записи:
     - Значения координат (по 2 байта на каждую)
     - Позиция данных в файле (5 байт)
     - Длина данных (3 байта)

3. **Данные**:
   - Сериализованные данные записей

## API Reference

### Класс MARDB

Основной класс для работы с базами данных.

```python
db = MARDB(mode='local', host='localhost', port=9999)
```

**Методы**:
- `create_database(db_name)`
- `create_table(db_name, table_name, columns)`
- `get_tables(db_name)`
- `find_in_table(db_name, table_name, cords)`
- `insert_into_table(db_name, table_name, cords, data)`
- `select_from_table(db_name, table_name)`
- `defragment_database(db_name)`
- `load_database(db_name, mode='fast')`
- `unload_database(db_name)`

### Класс MARDatabaseServer

Сервер для работы с базами данных в сетевом режиме.

```python
server = MARDatabaseServer(host='localhost', port=9999, log_level='INFO')
```

## Примеры использования

### Создание сложной структуры данных

```python
import marlib
from datetime import datetime
import decimal

db = marlib.MARDB(mode='local')
db.create_database('complex_data.marm')
db.create_table('complex_data.marm', 'transactions', ['id', 'date', 'amount'])

# Добавление записи с сложными данными
transaction = {
    'id': 12345,
    'date': datetime.now(),
    'amount': decimal.Decimal('199.99'),
    'items': ['product1', 'product2', 'product3'],
    'metadata': {
        'user': 'john_doe',
        'device': 'mobile'
    }
}

db.insert_into_table('complex_data.marm', 'transactions', [1, 2, 3], transaction)
```

### Работа с сервером

```python
import marlib

# Клиентская часть
db = marlib.MARDB(mode='server', host='localhost', port=9999)
db.load_database('my_database.marm')

# Серверная часть (запуск в отдельном процессе)
from marlib import MARDatabaseServer
server = MARDatabaseServer(host='localhost', port=9999)
server.start()
```

## Принципы работы

1. **Координатная адресация**: Каждая запись идентифицируется набором координат
2. **Фрагментация данных**: Данные хранятся в нескольких файлах для оптимизации
3. **Кэширование**: Серверная версия использует интеллектуальное кэширование
4. **Синхронизация**: Автоматическая синхронизация изменений с диском

## Советы по использованию

1. Для больших баз данных используйте серверный режим с кэшированием
2. Регулярно выполняйте дефрагментацию для оптимизации производительности
3. Используйте соответствующий режим загрузки в зависимости от patterns доступа к данным
4. Для сложных структур данных убедитесь в поддержке сериализации нужных типов

Версия: 1.0
