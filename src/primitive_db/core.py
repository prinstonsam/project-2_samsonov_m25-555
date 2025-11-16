#!/usr/bin/env python3

from src.primitive_db.constants import VALID_TYPES
from src.primitive_db.decorators import confirm_action, handle_db_errors, log_time


@handle_db_errors
def create_table(metadata, table_name, columns):
    """
    Создает новую таблицу.
    
    Args:
        metadata: Словарь с метаданными всех таблиц
        table_name: Имя создаваемой таблицы
        columns: Список столбцов в формате "имя:тип"
        
    Returns:
        dict: Обновленный словарь метаданных
        
    Raises:
        ValueError: Если таблица уже существует или тип данных некорректен
    """
    # Проверяем, не существует ли таблица
    if table_name in metadata:
        raise ValueError(f'Ошибка: Таблица "{table_name}" уже существует.')
    
    # Проверяем корректность типов данных
    for column in columns:
        if ':' not in column:
            raise ValueError(f'Некорректное значение: {column}. Попробуйте снова.')
        
        _, col_type = column.split(':', 1)
        if col_type not in VALID_TYPES:
            raise ValueError(f'Некорректное значение: {col_type}. Попробуйте снова.')
    
    # Автоматически добавляем столбец ID:int в начало
    table_columns = ['ID:int'] + columns
    
    # Добавляем таблицу в метаданные
    metadata[table_name] = table_columns
    
    return metadata


@handle_db_errors
@confirm_action("удаление таблицы")
def drop_table(metadata, table_name):
    """
    Удаляет таблицу.
    
    Args:
        metadata: Словарь с метаданными всех таблиц
        table_name: Имя удаляемой таблицы
        
    Returns:
        dict: Обновленный словарь метаданных
        
    Raises:
        ValueError: Если таблица не существует
    """
    if table_name not in metadata:
        raise ValueError(f'Ошибка: Таблица "{table_name}" не существует.')
    
    # Удаляем таблицу из метаданных
    del metadata[table_name]
    
    return metadata


def list_tables(metadata):
    """
    Возвращает список всех таблиц.
    
    Args:
        metadata: Словарь с метаданными всех таблиц
        
    Returns:
        list: Список имен таблиц
    """
    return list(metadata.keys())


def get_column_schema(metadata, table_name):
    """
    Извлекает схему столбцов таблицы.
    
    Args:
        metadata: Словарь с метаданными всех таблиц
        table_name: Имя таблицы
        
    Returns:
        list: Список кортежей (имя_столбца, тип)
    """
    if table_name not in metadata:
        raise ValueError(f'Ошибка: Таблица "{table_name}" не существует.')
    
    columns = []
    for col_def in metadata[table_name]:
        col_name, col_type = col_def.split(':', 1)
        columns.append((col_name, col_type))
    
    return columns


def validate_value_type(value, expected_type):
    """
    Проверяет и преобразует значение к ожидаемому типу.
    
    Args:
        value: Значение для проверки
        expected_type: Ожидаемый тип ('int', 'str', 'bool')
        
    Returns:
        Преобразованное значение
        
    Raises:
        ValueError: Если значение не может быть преобразовано
    """
    if expected_type == 'int':
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                raise ValueError(f'Некорректное значение: {value}. Ожидается int.')
        raise ValueError(f'Некорректное значение: {value}. Ожидается int.')
    
    elif expected_type == 'bool':
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            if value.lower() in ('true', 'false'):
                return value.lower() == 'true'
        raise ValueError(f'Некорректное значение: {value}. Ожидается bool.')
    
    elif expected_type == 'str':
        return str(value)
    
    raise ValueError(f'Неподдерживаемый тип: {expected_type}.')


@handle_db_errors
@log_time
def insert(metadata, table_name, values):
    """
    Вставляет новую запись в таблицу.
    
    Args:
        metadata: Словарь с метаданными всех таблиц
        table_name: Имя таблицы
        values: Список значений для вставки (без ID)
        
    Returns:
        tuple: (updated_data, new_id) - обновленные данные и ID новой записи
        
    Raises:
        ValueError: Если таблица не существует или данные некорректны
    """
    if table_name not in metadata:
        raise ValueError(f'Ошибка: Таблица "{table_name}" не существует.')
    
    # Получаем схему столбцов (без ID)
    columns = get_column_schema(metadata, table_name)
    # Пропускаем первый столбец (ID)
    data_columns = columns[1:]
    
    # Проверяем количество значений
    if len(values) != len(data_columns):
        msg = (
            'Некорректное значение: количество значений '
            'не соответствует количеству столбцов.'
        )
        raise ValueError(msg)
    
    # Преобразуем и валидируем значения
    validated_values = []
    for i, value in enumerate(values):
        col_name, col_type = data_columns[i]
        validated_value = validate_value_type(value, col_type)
        validated_values.append(validated_value)
    
    # Загружаем существующие данные (будет сделано в engine)
    # Здесь мы просто создаем новую запись
    # Генерация ID будет происходить в engine на основе существующих данных
    
    return validated_values


@log_time
def select(table_data, where_clause=None):
    """
    Выбирает записи из таблицы с опциональным условием WHERE.
    
    Args:
        table_data: Список словарей с данными таблицы
        where_clause: Словарь условий для фильтрации, например {'age': 28}
        
    Returns:
        list: Список словарей с отфильтрованными записями
    """
    if where_clause is None:
        return table_data.copy()
    
    result = []
    for record in table_data:
        match = True
        for column, value in where_clause.items():
            if column not in record or record[column] != value:
                match = False
                break
        if match:
            result.append(record.copy())
    
    return result


@handle_db_errors
def update(table_data, set_clause, where_clause):
    """
    Обновляет записи в таблице.
    
    Args:
        table_data: Список словарей с данными таблицы
        set_clause: Словарь с полями для обновления, например {'age': 29}
        where_clause: Словарь условий для поиска записей
        
    Returns:
        tuple: (updated_data, updated_count) - обновленные данные
            и количество обновленных записей
        
    Raises:
        ValueError: Если не найдено записей для обновления
    """
    updated_count = 0
    updated_data = []
    
    for record in table_data:
        # Проверяем, соответствует ли запись условию WHERE
        match = True
        for column, value in where_clause.items():
            if column not in record or record[column] != value:
                match = False
                break
        
        if match:
            # Обновляем поля согласно SET
            updated_record = record.copy()
            for column, new_value in set_clause.items():
                if column in updated_record:
                    updated_record[column] = new_value
            updated_data.append(updated_record)
            updated_count += 1
        else:
            updated_data.append(record.copy())
    
    if updated_count == 0:
        raise ValueError('Ошибка: Записи не найдены.')
    
    return updated_data, updated_count


@handle_db_errors
@confirm_action("удаление записи")
def delete(table_data, where_clause):
    """
    Удаляет записи из таблицы.
    
    Args:
        table_data: Список словарей с данными таблицы
        where_clause: Словарь условий для поиска записей
        
    Returns:
        tuple: (updated_data, deleted_count) - обновленные данные
            и количество удаленных записей
        
    Raises:
        ValueError: Если не найдено записей для удаления
    """
    deleted_count = 0
    updated_data = []
    
    for record in table_data:
        # Проверяем, соответствует ли запись условию WHERE
        match = True
        for column, value in where_clause.items():
            if column not in record or record[column] != value:
                match = False
                break
        
        if not match:
            updated_data.append(record.copy())
        else:
            deleted_count += 1
    
    if deleted_count == 0:
        raise ValueError('Ошибка: Записи не найдены.')
    
    return updated_data, deleted_count


@handle_db_errors
def info(metadata, table_name, table_data):
    """
    Выводит информацию о таблице.
    
    Args:
        metadata: Словарь с метаданными всех таблиц
        table_name: Имя таблицы
        table_data: Список словарей с данными таблицы
        
    Returns:
        str: Строка с информацией о таблице
    """
    if table_name not in metadata:
        raise ValueError(f'Ошибка: Таблица "{table_name}" не существует.')
    
    columns_str = ", ".join(metadata[table_name])
    record_count = len(table_data)
    
    return (
        f"Таблица: {table_name}\n"
        f"Столбцы: {columns_str}\n"
        f"Количество записей: {record_count}"
    )

