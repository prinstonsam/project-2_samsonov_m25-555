#!/usr/bin/env python3

import shlex


def convert_value(value_str):
    """
    Преобразует строковое значение в соответствующий тип данных.
    
    Args:
        value_str: Строковое представление значения
        
    Returns:
        int, bool, или str: Преобразованное значение
    """
    value_str = value_str.strip()
    
    # Проверяем, является ли значение булевым
    if value_str.lower() in ('true', 'false'):
        return value_str.lower() == 'true'
    
    # Проверяем, является ли значение числом
    try:
        return int(value_str)
    except ValueError:
        pass
    
    # Удаляем кавычки, если они есть
    if value_str.startswith('"') and value_str.endswith('"'):
        return value_str[1:-1]
    if value_str.startswith("'") and value_str.endswith("'"):
        return value_str[1:-1]
    
    # Возвращаем как строку
    return value_str


def parse_where_clause(where_str):
    """
    Парсит WHERE условие вида "column = value" в словарь.
    
    Args:
        where_str: Строка условия, например "age = 28" или 'name = "Sergei"'
        
    Returns:
        dict: Словарь вида {'column': converted_value}
        
    Raises:
        ValueError: Если формат условия некорректен
    """
    if not where_str:
        return None
    
    # Разбиваем по знаку равенства
    parts = where_str.split('=', 1)
    if len(parts) != 2:
        raise ValueError(f'Некорректное значение: {where_str}. Попробуйте снова.')
    
    column = parts[0].strip()
    value_str = parts[1].strip()
    
    # Преобразуем значение в нужный тип
    value = convert_value(value_str)
    
    return {column: value}


def parse_set_clause(set_str):
    """
    Парсит SET условие вида "column1 = value1, column2 = value2" в словарь.
    
    Args:
        set_str: Строка условия, например 'age = 29' или 'name = "Ivan", age = 25'
        
    Returns:
        dict: Словарь вида {'column1': converted_value1, 'column2': converted_value2}
        
    Raises:
        ValueError: Если формат условия некорректен
    """
    if not set_str:
        return {}
    
    result = {}
    
    # Разбиваем по запятым, учитывая кавычки
    try:
        # Используем shlex для правильной обработки кавычек
        parts = shlex.split(set_str.replace(',', ' , '))
    except ValueError:
        # Если shlex не справился, используем простой split
        parts = [p.strip() for p in set_str.split(',')]
    
    # Обрабатываем каждую пару column = value
    i = 0
    while i < len(parts):
        # Пропускаем запятые
        if parts[i] == ',':
            i += 1
            continue
        
        # Ищем знак равенства
        if i + 1 < len(parts) and parts[i + 1] == '=':
            column = parts[i].strip()
            if i + 2 < len(parts):
                value_str = parts[i + 2].strip()
                # Удаляем запятую в конце, если есть
                if value_str.endswith(','):
                    value_str = value_str[:-1]
                result[column] = convert_value(value_str)
                i += 3
            else:
                raise ValueError(f'Некорректное значение: {set_str}. Попробуйте снова.')
        else:
            # Пытаемся найти = в текущей части
            if '=' in parts[i]:
                eq_parts = parts[i].split('=', 1)
                column = eq_parts[0].strip()
                value_str = eq_parts[1].strip()
                # Удаляем запятую в конце, если есть
                if value_str.endswith(','):
                    value_str = value_str[:-1]
                result[column] = convert_value(value_str)
            i += 1
    
    if not result:
        raise ValueError(f'Некорректное значение: {set_str}. Попробуйте снова.')
    
    return result

