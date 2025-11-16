#!/usr/bin/env python3

import json
import os

from src.primitive_db.constants import DATA_DIR


def load_metadata(filepath):
    """
    Загружает метаданные из JSON-файла.
    
    Args:
        filepath: Путь к файлу с метаданными
        
    Returns:
        dict: Словарь с метаданными или пустой словарь, если файл не найден
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_metadata(filepath, data):
    """
    Сохраняет метаданные в JSON-файл.
    
    Args:
        filepath: Путь к файлу для сохранения
        data: Словарь с данными для сохранения
    """
    # Создаем директорию storage, если она не существует
    directory = os.path.dirname(filepath)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_table_data(table_name):
    """
    Загружает данные таблицы из JSON-файла.
    
    Args:
        table_name: Имя таблицы
        
    Returns:
        list: Список словарей с данными или пустой список, если файл не найден
    """
    filepath = f'{DATA_DIR}/{table_name}.json'
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def save_table_data(table_name, data):
    """
    Сохраняет данные таблицы в JSON-файл.
    
    Args:
        table_name: Имя таблицы
        data: Список словарей с данными для сохранения
    """
    # Создаем директорию data, если она не существует
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)
    
    filepath = f'{DATA_DIR}/{table_name}.json'
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

