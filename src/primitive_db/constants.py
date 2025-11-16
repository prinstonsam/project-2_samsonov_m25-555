#!/usr/bin/env python3

"""Константы проекта."""

# Поддерживаемые типы данных
VALID_TYPES = {'int', 'str', 'bool'}

# Директории для хранения данных
DATA_DIR = './data'
STORAGE_DIR = './storage'

# Имена файлов
METADATA_FILENAME = 'db_meta.json'

# Полный путь к файлу метаданных
METADATA_FILE = f'{STORAGE_DIR}/{METADATA_FILENAME}'

