#!/usr/bin/env python3

import functools
import time

import prompt


def handle_db_errors(func):
    """
    Декоратор для обработки ошибок базы данных.
    
    Перехватывает и обрабатывает:
    - FileNotFoundError: файл данных не найден
    - KeyError: таблица или столбец не найден
    - ValueError: ошибки валидации
    - Exception: прочие непредвиденные ошибки
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            msg = (
                "Ошибка: Файл данных не найден. "
                "Возможно, база данных не инициализирована."
            )
            print(msg)
            return None
        except KeyError as e:
            print(f"Ошибка: Таблица или столбец {e} не найден.")
            return None
        except ValueError as e:
            print(f"Ошибка валидации: {e}")
            return None
        except Exception as e:
            print(f"Произошла непредвиденная ошибка: {e}")
            return None
    
    return wrapper


def confirm_action(action_name):
    """
    Декоратор-фабрика для запроса подтверждения опасных операций.
    
    Args:
        action_name: Название действия для отображения пользователю
        
    Returns:
        Декоратор, который запрашивает подтверждение перед выполнением функции
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            msg = f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: '
            confirmation = prompt.string(msg)
            if confirmation.lower() != 'y':
                print("Операция отменена.")
                return None
            return func(*args, **kwargs)
        return wrapper
    return decorator


def log_time(func):
    """
    Декоратор для замера времени выполнения функции.
    
    Выводит время выполнения в формате:
    Функция <имя_функции> выполнилась за X.XXX секунд.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.monotonic()
        result = func(*args, **kwargs)
        end_time = time.monotonic()
        elapsed = end_time - start_time
        print(f"Функция {func.__name__} выполнилась за {elapsed:.3f} секунд.")
        return result
    
    return wrapper


def create_cacher():
    """
    Фабрика функций для создания кэшера с замыканием.
    
    Returns:
        tuple: (cache_result, clear_cache) - функции для кэширования и очистки кэша
    """
    cache = {}
    
    def cache_result(key, value_func):
        """
        Кэширует результат вызова value_func по ключу key.
        
        Args:
            key: Ключ для кэша (должен быть хешируемым)
            value_func: Функция для получения данных, если их нет в кэше
            
        Returns:
            Результат value_func (из кэша или новый)
        """
        if key in cache:
            return cache[key]
        
        result = value_func()
        cache[key] = result
        return result
    
    def clear_cache():
        """Очищает весь кэш."""
        cache.clear()
    
    return cache_result, clear_cache

