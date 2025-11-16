#!/usr/bin/env python3

import shlex

import prompt
from prettytable import PrettyTable

from src.primitive_db.constants import METADATA_FILE
from src.primitive_db.core import (
    create_table,
    delete,
    drop_table,
    get_column_schema,
    info,
    insert,
    list_tables,
    select,
    update,
)
from src.primitive_db.decorators import create_cacher
from src.primitive_db.parser import convert_value, parse_set_clause, parse_where_clause
from src.primitive_db.utils import (
    load_metadata,
    load_table_data,
    save_metadata,
    save_table_data,
)

# Создаем кэшер для результатов select
cache_result, clear_cache = create_cacher()


def print_help():
    """Выводит справочную информацию о командах."""
    print("\n***Операции с данными***")
    print("Функции:")
    print("<command> insert into <имя_таблицы> values (<значение1>, <значение2>, ...) - создать запись.")
    print("<command> select from <имя_таблицы> where <столбец> = <значение> - прочитать записи по условию.")
    print("<command> select from <имя_таблицы> - прочитать все записи.")
    print("<command> update <имя_таблицы> set <столбец1> = <новое_значение1> where <столбец_условия> = <значение_условия> - обновить запись.")
    print("<command> delete from <имя_таблицы> where <столбец> = <значение> - удалить запись.")
    print("<command> info <имя_таблицы> - вывести информацию о таблице.")
    print("<command> create_table <имя_таблицы> <столбец1:тип> .. - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")


def format_select_output(table_data, columns):
    """
    Форматирует данные для вывода с помощью PrettyTable.
    
    Args:
        table_data: Список словарей с данными
        columns: Список кортежей (имя_столбца, тип)
        
    Returns:
        str: Отформатированная таблица
    """
    if not table_data:
        return ""
    
    # Создаем таблицу
    table = PrettyTable()
    
    # Получаем имена столбцов
    column_names = [col[0] for col in columns]
    table.field_names = column_names
    
    # Добавляем строки данных
    for record in table_data:
        row = [record.get(col_name, '') for col_name in column_names]
        table.add_row(row)
    
    return str(table)


def run():
    """Главная функция - главный цикл и парсинг команд."""
    print("***База данных***\n")
    print_help()
    
    while True:
        # Загружаем актуальные метаданные
        metadata = load_metadata(METADATA_FILE)
        
        # Запрашиваем ввод пользователя
        user_input = prompt.string(">>>Введите команду: ")
        
        # Разбираем строку на команду и аргументы
        try:
            args = shlex.split(user_input)
        except ValueError as e:
            print(f"Ошибка разбора команды: {e}")
            continue
        
        if not args:
            continue
        
        command = args[0]
        
        # Обрабатываем команды
        if command == "exit":
            break
            
        elif command == "help":
            print_help()
            
        elif command == "create_table":
            if len(args) < 3:
                print("Некорректное значение: недостаточно аргументов. Попробуйте снова.")
                continue
            
            table_name = args[1]
            columns = args[2:]
            
            result = create_table(metadata, table_name, columns)
            if result is not None:
                metadata = result
                save_metadata(METADATA_FILE, metadata)
                
                # Формируем строку со всеми столбцами для вывода
                all_columns = ", ".join(metadata[table_name])
                print(f'Таблица "{table_name}" успешно создана со столбцами: {all_columns}')
                
        elif command == "drop_table":
            if len(args) < 2:
                print("Некорректное значение: укажите имя таблицы. Попробуйте снова.")
                continue
            
            table_name = args[1]
            
            result = drop_table(metadata, table_name)
            if result is not None:
                metadata = result
                save_metadata(METADATA_FILE, metadata)
                print(f'Таблица "{table_name}" успешно удалена.')
                
        elif command == "list_tables":
            tables = list_tables(metadata)
            if tables:
                for table in tables:
                    print(f"- {table}")
            else:
                print("Таблицы отсутствуют.")
        
        elif command == "insert" and len(args) >= 2 and args[1] == "into":
            # insert into <table> values (<value1>, <value2>, ...)
            if len(args) < 5 or args[3] != "values":
                print("Некорректное значение: неправильный формат команды. Попробуйте снова.")
                continue
            
            table_name = args[2]
            # Извлекаем значения из скобок
            values_str = " ".join(args[4:])
            # Удаляем скобки
            if values_str.startswith('(') and values_str.endswith(')'):
                values_str = values_str[1:-1]
            
            # Разбираем значения - используем простой подход с разделением по запятым
            # но учитываем кавычки
            values_args = []
            current_value = ""
            in_quotes = False
            quote_char = None
            
            for char in values_str:
                if char in ('"', "'") and not in_quotes:
                    in_quotes = True
                    quote_char = char
                    current_value += char
                elif char == quote_char and in_quotes:
                    in_quotes = False
                    quote_char = None
                    current_value += char
                elif char == ',' and not in_quotes:
                    if current_value.strip():
                        values_args.append(current_value.strip())
                    current_value = ""
                else:
                    current_value += char
            
            # Добавляем последнее значение
            if current_value.strip():
                values_args.append(current_value.strip())
            
            # Преобразуем значения
            values = [convert_value(v) for v in values_args]
            
            # Загружаем данные таблицы
            table_data = load_table_data(table_name)
            
            # Валидируем и получаем преобразованные значения
            validated_values = insert(metadata, table_name, values)
            if validated_values is not None:
                # Генерируем ID
                if table_data:
                    max_id = max(record.get('ID', 0) for record in table_data)
                    new_id = max_id + 1
                else:
                    new_id = 1
                
                # Создаем новую запись
                columns = get_column_schema(metadata, table_name)
                new_record = {'ID': new_id}
                for i, (col_name, _) in enumerate(columns[1:], 0):
                    new_record[col_name] = validated_values[i]
                
                # Добавляем запись
                table_data.append(new_record)
                save_table_data(table_name, table_data)
                
                # Очищаем кэш, так как данные изменились
                clear_cache()
                
                print(f'Запись с ID={new_id} успешно добавлена в таблицу "{table_name}".')
        
        elif command == "select" and len(args) >= 2 and args[1] == "from":
            # select from <table> [where <condition>]
            if len(args) < 3:
                print("Некорректное значение: укажите имя таблицы. Попробуйте снова.")
                continue
            
            table_name = args[2]
            where_clause = None
            
            # Проверяем наличие WHERE
            if len(args) > 3 and args[3] == "where":
                where_str = " ".join(args[4:])
                try:
                    where_clause = parse_where_clause(where_str)
                except ValueError as e:
                    print(str(e))
                    continue
            
            # Загружаем данные таблицы
            table_data = load_table_data(table_name)
            
            # Создаем ключ для кэша (на основе таблицы и условия WHERE)
            cache_key = (table_name, str(where_clause) if where_clause else None)
            
            # Используем кэширование для select
            def get_select_result():
                return select(table_data, where_clause)
            
            result = cache_result(cache_key, get_select_result)
            
            if result is not None:
                # Получаем схему столбцов
                columns = get_column_schema(metadata, table_name)
                
                # Форматируем вывод
                output = format_select_output(result, columns)
                if output:
                    print(output)
                else:
                    print("Записи не найдены.")
        
        elif command == "update":
            # update <table> set <column1> = <value1> where <column> = <value>
            if len(args) < 4:
                print("Некорректное значение: недостаточно аргументов. Попробуйте снова.")
                continue
            
            table_name = args[1]
            
            # Ищем "set" и "where"
            set_idx = -1
            where_idx = -1
            for i, arg in enumerate(args):
                if arg == "set":
                    set_idx = i
                elif arg == "where":
                    where_idx = i
            
            if set_idx == -1 or where_idx == -1:
                print("Некорректное значение: неправильный формат команды. Попробуйте снова.")
                continue
            
            set_str = " ".join(args[set_idx + 1:where_idx])
            where_str = " ".join(args[where_idx + 1:])
            
            try:
                set_clause = parse_set_clause(set_str)
                where_clause = parse_where_clause(where_str)
            except ValueError as e:
                print(str(e))
                continue
            
            # Загружаем данные таблицы
            table_data = load_table_data(table_name)
            
            # Выполняем обновление
            result = update(table_data, set_clause, where_clause)
            if result is not None:
                updated_data, updated_count = result
                save_table_data(table_name, updated_data)
                
                # Очищаем кэш, так как данные изменились
                clear_cache()
                
                # Находим ID обновленных записей
                updated_ids = []
                for record in updated_data:
                    if any(record.get(col) == val for col, val in where_clause.items()):
                        updated_ids.append(record.get('ID'))
                
                if updated_ids:
                    print(f'Запись с ID={updated_ids[0]} в таблице "{table_name}" успешно обновлена.')
        
        elif command == "delete" and len(args) >= 2 and args[1] == "from":
            # delete from <table> where <column> = <value>
            if len(args) < 5 or args[3] != "where":
                print("Некорректное значение: неправильный формат команды. Попробуйте снова.")
                continue
            
            table_name = args[2]
            where_str = " ".join(args[4:])
            
            try:
                where_clause = parse_where_clause(where_str)
            except ValueError as e:
                print(str(e))
                continue
            
            # Загружаем данные таблицы
            table_data = load_table_data(table_name)
            
            # Находим ID записей для удаления
            ids_to_delete = []
            for record in table_data:
                match = True
                for col, val in where_clause.items():
                    if record.get(col) != val:
                        match = False
                        break
                if match:
                    ids_to_delete.append(record.get('ID'))
            
            # Выполняем удаление
            result = delete(table_data, where_clause)
            if result is not None:
                updated_data, deleted_count = result
                save_table_data(table_name, updated_data)
                
                # Очищаем кэш, так как данные изменились
                clear_cache()
                
                if ids_to_delete:
                    print(f'Запись с ID={ids_to_delete[0]} успешно удалена из таблицы "{table_name}".')
        
        elif command == "info":
            if len(args) < 2:
                print("Некорректное значение: укажите имя таблицы. Попробуйте снова.")
                continue
            
            table_name = args[1]
            
            # Загружаем данные таблицы
            table_data = load_table_data(table_name)
            
            # Выводим информацию
            info_str = info(metadata, table_name, table_data)
            if info_str is not None:
                print(info_str)
                
        else:
            print(f"Функции {command} нет. Попробуйте снова.")
        
        print()  # Пустая строка для читаемости
