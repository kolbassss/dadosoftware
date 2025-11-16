import csv
import time
import os
import random
import string
import datetime
import json 
import sqlite3 
import sys 
from concurrent.futures import ProcessPoolExecutor, as_completed 
from typing import List, Dict, Tuple, Any, Optional 

# --- ГЛОБАЛЬНЫЕ КОНСТАНТЫ ---
RESULT_CHAR_LIMIT = 4000
INTERMEDIATE_REPORT_THRESHOLD = 500 # Новый лимит символов для вывода результатов по отдельному файлу (500)
REPORTS_DIR = "отчёты"
DB_FOLDER = "db" # Папка для всех данных
MASTER_DB_FILE = "master_index.db" 

# ===================================================================
#                      ФУНКЦИИ УТИЛИТЫ И ПОМОЩНИКИ
# ===================================================================

def restart_software():
    """Очищает консоль и возвращает в главное меню (эмулирует перезапуск)."""
    # Очистка консоли (для Windows, Linux, macOS)
    os.system('cls' if os.name == 'nt' else 'clear')

def get_data_files():
    """Сканирует папку 'db' и возвращает список полных путей к файлам, включая .db, .csv, .txt, .json."""
    
    data_files = []
    
    # 1. Проверка существования папки
    if not os.path.exists(DB_FOLDER):
        print(f"Внимание: Папка с базами данных '{DB_FOLDER}' не найдена. Создайте ее и переместите туда файлы.")
        return []

    # 2. Сканирование папки.
    for item in os.listdir(DB_FOLDER):
        full_path = os.path.join(DB_FOLDER, item)
        if os.path.isfile(full_path) and item.lower().endswith(('.csv', '.sql', '.txt', '.json', '.db')):
            data_files.append(full_path)
            
    return sorted(data_files)

def save_reports(large_results: List[Tuple[Tuple[str, Any], str]]):
    """Сохраняет список результатов, превышающих общий лимит, в файл."""
    if not large_results:
        return

    if not os.path.exists(REPORTS_DIR):
        try:
            os.makedirs(REPORTS_DIR)
        except OSError as e:
            print(f"Ошибка при создании папки '{REPORTS_DIR}': {e}")
            return

    random_id = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    output_filename = os.path.join(REPORTS_DIR, f"Отчет_{random_id}.txt")
    
    saved_count = 0
    
    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write(f"=== ЕДИНЫЙ ОТЧЕТ: Результаты поиска, превышающие {RESULT_CHAR_LIMIT} символов (общая длина) ===\n")
            f.write(f"Общее количество записей: {len(large_results)}\n")
            f.write(f"Время создания отчета: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            for (file_name, match), match_string_for_check in large_results:
                f.write("=" * 100 + "\n")
                f.write(f"НАЙДЕНО В ФАЙЛЕ: {os.path.basename(file_name)}\n")
                f.write(f"Длина записи (для проверки): {len(match_string_for_check)} символов\n\n")

                if isinstance(match, dict):
                    match_copy = match.copy()
                    if '__table_name' in match_copy:
                        match_copy.pop('__table_name')
                    for key, value in match_copy.items():
                        f.write(f"[ {key}: ] {str(value)}\n")
                else:
                    f.write(f"[ Совпадение: ] {match}\n")
                f.write("\n")
                saved_count += 1

        print("\n" + "=" * 50)
        print(f" Сохранено {saved_count} результатов в ЕДИНЫЙ отчет.")
        print(f"   Файл находится в папке '{REPORTS_DIR}', имя: {os.path.basename(output_filename)}")
        print("=" * 50)
        
    except Exception as e:
        print(f"КРИТИЧЕСКАЯ ОШИБКА при записи отчета: {e}")
        
# --- ФУНКЦИИ ВВОДА/ВЫВОДА ---
def slow_input(prompt, delay=0.005):
    print(prompt, end='', flush=True)
    time.sleep(delay)
    return input()

def slow_print(text, delay=0.005):
    for char in text:
        print(char, end='', flush=True)
        time.sleep(delay)
    print()

# ===================================================================
#                      ФУНКЦИИ ВЫВОДА РЕЗУЛЬТАТОВ
# ===================================================================

def print_sequential_file_results(file_results: List[Tuple[str, Any]], file_path: str) -> List[Tuple[Tuple[str, Any], str]]:
    """
    Обрабатывает результаты из одного файла: 
    1. Форматирует.
    2. Проверяет лимит INTERMEDIATE_REPORT_THRESHOLD (500).
    3. Печатает, если лимит не превышен, иначе сообщает о записи в итоговый отчет.
    Возвращает все результаты в формате, готовом для save_reports (только если они были отложены).
    """
    global INTERMEDIATE_REPORT_THRESHOLD
    
    if not file_results:
        return []

    file_name = os.path.basename(file_path)
    is_raw_file = not file_path.lower().endswith('.db')
    file_type = "RAW" if is_raw_file else "DB"
    
    formatted_report_results = []
    total_file_output_length = 0
    
    # 1. Форматирование и подсчет длины
    for index, (f_name, match_original) in enumerate(file_results, 1):
        
        # Копируем, чтобы избежать изменения оригинала, и удаляем служебные поля только из копии
        match = match_original.copy() if isinstance(match_original, dict) else match_original
        table_name = match.pop('__table_name') if isinstance(match, dict) and '__table_name' in match else None
        
        # Данные для отчета (чистый формат без служебных полей)
        match_clean_for_report = match.copy() if isinstance(match, dict) else match_original
        
        if isinstance(match, dict):
            match_string_for_check = " ".join(str(v).strip() for v in match.values())
            data_content = "\n".join(f'[ {key}: ] {str(value)}' for key, value in match.items())
        else:
            match_string_for_check = str(match)
            data_content = f'[ Совпадение: ] {match}'
            
        table_info = f'Таблица: {table_name}\n' if table_name else ''
        separator = "=" * 80
        index_str = f'[{index}/{len(file_results)}] '
        
        output_block = (
            f'\n{separator}\n'
            f'{index_str}Найдено совпадение в файле: "{file_name}"\n'
            f'{table_info}'
            f'{separator}\n'
            f'{data_content}\n'
            f'{separator}\n'
        )
        
        report_format = ((f_name, match_clean_for_report), match_string_for_check)
        
        formatted_report_results.append((report_format, output_block))
        total_file_output_length += len(output_block)

    # 2. Проверка лимита (500 символов)
    if total_file_output_length > INTERMEDIATE_REPORT_THRESHOLD:
        print(f"\n--- {file_type}-файл {file_name} ---")
        print(f"[ВНИМАНИЕ] Объем данных ({total_file_output_length} символов) из этого файла **ПРЕВЫШАЕТ** лимит консольного вывода ({INTERMEDIATE_REPORT_THRESHOLD}).")
        print("Подробный отчет по этому файлу будет автоматически сформирован **в конце поиска по всем базам**.")
        print("-" * 50)
        # Возвращаем только report_format часть, отбрасывая output_block, так как они отложены
        return [r[0] for r in formatted_report_results]
    
    # 3. Немедленный вывод, если лимит не превышен
    print(f"\n--- {file_type}-файл {file_name} (Найдено: {len(file_results)}) ---")
    for _, output_block in formatted_report_results:
        print(output_block, end='')
    
    # Возвращаем пустой список, так как все было выведено
    return []

def print_final_results(all_results_for_report: List[Tuple[Tuple[str, Any], str]], total_time: float, total_match_count: int) -> List[Tuple[Tuple[str, Any], str]]:
    """
    Проверяет общий лимит (RESULT_CHAR_LIMIT). Если лимит превышен, возвращает результаты для сохранения.
    Если лимит НЕ превышен, но есть отложенные результаты, **печатает их на экран**.
    """
    global RESULT_CHAR_LIMIT, INTERMEDIATE_REPORT_THRESHOLD
    
    print("-" * 50)
    print(f"Общий поиск завершён за {total_time:.2f} секунд. Найдено совпадений (выведено + отложено): {total_match_count}")
    
    if not all_results_for_report:
        if total_match_count == 0:
            return []
        print("Все результаты были выведены на экран.")
        return []
    
    # Расчет общей длины отложенных результатов
    total_output_length = sum(len(s[1]) for _, s in all_results_for_report) 
    # Оценка общей длины отчета: длина содержимого + метаданные (примерно 150 символов на запись)
    estimated_total_report_length = total_output_length + (len(all_results_for_report) * 150)
    
    print(f"Количество отложенных результатов (длиннее {INTERMEDIATE_REPORT_THRESHOLD} симв.): {len(all_results_for_report)}")
    print(f"Оценочная длина отчета: {estimated_total_report_length} символов.")
    
    # --- ИСПРАВЛЕНИЕ: ПЕЧАТЬ, ЕСЛИ ОБЩИЙ ЛИМИТ НЕ ПРЕВЫШЕН (estimated_total_report_length <= RESULT_CHAR_LIMIT) ---
    if estimated_total_report_length <= RESULT_CHAR_LIMIT:
        print("\n[ВНИМАНИЕ] Отложенные результаты не превысили общий лимит (4000 симв.). **Вывод на экран:**")
        
        for (file_name, match_clean_for_report), _ in all_results_for_report:
            
            # Восстановление данных для печати
            match = match_clean_for_report.copy() if isinstance(match_clean_for_report, dict) else match_clean_for_report
            
            if isinstance(match, dict):
                data_content = "\n".join(f'[ {key}: ] {str(value)}' for key, value in match.items())
            else:
                data_content = f'[ Совпадение: ] {match}'
            
            # Печать блока
            separator = "=" * 80
            print(
                f'\n{separator}\n'
                f'[ОТЛОЖЕНО] Найдено совпадение в файле: "{os.path.basename(file_name)}"\n'
                f'{separator}\n'
                f'{data_content}\n'
                f'{separator}\n'
            )
            
        print("\n--- Вывод отложенных результатов завершен ---")
        return [] # Возвращаем пустой список, так как все выведено на экран
        
    else:
        # Если общий лимит превышен (> 4000), сохраняем в файл.
        print(f"[ВНИМАНИЕ] Общий объем данных отложенного отчета ({estimated_total_report_length} символов) превысил лимит ({RESULT_CHAR_LIMIT}).")
        print(f"Все отложенные результаты будут сохранены в ЕДИНЫЙ отчет.")
        return all_results_for_report # Возвращаем список для сохранения в save_reports

# ===================================================================
#                           ФУНКЦИИ ПОИСКА
# ===================================================================

def search_in_sqlite(query_input, db_filepath, multi_criteria=False, exact_match=False):
    """Ищет во ВСЕХ таблицах SQLite-базы данных (быстрый, индексированный поиск)."""
    sqlite_results = []
    conn = None 
    
    query_parts_raw = [part for part in (query_input if isinstance(query_input, list) else [query_input])]
    
    if exact_match:
        query_parts = [f'{part.replace("%", "%%").replace("_", "__")}' for part in query_parts_raw if part.strip()]
    else:
        query_parts = [f'%{part.replace("%", "%%").replace("_", "__")}%' for part in query_parts_raw if part.strip()]
    
    if not query_parts:
        return []
    
    try:
        conn = sqlite3.connect(db_filepath, isolation_level=None)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall() if not row[0].startswith('sqlite_')]
        
        for table_name in tables:
            cursor.execute(f"PRAGMA table_info('{table_name}');")
            columns = [info[1] for info in cursor.fetchall()]
            
            if not columns:
                continue

            where_clauses = []
            params = []
            
            clean_func = f"\"{{col}}\""
            
            if multi_criteria: 
                for part in query_parts:
                    part_clauses = [f"{clean_func.format(col=col)} LIKE ? COLLATE NOCASE" for col in columns] 
                    where_clauses.append(f"({' OR '.join(part_clauses)})")
                    params.extend([part] * len(columns))

                where_sql = " AND ".join(where_clauses)
            
            else: 
                all_col_clauses = []
                for part in query_parts:
                    part_clauses = [f"{clean_func.format(col=col)} LIKE ? COLLATE NOCASE" for col in columns]
                    all_col_clauses.append(f"({' OR '.join(part_clauses)})")
                    params.extend([part] * len(columns))

                where_sql = " OR ".join(all_col_clauses)
            
            select_query = f"SELECT * FROM \"{table_name}\" WHERE {where_sql}"
            cursor.execute(select_query, params)
            
            rows = cursor.fetchall()
            if rows:
                col_names = [desc[0] for desc in cursor.description]
                for row in rows:
                    result_dict = dict(zip(col_names, row))
                    result_dict['__table_name'] = table_name 
                    sqlite_results.append((db_filepath, result_dict))
        
    except sqlite3.Error as e:
        print(f" ❌ Ошибка SQLite при поиске в {os.path.basename(db_filepath)}: {e}")
        
    finally:
        if conn:
            conn.close() 
            
    return sqlite_results


def search_in_files(query_input, file, multi_criteria=False, exact_match=False):
    """Ищет в ОДНОМ RAW-файле (медленный, последовательный поиск)."""
    results = []
    
    if isinstance(query_input, str):
        query_parts = [query_input.lower()]
    else:
        query_parts = [part.lower() for part in query_input]
    
    if not query_parts:
        return []

    if not file.lower().endswith(('.csv', '.sql', '.txt', '.json')):
        return [] 
    
    if os.path.exists(file):
        try:
            encoding_to_use = 'utf-8-sig' if file.lower().endswith('.csv') else 'utf-8' 
            with open(file, 'r', encoding=encoding_to_use, errors='ignore') as f:
                
                if file.lower().endswith(('.csv', '.json')):
                    
                    records = []
                    
                    if file.lower().endswith('.csv'):
                        csv.field_size_limit(10 * 1024 * 1024)
                        f.seek(0)
                        sample = f.read(10240) 
                        f.seek(0)
                        
                        try:
                            dialect = csv.Sniffer().sniff(sample)
                            reader = csv.DictReader(f, dialect=dialect)
                        except csv.Error:
                            reader = csv.DictReader(f)
                            
                        records = list(reader)
                        
                    elif file.lower().endswith('.json'):
                        f.seek(0)
                        try:
                            data = json.load(f)
                            if isinstance(data, list):
                                records = data
                            elif isinstance(data, dict):
                                records = [data]
                            
                        except json.JSONDecodeError:
                            f.seek(0) 
                            for line in f:
                                line = line.strip()
                                if line:
                                    try:
                                        record = json.loads(line)
                                        if isinstance(record, dict):
                                            records.append(record)
                                    except json.JSONDecodeError:
                                        continue 
                    
                    for record in records:
                        if record and isinstance(record, dict):
                            row_values_lower = [str(v).strip().lower() for v in record.values()]
                            
                            if exact_match:
                                if len(query_parts) == 1 and any(v.strip() == query_parts[0].strip() for v in row_values_lower):
                                    results.append((file, record))
                            else:
                                if multi_criteria:
                                    if all(any(part in value for value in row_values_lower) for part in query_parts):
                                        results.append((file, record))
                                else:
                                    if any(part in value for value in row_values_lower for part in query_parts):
                                        results.append((file, record))
                                    
                else: 
                    for line in f:
                        line_lower = line.strip().lower()

                        if exact_match:
                            if len(query_parts) == 1 and line_lower == query_parts[0].strip():
                                results.append((file, line.strip()))
                        elif multi_criteria:
                            if all(part in line_lower for part in query_parts):
                                results.append((file, line.strip()))
                        else:
                            if any(part in line_lower for part in query_parts):
                                results.append((file, line.strip()))

        except Exception as e:
            pass
        
    return results


def parallel_search(query_input, files_list, multi_criteria=False, exact_match=False):
    """
    ОСНОВНАЯ ФУНКЦИЯ ПОИСКА: 
    DB-поиск: СИНХРОННЫЙ по файлам.
    RAW-поиск: АСИНХРОННЫЙ по файлам.
    Обеспечивает промежуточный вывод (500) и финальную проверку/отчет (4000).
    """
    
    db_files_to_search = [f for f in files_list if f.lower().endswith('.db')]
    raw_files_to_search = [f for f in files_list if not f.lower().endswith('.db')]
    
    all_results_for_report = []
    total_start_time = time.time()
    
    print("-" * 50)
    print(f"🔎 Обнаружено: {len(db_files_to_search)} DB-баз и {len(raw_files_to_search)} RAW-файлов.")

    total_match_count = 0

    # 1. Поиск в SQLite DB (СИНХРОННО по файлам)
    if db_files_to_search:
        print(f" Выполняется **быстрый** индексированный поиск в {len(db_files_to_search)} DB-базах.")
        
        for db_file in db_files_to_search:
            sqlite_results = search_in_sqlite(query_input, db_file, multi_criteria, exact_match)
            total_match_count += len(sqlite_results)
            
            report_results_for_file = print_sequential_file_results(sqlite_results, db_file)
            all_results_for_report.extend(report_results_for_file)
            
        print(f" Поиск в DB завершён.")
    
    # 2. Поиск в остальных RAW-файлах (АСИНХРОННО)
    if raw_files_to_search:
        MAX_WORKERS = min(os.cpu_count() or 4, len(raw_files_to_search)) 

        print(f" Запуск **медленного** параллельного поиска в {len(raw_files_to_search)} RAW-файлах с {MAX_WORKERS} процессами...")
        
        raw_file_results_map = {}
        future_to_file = {}
        
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_file = {
                executor.submit(
                    search_in_files, 
                    query_input, 
                    f, 
                    multi_criteria, 
                    exact_match
                ): f for f in raw_files_to_search
            }
            
            for future in as_completed(future_to_file):
                file = future_to_file[future]
                try:
                    results = future.result()
                    raw_file_results_map[file] = results 
                    total_match_count += len(results)
                except Exception as exc:
                    print(f' ❌ Файл {os.path.basename(file)} вызвал исключение при обработке: {exc}')
        
        print(f" Параллельный поиск в RAW завершён. Обработка результатов...")
        
        # Промежуточный вывод для RAW-файлов (СИНХРОННО)
        for raw_file in raw_files_to_search: 
             results_for_file = raw_file_results_map.get(raw_file, [])
                 
             report_results_for_file = print_sequential_file_results(results_for_file, raw_file)
             all_results_for_report.extend(report_results_for_file)
        
    elapsed_time = time.time() - total_start_time
    
    # Финальная проверка 4000 символов (только для отложенных результатов)
    large_results_to_save = print_final_results(all_results_for_report, elapsed_time, total_match_count)
    
    if large_results_to_save:
        save_reports(large_results_to_save)
        
    return 


# ===================================================================
#                           ФУНКЦИИ МЕНЮ 
# ===================================================================

def initialize_data_context():
    """
    Сканирует файлы данных, формирует словарь групп для меню.
    Возвращает: (all_files_raw, filemapping)
    """
    print("\nЗапуск сканирования файлов данных...")
    all_files_raw = get_data_files() 
    
    if not all_files_raw:
        print("Внимание: Папка 'db' не содержит подходящих файлов.")
        return [], {}

    
    # --- СЛОВАРЬ ГРУПП ---
    groups = {
        "поиск по тг(номер ИЛИ id)": ["EYEOFGOD.csv", 
                                              "EYEOFGODtelegram774k.csv", 
                                              "Telegram_200k-27(Telegram_17kk.csv).csv",
                                              "Telegram2.txt",
                                              "Telegram3.txt",
                                              "telegramusers520k10knumbers.txt",
                                              "Telegram_2022_13.5kk.json", 
                                              "Telegram.txt",
                                              "telegram.csv"
                                              ],
        "поиск по вк(username,passwd,email)": ["VK - 5 mln - login-pass.csv", "VK.txt"],
        
        "поиск по операторам: телефон(7xxxxxxxxxx) ИЛИ фамилия": ["tele2.csv", "megafon1.csv", "beeline_employers_normalized.csv"],
        
        
        "поиск по номеру":[
                            "",
                        ],
                    
    }
    
    or_only_groups = ["поиск по тг(номер ИЛИ id)", "поиск по вк(username,passwd,email)", "поиск по операторам: телефон(7xxxxxxxxxx) ИЛИ фамилия","поиск по email","поиск по номеру 2(рекомендуется)"]
    
    filemapping = {}
    current_index = 4 

    # --- ЛОГИКА ФОРМИРОВАНИЯ ГРУПП --- 
    for name, files_in_group in groups.items():
        existing_files = []
        
        for bare_filename_raw in files_in_group:
             full_path_raw = os.path.join(DB_FOLDER, bare_filename_raw)
             if full_path_raw in all_files_raw: 
                 existing_files.append(full_path_raw)
                 
             base_name = os.path.splitext(bare_filename_raw)[0]
             bare_filename_db = base_name + '.db'
             full_path_db = os.path.join(DB_FOLDER, bare_filename_db)
             
             if full_path_db in all_files_raw and full_path_db not in existing_files:
                 existing_files.append(full_path_db)
        
        if existing_files:
            is_or_only = name in or_only_groups
            filemapping[current_index] = {"name": name, "files": existing_files, "or_only": is_or_only}
            current_index += 1
            
    print(f"✅ Обнаружено {len(all_files_raw)} файлов данных. Сформировано {len(filemapping)} групп.")
    
    return all_files_raw, filemapping

def search_single_db_submenu(all_files):
    """
    Команда 77: Отображает список всех файлов, запрашивает выбор одного или нескольких файлов
    и запускает поиск. Проверяет, чтобы не были выбраны DB и RAW файлы одновременно.
    """
    
    files_to_select = sorted(all_files) 
    
    if not files_to_select:
        print("\nНет файлов в папке db/ для отдельного поиска.")
        input("Нажмите Enter, чтобы вернуться в главное меню...")
        return

    while True:
        restart_software()
        print("\n--- 🔎 Команда 77: Выбор отдельных файлов для поиска (DB с DB, RAW с RAW) ---")
        
        file_map = {}
        for i, full_path in enumerate(files_to_select, 1):
            file_name = os.path.basename(full_path) 
            file_type = "(DB)" if full_path.lower().endswith('.db') else "(RAW)"
            print(f"  {i}. {file_name} {file_type}")
            file_map[str(i)] = full_path 
        
        print("\n  0. Назад в Главное меню")

        choice = slow_input("\nВведите номера файлов для поиска через пробел (например, 1 5 12) или 0 для возврата: ")
        
        if choice == '0':
            print("Возврат в Главное меню.")
            return 
        
        selected_indices = [s.strip() for s in choice.replace(',', ' ').split() if s.strip()]
        files_to_search = []
        
        valid_selection = True
        is_db_selected = False
        is_raw_selected = False
        
        for index_str in selected_indices:
            if index_str.isdigit() and index_str in file_map:
                file_path = file_map[index_str]
                files_to_search.append(file_path)
                
                if file_path.lower().endswith('.db'):
                    is_db_selected = True
                else:
                    is_raw_selected = True
            else:
                print(f" ❌ Неверный номер файла: {index_str}.")
                valid_selection = False
                break
        
        if not valid_selection:
            input("Нажмите Enter, чтобы попробовать снова.")
            continue
        
        if is_db_selected and is_raw_selected:
            print(" ❌ Ошибка: Нельзя выбирать DB-файлы и RAW-файлы одновременно.")
            print(" Пожалуйста, выберите только DB-файлы или только RAW-файлы.")
            input("Нажмите Enter, чтобы попробовать снова.")
            continue

        if not files_to_search:
            print(" Вы не выбрали ни одного файла.")
            input("Нажмите Enter, чтобы попробовать снова.")
            continue
            
        selected_names = [os.path.basename(f) for f in files_to_search]
        print(f"Выбрано {len(files_to_search)} файлов: {', '.join(selected_names[:3])}...")
        
        search_mode = prompt_for_search_mode(f"Выбранные {len(files_to_search)} файлов")

        if search_mode == 'BACK': 
            continue

        if search_mode == 'FIO_AND':
            if handle_fio_search(files_to_search, is_group_search=True) == 'BACK':
                 continue
        
        elif search_mode == 'SIMPLE_OR':
            query = slow_input(f"Ищем в {len(files_to_search)} файлах (ОБЫЧНЫЙ поиск). Введите информацию или [B] Назад: ")
            
            if not query or query.lower() == 'b':
                print("Возврат к выбору базы данных.")
                input("Нажмите Enter, чтобы продолжить...")
                continue
                
            start_time = time.time()
            parallel_search(query, files_to_search, multi_criteria=False, exact_match=False) 
            
            input("Нажмите Enter, чтобы вернуться к выбору базы данных...")
            
        # Возвращаемся в цикл выбора файлов 

def handle_or_only_group_search(files_list, group_name):
    """Обрабатывает поиск в группе, предлагая только 'Обычный поиск' (OR)."""
    
    print(f"\n--- Выбран поиск: '{group_name}' ---")
    print("Режим поиска: ОБЫЧНЫЙ ПОИСК (подстрока, OR)")
    
    query = slow_input("Введите доступную информацию полностью в строчных буквах (иван), номер 79999999999 или [B] Назад: ")
    
    if not query or query.lower() == 'b':
        print("Возврат в меню.")
        input("Нажмите Enter, чтобы вернуться в меню.")
        return
        
    print(f"\nИдет ОБЫЧНЫЙ поиск в {len(files_list)} файлах. Запрос: {query}")
    start_time = time.time()
    
    parallel_search(query, files_list, multi_criteria=False, exact_match=False)
            
    input("Нажмите Enter, чтобы вернуться в меню.")


def print_menu(filemapping):
    """Динамически строит меню без использования цвета."""
    
    menu_options_lines = []
    
    fio_phone_option = "[1] ПОИСК ПО ФИО + ТЕЛЕФОНУ (неполные данные, AND)"
    normal_option = "[2] ОБЫЧНЫЙ ПОИСК ПО ВСЕЙ БАЗЕ (подстрока, OR)"
    phone_option = "[3] ПОИСК ПО ТЕЛЕФОНУ (7xxxxxxxxxx, подстрока, OR)"

    menu_options_lines.append(fio_phone_option)
    menu_options_lines.append(normal_option)
    menu_options_lines.append(phone_option)
    
    for i, data in filemapping.items():
        group_name = data['name']
        if data.get("or_only"):
             menu_options_lines.append(f"[{i}] {group_name} (Сразу OR-поиск)")
        else:
             menu_options_lines.append(f"[{i}] {group_name}")

    single_db_option = "[77] ПОИСК ПО ОТДЕЛЬНЫМ ФАЙЛАМ (DB с DB, RAW с RAW)"
    menu_options_lines.append(single_db_option)
    
    update_files_option = "[99] Обновить список файлов/групп"
    menu_options_lines.append(update_files_option)
    
    logo_art = """
                                ▄████▄   ▄▄▄▄   ▄████▄   ▄████▄  
                                ▀█▄  ▀█ ▀▀ ▄██  ▀█▄  ▀█ ▀█▄  ▀█ 
                                 ██   █ ▄█▀ ██   ██   █  ██   █ 
                                 ▀█▄▄█▀ ▀█▄▄▀█▀  ▀█▄▄█▀  ▀█▄▄█▀  
    """
    
    print(logo_art)
    print("                                          создатель: @CEKPET_B_KODE")
    print("                                            Главное меню")
    
    print("                        ╔═══════════════════════════════ ══════════════════════╗")
    
    PADDING_WIDTH = 51 
    
    for text in menu_options_lines:
        print(f"                        ║ {text.ljust(PADDING_WIDTH)} ║")
        
    print("                        ║ --------------------------------------------------- ║")
    print(f"                        ║ {'[88] Перезагрузка'.ljust(PADDING_WIDTH)} ║") 
    print(f"                        ║ {'[0] Выход'.ljust(PADDING_WIDTH)} ║")                        
    print("                        ╚═════════════════════════════════════════════════════╝")
    print("\n")


def handle_fio_search(files_list, is_group_search=False):
    """Обрабатывает поиск по неполным данным (ФИО + Телефон)."""
         
    if is_group_search:
         print("--- РЕЖИМ ГРУППОВОГО ПОИСКА: ФИО + Телефон (Режим AND) ---")
    else:
         print("--- Поиск по неполным данным (ФИО + Телефон, Режим AND) ---")
         
    print("Оставьте поля пустыми, если они неизвестны. Введите [B] Назад для отмены.")
    
    last_name = slow_input("Введите Фамилию с маленькой буквы или [B] Назад: ")
    if last_name.lower() == 'b': return 'BACK'
    first_name = slow_input("Введите Имя с маленькой буквы или [B] Назад: ")
    if first_name.lower() == 'b': return 'BACK'
    patronymic = slow_input("Введите Отчество с маленькой буквы или [B] Назад: ")
    if patronymic.lower() == 'b': return 'BACK'
    phone = slow_input("Введите Телефон (7xxxxxxxxxх) или [B] Назад: ")
    if phone.lower() == 'b': return 'BACK'
    
    query_parts = []
    if last_name: query_parts.append(last_name)
    if first_name: query_parts.append(first_name)
    if patronymic: query_parts.append(patronymic)
    if phone: query_parts.append(phone)
    

    if not query_parts:
        print("Вы не ввели никаких данных для поиска.")
        input("Нажмите Enter, чтобы вернуться в меню.")
        return

    print(f"\nИдет поиск в {len(files_list)} файлах. Критерии: {', '.join(query_parts)}")
    start_time = time.time()
    
    parallel_search(query_parts, files_list, multi_criteria=True, exact_match=False) 
    
    print("\nвведите получившийся результат в поиск")
    input("Нажмите Enter, чтобы вернуться в меню.")

def handle_phone_search(all_files):
    """Обрабатывает поиск по номеру телефона в режиме ОБЫЧНЫЙ ПОИСК."""
    
    print("--- ПОИСК ПО ТЕЛЕФОНУ (ОБЫЧНЫЙ ПОИСК) ---")
    query = slow_input("Введите номер телефона (формат: 7xxxxxxxxxx) или [B] Назад: ")
    
    if not query or query.lower() == 'b':
        print("Возврат в меню.")
        input("Нажмите Enter, чтобы вернуться в меню.")
        return
        
    print(f"\nИдет ОБЫЧНЫЙ поиск по всем базам. Запрос: {query}")
    start_time = time.time()
    
    parallel_search(query, all_files, multi_criteria=False, exact_match=False)

    input("Нажмите Enter, чтобы вернуться в меню.")

def prompt_for_search_mode(group_name):
    print(f"\n--- Выбран поиск: '{group_name}' ---")
    print("Выберите режим поиска:")
    print("[1] По неполным данным (ФИО + Телефон, Режим AND)")
    print("[2] По всей базе (Обычный поиск, Режим OR)")
    print("[B] Назад к выбору группы/файлов") 
    
    while True:
        mode_choice = input("Ваш выбор: ").strip().lower()
        if mode_choice == '1':
            return 'FIO_AND'
        elif mode_choice == '2':
            return 'SIMPLE_OR'
        elif mode_choice == 'b':
            return 'BACK'
        else:
            print("Неверный выбор. Введите 1, 2 или B.")

# ===================================================================
#                               MAIN
# ===================================================================
def main():
    print("Загрузка...")
    time.sleep(1)

    all_files_raw, filemapping = initialize_data_context()
    
    if not all_files_raw and not filemapping:
        input("Нажмите Enter для выхода...")
        return
            
    while True:
        restart_software() 
        print_menu(filemapping) 

        choice = input("Выберите: ")
        
        files_to_search = []
        
        if choice == '1': 
            if handle_fio_search(all_files_raw) == 'BACK':
                 continue
            continue
            
        elif choice == '2': 
            files_to_search = all_files_raw
            query = slow_input("Введите доступную информацию для ОБЫЧНОГО поиска или [B] Назад: ")
            
            if not query or query.lower() == 'b':
                continue
                
            start_time = time.time()
            parallel_search(query, files_to_search, multi_criteria=False, exact_match=False)
            
            input("Нажмите Enter, чтобы вернуться в меню.")
            continue
        
        elif choice == '3':
            handle_phone_search(all_files_raw)
            continue
        
        elif choice == '77':
            search_single_db_submenu(all_files_raw) 
            continue
            
        elif choice == '88':
            print("Перезагрузка программы...")
            time.sleep(1)
            continue
            
        elif choice == '99':
            print("\nЗапуск обновления списка файлов и групп...")
            all_files_raw, filemapping = initialize_data_context()
            input("Нажмите Enter, чтобы вернуться в главное меню.")
            continue
            
        elif choice == '0':
            print("Выход из программы...")
            break

        
        elif choice.isdigit():
            choice_int = int(choice)
            
            if choice_int in filemapping:
                group_data = filemapping[choice_int] 
                files_to_search = group_data["files"]
                group_name = group_data["name"]
                
                if group_data.get("or_only"):
                    handle_or_only_group_search(files_to_search, group_name)
                    
                else:
                    search_mode = prompt_for_search_mode(group_name)
                    
                    if search_mode == 'BACK':
                        continue 
                        
                    elif search_mode == 'FIO_AND':
                        if handle_fio_search(files_to_search, is_group_search=True) == 'BACK':
                            continue
                    
                    elif search_mode == 'SIMPLE_OR':
                        query = slow_input(f"Ищем в '{group_name}' (ОБЫЧНЫЙ поиск). Введите информацию или [B] Назад: ")
                        
                        if not query or query.lower() == 'b':
                            print("Возврат в меню.")
                            input("Нажмите Enter, чтобы вернуться в меню.")
                            continue
                            
                        start_time = time.time()
                        parallel_search(query, files_to_search, multi_criteria=False, exact_match=False)
                        
                        input("Нажмите Enter, чтобы вернуться в меню.")
                        
                continue 

            else:
                slow_print("Неверный выбор. Пожалуйста, выберите снова.")
                time.sleep(1)
                continue
        else:
            slow_print("Неверный выбор. Пожалуйста, выберите снова.")
            time.sleep(1)
            continue

if __name__ == "__main__":
    try:
        if os.name == 'nt': 
            from multiprocessing import freeze_support
            freeze_support()
        main()
    except Exception as e:
        print(f"Критическая ошибка: {e}")
