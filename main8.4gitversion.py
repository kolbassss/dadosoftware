import csv
import time
import os
import random
import string
import datetime
import json 
from concurrent.futures import ProcessPoolExecutor, as_completed 

# --- ГЛОБАЛЬНЫЕ КОНСТАНТЫ ---
RESULT_CHAR_LIMIT = 2000
REPORTS_DIR = "отчёты"

# ===================================================================
#                      ФУНКЦИИ УТИЛИТЫ И ПОМОЩНИКИ
# ===================================================================

# --- НОВАЯ ФУНКЦИЯ ПЕРЕЗАГРУЗКИ ---
def restart_software():
    """Очищает консоль и возвращает в главное меню (эмулирует перезапуск)."""
    # Очистка консоли (для Windows, Linux, macOS)
    os.system('cls' if os.name == 'nt' else 'clear')

def get_data_files():
    """Сканирует папку 'db' и возвращает список полных путей к файлам."""
    DB_FOLDER = "db"
    data_files = []
    
    # 1. Проверка существования папки
    if not os.path.exists(DB_FOLDER):
        print(f"Внимание: Папка с базами данных '{DB_FOLDER}' не найдена. Создайте ее и переместите туда файлы.")
        return []

    # 2. Сканирование папки
    for item in os.listdir(DB_FOLDER):
        full_path = os.path.join(DB_FOLDER, item)
        # Проверка, что это файл и имеет нужное расширение
        if os.path.isfile(full_path) and item.lower().endswith(('.csv', '.sql', '.txt', '.json')):
            data_files.append(full_path)
            
    return sorted(data_files)

def save_reports(large_results):
    if not large_results:
        return

    if not os.path.exists(REPORTS_DIR):
        try:
            os.makedirs(REPORTS_DIR)
        except OSError as e:
            # Вывод ошибки без цвета
            print(f"Ошибка при создании папки '{REPORTS_DIR}': {e}")
            return

    random_id = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    output_filename = os.path.join(REPORTS_DIR, f"Отчет_{random_id}.txt")
    
    saved_count = 0
    
    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write(f"=== ЕДИНЫЙ ОТЧЕТ: Результаты поиска, превышающие {RESULT_CHAR_LIMIT} символов ===\n")
            f.write(f"Общее количество больших записей: {len(large_results)}\n")
            f.write(f"Время создания отчета: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            for (file_name, match), match_string_for_check in large_results:
                f.write("=" * 100 + "\n")
                f.write(f"НАЙДЕНО В ФАЙЛЕ: {os.path.basename(file_name)}\n")
                f.write(f"Длина записи: {len(match_string_for_check)} символов\n\n")

                if isinstance(match, dict):
                    for key, value in match.items():
                        f.write(f"[ {key}: ] {str(value)}\n")
                else:
                    f.write(f"[ Совпадение: ] {match}\n")
                f.write("\n")
                saved_count += 1

        print("\n" + "=" * 50)
        print(f"✅ Сохранено {saved_count} больших результатов в ЕДИНЫЙ отчет.")
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

# --- ФУНКЦИЯ ВЫВОДА РЕЗУЛЬТАТОВ (БЕЗ ЦВЕТА) ---
def print_results(match_count, results):
    large_results_to_save = [] 
    separator = "=" * 80
    
    for index, (file_name, match) in enumerate(results, 1):
        
        if isinstance(match, dict):
            match_string_for_check = " ".join(str(v) for v in match.values())
        else:
            match_string_for_check = match

        if len(match_string_for_check) > RESULT_CHAR_LIMIT:
            large_results_to_save.append(((file_name, match), match_string_for_check))
            
            print(f'\n{separator}')
            print(f'[{index}/{match_count}] Результат в "{os.path.basename(file_name)}" слишком длинный ({len(match_string_for_check)}).')
            print(f'{separator}\n')
            
            continue 

        print(f'\n{separator}')
        print(f'[{index}/{match_count}] Найдено совпадение в файле: "{os.path.basename(file_name)}"')
        print(f'{separator}\n')

        if isinstance(match, dict):
            for key, value in match.items():
                print(f'[ {key}: ] {str(value)}')
            print() 
            
        else:
            print('[ Match: ]')
            print(match)
            print()
            
        print(f'{separator}\n') 

    return large_results_to_save 

# ===================================================================
#                           ФУНКЦИИ ПОИСКА
# ===================================================================

def search_in_files(query_input, file, multi_criteria=False, exact_match=False):
    """Ищет в ОДНОМ файле, поддерживая CSV, JSON, SQL, TXT."""
    results = []
    
    # --- ЛОГИКА ОПРЕДЕЛЕНИЯ КРИТЕРИЕВ ПОИСКА ---
    if isinstance(query_input, str):
        query_parts = [query_input.lower()]
    else:
        query_parts = [part.lower() for part in query_input]
    
    if not query_parts:
        return []
    # ----------------------------------------------------------

    if os.path.exists(file):
        try:
            encoding_to_use = 'utf-8-sig' if file.endswith('.csv') else 'utf-8' 
            with open(file, 'r', encoding=encoding_to_use, errors='ignore') as f:
                
                if file.endswith(('.csv', '.json')):
                    
                    records = []
                    if file.endswith('.csv'):
                        csv.field_size_limit(10 * 1024 * 1024)
                        reader = csv.DictReader(f)
                        records = list(reader)
                        
                    elif file.endswith('.json'):
                        # --- ОПТИМИЗИРОВАННАЯ ЛОГИКА ДЛЯ JSON/NDJSON ---
                        f.seek(0)
                        
                        try:
                            # 1. Попытка стандартной загрузки (один массив или объект)
                            data = json.load(f)
                            if isinstance(data, list):
                                records = data
                            elif isinstance(data, dict):
                                records = [data]
                            
                        except json.JSONDecodeError as e:
                            # Обработка ошибки. Если это 'Extra data' или 'Unterminated string', читаем построчно.
                            error_message = str(e)
                            if "Extra data" in error_message or "Unterminated string" in error_message:
                                # Переход к построчному чтению (ndjson) без вывода предупреждения
                                f.seek(0) 
                                
                                for line in f:
                                    line = line.strip()
                                    if line:
                                        try:
                                            record = json.loads(line)
                                            if isinstance(record, dict):
                                                records.append(record)
                                        except json.JSONDecodeError:
                                            # Пропускаем невалидные JSON-строки
                                            continue 
                            else:
                                # Критическая ошибка, которую не можем обработать
                                print(f"Ошибка декодирования JSON в файле {file}: {e}")
                                return []
                        # --- КОНЕЦ ОПТИМИЗИРОВАННОЙ ЛОГИКИ ДЛЯ JSON/NDJSON ---

                    for record in records:
                        if record and isinstance(record, dict):
                            row_values_lower = [str(v).lower() for v in record.values()]
                            
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
            print(f"Ошибка при чтении файла {file}: {e}") 
    else:
        print(f"Файл {file} не найден, пропускаем.")
        
    return results

def parallel_search(query_input, files_list, multi_criteria=False, exact_match=False):
    """Запускает search_in_files параллельно для каждого файла с использованием процессов."""
    all_results = []
    
    MAX_WORKERS = min(os.cpu_count() or 4, len(files_list)) 

    print(f"Запуск параллельного поиска в {len(files_list)} файлах с {MAX_WORKERS} процессами...")
    
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {
            executor.submit(
                search_in_files, 
                query_input, 
                f, 
                multi_criteria, 
                exact_match
            ): f for f in files_list
        }
        
        for future in as_completed(future_to_file):
            file = future_to_file[future]
            try:
                results = future.result()
                all_results.extend(results)
            except Exception as exc:
                print(f'Файл {file} вызвал исключение при обработке: {exc}')
                
    return all_results
# ===================================================================
#                           ФУНКЦИИ МЕНЮ
# ===================================================================

# --- НОВАЯ ФУНКЦИЯ: ПОИСК ПО ОТДЕЛЬНОЙ БД (КОМАНДА 77) ---
def search_single_db_submenu(all_db_files):
    """
    Команда 77: Отображает список файлов (баз данных) в папке db/,
    запрашивает выбор БД и режим поиска (как для групп).
    """
    
    if not all_db_files:
        print("\nНет баз данных (файлов) в папке db/.")
        input("Нажмите Enter, чтобы вернуться в главное меню...")
        return

    while True:
        print("\n--- 🔎 Команда 77: Выбор отдельной базы данных для поиска ---")
        
        # 1. Отображение списка баз данных с нумерацией
        db_map = {}
        for i, db_full_path in enumerate(all_db_files, 1):
            db_name = os.path.basename(db_full_path) # Показываем только имя файла
            print(f"  {i}. {db_name}")
            db_map[str(i)] = db_full_path # Связываем номер и полный путь к файлу
        
        print("\n  0. Назад в Главное меню")

        # 2. Запрос выбора
        choice = input("\nВведите номер базы данных для поиска (или 0 для возврата): ")
        
        if choice == '0':
            print("Возврат в Главное меню.")
            return # Выход из подменю
        
        if choice in db_map:
            selected_db_path = db_map[choice]
            selected_db_name = os.path.basename(selected_db_path)
            
            # 3. Запрос режима поиска (как для групп)
            search_mode = prompt_for_search_mode(f"База данных: {selected_db_name}")
            
            files_to_search = [selected_db_path]

            if search_mode == 'FIO_AND':
                handle_fio_search(files_to_search, is_group_search=True) 
            
            elif search_mode == 'SIMPLE_OR':
                query = slow_input(f"Ищем в '{selected_db_name}' (ОБЫЧНЫЙ поиск). Введите информацию: ")
                
                if not query:
                    print("Вы не ввели запрос.")
                    input("Нажмите Enter, чтобы вернуться к выбору базы данных.")
                    continue
                    
                start_time = time.time()
                results = parallel_search(query, files_to_search, multi_criteria=False, exact_match=False)
                elapsed_time = time.time() - start_time
                
                if results:
                    match_count = len(results)
                    large_results = print_results(match_count, results)
                    print(f"\nПоиск завершён за {elapsed_time:.2f} секунд. Найдено совпадений: {match_count}")
                    if large_results:
                        save_reports(large_results)
                else:
                    print("Совпадений не найдено.")
                
                input("Нажмите Enter, чтобы вернуться к выбору базы данных...")
                
            continue # После поиска остаемся в подменю, чтобы выбрать другую БД

        else:
            print("❌ Неверный ввод. Попробуйте снова.")

# --- НОВАЯ ФУНКЦИЯ: ОБРАБОТКА ГРУППЫ ТОЛЬКО С OR-ПОИСКОМ ---
def handle_or_only_group_search(files_list, group_name):
    """Обрабатывает поиск в группе, предлагая только 'Обычный поиск' (OR)."""
    
    print(f"\n--- Выбран поиск: '{group_name}' ---")
    print("Режим поиска: ОБЫЧНЫЙ ПОИСК (подстрока, OR)")
    
    query = slow_input("Введите доступную информацию: ")
    
    if not query:
        print("Вы не ввели запрос.")
        input("Нажмите Enter, чтобы вернуться в меню.")
        return
        
    print(f"\nИдет ОБЫЧНЫЙ поиск в {len(files_list)} файлах. Запрос: {query}")
    start_time = time.time()
    
    # Используем parallel_search с multi_criteria=False для OR-поиска
    results = parallel_search(query, files_list, multi_criteria=False, exact_match=False)
    elapsed_time = time.time() - start_time
    
    if results:
        match_count = len(results)
        large_results = print_results(match_count, results)
        
        print(f"\nПоиск завершён за {elapsed_time:.2f} секунд. Найдено совпадений: {match_count}")
        
        if large_results:
            save_reports(large_results)
            
    else:
        print("Совпадений не найдено.")
    
    input("Нажмите Enter, чтобы вернуться в меню.")


# --- ФУНКЦИЯ МЕНЮ (БЕЗ ЦВЕТА) ---
def print_menu(filemapping):
    """Динамически строит меню без использования цвета. Теперь только команды и ГРУППЫ."""
    
    menu_options_lines = []
    
    fio_phone_option = "[1] ПОИСК ПО ФИО + ТЕЛЕФОНУ (неполные данные, AND)"
    normal_option = "[2] ОБЫЧНЫЙ ПОИСК ПО ВСЕЙ БАЗЕ (подстрока, OR)"
    phone_option = "[3] ПОИСК ПО ТЕЛЕФОНУ (7xxxxxxxxxx, подстрока, OR)"

    menu_options_lines.append(fio_phone_option)
    menu_options_lines.append(normal_option)
    menu_options_lines.append(phone_option)
    
    # Динамические опции (только группы, одиночные файлы удалены)
    for i, data in filemapping.items():
        group_name = data['name']
        if data.get("or_only"):
             menu_options_lines.append(f"[{i}] {group_name} (Сразу OR-поиск)")
        else:
             menu_options_lines.append(f"[{i}] {group_name}")

    # --- ДОБАВЛЕНИЕ КОМАНД ---
    single_db_option = "[77] ПОИСК ПО ОТДЕЛЬНОЙ БАЗЕ ДАННЫХ"
    menu_options_lines.append(single_db_option)
    
    logo_art = """
                                ▄████▄   ▄▄▄▄   ▄████▄   ▄████▄  
                                ▀█▄  ▀█ ▀▀ ▄██  ▀█▄  ▀█ ▀█▄  ▀█ 
                                 ██   █ ▄█▀ ██   ██   █  ██   █ 
                                 ▀█▄▄█▀ ▀█▄▄▀█▀  ▀█▄▄█▀  ▀█▄▄█▀  
    """
    
    # Печатаем лого и заголовки
    print(logo_art)
    print("                                          создатель: @CEKPET_B_KODE")
    print("                                            Главное меню")
    
    # Печатаем верхнюю рамку
    print("                        ╔═══════════════════════════════ ══════════════════════╗")
    
    # Печатаем опции
    PADDING_WIDTH = 51 # Ширина для выравнивания текста
    
    for text in menu_options_lines:
        print(f"                        ║ {text.ljust(PADDING_WIDTH)} ║")
        
    # Печатаем нижние опции и границу
    print("                        ║ --------------------------------------------------- ║")
    print(f"                        ║ {'[88] Перезагрузка'.ljust(PADDING_WIDTH)} ║") # <-- НОВАЯ ОПЦИЯ
    print(f"                        ║ {'[0] Выход'.ljust(PADDING_WIDTH)} ║")                        
    print("                        ╚═════════════════════════════════════════════════════╝")
    print("\n")


def handle_fio_search(files_list, is_group_search=False):
    """Обрабатывает поиск по неполным данным (ФИО + Телефон)."""
         
    if is_group_search:
         print("--- РЕЖИМ ГРУППОВОГО ПОИСКА: ФИО + Телефон (Режим AND) ---")
    else:
         print("--- Поиск по неполным данным (ФИО + Телефон, Режим AND) ---")
         
    print("Оставьте поля пустыми, если они неизвестны.")
    
    last_name = slow_input("Введите Фамилию: ")
    first_name = slow_input("Введите Имя: ")
    patronymic = slow_input("Введите Отчество: ")
    phone = slow_input("Введите Телефон (7xxxxxxxxxx, необязательно): ")
    
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
    
    results = parallel_search(query_parts, files_list, multi_criteria=True, exact_match=False) 
    elapsed_time = time.time() - start_time

    if results:
        match_count = len(results)
        large_results = print_results(match_count, results)
        
        print(f"\nПоиск завершён за {elapsed_time:.2f} секунд. Найдено совпадений: {match_count}")
        
        if large_results:
            save_reports(large_results)
            
    else:
        print("Совпадений не найдено.")
    
    print("\nвведите получившийся результат в поиск")
    input("Нажмите Enter, чтобы вернуться в меню.")

def handle_phone_search(all_db_files):
    """Обрабатывает поиск по номеру телефона в режиме ОБЫЧНЫЙ ПОИСК."""
    
    print("--- ПОИСК ПО ТЕЛЕФОНУ (ОБЫЧНЫЙ ПОИСК) ---")
    query = slow_input("Введите номер телефона (формат: 7xxxxxxxxxx): ")
    
    if not query:
        print("Вы не ввели номер.")
        input("Нажмите Enter, чтобы вернуться в меню.")
        return
        
    print(f"\nИдет ОБЫЧНЫЙ поиск по всем базам. Запрос: {query}")
    start_time = time.time()
    
    results = parallel_search(query, all_db_files, multi_criteria=False, exact_match=False)
    elapsed_time = time.time() - start_time

    if results:
        match_count = len(results)
        large_results = print_results(match_count, results)
        
        print(f"\nПоиск завершён за {elapsed_time:.2f} секунд. Найдено совпадений: {match_count}")
        
        if large_results:
            save_reports(large_results)
            
    else:
        print("Совпадений не найдено.")

    input("Нажмите Enter, чтобы вернуться в меню.")

def prompt_for_search_mode(group_name):
    print(f"\n--- Выбран поиск: '{group_name}' ---")
    print("Выберите режим поиска:")
    print("[1] По неполным данным (ФИО + Телефон, Режим AND)")
    print("[2] По всей базе (Обычный поиск, Режим OR)")
    
    while True:
        mode_choice = input("Ваш выбор: ").strip()
        if mode_choice == '1':
            return 'FIO_AND'
        elif mode_choice == '2':
            return 'SIMPLE_OR'
        else:
            print("Неверный выбор. Введите 1 или 2.")

# ===================================================================
#                               MAIN
# ===================================================================
def main():
    print("Загрузка...")
    time.sleep(1)

    all_files_raw = get_data_files()
    
    if not all_files_raw:
        # get_data_files() уже выводит предупреждение о папке db/
        input("Нажмите Enter для выхода...")
        return

    # Константа, используемая для сопоставления имен файлов в группах с их полным путем
    DB_FOLDER = "db" 
    
    # --- ОБНОВЛЕННЫЙ СЛОВАРЬ ГРУПП ---
    
    
    groups = {
        "поиск по большой перемене и тп(фи)": ["bolshayaperemena1_normalized.csv", "bolshayaperemena2_normalized.csv","login.csv","part2"], 
        #-----------------------------------------------------------------------------------------------------------------------------------
        "поиск по ФИО(рекомендуется искать фи)":[
                                                "бд.расширение",
                                            ],
        
        #=======================================================OR-ONLY Группа==============================================================
        "поиск по тг(номер ИЛИ id)": [
                                                "бд.расширение",
                                            ],
        #-----------------------------------------------------------------------------------------------------------------------------------
        "поиск по вк(username,passwd,email)": [
                                                "бд.расширение",
                                            ],
        #-----------------------------------------------------------------------------------------------------------------------------------
        "поиск по операторам: телефон(7xxxxxxxxxx) ИЛИ фамилия": [
                                                "бд.расширение",
                                            ],
        #-----------------------------------------------------------------------------------------------------------------------------------
        "поиск по номеру 2(рекомендуется)":[
                                                "бд.расширение",
                                            ],
        #-----------------------------------------------------------------------------------------------------------------------------------
        "поиск по email":[
                                                "бд.расширение",
                                            ],
                    
    }
    
    # Список имен групп, которые должны использовать ТОЛЬКО OR-поиск
    or_only_groups = ["поиск по тг(номер ИЛИ id)", "поиск по вк(username,passwd,email)", "поиск по операторам: телефон(7xxxxxxxxxx) ИЛИ фамилия","поиск по email","поиск по номеру 2(рекомендуется)"]
    
    filemapping = {}
    grouped_files_list = [] 
    current_index = 4 

    # 1. Формирование групп (Модифицировано для работы с папкой db/)
    for name, files_in_group in groups.items():
        existing_files = []
        
        # Мы должны восстановить полный путь ('db/имя_файла.csv') для сравнения с all_files_raw
        for bare_filename in files_in_group:
             full_path = os.path.join(DB_FOLDER, bare_filename)
             if full_path in all_files_raw: # Проверяем, существует ли полный путь
                 existing_files.append(full_path)
        
        if existing_files:
            # Добавление флага для OR-Only групп
            is_or_only = name in or_only_groups
            filemapping[current_index] = {"name": name, "files": existing_files, "or_only": is_or_only}
            grouped_files_list.extend(existing_files)
            current_index += 1

    # ******************************************************************
    # 2. УДАЛЕНА ЛОГИКА ДОБАВЛЕНИЯ ОДИНОЧНЫХ ФАЙЛОВ
    # ******************************************************************
            
    while True:
        print_menu(filemapping) 

        choice = input("Выберите: ")
        
        files_to_search = []
        do_search = False
        query = ""
        
        if choice == '1': 
            handle_fio_search(all_files_raw) 
            continue
            
        elif choice == '2': 
            files_to_search = all_files_raw
            query = slow_input("Введите доступную информацию для ОБЫЧНОГО поиска: ")
            do_search = True
        
        elif choice == '3':
            handle_phone_search(all_files_raw)
            continue
        
        # --- ОБРАБОТКА КОМАНДЫ 77 ---
        elif choice == '77':
            search_single_db_submenu(all_files_raw)
            continue
            
        # --- ОБРАБОТКА КОМАНДЫ 88 (Перезагрузка) ---
        elif choice == '88':
            print("Перезагрузка программы...")
            time.sleep(1)
            restart_software()
            continue
            
        elif choice == '0':
            print("Выход из программы...")
            break

        
        elif choice.isdigit():
            choice_int = int(choice)
            
            if choice_int in filemapping:
                group_data = filemapping[choice_int] # Получаем данные группы
                files_to_search = group_data["files"]
                group_name = group_data["name"]
                
                # --- ЛОГИКА ОБРАБОТКИ ГРУППЫ ---
                if group_data.get("or_only"):
                    # Для групп, помеченных как or_only, вызываем функцию прямого OR-поиска
                    handle_or_only_group_search(files_to_search, group_name)
                    
                else:
                    # Для стандартных групп показываем меню выбора режима
                    search_mode = prompt_for_search_mode(group_name)
                    
                    if search_mode == 'FIO_AND':
                        handle_fio_search(files_to_search, is_group_search=True) 
                    
                    elif search_mode == 'SIMPLE_OR':
                        query = slow_input(f"Ищем в '{group_name}' (ОБЫЧНЫЙ поиск). Введите информацию: ")
                        
                        if not query:
                            print("Вы не ввели запрос.")
                            input("Нажмите Enter, чтобы вернуться в меню.")
                            continue
                            
                        start_time = time.time()
                        results = parallel_search(query, files_to_search, multi_criteria=False, exact_match=False)
                        elapsed_time = time.time() - start_time
                        
                        if results:
                            match_count = len(results)
                            large_results = print_results(match_count, results)
                            print(f"\nПоиск завершён за {elapsed_time:.2f} секунд. Найдено совпадений: {match_count}")
                            if large_results:
                                save_reports(large_results)
                        else:
                            print("Совпадений не найдено.")
                        
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
        
        if do_search:
            if not query:
                print("Вы не ввели запрос.")
                input("Нажмите Enter, чтобы вернуться в меню.")
                continue

            start_time = time.time()
            results = parallel_search(query, files_to_search, multi_criteria=False, exact_match=False)
            elapsed_time = time.time() - start_time

            if results:
                match_count = len(results)
                large_results = print_results(match_count, results)
                
                print(f"\nПоиск завершён за {elapsed_time:.2f} секунд. Найдено совпадений: {match_count}")
                
                if large_results:
                    save_reports(large_results)
                    
            else:
                print("Совпадений не найдено.")

            input("Нажмите Enter, чтобы вернуться в меню.")

if __name__ == "__main__":
    try:
        if os.name == 'nt': 
            from multiprocessing import freeze_support
            freeze_support()
        main()
    except Exception as e:
        print(f"Критическая ошибка: {e}")
