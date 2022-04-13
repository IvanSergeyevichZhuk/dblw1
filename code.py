import os
import psycopg2
import csv
import time
from datetime import datetime
"""
1. Написати програму, що завантажує результати ЗНО з https://zno.testportal.com.ua/opendata
за декілька років у таблицю (1 таблицю) в реляційній базі даних. Структуру таблиці
(колонки та їх типи) студенти мають визначити на основі датасету.

2. Програма має поновлювати свою роботу у разі помилки (наприклад помилки в роботі
програми, розриву мережевого з‘єднання або помилки в роботі СУБД). 

3. Програма не має породжувати дублікати записів.

Студенти мають продумати та продемонструвати
сценарій "падіння" бази, та те як програма поновлює свою роботу.

4. Виконати запити, що повертають порівняльну статистику за кілька років

Результат запиту має бути записаний у CSV-файл (засобами обраного
стеку технологій). Але студент має бути готовим виконати запити з клієнта до БД.

● Мова імплементації — Python (модуль psycopg2)
● РСКБД — PostgreSQL
● Клієнт БД — pgAdmin


"""

columns = ['OUTID', 'REGNAME', 'UkrTest', "UkrTestStatus", "UkrBall100"]
columns_types = ['varchar', 'varchar', 'varchar', 'varchar', 'integer']
table_name = 'UkrTestStatus'
db_string = "dbname=sample_db2022_lab1 user=postgres password=1314151617"
data_2020 = 'data/Odata2020File.csv'

def create_db():
    conn = psycopg2.connect(
        database="postgres", user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD"), host = os.getenv("POSTGRES_SERVER")
    )
    cursor = conn.cursor()
    conn.autocommit = True
    sql = '''CREATE database ''' + os.getenv("POSTGRES_DB")
    try:
        cursor.execute(sql)
        print("Database created successfully........")
    except BaseException as e:
        print(str(e).replace('\n',''))

    conn.close()

# Формування з'єднання з базою даних
def db_conn():
    #conn = psycopg2.connect(user='postgress', password='1314151617', database='sample_db2022_lab1')
    user = os.environ["POSTGRES_USER"]
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")
    server = os.getenv("POSTGRES_SERVER")
    print(user, password, db)
    link = "postgres://"+user+":"+password+"@"+server+"/"+db+""
    conn = psycopg2.connect(link)
    return conn


# Обчислення кількості рядків у файлі
def csv_lines_count(filename):
    file = open(filename)
    file_object = csv.reader(file)
    row_count = sum(1 for row in file_object)
    return row_count


# Формування csv reader і заголовку файлу
def read_csv(filename = data_2020):
    file = open(filename, encoding="cp1251")
    csvreader = csv.reader(file)
    header = next(csvreader)
    header = header[0].replace('"', '').split(';')
    #header = header[0].split(';')
    print(header)
    print(len(header))
    return csvreader, header


# Очищення таблиці, якщо вона непуста
def check_table(conn, cur, tbl_name):
    cur.execute("Select * from " + tbl_name + ";")
    result = cur.fetchone()
    if result != None:
        cur.execute("Delete from " + tbl_name + ";")
        conn.commit()
    print(result)
    return False


# Створення таблиці для даних, якщо вона не створена
def create_table(cur, tbl_name, columns_list, columns_types_list):
    sql_statement = "CREATE TABLE IF NOT EXISTS " + tbl_name;
    columns_specification = ' ('
    N = len(columns_list)
    for i in range(N):
        tmp = columns_list[i] + ' ' + columns_types_list[i]
        if i == 0:
             tmp = tmp + " primary key"
        if i != N - 1:
            tmp += ', '
        columns_specification += tmp
    columns_specification += ');'
    sql_statement += columns_specification
    cur.execute(sql_statement)
    print(sql_statement)


# зчитування частини рядків з csv reader
def get_pack(rdr, k, pack):
    start_obj = k
    objects_to_insert = []
    full_pack = False

    for object in rdr:
        objects_to_insert.append(object[0].replace('"', '').split(';'))
        k += 1
        if k - start_obj == pack:
            full_pack = True
            break

    return full_pack, objects_to_insert


# Формування тексту запиту на додавання
def get_insert_sql(tbl_name, columns_list):
    sql = "Insert into " + tbl_name
    columns_statement = " ("
    vals_statement = " ("
    N = len(columns_list)
    for i in range(N):
        tmp = columns_list[i]
        if i != N - 1:
            tmp += ', '
        columns_statement += tmp
        tmp = '%s'
        if i != N - 1:
            tmp += ', '
        vals_statement += tmp
    columns_statement += ') VALUES ' + vals_statement + ')'
    sql += columns_statement
    return sql


# Формування набору значень упорядкованого за списком стовпців
def get_values_list(header, record, columns_list):
    values = []
    M = header.index(columns_list[-1])

    if M >= len(record):
        return None
    elif record[M] == 'null' or record[M - 1] == 'null':
        # print(record[M-1])
        return None
    else:
        for column in columns_list:
            M = header.index(column)
            values.append(record[M])
    values = tuple(values)
    return values


# Запис стану поточного запуску процесу зчитування
def log_failure(k, e, fname="logs/failure.txt"):
    file = open(fname, "a+")
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H-%M-%S")
    file.write(dt_string + ";1st failed record:" + str(k) + ";error:" + str(e).replace('\n',' ') + '\n')
    file.close()

def check_files_dirs():
    try:
        file = open("logs/failure.txt", "r")
    except FileNotFoundError:
        os.mkdir('logs')


# Перевірка попереднього запуску процесу зчитування
def check_insert_failure(fname="logs/failure.txt"):
    try:
        file = open(fname, "r")
    except BaseException as e:
        return -1

    lines = file.readlines()
    cur_stat = lines[-1]
    cur_stat = cur_stat.replace('\n', '').split(';')
    cur_stat_over = []
    for i in range(len(cur_stat)):
        tmp = cur_stat[i].split(':')
        cur_stat_over += tmp
    print(cur_stat_over)
    index = cur_stat_over.index('1st failed record')
    return int(cur_stat_over[index + 1])


# Вставка даних з заготовленого csv reader з відомим заголовком у header
def insert_data(rdr, header, crsr, conn, tbl_name, columns_list, start = 1, end_pack = -1, pack = 20000):
    insert = True
    k = start
    m = 1
    records_added = 0
    failure = False
    start_time = time.time()

    if m != k:
        for object in rdr:
            if m == k:
                break
            m += 1

    packs_inserted = 0
    while insert:

        full_pack, objects_to_insert = get_pack(rdr, k, pack)
        final_record = k + len(objects_to_insert) - 1
        for record in objects_to_insert:
            sql = get_insert_sql(tbl_name, columns_list)
            values = get_values_list(header, record, columns_list)
            if values is None:
                continue

            try:
                crsr.execute(sql, values)
            except BaseException as e:
                print('Запит')
                print(sql, values)
                print('Не може бути виконаний')
                print('Текст помилки: ' + str(e))
                log_failure(k, e)
                insert = False
                failure = True
                break
            else:
                records_added += 1

        if insert:
            try:
                conn.commit()
            except BaseException as e:

                print('Записи', k, '-', final_record, "не можуть бути додані в базу")
                print('Текст помилки: ' + str(e))
                log_failure(k, e)
                insert = False
                failure = True
                continue
            else:

                packs_inserted += 1
                print('Додано всього', records_added, "записів")
                if end_pack != -1:
                    if packs_inserted == end_pack:
                        log_failure(k, 'partial insert', fname="logs/failure.txt")
                        insert = False

                elif full_pack == False:
                    insert = False
        k = final_record + 1
    end_time = time.time()
    file = open("logs/time_for_insert.txt", "a+")
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H-%M-%S")
    if failure:
        stat = "failed"
    else:
        stat = "good"
        log_failure(0, 'success', fname="logs/failure.txt")
    file.write(dt_string + ": " +str(end_time - start_time) + ',' +stat+ '\n')
    file.close()


# Загальна функція для вставки даних разом з перевіркою стану попередньої спроби вставки даних
def check_insert_data(cur, conn, tbl_name, columns_list, columns_types_list, end_pack=-1, pack=20000, fname="logs/failure.txt", drop=False):
    if drop:
        start = 0
    else:
        k = check_insert_failure(fname)
        if k == 0:
            start = 0
            print("All necessary data inserted")
        elif k == -1:
            start = 1
        else:
            start = k

    if start != 0:
        create_table(cur, table_name, columns_list, columns_types_list)
        rdr, header = read_csv()
        conn.commit()
        insert_data(rdr, header, cur, conn, tbl_name, columns_list, start=start, end_pack=end_pack, pack=pack)


# Основний код
check_files_dirs()
create_db()
connection = db_conn()
cursor = connection.cursor()
check_insert_data(cursor, connection, table_name, columns, columns_types, end_pack=-1, pack=20000, fname="logs/failure.txt")
connection.close()
