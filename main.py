import sqlite3
import os
import pandas as pd
import csv
import jaydebeapi



connect = sqlite3.connect('database.db')
cursor = connect.cursor()

# Создание таблиц. Имена таблиц указаны в кавычках, т.к. в sqlite схемы данных не реализованы.
def init():
    cursor = connect.cursor()
    cursor.execute('DROP TABLE if exists "de2hk.s_21_DWH_DIM_CLIENTS"')
    cursor.execute('''
        CREATE TABLE "de2hk.s_21_DWH_DIM_CLIENTS"(
            client_id varchar(128) primary key,
            last_name varchar(128),
            first_name varchar(128),
            patronymic varchar(128),
            date_of_birth date,
            passport_num varchar(128),
            passport_valid_to date,
            phone varchar(128),
            create_dt date,
            update_dt date
        )
    ''')

    cursor.execute('DROP TABLE if exists "de2hk.s_21_DWH_DIM_ACCOUNTS"')
    cursor.execute('''
        CREATE TABLE "de2hk.s_21_DWH_DIM_ACCOUNTS"(
            account_num varchar(128) primary key,
            valid_to date,
            client varchar(128),
            create_dt date,
            update_dt date,
            foreign key (client) references "de2hk.s_21_DWH_DIM_CLIENT" (client_id)
        )
    ''')

    cursor.execute('DROP TABLE if exists "de2hk.s_21_DWH_DIM_CARDS"')
    cursor.execute('''
        CREATE TABLE "de2hk.s_21_DWH_DIM_CARDS"(
            card_num varchar(128) primary key,
            account_num varchar(128),
            create_dt date,
            update_dt date,
            foreign key (account_num) references "de2hk.s_21_DWH_DIM_ACCOUNTS" (account_num)
        )
    ''')

    cursor.execute('DROP TABLE if exists "de2hk.s_21_DWH_DIM_TERMINALS"')
    cursor.execute('''
        CREATE TABLE "de2hk.s_21_DWH_DIM_TERMINALS"(
            id integer primary key autoincrement,
            terminal_id varchar(128),
            terminal_type varchar(128),
            terminal_city varchar(128),
            terminal_address varchar(128),
            create_dt date default current_timestamp,
            update_dt date default (datetime('2999-12-31 23:59:59'))
        )
    ''')

    cursor.execute('DROP TABLE if exists "de2hk.s_21_DWH_FACT_TRANSACTIONS"')
    cursor.execute('''
        CREATE TABLE "de2hk.s_21_DWH_FACT_TRANSACTIONS"(
            trans_id varchar(128),
            trans_date date,
            card_num varchar(128),
            oper_type varchar(128),
            amt decimal(10,2),
            oper_result varchar(128),
            terminal varchar(128),
            foreign key (card_num) references "de2hk.s_21_DWH_DIM_CARDS" (card_num),
            foreign key (terminal) references "de2hk.s_21_DWH_DIM_TERMINALS" (terminal_id)
        )
    ''')

    cursor.execute('DROP TABLE if exists "de2hk.s_21_DWH_FACT_PASSPORT_BLACKLIST"')
    cursor.execute('''
        CREATE TABLE "de2hk.s_21_DWH_FACT_PASSPORT_BLACKLIST"(
            passport_num varchar(128),
            entry_dt date
        )
    ''')

    cursor.execute('''
        CREATE TABLE if not exists "de2hk.s_21_REP_FRAUD"(
            event_dt date,
            passport varchar(128),
            fio varchar(364),
            phone varchar(128),
            event_type varchar(128),
            report_dt date default current_timestamp
        )
    ''')

#-------------------------------------
# Функция проверяет наличие шаблона(даты) в названии файлов и останавливает скрипт в случае отсутсвия такового.
def fileDate():
    date = ''
    lst = os.listdir('.')
    lst.sort()
    for i in lst:
        if i.startswith('transactions'):
            date = i.split("_", 1)[1].split(".", 1)[0]
            break
    if date == '':
        raise Exception('Файлы не найдены')
    if not os.path.isfile('transactions_' + date + '.txt'):
        raise Exception('Файл transactions_DDMMYYYY.txt не найден')
    if not os.path.isfile('passport_blacklist_' + date + '.xlsx'):
        raise Exception('Файл passport_blacklist_DDMMYYYY.xlsx не найден')
    if not os.path.isfile('terminals_' + date + '.xlsx'):
        raise Exception('Файл terminals_DDMMYYYY.xlsx не найден')
    return date
ddmmyyyy = fileDate()

#-------------------------------------
#Функция делает выгрузку из СУБД Oracle, схемы BANK в файлы.
def load():
    connect = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
        ['de2hk', 'bilbobaggins'],
        'ojdbc7.jar'
    )

    cursor = connect.cursor()

    with open('bank_cards.csv', 'w', newline='') as file:
        cursor.execute('select * from bank.cards')
        i = cursor.fetchall()
        csv_writer = csv.writer(file)
        csv_writer.writerows(i)
    with open('bank_accounts.csv', 'w', newline='') as file:
        cursor.execute('select * from bank.accounts')
        i = cursor.fetchall()
        csv_writer = csv.writer(file)
        csv_writer.writerows(i)
    with open('bank_clients.csv', 'w', newline='') as file:
        cursor.execute('select * from bank.clients')
        i = cursor.fetchall()
        csv_writer = csv.writer(file)
        csv_writer.writerows(i)
    connect.close()

#-------------------------------------
#Функции создают таблицы из файлов выгрузки из СУБД Oracle, схемы BANK.
def tableCards():
    cursor = connect.cursor()
    source = "bank_cards.csv"
    df = pd.read_csv(source)
    df.to_sql('de2hk.s_21_STG_CARDS', con=connect, if_exists='replace', index=False)
    cursor.execute('''
        INSERT INTO "de2hk.s_21_DWH_DIM_CARDS" (card_num, account_num, create_dt, update_dt
        ) SELECT * FROM "de2hk.s_21_STG_CARDS";
    ''')
    connect.commit()


def tableAccounts():
    cursor = connect.cursor()
    source = "bank_accounts.csv"
    df = pd.read_csv(source)
    df.to_sql('de2hk.s_21_STG_ACCOUNTS', con=connect, if_exists='replace', index=False)
    cursor.execute('''
        INSERT INTO "de2hk.s_21_DWH_DIM_ACCOUNTS" (account_num, valid_to, client, create_dt, update_dt
        ) SELECT * FROM "de2hk.s_21_STG_ACCOUNTS";
    ''')
    connect.commit()


def tableClients():
    cursor = connect.cursor()
    source = "bank_clients.csv"
    df = pd.read_csv(source, encoding= 'windows-1251')
    df.to_sql('de2hk.s_21_STG_CLIENTS', con=connect, if_exists='replace', index=False)
    cursor.execute('''
        INSERT INTO "de2hk.s_21_DWH_DIM_CLIENTS" (client_id, last_name, first_name, patronymic, date_of_birth, passport_num,
            passport_valid_to, phone, create_dt, update_dt 
        ) SELECT * FROM "de2hk.s_21_STG_CLIENTS";
    ''')
    connect.commit()

#-------------------------------------
#Функции создают таблицы из предоставленных файлов выгрузки в формате transactions_DDMMYYYY.txt,
#passport_blacklist_DDMMYYYY.xlsx, terminals_DDMMYYYY.xlsx и перемещают отработанные файлы в Archive с разширением .backup
def transactions(ddmmyyyy):
    cursor = connect.cursor()
    source = "transactions_" + ddmmyyyy + ".txt"
    df = pd.read_csv(source, sep=';')
    data = df.values.tolist()
    cursor.executemany('''
        INSERT INTO "de2hk.s_21_DWH_FACT_TRANSACTIONS" (
            trans_id, trans_date,amt, card_num, oper_type, oper_result,
            terminal) VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', data)
    connect.commit()
    backup_file_to_archive = os.path.join("archive", "transactions_" + ddmmyyyy + ".txt.backup")
    os.rename(source, backup_file_to_archive)


def passport_blacklist(ddmmyyyy):
    cursor = connect.cursor()
    source = "passport_blacklist_" + ddmmyyyy + ".xlsx"
    df = pd.read_excel(source, engine='openpyxl')
    df.to_sql('de2hk.s_21_STG_PASSPORT_BLACKLIST', con=connect, if_exists='replace', index=False)
    cursor.execute('''
        INSERT INTO "de2hk.s_21_DWH_FACT_PASSPORT_BLACKLIST" (
            passport_num, entry_dt
        ) SELECT
            passport, date from "de2hk.s_21_STG_PASSPORT_BLACKLIST";
    ''')
    cursor.execute('DROP TABLE if exists "de2hk.s_21_STG_PASSPORT_BLACKLIST"')
    connect.commit()
    backup_file = os.path.join("archive", "passport_blacklist_" + ddmmyyyy + ".xlsx.backup")
    os.rename(source, backup_file)


def terminals(ddmmyyyy):
    cursor = connect.cursor()
    source = "terminals_" + ddmmyyyy + ".xlsx"
    df = pd.read_excel(source, engine='openpyxl')
    df.to_sql('de2hk.s_21_STG_TERMINALS', con=connect, if_exists='replace', index=False)
    cursor.execute('''
        INSERT INTO "de2hk.s_21_DWH_DIM_TERMINALS" (
            terminal_id, terminal_type, terminal_city, terminal_address
        ) SELECT
            terminal_id, terminal_type, terminal_city, terminal_address from "de2hk.s_21_STG_TERMINALS";
    ''')
    cursor.execute('DROP TABLE if exists "de2hk.s_21_STG_TERMINALS"')
    connect.commit()
    backup_file = os.path.join("archive", "terminals_" + ddmmyyyy + ".xlsx.backup")
    os.rename(source, backup_file)

#-------------------------------------
#Функция создает сводную таблицу из файлов выгрузки из СУБД Oracle (используется в дальнейшем).
def tableBank():
    cursor = connect.cursor()
    cursor.execute('DROP TABLE if exists "de2hk.s_21_STG_BANK"')
    cursor.execute('''
        CREATE TABLE "de2hk.s_21_STG_BANK" as
            SELECT
                t1.client_id,
                t1.last_name || ' ' || first_name || ' ' || patronymic as fio,
                t1.phone,
                t1.passport_num,
                t1.passport_valid_to,
                t2.account_num,
                t2.valid_to,
                t2.create_dt as acc_create_dt,
                t2.update_dt as acc_update_dt
            FROM "de2hk.s_21_DWH_DIM_CLIENTS" t1
            JOIN "de2hk.s_21_DWH_DIM_ACCOUNTS" t2  ON t1.client_id = t2.client
        ''')

    cursor.execute('DROP TABLE if exists "de2hk.s_21_STG_BANK_ALL"')
    cursor.execute('''
        CREATE TABLE "de2hk.s_21_STG_BANK_ALL" as
            SELECT 
                t1.client_id,
                t1.fio,
                t1.phone,
                t1.passport_num,
                t1.passport_valid_to,
                t1.account_num,
                t1.valid_to,
                t1.acc_create_dt,
                t1.acc_update_dt,
                RTRIM(t2.card_num) as card_num,
                t2.create_dt as card_create_dt,
                t2.update_dt as card_update_dt
            FROM "de2hk.s_21_STG_BANK" t1
            JOIN "de2hk.s_21_DWH_DIM_CARDS" t2 ON t1.account_num = t2.account_num
    ''')

#-------------------------------------
#Функция строит накоплением витрину отчетности по операциям с
#просроченным (passport_valid_to < transaction_date) или заблокированном паспортом (из черного списка).
def passportFraudReport():
    cursor = connect.cursor()
    cursor.execute('''
        INSERT INTO "de2hk.s_21_REP_FRAUD"(
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        ) 
        SELECT
            t2.trans_date,
            t1.passport_num,
            t1.fio,
            t1.phone,
            'просроченный или заблокированный паспорт',
            current_timestamp
        FROM "de2hk.s_21_STG_BANK_ALL" t1
        INNER JOIN "de2hk.s_21_DWH_FACT_TRANSACTIONS" t2
        on t1.card_num=t2.card_num
        WHERE t1.passport_num in (SELECT passport_num FROM "de2hk.s_21_DWH_FACT_PASSPORT_BLACKLIST")
        OR  t1.passport_valid_to < t2.trans_date 
    ''')
    connect.commit()

#-------------------------------------
#Функция строит накоплением витрину отчетности по операциям при недействующем договоре.
def transactionFraudReport():
    cursor = connect.cursor()
    cursor.execute('''
        INSERT INTO "de2hk.s_21_REP_FRAUD"(
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        ) 
        SELECT
            t1.trans_date,
            t2.passport_num,
            t2.fio,
            t2.phone,
            'недействующий договор',
            current_timestamp
        FROM "de2hk.s_21_DWH_FACT_TRANSACTIONS" t1
        INNER JOIN "de2hk.s_21_STG_BANK_ALL" t2 
        on t1.card_num = t2.card_num 
        WHERE t1.trans_date > t2.valid_to
    ''')
    connect.commit()

#-------------------------------------
#Функция определяет в т.ч. номера карт, по которым совершены транзакции в двух и более разных городах; список транзакций
#по этим картам с данными клиентов, местом и временем; создает таблицу с использованием оконных функций (LAG OVER)
# для дальнейшего анализа по условиям.
def cityFraud():
    cursor = connect.cursor()
    cursor.execute('DROP TABLE if exists "de2hk.s_21_STG_CITY_COUNT"')
    cursor.execute('''
        CREATE TABLE "de2hk.s_21_STG_CITY_COUNT" as
            SELECT 
                t3.fio,
                t3.passport_num as passport,
                t3.phone,
                t1.trans_date as event_dt,
                t1.card_num,
                t2.terminal_city,
                count(distinct t2.terminal_city)
            FROM "de2hk.s_21_DWH_FACT_TRANSACTIONS" t1
            INNER JOIN "de2hk.s_21_DWH_DIM_TERMINALS" t2 on t1.terminal=t2.terminal_id
            INNER JOIN "de2hk.s_21_STG_BANK_ALL" t3
            on t1.card_num = t3.card_num
            GROUP BY t1.card_num
            HAVING count(distinct terminal_city) > 1
    ''')

    cursor.execute('DROP TABLE if exists "de2hk.s_21_STG_CITY_FAUD"')
    cursor.execute('''
        CREATE TABLE "de2hk.s_21_STG_CITY_FAUD" as
            SELECT 
                t2.terminal_city,
                t1.trans_date,
                t1.trans_id,
                t3.fio,
                t3.passport,
                t3.phone,
                t1.card_num                
            FROM "de2hk.s_21_DWH_FACT_TRANSACTIONS" t1
            INNER JOIN "de2hk.s_21_DWH_DIM_TERMINALS" t2 on t1.terminal=t2.terminal_id
            INNER JOIN "de2hk.s_21_STG_CITY_COUNT" t3 on t1.card_num = t3.card_num
    ''')

    cursor.execute('DROP TABLE if exists "de2hk.s_21_STG_CITY_FAUD_1"')
    cursor.execute('''
        CREATE TABLE "de2hk.s_21_STG_CITY_FAUD_1" as
            SELECT
                fio,
                passport,
                phone,
                card_num,
                trans_id,
                terminal_city as city_1,
                trans_date as city_date_1,
                lag(terminal_city) over(partition by card_num order by trans_date) as city_2,
                lag(trans_date) over(partition by card_num order by trans_date) as city_date_2
            FROM "de2hk.s_21_STG_CITY_FAUD"
    ''')

#Функция строит накоплением витрину отчетности с условиями: разные города совершения транзакций (city_1 != city_2)
# и временной интервал менее часа (strftime('%s', city_date_1, '-1 hours') < strftime('%s', city_date_2))
def cityFraudReport():
    cursor = connect.cursor()
    cursor.execute('''
        INSERT INTO "de2hk.s_21_REP_FRAUD"(
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        ) SELECT
            city_date_1 as trans_date,
            passport,
            fio,
            phone,
            'cовершение операций в разных городах в течение одного часа',
            current_timestamp
        FROM "de2hk.s_21_STG_CITY_FAUD_1"
            WHERE
            city_1 != city_2
            AND
            strftime('%s', city_date_1, '-1 hours') < strftime('%s', city_date_2)
    ''')
    connect.commit()

#-------------------------------------
#Функция строит промежуточную сводную таблицу с использованием оконных фунций в т.ч с результатами совершения транзакций
#(REJECT-SUCCESS) от большего к меньшему по номеру карты для дальнейшего анализа по условиям.
def operationFraud():
    cursor = connect.cursor()
    cursor.execute('DROP TABLE if exists "de2hk.s_21_STG_OPERATION_FRAUD"')
    cursor.execute('''
        CREATE TABLE "de2hk.s_21_STG_OPERATION_FRAUD" as
            SELECT 
                t2.fio,
                t2.passport_num as passport,
                t2.phone,
                t1.oper_type,
                t1.oper_result,
                t1.trans_date as event_dt,
                t1.card_num,
                LAG(t1.oper_result) over (partition by t1.card_num order by
                    t1.trans_date) as oper_result_1,
                LAG(t1.oper_result, 2) over (partition by t1.card_num order by
                    t1.trans_date) as oper_result_2,
                LAG(t1.oper_result, 3) over (partition by t1.card_num order by
                    t1.trans_date) as oper_result_3,
                LAG(t1.trans_date, 3) over (partition by t1.card_num order by
                    t1.trans_date) as prev_oper_date
            FROM "de2hk.s_21_DWH_FACT_TRANSACTIONS" t1
            INNER JOIN "de2hk.s_21_STG_BANK_ALL" t2
            on t1.card_num = t2.card_num

    ''')

#Функция строит витрину отчетности с условиями: результат выполнения транзакций по одной карте REJECT-REJECT-REJECT-SUCCESS,
#время на  REJECT-REJECT-REJECT-SUCCESS  - 20 минут
def operationFraudReport():
    cursor = connect.cursor()
    cursor.execute('''
            INSERT INTO "de2hk.s_21_REP_FRAUD"(
                event_dt,
                passport,
                fio,
                phone,
                event_type,
                report_dt
            ) SELECT
                event_dt,
                passport,
                fio,
                phone,
                'попытка подбора суммы',
                current_timestamp
            FROM "de2hk.s_21_STG_OPERATION_FRAUD"
                WHERE
                oper_result = 'SUCCESS'
                AND oper_result_1 = 'REJECT' AND oper_result_2 = 'REJECT' AND oper_result_3 = 'REJECT'
                AND strftime('%s', event_dt, '-20 minutes') < strftime('%s', prev_oper_date)

    ''')
    connect.commit()



def show():
    cursor.execute('select * from "de2hk.s_21_REP_FRAUD"')
    for row in cursor.fetchall():
        print(row)




init()
load()
tableCards()
tableAccounts()
tableClients()
transactions(ddmmyyyy)
passport_blacklist(ddmmyyyy)
terminals(ddmmyyyy)
tableBank()
passportFraudReport()
transactionFraudReport()
cityFraud()
cityFraudReport()
operationFraud()
operationFraudReport()
show()