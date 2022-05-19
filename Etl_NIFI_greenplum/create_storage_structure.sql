CREATE DATABASE examination WITH OWNER teran;
CREATE SCHEMA exam AUTHORIZATION teran;

DROP TABLE IF EXISTS exam.all_data CASCADE;
CREATE TABLE exam.all_data (
    trans_id  VARCHAR(11) PRIMARY KEY,            
    trans_date   timestamp,        
    card_num  VARCHAR(25),
    account_num VARCHAR(25),
    valid_to date,
    client VARCHAR(30),
    last_name VARCHAR(30),
    first_name VARCHAR(30),
    patronymic VARCHAR(30),
    date_of_birth date,
    passport_num VARCHAR(15),
    passport_valid_to VARCHAR(1000),
    phone VARCHAR(15),
    oper_type  VARCHAR(30),
    amt	decimal,
    oper_result VARCHAR(30),
    terminal VARCHAR(30),
    terminal_type VARCHAR(30),
    terminal_city VARCHAR(100),
    terminal_address VARCHAR(1000)
    )    
DISTRIBUTED BY (trans_id);

--таблицы измерений SCD1

DROP TABLE IF EXISTS exam.dim_clients CASCADE;
CREATE TABLE exam.dim_clients (
    client_id   VARCHAR(10) PRIMARY KEY,
    last_name VARCHAR(30),
    first_name VARCHAR(30),
    patronymic VARCHAR(30),
    date_of_birth date,
    passport_num VARCHAR(15),
    passport_valid_to date,
    phone VARCHAR(15),
    create_dt date,
    update_dt date
    )    
DISTRIBUTED BY (client_id);

DROP TABLE IF EXISTS exam.dim_accounts CASCADE;
CREATE TABLE exam.dim_accounts (
    account_num   VARCHAR(25) PRIMARY KEY,
    valid_to date,
    client VARCHAR(10) references exam.dim_clients (client_id),
    create_dt date,
    update_dt date
    )    
DISTRIBUTED BY (account_num);

DROP TABLE IF EXISTS exam.dim_cards CASCADE;
CREATE TABLE exam.dim_cards (
    card_num   VARCHAR(25) PRIMARY KEY,
    account_num VARCHAR(25) references exam.dim_accounts (account_num),
    create_dt date,
    update_dt date
    )    
DISTRIBUTED BY (card_num);

DROP TABLE IF EXISTS exam.dim_terminals CASCADE;
CREATE TABLE exam.dim_terminals (
    terminal_id VARCHAR(15) PRIMARY KEY,
    terminal_type VARCHAR(10),
    terminal_city VARCHAR(40),
    terminal_address VARCHAR(100),
    create_dt date,
    update_dt date
    )    
DISTRIBUTED BY (terminal_id);

DROP TABLE IF EXISTS exam.fact_transaction CASCADE;
CREATE TABLE exam.fact_transaction (
    trans_id  VARCHAR(11),            
    trans_date   timestamp,        
    card_num   VARCHAR(25) references exam.dim_cards (card_num),      
    oper_type  VARCHAR(12),
    amt	decimal,
    oper_result VARCHAR(10),
    terminal VARCHAR(15) references exam.dim_terminals (terminal_id),
    create_dt date,
    update_dt date
    )    
DISTRIBUTED BY (trans_id)
PARTITION BY range (create_dt)
( START (date '2022-02-12') INCLUSIVE
   END (date '2023-02-13') EXCLUSIVE
   EVERY (INTERVAL '1 day') );

--таблицы логов

DROP TABLE IF EXISTS exam.logs;
CREATE TABLE exam.logs (
	table_name VARCHAR(100),
	values_key VARCHAR(100),
	status_key VARCHAR(100),
	create_dt date
	)
DISTRIBUTED BY (values_key);

--таблица отчета

DROP TABLE IF EXISTS exam.report;
CREATE TABLE exam.report (
	FRAUD_DT timestamp,
	PASSPORT VARCHAR(100),
	FIO VARCHAR(100),
	PHONE VARCHAR(100),
	FRAUD_TYPE VARCHAR(100), 
	REPORT_DT date
	)
DISTRIBUTED BY (FRAUD_DT);

--таблицы измерений SCD2

DROP TABLE IF EXISTS exam.dim_terminals_hist;
CREATE TABLE exam.dim_terminals_hist (
    terminal_id VARCHAR(15),
    terminal_type VARCHAR(10),
    terminal_city VARCHAR(40),
    terminal_address VARCHAR(100),
    start_dt date,
    end_dt date
    )    
DISTRIBUTED BY (terminal_id);

DROP TABLE IF EXISTS exam.dim_cards_hist;
CREATE TABLE exam.dim_cards_hist (
    card_num   VARCHAR(25),
    account_num VARCHAR(25),
    start_dt date,
    end_dt date
    )    
DISTRIBUTED BY (card_num);

DROP TABLE IF EXISTS exam.dim_accounts_hist;
CREATE TABLE exam.dim_accounts_hist (
    account_num   VARCHAR(25),
    valid_to date,
    client VARCHAR(10),
    start_dt date,
    end_dt date
    )    
DISTRIBUTED BY (account_num);

DROP TABLE IF EXISTS exam.dim_clients_hist;
CREATE TABLE exam.dim_clients_hist (
    client_id   VARCHAR(10),
    last_name VARCHAR(30),
    first_name VARCHAR(30),
    patronymic VARCHAR(30),
    date_of_birth date,
    passport_num VARCHAR(15),
    passport_valid_to date,
    phone VARCHAR(15),
    start_dt date,
    end_dt date
    )    
DISTRIBUTED BY (client_id);

