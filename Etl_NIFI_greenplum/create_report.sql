DROP FUNCTION IF EXISTS exam.report();
CREATE OR REPLACE FUNCTION exam.report() 
RETURNS VOID
AS $$
declare
	FRAUD_DT timestamp := now();
	PASSPORT text := ' ';
	FIO text := ' ';
	PHONE_Number text := '';
	crime_ac text := '';
begin
	
	--Совершение операции при просроченном паспорте.
	
	for FRAUD_DT, FIO, PASSPORT, PHONE_Number in
		select trans_date, acc_cl_cr.f, acc_cl_cr.passport_num, acc_cl_cr.phone
		from exam.fact_transaction as dt
		left join
	-- Присоединяем таблицу с номерами карточек что бы из нее получить номер аккаунта клиента
		(select card_num, acc_cl.passport_valid_to, acc_cl.f, acc_cl.passport_num, acc_cl.phone
		from exam.dim_cards
	-- Присоединяем таблицу с аккаунтами что бы из нее получить номер клиента
		left join
		(select 
		account_num, cl.passport_valid_to, cl.f, cl.passport_num, cl.phone
		from exam.dim_accounts as acc
		left join
	-- Присоединяем таблицу с клиентов что бы из нее получить данные о клиенте
		(select client_id, passport_valid_to, last_name || ' ' || first_name || ' ' || patronymic as f, passport_num, phone from exam.dim_clients) as cl
		on acc.client = cl.client_id) as acc_cl
		on exam.dim_cards.account_num = acc_cl.account_num) as acc_cl_cr
		on dt.card_num = acc_cl_cr.card_num
		where trans_date > acc_cl_cr.passport_valid_to
	loop 
		insert into exam.report values (FRAUD_DT, PASSPORT, FIO, PHONE_Number, 'Просроченный паспорт', CURRENT_DATE);
	end loop;
	
	--Совершение операции при недействующем договоре.

	for FRAUD_DT, crime_ac in
		select trans_date, acc_cl.account_num
		from exam.fact_transaction as dt
		left join
	-- Присоединяем таблицу с номерами карточек что бы из нее получить номер аккаунта клиента
		(select card_num, valid_to, acc.account_num
		from exam.dim_cards as cards
		left join
	-- Присоединяем таблицу с аккаунтами что бы из нее получить срок действия договора
		(select account_num, valid_to
		from exam.dim_accounts) as acc
		on cards.account_num = acc.account_num) as acc_cl
		on dt.card_num = acc_cl.card_num
		where trans_date > valid_to
	loop
		for PASSPORT, FIO, PHONE_Number in
	-- По номеру аккаунта находим клиента и получаем его данные
			select passport_num, last_name || ' ' || first_name || ' ' || patronymic, phone 
			from exam.dim_clients
			where client_id =
				(
				select client from exam.dim_accounts where account_num = crime_ac
				)
		loop 
			insert into exam.report values (FRAUD_DT, PASSPORT, FIO, PHONE_Number, 'Недействующий договор', CURRENT_DATE);
		end loop;
	end loop;

	-- Совершение операции в разных городах в течение 1 часа.

	for FRAUD_DT, crime_ac in
		with opr as (
		select trans_id, card_num, trans_date, terminal, terminal_city, oper_result,
	-- Находим разницу между совершенными операциями для определния промежутка времени
		trans_date - lag(trans_date) over (partition by card_num order by trans_date) as lag,
		lead(trans_date) over (partition by card_num order by trans_date) - trans_date as lead
		from exam.fact_transaction dt 
		left join (select terminal_id, terminal_city from exam.dim_terminals) as t
		on dt.terminal = t.terminal_id
		),
		res as (
		select card_num, count( distinct terminal_city) as city, max(trans_date) as crime_time 
		from opr
	-- Применяем условия, о том что операции должны быть с разницей в один час
		where (lag <= '01:00:00' or lead <= '01:00:00' )
		group by card_num)
	-- Применяем условия, что это должны быть разные города
		select crime_time, card_num from res where city > 1
	loop
		for PASSPORT, FIO, PHONE_Number in
	-- По номеру аккаунта находим клиента и получаем его данные
			select passport_num, last_name || ' ' || first_name || ' ' || patronymic, 
			phone 
			from exam.dim_clients
			where client_id =
				(
				select client 
				from exam.dim_accounts 
				where account_num = 
					(
					select account_num 
					from exam.dim_cards 
					where card_num = crime_ac
					)
				)
		loop 
			insert into exam.report values (FRAUD_DT, PASSPORT, FIO, PHONE_Number, 'Операции в разных городах в течение 1 часа', CURRENT_DATE);
		end loop;
	end loop;

	-- Попытка подбора сумм

	for FRAUD_DT, crime_ac in
		with opr as (
		select trans_id, card_num, trans_date, terminal, terminal_city, amt, oper_result, 
		trans_date - lag(trans_date) over (partition by card_num order by trans_date) as time_lag,
		lead(trans_date) over (partition by card_num order by trans_date) - trans_date as time_lead,
		amt - lead(amt) over (partition by card_num order by trans_date) as amt_lead
		from exam.fact_transaction dt 
		left join (select terminal_id, terminal_city from exam.dim_terminals) as t
		on dt.terminal = t.terminal_id
		),
	-- Находим количество сделок с отказом
		stat as (
		select card_num, count(oper_result) as status from opr 
		where oper_result = 'Отказ'
		group by card_num),
		res as (
		select card_num, count(terminal_city) as city, max(trans_date) as crime_time, sum(time_lead) as s, max(oper_result) as oper_result 
		from opr
		where (amt_lead > 0 or amt_lead is null)
		group by opr.card_num
		)
		select res.crime_time, res.card_num
		from res
		left join stat 
		on stat.card_num = res.card_num
	-- Устанавливаем условия поиска
		where city > 3 and oper_result = 'Успешно' and s < '00:20:00' and (res.city - stat.status = 1)
	loop
		for PASSPORT, FIO, PHONE_Number in
			select passport_num, last_name || ' ' || first_name || ' ' || patronymic, 
			phone 
			from exam.dim_clients
			where client_id =
				(
				select client 
				from exam.dim_accounts 
				where account_num = 
					(
					select account_num 
					from exam.dim_cards 
					where card_num = crime_ac
					)
				)
		loop 
			insert into exam.report values (FRAUD_DT, PASSPORT, FIO, PHONE_Number, 'Попытка подбора сумм', CURRENT_DATE);
		end loop;
	end loop;
	
END;
$$
LANGUAGE plpgsql;

select exam.report();

truncate exam.report;
select distinct * from exam.report order by fraud_type ;