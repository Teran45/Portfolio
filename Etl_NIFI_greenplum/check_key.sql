DROP FUNCTION IF EXISTS exam.check_data();
CREATE OR REPLACE FUNCTION exam.check_data() 
RETURNS VOID
AS $$
declare
	external_key text := ' ';
begin
	
	--Проверка внешнего ключа для таблицы dim_terminals
	
	FOR external_key IN 
    SELECT distinct dt.terminal FROM exam.fact_transaction dt
    loop
    	if (select terminal_type FROM exam.dim_terminals where terminal_id = external_key) is not null THEN
    		insert into exam.logs values ('dim_terminals', external_key, 'successful', CURRENT_DATE);
    	else 
    		insert into exam.logs values ('dim_terminals', external_key, 'failed', CURRENT_DATE);
    	end if;
	end loop;

	--Проверка внешнего ключа для таблицы dim_cards

	FOR external_key IN 
    SELECT distinct dt.card_num FROM exam.fact_transaction dt
    loop
    	if (select account_num FROM exam.dim_cards where card_num = external_key) is not null THEN
    		insert into exam.logs values ('dim_cards', external_key, 'successful', CURRENT_DATE);
    	else 
    		insert into exam.logs values ('dim_cards', external_key, 'failed', CURRENT_DATE);
    	end if;
	end loop;

	--Проверка внешнего ключа для таблицы dim_accounts

	FOR external_key IN 
    SELECT distinct account_num FROM exam.dim_cards
    loop
    	if (select valid_to FROM exam.dim_accounts where account_num = external_key) is not null THEN
    		insert into exam.logs values ('dim_accounts', external_key, 'successful', CURRENT_DATE);
    	else 
    		insert into exam.logs values ('dim_accounts', external_key, 'failed', CURRENT_DATE);
    	end if;
	end loop;

	--Проверка внешнего ключа для таблицы dim_clients

	FOR external_key IN 
    SELECT distinct client FROM exam.dim_accounts
    loop
    	if (select last_name FROM exam.dim_clients where client_id = external_key) is not null THEN
    		insert into exam.logs values ('dim_clients', external_key, 'successful', CURRENT_DATE);
    	else 
    		insert into exam.logs values ('dim_clients', external_key, 'failed', CURRENT_DATE);
    	end if;
	end loop;

END;
$$
LANGUAGE plpgsql;

select exam.check_data();
truncate exam.logs;
select * from exam.logs

