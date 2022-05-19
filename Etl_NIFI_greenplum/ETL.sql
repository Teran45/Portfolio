-- XML для обновления таблиц
<sources>
	<source>
		<query>
		   with terminals as (
              select 
              		terminal_id,
				terminal_type,
				terminal_city ,
				terminal_address 
              from exam.dim_terminals 
              order by terminal_id 
              ),
              facts as (
              select
                distinct terminal as terminal_id,
				terminal_type,
				terminal_city ,
				terminal_address
              from exam.all_data
              order by terminal_id)
              select *, CURRENT_DATE as update_dt from facts
              where terminal_id in (
              select facts.terminal_id from facts, terminals
              where facts.terminal_id = terminals.terminal_id and ( 
              facts.terminal_type &lt;&gt; terminals.terminal_type or facts.terminal_city &lt;&gt; terminals.terminal_city or 
              facts.terminal_address &lt;&gt; terminals.terminal_address))
		</query>
		<out>
				dim_terminals
		</out>
		<row>
				terminal_id
		</row>
	</source>

	<source>
		<query>
			  with cards as (
              select 
              	card_num,
				account_num 
              from exam.dim_cards 
              order by card_num 
              ),
              facts as (
              select
                distinct card_num ,
				account_num
              from exam.all_data
              order by card_num)
              select *, CURRENT_DATE as update_dt from facts
              where card_num in (
              select facts.card_num from facts, cards
              where facts.card_num = cards.card_num and ( 
              facts.account_num &lt;&gt; cards.account_num))
		</query>
		<out>
				dim_cards
		</out>
		<row>
				card_num
		</row>
	</source>

	<source>
		<query>
			  with accounts as (
              select 
              account_num,
			  valid_to,
			  client 
              from exam.dim_accounts 
              order by account_num 
              ),
              facts as (
              select
              distinct account_num,
			  valid_to,
			  client
              from exam.all_data
              order by account_num)
              select *, CURRENT_DATE as update_dt from facts
              where account_num in (
              select facts.account_num from facts, accounts
              where facts.account_num = accounts.account_num and ( 
              facts.valid_to &lt;&gt; accounts.valid_to or facts.client &lt;&gt; accounts.client))
		</query>
		<out>
				dim_accounts
		</out>
		<row>
				account_num
		</row>
	</source>

	<source>
		<query>
			  with clients as (
              select 
              client_id,
				last_name,
				first_name,
				patronymic,
				date_of_birth,
				passport_num,
				passport_valid_to,
				phone 
              from exam.dim_clients 
              order by client_id 
              ),
              facts as (
              select
              distinct client as client_id,
				last_name,
				first_name,
				patronymic,
				date_of_birth,
				passport_num,
				passport_valid_to,
				phone
              from exam.all_data
              order by client_id)
              select *, CURRENT_DATE as update_dt from facts
              where client_id in (
              select facts.client_id from facts, clients
              where facts.client_id = clients.client_id and ( 
              facts.last_name &lt;&gt; clients.last_name or facts.first_name &lt;&gt; clients.first_name or facts.passport_valid_to &lt;&gt; clients.passport_valid_to
			  or facts.patronymic &lt;&gt; clients.patronymic or facts.date_of_birth &lt;&gt; clients.date_of_birth or facts.passport_num &lt;&gt; clients.passport_num
  			  or facts.phone &lt;&gt; clients.phone))
          </query>
		<out>
				dim_clients
		</out>
		<row>
				client_id
		</row>
	</source>
</sources>
-- XML для парсинга таблицы фактов
<sources>

	<source>
		<query>
			select
				trans_id,
				trans_date,
				card_num ,
				oper_type,
				amt,
				oper_result,
				terminal,
				CURRENT_DATE as create_dt,
				to_date('9999-12-31', 'YYYY-MM-DD') as update_dt
			from exam.all_data
			where trans_id not in (select trans_id from exam.fact_transaction) and LENGTH(card_num) = 20 and card_num NOT LIKE '%[^0-9]%' and trans_id NOT LIKE '%[^0-9]%'
		</query>
		<out>
				fact_transaction
		</out>
	</source>

	<source>
		<query>
			select
				distinct terminal as terminal_id,
				terminal_type,
				terminal_city,
				terminal_address,
				CURRENT_DATE as create_dt,
				to_date('9999-12-31', 'YYYY-MM-DD') as update_dt
			from exam.all_data
			where terminal not in (select terminal_id from exam.dim_terminals) and LENGTH(terminal_type) = 3
		</query>
		<out>
				dim_terminals
		</out>
	</source>

	<source>
		<query>
			select
				distinct card_num ,
				account_num,
				CURRENT_DATE as create_dt,
				to_date('9999-12-31', 'YYYY-MM-DD') as update_dt
			from exam.all_data
			where card_num not in (select card_num from exam.dim_cards) and LENGTH(card_num) = 20 and card_num NOT LIKE '%[^0-9]%'
		    and LENGTH(account_num) = 20 and account_num NOT LIKE '%[^0-9]%' 
		</query>
		<out>
				dim_cards
		</out>
	</source>

	<source>
		<query>
		select
			distinct account_num,
			valid_to,
			client,
			CURRENT_DATE as create_dt,
			to_date('9999-12-31', 'YYYY-MM-DD') as update_dt
		from exam.all_data
		where account_num not in (select account_num from exam.dim_accounts) and LENGTH(account_num) = 20 and account_num NOT LIKE '%[^0-9]%'
		</query>
		<out>
			dim_accounts
		</out>
	</source>

	<source>
		<query>
			select
				distinct client as client_id,
				last_name,
				first_name,
				patronymic,
				date_of_birth,
				passport_num,
				passport_valid_to,
				phone,
				CURRENT_DATE as create_dt,
				to_date('9999-12-31', 'YYYY-MM-DD') as update_dt
			from exam.all_data
		where client not in (select client_id from exam.dim_clients) and LENGTH(passport_num) = 10 and passport_num NOT LIKE '%[^0-9]%'
		    and LENGTH(phone) = 11 and phone NOT LIKE '%[^0-9]%'
		</query>
		<out>
			dim_clients
		</out>
	</source>
</sources>
-- XML для перемещения данныч в SCD2

<sources>
	<source>
		<query>
			  with terminals as (
              select 
              	terminal_id,
				terminal_type,
				terminal_city ,
				terminal_address,
				create_dt as start_dt 
              from exam.dim_terminals 
              order by terminal_id 
              ),
              facts as (
              select
                distinct terminal as terminal_id,
				terminal_type,
				terminal_city ,
				terminal_address
              from exam.all_data
              order by terminal_id)
              select *, CURRENT_DATE  as end_dt from terminals
              where terminal_id in (
              select facts.terminal_id from facts, terminals
              where facts.terminal_id = terminals.terminal_id and ( 
              facts.terminal_type &lt;&gt; terminals.terminal_type or facts.terminal_city &lt;&gt; terminals.terminal_city or 
              facts.terminal_address &lt;&gt; terminals.terminal_address))
		</query>
		<out>
				dim_terminals_hist
		</out>
		<row>
				terminal_id
		</row>
	</source>

	<source>
		<query>
			  with cards as (
              select 
              	card_num,
				account_num,
				create_dt as start_dt 
              from exam.dim_cards 
              order by card_num 
              ),
              facts as (
              select
                distinct card_num ,
				account_num
              from exam.all_data
              order by card_num)
              select *, CURRENT_DATE as end_dt from cards
              where card_num in (
              select facts.card_num from facts, cards
              where facts.card_num = cards.card_num and ( 
              facts.account_num &lt;&gt; cards.account_num))
		</query>
		<out>
				dim_cards_hist
		</out>
		<row>
				card_num
		</row>
	</source>

	<source>
		<query>
			  with accounts as (
              select 
              account_num,
			  valid_to,
			  client,
				create_dt as start_dt 
              from exam.dim_accounts 
              order by account_num 
              ),
              facts as (
              select
              distinct account_num,
			  valid_to,
			  client
              from exam.all_data
              order by account_num)
              select *, CURRENT_DATE as end_dt from accounts
              where account_num in (
              select facts.account_num from facts, accounts
              where facts.account_num = accounts.account_num and ( 
              facts.valid_to &lt;&gt; accounts.valid_to or facts.client &lt;&gt; accounts.client))
		</query>
		<out>
				dim_accounts_hist
		</out>
		<row>
				account_num
		</row>
	</source>

	<source>
		<query>
			  with clients as (
              select 
              client_id,
				last_name,
				first_name,
				patronymic,
				date_of_birth,
				passport_num,
				passport_valid_to,
				phone,
				create_dt as start_dt 
              from exam.dim_clients 
              order by client_id 
              ),
              facts as (
              select
              distinct client as client_id,
				last_name,
				first_name,
				patronymic,
				date_of_birth,
				passport_num,
				passport_valid_to,
				phone
              from exam.all_data
              order by client_id)
              select *, CURRENT_DATE as end_dt from clients
              where client_id in (
              select facts.client_id from facts, clients
              where facts.client_id = clients.client_id and ( 
              facts.last_name &lt;&gt; clients.last_name or facts.first_name &lt;&gt; clients.first_name or facts.passport_valid_to &lt;&gt; clients.passport_valid_to
			  or facts.patronymic &lt;&gt; clients.patronymic or facts.date_of_birth &lt;&gt; clients.date_of_birth or facts.passport_num &lt;&gt; clients.passport_num
  			  or facts.phone &lt;&gt; clients.phone))
          </query>
		<out>
				dim_clients_hist
		</out>
		<row>
				client_id
		</row>
	</source>
</sources>