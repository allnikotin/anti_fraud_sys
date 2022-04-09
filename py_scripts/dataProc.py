import jaydebeapi


def showData(cursor, object):
	'''
	Отображение объекта (таблицы или представления) в терминале
	'''
	print('\n', '-_'*10, object, '-_'*10, '\n')
	cursor.execute('SELECT * FROM', object)
	col = [t[0] for t in cursor.description]
	print(col, '\n')
	for row in cursor.fetchall():
		print(row)
	print()


def show_in_file(cursor, object):
	'''
	Отображение объекта (таблицы или представления) в файле term.txt
	'''
	cursor.execute('SELECT * FROM', object)
	with open('terminal.txt', 'a', encoding='utf-8') as f:
		print('\n', '-_'*10, object, '-_'*10, '\n', file=f)
		col = [t[0] for t in cursor.description]
		print(col, '\n', file=f)
		for row in cursor.fetchall():
			print(row, file=f)
		print(file=f)


def to_rep_fraud(cursor):
	'''
	Выгрузка данных в историческую витрину
	'''
	cursor.execute(''' 
		INSERT INTO DE2HK.s_20_REP_FRAUD(
			event_dt,
			passport,
			fio,
			phone,
			event_type,
			report_dt
		) select distinct
			t1.event_dt,
			t1.passport,
			t1.fio,
			t1.phone,
			t1.event_type,
			t1.report_dt
		from DE2HK.s_20_STG_FRAUD t1
		left join DE2HK.s_20_REP_FRAUD t2
		on t1.event_dt = t2.event_dt
		and t1.passport = t2.passport
		where t2.passport is null
		''')


def report_gen(cursor, date):
	'''
	Функция создания отчетов по типам и загрузки в витрину
	'''
	# Подготовка под создание новых представлений
	del_ref(cursor)

	# Представление - сводные данные о пользователях, картах и их договорах из схемы BANK
	cursor.execute(''' 
	CREATE VIEW DE2HK.s_20_VIEW_INFO_CUST as
		select
			trim(t1.card_num) as card_num,
			trim(t2.account) as account,
			t2.valid_to,
			trim(t2.client) as client,
			trim(t3.passport_num) as  passport_num,
			case
				when t3.passport_valid_to is null
					then date '2999-12-31'
				else t3.passport_valid_to
			end as passport_valid_to,
			trim(t3.last_name) || ' ' || trim(t3.first_name) || ' ' || trim(t3.PATRONYMIC) as fio,
			trim(t3.PHONE) as phone
		from BANK.CARDS t1
		inner join BANK.ACCOUNTS t2	on t1.account = t2.account
		inner join BANK.CLIENTS t3 on t2.client = t3.client_id
	''')

	# Стейджинговая таблица для отчетов всех типов
	cursor.execute(''' 
		CREATE TABLE DE2HK.s_20_STG_FRAUD(
		event_dt timestamp,
		passport varchar(11),
		fio varchar(200),
		phone varchar(20),
		event_type varchar(200),
		report_dt date
		)
		''')

	# Подготовка стейджинговой таблицы отчетов
	cursor.execute('TRUNCATE TABLE DE2HK.s_20_STG_FRAUD')

	# ************************************* #
	# Таблица анализа попыток подбора суммы #
	cursor.execute(''' 
	CREATE TABLE DE2HK.s_20_STG_ANALYS_4(
		trans_id varchar(20),
		trans_date1 timestamp,
		card_num varchar(19),
		oper_type varchar(20),
		amt1 decimal(*,2),
		trans_date2 timestamp,
		amt2 decimal(*,2),
		oper_result2 integer
	)
	''')

	# Выявление мошеннических операций 4 типа
	cursor.execute(''' 
		INSERT INTO DE2HK.s_20_STG_ANALYS_4 (
			trans_id,
			trans_date1,
			card_num,
			oper_type,
			amt1,
			trans_date2,
			amt2,
			oper_result2
			)
			select
				t1.trans_id,
				t1.trans_date,
				t1.card_num,
				t1.oper_type,
				t1.amt,
				t2.trans_date,
				t2.amt,
				t2.oper_result
			from DE2HK.s_20_DWH_FACT_TRANSACTIONS t1
			inner join DE2HK.s_20_DWH_FACT_TRANSACTIONS t2
			on t1.card_num = t2.card_num
			and t1.oper_type = t2.oper_type
			and t2.trans_date between t1.trans_date - interval '20' minute and t1.trans_date
			where t1.trans_date > cast(to_date(?, 'YYYY-MM-DD') as timestamp) - interval '20' minute
			and t1.oper_result = 1
		''', [date])

	cursor.execute(''' 
		INSERT INTO DE2HK.s_20_STG_FRAUD (
			event_dt,
			passport,
			fio,
			phone,
			event_type,
			report_dt
			)
			SELECT
				t1.trans_date1,
				t2.passport_num,
				t2.fio,
				t2.phone,
				'Подбор суммы',
				to_date(?, 'YYYY-MM-DD')
			FROM (
				SELECT
					t.*,
					case
						when lead(oper_result2,1,2) over(partition by trans_id order by trans_date2) = 2
							and lag(amt2,2,0) over(partition by trans_id order by trans_date2) > lag(amt2,1,0) over(partition by trans_id order by trans_date2)
							and	lag(amt2,1,0) over(partition by trans_id order by trans_date2) > amt2
							and lag(oper_result2,2,1) over(partition by trans_id order by trans_date2) = 0
							and lag(oper_result2,1,1) over(partition by trans_id order by trans_date2) = 0
							then 1
						else 0
					end as decr_amt_flg
				FROM DE2HK.s_20_STG_ANALYS_4 t
					ORDER BY trans_id, trans_date2
				) t1
			INNER JOIN DE2HK.s_20_VIEW_INFO_CUST t2
			ON t1.card_num = t2.card_num
			WHERE t1.decr_amt_flg = 1
		''', [date])

	# Выгрузка данных в витрину
	to_rep_fraud(cursor)
	# ********************** #

	
	# Подготовка стейджинговой таблицы отчетов
	cursor.execute('TRUNCATE TABLE DE2HK.s_20_STG_FRAUD')

	
	# ***************************************** #
	# Таблица анализа операция в разных городах #
	cursor.execute(''' 
		CREATE TABLE DE2HK.s_20_STG_ANALYS_3(
			trans_id varchar(20),
			trans_date timestamp,
			card_num varchar(19),
			terminal varchar(10),
			terminal_city varchar(50)
			)
		''')

	# выявление мошеннических операций 3 типа (разные города)
	cursor.execute(''' 
		INSERT INTO DE2HK.s_20_STG_ANALYS_3 (
			trans_id,
			trans_date,
			card_num,
			terminal,
			terminal_city
			)
			select
				t1.trans_id,
				t1.trans_date,
				t1.card_num,
				t1.terminal,
				t2.terminal_city
			from DE2HK.s_20_DWH_FACT_TRANSACTIONS t1
			inner join DE2HK.s_20_DWH_DIM_TERMINALS_HIST t2
			on t1.terminal = t2.terminal_id 
			and trunc(t1.trans_date) between t2.effective_from and t2.effective_to
			and t2.deleted_flg = 0
			where t1.trans_date > cast(to_date(?, 'YYYY-MM-DD') as timestamp) - interval '1' hour
		''', [date])

	cursor.execute(''' 
		INSERT INTO DE2HK.s_20_STG_FRAUD (
			event_dt,
			passport,
			fio,
			phone,
			event_type,
			report_dt
			)
			SELECT
				p1.trans_date2,
				p2.passport_num,
				p2.fio,
				p2.phone,
				'Операции в разных городах в течении часа',
				to_date(?, 'YYYY-MM-DD')
			FROM (
				SELECT
					t2.trans_date as trans_date2,
					t2.card_num,
					t1.trans_id as trans_id1,
					t1.terminal_city as terminal_city1,
					t1.trans_date as trans_date1,
					t2.trans_id as trans_id2,
					t2.terminal_city as terminal_city2
				FROM DE2HK.s_20_STG_ANALYS_3 t1
				INNER JOIN DE2HK.s_20_STG_ANALYS_3 t2
				on t1.card_num = t2.card_num
				and t1.terminal_city <> t2.terminal_city
				and t1.trans_date between t2.trans_date - interval '1' hour and t2.trans_date
				) p1
				INNER JOIN DE2HK.s_20_VIEW_INFO_CUST p2
				ON p1.card_num = p2.card_num
		''', [date])

	# Выгрузка данных в витрину
	to_rep_fraud(cursor)
	# ********************** #

	# Подготовка стейджинговой таблицы отчетов
	cursor.execute('TRUNCATE TABLE DE2HK.s_20_STG_FRAUD')

	
	# ************************************************ #
	# Таблица анализа просроченных или заблокированных #
	# паспортов, просроченных договоров                #

	cursor.execute(''' 
	CREATE TABLE DE2HK.s_20_STG_ANALYS_12 (
		trans_id varchar(20),
		trans_date timestamp,
		card_num varchar(19),
		account varchar(30),
		valid_to date,
		passport_num varchar(11),
		passport_valid_to date,
		pspt_bl varchar(11),
		fio varchar(200),
		phone varchar(20)
		)
	''')

	# выявления мошеннических пераций 1 и 2 типа 
	cursor.execute(''' 
		INSERT INTO DE2HK.s_20_STG_ANALYS_12 (
			trans_id,
			trans_date,
			card_num,
			account,
			valid_to,
			passport_num,
			passport_valid_to,
			pspt_bl,
			fio,
			phone
			)
			select
				t1.trans_id,
				t1.trans_date,
				t1.card_num,
				t2.account,
				t2.valid_to,
				t2.passport_num,
				t2.passport_valid_to,
				t3.passport_num,
				t2.fio,
				t2.phone
			from DE2HK.s_20_DWH_FACT_TRANSACTIONS t1
			inner join DE2HK.s_20_VIEW_INFO_CUST t2
			on t1.card_num = t2.card_num
			left join DE2HK.s_20_DWH_FACT_PSSPRT_BLCKLST t3
			on t2.passport_num = t3.passport_num and trunc(t1.trans_date) = t3.entry_dt
			where t1.trans_date > cast(to_date(?, 'YYYY-MM-DD') as timestamp)
		''', [date])


	cursor.execute(''' 
		INSERT INTO DE2HK.s_20_STG_FRAUD (
			event_dt,
			passport,
			fio,
			phone,
			event_type,
			report_dt
			)
			SELECT
				trans_date,
				passport_num,
				fio,
				phone,
				case
					when pspt_bl is not null 
						then 'Операция по заблокированному паспорту'
					when expired_psst = 1
						then 'Операция по просроченному паспорту'
					when invalid_acct = 1
						then 'Операция при недействующем договоре'
				end as event_type,
				to_date(?, 'YYYY-MM-DD') as report_dt
			FROM (
				SELECT
					trans_date,
					passport_num,
					fio,
					phone,
					pspt_bl,
					case
						when trunc(trans_date) > passport_valid_to
							then 1
						else 0 
					end as expired_psst,
					case
						when trunc(trans_date) > valid_to
							then 1
						else 0
					end as invalid_acct
				FROM DE2HK.s_20_STG_ANALYS_12
				)
			WHERE pspt_bl is not null or expired_psst = 1 or invalid_acct = 1
			ORDER BY trans_date
			''', [date])

	# Выгрузка данных в витрину
	to_rep_fraud(cursor)
	# ********************** #

	del_ref(cursor)




def del_ref(cursor):
	'''
	Удаление временных таблиц и представлений модуля dataProc
	'''
	try:
		cursor.execute('DROP VIEW DE2HK.s_20_VIEW_INFO_CUST')
	except jaydebeapi.DatabaseError:
		pass

	try:
		cursor.execute('DROP TABLE DE2HK.s_20_STG_FRAUD')
	except jaydebeapi.DatabaseError:
		pass

	try:
		cursor.execute('DROP TABLE DE2HK.s_20_STG_ANALYS_4')
	except jaydebeapi.DatabaseError:
		pass

	try:
		cursor.execute('DROP TABLE DE2HK.s_20_STG_ANALYS_3')
	except jaydebeapi.DatabaseError:
		pass

	try:
		cursor.execute('DROP TABLE DE2HK.s_20_STG_ANALYS_12')
	except jaydebeapi.DatabaseError:
		pass	