import jaydebeapi
from datetime import datetime, timedelta
import pandas as pd

import py_scripts.dataProc as dtpr

def init(cursor):
	'''
	Создание пользовательских (s_20) таблиц.
	Если такая таблица существует, то ничего не делать 
	'''
	
	# Удаление стейджинговых таблиц - если они есть
	delete_stg_tbls(cursor)
	dtpr.del_ref(cursor)

	# ********************* #
	# Таблица для черного списка паспортов
	# Основная таблица Фактов
	try:
		cursor.execute(''' 
			CREATE TABLE DE2HK.s_20_DWH_FACT_PSSPRT_BLCKLST(
				passport_num varchar(11),
				entry_dt date default current_date
			)
			''')
	except jaydebeapi.DatabaseError:
		pass
	# ******************* #

	# ******************* #
	# Таблица транзакций - таблица фактов
	try:
		cursor.execute(''' 
			CREATE TABLE DE2HK.s_20_DWH_FACT_TRANSACTIONS(
				trans_id varchar(20),
				trans_date timestamp,
				card_num varchar(19),
				oper_type varchar(20),
				amt	decimal(*,2),
				oper_result integer,
				terminal varchar(10)
			)
			''')
	except jaydebeapi.DatabaseError:
		pass
	# ******************* #

	# ******************* #
	# Таблица для списка терминалов
	# Основная (историческая)
	try:
		cursor.execute(''' 
			CREATE TABLE DE2HK.s_20_DWH_DIM_TERMINALS_HIST(
				terminal_id varchar(10),
				terminal_type varchar(5),
				terminal_city varchar(50),
				terminal_address varchar(128),
				deleted_flg integer default 0,
				effective_from date,
				effective_to date default (date '2999-12-31')
			)
			''')
	except jaydebeapi.DatabaseError:
		pass

	# ******************* #
	# Витрина отчетности
	try:
		cursor.execute(''' 
			CREATE TABLE DE2HK.s_20_REP_FRAUD(
				event_dt timestamp,
				passport varchar(11),
				fio varchar(200),
				phone varchar(20),
				event_type varchar(200),
				report_dt date
				)
			''')
	except jaydebeapi.DatabaseError:
		pass
	# ******************* #

	# ************************** #
	# Таблица дат - план отладки #
	try:
		cursor.execute(''' 
			CREATE TABLE DE2HK.s_20_META_TRAIN_LOAD (
				id integer,
				date_op date
			)
			''')
	except jaydebeapi.DatabaseError:
			pass
	# *************************** #

def delete_tbls(cursor):
	'''
	Удаление пользовательских таблиц.
	'''
	try:
		cursor.execute('DROP TABLE DE2HK.s_20_DWH_FACT_PSSPRT_BLCKLST')
	except jaydebeapi.DatabaseError:
		pass

	try:	
		cursor.execute('DROP TABLE DE2HK.s_20_DWH_FACT_TRANSACTIONS')
	except jaydebeapi.DatabaseError:
		pass

	try:	
		cursor.execute('DROP TABLE DE2HK.s_20_DWH_DIM_TERMINALS_HIST')
	except jaydebeapi.DatabaseError:
		pass

	try:	
		cursor.execute('DROP TABLE DE2HK.s_20_REP_FRAUD')
	except jaydebeapi.DatabaseError:
		pass

	try:	
		cursor.execute('DROP TABLE DE2HK.s_20_META_TRAIN_LOAD')
	except jaydebeapi.DatabaseError:
		pass	

	
def delete_stg_tbls(cursor):
	'''
	Удаление стейджинговых таблиц
	'''
	try:
		cursor.execute('DROP TABLE DE2HK.s_20_STG_PSSPRT_BLCKLST')
	except jaydebeapi.DatabaseError:
		pass

	try:
		cursor.execute('DROP TABLE DE2HK.s_20_STG_TERMINALS')
	except jaydebeapi.DatabaseError:
		pass

	try:
		cursor.execute('DROP TABLE DE2HK.s_20_STG_NEW_TERMINALS')
	except jaydebeapi.DatabaseError:
		pass

	try:
		cursor.execute('DROP TABLE DE2HK.s_20_STG_DEL_TERMINALS')
	except jaydebeapi.DatabaseError:
		pass

	try:
		cursor.execute('DROP TABLE DE2HK.s_20_STG_CH_TERMINALS')
	except jaydebeapi.DatabaseError:
		pass

	try:
		cursor.execute('DROP TABLE DE2HK.s_20_STG_TRANSACTIONS')
	except jaydebeapi.DatabaseError:
		pass

	try:	
		cursor.execute('DROP TABLE DE2HK.s_20_STG_ACT_TERMINALS')
	except jaydebeapi.DatabaseError:
		pass	

	
def upl_to_stor_trans(cursor, trans, date):
	'''
	Выгрузка данных в хранилище:
	trans - Список транзакций за текущий день;
	Именованный агрумент date - предыдущая дата,
	При проверочной загрузке - задается в формате 'YYYY-MM-DD'.
	
	'''
	# ********* TRANSACTIONS ********** #
	# Выгрузка списка транзакции - список фактов
	try:
		df_tr = pd.read_csv(trans, sep=';', decimal=',')
		data_tr = df_tr.values.tolist()
	except FileNotFoundError:
		print('\n', '!!!!! Файл', trans, 'отсутствует !!!!!', '\n')
		return

	# Создание стейдженговой таблицы транзакций
	# (для предупреждения повторной загрузки такой же таблицы)
	cursor.execute(''' 
		CREATE TABLE DE2HK.s_20_STG_TRANSACTIONS(
			trans_id varchar(20),
			trans_date timestamp,
			card_num varchar(19),
			oper_type varchar(20),
			amt	decimal(*,2),
			oper_result varchar(20),
			terminal varchar(10)
			)
		''')

	# Выгрузка данных транзакций в стейдженговую таблицу
	cursor.executemany('''
		INSERT INTO DE2HK.s_20_STG_TRANSACTIONS (
			trans_id,
			trans_date,
			amt,
			card_num,
			oper_type,
			oper_result,
			terminal
		) values (
			trim(?),
			to_timestamp(trim(?), 'YYYY-MM-DD HH24:MI:SS'),
			trim(?),
			trim(?),
			trim(?),
			trim(?),
			trim(?))
		''', data_tr)

	# добаление данных (отличных от загруженных ранее) в траблицу транзакций
	cursor.execute('''
		INSERT INTO DE2HK.s_20_DWH_FACT_TRANSACTIONS (
			trans_id,
			trans_date,
			amt,
			card_num,
			oper_type,
			oper_result,
			terminal
		) 	select
				t1.trans_id,
				t1.trans_date,
				t1.amt,
				t1.card_num,
				t1.oper_type,
				case
					when t1.oper_result = 'SUCCESS' then 1
					else 0
				end as oper_result,
				t1.terminal
			from DE2HK.s_20_STG_TRANSACTIONS t1
			left join DE2HK.s_20_DWH_FACT_TRANSACTIONS t2
			on t1.trans_id = t2.trans_id
			where t2.trans_id is null
		''')
	# ******************* #

def upl_to_stor_bl(cursor, psspt_bl, date):
	'''
	Выгрузка данных в хранилище:
	psspt_bl - Список паспортов, включенных в «черный список»;
	Именованный агрумент date - предыдущая дата,
	При проверочной загрузке - задается в формате 'YYYY-MM-DD'.
	
	'''
	# ********** PASSPORT BLACKLIST ************** #
	# Выгрузка  "черного" списка паспортов, с начала в стеджинговую таблицу
	try:
		df_psp = pd.read_excel(psspt_bl, dtype={'date': str, 'passport': str})
		data_psp = df_psp.values.tolist()	
	except FileNotFoundError:
		print('\n', '!!!!! Файл',psspt_bl, 'отсутствует !!!!!', '\n')
		return
	
	
	
	# Сздание стейдженговой таблица "черного" списка паспортов
	cursor.execute(''' 
		CREATE TABLE DE2HK.s_20_STG_PSSPRT_BLCKLST(
			passport_num varchar(11),
			entry_dt date
			)
		''')

	# Выгрузка данных в стейдженговую таблицу "черного" списка паспортов
	cursor.executemany('''
		INSERT INTO DE2HK.s_20_STG_PSSPRT_BLCKLST (
			entry_dt,
			passport_num
		) values (
			to_date(regexp_replace(?, '\s.{8}$'), 'YYYY-MM-DD'),
			?)
		''', data_psp)

	# затем, добаление новых записей в таблицу фактов
	cursor.execute(''' 
		INSERT INTO DE2HK.s_20_DWH_FACT_PSSPRT_BLCKLST (
			passport_num,
			entry_dt
		)
			select
				t1.passport_num,
				t1.entry_dt
			from DE2HK.s_20_STG_PSSPRT_BLCKLST t1
			left join DE2HK.s_20_DWH_FACT_PSSPRT_BLCKLST t2
			on t1.passport_num = t2.passport_num
			where t2.passport_num is null
		''')
	# ******************* #

def upl_to_stor_trms(cursor, trms, date):
	'''
	Выгрузка данных в хранилище:
	trms - Список терминалов.
	Именованный агрумент date - предыдущая дата,
	При проверочной загрузке - задается в формате 'YYYY-MM-DD'.
	
	'''

	# *********** TERMINALS ************ #
	# Выгрузка списка терминалов (через инкрементную загрузку)
	try:
		df_tm = pd.read_excel(trms)
		data_tm = df_tm.values.tolist()
	except FileNotFoundError:
		print('\n', '!!!!! Файл', trms, 'отсутствует !!!!!', '\n')
		return

	

	# стейджинговая таблица с последними (актуальными) терминалами
	cursor.execute(''' 
			CREATE TABLE DE2HK.s_20_STG_ACT_TERMINALS(
				terminal_id varchar(10),
				terminal_type varchar(5),
				terminal_city varchar(50),
				terminal_address varchar(128)
			)
			''')
	
	cursor.execute(''' 
	INSERT INTO DE2HK.s_20_STG_ACT_TERMINALS (
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address) 
		select
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
		from DE2HK.s_20_DWH_DIM_TERMINALS_HIST
		where deleted_flg = 0
		and to_date(?, 'YYYY-MM-DD') between effective_from and effective_to
	''', [date])

	# Стейджинговая таблица терминалов
	cursor.execute(''' 
		CREATE TABLE DE2HK.s_20_STG_TERMINALS(
			terminal_id varchar(10),
			terminal_type varchar(5),
			terminal_city varchar(50),
			terminal_address varchar(128)
		)
		''')	
	# загрузка в стейджинговую таблицу терминалов (первичная загрузка)
	cursor.executemany(''' 
		INSERT INTO DE2HK.s_20_STG_TERMINALS(
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
		) values (
			trim(?),
			trim(?),
			trim(?),
			trim(?))
		''', data_tm)
	
	# новые терминалы
	cursor.execute(''' 
		CREATE TABLE DE2HK.s_20_STG_NEW_TERMINALS as
			select
				t1.terminal_id,
				t1.terminal_type,
				t1.terminal_city,
				t1.terminal_address
			from DE2HK.s_20_STG_TERMINALS t1
			left join DE2HK.s_20_STG_ACT_TERMINALS t2
			on t1.terminal_id = t2.terminal_id
			where t2.terminal_id is null
		''')

	# удаленные терминалы
	cursor.execute(''' 
		CREATE TABLE DE2HK.s_20_STG_DEL_TERMINALS as
			select
				t1.terminal_id,
				t1.terminal_type,
				t1.terminal_city,
				t1.terminal_address
			from DE2HK.s_20_STG_ACT_TERMINALS t1
			left join DE2HK.s_20_STG_TERMINALS t2
			on t1.terminal_id = t2.terminal_id
			where t2.terminal_id is null
		''')

	# терминалы с изменениями (по какому-нибудь полю)
	cursor.execute(''' 
		CREATE TABLE DE2HK.s_20_STG_CH_TERMINALS as
			select
				t1.terminal_id,
				t1.terminal_type,
				t1.terminal_city,
				t1.terminal_address
			from DE2HK.s_20_STG_TERMINALS t1
			inner join DE2HK.s_20_STG_ACT_TERMINALS t2
			on t1.terminal_id = t2.terminal_id
			and (t1.terminal_type <> t2.terminal_type
			or t1.terminal_city <> t2.terminal_city
			or t1.terminal_address <> t2.terminal_address
			)
		''')

	# Обновление исторической таблицы терминалов
	# для удаленных 
	cursor.execute(''' 
		UPDATE DE2HK.s_20_DWH_DIM_TERMINALS_HIST
		SET effective_to = to_date(?, 'YYYY-MM-DD') - 1
		where terminal_id in (select terminal_id from DE2HK.s_20_STG_DEL_TERMINALS)
		''', [date])
	# для измененных
	cursor.execute(''' 
		UPDATE DE2HK.s_20_DWH_DIM_TERMINALS_HIST
		SET effective_to = to_date(?, 'YYYY-MM-DD') - 1
		where terminal_id in (select terminal_id from DE2HK.s_20_STG_CH_TERMINALS)
		''', [date])
	# добавление новых терминалов
	cursor.execute(''' 
		INSERT INTO DE2HK.s_20_DWH_DIM_TERMINALS_HIST (
					terminal_id,
					terminal_type,
					terminal_city,
					terminal_address,
					effective_from
					)
			select
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address,
				to_date(?, 'YYYY-MM-DD')
			from DE2HK.s_20_STG_NEW_TERMINALS
		''', [date])
	# добавление измененных терминалов
	cursor.execute(''' 
		INSERT INTO DE2HK.s_20_DWH_DIM_TERMINALS_HIST (
					terminal_id,
					terminal_type,
					terminal_city,
					terminal_address,
					effective_from
					)
			select
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address,
				to_date(?, 'YYYY-MM-DD')
			from DE2HK.s_20_STG_CH_TERMINALS
		''', [date])
	# добавление удаленных терминалов
	cursor.execute(''' 
		INSERT INTO DE2HK.s_20_DWH_DIM_TERMINALS_HIST (
					terminal_id,
					terminal_type,
					terminal_city,
					terminal_address,
					deleted_flg,
					effective_from
					)
			select
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address,
				1,
				to_date(?, 'YYYY-MM-DD')
			from DE2HK.s_20_STG_DEL_TERMINALS
		''', [date])

	# обновление данных в актульной таблице
	cursor.execute('TRUNCATE TABLE DE2HK.s_20_STG_ACT_TERMINALS')
	cursor.execute(''' 
		INSERT INTO DE2HK.s_20_STG_ACT_TERMINALS (
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address) 
			select
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address
			from DE2HK.s_20_DWH_DIM_TERMINALS_HIST
			where deleted_flg = 0
			and to_date(?, 'YYYY-MM-DD') between effective_from and effective_to
		''', [date])

	# ******************* #

	
