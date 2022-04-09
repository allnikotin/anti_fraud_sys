import jaydebeapi
from datetime import datetime, timedelta
import os
import re


def after_proc(file_name):
	'''
	Функция переименовует обработанный файл (добавляет расширение .backup)
	и пеермещает в каталог archive.
	Что бы при следующем запуске не искался
	'''
	new_name = file_name + '.backup'
	to_dir = 'archive/' + new_name
	try:
		os.rename(file_name, new_name)
		os.replace(new_name, to_dir)
	except FileNotFoundError:
		pass

def test_dates(cursor):
	'''
	Подстановка дат для отладочной загрузки согласно плана:
	2021-03-01 -> 2021-03-02 -> 2021-03-03
	'''
	
	cursor.execute('SELECT count(*) FROM DE2HK.s_20_META_TRAIN_LOAD')

	if cursor.fetchone()[0] == 0:
		cursor.execute(''' 
			INSERT INTO DE2HK.s_20_META_TRAIN_LOAD(id, date_op)
				VALUES (1, to_date('2021-03-01', 'YYYY-MM-DD'))
			''')
		cursor.execute(''' 
			INSERT INTO DE2HK.s_20_META_TRAIN_LOAD(id, date_op)
				VALUES (2, to_date('2021-03-02', 'YYYY-MM-DD'))
			''')
		cursor.execute(''' 
			INSERT INTO DE2HK.s_20_META_TRAIN_LOAD(id, date_op)
				VALUES (3, to_date('2021-03-03', 'YYYY-MM-DD'))
			''')
	else:
		cursor.execute(''' 
			CREATE TABLE DE2HK.s_20_STG_META_TRAIN_LOAD as
				SELECT
					id,
					lead(date_op, 1, (
							select 
								date_op 
							from DE2HK.s_20_META_TRAIN_LOAD
							where id = 1
							)
						) 
					over(order by id) as date_op
				from DE2HK.s_20_META_TRAIN_LOAD
			''')
		
		cursor.execute('TRUNCATE TABLE DE2HK.s_20_META_TRAIN_LOAD')

		cursor.execute(''' 
			INSERT INTO DE2HK.s_20_META_TRAIN_LOAD (
				id,
				date_op
				)
				select id, date_op from DE2HK.s_20_STG_META_TRAIN_LOAD
			''')
		
		cursor.execute('DROP TABLE DE2HK.s_20_STG_META_TRAIN_LOAD')

	cursor.execute('SELECT date_op FROM DE2HK.s_20_META_TRAIN_LOAD WHERE id = 1')

	dfor_op = re.sub('\s.{8}$', '', cursor.fetchone()[0])
	dfor_load = datetime.strptime(dfor_op, '%Y-%m-%d').strftime('%d%m%Y')

	return dfor_op, dfor_load


if __name__ == '__main__':
	file_name = 'pasport_blaclist_DDMMYYYY.xlsx'
	print(date_of_analys(file_name))

	after_proc('../transactions_02032021.txt')

	
