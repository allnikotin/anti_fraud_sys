import jaydebeapi
import pandas as pd
from datetime import datetime, timedelta
import os

import py_scripts.fileMG as fm
import py_scripts.tableMG as tbm
import py_scripts.dataProc as dtpr



# ВЫБОР РЕЖИМА РАБОТЫ:0 - РАБОТА;  1 - ОТЛАДКА
sel_mode = 1


# Подключение к базе 
conn = jaydebeapi.connect(
		# Параметры для поключения СУБД (Oracle) на внешнем сервере
	)
cursor = conn.cursor()

# Создание отчетных таблиц (ели есть)
tbm.init(cursor)

# присвоение переменным значение даты текущего анализа
date_op = (datetime.now().date() - timedelta(days=1)).strftime('%Y-%m-%d')
date_for_load = (datetime.now().date() - timedelta(days=1)).strftime('%d%m%Y')

# При режиме отладки пере-присвоение переменным значение даты из мета-таблицы,
# при переключении в рабочий режим происходит сброс плана отладки к дате 2021-03-01
if sel_mode:
	date_op, date_for_load = fm.test_dates(cursor)
else:
	cursor.execute('TRUNCATE TABLE DE2HK.s_20_META_TRAIN_LOAD')


# присвоения даты для выгрузки (файлов отладки или реальной даты) 
trans = 'transactions_' + date_for_load + '.txt'
psspt_bl = 'passport_blacklist_' + date_for_load + '.xlsx'
trms = 'terminals_' + date_for_load + '.xlsx'

# выгрузка данных в хранилище
tbm.upl_to_stor_trans(cursor, trans, date_op)
tbm.upl_to_stor_bl(cursor, psspt_bl, date_op)
tbm.upl_to_stor_trms(cursor, trms, date_op)

# формирование отчета и загрузка в витрину DE2HK.s_20_REP_FRAUD
dtpr.report_gen(cursor, date_op)

# Переименование файла и перенос его в каталог archive
fm.after_proc(trans)
fm.after_proc(psspt_bl)
fm.after_proc(trms)

# отображения таблиц (проверка)

# вартант отображения 1 (в терминале):
# dtpr.show_in_file(cursor, 'DE2HK.s_20_STG_ANALYS_12')

# вартант отображения 2 (в файле terminal.txt):
cursor.execute('SELECT * FROM DE2HK.s_20_REP_FRAUD order by event_dt')
with open('terminal.txt', 'a', encoding='utf-8') as f:
	col = [t[0] for t in cursor.description]
	print(col, '\n', file=f)
	for row in cursor.fetchall():
		print(row, file=f)
	print(file=f)

# Удаление стейджинговых таблиц
tbm.delete_stg_tbls(cursor)

cursor.close()
conn.close()