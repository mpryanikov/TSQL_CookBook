# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

import pandas as pd
import numpy as np

# #### Подключение к СУБД

import sqlalchemy
# sqlalchemy.__version__

# # !pip install pyodbc
import pyodbc
import warnings
warnings.filterwarnings('ignore')

conn = pyodbc.connect('DSN=CookBook_221230;Trusted_Connection=yes;')


def select(sql):
  return pd.read_sql(sql,conn)


# # Глава 5. Временные Данные

# <a href='https://streletzcoder.ru/kak-uznat-ili-izmenit-format-datyi-po-umolchaniyu-v-sql-server/'>
#     Как узнать или изменить формат даты по умолчанию в SQL Server - Стрелец Coder</a>

sql = """
SELECT
  date_format
FROM
  sys.dm_exec_sessions
WHERE
  session_id = @@spid
"""
select(sql)

# + active=""
#
# date_format
# 0	dmy
# -

#  данная команда действует только для текущего соединения.
cur = conn.cursor()
sql = """
SET DATEFORMAT 'dmy'
"""
cur.execute(sql)
conn.commit()
cur.close()









cur = conn.cursor()
sql = """
drop table if exists LibraryReservations;
CREATE TABLE LibraryReservations(
   BookId CHAR(10),
   UserId CHAR(10),
   ReservedFrom DATETIME,
   ReservedTo DATETIME
)

INSERT INTO LibraryReservations
   (BookId, UserId, ReservedFrom, ReservedTo)
   VALUES ('XF101','Jeff','2001-11-5','2001-11-6')
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from LibraryReservations'
select(sql)

sql = """
SELECT *
FROM LibraryReservations
WHERE BookId='XF101' and UserId='Jeff'
"""
select(sql)

# ### 5.2 Пример Расписания

# Обратите внимание, что в праздничные дни нет идентификатора работы. Это нарушает правило проектирования базы данных, предоставляя каждой записи уникальный идентификатор, известный как первичный ключ, но это дает нам хорошее игровое поле, на котором можно продемонстрировать некоторые трюки, чтобы использовать, когда у вас нет уникального идентификатора для ваших данных. Обычно у вас будет jobid в качестве уникального идентификатора и просто назначьте уникальные значения идентификатора задания для праздников

cur = conn.cursor()
sql = """
drop table if exists ContractorsSchedules;
CREATE TABLE ContractorsSchedules(
   JobID CHAR(10),
   ContractorID CHAR(10),
   JobStart DATETIME,
   JobEnd DATETIME,
   JobType CHAR(1) CHECK(JobType in ('B','H')),
   PRIMARY KEY(ContractorId, JobStart)
)

INSERT INTO ContractorsSchedules
   (JobId, ContractorId, JobStart, JobEnd, JobType)
   VALUES (Null,'Alex',CAST('2001-01-01' AS DATE),CAST('2001-01-10' AS DATE), 'H')
INSERT INTO ContractorsSchedules
   (JobId, ContractorId, JobStart, JobEnd, JobType)
   VALUES ('RF10001','Alex',CAST('2001-01-11' AS DATE),CAST('2001-01-20' AS DATE), 'B')   
INSERT INTO ContractorsSchedules
   (JobId, ContractorId, JobStart, JobEnd, JobType)
   VALUES ('RF10002','Alex',CAST('2001-01-21' AS DATE),CAST('2001-01-30' AS DATE), 'B')
INSERT INTO ContractorsSchedules
   (JobId, ContractorId, JobStart, JobEnd, JobType)
   VALUES ('RF10020','Alex',CAST('2001-02-01' AS DATE),CAST('2001-02-05' AS DATE), 'B')
INSERT INTO ContractorsSchedules
   (JobId, ContractorId, JobStart, JobEnd, JobType)
   VALUES ('RF10034','Alex',CAST('2001-02-11' AS DATE),CAST('2001-02-20' AS DATE), 'B')   
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from ContractorsSchedules'
select(sql)

# ### 5.3 Применение Правил Детализации

# + active=""
# cur = conn.cursor()
# sql = """
# drop TRIGGER if exists ContractorSchedulesUpdate;
# """
# cur.execute(sql)
# conn.commit()
# cur.close()
# -

cur = conn.cursor()
sql = """
CREATE TRIGGER ContractorSchedulesUpdate
ON ContractorsSchedules
FOR UPDATE, INSERT
AS 
    UPDATE ContractorsSchedules 
    SET JobStart=CONVERT(CHAR(10),i.JobStart,104), 
      JobEnd=CONVERT(CHAR(10),i.JobEnd,104)
    FROM ContractorsSchedules c, inserted i 
    WHERE c.JobId=i.JobId  
"""
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = """
INSERT INTO ContractorsSchedules
   (JobId, ContractorId, JobStart, JobEnd, JobType)
   VALUES ('','Cindy',CAST(N'2001-01-01T05:12:00' AS DATETIME),CAST(N'2001-01-10T19:15:00' AS DATETIME), 'H')
"""
cur.execute(sql)
conn.commit()
cur.close()

sql = """
SELECT ContractorId, JobStart, JobEnd 
FROM ContractorsSchedules WHERE ContractorId='Cindy'
"""
select(sql)

# Триггер приносит дополнительные накладные расходы на операции INSERT и UPDATE; однако он обеспечивает безопасность, зная наверняка, что вся временная информация хранится с требуемой степенью детализации. Вы управляете детализацией, регулируя константу в предложении CONVERT. Например, если вы хотите установить степень детализации в часы, вы бы расширили константу еще на три символа:

cur = conn.cursor()
sql = """
CREATE TRIGGER ContractorSchedulesUpdate
ON ContractorsSchedules
FOR UPDATE, INSERT
AS 
    UPDATE ContractorsSchedules 
    SET JobStart=CONVERT(CHAR(13),i.JobStart,121)+':00', 
      JobEnd=CONVERT(CHAR(13),i.JobEnd,121)+':00'
    FROM ContractorsSchedules c, inserted i 
    WHERE c.JobId=i.JobId
"""
cur.execute(sql)
conn.commit()
cur.close()

# Установка области действия типа CHAR эффективно отсекает нежелательные символы из формата ISO YYYY-MM-DD HH:MI:SS.MMM, так что мы остались с YYYY-MM-DD HH. Однако после этого мы нарушаем требуемый формат ISO, поэтому мы добавляем строку ':00', чтобы соответствовать требуемой форме, которая требует по крайней мере минут вместе с часом. Таким образом, вы можете легко ограничить степень детализации в таблице в любой степени.

# ### 5.4 Хранение временных значений вне диапазона

# Используйте формат ISO 8601, предпочтительно без тире между элементами даты, и сохраните данные в виде строки:

cur = conn.cursor()
sql = """
drop table if exists Archive;
CREATE TABLE Archive(
   EventId CHAR(40),
   EventDate CHAR(8)
)

INSERT INTO Archive 
  VALUES ('Columbus departs from Palos, Spain', '14920802')
INSERT INTO Archive 
  VALUES ('Columbus arrives at Cuba', '14921029')
INSERT INTO Archive 
  VALUES ('Columbus returns to Spain', '14930315') 
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from Archive'
select(sql)

# ### 5.5 Вывод первой и последней дат месяца

# Учитывая произвольную дату, найдите первый и последний день месяца, в который эта дата выпадает.

sql = """
SELECT 
   CONVERT(CHAR(8),CURRENT_TIMESTAMP,120)+'01' First_date,
   CAST(SPACE(
      DATEPART(weekday,
         CONVERT(CHAR(8),CURRENT_TIMESTAMP,120)+'01'
      )-1)+'*' as CHAR(8)) "SMTWTFS",
   CONVERT(CHAR(10),
      DATEADD(day,-1,
         DATEADD(month,1,CONVERT(CHAR(8),CURRENT_TIMESTAMP,120)+'01')
      ),120) Last_date,
   CAST(SPACE(
      DATEPART(weekday,
         DATEADD(day,-1,DATEADD(m,1,
            CONVERT(CHAR(8),CURRENT_TIMESTAMP,120)+'01')
         )
      )-1)+'*' AS CHAR(8)) "SMTWTFS "
"""
select(sql)

# #### Discussion

# Функция CONVERT может возвращать результаты в нескольких различных стилях. Стиль 120, который мы используем здесь, соответствует ISO и принимает форму YYYY-MM-DD HH:MI:SS. Мы сохраняем только первые восемь символов,что приводит к значению в формате YYYY-MM. Затем мы добавляем строку '01', и результатом является дата первого дня месяца.

# Чтобы узнать день недели, на который приходится данный день, мы используем функцию DATEPART с параметром weekday. Эта функция возвращает 1 для воскресенья, 2 Для понедельника и так далее. Чтобы напечатать результат в графическом формате, мы использовали числовое значение дня недели, а также функцию SPACE для установки звездочки ( * ) в нужном месте:

# + active=""
# SPACE(
#       DATEPART(weekday,
#          CONVERT(CHAR(8),CURRENT_TIMESTAMP,120)+'01'
#       )-1)+'*'
# -

# Вычислить последний день месяца сложнее. В запросе используется следующая логика:

# + active=""
# DATEADD(day,-1,DATEADD(month,1,
#    CONVERT(CHAR(8),CURRENT_TIMESTAMP,120)+'01')
# -

# Мы берем текущую дату (полученную CURRENT_TIMESTAMP), устанавливаем ее в первый день месяца, добавляем один месяц к результату, чтобы получить первый день следующего месяца, и, наконец, вычитаем один день, чтобы получить последний день текущего месяца.

# В этом вопросе есть еще один маленький трюк, на который мы должны указать. Обратите внимание, что у нас есть два столбца вывода с надписью "SMTWTFS". Как вы знаете, SQL Server не позволяет использовать две метки с одинаковым именем. Трюк, который вы можете использовать в таких случаях, - это добавить дополнительное пространство ко второй метке. Для сервера" SMTWTFS "и" SMTWTFS " не являются одинаковыми метками, но для пользователя они выглядят одинаково.

# ### 5.6 Печать Календарей

sql = """
SELECT  
   STR(YEAR(CAST('2023-1-1' AS DATETIME)+i-6))+ SPACE(1)+
      SUBSTRING('JANFEBMARAPRMAYJUNJULAUGSEPOCTNOVDEC',
      MONTH(CAST('2023-1-1' AS DATETIME)+i)*3-2,3) Month,
   DAY(CAST('2023-1-1' AS DATETIME)+i-6) AS S,
   DAY(CAST('2023-1-1' AS DATETIME)+i-5) AS M,
   DAY(CAST('2023-1-1' AS DATETIME)+i-4) AS T,
   DAY(CAST('2023-1-1' AS DATETIME)+i-3) AS W,
   DAY(CAST('2023-1-1' AS DATETIME)+i-2) AS T,
   DAY(CAST('2023-1-1' AS DATETIME)+i-1) AS F,
   DAY(CAST('2023-1-1' AS DATETIME)+i) AS S 
FROM [Pivot]
WHERE  DATEPART(dw,CAST('2023-1-1' AS DATETIME)+i)%7=0
ORDER BY i
"""
select(sql)

# Вместо извлечения аббревиатуры месяца из Строковой константы можно также использовать функцию DATENAME. Однако, есть некоторые проблемы с его использованием. Самая важная проблема заключается в том, что DATENAME возвращает имена месяцев в соответствии с текущими настройками языка операционной системы. Языковые настройки иногда могут быть неправильными, поэтому мы предпочитаем использовать строковую константу, содержащую сокращения месяца. Тем не менее, если вы предпочитаете использовать DATENAME, ниже приведена версия имени даты решения этого рецепта:

sql = """
SELECT  
   STR(YEAR(CAST('2023-1-1' AS DATETIME)+i-6))+ SPACE(1)+
   DATENAME(month, CAST('2023-1-1' AS DATETIME)+i) Month,
   DAY(CAST('2023-1-1' AS DATETIME)+i-6) AS S,
   DAY(CAST('2023-1-1' AS DATETIME)+i-5) AS M,
   DAY(CAST('2023-1-1' AS DATETIME)+i-4) AS T,
   DAY(CAST('2023-1-1' AS DATETIME)+i-3) AS W,
   DAY(CAST('2023-1-1' AS DATETIME)+i-2) AS T,
   DAY(CAST('2023-1-1' AS DATETIME)+i-1) AS F,
   DAY(CAST('2023-1-1' AS DATETIME)+i) AS S 
FROM [Pivot]
WHERE  DATEPART(dw,CAST('2023-1-1' AS DATETIME)+i)%7=0
ORDER BY i
"""
select(sql)

# ### 5.7 Расчет Длительности



# ---

conn.close()


