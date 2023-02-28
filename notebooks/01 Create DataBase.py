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

# ### Create DataBase

# + active=""
# drop DataBase CookBook

# + active=""
# USE master
#
# Create DataBase CookBook_221230
#   on Primary
#   (
#   Name = KTDGL,
#   FileName = 'A:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\CookBook_221230.mdf',
#   Size = 10MB, 
#   MaxSize = Unlimited, 
#   FileGrowth = 10MB
#   )
# LOG ON 
#   (
#   Name = KTDGL_LOG,
#   FileName = 'A:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\CookBook_221230.ldf',
#   Size = 20MB, 
#   MaxSize = 1GB, 
#   FileGrowth = 10%
#   )
# -

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


# ## 1.1 Using a Pivot Table

# Поддержка последовательности элементов часто необходима для решения различных задач SQL. Например, учитывая диапазон дат, вы можете создать одну строку для каждой даты в диапазоне. Или можно преобразовать ряд значений, возвращаемых в отдельных строках, в ряд значений в отдельных столбцах одной строки. Для реализации такой функциональности можно использовать *постоянную таблицу, в которой хранится серия последовательных номеров*. Такая Таблица называется **сводной таблицей**.
#
# Многие из рецептов в нашей книге используют сводную таблицу, и во всех случаях имя таблицы-Pivot. Этот рецепт показывает, как создать эту таблицу.

cur = conn.cursor()
sql = """
drop table if exists [Pivot];
CREATE TABLE [Pivot] (
   i INT,
   PRIMARY KEY(i)
)

drop table if exists Foo;
CREATE TABLE [Foo](
   i CHAR(1)
)

INSERT INTO Foo VALUES('0')
INSERT INTO Foo VALUES('1')
INSERT INTO Foo VALUES('2')
INSERT INTO Foo VALUES('3')
INSERT INTO Foo VALUES('4')
INSERT INTO Foo VALUES('5')
INSERT INTO Foo VALUES('6')
INSERT INTO Foo VALUES('7')
INSERT INTO Foo VALUES('8')
INSERT INTO Foo VALUES('9')

INSERT INTO [Pivot]
   SELECT f1.i+f2.i+f3.i
   FROM Foo f1, Foo F2, Foo f3
"""
cur.execute(sql)
conn.commit()
cur.close()

sql = 'select * from [Pivot]'
select(sql)

# ### 1.1.3 Discussion

# + active=""
# Как вы увидите в рецептах, приведенных в этой книге, Сводная Таблица часто используется для добавления свойства последовательности в запрос. Некоторая форма сводной таблицы встречается во многих системах на основе SQL, хотя она часто скрыта от пользователя и используется в основном в предопределенных запросах и процедурах.
#
# Вы видели, как число соединений таблиц (таблицы Foo) управляет количеством строк, которые создает наша инструкция INSERT для сводной таблицы. Значения от 0 до 999 создаются путем объединения строк. Цифровые значения в Foo являются символьными строками. Таким образом, когда оператор plus (+) используется для их объединения, мы получаем такие результаты, как:
# '0' + '0' + '0' = '000'
# '0' + '0' + '1' = '001'
# ...
#
# Эти результаты вставляются в столбец INTEGER в целевой сводной таблице. При использовании инструкции INSERT для вставки строк в столбец INTEGER база данных неявно преобразует эти строки в целые числа. Декартово произведение экземпляров Foo гарантирует, что будут сгенерированы все возможные комбинации и, следовательно, будут сгенерированы все возможные значения от 0 до 999.
#
# Стоит отметить, что в данном примере используются строки от 0 до 999 и никаких отрицательных чисел. При необходимости можно легко создать отрицательные числа, повторив инструкцию INSERT со знаком " - " перед объединенной строкой и соблюдая осторожность в отношении строки 0. Нет такого понятия, как -0, поэтому Вы не хотите вставлять строку " 000 " при генерации отрицательных чисел разворота. Если бы вы сделали это, вы бы в конечном итоге с двумя 0 строк в сводной таблице. В нашем случае две строки 0 невозможны, так как мы определяем первичный ключ для нашей сводной таблицы.

# + active=""
# Сводная Таблица, вероятно, является самой полезной таблицей в мире SQL. После того, как вы привыкнете к нему, почти невозможно создать серьезное приложение SQL без него. В качестве демонстрации, давайте использовать сводную таблицу для создания таблицы ASCII быстро с кодами от 32 до 126:
# -

sql = """
SELECT i Ascii_Code, CHAR(i) Ascii_Char 
FROM [Pivot]
WHERE i BETWEEN 32 AND 126
"""
select(sql)

# + active=""
# Что хорошего в использовании сводной таблицы в этом конкретном экземпляре, так это то, что вы сгенерировали строки вывода, не имея равного количества строк ввода. Без сводной таблицы это сложная, если не невозможная задача. Просто указав диапазон, а затем выбрав сводные строки на основе этого диапазона, мы смогли создать данные, которые не существуют ни в одной таблице базы данных.

# + active=""
# В качестве еще одного примера полезности сводной таблицы мы можем легко использовать ее для создания календаря на следующие семь дней:
# -

sql = """
SELECT 
    CONVERT(CHAR(10), DATEADD(d, i, CURRENT_TIMESTAMP), 121) date,
    DATENAME(dw, DATEADD(d, i, CURRENT_TIMESTAMP)) day FROM [Pivot]
 WHERE i BETWEEN 0 AND 6
"""
select(sql)

# ---

conn.close()


