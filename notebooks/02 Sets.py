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


# # Глава 2. Множества

cur = conn.cursor()
sql = """
drop table if exists OrderItems;
CREATE TABLE OrderItems(
   OrderId INTEGER,
   ItemId INTEGER,
   ProductId CHAR(10),
   Qty INTEGER,
   PRIMARY KEY(OrderId,ItemId)
)
"""
cur.execute(sql)
conn.commit()
cur.close()

sql = 'select * from OrderItems'
select(sql)

# ## 2.2 The Students Example

cur = conn.cursor()
sql = """
drop table if exists Students;
CREATE TABLE Students (
   CourseId CHAR(20),
   StudentName CHAR(40),
   Score DECIMAL(4,2),
   TermPaper INTEGER
)

Insert into Students values 
    ('ACCN101','Andrew',15.60,4),
    ('ACCN101','Andrew',10.40,2), 
    ('ACCN101','Andrew',11.00,3),
    ('ACCN101','Bert',13.40,1),
    ('ACCN101','Bert',11.20,2),          
    ('ACCN101','Bert',13.00,3),                     
    ('ACCN101','Cindy',12.10,1),
    ('ACCN101','Cindy',16.20,2),
    ('MGMT120','Andrew',20.20,1),
    ('MGMT120','Andrew',21.70,2),
    ('MGMT120','Andrew',23.10,3),
    ('MGMT120','Cindy',12.10,1),
    ('MGMT120','Cindy',14.40,2),
    ('MGMT120','Cindy',16.00,3)
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from Students'
select(sql)

# ## 2.3 Реализация Set Difference

# ### 2.3.2.1 Вычитание одного набора из другого

sql = 'select * from Students'
select(sql)

# Рассмотрим проблему выяснения, какие курсовые работы **Эндрю завершил, что Синди не завершила**. Есть два набора: набор документов, которые Эндрю завершил и набор документов, которые Синди завершила. Следующий запрос возвращает разницу между этими двумя наборами:

sql = """
SELECT s.CourseId, s.TermPaper
FROM Students s
WHERE s.StudentName='Andrew' AND 
   NOT EXISTS(
      SELECT * FROM Students s1 
      WHERE s1.CourseId=s.CourseId AND 
         s1.TermPaper=s.TermPaper AND 
         s1.StudentName='Cindy'
         )
"""
select(sql)

# ### Обсуждение

# #### сначала рассматривается вн. запись

sql = """
SELECT s.CourseId, s.TermPaper
FROM Students s
WHERE s.StudentName='Cindy'
"""
select(sql)

# #### потом внешняя и результат из внешней

sql = """
SELECT s.CourseId, s.TermPaper
FROM Students s
WHERE s.StudentName='Andrew'
"""
select(sql)

# + active=""
# CourseId             TermPaper   
# -------------------- ----------- 
# ACCN101              4
# ACCN101              2  --есть в верхней записи
# ACCN101              3
# MGMT120              1  --есть в верхней записи
# MGMT120              2  --есть в верхней записи
# MGMT120              3  --есть в верхней записи
# -

# ### 2.3.2.2 Вычитание одного набора из всех остальных

sql = 'select * from Students'
select(sql)

# Небольшое изменение в этой проблеме заключается в **вычитании одного конкретного множества из объединения всех других множеств**. Это приводит ко второй части нашей проблемы-к поиску курсовых работ, **которые взяли друзья Синди, но не Синди**. Следующий запрос сделает это:

sql = """
SELECT s.StudentName, s.CourseId, s.TermPaper
FROM Students s
WHERE s.StudentName <> 'Cindy' AND
   NOT EXISTS(
      SELECT * FROM Students s1 
      WHERE s.CourseId=s1.CourseId AND 
         s.TermPaper=s1.TermPaper AND 
         s1.StudentName='Cindy'
         )
"""
select(sql)

# ### Обсуждение

# #### сначала рассматривается вн. запись

sql = """
SELECT s.StudentName, s.CourseId, s.TermPaper
FROM Students s
WHERE s.StudentName = 'Cindy'
"""
select(sql)

# #### потом внешняя и результат из внешней

sql = """
SELECT s.StudentName, s.CourseId, s.TermPaper
FROM Students s
WHERE s.StudentName <> 'Cindy'
"""
select(sql)

# + active=""
# StudentName                              CourseId             TermPaper   
# ---------------------------------------- -------------------- ----------- 
# Andrew                                   ACCN101              4
# Andrew                                   ACCN101              2  --есть в верхней записи
# Andrew                                   ACCN101              3
# Bert                                     ACCN101              1  --есть в верхней записи
# Bert                                     ACCN101              2  --есть в верхней записи
# Bert                                     ACCN101              3
# Andrew                                   MGMT120              1  --есть в верхней записи
# Andrew                                   MGMT120              2  --есть в верхней записи
# Andrew                                   MGMT120              3  --есть в верхней записи
# -

# ### 2.3.3.2 вычитание одного набора из всех остальных

sql = 'select * from Students'
select(sql)

sql = """
SELECT s.CourseId, s.TermPaper
FROM Students s
WHERE s.StudentName='Andrew' AND 
   NOT EXISTS(
      SELECT * FROM Students s1 
      WHERE s1.CourseId=s.CourseId AND 
         s1.TermPaper=s.TermPaper AND 
         s1.StudentName<>'Andrew')
"""
select(sql)

# #### сначала рассматривается вн. запись

sql = """
SELECT s.CourseId, s.TermPaper
FROM Students s
WHERE s.StudentName <> 'Andrew'
"""
select(sql)

# #### потом внешняя и результат из внешней

sql = """
SELECT s.CourseId, s.TermPaper
FROM Students s
WHERE s.StudentName='Andrew'
"""
select(sql)

# + active=""
# CourseId             TermPaper   
# -------------------- ----------- 
# ACCN101              4
# ACCN101              2  --есть в верхней записи
# ACCN101              3  --есть в верхней записи
# MGMT120              1  --есть в верхней записи
# MGMT120              2  --есть в верхней записи
# MGMT120              3  --есть в верхней записи
# -

# ## 2.4 сравнение двух множеств на равенство

sql = 'select * from Students'
select(sql)

# Вы хотите **сравнить два набора строк на равенство**. Например, вы сделали снимок таблицы "учащиеся" в октябре и другой снимок в ноябре. Теперь, вы хотите сравнить эти две копии.

cur = conn.cursor()
sql = """
drop table if exists StudentsOct;
CREATE TABLE StudentsOct(
   CourseId CHAR(20),
   StudentName CHAR(40),
   Score DECIMAL(4,2),
   TermPaper INTEGER
)

Insert into StudentsOct values 
    ('ACCN101','Andrew',11.00,3)
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from StudentsOct'
select(sql)

cur = conn.cursor()
sql = """
drop table if exists StudentsNov;
CREATE TABLE StudentsNov(
   CourseId CHAR(20),
   StudentName CHAR(40),
   Score DECIMAL(4,2),
   TermPaper INTEGER
)

Insert into StudentsNov values 
    ('ACCN101','Andrew',11.00,3),
    ('ACCN101','Andrew',11.00,3),
    ('ACCN101','Bert',13.40,1)
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from StudentsNov'
select(sql)

# ### Решение

sql = """
SELECT so.*, COUNT(*) DupeCount, 'StudentsOct' TableName
FROM StudentsOct so
GROUP BY so.CourseId, so.StudentName, so.Score, so.TermPaper
HAVING NOT EXISTS (
      SELECT sn.*, COUNT(*)
      FROM StudentsNov sn
      GROUP BY sn.CourseId, sn.StudentName, sn.Score, sn.TermPaper
      HAVING sn.CourseId=so.CourseId AND 
         sn.TermPaper=so.TermPaper AND 
         sn.StudentName=so.StudentName AND
         COUNT(*) = COUNT(ALL so.CourseId)
         )
UNION
SELECT sn.*, COUNT(*) DupeCount, 'StudentsNov' TableName
FROM StudentsNov sn
GROUP BY sn.CourseId, sn.StudentName, sn.Score, sn.TermPaper
HAVING NOT EXISTS (
      SELECT so.*, COUNT(*)
      FROM StudentsOct so
      GROUP BY so.CourseId, so.StudentName, so.Score, so.TermPaper
      HAVING so.CourseId=sn.CourseId AND 
         so.TermPaper=sn.TermPaper AND 
         so.StudentName=sn.StudentName AND
         COUNT(*) = COUNT(ALL sn.CourseId)
         )
"""
select(sql)

# ### обсуждение

# Давайте сосредоточимся на первой части инструкции SELECT:

sql = """
SELECT so.*, COUNT(*) dupeCount, 'StudentsOct' tableName
FROM StudentsOct so
GROUP BY so.CourseId, so.StudentName, so.Score, so.TermPaper
"""
select(sql)

# #### Различия в таблицах при использовании первичных Ключей

sql = """
SELECT so.*, 'StudentsOct' TableName
FROM StudentsOct so
WHERE NOT EXISTS (
      SELECT sn.*
      FROM StudentsNov sn
      WHERE sn.CourseId=so.CourseId AND 
         sn.TermPaper=so.TermPaper AND 
         sn.StudentName=so.StudentName
         )
UNION
SELECT sn.*, 'StudentsNov' TableName
FROM StudentsNov sn
WHERE NOT EXISTS (
      SELECT so.*
      FROM StudentsOct so
      WHERE so.CourseId=sn.CourseId AND 
         so.TermPaper=sn.TermPaper AND 
         so.StudentName=sn.StudentName
         )
"""
select(sql)



sql = """
SELECT so.*, COUNT(*) DupeCount, 'StudentsOct' TableName
FROM StudentsOct so
GROUP BY so.CourseId, so.StudentName, so.Score, so.TermPaper
HAVING NOT EXISTS (
      SELECT sn.*, COUNT(*)
      FROM StudentsNov sn
      GROUP BY sn.CourseId, sn.StudentName, sn.Score, sn.TermPaper
      HAVING sn.CourseId=so.CourseId AND 
         sn.TermPaper=so.TermPaper AND 
         sn.StudentName=so.StudentName AND
         COUNT(*) = COUNT(ALL so.CourseId)
         )
UNION
SELECT sn.*, COUNT(*) DupeCount, 'StudentsNov' TableName
FROM StudentsNov sn
GROUP BY sn.CourseId, sn.StudentName, sn.Score, sn.TermPaper
HAVING NOT EXISTS (
      SELECT so.*, COUNT(*)
      FROM StudentsOct so
      GROUP BY so.CourseId, so.StudentName, so.Score, so.TermPaper
      HAVING so.CourseId=sn.CourseId AND 
         so.TermPaper=sn.TermPaper AND 
         so.StudentName=sn.StudentName AND
         COUNT(*) = COUNT(ALL sn.CourseId)
         )
"""
select(sql)

# **разобрать надо бы вышестоящее**

# ## 2.5 Реализация Частичного Пересечения

sql = 'select * from Students'
select(sql)

# У вас есть набор. Требуется найти элементы, представляющие пересечения между этими наборами, а затем подсчитать число наборов, к которым принадлежит каждый из этих элементов. Пересечения могут быть частичными. Другими словами, не обязательно, чтобы элемент присутствовал во всех множествах. Однако необходимо задать пороговое значение в терминах числа наборов, чтобы Результаты запроса исключали элемент, который опускается ниже него. В качестве примера такого типа задачи вы хотите **перечислить все курсовые работы и показать, сколько студентов представили каждую из них**. 

sql = """
SELECT CourseId, TermPaper, COUNT(*) NumStudents
FROM Students
GROUP BY TermPaper, CourseId
ORDER BY COUNT(*) DESC
"""
select(sql)

# Этот запрос возвращает количество отправок для каждой курсовой работы. Если вы хотите наложить порог-скажите, что вас волнуют только курсовые работы, **которые были сданы по крайней мере двумя студентами** — вы можете добавить предложение HAVING следующим образом:

sql = """
SELECT CourseId, TermPaper, COUNT(*) NumStudents
FROM Students
GROUP BY TermPaper, CourseId
HAVING COUNT(*) >= 2
ORDER BY COUNT(*) DESC
"""
select(sql)

# ### обсуждение

# Строго говоря, **если количество меньше 2, то оно вообще не представляет пересечение**.
# Либо курсовую работу еще никто не написал, либо ее написал только один человек.
# Если вам нужно математически **правильное пересечение, укажите минимальное количество 2 
# в предложении HAVING**. Таким образом, вы увидите только курсовые работы, 
# которые попадают по крайней мере в два набора. Например, следующий запрос возвращает
#  пересечение между набором курсовых работ, написанных Эндрю, и набором, написанным Синди:

sql = """
SELECT CourseId, TermPaper
FROM Students
WHERE StudentName IN ('Andrew','Cindy')
GROUP BY TermPaper, CourseId
HAVING COUNT(*) >= 2
ORDER BY  COUNT(*) DESC
"""
select(sql)

# Все запросы, показанные в этом рецепте, основаны на предположении, что студент не может 
# представить одну и ту же курсовую работу дважды или, по крайней мере, что такая двойная
# подача не будет записана дважды в таблице Students.

# ## 2.6 Реализация Полного Пересечения

sql = 'select * from Students'
select(sql)

# У вас есть набор множеств, и вы хотите найти полное пересечение между ними. Продолжая пример со студентами, вы хотите перечислить **курсовые работы, которые были сданы всеми студентами**.

sql = """
SELECT CourseId, TermPaper 
FROM Students
GROUP BY TermPaper, CourseId
HAVING COUNT(*)=(
        SELECT COUNT(DISTINCT StudentName) FROM Students
    )
"""
select(sql)

# ### обсуждение

# Идея заключается в том, что для каждой курсовой работы мы используем COUNT(*) в предложении HAVING для подсчета количества представлений. Затем мы сравниваем это число с общим количеством студентов в таблице

sql = """
SELECT CourseId, TermPaper, COUNT(*) Cnt
FROM Students
GROUP BY TermPaper, CourseId
"""
select(sql)

# #### количество студентов (неповторяющихся записей по полю StudentName):

sql = """
SELECT COUNT(DISTINCT StudentName) 
FROM Students
"""
select(sql)

# ####  тот предмет где все сдали и показать бал:

sql = """
SELECT CourseId, TermPaper 
FROM Students
GROUP BY TermPaper, CourseId
HAVING COUNT(*)=(
    SELECT COUNT(DISTINCT StudentName) FROM Students
    )
"""
select(sql)

# ## 2.7 Классификации Подмножеств

sql = 'select * from Students'
select(sql)

# Вы хотите классифицировать агрегированные результаты из подмножеств в классы общих свойств. Например, вы хотите дать каждому студенту оценку за каждый курс, который они проходят. Оценки основаны на среднем Балле, рассчитанном на основе курсовых работ по каждому курсу. 

sql = """
SELECT CourseId, StudentName, 
AVG(Score) Score,
(
CASE 
    WHEN AVG(Score)>=22 THEN 'A' 
    WHEN AVG(Score)>=19 THEN 'B'
    WHEN AVG(Score)>=16 THEN 'C'
    WHEN AVG(Score)>=13 THEN 'D'
    WHEN AVG(Score)>=10 THEN 'E'
    ELSE 'F' 
END
) Grade
FROM Students s
GROUP BY CourseId, StudentName
"""
select(sql)

# ### обсуждение

# Альтернативным решением было бы объединение результирующего набора из нескольких запросов с помощью оператора UNION. Например, следующий запрос union эквивалентен показанному ранее запросу non union:

sql = """
SELECT CourseId, StudentName, 
    AVG(Score) Score, 'A' grade
FROM Students s
GROUP BY CourseId, StudentName
HAVING AVG(Score)<=25 and AVG(Score)>=22
UNION
SELECT CourseId, StudentName, 
    AVG(Score) Score, 'B' grade
FROM Students s
GROUP BY CourseId, StudentName
HAVING AVG(Score)<22 and AVG(Score)>=19
UNION
SELECT CourseId, StudentName, 
    AVG(Score) Score, 'C' grade
FROM Students s
GROUP BY CourseId, StudentName
HAVING AVG(Score)<19 and AVG(Score)>=16
UNION
SELECT CourseId, StudentName, 
    AVG(Score) Score, 'D' grade
FROM Students s
GROUP BY CourseId, StudentName
HAVING AVG(Score)<16 and AVG(Score)>=13
UNION
SELECT CourseId, StudentName, 
    AVG(Score) Score, 'E' grade
FROM Students s
GROUP BY CourseId, StudentName
HAVING AVG(Score)<13 and AVG(Score)>=10
UNION
SELECT CourseId, StudentName, 
    AVG(Score) Score, 'F' grade
FROM Students s
GROUP BY CourseId, StudentName
HAVING AVG(Score)<10
"""
select(sql)

# Запрос UNION представляет выполнение нескольких инструкций SELECT, каждая из которых требует полного прохождения через таблицу Students. Следовательно, он будет **гораздо менее эффективен, чем альтернативный запрос, использующий оператор CASE и требующий только одного прохода через таблицу**

# ## 2.8 обобщение классов-комплектов

sql = 'select * from Students'
select(sql)

# Вы хотите рассчитать, сколько раз подмножества попадают в разные классы, и вы хотите измерить размеры этих классов, когда классификация выполняется на неагрегированных данных. В качестве примера такого типа задачи предположим, что вы хотите подсчитать количество работ A, B и так далее для каждого студента. 

# Альтернативный способ постановки задачи состоит в том, чтобы сказать, что вам нужно подсчитать, **сколько раз каждому ученику дается каждая оценка**. Это делает следующий запрос:

sql = """
SELECT s.StudentName,
    (
    CASE 
        WHEN s.Score>=22 THEN 'A' 
        WHEN s.Score>=19 THEN 'B'
        WHEN s.Score>=16 THEN 'C'
        WHEN s.Score>=13 THEN 'D'
        WHEN s.Score>=10 THEN 'E'
        ELSE 'F' 
    END
    ) Grade,
    COUNT(*) NoOfPapers
FROM Students s
GROUP BY s.StudentName,
    CASE 
        WHEN s.Score>=22 THEN 'A' 
        WHEN s.Score>=19 THEN 'B'
        WHEN s.Score>=16 THEN 'C'
        WHEN s.Score>=13 THEN 'D'
        WHEN s.Score>=10 THEN 'E'
        ELSE 'F' 
    END
ORDER BY s.StudentName
"""
select(sql)

# ### обсуждение

sql = """
SELECT s.StudentName,(
    CASE 
        WHEN s.Score>=22 THEN 'A' 
        WHEN s.Score>=19 THEN 'B'
        WHEN s.Score>=16 THEN 'C'
        WHEN s.Score>=13 THEN 'D'
        WHEN s.Score>=10 THEN 'E'
        ELSE 'F' 
    END
    ) Grade
FROM Students s
ORDER BY s.StudentName
"""
select(sql)

# + active=""
# sql = """
# SELECT s.StudentName,
# (
# CASE 
# WHEN s.Score>=22 THEN 'A' 
# WHEN s.Score>=19 THEN 'B'
# WHEN s.Score>=16 THEN 'C'
# WHEN s.Score>=13 THEN 'D'
# WHEN s.Score>=10 THEN 'E'
# ELSE 'F' 
# END
# ) Grade,
# COUNT(*) NoOfPapers,
# 100*count(*)/(
# SELECT count(*) 
# FROM Students s1
# WHERE s1.StudentName=s.StudentName
# """
# select(sql)
# -

# ## 2.9 Вложенные агрегирующие запросы

sql = 'select * from Students'
select(sql)

# Вы хотите выбрать некоторые данные, агрегировать их, а затем снова агрегировать. Вот пример такого рода проблемы: администрация университета готовит внутренний отчет для декана, который хочет сравнить оценочные привычки профессоров. Одной из мер, на которую хочет обратить внимание декан, является разброс между средним баллом за курсовую работу для каждого студента в данном курсе. Спред-это разница между лучшим и худшим результатами ученика за курс. Ваша задача состоит в том, чтобы **найти лучший и худший Средний балл в каждом курсе и вычислить разницу**. 

sql = """
SELECT CourseId, 
    MAX(l.s) Best ,
    MIN(l.s) Worst, 
    MAX(l.s)-MIN(l.s) Spread 
FROM (
   SELECT CourseId, AVG(Score) AS s 
   FROM Students 
   GROUP BY CourseId, StudentName
   ) AS l
GROUP BY CourseId
"""
select(sql)

# ### обсуждение

# + active=""
# SQL не позволяет напрямую заключать одну статистическую функцию в другую. Другими словами, запрос, написанный следующим образом, не будет выполняться:
#
# SELECT CourseId, MAX(AVG(stock)), MIN(AVG(stock))
# FROM Students
# GROUP BY CourseId, studentsName
# -

# ## 2.10 Суммирование Агрегированных Классов

sql = 'select * from Students'
select(sql)

# Необходимо рассчитать, сколько раз подмножества попадают в разные классы, и измерить размеры этих классов при выполнении классификации по уже агрегированным данным. Например, предположим, вы хотите подсчитать **количество оценок курса на одного учащегося**. Оценка курса рассчитывается путем усреднения оценки всех работ по данному курсу, а затем классификации этого среднего значения в соответствии с таблицей 2-1. Это похоже на предыдущий рецепт под названием "суммирование классов множеств", но **на этот раз мы должны агрегировать данные дважды**.

sql = """
SELECT s.StudentName,(
    CASE 
        WHEN s.Score>=22 THEN 'A' 
        WHEN s.Score>=19 THEN 'B'
        WHEN s.Score>=16 THEN 'C'
        WHEN s.Score>=13 THEN 'D'
        WHEN s.Score>=10 THEN 'E'
        ELSE 'F' 
    END
    ) Grade,
    COUNT(*) NoOfCourses
FROM (
    SELECT CourseId, StudentName, 
        AVG(Score) AS Score 
    FROM Students 
    GROUP BY CourseId, StudentName
   ) AS s
GROUP BY s.StudentName,
    CASE 
        WHEN s.Score>=22 THEN 'A' 
        WHEN s.Score>=19 THEN 'B'
        WHEN s.Score>=16 THEN 'C'
        WHEN s.Score>=13 THEN 'D'
        WHEN s.Score>=10 THEN 'E'
        ELSE 'F' 
    END
ORDER BY s.StudentName
"""
select(sql)

# ### обсуждение

# На первый взгляд, этот запрос выглядит немного сложным и пугающим. Чтобы понять это, лучше всего рассматривать запрос как **двухэтапный процесс**. Встроенный выбор в предложении FROM вычисляет Средний балл для каждой комбинации курса и учащегося. Этот Средний балл вычисляется из индивидуальных баллов всех курсовых работ. 

sql = """
SELECT CourseId, StudentName, 
    AVG(Score) AS Score 
FROM Students 
GROUP BY CourseId, StudentName
"""
select(sql)

# Результаты встроенного выбора передаются во внешний запрос, который преобразует средние баллы в буквенные оценки, а затем подсчитывает количество раз, когда происходит каждая оценка. Оператор CASE в списке выбора выполняет классификацию. Оператор case в предложении GROUP BY агрегирует результаты по классам, позволяя вычислить счетчик.

# ## 2.11 сводные данные,  включая неагрегированные столбцы

sql = 'select * from Students'
select(sql)

# Необходимо написать запрос GROUP BY, возвращающий сводные данные, а также включить в результат неагрегированные столбцы. Эти неагрегированные столбцы не отображаются в предложении GROUP BY. С уважением к примеру, студентов, скажем, что каждый курс оценивается по лучшей работе, что каждый студент представил на курс. По административным причинам необходимо выяснить, **какая курсовая работа имеет лучший результат для каждой комбинации студент / курс**. 

sql = """
SELECT StudentName,CourseId,
    (
    SELECT MAX(TermPaper) 
    FROM Students 
    WHERE Score = MAX(s.Score) and 
    StudentName = s.StudentName and 
    CourseId = s.CourseId
    ) TermPaper, 
    MAX(s.Score) Score
FROM Students s
GROUP BY CourseId, StudentName
"""
select(sql)

# ### обсуждение

# Этот тип запроса является недопустимым. Проблема в том, что он может быть выполнен только в том случае, если столбец TermPaper добавляется в предложение GROUP BY. Мы потенциально сталкиваемся с той же проблемой при написании нашего запроса, но мы избежали этого, написав встроенную инструкцию SELECT, чтобы получить номер курсовой работы, соответствующий высокой оценке

# + active=""
# sql = """
# SELECT StudentName, CourseId, 
#     TermPaper, 
#     MAX(Score)
# FROM Students s
# GROUP BY CourseId, StudentName
# """
# select(sql)
# -

# Внешний запрос группирует таблицу в наборы на основе имен учащихся и идентификации курса. Затем он находит лучший результат для каждого студента в курсе. Это легко понять. Внутренний запрос - это то, где все становится интересным. Он извлекает номер курсовой работы, в которой балл соответствует высокому баллу за курс соответствующего студента. Запрос является коррелированным вложенным запросом, то есть выполняется один раз для каждого сочетания курса и учащегося. Студент может иметь две курсовые работы на курсе с одинаковым баллом. Чтобы убедиться, что возвращается только одно значение, внутренний запрос использует функцию MAX в столбце TermPaper. Каждый раз, когда две курсовые работы имеют наивысший балл, курсовая работа, перечисленная в результатах, будет иметь наибольшее число. Это довольно произвольный выбор, но это лучшее, что вы можете сделать в данных обстоятельствах.

sql = 'select * from Students'
select(sql)

sql = """
SELECT StudentName,CourseId,
    MAX(s.Score)
FROM Students s
GROUP BY CourseId, StudentName
"""
select(sql)

sql = """
SELECT MAX(TermPaper) 
FROM Students 
WHERE Score = 15.6 and 
StudentName = 'Andrew' and 
CourseId = 'ACCN101'
"""
select(sql)

# ## 2.12 Поиск верхних N значений в наборе

sql = 'select * from Students'
select(sql)

# Наиболее простым решением этой проблемы является использование ключевого слова TOP. TOP-это расширение MS SQL Server для SQL, которое позволяет ограничить запрос так, чтобы он возвращал только первые N записей. Следующий запрос возвращает два лучших результата для каждого учащегося в каждом курсе

sql = """
SELECT  s1.StudentName, s1.CourseId, s1.TermPaper, 
    MAX(s1.Score) Score
FROM Students s1
GROUP BY s1.CourseId, s1.StudentName, s1.TermPaper
HAVING MAX(s1.Score) IN 
   (
   SELECT TOP 2 s2.Score 
       FROM Students s2
       WHERE s1.CourseId=s2.CourseId AND
          s1.StudentName=s2.StudentName
    ORDER BY s2.Score DESC
    )
ORDER BY s1.StudentName, s1.CourseId, MAX(s1.Score) DESC
"""
select(sql)

# Альтернативное решение немного менее специфично для Transact-SQL и немного менее интуитивно. Однако он является более общим и соответствует стандарту SQL

sql = """
SELECT  s1.StudentName,s1.CourseId, s1.TermPaper, 
    MAX(s1.Score) Score
FROM Students s1 
INNER JOIN Students s2
   ON s1.CourseId=s2.CourseId AND
      s1.StudentName=s2.StudentName
GROUP BY s1.CourseId, s1.StudentName, s1.TermPaper
HAVING SUM(
    CASE 
        WHEN s1.Score <= s2.Score THEN 1 
    END
) <= 2
ORDER BY s1.StudentName, s1.CourseId, MAX(s1.Score) DESC
"""
select(sql)

# ### обсуждение

# #### использование TOP

sql = 'select * from Students'
select(sql)

# + active=""
# SELECT  s1.StudentName, s1.CourseId, s1.TermPaper, 
#     MAX(s1.Score) Score
# FROM Students s1
# GROUP BY s1.CourseId, s1.StudentName, s1.TermPaper
# HAVING MAX(s1.Score) IN 
#    (
#    SELECT TOP 2 s2.Score 
#        FROM Students s2
#        WHERE s1.CourseId=s2.CourseId AND
#           s1.StudentName=s2.StudentName
#     ORDER BY s2.Score DESC
#     )
# ORDER BY s1.StudentName, s1.CourseId, MAX(s1.Score) DESC
# -

sql = """
SELECT  s1.StudentName, s1.CourseId, s1.TermPaper, 
    MAX(s1.Score) Score
FROM Students s1
GROUP BY s1.CourseId, s1.StudentName, s1.TermPaper
"""
select(sql)

sql = """
SELECT TOP 2 s2.Score 
   FROM Students s2
   WHERE 'ACCN101'=s2.CourseId AND
      'Andrew'=s2.StudentName
ORDER BY s2.Score DESC
"""
select(sql)

# #### использование самосоединения

sql = 'select * from Students'
select(sql)

# + active=""
# SELECT  s1.StudentName,s1.CourseId, s1.TermPaper, 
#     MAX(s1.Score) Score
# FROM Students s1 
# INNER JOIN Students s2
#    ON s1.CourseId=s2.CourseId AND
#       s1.StudentName=s2.StudentName
# GROUP BY s1.CourseId, s1.StudentName, s1.TermPaper
# HAVING SUM(
#     CASE 
#         WHEN s1.Score <= s2.Score THEN 1 
#     END
# ) <= 2
# ORDER BY s1.StudentName, s1.CourseId, MAX(s1.Score) DESC
# -

sql = """
SELECT  s1.StudentName,s1.CourseId, s1.TermPaper, s1.Score
FROM Students s1 
INNER JOIN Students s2
   ON s1.CourseId=s2.CourseId AND
      s1.StudentName=s2.StudentName
ORDER BY s1.StudentName,s1.CourseId, s1.TermPaper, s1.Score      
"""
select(sql)

sql = """
SELECT  s1.StudentName,s1.CourseId, s1.TermPaper, 
    MAX(s1.Score) Score
FROM Students s1 
INNER JOIN Students s2
   ON s1.CourseId=s2.CourseId AND
      s1.StudentName=s2.StudentName
GROUP BY s1.CourseId, s1.StudentName, s1.TermPaper
ORDER BY s1.StudentName, s1.CourseId, MAX(s1.Score) DESC
"""
select(sql)

sql = """
SELECT  s1.StudentName,s1.CourseId, s1.TermPaper, 
    MAX(s1.Score) Score,
    SUM(
        CASE 
            WHEN s1.Score <= s2.Score THEN 1 
            ELSE 0
        END
    ) rang
FROM Students s1 
INNER JOIN Students s2
   ON s1.CourseId=s2.CourseId AND
      s1.StudentName=s2.StudentName
GROUP BY s1.CourseId, s1.StudentName, s1.TermPaper
ORDER BY s1.StudentName, s1.CourseId, MAX(s1.Score) DESC
"""
select(sql)

sql = """
SELECT  s1.StudentName,s1.CourseId, s1.TermPaper, 
    MAX(s1.Score) Score
FROM Students s1 
INNER JOIN Students s2
   ON s1.CourseId=s2.CourseId AND
      s1.StudentName=s2.StudentName
GROUP BY s1.CourseId, s1.StudentName, s1.TermPaper
HAVING SUM(
    CASE 
        WHEN s1.Score <= s2.Score THEN 1 
    END
) <= 2
ORDER BY s1.StudentName, s1.CourseId, MAX(s1.Score) DESC
"""
select(sql)

# ## 2.13 Отчёт размера дополнения набора

sql = 'select * from Students'
select(sql)

# Вы хотите сообщить количество отсутствующих значений для набора. В качестве примера предположим, что **каждый студент должен представить четыре курсовые работы для каждого курса**. Не все студенты представили все необходимые документы, и вы хотите создать **отчет, показывающий номер, который каждый студент еще не представил для каждого курса**.

sql = """
SELECT  s.StudentName, s.CourseId, 
    4-COUNT(TermPaper) Missing
FROM Students s
GROUP BY s.StudentName, s.CourseId
ORDER BY s.StudentName
"""
select(sql)

# ### обсуждение

sql = """
SELECT  s.StudentName, s.CourseId, 
    COUNT(TermPaper) Missing
FROM Students s
GROUP BY s.StudentName, s.CourseId
ORDER BY s.StudentName
"""
select(sql)

# ## 2.14 Нахождение дополнения множества

sql = 'select * from Students'
select(sql)

# Требуется найти дополнение к набору. Учитывая пример студентов, используемый в этой главе, вы хотите **перечислить недостающие курсовые работы для каждого студента**. 

# #### Шаг 1: Создание сводной таблицы

# Поскольку мы имеем дело с номерами курсовых работ, нам нужна Сводная таблица с одним числовым столбцом. Для этого мы будем использовать стандартную сводную таблицу, как описано в разделе 1.1 рецепт в главе 1. 

# Здесь стоит отметить, что эта Сводная таблица по-прежнему полезна, даже если количество курсовых работ, необходимых для каждого курса, отличается. Главное, чтобы количество строк в сводной таблице соответствовало наибольшему числу курсовых работ, требуемых для любого курса.

# #### Шаг 2: выполнить запрос

sql = """
SELECT  s.StudentName, s.CourseId, f.i TermPaper
FROM Students s, [Pivot] f
WHERE f.i BETWEEN 1 AND 4 
GROUP BY s.StudentName, s.CourseId, f.i
HAVING NOT EXISTS(
   SELECT * FROM Students 
   WHERE CourseId=s.CourseId AND 
      StudentName=s.StudentName AND 
      TermPaper=f.i)
ORDER BY s.StudentName
"""
select(sql)

# ### обсуждение

# Запросы в этом рецепте используют сводную таблицу в дополнение к таблицам, содержащим фактические данные. Нам нужна Сводная Таблица, потому что нам нужно знать, какие номера курсовых работ возможны, и нам нужно генерировать строки для курсовых работ, которые не существуют. Если курсовая работа не сдана, в таблице студентов не будет соответствующей строки. Сводная Таблица позволяет создать строку для отсутствующей бумаги, которая будет включена в результат запроса. 

# + active=""
# SELECT  s.StudentName, s.CourseId, f.i TermPaper
# FROM Students s, [Pivot] f
# WHERE f.i BETWEEN 1 AND 4 
# GROUP BY s.StudentName, s.CourseId, f.i
# HAVING NOT EXISTS(
#    SELECT * FROM Students 
#    WHERE CourseId=s.CourseId AND 
#       StudentName=s.StudentName AND 
#       TermPaper=f.i)
# ORDER BY s.StudentName
# -

# Запросы присоединяют таблицу Students к сводной таблице и группируют результат по столбцам идентификатор курса, имя учащегося и номер поворота. Предложение WHERE в основном запросе ограничивает объединение количеством сводных записей, соответствующим количеству необходимых курсовых работ для каждого курса. 

# Затем оператор HAVING проверяет, какие значения pivot не существуют в списке курсовых работ для каждой конкретной группы. Если курсовая работа не существует, выражение в предложении HAVING возвращает значение TRUE и номер разворота бумаги сообщается для определения отсутствующего курсовую работу. 

sql = """
SELECT  s.StudentName, s.CourseId, f.i TermPaper
FROM Students s, [Pivot] f
WHERE f.i BETWEEN 1 AND 4 
GROUP BY s.StudentName, s.CourseId, f.i
ORDER BY s.StudentName
"""
select(sql)

sql = """
SELECT  s.StudentName, s.CourseId, f.i TermPaper
FROM Students s, [Pivot] f
WHERE f.i BETWEEN 1 AND 4 
GROUP BY s.StudentName, s.CourseId, f.i
HAVING NOT EXISTS(
   SELECT * FROM Students 
   WHERE CourseId=s.CourseId AND 
      StudentName=s.StudentName AND 
      TermPaper=f.i)
ORDER BY s.StudentName
"""
select(sql)

# ## 2.15 Поиск дополнения к отсутствующему набору

sql = 'select * from Students'
select(sql)

# Запрос в предыдущем рецепте имеет один существенный недостаток: он не сообщает о пропущенных курсовых работах для студентов, которые еще не завершили хотя бы одну курсовую работу.

# Ключом к сообщению о пропаже курсовых работ для студентов, которые еще не сдали ни одной курсовой работы, является поиск надежного способа идентификации этих студентов. Есть две возможности:  
# - Определите пустую строку в таблице Students для идентификации каждого студента.  
# - Создайте главную таблицу, содержащую по одной строке для каждого учащегося. 
#
# С точки зрения проектирования базы данных второе решение, вероятно, является лучшим, потому что в конечном итоге вы получаете типичное отношение "многие к одному" между двумя таблицами. Первое решение является чем-то вроде kludge, потому что оно использует таблицу Students как основную таблицу, так и таблицу detail.

# ####  Решение 1. Определите пустые строки в таблице Students

# + active=""
# INSERT INTO Students(CourseId, StudentName, Score, TermPaper)
#  VALUES('ACCN101','David',0,0)
# -

# Обратите внимание, что оценка и номер курсовой работы были установлены в 0. При наличии этих записей запрос, представленный в предыдущем рецепте, может использоваться для отображения списка отсутствующих курсовых работ. Этот список теперь будет включать случаи, когда студент пропустил все курсовые работы в данном курсе. Нулевые записи, которые мы вставили, не будут отображаться в конечном результате, потому что наш запрос исключает строку сводной таблицы для нуля.

# #### 2 Решение 2: Создайте главную таблицу учащихся

cur = conn.cursor()
sql = """
drop table if exists StudentMaster;
CREATE TABLE StudentMaster(
   CourseId CHAR(20),
   StudentName CHAR(40)
)

INSERT INTO StudentMaster VALUES('ACCN101','Andrew')
INSERT INTO StudentMaster VALUES('MGMT120','Andrew')
INSERT INTO StudentMaster VALUES('ACCN101','Bert')
INSERT INTO StudentMaster VALUES('ACCN101','Cindy')
INSERT INTO StudentMaster VALUES('MGMT120','Cindy')
INSERT INTO StudentMaster VALUES('ACCN101','David')
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from StudentMaster'
select(sql)

# В отчете перечислены отсутствующие курсовые работы для каждого студента:

sql = """
SELECT  s.StudentName, s.CourseId, f.i TermPaper
FROM StudentMaster s, [Pivot] f
WHERE f.i BETWEEN 1 AND 4
GROUP BY s.StudentName, s.CourseId, f.i
HAVING NOT EXISTS(
   SELECT * FROM Students 
   WHERE CourseId=s.CourseId AND 
      StudentName=s.StudentName AND 
      TermPaper=f.i)
ORDER BY s.StudentName
"""
select(sql)

# ## 2.16 Поиск дополнений множеств с различными Universes

sql = 'select * from Students'
select(sql)

sql = """
select distinct CourseId from Students
"""
select(sql)

# Вы хотите написать запрос, который возвращает дополнение из нескольких наборов, но каждый из этих наборов имеет другой universe. Например, учтите, что **для разных курсов требуется разное количество курсовых работ**. Вы хотите **перечислить документы, которые отсутствуют для каждого студента в каждом курсе**. В отличие от запросов в предыдущих рецептах, этот запрос должен корректно обрабатывать различные требования к курсовой работе (the universe) каждого курса.

cur = conn.cursor()
sql = """
drop table if exists CourseMaster;
CREATE TABLE CourseMaster(
   CourseId CHAR(20),
   numTermPapers INTEGER
)

/*
После создания таблицы CourseMaster курса необходимо заполнить ее данными. 
Следующие две вставки указывают, что курс 
    ACCNT101 требует четырех курсовых работ
    MGMT120 требует трех курсовых работ:
*/

INSERT INTO CourseMaster VALUES('ACCN101',4)
INSERT INTO CourseMaster VALUES('MGMT120',3)
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from CourseMaster'
select(sql)

sql = """
SELECT  s.StudentName, s.CourseId, f.i TermPaper
FROM Students s, [Pivot] f, CourseMaster c
WHERE f.i BETWEEN 1 AND c.numTermPapers 
   AND c.CourseId=s.CourseId 
GROUP BY s.StudentName, s.CourseId, f.i
HAVING NOT EXISTS(
   SELECT * FROM Students 
   WHERE CourseId=s.CourseId AND 
      StudentName=s.StudentName AND 
      TermPaper=f.i)
ORDER BY s.StudentName
"""
select(sql)

# ## 2.17 Содержит ли набор все элементы Universe

sql = 'select * from Students'
select(sql)

# Вы хотите узнать, содержит ли набор все элементы своей universe. Затем вы хотите сообщить об этом как о полном или частичном совпадении. Например, предположим, что требуется создать список, показывающий, **какие студенты выполнили все требования в отношении курсовых работ, которые они должны сдать, и какие студенты выполнили только частичное число требований к курсовой работе**.

# Используйте таблицу CourseMaster, определенную в предыдущем рецепте, чтобы определить universe необходимых работ для каждого студента и курса, а затем сравнить ее с фактическими представленными работами. Вот один из подходов к этому:

sql = """
SELECT s.StudentName, s.CourseId,
    CASE 
    WHEN COUNT(*)=MAX(c.numTermPapers) THEN 'All submitted'
    ELSE CONVERT(VARCHAR(8),MAX(c.numTermPapers)-COUNT(*))+' missing'
    END status
FROM Students s 
JOIN CourseMaster c ON s.CourseId=c.CourseId
GROUP BY s.StudentName, s.CourseId
ORDER BY s.StudentName, s.CourseId
"""
select(sql)

# ### Обсуждение

# Запрос соединяет таблицу Students и таблицу CourseMaster, чтобы найти необходимое количество курсовых работ для каждого курса. Соединение выполняется с помощью столбца CourseId для получения соответствующего значения numTermPapers. Затем результаты группируются так, чтобы для каждого студента и комбинации курсов была одна строка.

# Ядром запроса является оператор CASE. В наборе строк для каждого курса и студента каждая строка представляет собой курсовую работу. Чтобы найти количество курсовых работ, которые сдал студент, нам просто нужно подсчитать строки. Затем оператор CASE сравнивает это количество с требуемым количеством работ для курса. Результатом утверждения case будет сообщение, указывающее, были ли выполнены все требования.

# Функция MAX в этом запросе используется только в синтаксических целях. Количество курсовых зависит от значения CourseId, поэтому для данной группы будет только одно число. MAX необходим только потому, что numTermPapers не является группой по столбцам. Вы можете добавить курсовые работы numTermPapers в список столбцов в предложении GROUP BY, что позволит вам обойтись без MAX, но дополнительная группа по столбцу повредит эффективности запроса.

# ## 2.18 Система Динамической Классификации

sql = 'select * from Students'
select(sql)

# Определите структуру обработки запросов, которая будет классифицировать наборы в соответствии с правилами, которые можно определить и изменить динамически. Количество правил не ограничено. Например, в нашем университете введена специальная кредитная система в рамках новой междисциплинарной программы, введенной новым деканом. Студенты не получают кредитные баллы непосредственно за курсы, которые они закончили, а вместо этого получают кредит за различные комбинации курсовых работ. Данная комбинация курсовых работ не обязательно должна представлять один курс и, скорее всего, будет представлять несколько курсов. Существуют различные категории кредитов, и категории отмечены кодами типов, такими как A, B и C. Чтобы закончить программу, студент должен заработать по крайней мере один кредитный балл в каждой категории. В таблице 2-2 показана матрица требований, которая определяет, может ли кредит предоставляться в данной категории.

# ![image.png](attachment:image.png) 

# + active=""
# Table 2-2. Category credit requirement matrix  
# Course  Paper 1 Paper 2 Paper 3
# ACCN101  A,C2    A,C2     A,B
# MGMT120   C1      C1      C1,B
# -

# Чтобы получить кредитный балл, студент должен выполнить первую, вторую и третью курсовую работу в курсе ACC 101. Есть два способа заработать с кредитной точки C. Один из них-представить первый, второй и третий курсовые работы для MGMT 120. Другой способ заработать кредитный балл C-представить первый и второй курсовые работы для ACCN101.

# Эту проблему нельзя решить напрямую, просто применив один запрос. Сначала необходимо создать дополнительную таблицу, в которой будут храниться правила. Затем вы можете написать запрос, который рассматривает как правила, так и фактические курсовые работы, представленные студентами. Этот запрос может применить правила к фактическим результатам и определить правильные кредиты для предоставления в каждой категории.

# #### Следующая таблица может использоваться для хранения правил для нашего примера сценария:

cur = conn.cursor()
sql = """
drop table if exists CreditRules;
CREATE TABLE CreditRules(
   RuleId INTEGER,
   TermId INTEGER,
   CourseId CHAR(20),
   TermPaper INTEGER
)
"""
cur.execute(sql)
conn.commit()
cur.close()

# После создания таблицы необходимо заполнить ее кредитными правилами. Как вы могли заметить, правила могут быть непосредственно преобразованы в следующие логические выражения:

# + active=""
# Rule1: Acc1 AND Acc2 AND Acc3
# Rule2: Acc3 AND Mgm3
# Rule3: (Mgm1 AND Mgm2 AND Mgm3) OR (Acc1 AND Acc2)
# -

# Следующие данные показывают представление в таблице кредитных правил правил правил правил, описанных в таблице 2-2. Каждое правило идентифицируется уникальным идентификатором правила. В каждом правиле каждый термин идентифицируется номером термина.

cur = conn.cursor()
sql = """
truncate table CreditRules
INSERT INTO CreditRules VALUES
    (1, 1, 'ACCN101', 1),
    (1, 1, 'ACCN101', 2),
    (1, 1, 'ACCN101', 3),
    (2, 1, 'ACCN101', 3),
    (2, 1, 'MGMT120', 3),
    (3, 1, 'MGMT120', 1),
    (3, 1, 'MGMT120', 2),
    (3, 1, 'MGMT120', 3),
    (3, 2, 'ACCN101', 1),
    (3, 2, 'ACCN101', 2)
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from CreditRules'
select(sql)

sql = """
SELECT DISTINCT s.StudentName,
    (CASE 
        WHEN c.RuleId=1 THEN 'A' 
        WHEN c.RuleId=2 THEN 'B' 
        WHEN c.RuleId=3 THEN 'C' 
    END) credit
FROM Students s 
JOIN CreditRules c
   ON s.CourseId=c.CourseId AND s.TermPaper=c.TermPaper
GROUP BY c.RuleId, c.TermId, s.StudentName
HAVING COUNT(*)=(
    SELECT COUNT(*)
    FROM CreditRules AS c1
    WHERE c.RuleId=c1.RuleId AND c.TermId=c1.TermId
           )
ORDER BY StudentName
"""
select(sql)

# ### Обсуждение

sql = """
SELECT DISTINCT s.StudentName,
    (CASE 
        WHEN c.RuleId=1 THEN 'A' 
        WHEN c.RuleId=2 THEN 'B' 
        WHEN c.RuleId=3 THEN 'C' 
    END) credit,
    c.*
FROM Students s 
JOIN CreditRules c
   ON s.CourseId=c.CourseId AND s.TermPaper=c.TermPaper
"""
select(sql)

sql = """
SELECT DISTINCT c.RuleId, c.TermId, s.StudentName,
    (CASE 
        WHEN c.RuleId=1 THEN 'A' 
        WHEN c.RuleId=2 THEN 'B' 
        WHEN c.RuleId=3 THEN 'C' 
    END) credit,
    COUNT(*) Cnt
FROM Students s 
JOIN CreditRules c
   ON s.CourseId=c.CourseId AND s.TermPaper=c.TermPaper
GROUP BY c.RuleId, c.TermId, s.StudentName
ORDER BY StudentName
"""
select(sql)

sql = """
SELECT COUNT(*)
FROM CreditRules AS c1
WHERE 1=c1.RuleId AND 1=c1.TermId
"""
select(sql)

sql = """
SELECT DISTINCT s.StudentName,
    (CASE 
        WHEN c.RuleId=1 THEN 'A' 
        WHEN c.RuleId=2 THEN 'B' 
        WHEN c.RuleId=3 THEN 'C' 
    END) credit
FROM Students s 
JOIN CreditRules c
   ON s.CourseId=c.CourseId AND s.TermPaper=c.TermPaper
GROUP BY c.RuleId, c.TermId, s.StudentName
HAVING COUNT(*)=(
    SELECT COUNT(*)
    FROM CreditRules AS c1
    WHERE c.RuleId=c1.RuleId AND c.TermId=c1.TermId
           )
ORDER BY StudentName
"""
select(sql)

# ---

conn.close()


