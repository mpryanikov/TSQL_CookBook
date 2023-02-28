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


# # Глава 3. Структуры данных

# ## 3.1 Типы структур данных

# Списки, стеки и очереди-все это линейные структуры данных; термин линейный относится к тому факту, что концептуально вы имеете дело с набором элементов, расположенных в форме строки. Использование SQL хорошо подходит для таких структур, поскольку они легко сопоставляются с парадигмой реляционной таблицы. Массивы и матрицы, с другой стороны, являются многомерными структурами данных. Вы можете использовать SQL для работы с массивами и матрицами, но это лучше подходит для линейных структур.

# Список-это последовательность элементов с известным порядком. Список неограничен по размеру и может уменьшаться или расти по требованию. Элементы могут быть добавлены в список, удалены из списка, и вы можете запросить, есть ли данный элемент в списке

# Кумулятивный запрос агрегации аналогичен запущенному агрегатному запросу с одним отличием. Запущенный агрегат-это тип, создаваемый предложением GROUP BY в сочетании с агрегатной функцией, и он генерирует одно агрегатное значение для каждой группы элементов. Кумулятивные агрегаты, с другой стороны, включают все элементы с начала списка. Например, можно создать отчет, показывающий продажи в январе, продажи в январе и феврале, продажи в январе-марте и так далее. В то время как запущенные агрегаты используются в основном в статистике, кумулятивные агрегаты используются в основном в комбинаторных задачах.

# Область-это непрерывный шаблон в списке, в котором все значения элементов одинаковы. Типичным использованием такого шаблона является поиск пустых слотов в списке. Предположим, у вас есть список, в котором элементы могут быть пустыми или полными слотами. Если вы ищете пять последовательных пустых слотов, вы на самом деле ищете область такого размера. Наиболее типичным примером является построение складского приложения с контейнером тройного размера. Чтобы сохранить его, вам нужно найти пустой слот, который может поместиться в три контейнера обычного размера. Поэтому вы ищете область третьего размера.

# Прогон-это непрерывный шаблон, в котором значения монотонно увеличиваются. Каждое значение больше предыдущего. Например, скажем, что у вас есть список последовательно проведенных измерений для вулкана. В определенный период времени вы можете обнаружить, что температура вулкана постоянно повышалась. Измерения за этот период времени, каждое из которых представляет более высокую температуру, чем предыдущее, будут рассматриваться как прогон.

# # ...

# ## 3.2 Рабочий Пример

# Вы создаете систему контроля качества для биотехнологической компании. Одна из производственных линий производит специальную жидкость для лабораторных исследований. Специфическая характеристика вам нужно измерить очищенность жидкости, которая показана индексом очищенности. Нормальный уровень чистоты представлен индексом чистоты 100. Все, что превышает 100, считается ненормальным и требует некоторых действий, чтобы вернуть продукт в спецификацию.

# Жидкость поступает с производственной линии в контейнерах. Таблица, в которой хранятся данные контроля качества, содержит идентификатор каждого контейнера вместе с измеренной чистотой жидкости в контейнере. Следующая инструкция CREATE TABLE показывает структуру этой таблицы. Столбец ContainerId содержит идентификационные номера контейнеров, а столбец Purity содержит значения Индекса чистоты.

cur = conn.cursor()
sql = """
drop table if exists ProductionLine;
CREATE TABLE ProductionLine (
   ContainerId INTEGER,
   Purity INTEGER
)

Insert into ProductionLine values 
    (1, 100),
    (2, 100),
    (3, 101),
    (4, 102),
    (5, 103),
    (6, 100),
    (7, 103),
    (8, 108),
    (9, 109),
    (10, 100),
    (11, 100),
    (12, 100)
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from ProductionLine'
select(sql)

# ### 3.2.2 Arrays

# Чтобы продемонстрировать использование массивов, мы расширим наш пример производственной линии. Если необходимо управлять несколькими производственными линиями,можно представить их данные чистоты в виде массива. Например:

cur = conn.cursor()
sql = """
drop table if exists ProductionFacility;
CREATE TABLE ProductionFacility(
   Line INTEGER,
   ContainerId INTEGER,
   Purity INTEGER
)

Insert into ProductionFacility values 
    (0, 1, 100),
    (0, 2, 100),
    (0, 3, 100),
    (1, 1, 102),
    (1, 2, 103),
    (1, 3, 100),
    (2, 1, 103),
    (2, 2, 108),
    (2, 3, 109),
    (3, 1, 100),
    (3, 2, 100),
    (3, 3, 100)
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from ProductionFacility'
select(sql)

# ### 3.2.3 Matrices

# Когда вы пишете программу для работы с матрицами, обычно лучше хранить матрицы в таблице так же, как и массивы. Используйте одну строку для каждого элемента матрицы и включите координаты элемента как часть этой строки. Для демонстрационных целей мы будем использовать следующую таблицу Matrices

# + active=""
# Каждая строка в таблице Matrices представляет один элемент matrix. Столбцы используются следующим образом:
#
# Name 
# Associates a row with a specific matrix. Our table can hold multiple matrices, and each is given a unique name. 
# Связывает строку с определенной матрицей. Наша таблица может содержать несколько матриц, и каждой дается уникальное имя.
# X 
# Holds the X coordinate of the element in question. 
# Содержит координату X рассматриваемого элемента.
# Y 
# Holds the Y coordinate of the element in question.
# Содержит координату Y рассматриваемого элемента.
# Value 
# Holds the value of the element in question. 
# Содержит значение рассматриваемого элемента.
#
# We fill our sample table with the following:
# Мы заполняем нашу таблицу образцов следующим:
# Two 2 x 2 matrices, named A and B
# One vector, named S
# One 3 x 3 matrix, named D
# -

cur = conn.cursor()
sql = """
drop table if exists Matrices;
CREATE TABLE Matrices (
   Matrix VARCHAR(20),
   X INTEGER,
   Y INTEGER,
   Value INTEGER
)

Insert into Matrices values 
    ('A', 1, 1, 6),
    ('A', 1, 2, 3),
    ('A', 2, 1, 4),
    ('A', 2, 2, 7),
    ('B', 1, 1, 6),
    ('B', 1, 2, 3),
    ('B', 2, 1, 5),
    ('B', 2, 2, 2),
    ('S', 1, 1, 5),
    ('S', 2, 1, 6),
    ('D', 1, 1, 3),
    ('D', 1, 2, 4),
    ('D', 1, 3, 5),
    ('D', 2, 1, 5),
    ('D', 2, 2, 6),
    ('D', 2, 3, 7),
    ('D', 3, 1, 8),
    ('D', 3, 2, 9),
    ('D', 3, 3, 0)
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from Matrices'
select(sql)

# ## 3.3 Нахождение области

sql = 'select * from ProductionLine'
select(sql)

# Найдите регион в списке. В нашем примере необходимо найти все контейнеры в производственной линии, имеющие индекс чистоты 100. Эти представляют нормальный выход продукции. Кроме того, вам нужны только случаи, когда по крайней мере **два контейнера подряд имеют чистоту 100**. Нечетный контейнер с чистотой 100 посреди нескольких контейнеров с плохими уровнями очищенности не быть сообщенным как нормальный выход продукции.

sql = """
SELECT DISTINCT p1.ContainerID
FROM ProductionLine p1, ProductionLine p2
WHERE
   p1.Purity=100 AND 
   p2.Purity=100 AND
   abs(p1.ContainerId-p2.ContainerId)=1
"""
select(sql)

# ### Discussion

sql = """
SELECT * FROM ProductionLine
WHERE Purity=100
"""
select(sql)

# Чтобы найти соседние строки с одинаковым значением, нам нужны две копии одной и той же таблице.

sql = """
SELECT p1.ContainerID
FROM ProductionLine p1, ProductionLine p2
"""
select(sql)

# Затем мы отфильтровываем все строки, которые не соответствуют критерию наличия одного соседа одного и того же значения. Трюк здесь, чтобы найти соседей, вычисляет расстояние между p1.ContainerId и p2.ContainerId. Если расстояние равно 1, два элемента соседей. Если они имеют одинаковое значение, они должны быть включены в результат:

sql = """
SELECT p1.ContainerID, p2.ContainerID
FROM ProductionLine p1, ProductionLine p2
WHERE 
   abs(p1.ContainerId-p2.ContainerId)=1
"""
select(sql)

sql = """
SELECT p1.ContainerID, p2.ContainerID
FROM ProductionLine p1, ProductionLine p2
WHERE 
   p1.Purity=100 AND p2.Purity=100 AND
   abs(p1.ContainerId-p2.ContainerId)=1
"""
select(sql)

sql = """
SELECT DISTINCT p1.ContainerID
FROM ProductionLine p1, ProductionLine p2
WHERE
   p1.Purity=100 AND 
   p2.Purity=100 AND
   abs(p1.ContainerId-p2.ContainerId)=1
"""
select(sql)

# ## 3.4 Нахождение границ области

sql = 'select * from ProductionLine'
select(sql)

# Как и в предыдущем рецепте, вы хотите найти регионы в данных. Однако теперь вы хотите сообщать только о границах регионов, а не обо всех членах региона. Отчетность только о границах регионов полезна, когда набор данных большой и/или когда ожидается, что размеры любых регионов будут большими.

sql = """
SELECT p1.ContainerId RegBeg, p2.ContainerId RegEnd
FROM ProductionLine p1, ProductionLine p2
WHERE (p1.ContainerId < p2.ContainerId) AND
    NOT EXISTS(
        SELECT * 
        FROM ProductionLine p3 
        WHERE (p3.Purity!=100 AND 
            p3.ContainerId BETWEEN p1.ContainerId AND p2.ContainerId) 
        OR (p3.ContainerId=p1.ContainerId-1 AND p3.Purity=100) 
        OR (p3.ContainerId=p2.ContainerId+1 AND p3.Purity=100)
    )
"""
select(sql)

# ### Discussion

sql = """
SELECT p1.ContainerId RegBeg, p2.ContainerId RegEnd
FROM ProductionLine p1, ProductionLine p2
WHERE (p1.ContainerId < p2.ContainerId) 
"""
select(sql)

# Затем мы пишем подзапрос с именем p3, который использует третий экземпляр таблицы производственной линии. Для каждой пары кандидатов из внешнего запроса мы проверяем, что нет строк между кандидатами с чистотой, отличной от 100:

# + active=""
# NOT EXISTS(SELECT * FROM ProductionLine p3 
#       WHERE (p3.Purity!=100 AND 
#       p3.ContainerId BETWEEN p1.ContainerId AND p2.ContainerId)
# -

# Если есть хотя бы одна строка, возвращаемая этим подзапросом, это означает, что область нарушена (т. е. это не непрерывная область чистоты=100), и пара кандидатов должна быть отброшена. Однако этого недостаточно для достижения желаемого результата, поскольку подзапрос не устраняет небольшие области, которые полностью содержатся в больших областях. Например, в области между 10 и 12 мы также найдем области 10-11 и 11-12. Решение находится в двух дополнительных условиях в конце подзапроса, которые проверяют нижние и верхние границы для возможных соседей, которые соответствуют требованию региона:

# + active=""
# (p3.ContainerId=p1.ContainerId-1 AND p3.Purity=100) OR
# (p3.ContainerId=p2.ContainerId+1 AND p3.Purity=100
# -

# ## 3.5 Ограничение Размера Области

sql = 'select * from ProductionLine'
select(sql)

# Поскольку мы использовали увеличение числа ContainerId, мы можем ограничить размер области в предложении WHERE нашего запроса, ограничив расстояние между индексами с помощью формулы SIZE-1. Поскольку мы ищем области размера 2, мы используем 2-1 или 1:

sql = """
SELECT p1.ContainerId RegBeg, p2.ContainerId RegEnd
FROM ProductionLine p1, ProductionLine p2
WHERE (p1.ContainerId < p2.ContainerId) AND
    p2.ContainerId-p1.ContainerId=1 AND
    NOT EXISTS(
        SELECT * FROM ProductionLine p3 
        WHERE (p3.Purity!=100 AND 
            p3.ContainerId BETWEEN p1.ContainerId AND p2.ContainerId)
    )
"""
select(sql)

# ### Discussion

# Этот запрос обычно используется в различных комбинаторных задач. Он похож на запрос в предыдущем рецепте, но с двумя важными отличиями. Во-первых, он ограничивает размер области фиксированным размером. Поскольку таблица имеет арифметически возрастающие значения ContainerId, это может быть достигнуто путем ограничения разницы между двумя индексами.

# + active=""
# p2.ContainerId-p1.ContainerId=1
# -

# Второе отличие заключается в том, что вам не нужно искать максимально возможные регионы, поэтому последние два условия в предложении WHERE подзапроса предыдущего рецепта могут быть опущены из этого запроса. 
# Очень легко расширить этот рецепт, чтобы найти все регионы, которые больше или меньше определенного размера. Например, чтобы найти все области двух или более контейнеров, используйте следующее ограничение предложения WHERE:

# + active=""
# ...
# p2.ContrainerId - p1.ContainerId >=1
# ...
# -

# Таким же образом можно ограничить запрос всеми регионами, имеющими пять или менее строк:

# + active=""
# ...
# p2.ContrainerId - p1.ContainerId <=4
# ...
# -

# Эти последние две модификации вернут все регионы-кандидаты, даже те небольшие регионы, которые находятся внутри больших регионов. В зависимости от результатов, которые вы хотите, если вы используете одну из этих модификаций, вы можете добавить обратно те два условия WHERE из предыдущего рецепта, которые ограничивали регионы, возвращенные только тем, которые не содержатся в другой большей области.

# ## 3.6 Ранжирование областей по размеру

sql = 'select * from ProductionLine'
select(sql)

# Вы хотите перечислить все регионы в таблице, и вы хотите перечислить их в соответствии с их размером. Что касается нашего примера, вы хотите перечислить все регионы двух или более контейнеров с чистотой 100, и вы хотите отсортировать этот список по количеству контейнеров в каждом регионе.

sql = """
SELECT p1.ContainerId RegBeg, p2.ContainerId RegEnd, 
    p2.ContainerId-p1.ContainerId+1 RegionSize
FROM ProductionLine p1, ProductionLine p2
WHERE (p1.ContainerId < p2.ContainerId) AND
    NOT EXISTS(
        SELECT * 
        FROM ProductionLine p3 
        WHERE (p3.Purity!=100 AND 
                p3.ContainerId BETWEEN p1.ContainerId AND p2.ContainerId) 
            OR (p3.ContainerId=p1.ContainerId-1 AND p3.Purity=100) 
            OR (p3.ContainerId=p2.ContainerId+1 AND p3.Purity=100)
    )
ORDER BY p2.ContainerId-p1.ContainerId DESC
"""
select(sql)

# ### Discussion

# Как видите, этот запрос похож на тот, который используется для поиска регионов. Добавленной особенностью является предложение ORDER BY, которое сортирует регионы в соответствии с их размером. Он основан на том, что в таблице используется арифметически увеличивающийся индекс, с помощью которого можно рассчитать размер региона на основе разницы между двумя индексами, составляющими границы региона.

# Вместо того чтобы просто сообщать начальный и конечный индекс для каждого региона, этот запрос использует тот же расчет в списке выбора, что и в предложении ORDER BY, чтобы сообщить размер каждого региона с точки зрения количества контейнеров.

# Запрос пригодится, когда вам нужно подготовить данные для наиболее подходящего алгоритма, и вы хотите использовать базу данных для предварительной сортировки данных.

# Вы можете развернуть решение, показанное в этом рецепте, если хотите, чтобы показать наименьшую доступную область, которая все еще больше заданного размера. Для этого добавьте выражение предложения WHERE, чтобы ограничить размер отсортированных областей. Например:

sql = """
SELECT TOP 1
    p1.ContainerId RegBeg, p2.ContainerId RegEnd, 
    p2.ContainerId-p1.ContainerId+1 RegionSize
FROM ProductionLine p1, ProductionLine p2
WHERE 
    (p1.ContainerId < p2.ContainerId) AND
    (p2.ContainerId-p1.ContainerId)>=2 AND 
    NOT EXISTS(
        SELECT * 
        FROM ProductionLine p3 
        WHERE (p3.Purity!=100 AND 
            p3.ContainerId BETWEEN p1.ContainerId AND p2.ContainerId) 
        OR (p3.ContainerId=p1.ContainerId-1 AND p3.Purity=100) 
        OR (p3.ContainerId=p2.ContainerId+1 AND p3.Purity=100)
    )
ORDER BY p2.ContainerId-p1.ContainerId ASC
"""
select(sql)

# Этот запрос возвращает наименьшую возможную область, которая все еще вписывается в предел. В этом случае возвращается только первая область, которая соответствует ограничениям

# ## 3.7 Работа с последовательностями

sql = 'select * from ProductionLine'
select(sql)

# Вы хотите найти любую арифметически возрастающую последовательность в данных. В нашем примере вы хотите найти любые последовательности, в которых уровень чистоты арифметически увеличивается (100, 101, 102 и т. д.). Любые такие последовательности длиной в три или более контейнера указывают на перегрев производственной линии.

sql = """
SELECT p1.ContainerId SeqBeg, p2.ContainerId SeqEnd,
    p2.ContainerId-p1.ContainerId+1 SequenceSize
FROM ProductionLine p1, ProductionLine p2
WHERE (p1.ContainerId < p2.ContainerId) AND
    NOT EXISTS(
        SELECT * 
        FROM ProductionLine p3 
        WHERE (p3.Purity-p3.ContainerId!=p1.Purity-p1.ContainerId AND 
                p3.ContainerId BETWEEN p1.ContainerId AND p2.ContainerId) 
            OR (p3.ContainerId=p1.ContainerId-1 AND 
                p3.Purity-p3.ContainerId=p1.Purity-p1.ContainerId) 
            OR (p3.ContainerId=p2.ContainerId+1 AND 
                p3.Purity-p3.ContainerId=p1.Purity-p1.ContainerId)
    )
"""
select(sql)

# ### Discussion

# Этот запрос использует структуру, аналогичную той, которая используется для поиска регионов. Разница в том, что подзапрос содержит условие предложения WHERE для идентификации последовательностей. Чтобы точно объяснить, как это работает, давайте начнем с просмотра необработанных данных:

# + active=""
# ContainerId Purity      Diff        
# ----------- ----------- ----------- 
# 1           100         99
# 2           100         98
# 3           101         98
# 4           102         98
# 5           103         98
# 6           100         94
# 7           103         96
# 8           108         100
# 9           109         100
# 10          100         90
# 11          100         89
# 12          100         88
# -

# Обратите внимание, что уровни чистоты контейнеров 2-5 представляют собой арифметически возрастающую последовательность. Обратите внимание также, что идентификаторы контейнеров представляют арифметически возрастающую последовательность. Мы можем использовать тот факт, что обе последовательности монотонны в наших интересах. Это означает, что в любой заданной последовательности разница между контейнером и чистотой будет постоянной. Например:

# + active=""
# 100 - 2 = 98
# 101 - 3 = 98
# ...
# -

# Мы используем знание этого шаблона в подзапросе, который использует следующее условие WHERE для поиска любых кандидатов, которые нарушают последовательность:

# + active=""
# p3.Purity-p3.ContainerId!=p1.Purity-p1.ContainerId
# -

# Если какая-либо строка в последовательности-кандидате (p3) имеет ту же разницу, что и первая строка последовательности (p1), она является членом последовательности. Если нет, то следует отказаться от пары кандидатов (p1, p2).   
# Остальная часть структуры точно такая же, как и для поиска областей, и вы можете легко расширить ее для последовательностей так же, как и при поиске областей. Например, чтобы найти только последовательности больше трех строк, добавьте следующее условие WHERE:

# + active=""
# p2.ContainerId-p1.ContainerId>=2 AND
# -

# #### For example:

sql = """
SELECT p1.ContainerId SeqBeg, p2.ContainerId SeqEnd,
    p2.ContainerId-p1.ContainerId+1 SequenceSize
FROM ProductionLine p1, ProductionLine p2
WHERE 
    (p1.ContainerId < p2.ContainerId) AND
    p2.ContainerId-p1.ContainerId>=2 AND
    NOT EXISTS(
        SELECT * FROM ProductionLine p3 
        WHERE (p3.Purity-p3.ContainerId!=p1.Purity-p1.ContainerId AND 
            p3.ContainerId BETWEEN p1.ContainerId AND p2.ContainerId) 
        OR (p3.ContainerId=p1.ContainerId-1 AND 
            p3.Purity-p3.ContainerId=p1.Purity-p1.ContainerId) 
        OR (p3.ContainerId=p2.ContainerId+1 AND 
            p3.Purity-p3.ContainerId=p1.Purity-p1.ContainerId)
    )
"""
select(sql)

# С помощью этой структуры можно использовать алгоритмы для регионов и применять их к последовательностям с минимальными изменениями кода. Обычно вам просто нужно добавить дополнительное условие, такое как то, которое мы добавили в этот рецепт.

# ## 3.8 Работа с Runs

sql = 'select * from ProductionLine'
select(sql)

# Вы хотите найти прогоны в своей таблице. В нашем примере вы хотите найти любые возрастающие (арифметически и не арифметически) последовательности значений чистоты.

sql = """
SELECT 
   p1.ContainerId SeqBeg, p2.ContainerId SeqEnd
FROM ProductionLine p1, ProductionLine p2
WHERE 
   (p1.ContainerId < p2.ContainerId) AND
   NOT EXISTS(SELECT * FROM ProductionLine p3, ProductionLine p4 
      WHERE ( 
      p3.Purity<=p4.Purity AND 
      p4.ContainerId=p3.ContainerId-1 AND
      p3.ContainerId BETWEEN p1.ContainerId+1 AND p2.ContainerId) 
      OR (p3.ContainerId=p1.ContainerId-1 AND p3.Purity<p1.Purity) 
      OR (p3.ContainerId=p2.ContainerId+1 AND p3.Purity>p2.Purity))
"""
select(sql)

# ### Discussion

# Этот запрос использует структуру, подобную той, которую вы видели много раз в этой главе. В отличие от последовательности, прогон представляет собой непрерывно возрастающий, хотя и не обязательно монотонно возрастающий ряд значений. В отличие от предыдущего рецепта, в котором мы искали монотонно возрастающие последовательности, у нас нет постоянной разницы между значениями ContainerId и Purity. Следовательно, нам нужна четвертая таблица, p4 в этом случае, чтобы проверить строки в середине интервала кандидата, которые не соответствуют требованию выполнения. Эта таблица p4 вступает в игру в подзапросе, где мы присоединяемся к ней к p3.

# Для каждого элемента между p1 и p2 сравниваются p3 и его предшественник, чтобы увидеть, если их значения увеличиваются:

# + active=""
# p3.Purity<=p4.Purity AND 
# p4.ContainerId=p3.ContainerId-1 AND
# p3.ContainerId BETWEEN p1.ContainerId+1 AND p2.ContainerId
# -

# Предложение BETWEEN ограничивает область действия строками между границами (p1 и p2) рассматриваемого кандидата. Граница p1 увеличивается на 1, которая охватывает все пары в пределах области. Обратите внимание, что всегда на одну пару меньше, чем количество строк.

# Подобно другим запросам для регионов и последовательностей, последние два условия в предложении WHERE подзапроса гарантируют, что границы выполнения кандидата не могут быть расширены:

# + active=""
# (p3.ContainerId=p1.ContainerId-1 AND p3.Purity<p1.Purity) OR
# (p3.ContainerId=p2.ContainerId+1 AND p3.Purity>p2.Purity)
# -

# Если строка может быть возвращена для удовлетворения этих условий, то запуск может быть расширен и должен быть отклонен в пользу большего запуска

# Общая структура, которую это решение разделяет с более ранними рецептами, позволяет использовать методы, представленные ранее для регионов и последовательностей, и применять их к запускам.

# ## 3.9 Совокупные агрегаты в списках

sql = 'select * from ProductionLine'
select(sql)

# Вам нужно сообщить совокупные итоги и средние значения. В отношении нашего примера предположим, что значение чистоты является мерой веса, скажем килограммов. Для целей упаковки вы хотите увидеть, в каком контейнере общий вес продукции производственной линии поднимается выше 1,000. Кроме того, вам интересно узнать, как каждый дополнительный контейнер влияет на средний вес в отгрузке.

sql = """
SELECT 
   p1.ContainerId, SUM(p2.Purity) Total, AVG(p2.Purity) Average 
FROM ProductionLine p1, ProductionLine p2
WHERE 
   p1.ContainerId >= p2.ContainerId
GROUP BY p1.ContainerId
"""
select(sql)

# ### Discussion

# Код использует старый трюк SQL для заказа. Вы берете два экземпляра таблицы производственной линии с именами p1 и p2 и соединяете их. Затем вы группируете результаты по p1.ContainerId, и вы ограничиваете строки второй таблицы (p2) так, чтобы они имели значения ContainerId меньше, чем строка p1, к которой они присоединены. Это заставляет сервер создавать промежуточный результирующий набор, который выглядит следующим образом:

# + active=""
# p1_Id       p1_Purity   p2_Id       p2_Purity   
# ----------- ----------- ----------- ----------- 
# 1           100         1           100
# 2           100         1           100
# 2           100         2           100
# 3           101         1           100
# 3           101         2           100
# 3           101         3           101
# 4           102         1           100
# 4           102         2           100
# 4           102         3           101
# 4           102         4           102
# 5           103         1           100
# ...
# -

# Каждая группа, идентифицированная p1.ContainerId, включает все строки из p2 с более низкими или эквивалентными значениями ContainerId. Затем функции AVG и SUM применяются к столбцу p2_Purity. Две функции работают над строками p2 в каждой группе и, таким образом, вычисляют кумулятивные результаты.

# ## 3.10 Реализация стека

# Вам нужно реализовать структуру данных стека в SQL. Что касается нашего примера, вам нужно построить интерфейс к обрабатывающей машине, которая добавляет и удаляет контейнеры В и из производственной линии. Производственная линия должна обрабатываться как стек. Поэтому необходимо реализовать функции POP, PUSH и TOP.

# ### 3.10.2 Solution

cur = conn.cursor()
sql = """
drop table if exists ProductionLine;
CREATE TABLE ProductionLine (
   ContainerId INTEGER,
   Purity INTEGER
)

Insert into ProductionLine values 
    (1, 100),
    (2, 100),
    (3, 101),
    (4, 102),
    (5, 103),
    (6, 100),
    (7, 103),
    (8, 108),
    (9, 109),
    (10, 100),
    (11, 100),
    (12, 100)
"""
cur.execute(sql)
conn.commit()
cur.close()

# Используйте таблицу как стек. В следующих разделах показано, как реализовать различные функции стека с помощью SQL. Обратите внимание, что мы используем столбец ContainerId для сохранения элементов стека в правильном порядке. Каждый новый элемент, помещенный в стек, получает более высокое значение ContainerId, чем предыдущий элемент. Верхняя часть стека определяется как строка с самым высоким значением ContainerId.

sql = 'select * from ProductionLine'
select(sql)

# ### 3.10.2.1 TOP function in SQL

cur = conn.cursor()
sql = """
declare @ProcName sysname set @ProcName = 'TopProduction'
if exists (select * from dbo.sysobjects where id = object_id(@ProcName))
  exec ('drop procedure ' + @ProcName)
"""
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = """
CREATE PROCEDURE TopProduction
AS
SELECT TOP 1 * FROM ProductionLine ORDER BY ContainerId DESC
"""
cur.execute(sql)
conn.commit()
cur.close()

sql = """
Exec TopProduction
"""
select(sql)

# ### 3.10.2.2 POP function in SQL

cur = conn.cursor()
sql = """
declare @ProcName sysname set @ProcName = 'Pop'
if exists (select * from dbo.sysobjects where id = object_id(@ProcName))
  exec ('drop procedure ' + @ProcName)
"""
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = """

CREATE PROCEDURE Pop 
AS 
DECLARE @id INTEGER
SELECT TOP 1 @id=ContainerId FROM ProductionLine 
   ORDER BY ContainerId DESC
SELECT * FROM ProductionLine WHERE @id=ContainerId
DELETE FROM ProductionLine WHERE @id=ContainerId
"""
cur.execute(sql)
conn.commit()
cur.close()

sql = """
Exec Pop
"""
conn.commit()
select(sql)

# ### 3.10.2.3 PUSH function in SQL

cur = conn.cursor()
sql = """
declare @ProcName sysname set @ProcName = 'Push'
if exists (select * from dbo.sysobjects where id = object_id(@ProcName))
  exec ('drop procedure ' + @ProcName)
"""
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = """
CREATE PROCEDURE Push @Purity INTEGER 
AS 
DECLARE @id INTEGER
SELECT TOP 1 @id=ContainerId FROM ProductionLine 
   ORDER BY ContainerId DESC
INSERT INTO ProductionLine(ContainerId,Purity) VALUES(@id+1, @Purity)
SELECT * FROM ProductionLine WHERE ContainerId=@id+1
"""
cur.execute(sql)
conn.commit()
cur.close()

# + active=""
# sql = """
# Exec Push 503
# """
# select(sql)
# -

sql = 'select * from ProductionLine'
select(sql)

# ### Discussion

# Код, показанный в нашем решении, является упрощенной версией реальной системы. Если вы хотите использовать концепцию в живой системе, убедитесь, что вы делаете процедуры транзакционными. Кроме того, если несколько пользователей используют функции POP и PUSH, используйте механизм приращения, отличный от функции plain MAX, используемой в нашем решении. Например, для обеспечения уникальности значений идентификаторов можно использовать собственные решения SQL server, такие как IDENITY Microsoft или типы данных UNIQUEIDENTIFIER. Будьте осторожны: такие решения могут быть дорогостоящими и не всегда применимы.

# ## 3.11 Реализация Очередей

# Необходимо реализовать очередь со стандартными операциями, такими как TOP, ENQUEUE и DEQUEUE. Что касается нашего примера, вы хотите реализовать ту же функциональность, что и в предыдущем рецепте, но на этот раз вы хотите рассматривать производственную линию как очередь, а не как стек.

sql = 'select * from ProductionLine'
select(sql)

# ### 3.11.2.1 TOP function in SQL

sql = """
SELECT TOP 1 *  FROM ProductionLine ORDER BY ContainerId ASC
"""
select(sql)

# ### 3.11.2.2 DEQUEUE function in SQL

cur = conn.cursor()
sql = """
declare @ProcName sysname set @ProcName = 'dequeue'
if exists (select * from dbo.sysobjects where id = object_id(@ProcName))
  exec ('drop procedure ' + @ProcName)
"""
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = """
CREATE PROCEDURE dequeue 
AS 
DECLARE @id INTEGER
SELECT TOP 1 @id=ContainerId FROM ProductionLine ORDER BY ContainerId ASC
SELECT * FROM ProductionLine WHERE @id=ContainerId
DELETE FROM ProductionLine WHERE @id=ContainerId
"""
cur.execute(sql)
conn.commit()
cur.close()

sql = """
Exec dequeue
"""
select(sql)

# ### 3.11.2.3 ENQUEUE function in SQL

cur = conn.cursor()
sql = """
declare @ProcName sysname set @ProcName = 'enqueue'
if exists (select * from dbo.sysobjects where id = object_id(@ProcName))
  exec ('drop procedure ' + @ProcName)
"""
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = """
CREATE PROCEDURE enqueue @Purity INTEGER 
AS 
DECLARE @id INTEGER
SELECT TOP 1 @id=ContainerId FROM ProductionLine ORDER BY ContainerId DESC
INSERT INTO ProductionLine(ContainerId,Purity) VALUES(@id+1, @Purity)
SELECT * FROM ProductionLine WHERE ContainerId=@id+1
"""
cur.execute(sql)
conn.commit()
cur.close()

# + active=""
# sql = """
# Exec enqueue 777
# """
# select(sql)
# -

conn.commit()

# ## 3.12 Реализация Очередей Приоритетов

# Как и в случае стеков и регулярных очередей, мы можем реализовать очередь приоритетов в таблице производственных линий

sql = 'select * from ProductionLine'
select(sql)

# ### 3.12.2.1 TOP function in SQL

sql = """
SELECT TOP 1 *  FROM ProductionLine ORDER BY Purity DESC
"""
select(sql)

# ### 3.12.2.2 DEQUEUE function in SQL

cur = conn.cursor()
sql = """
declare @ProcName sysname set @ProcName = 'dequeue_priority'
if exists (select * from dbo.sysobjects where id = object_id(@ProcName))
  exec ('drop procedure ' + @ProcName)
"""
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = """
CREATE PROCEDURE dequeue_priority 
AS 
DECLARE @id INTEGER
SELECT TOP 1 @id=ContainerId FROM ProductionLine ORDER BY Purity DESC
SELECT * FROM ProductionLine WHERE @id=ContainerId
DELETE FROM ProductionLine WHERE @id=ContainerId
"""
cur.execute(sql)
conn.commit()
cur.close()

sql = """
Exec dequeue_priority
"""
select(sql)

# ### 3.12.2.3 ENQUEUE function in SQL

cur = conn.cursor()
sql = """
declare @ProcName sysname set @ProcName = 'enqueue_priority'
if exists (select * from dbo.sysobjects where id = object_id(@ProcName))
  exec ('drop procedure ' + @ProcName)
"""
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = """
CREATE PROCEDURE enqueue_priority @Purity INTEGER 
AS 
DECLARE @id INTEGER
SELECT TOP 1 @id=ContainerId FROM ProductionLine ORDER BY ContainerId DESC
INSERT INTO ProductionLine(ContainerId,Purity) VALUES(@id+1, @Purity)
SELECT * FROM ProductionLine WHERE ContainerId=@id+1
"""
cur.execute(sql)
conn.commit()
cur.close()

# + active=""
# sql = """
# Exec enqueue_priority 77788
# """
# select(sql)
# -

conn.commit()

# ### Discussion

# Приоритетные очереди используют структуру, почти идентичную той, которая используется для стеков и обычных очередей. Разница, опять же, заключается только в том, как реализуется функция TOP. Когда вы настраиваете TOP, чтобы посмотреть на очередь с точки зрения приоритета, в нашем случае в столбце чистоты, все остальные части встают на место. Функция ENQUEUE такая же, как и для обычных очередей. За исключением использования функции TOP на основе приоритета, функция DEQUEUE также совпадает с функцией регулярных очередей.

# При использовании таблицы в качестве очереди приоритетов функция ENQUEUE больше не может обеспечивать монотонно возрастающий индекс (как в случае со стеками и очередями). Это потому, что функция DEQUEUE извлекает элементы из очереди на основе их приоритета, а не их индекса. Например, если у вас есть 10 элементов, идентифицированных со значениями индекса от 1 до 10, а пятый элемент удален, поскольку он имеет наивысший приоритет, в индексе будет пробел. Но при добавлении нового элемента функция ENQUEUE не заполнит этот пробел, а добавит новый элемент со значением индекса 11. Такое поведение легко пропустить, что может вызвать некоторую путаницу, поэтому имейте это в виду при работе с очередями приоритетов

# ## 3.13 Сравнение двух строк в массиве

sql = 'select * from ProductionFacility'
select(sql)

# Вы хотите проверить, равны ли две строки в массиве. В нашем примере необходимо проверить, равны ли две производственные линии в таблице производственный объект.

sql = """
SELECT p1.Line p1_Line, 'is equal to', p2.Line p2_Line
FROM ProductionFacility p1, ProductionFacility p2
WHERE p1.Purity=p2.Purity AND p1.ContainerId=p2.ContainerId AND
   p1.Line<p2.Line
GROUP BY p1.Line, p2.Line
HAVING 
   COUNT(*)=(SELECT COUNT(*) FROM ProductionFacility p3 WHERE p3.Line=p1.Line) 
   AND
   COUNT(*)=(SELECT COUNT(*) FROM ProductionFacility p4 WHERE p4.Line=p2.Line) 
"""
select(sql)

# ### Discussion

# Этот запрос оказывается довольно дорогим, используя четыре экземпляра таблицы; в результате это хорошая демонстрация того, как SQL не очень эффективен в работе с массивами. Однако, как бы дорого это ни было, это позволяет получить результаты, используя только один запрос.

# Предложение FROM создает перекрестное соединение между двумя экземплярами производственного объекта. Мы называем два экземпляра p1 и p2 для упрощения ссылки. Мы определяем в инструкции SELECT, что результат будет сообщать одну строку для каждой пары строк, которые равны. Поскольку перекрестное соединение создает много строк, мы используем оператор GROUP BY, чтобы ограничить результат только одной строкой вывода на строку в массиве.

# Предложение WHERE указывает три условия:  
#   -  Уровни чистоты должны быть равными.  
#   -  Идентификаторы контейнеров должны быть равны.  
#   -  Номера производственных линий из p1 должны быть меньше, чем из p2.

# Если вы работаете с многомерными массивами, просто добавьте дополнительные предложения сравнения в предложение WHERE для сравнения параметров равенства. Чтобы сравнить для полного равенства между двумя строками, необходимо иметь одно выражение сравнения для каждого измерения в массиве. В нашем примере два предложения сравнения включают столбцы ContainerId и Line. Выражение сравнения, включающее столбцы чистоты, используется для определения равенства двух элементов массива. Таким образом, совпадение на ContainerId и Line определяет два элемента, которые необходимо сравнить, а тест равенства включает столбец чистоты.

# Промежуточные результаты на данный момент, без предложения GROUP BY, являются следующими:

sql = """
SELECT p1.ContainerId, p1.Purity, p1.Line, p2.Line
FROM ProductionFacility p1, ProductionFacility p2
WHERE p1.Purity=p2.Purity AND p1.ContainerId=p2.ContainerId AND
   p1.Line<p2.Line
"""
select(sql)

# Добавьте в предложение GROUP BY, и мы получим:

sql = """
SELECT COUNT(*) ContainerCount, p1.Line, p2.Line
FROM ProductionFacility p1, ProductionFacility p2
WHERE p1.Purity=p2.Purity AND p1.ContainerId=p2.ContainerId AND
   p1.Line<p2.Line
GROUP BY p1.Line, p2.Line
"""
select(sql)

# Предложение HAVING является дорогостоящим. Он сравнивает количество совпадающих пар из предложения WHERE с количеством столбцов в обеих строках. Первый подзапрос проверяет количество строк в p1, а второй-количество строк в p2. Предложение HAVING гарантирует, что в конечном результате отображаются только строки одинакового размера. В нашем примере, каждая производственная линия производила 3 контейнера. Глядя на промежуточные результаты, показанные здесь, вы можете увидеть, что только две производственные линии с количеством контейнеров три строки 0 и 3. Предложение HAVING гарантирует, что они сообщаются как конечный результат запроса

# ## 3.14 Вывод матриц и массивов

sql = 'select * from Matrices'
select(sql)

# Используйте следующий метод поворота, который в этом случае печатает матрицу D:

cur = conn.cursor()
sql = """
declare @ProcName sysname set @ProcName = 'Matrices_Print'
if exists (select * from dbo.sysobjects where id = object_id(@ProcName))
  exec ('drop procedure ' + @ProcName)
"""
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = """
CREATE PROCEDURE Matrices_Print @Matr CHAR(1) 
AS 
SELECT  X, 
   MAX(CASE Y WHEN 1 THEN Value END) y1,
   MAX(CASE Y WHEN 2 THEN Value END) y2,
   MAX(CASE Y WHEN 3 THEN Value END) y3
FROM Matrices
WHERE Matrix=@Matr
GROUP BY X
ORDER BY X
"""
cur.execute(sql)
conn.commit()
cur.close()

sql = """
Exec Matrices_Print 'D'
"""
select(sql)

# ### Discussion

sql = 'select * from ProductionFacility'
select(sql)

# Предположим, вы хотите напечатать массив в виде отчета с каждым измерением в отдельном столбце. В нашем примере вы хотите напечатать отчет об уровнях чистоты для всех контейнеров во всех производственных линиях и хотите, чтобы каждая производственная линия была представлена собственным столбцом.

sql = """
SELECT  ContainerId, 
   MAX(CASE Line WHEN 0 THEN Purity END) Line0,
   MAX(CASE Line WHEN 1 THEN Purity END) Line1,
   MAX(CASE Line WHEN 2 THEN Purity END) Line2,
   MAX(CASE Line WHEN 3 THEN Purity END) Line3
FROM ProductionFacility
GROUP BY ContainerId
ORDER BY ContainerId
"""
select(sql)

# См. обсуждение использования **сводных таблиц** в Главе 1 Chapter 1. Pivot Tables. Обратите внимание, в частности, что число выражений CASE должно соответствовать измерению y матрицы. В этом случае мы знаем, что матрица, которую мы хотим напечатать, имеет три столбца, поэтому мы написали три выражения CASE.

# ## 3.15 Транспонирование матрицы

sql = 'select * from Matrices'
select(sql)

sql = """
SELECT  X, 
   MAX(CASE Y WHEN 1 THEN Value END) y1,
   MAX(CASE Y WHEN 2 THEN Value END) y2,
   MAX(CASE Y WHEN 3 THEN Value END) y3
FROM Matrices
WHERE Matrix='D'
GROUP BY X
ORDER BY X
"""
select(sql)

# Вы хотите транспонировать матрицу. Чтобы транспонировать матрицу, замените все значения X и Y. Например, элемент, расположенный при X=1, Y=2, будет заменен элементом, расположенным при X=2, Y=1.

sql = """
SELECT Y AS X, X AS Y, Value 
FROM Matrices
WHERE Matrix='D'
"""
select(sql)

sql = """
with 
Matrices_tr as (
    SELECT Y AS X, X AS Y, Value 
    FROM Matrices
    WHERE Matrix='D'
)
SELECT  X, 
   MAX(CASE Y WHEN 1 THEN Value END) y1,
   MAX(CASE Y WHEN 2 THEN Value END) y2,
   MAX(CASE Y WHEN 3 THEN Value END) y3
FROM Matrices_tr
GROUP BY X
ORDER BY X
"""
select(sql)

# ### Discussion

# Транспозиция, вероятно, является одной из самых простых операций над матрицами. Единственное, что вам нужно сделать, это сообщить X как Y и Y как X, и это транспонирует матрицу. Если вы хотите сохранить транспозицию-этот рецепт печатает только транспонированную версию-вы можете написать INSERT . . . SELECT . . . FROM:

# + active=""
# sql = """
# INSERT INTO Matrices
# SELECT 'Dt',Y, X, Value 
# FROM Matrices
# WHERE Matrix='D'
# """
# select(sql)
# -

# Этот оператор транспонирует матрицу D и сохраняет результаты в новой матрице с именем Dt.

# ## 3.16 Расчет матрицы трассировки

sql = 'select * from Matrices'
select(sql)

sql = """
Exec Matrices_Print 'D'
"""
select(sql)

# Вы хотите вычислить след матрицы. След матрицы-это суммирование значений по главной диагонали матрицы.

sql = """
SELECT SUM(Value) Trace 
FROM Matrices
WHERE Matrix='D' and X=Y
"""
select(sql)

# ## 3.17 Сравнение двух матриц по размеру

sql = 'select * from Matrices'
select(sql)

sql = """
Exec Matrices_Print 'A'
"""
select(sql)

sql = """
Exec Matrices_Print 'B'
"""
select(sql)

sql = """
Exec Matrices_Print 'D'
"""
select(sql)

# Вы хотите сравнить две матрицы, чтобы увидеть, равны ли они по размеру. Под равным размером мы подразумеваем, что их самые высокие размеры X и Y одинаковы.

sql = """
SELECT m1.Matrix, 'is of equal size as', m2.Matrix
FROM Matrices m1, Matrices m2
WHERE m1.X=m2.X AND m1.Y=m2.Y AND m1.Matrix='A' AND m2.Matrix='B'
GROUP BY m1.Matrix, m2.Matrix 
HAVING 
   COUNT(*)=(SELECT COUNT(*) FROM Matrices WHERE Matrix='A') 
   AND COUNT(*)=(SELECT COUNT(*) FROM Matrices WHERE Matrix='B')
"""
select(sql)

# ### Discussion

# Некоторые матричные операции требуют, чтобы задействованные матрицы были одинакового размера. Используйте запрос в этом рецепте, чтобы убедиться, что это так.

# Во-первых, мы создаем два экземпляра таблицы матриц (m1 и m2) и ограничиваем каждый из них одной из матриц, которые нас интересуют. В нашем случае m1 представляет матрицу A, в то время как m2 представляет матрицу B. Если матрицы равны, это даст нам две строки для каждой комбинации значений индекса X и Y.

# Затем, в предложении WHERE, мы сопоставляем координаты двух матриц. Предложение GROUP BY используется так, что запрос сообщает только одну строку вывода. Результаты сгруппированы по двум названиям матриц. Затем предложение HAVING проверяет, соответствует ли общее число суммированных строк общему числу элементов в A и B. Если все итоговые значения совпадают, то две матрицы имеют одинаковый размер.

# ## 3.18 Добавление и вычитание матриц

sql = 'select * from Matrices'
select(sql)

sql = """
Exec Matrices_Print 'A'
"""
select(sql)

sql = """
Exec Matrices_Print 'B'
"""
select(sql)

# Чтобы добавить матрицы A и B, используйте:

sql = """
WITH
res AS (
SELECT DISTINCT m1.X, m2.Y, m1.Value + m2.Value Value
FROM Matrices m1, Matrices m2
WHERE m1.Matrix='A' AND m2.Matrix='B'
   AND m1.X=m2.X AND m1.Y=m2.Y
)
select * from res
"""
select(sql)

sql = """
WITH
res AS (
SELECT DISTINCT m1.X, m2.Y, m1.Value + m2.Value Value
FROM Matrices m1, Matrices m2
WHERE m1.Matrix='A' AND m2.Matrix='B'
   AND m1.X=m2.X AND m1.Y=m2.Y
)
SELECT  X, 
   MAX(CASE Y WHEN 1 THEN Value END) y1,
   MAX(CASE Y WHEN 2 THEN Value END) y2,
   MAX(CASE Y WHEN 3 THEN Value END) y3
FROM res
GROUP BY X
ORDER BY X
"""
select(sql)

# Чтобы вычесть матрицу B из матрицы A, используйте этот запрос, но замените знак плюс знаком минус

sql = """
WITH
res AS (
SELECT DISTINCT m1.X, m2.Y, m1.Value - m2.Value Value
FROM Matrices m1, Matrices m2
WHERE m1.Matrix='A' AND m2.Matrix='B'
   AND m1.X=m2.X AND m1.Y=m2.Y
)
select * from res
"""
select(sql)

sql = """
WITH
res AS (
SELECT DISTINCT m1.X, m2.Y, m1.Value - m2.Value Value
FROM Matrices m1, Matrices m2
WHERE m1.Matrix='A' AND m2.Matrix='B'
   AND m1.X=m2.X AND m1.Y=m2.Y
)
SELECT  X, 
   MAX(CASE Y WHEN 1 THEN Value END) y1,
   MAX(CASE Y WHEN 2 THEN Value END) y2,
   MAX(CASE Y WHEN 3 THEN Value END) y3
FROM res
GROUP BY X
ORDER BY X
"""
select(sql)

# ### Discussion

# Этот код следует определениям сложения и вычитания матриц из алгебры. Чтобы добавить две матрицы, они должны быть одинаковой размерности (т. е. они должны быть равны), а затем вы просто добавляете элементы по тем же координатам. Вычитание работает так же, за исключением того, что вы вычитаете значения элементов, а не добавляете.

# Хитрость решения этого рецепта заключается в сопоставлении элементов по одним и тем же координатам из двух матриц. Мы предполагаем, что матрицы уже имеют одну и ту же размерность; другими словами, мы предполагаем, что они равны. Затем мы создаем два экземпляра таблицы матриц (m1 и m2). Мы ограничиваем m1 в предложении WHERE, чтобы он представлял матрицу A, и мы ограничиваем m2, чтобы он представлял матрицу B. элементы каждой матрицы теперь сопоставлены, и оператор плюс или минус в предложении SELECT вычисляет сумму или разницу

# ## 3.19 Перемножения Матриц

sql = 'select * from Matrices'
select(sql)

sql = """
Exec Matrices_Print 'A'
"""
select(sql)

# Существует три способа умножения матрицы:  
# - Скалярное значение  
# - Вектор значений  
# - Другой матрицей  
#
# При умножении на вектор длина вектора должна соответствовать максимальному индексу X. При умножении двух матриц матрицы должны быть равными.

# ### 3.19.2.1 Умножение матрицы на скалярное значение

sql = """
SELECT DISTINCT X, Y ,Value * 5 Value
FROM Matrices
WHERE Matrix='A'
"""
select(sql)

# ### 3.19.2.2 Умножения матрицы с вектором

# Умножение матрицы на вектор несколько сложнее. В нашем примере, если мы запишем матрицу A и векторы вместе, мы получим следующее:

# + active=""
# Matrix A:   6   3
#             4   7
#
# Vector S:   5   6
# -

# Алгебраические правила утверждают, что первый векторный элемент умножает значения в первом столбце матрицы, второй векторный элемент умножает значения во втором столбце матрицы и т. д. Это дает нам следующую матрицу значений:

# + active=""
# 6x5   3x6
# 4x5   7x6
# -

# Последним шагом является суммирование всех значений в каждой строке этой матрицы, поэтому результатом является вектор:

# + active=""
# 6x5 + 3x6 = 30 + 18 = 48
# 4x5 + 7x6 = 20 + 42 = 62
# -

sql = """
SELECT m1.X, SUM(m1.Value * v.Value) VALUE
FROM Matrices m1, Matrices v
WHERE m1.Matrix='A' AND v.Matrix='S' AND m1.Y=v.X 
GROUP BY m1.X
"""
select(sql)

# ### 3.19.2.3 Умножение двух матриц

# Запрос на умножение двух матриц использует тот же принцип, что и запрос на умножение матрицы на вектор. Запрос сопоставляет элементы в соответствии с их положением, выполняет умножение и суммирует результаты этих умножения так, чтобы результат был вектором. В нашем примере следующие две матрицы умножаются вместе:

sql = """
Exec Matrices_Print 'A'
"""
select(sql)

sql = """
Exec Matrices_Print 'B'
"""
select(sql)

# + active=""
# Matrix A      Matrix B
#    6   3         6   3
#    4   7         5   2
# -

# Когда мы говорим, что при умножении матрицы вы "перекрестно сопоставляете" элементы,мы имеем в виду,что значения X, Y из одной матрицы умножаются на соответствующие значения Y, X из другой. Например, элемент 1,2 из матрицы A должен быть умножен на элемент 2,1 из Матрицы B. В нашем примере это перекрестное сопоставление дает следующие умножения:

# + active=""
# 6*6   3*5
# 4*6   7*5
# 6*3   3*2
# 4*3   7*2
# -

# Затем результаты должны быть суммированы в вектор:

# + active=""
# 6*6 + 3*5 = 36 + 15 = 51
# 4*6 + 7*5 = 24 + 35 = 59
# 6*3 + 3*2 = 18 +  6 = 24
# 4*3 + 7*2 = 12 + 14 = 26
# -

# Чтобы умножить матрицу A на матрицу B, используйте следующий код:

sql = """
WITH
res AS (
    SELECT m1.X, m2.Y, SUM(m1.Value*m2.Value) Value
    FROM Matrices m1, Matrices m2
    WHERE m1.Matrix='A' AND m2.Matrix='B' AND m1.Y=m2.X
    GROUP BY m1.X, m2.Y
)
select * from res
"""
select(sql)

sql = """
WITH
res AS (
    SELECT m1.X, m2.Y, SUM(m1.Value*m2.Value) Value
    FROM Matrices m1, Matrices m2
    WHERE m1.Matrix='A' AND m2.Matrix='B' AND m1.Y=m2.X
    GROUP BY m1.X, m2.Y
)
SELECT  X, 
   MAX(CASE Y WHEN 1 THEN Value END) y1,
   MAX(CASE Y WHEN 2 THEN Value END) y2,
   MAX(CASE Y WHEN 3 THEN Value END) y3
FROM res
GROUP BY X
ORDER BY X
"""
select(sql)

# ### 3.19.3.4 Квадратура матрицы

sql = """
Exec Matrices_Print 'A'
"""
select(sql)

# Запрос умножения матрицы можно легко изменить на квадрат матрицы. Квадратировать матрицу-значит умножать ее на саму себя. Единственное, что нужно изменить, - это то, что m1 и m2 должны быть ограничены одной и той же матрицей. В следующем примере M1 и m2 представляют матрицу A:

sql = """
SELECT m1.X, m2.Y, SUM(m1.Value*m2.Value) Value
FROM Matrices m1, Matrices m2
WHERE m1.Matrix='A' AND m2.Matrix='A' AND m1.Y=m2.X
GROUP BY m1.X, m2.Y
"""
select(sql)

# ---

conn.close()


