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

# # НЕОБХОДИМЫ УТОЧНЕНИЯ В ДАННЫХ

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


# # Глава 4. Иерархии в SQL

cur = conn.cursor()
sql = """
drop table if exists Projects;
CREATE TABLE Projects(
   Name VARCHAR(20),
   Cost INTEGER,
   Parent INTEGER,
   VertexId INTEGER,
   Primary key(VertexId)
)

INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'New SW', 0, 0, 1)
INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'Specifications', 0, 1, 2)
INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'Interviews', 5, 2, 3)
INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'Drafts', 10, 2, 4)
INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'Consolidations', 2, 2, 5)
INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'Final document', 15, 2, 6)
INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'Presentation', 1, 6, 7)
INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'Prototype', 0, 1, 8)
INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'UI Design', 10, 8, 9)
INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'Calculations', 10, 8, 10)
INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'Correctness Testing', 3, 10, 11)
INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'Database', 10, 8, 12)
INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'Development', 30, 1, 13)
INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'UI Implementation', 10, 13, 14)
INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'Coding', 20, 13, 15)
INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'Initial testing', 40, 13, 16)
INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'Beta testing', 40, 1, 17)
INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'Final adjustments', 5, 17, 18)
INSERT [dbo].[Projects] ([Name], [Cost], [Parent], [VertexId]) VALUES (N'Production testing', 20, 1, 19)
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from Projects'
select(sql)

# ## 4.2 Создание иерархии 

# Необходимо создать систему для поддержания простой иерархии для торговой системы. Разрешения должны быть управляемыми в группах, поэтому набор разрешений может быть легко предоставлен трейдеру. Кроме того, ваше решение должно позволять динамически добавлять типы разрешений во время работы системы.

# В нашей таблице разрешений группы есть два атрибута, описывающие торговые разрешения для каждой группы продуктов. Один атрибут-это атрибут состояния. Статус каждого разрешения на торговлю может быть установлен в valid ('V') или suspended ('S'). Второй атрибут-это ограничение ордера, определяющее максимально допустимый размер сделки с участием группы продуктов. Если трейдер уполномочен торговать облигациями и имеет торговый лимит 1000, этот трейдер не может инициировать торговлю, включающую более 1000 облигаций. Ниже представлен типичный пример типа данных, которые можно хранить в этой таблице:

cur = conn.cursor()
sql = """
drop table if exists GroupPermissions;
CREATE TABLE GroupPermissions(
   GroupId VARCHAR(20) NOT NULL,
   ProductType VARCHAR(10) NOT NULL,
   Status CHAR(1) CHECK(status in ('V','S')) DEFAULT('V'),
   Limit NUMERIC(10,2) NULL, 
   PRIMARY KEY(GroupId,ProductType)
)

INSERT [dbo].[GroupPermissions] ([GroupId], [ProductType], [Status], [Limit]) VALUES 
    (N'Debt', N'Bill', N'V', CAST(10000.00 AS Numeric(10, 2)))
INSERT [dbo].[GroupPermissions] ([GroupId], [ProductType], [Status], [Limit]) VALUES 
    (N'Debt', N'Bond', N'V', CAST(10000.00 AS Numeric(10, 2)))
INSERT [dbo].[GroupPermissions] ([GroupId], [ProductType], [Status], [Limit]) VALUES 
    (N'Derivatives', N'Future', N'V', CAST(200.00 AS Numeric(10, 2)))
INSERT [dbo].[GroupPermissions] ([GroupId], [ProductType], [Status], [Limit]) VALUES 
    (N'Derivatives', N'Option', N'V', CAST(100.00 AS Numeric(10, 2)))
INSERT [dbo].[GroupPermissions] ([GroupId], [ProductType], [Status], [Limit]) VALUES 
    (N'Equities', N'Share', N'V', CAST(1000.00 AS Numeric(10, 2)))
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from GroupPermissions'
select(sql)

# Вторая таблица должна отслеживать группы разрешений, назначенные каждому трейдеру. Его определение может быть следующим::

cur = conn.cursor()
sql = """
drop table if exists GroupMembership;
CREATE TABLE GroupMembership(
   AccountId VARCHAR(20) NOT NULL,
   GroupId VARCHAR(20) NOT NULL
   PRIMARY KEY(AccountId,GroupId)
)
INSERT [dbo].[GroupMembership] ([AccountId], [GroupId]) VALUES (N'Alex0001', N'Debt')
INSERT [dbo].[GroupMembership] ([AccountId], [GroupId]) VALUES (N'Alex0001', N'Derivatives')
INSERT [dbo].[GroupMembership] ([AccountId], [GroupId]) VALUES (N'Alex0001', N'Equities')
INSERT [dbo].[GroupMembership] ([AccountId], [GroupId]) VALUES (N'Betty0002', N'Derivatives')
INSERT [dbo].[GroupMembership] ([AccountId], [GroupId]) VALUES (N'Charles0003', N'Debt')
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from GroupMembership'
select(sql)

# ### Discussion

# Основная цель нашего решения-отделить определение разрешений и групп от назначения групп трейдерам. Обратите внимание, что таблица разрешений группы не полностью нормализована. Чтобы нормализовать его, вам нужно будет отделить определение групп от определения продуктов, а затем использовать таблицу пересечений для определения атрибутов Limit и Status. Отсутствие нормализации здесь несколько упрощает пример, но это не влияет на общее поведение нашего дизайна и не влияет на запросы, которые вы увидите в этом разделе.

# #### 4.2.3.1 Проверка разрешений для трейдера

sql = """
SELECT m.AccountId, g.ProductType, MIN(g.Limit) Limit
FROM GroupMembership m 
JOIN GroupPermissions g ON m.groupId=g.groupId
WHERE  Status='V' AND AccountId='Alex0001'
GROUP BY m.AccountId, g.ProductType
"""
select(sql)

# Хотя результаты этого запроса просты, использование предложения GROUP BY вместе с агрегатной функцией MIN заслуживает некоторого дополнительного объяснения. Группировка позволяет нам иметь дело со случаями, когда несколько групп разрешений трейдера определяют лимит для одного и того же продукта. В нашем примере как долговые, так и фондовые группы определяют торговый лимит для облигаций. Alex0001 является членом обеих групп, поэтому не сгруппированный запрос возвращает следующие две записи:

# + active=""
# AccountId            ProductType Limit        
# -------------------- ----------- ------------ 
# Alex0001             Bond        10000.00
# Alex0001             Bond        2000.00
# -

# При двух ограничениях для одного и того же продукта вопрос заключается в том, какой предел имеет приоритет. В нашем примере запроса мы использовали функцию MIN для нижнего предела, чтобы иметь приоритет над более высоким пределом. Если вы хотите, чтобы более высокий предел имел приоритет, вы бы сделали это, просто используя MAX вместо MIN. Предложение GROUP BY является требованием при использовании агрегатных функций таким образом и гарантирует, что для каждого продукта, которым трейдер уполномочен торговать, в конечном итоге возвращается только одно разрешение.

# #### 4.2.3.2 Отмена разрешений для группы

cur = conn.cursor()
sql = """
UPDATE GroupPermissions SET Status='S' WHERE GroupId='Debt'
"""
cur.execute(sql)
conn.commit()
cur.close()

sql = 'select * from GroupPermissions'
select(sql)

# ## 4.3 Изменение Индивидуальных Разрешений

# Решение двоякое. Во-первых, создайте дополнительную таблицу для записи исключительных разрешений, предоставляемых непосредственно трейдеру. Во-вторых, измените запрос, используемый для получения разрешений, действительных в данный момент для данного трейдера. Дополнительная таблица может выглядеть как следующая таблица разрешений учетной записи:

cur = conn.cursor()
sql = """
drop table if exists AccountPermissions;
CREATE TABLE AccountPermissions(
   AccountId VARCHAR(20) NOT NULL,
   ProductType VARCHAR(20) NOT NULL,
   Status CHAR(1) CHECK(Status in ('V','S')) DEFAULT('V'), 
   Limit NUMERIC(10,2) NULL,
   PRIMARY KEY(AccountId, ProductType)
)

INSERT [dbo].[AccountPermissions] ([AccountId], [ProductType], [Status], [Limit]) VALUES 
    (N'Alex0001', N'Share', N'V', CAST(5000.00 AS Numeric(10, 2)))
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from AccountPermissions'
select(sql)

# С помощью таблицы разрешения учетной записи необходимо изменить запрос, используемый для получения разрешений для данного пользователя. Этот запрос теперь должен учитывать эту новую таблицу. Запрос в следующем примере, который возвращает разрешения для Alex0001, сделает это:

sql = """
SELECT m.AccountId, g.ProductType, 
    CASE 
        WHEN isnull(MIN(a.Limit),0) > MIN(g.Limit) 
            THEN MIN(a.Limit) 
        ELSE MIN(g.Limit) 
    END Limit
FROM GroupMembership m 
JOIN GroupPermissions g ON (m.GroupId=g.GroupId)
LEFT OUTER JOIN AccountPermissions a ON (m.AccountId=a.AccountId AND g.ProductType=a.ProductType)
WHERE  m.AccountId='Alex0001' 
GROUP BY m.AccountId, g.ProductType
HAVING MIN(g.status)='V' AND isnull(MIN(a.status),MIN(g.status))='V'
"""
select(sql)

# ### Discussion

# Идея модели разрешений, описанной в этой главе, заключается в том, чтобы по возможности устанавливать разрешения через членство в группах. Однако на практике вы часто обнаружите, что полезно иметь средство настройки, которое позволяет напрямую изменять конкретное разрешение для конкретного пользователя. С этой целью мы создали дополнительную таблицу с именем разрешения учетной записи, в которой записываются такие исключения. Наш новый запрос соединяет таблицу разрешений группы с таблицей членства в группе для возврата разрешений, установленных на уровне группы. Затем эти результаты объединяются с таблицей разрешений учетной записи, которая добавляет исключения для конкретной учетной записи в результирующий набор. Внешнее соединение используется для этой третьей таблицы, чтобы сделать исключения для конкретной учетной записи необязательными.

# Предложение GROUP BY снова используется в этой новой версии запроса по той же причине, что и раньше — мы хотим вернуть только одно разрешение для каждого типа продукта. Однако есть одно отличие: на этот раз функция GROUP BY включает в свой список столбцов больше столбцов:

# + active=""
# GROUP BY m.AccountId, g.ProductType
# -

# Оператор CASE, который вы видите в запросе, используется для определения значения, которое следует принять, если присутствуют индивидуальные и групповые разрешения для одной учетной записи и продукта. Он проверяет оба значения и сообщает только один:

# + active=""
# CASE WHEN isnull(MIN(a.Limit),0) > MIN(g.Limit) 
#       THEN MIN(a.Limit) 
#       ELSE MIN(g.Limit) 
#    END Limit
# -

# В нашем случае наша политика авторизации заключается в том, что разрешения для конкретной учетной записи имеют приоритет над разрешениями, предоставляемыми на уровне группы, только если ограничение для конкретной учетной записи больше ограничения для конкретной группы. Функция isnull () заботится о случаях, когда отдельные разрешения не установлены. Он делает это, предоставляя нулевое значение для этих случаев. Использование оператора CASE, как это очень гибкий подход, потому что вы можете легко реализовать различные политики авторизации. Было бы тривиально, например, изменить оператор CASE таким образом, чтобы разрешения для конкретной учетной записи всегда имели приоритет над разрешениями на уровне группы, независимо от того, указано ли разрешение для конкретной учетной записи более высокое или более низкое ограничение.

# Решение, показанное в этом рецепте, очень полезно, когда необходимо сделать исключения для существующих разрешений, установленных на уровне группы. Однако у него есть одна существенная проблема: вы не можете определить разрешение для конкретного счета для трейдера в случаях, когда разрешение на тот же продукт также не было предоставлено на уровне группы.

# ### 4.4 Добавление Новых Индивидуальных Разрешений

# Решение предыдущего рецепта позволяет определить отдельные исключения из разрешений, которые трейдер уже имеет на уровне группы. Однако вы хотите предоставить разрешения конкретным трейдерам независимо от того, были ли предоставлены разрешения для тех же продуктов на уровне группы.

# В предыдущем рецепте можно было определить исключение из разрешения, которое трейдер уже получил на уровне группы. Например, можно создать следующие две строки в учетной записи :

# + active=""
# AccountId            ProductType          Status Limit        
# -------------------- -------------------- ------ ------------ 
# Alex0001             Share                V      5000.00
# Betty0002            Share                V      8000.00
# -

# Запрос в предыдущем рецепте, однако, будет уважать эти разрешения для конкретных учетных записей только в тех случаях, когда трейдеры, о которых идет речь, также получили разрешение торговать акциями на уровне группы. Это ограничение, если вы хотите назвать его так, возникает в результате трехстороннего соединения, используемого в запросе.

# Вы можете пожелать, чтобы разрешения для конкретной учетной записи вступали в силу все время. Для этого можно использовать ту же модель данных, что и в предыдущем рецепте, но с другим запросом. Запрос, показанный в следующем примере, является запросом объединения и правильно возвращает разрешения на уровне группы и учетной записи для Betty0002:

sql = """
SELECT m.AccountId, g.ProductType, MIN(g.Limit) Limit
FROM GroupMembership m 
JOIN GroupPermissions g ON m.groupId=g.groupId 
WHERE Status='V' AND AccountId='Betty0002' 
   AND NOT EXISTS(
        SELECT * FROM AccountPermissions  a
        WHERE m.AccountId=a.AccountId AND g.ProductType=a.ProductType
      ) 
GROUP BY m.AccountId, g.ProductType
UNION
SELECT a.AccountId, a.ProductType, a.Limit 
FROM AccountPermissions a
WHERE a.AccountId='Betty0002' AND a.Status='V'
"""
select(sql)

# ### Discussion

# Ключ на этот запрос-это запрос на объединение. Первый запрос в объединении сообщает обо всех разрешениях, определенных только на уровне группы. Вложенный запрос в предложении WHERE этого запроса гарантирует, что разрешения группового уровня для продуктов исключаются, если для этих же продуктов существуют разрешения для конкретных учетных записей. Затем второй запрос в объединении возвращает разрешения для конкретной учетной записи. Результаты двух запросов объединяются в результате предложения UNION.

# Есть два недостатка в решении, показанном в этом рецепте. Один из них заключается в том, что это решение менее эффективно, чем показано в предыдущем рецепте. Это связано с тем, что есть три оператора SELECT вместо одного. Еще один недостаток заключается в том, что это решение является негибким с точки зрения разрешения на использование, когда разрешение на один и тот же продукт предоставляется как на уровне группы, так и на уровне учетной записи. В таких случаях разрешение для конкретной учетной записи всегда имеет приоритет над разрешением на уровне группы. Чтобы преодолеть это последнее ограничение, необходимо создать более общий запрос объединения, включающий разрешения на уровне группы и учетной записи, внедрить этот запрос в представление, а затем управлять представлением с помощью соответствующего запроса. Следующая инструкция создает такое представление:

cur = conn.cursor()
sql = """
drop view if exists Permissions;
"""
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = """
CREATE VIEW Permissions 
AS
SELECT m.AccountId, g.ProductType, MIN(g.Limit) Limit
   FROM GroupMembership m JOIN GroupPermissions g
      ON m.groupId=g.groupId 
   WHERE Status='V' 
   GROUP BY m.AccountId, g.ProductType
UNION
   SELECT a.AccountId, a.ProductType,a.Limit 
   FROM AccountPermissions a
   WHERE  a.Status='V'
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from Permissions'
select(sql)

# Это представление возвращает разрешения группы, развернутые для каждой учетной записи, а также все существующие разрешения для конкретной учетной записи. Чтобы перечислить все разрешения для конкретной учетной записи, запросите представление и примените политику интерпретации в запросе. Например:

sql = """
SELECT ProductType
    , MIN(Limit) Limit 
FROM permissions 
WHERE AccountId='Alex0001'
GROUP BY ProductType
"""
select(sql)

# В этом запросе перечислены разрешения для Alex0001. Функция MIN разрешает случаи, когда для одного типа продукта Существует несколько разрешений. Когда такие случаи происходят, мин гарантирует, что возвращается только самый низкий ограничение. Чтобы всегда возвращать максимально допустимый предел, можно использовать функцию MAX.

# ### 4.5 Централизация Логики Авторизации

# Вы хотите централизовать логику политики авторизации. Вы хотите иметь возможность запрашивать разрешения отдельного трейдера, но не хотите, чтобы этот запрос содержал логику, которая обрабатывает конфликтующие разрешения и отдельные исключения. Ваша цель - внести разумные изменения в политику разрешений и модель без принудительного изменения запросов, уже запущенных в производственных программах.

cur = conn.cursor()
sql = """
drop view if exists orderAuthorization;
"""
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = """
CREATE VIEW orderAuthorization 
AS
SELECT AccountId, ProductType
    , MIN(Limit) Limit
FROM GroupMembership m JOIN GroupPermissions g
   ON m.groupId=g.groupId 
WHERE Status='V'
GROUP BY AccountId, ProductType
"""
cur.execute(sql)
conn.commit()
cur.close()
sql = 'select * from orderAuthorization'
select(sql)

# С помощью этого представления теперь можно получить разрешения для конкретного трейдера, выполнив простой запрос, как показано в следующем примере:

sql = """
SELECT AccountId, ProductType, Limit
FROM OrderAuthorization
WHERE AccountId = 'Alex0001'
"""
select(sql)

# Можно также выполнить запрос к этому представлению, чтобы проверить наличие определенного разрешения. Например, предположим, что в систему поступает приказ торговать 3000 акций на счет Alex0001. Можно использовать простой запрос, показанный в следующем примере, чтобы получить максимальное ограничение для этого типа заказа:

sql = """
SELECT Limit 
FROM orderAuthorization 
WHERE AccountId='Alex0001' AND productType='Share'
"""
select(sql)

# ### 4.6 Реализация Общих Иерархий

sql = 'select * from Projects'
select(sql)

# #### 4.6.2.1 Перечислить все листовые узлы

# Листовые узлы-это узлы без детей. Подзапрос в следующей инструкции SELECT проверяет каждый узел, чтобы узнать, являются ли другие узлы его родительскими. Если нет, то этот узел является листовым узлом:

sql = """
SELECT Name 
FROM Projects p 
WHERE NOT EXISTS(
   SELECT * FROM Projects 
   WHERE Parent=p.VertexId
   )
"""
select(sql)

# #### 4.6.2.2  Список все узлы, которые не листовые узлы

sql = """
SELECT Name FROM Projects p 
WHERE EXISTS(
   SELECT * FROM Projects 
   WHERE Parent=p.VertexId
   )
"""
select(sql)

# #### 4.6.2.3 Поиск самых дорогих вершин

sql = """
SELECT TOP 5 Name, Cost 
FROM Projects 
ORDER BY cost DESC
"""
select(sql)

# #### 4.6.2.4 Найти непосредственных дочерних узлов

sql = """
SELECT Name FROM Projects 
WHERE Parent=(
   SELECT VertexId FROM Projects 
   WHERE Name='Specifications' 
   )
"""
select(sql)

# #### 4.6.2.5 Найти корень

sql = """
SELECT Name FROM Projects WHERE Parent=0
"""
select(sql)

# ### 4.7 Обход Иерархии Рекурсивно

cur = conn.cursor()
sql = """
drop PROCEDURE if exists TraverseProjectsRecursive;
"""
cur.execute(sql)
conn.commit()
cur.close()

cur = conn.cursor()
sql = """
CREATE PROCEDURE TraverseProjectsRecursive
@VertexId INTEGER
AS
   /* to change action on each vertex, change these lines */
   DECLARE @Name VARCHAR(20)
   SELECT @Name = (SELECT Name 
                    FROM Projects WHERE VertexId = @VertexId) 
   PRINT SPACE(@@NESTLEVEL * 2) + STR(@VertexId) + ' ' + @Name
   /* ****** */
   DECLARE subprojects CURSOR LOCAL FOR
      SELECT VertexId FROM Projects WHERE Parent = @VertexId      
   OPEN subprojects
      FETCH NEXT FROM subprojects INTO @VertexId
      WHILE @@FETCH_STATUS=0 BEGIN
         EXEC TraverseProjectsRecursive @VertexId
         FETCH NEXT FROM subprojects INTO @VertexId
      END
   CLOSE subprojects
   DEALLOCATE subprojects
"""
cur.execute(sql)
conn.commit()
cur.close()

# + active=""
# exec TraverseProjectsRecursive 1

# + active=""
#            1 New SW
#              2 Specifications
#                3 Interviews
#                4 Drafts
#                5 Consolidations
#                6 Final document
#                  7 Presentation
#              8 Prototype
#                9 UI Design
#               10 Calculations
#                 11 Correctness Testing
#               12 Database
#             13 Development
#               14 UI Implementation
#               15 Coding
#               16 Initial testing
#             17 Beta testing
#               18 Final adjustments
#             19 Production testing
# -

# #### Discussion

# В этом операторе печати мы используем переменную @@NEXTLEVEL. Эта переменная поддерживается SQL Server автоматически и возвращает текущий уровень вложенности. Эта информация является большим преимуществом при работе с хранимыми процедурами. В нашем случае мы хотим, чтобы отступ каждого проекта пропорционально его глубине. Для этого мы умножим уровень вложенности два отступа каждого ребенка два помещения под своего родителя.

# Это определение курсора использует локальное предложение, гарантируя, что курсор определен только для текущей процедуры. По умолчанию курсоры являются глобальными, что означает, что любой определенный курсор виден из всего кода, выполняемого в текущем соединении. Поскольку эта процедура вызывает себя повторно в контексте одного соединения, мы должны указать, что это определение курсора является локальным для каждого вызова процедуры.

# Несмотря на то, что рекурсивные механизмы очень элегантны, они не очень эффективны. Кроме того, в SQL Server они имеют ограничение только 32 уровня вложенности











# ---

conn.close()


