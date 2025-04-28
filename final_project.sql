Create Database Customers_transactions;

Update customer set Gender = null where Gender = '';
Update customer set Age = null where Age = '';
Alter table customer modify Age int null;

Create table TRANSACTIONS

(date_new Date,
Id_check Int,
ID_client int,
Count_products Decimal(10,3),
Sum_payment decimal(10,2));

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\transactions_info_converted.csv"
INTO TABLE TRANSACTIONS
FIELDS TERMINATED BY ','
LINES TERMINATED BY'\n'
IGNORE 1 ROWS;

SHOW variables LIKE 'secure_file_pric';

Select * from transactions;
Select * from customer;

#1. Клиенты с непрерывной историей за год (без пропусков в месяцах)

WITH months AS (
    SELECT DISTINCT EXTRACT(MONTH FROM date_new) AS month, EXTRACT(YEAR FROM date_new) AS year
    FROM transactions
    WHERE date_new >= '2015-06-01' AND date_new < '2016-06-01'
),
client_months AS (
    SELECT ID_client, EXTRACT(MONTH FROM date_new) AS month, EXTRACT(YEAR FROM date_new) AS year
    FROM transactions
    WHERE date_new >= '2015-06-01' AND date_new < '2016-06-01'
    GROUP BY ID_client, year, month
),
clients_with_full_history AS (
    SELECT ID_client
    FROM client_months
    GROUP BY ID_client
    HAVING COUNT(DISTINCT year || month) = (SELECT COUNT(*) FROM months)
)
SELECT * 
FROM customer
WHERE Id_client IN (SELECT ID_client FROM clients_with_full_history);


#2. Средний чек за период с 01.06.2015 по 01.06.2016

SELECT 
    ID_client, 
    AVG(Sum_payment) AS avg_check
FROM transactions
WHERE date_new >= '2015-06-01' AND date_new < '2016-06-01'
GROUP BY ID_client;

#3. Средняя сумма покупок за месяц для каждого клиента

SELECT 
    ID_client, 
    EXTRACT(MONTH FROM date_new) AS month,
    AVG(Sum_payment) AS avg_monthly_spend
FROM transactions
WHERE date_new >= '2015-06-01' AND date_new < '2016-06-01'
GROUP BY ID_client, EXTRACT(MONTH FROM date_new)
ORDER BY ID_client, month;

#4. Количество операций по клиенту за период с 01.06.2015 по 01.06.2016

SELECT 
    ID_client, 
    COUNT(Id_check) AS operations_count
FROM transactions
WHERE date_new >= '2015-06-01' AND date_new < '2016-06-01'
GROUP BY ID_client;

#5. Месячная аналитика

SELECT 
    EXTRACT(MONTH FROM date_new) AS month, 
    AVG(Sum_payment) AS avg_monthly_check
FROM transactions
WHERE date_new >= '2015-06-01' AND date_new < '2016-06-01'
GROUP BY EXTRACT(MONTH FROM date_new)
ORDER BY month;

#Среднее количество операций по месяцам

SELECT 
    EXTRACT(MONTH FROM date_new) AS month, 
    COUNT(Id_check) / COUNT(DISTINCT ID_client) AS avg_operations_count
FROM transactions
WHERE date_new >= '2015-06-01' AND date_new < '2016-06-01'
GROUP BY EXTRACT(MONTH FROM date_new)
ORDER BY month;

#Среднее количество клиентов, совершавших операции по месяцам

SELECT 
    EXTRACT(MONTH FROM date_new) AS month, 
    COUNT(DISTINCT ID_client) AS avg_clients_count
FROM transactions
WHERE date_new >= '2015-06-01' AND date_new < '2016-06-01'
GROUP BY EXTRACT(MONTH FROM date_new)
ORDER BY month;

#Доля операций за год и доля суммы по месяцам

WITH total_operations AS (
    SELECT COUNT(*) AS total_operations, SUM(Sum_payment) AS total_sum
    FROM transactions
    WHERE date_new >= '2015-06-01' AND date_new < '2016-06-01'
)
SELECT 
    EXTRACT(MONTH FROM date_new) AS month, 
    COUNT(Id_check) / (SELECT total_operations FROM total_operations) AS operations_share,
    SUM(Sum_payment) / (SELECT total_sum FROM total_operations) AS sum_share
FROM transactions
WHERE date_new >= '2015-06-01' AND date_new < '2016-06-01'
GROUP BY EXTRACT(MONTH FROM date_new)
ORDER BY month;

#6. Соотношение M/F/NA в каждом месяце с их долей затрат

SELECT 
    EXTRACT(MONTH FROM t.date_new) AS month, 
    c.Gender, 
    SUM(t.Sum_payment) AS gender_sum_payment,
    SUM(t.Sum_payment) / SUM(SUM(t.Sum_payment)) OVER(PARTITION BY EXTRACT(MONTH FROM t.date_new)) AS gender_share
FROM transactions t
JOIN customer c ON t.ID_client = c.Id_client
WHERE t.date_new >= '2015-06-01' AND t.date_new < '2016-06-01'
GROUP BY month, c.Gender
ORDER BY month, c.Gender;

#7. Возрастные группы клиентов с шагом 10 лет

SELECT 
    CASE 
        WHEN Age < 18 THEN '0-17'
        WHEN Age BETWEEN 18 AND 27 THEN '18-27'
        WHEN Age BETWEEN 28 AND 37 THEN '28-37'
        WHEN Age BETWEEN 38 AND 47 THEN '38-47'
        WHEN Age BETWEEN 48 AND 57 THEN '48-57'
        WHEN Age BETWEEN 58 AND 67 THEN '58-67'
        ELSE '68+' 
    END AS age_group,
    COUNT(*) AS client_count,
    SUM(Sum_payment) AS total_spent,
    COUNT(Id_check) AS total_operations
FROM transactions t
JOIN customer c ON t.ID_client = c.Id_client
WHERE t.date_new >= '2015-06-01' AND t.date_new < '2016-06-01'
GROUP BY age_group
ORDER BY age_group;

#8. Поквартальные показатели

SELECT 
    age_group,
    EXTRACT(QUARTER FROM t.date_new) AS quarter,
    AVG(t.Sum_payment) AS avg_quarterly_spend,
    COUNT(t.Id_check) / COUNT(DISTINCT t.ID_client) AS avg_operations_per_client
FROM transactions t
JOIN customer c ON t.ID_client = c.Id_client
WHERE t.date_new >= '2015-06-01' AND t.date_new < '2016-06-01'
GROUP BY age_group, quarter
ORDER BY age_group, quarter;
