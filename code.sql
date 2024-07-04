-- Creating Tables & Importing Data ------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE order_item
(
id numeric primary key,
order_id numeric,
user_id numeric,
product_id numeric ,
inventory_item_id numeric,
status varchar,
created_at  timestamp,
shipped_at timestamp,
delivered_at timestamp,
returned_at timestamp,
sale_price numeric
);

CREATE TABLE products
(
id numeric primary key,
cost numeric,
category varchar,
name varchar,
brand varchar,
retail_price numeric,
department varchar,
sku varchar,
distribution_center_id numeric
);

CREATE TABLE users
(
id numeric primary key,
first_name varchar,
last_name varchar,
email varchar,
age numeric, 
gender varchar,
state varchar,
street_address varchar,
postal_code varchar,
city varchar,
country varchar,
latitude numeric,
longitude numeric,
traffic_source varchar,
created_at timestamp
);


-- Cleaning & Structuring Data -----------------------------------------------------------------------------------------------------------------------------------

-- cleaning
-- 0 values IS NULL
select * from order_item
where id is NULL


select * from products
where id IS NULL 

select * from users
where id IS NULL

-- 0 Duplicate Value
SELECT * FROM (
select  *,
        ROW_NUMBER() OVER(
                          PARTITION BY order_id, user_id, product_id, inventory_item_id
                        ) as stt
from order_item
) as tablet
WHERE stt>1;

SELECT * FROM (
select  *,
        ROW_NUMBER() OVER(
                          PARTITION BY id, cost, category, name
                        ) as stt
from products
) as tablet
WHERE stt>1;

SELECT * FROM (
select  *,
        ROW_NUMBER() OVER(
                          PARTITION BY id
                        ) as stt
from users
) as tablet
WHERE stt>1;

-- structuring
CREATE TABLE customers AS (
SELECT * FROM users
WHERE id IN (
	SELECT 	user_id
	FROM order_item
	WHERE created_at BETWEEN '2023-01-01' AND '2023-12-31' AND status NOT IN ('Cancelled', 'Returned')
	GROUP BY user_id
	ORDER BY user_id )
)

	
-- Analyzing -----------------------------------------------------------------------------------------------------------------------------------------------------

/* Exploratory Data Analysis */ ----------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------- CUSTOMER ----------------------------------------------------------------------------------------------------

WITH step_1 AS (
SELECT 	user_id, MAX(created_at) as latest_date, 
		('2023-12-31' - MAX(created_at)) as date_diff
FROM order_item
WHERE created_at BETWEEN '2023-01-01' AND '2023-12-31' AND status NOT IN ('Cancelled', 'Returned')
GROUP BY user_id
ORDER BY user_id
),
step_2 AS (
SELECT	*,
		CASE
			WHEN EXTRACT(DAY FROM date_diff) > 90 THEN 'churn'
			ELSE 'normal'
		END as cus_category
FROM step_1
)
-- , step_3 AS (
SELECT a.*, b.cus_category
FROM customers as a
INNER JOIN step_2 as b
	ON a.id = b.user_id
-- )

-- country
SELECT country, COUNT(id) as number
FROM step_3
WHERE cus_category = 'churn'
GROUP BY country
ORDER BY number DESC
LIMIT 5
-- country, city
SELECT country, city, COUNT(id) as number
FROM step_3
WHERE cus_category = 'churn'
GROUP BY country, city
ORDER BY number DESC
-- gender
SELECT gender, COUNT(id) as number
FROM step_3
WHERE cus_category = 'churn'
GROUP BY gender
ORDER BY number DESC
--- traffic_source
, number_churn AS (
SELECT traffic_source, COUNT(id) as number_churn
FROM step_3
WHERE cus_category = 'churn'
GROUP BY traffic_source
)
, number_all AS (
SELECT traffic_source, COUNT(id) as number_all
FROM step_3
GROUP BY traffic_source
ORDER BY number_all DESC
)
SELECT 	a.*, b.number_churn,
		ROUND(1.00 * b.number_churn / a.number_all , 2) as churn_perc
FROM number_all AS a
INNER JOIN number_churn AS b
	ON a.traffic_source = b.traffic_source
ORDER BY churn_perc DESC
-- age_group
, age_group AS (
SELECT *, CASE
			WHEN age <= 15 THEN 'children'
			WHEN age <= 24 THEN 'youth'
			WHEN age <= 64 THEN 'adult'
			ELSE 'senior'
		END as age_group
FROM step_3
)
, number_churn AS (
SELECT age_group, COUNT(id) as number_churn
FROM age_group
WHERE cus_category = 'churn'
GROUP BY age_group
)
, number_all AS (
SELECT age_group, COUNT(id) as number_all
FROM age_group
GROUP BY age_group
ORDER BY number_all DESC
)

SELECT 	a.*, b.number_churn,
		ROUND(1.00 * b.number_churn / a.number_all , 2) as churn_perc
FROM number_all AS a
INNER JOIN number_churn AS b
	ON a.age_group = b.age_group
ORDER BY churn_perc DESC

-------------------------------------------------- PRODUCT ----------------------------------------------------------------------------------------------------
-- step 1 - 2 = CUSTOMER
, step_3 AS (
SELECT a.*, b.cus_category, c.category, c.name as product_name, c.brand
FROM order_item as a
LEFT JOIN products as c
	ON c.id = a.product_id
INNER JOIN step_2 as b
	ON a.id = b.user_id
)
-- category
, number_churn AS (
SELECT category, COUNT(id) as number_churn
FROM step_3
WHERE cus_category = 'churn'
GROUP BY category
)
, number_all AS (
SELECT category, COUNT(id) as number_all
FROM step_3
GROUP BY category
ORDER BY number_all DESC
)
SELECT 	a.*, b.number_churn,
		ROUND(1.00 * b.number_churn / a.number_all , 2) as churn_perc
FROM number_all AS a
INNER JOIN number_churn AS b
	ON a.category = b.category
ORDER BY churn_perc DESC
-- brand
, number_churn AS (
SELECT brand, COUNT(id) as number_churn
FROM step_3
WHERE cus_category = 'churn'
GROUP BY brand
)
, number_all AS (
SELECT brand, COUNT(id) as number_all
FROM step_3
GROUP BY brand
ORDER BY number_all DESC
)
, brand_analysis AS (
SELECT 	a.*, b.number_churn,
		ROUND(1.00 * b.number_churn / a.number_all , 2) as churn_perc
FROM number_all AS a
INNER JOIN number_churn AS b
	ON a.brand = b.brand
ORDER BY churn_perc DESC
)

SELECT churn_perc, COUNT(brand) as n_brand
FROM brand_analysis
GROUP BY churn_perc
ORDER BY churn_perc DESC
-- product
, number_churn AS (
SELECT product_name, COUNT(id) as number_churn
FROM step_3
WHERE cus_category = 'churn'
GROUP BY product_name
)
, number_all AS (
SELECT product_name, COUNT(id) as number_all
FROM step_3
GROUP BY product_name
ORDER BY number_all DESC
)
, product_analysis AS (
SELECT 	a.*, b.number_churn,
		ROUND(1.00 * b.number_churn / a.number_all , 2) as churn_perc
FROM number_all AS a
INNER JOIN number_churn AS b
	ON a.product_name = b.product_name
ORDER BY churn_perc DESC
)

SELECT churn_perc, COUNT(product_name) as n_product
FROM product_analysis
GROUP BY churn_perc
ORDER BY churn_perc DESC
-- n_product by amount of time that product is bought
-- => mostly by 1 time | properly for testing
, number_all AS (
SELECT product_name, COUNT(id) as number_all
FROM step_3
GROUP BY product_name
ORDER BY number_all DESC
)
SELECT number_all, COUNT(product_name) as n_product
FROM number_all
GROUP BY number_all
ORDER BY number_all

-- price group
, price_group AS (
SELECT 	*,
		CASE
			WHEN sale_price <= 100 THEN '0-100$'
			WHEN sale_price <= 300 THEN '101-300$'
			WHEN sale_price <= 700 THEN '301-700$'
			ELSE '701-1000$'
		END as price_group
FROM step_3
)
, number_churn AS (
SELECT price_group, COUNT(id) as number_churn
FROM price_group
WHERE cus_category = 'churn'
GROUP BY price_group
)
, number_all AS (
SELECT price_group, COUNT(id) as number_all
FROM price_group
GROUP BY price_group
ORDER BY number_all DESC
)

SELECT 	a.*, b.number_churn,
		ROUND(1.00 * b.number_churn / a.number_all , 2) as churn_perc
FROM number_all AS a
INNER JOIN number_churn AS b
	ON a.price_group = b.price_group
ORDER BY churn_perc DESC
	
	
/* Cohort Analysis */ --------------------------------------------------------------------------------------------------------------------------------------------
-- step_1: find the first purchased date + selecting needed data
WITH B_1 AS(
SELECT *
FROM (
SELECT  created_at,
        MIN(created_at) OVER(PARTITION BY user_id) as first_date,
        user_id,
        sale_price
FROM order_item
WHERE status NOT IN ('Cancelled', 'Returned') ) as B1_1
WHERE first_date BETWEEN '2023-01-01' AND '2023-12-31'
)

-- step_2: monthly difference from the first purchase time (index column)
, B_2 AS(
SELECT  TO_CHAR(first_date, 'yyyy-mm') as cohort_date,
        (EXTRACT(YEAR FROM created_at) - EXTRACT(YEAR FROM first_date))*12
        + (EXTRACT(MONTH FROM created_at) - EXTRACT(MONTH FROM first_date)) +1 as index,
        user_id,
        sale_price
FROM B_1
WHERE created_at BETWEEN '2023-01-01' AND '2023-12-31'
)

-- step_3: total revenue and total customer 
-- group by first time purchasing (cohort_date) and index
-- where index <=12
, B_3 AS(
SELECT  cohort_date, index,
        SUM(sale_price) as revenue,
        COUNT(DISTINCT user_id) as customer
FROM B_2
where index <=12
GROUP BY cohort_date, index
ORDER BY cohort_date, index
)

-- step_4: Cohort Chart = Pivot CASE-WHEN
SELECT  cohort_date,
        SUM(CASE WHEN index = 1 then customer ELSE 0 END) as t1,
        SUM(CASE WHEN index = 2 then customer ELSE 0 END) as t2,
        SUM(CASE WHEN index = 3 then customer ELSE 0 END) as t3,
        SUM(CASE WHEN index = 4 then customer ELSE 0 END) as t4,
	SUM(CASE WHEN index = 5 then customer ELSE 0 END) as t5,
        SUM(CASE WHEN index = 6 then customer ELSE 0 END) as t6,
        SUM(CASE WHEN index = 7 then customer ELSE 0 END) as t7,
        SUM(CASE WHEN index = 8 then customer ELSE 0 END) as t8,
	SUM(CASE WHEN index = 9 then customer ELSE 0 END) as t9,
        SUM(CASE WHEN index = 10 then customer ELSE 0 END) as t10,
        SUM(CASE WHEN index = 11 then customer ELSE 0 END) as t11,
        SUM(CASE WHEN index = 12 then customer ELSE 0 END) as t12
FROM B_3
GROUP BY cohort_date
ORDER BY cohort_date

-- Retention Cohort 
SELECT  cohort_date,
        ROUND(100.00* t1 / t1 ,2) as t1,
        ROUND(100.00* t2 / t1 ,2) as t2,
        ROUND(100.00* t3 / t1 ,2) as t3,
        ROUND(100.00* t4 / t1 ,2) as t4,
	ROUND(100.00* t5 / t1 ,2) as t5,
        ROUND(100.00* t6 / t1 ,2) as t6,
        ROUND(100.00* t7 / t1 ,2) as t7,
        ROUND(100.00* t8 / t1 ,2) as t8,
	ROUND(100.00* t9 / t1 ,2) as t9,
        ROUND(100.00* t10 / t1 ,2) as t10,
        ROUND(100.00* t11 / t1 ,2) as t11,
        ROUND(100.00* t12 / t1 ,2) as t12
FROM B_4

-- Churn Cohort
SELECT  cohort_date,
        ROUND(100 - 100.00* t1 / t1 ,2) as t1,
        ROUND(100 - 100.00* t2 / t1 ,2) as t2,
        ROUND(100 - 100.00* t3 / t1 ,2) as t3,
        ROUND(100 - 100.00* t4 / t1 ,2) as t4,
	ROUND(100 - 100.00* t5 / t1 ,2) as t5,
        ROUND(100 - 100.00* t6 / t1 ,2) as t6,
        ROUND(100 - 100.00* t7 / t1 ,2) as t7,
        ROUND(100 - 100.00* t8 / t1 ,2) as t8,
	ROUND(100 - 100.00* t9 / t1 ,2) as t9,
        ROUND(100 - 100.00* t10 / t1 ,2) as t10,
        ROUND(100 - 100.00* t11 / t1 ,2) as t11,
        ROUND(100 - 100.00* t12 / t1 ,2) as t12
FROM B_4
