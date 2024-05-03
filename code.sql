-- Creating Tables & Importing Data
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
	
CREATE TABLE orders
(
order_id numeric primary key,
user_id numeric,
status varchar,
gender varchar,
created_at  timestamp,
returned_at timestamp,
shipped_at timestamp,
delivered_at timestamp,
num_of_item numeric
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


-- Cleaning & Structuring Data
-- 0 values IS NULL
select * from order_item
where id is NULL

select * from orders
where order_id is NULL

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
                          PARTITION BY order_id, user_id
                        ) as stt
from orders
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
	

-- Analyzing
/* Amount of Customers and Orders each months in 2023 */
-- Output: month_year ( yyyy-mm) , total_user, total_order
WITH B1 AS(
SELECT	TO_CHAR(created_at, 'yyyy-mm') as month_year,
	COUNT(order_id) as total_order,
	COUNT(DISTINCT user_id) as total_user
FROM orders
WHERE 	DATE(created_at) BETWEEN '2023-01-01' AND '2023-12-31'
	AND status  = 'Complete'
GROUP BY TO_CHAR(created_at, 'yyyy-mm')
)
SELECT	month_year,
	total_order,
	COALESCE(
		ROUND(100.00*(total_order - pre_order) / pre_order,2)
		, '0.00') as order_growth,
	total_user,
	COALESCE(
		ROUND(100.00*(total_user - pre_customer) / pre_order,2) 
		, '0.00') as customer_growth
FROM (
SELECT	month_year,
		total_order,
		LAG(total_order) OVER(ORDER BY month_year) as pre_order,
		total_user,
		LAG(total_user) OVER(ORDER BY month_year) as pre_customer
FROM B1) as B2

/* Average Order Value (AOV) and Monthly Active Customers 
- in 2023 */
-- Output: month_year ( yyyy-mm), distinct_users, average_order_value
WITH B1 AS(
SELECT	TO_CHAR(a.created_at, 'yyyy-mm') as month_year,
		ROUND(
		  AVG(b.sale_price)
		,2) as average_order_value,
	COUNT(DISTINCT a.user_id) as total_user
FROM orders as a
INNER JOIN order_item as b
	ON a.order_id = b.order_id
WHERE DATE(a.created_at) BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY TO_CHAR(a.created_at, 'yyyy-mm')
)
SELECT	month_year,
	average_order_value,
	COALESCE(
		ROUND(100.00*(average_order_value - pre_order) / pre_order,2)
		, '0.00') as value_growth,
	total_user,
	COALESCE(
		ROUND(100.00*(total_user - pre_customer) / pre_customer,2) 
		, '0.00') as customer_growth
FROM (
SELECT	month_year,
	average_order_value,
	LAG(average_order_value) OVER(ORDER BY month_year) as pre_order,
	total_user,
	LAG(total_user) OVER(ORDER BY month_year) as pre_customer
FROM B1) as B2

/* Customer Segmentation by Age: Identify the youngest and oldest customers 
for each gender in 2023 */
-- Output: full_name, gender, age, tag (youngest-oldest)
(SELECT	CONCAT(first_name, ' ', last_name) as full_name,
	gender,
	age,
	'youngest' as tag
FROM users as a
WHERE age IN (SELECT MIN(age) FROM users GROUP BY gender)
	AND DATE(a.created_at) BETWEEN '2023-01-01' AND '2023-12-31')
UNION ALL
(SELECT	CONCAT(first_name, ' ', last_name) as full_name,
	gender,
	age,
	'oldest' as tag
FROM users as a
WHERE age IN (SELECT MAX(age) FROM users GROUP BY gender)
	AND DATE(a.created_at) BETWEEN '2023-01-01' AND '2023-12-31')

-- analyzing the results
-- Create temporary table for the above result
CREATE TEMP TABLE customer_age
AS (
(SELECT	CONCAT(first_name, ' ', last_name) as full_name,
	gender,
	age,
	'youngest' as tag
FROM users as a
WHERE age IN (SELECT MIN(age) FROM users GROUP BY gender)
	AND DATE(a.created_at) BETWEEN '2023-01-01' AND '2023-12-31')
UNION ALL
(SELECT	CONCAT(first_name, ' ', last_name) as full_name,
	gender,
	age,
	'oldest' as tag
FROM users as a
WHERE age IN (SELECT MAX(age) FROM users GROUP BY gender)
	AND DATE(a.created_at) BETWEEN '2023-01-01' AND '2023-12-31')
)

-- analyzing	
SELECT DISTINCT tag, gender, age, 
		COUNT(full_name) OVER(PARTITION BY gender, tag)
FROM customer_age

	
/* Top 5 products with the highest profit each month (rank each product) */
-- Output: month_year ( yyyy-mm), product_id, product_name, 
-- sales, cost, profit, rank_per_month

-- B1: rank profit by month
WITH B_1 AS(
SELECT 	TO_CHAR(a.created_at, 'yyyy-mm') as month_year,
	a.product_id,
	b.name,
	ROUND(
		SUM(a.sale_price),2) as sales,
	ROUND(
		SUM(b.cost),2) as cost,
	ROUND(
		SUM(a.sale_price) - SUM(b.cost),2) as profit,
	DENSE_RANK() OVER(
			PARTITION BY TO_CHAR(a.created_at, 'yyyy-mm')
			ORDER BY (SUM(a.sale_price) - SUM(b.cost)) DESC) as rank_per_month
FROM order_item as a
INNER JOIN products as b
	on a.product_id = b.id
GROUP BY TO_CHAR(a.created_at, 'yyyy-mm'),a.product_id,b.name 
)
-- B2: CTEs B1 with WHERE rank<=5
SELECT * FROM B_1
WHERE rank_per_month <=5

/* Revenue for each category: total daily revenue for each product category 
over the past 3 months (assuming the current date is 15/4/2022) */
SELECT  b.category,
       	DATE(a.created_at) as dates,
        ROUND(SUM(a.sale_price),2) as profit
FROM order_item as a
INNER JOIN products as b
ON a.product_id = b.id
WHERE DATE(a.created_at) BETWEEN '2022-01-15' AND '2022-04-15'
GROUP BY DATE(a.created_at),b.category
ORDER BY b.category,dates


/* CREATING DATASET */
-- B1:
WITH B_1 AS (
SELECT  EXTRACT(MONTH FROM c.created_at) as  month,
        EXTRACT(YEAR FROM c.created_at) as  year,
        b.category,
        ROUND(
              SUM(a.sale_price),2) as TPV,
        COUNT(a.order_id) as TPO,
        ROUND(
              SUM(b.cost),2) as total_cost,
        ROUND(
              SUM(a.sale_price)-SUM(b.cost),2) as total_profit,
        ROUND(1.00*
              (SUM(a.sale_price)-SUM(b.cost))
              / SUM(b.cost)
              ,2) as profit_to_cost_ratio
FROM order_item as a
INNER JOIN products as b
	ON a.product_id = b.id
INNER JOIN orders as c
	ON a.order_id =c.order_id
GROUP BY year, month, b.category
ORDER BY year, month, b.category
)
-- B2: 
SELECT  month,
        year,
        category,
        TPV,
        TPO,
        COALESCE(
        ROUND(100.00*
                (TPV - prev_TPV) / prev_TPV
                ,2) || '%'
                ,'0.00%') as Revenue_growth,
        COALESCE(
        ROUND(100.00*
                (TPO - prev_TPO) / prev_TPV
                ,2) || '%' 
                ,'0.00%') as Order_growth,
        total_cost,
        total_profit,
        profit_to_cost_ratio
FROM (
SELECT  *,
        LAG(TPV) OVER(PARTITION BY category ORDER BY year,month) as prev_TPV,
        LAG(TPO) OVER(PARTITION BY category ORDER BY year,month) as prev_TPO
FROM B_1
) as tablet

/* Cohort Analysis */
-- B_1: find the first purchased date + selecting needed data
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

-- B_2: monthly difference from the first purchase time (index column)
, B_2 AS(
SELECT  TO_CHAR(first_date, 'yyyy-mm') as cohort_date,
        (EXTRACT(YEAR FROM created_at) - EXTRACT(YEAR FROM first_date))*12
        + (EXTRACT(MONTH FROM created_at) - EXTRACT(MONTH FROM first_date)) +1 as index,
        user_id,
        sale_price
FROM B_1
WHERE created_at BETWEEN '2023-01-01' AND '2023-12-31'
)

-- B2_3: total revenue and total customer 
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

-- B2_4: Cohort Chart = Pivot CASE-WHEN
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
