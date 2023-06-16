-- Databricks notebook source
-- Total Order 

select 
  count(distinct s.order_number) as TotalOrder
from adventure.sales s

-- COMMAND ----------

select 
  count(s.order_quantity) as TotalOrder
from adventure.sales s

-- COMMAND ----------

select 
  count(s.return_quantity) as TotalOrder
from adventure.returns s

-- COMMAND ----------



SELECT CONCAT('$', FORMAT_NUMBER(ROUND(SUM(s.order_quantity * p.product_price), 2), 2)) AS TotalRevenue
FROM adventure.sales s
INNER JOIN adventure.products p ON p.product_id = s.product_id



-- COMMAND ----------

SELECT CONCAT('$', FORMAT_NUMBER(ROUND(SUM(s.order_quantity * p.product_price)- SUM(s.order_quantity * p.product_cost), 2), 2)) AS TotalProfit
FROM adventure.sales s
INNER JOIN adventure.products p ON p.product_id = s.product_id

-- COMMAND ----------


SELECT 
  s.year,
  ROUND(SUM(s.order_quantity * p.product_price), 2) AS TotalRevenue,
  ROUND(SUM(s.order_quantity * p.product_price)- SUM(s.order_quantity * p.product_cost), 2) as TotalProfit
FROM adventure.sales s
INNER JOIN adventure.products p ON p.product_id = s.product_id
group by 
  s.year

-- COMMAND ----------

SELECT 
  pc.category_name,
  ROUND(SUM(s.order_quantity * p.product_price), 2) AS TotalRevenue,
  ROUND(SUM(s.order_quantity * p.product_price)- SUM(s.order_quantity * p.product_cost), 2) as TotalProfit
FROM adventure.sales s
INNER JOIN adventure.products p ON p.product_id = s.product_id
inner join adventure.product_sub_category ps on ps.product_sub_category_id = p.product_subcategory_id
inner join adventure.product_category pc on pc.product_category_id = ps.product_category_key
group by 
  pc.category_name

-- COMMAND ----------

use adventure;
SELECT
    p.product_name,
    COALESCE(s.total_orders, 0) AS total_orders,
    COALESCE(r.total_returns, 0) AS total_returns,
    round((COALESCE(r.total_returns, 0)/COALESCE(s.total_orders, 0))*100,2) as return_rate
FROM
    products AS p
LEFT JOIN
    (
        SELECT
            product_id,
            SUM(order_quantity) AS total_orders
        FROM
            sales
        GROUP BY
            product_id
    ) AS s ON p.product_id = s.product_id
LEFT JOIN
    (
        SELECT
            product_id,
            SUM(return_quantity) AS total_returns
        FROM
            returns
        GROUP BY
            product_id
    ) AS r ON p.product_id = r.product_id

order by return_rate desc
limit 10;


-- COMMAND ----------

SELECT
    DATE_TRUNC('month', order_date) AS month,
    SUM(order_line_item) AS total_orders
FROM
    sales
GROUP BY
    DATE_TRUNC('month', order_date)
ORDER BY
    month;


-- COMMAND ----------


