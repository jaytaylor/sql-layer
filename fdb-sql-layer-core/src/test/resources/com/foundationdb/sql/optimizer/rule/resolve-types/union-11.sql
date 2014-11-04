SELECT name, NULL AS quan FROM parent
UNION
SELECT name, NULL AS quan FROM customers
UNION
select sku AS name, quan FROM items