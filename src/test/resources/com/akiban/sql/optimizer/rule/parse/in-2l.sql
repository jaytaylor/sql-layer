SELECT * FROM customers
 WHERE name IN (SELECT parent.name FROM parent ORDER BY 1 LIMIT 2)
