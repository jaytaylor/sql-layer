SELECT * FROM customers
 WHERE name IN (SELECT parent.name FROM parent)
