SELECT * FROM customers
 WHERE name IN (SELECT MAX(child.name) FROM child GROUP BY pid)
