SELECT cid FROM customers
 WHERE name IN (SELECT DISTINCT c2.name FROM customers c2
                 INNER JOIN parent ON c2.cid = parent.id 
                 WHERE parent.id > 10)