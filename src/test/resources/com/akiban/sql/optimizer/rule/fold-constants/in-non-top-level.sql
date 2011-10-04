SELECT name FROM customers
 WHERE name = 'Smith' OR cid IN (1,F(null),2)