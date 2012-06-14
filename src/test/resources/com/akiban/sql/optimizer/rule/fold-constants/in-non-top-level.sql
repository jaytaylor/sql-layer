SELECT name FROM customers
 WHERE name = 'Smith' OR cid IN (1,LCASE(null),2)