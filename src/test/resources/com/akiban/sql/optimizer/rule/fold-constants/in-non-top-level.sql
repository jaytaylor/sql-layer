SELECT name FROM customers
 WHERE name = 'Smith' OR cid IN (1,WEEKDAY(NULL),2)