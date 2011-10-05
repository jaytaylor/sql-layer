SELECT UPPER(name) AS uname, COUNT(*) AS cvalue 
  FROM customers, orders
 WHERE customers.cid = orders.cid
 GROUP BY uname
 ORDER BY cvalue DESC
