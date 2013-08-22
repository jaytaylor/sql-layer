SELECT customers.cid, order_date
  FROM customers INNER JOIN orders 
    ON oid = (SELECT MIN(oid) FROM orders o2 WHERE o2.cid = customers.cid
                 AND o2.order_date > '2010-01-01')
 WHERE name = 'Smith'
