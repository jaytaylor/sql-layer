SELECT name FROM customers
 WHERE name <> 'Fred'
   AND NOT EXISTS (SELECT * FROM orders
                    WHERE 1=0)