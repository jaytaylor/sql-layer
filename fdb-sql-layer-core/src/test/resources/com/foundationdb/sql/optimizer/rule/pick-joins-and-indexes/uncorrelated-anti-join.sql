SELECT name FROM customers
 WHERE NOT EXISTS (SELECT * FROM orders INNER JOIN items USING (oid)
                    WHERE sku = '1234' AND order_date > '2010-10-10')
