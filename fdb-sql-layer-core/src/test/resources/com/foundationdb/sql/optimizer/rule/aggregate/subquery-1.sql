SELECT name FROM customers 
 WHERE cid IN (SELECT orders.cid FROM orders INNER JOIN items USING (oid) 
                WHERE sku = '1234'
                GROUP BY orders.cid, orders.order_date)