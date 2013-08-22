UPDATE items SET quan = 66
 WHERE oid IN (SELECT orders.oid 
                 FROM orders INNER JOIN items i2 on orders.oid = i2.oid 
                WHERE order_date > '2011-03-02')