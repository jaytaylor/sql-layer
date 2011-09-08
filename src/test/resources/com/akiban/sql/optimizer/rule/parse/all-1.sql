SELECT order_date FROM orders
 WHERE 100 < ALL (SELECT price * quan FROM items 
                   WHERE items.oid = orders.oid)
