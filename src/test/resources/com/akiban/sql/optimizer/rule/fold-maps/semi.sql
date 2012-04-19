SELECT order_date FROM orders
 WHERE EXISTS (SELECT * FROM items, child
                WHERE items.oid = orders.oid 
                  AND child.id = items.iid
                  AND sku in ('1234', '4567'))
           OR order_date = '0000-00-00'