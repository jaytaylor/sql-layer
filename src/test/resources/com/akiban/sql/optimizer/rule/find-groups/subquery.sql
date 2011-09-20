SELECT name, order_date FROM customers, orders
 WHERE customers.cid = orders.oid
  AND EXISTS (SELECT * FROM items
               WHERE items.oid = orders.oid
                AND items.sku = '1234')
