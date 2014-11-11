SELECT * FROM customers, orders, items
    WHERE customers.cid = orders.cid
          AND orders.oid = items.oid
          AND customers.name = 'Atzdz'
          AND orders.order_date = '2019-09-28'
          AND items.sku = '0275'