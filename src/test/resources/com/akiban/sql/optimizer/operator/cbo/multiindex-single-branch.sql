SELECT * FROM customers, orders
    WHERE customers.cid = orders.cid
          AND customers.name = 'Atzdz'
          AND orders.order_date = '2019-09-28'