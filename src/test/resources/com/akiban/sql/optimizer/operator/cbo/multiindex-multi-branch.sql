SELECT customers.cid
    FROM customers, orders, addresses
    WHERE customers.cid = orders.cid
    AND   customers.cid = addresses.cid
    AND   addresses.state = 'CA'
    AND   orders.order_date = '2010-04-22'