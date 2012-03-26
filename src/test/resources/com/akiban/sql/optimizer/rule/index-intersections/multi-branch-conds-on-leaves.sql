SELECT * FROM customers, orders, addresses
    WHERE   customers.cid = orders.cid
    AND     customers.cid = addresses.cid
    AND     orders.order_date = '2001-02-03'
    AND     addresses.state = 'MA'