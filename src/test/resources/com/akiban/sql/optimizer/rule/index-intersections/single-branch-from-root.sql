SELECT * FROM customers, orders, ITEMS
    WHERE   customers.cid = orders.cid
    AND     orders.oid = items.oid
    AND     customers.name = 'Alpha'
    AND     items.sku = '1234'