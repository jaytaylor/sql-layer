SELECT * FROM orders, items
    WHERE   orders.oid = items.oid
    AND     orders.order_date = '2000-10-31'
    AND     items.sku = '1234'