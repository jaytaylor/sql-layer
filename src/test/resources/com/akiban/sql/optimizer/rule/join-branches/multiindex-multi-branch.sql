SELECT customers.cid FROM customers, orders, items, addresses
    WHERE customers.cid = addresses.cid
    AND   customers.cid = orders.cid
    AND   orders.oid = items.oid
    AND   addresses.state = 'CA'
    AND   items.sku = '0147'
