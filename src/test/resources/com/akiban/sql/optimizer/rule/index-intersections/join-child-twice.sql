SELECT * FROM customers, addresses a1, addresses a2
    WHERE   customers.cid = a1.cid
    AND     customers.cid = a2.cid
    AND     a1.state = 'MA'
    AND     a2.city = 'Boston'