SELECT *
    FROM customers
    INNER JOIN parent ON (parent.id = customers.cid)
    INNER JOIN child ON (child.pid = parent.id)
    LEFT JOIN orders ON (orders.cid = customers.cid)