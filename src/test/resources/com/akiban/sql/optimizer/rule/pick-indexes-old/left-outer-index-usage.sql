SELECT customers.cid, parent.name
    FROM customers
    LEFT OUTER JOIN parent
        ON parent.id = customers.cid
    WHERE customers.name = 'foo'
        AND parent.name IS NULL
