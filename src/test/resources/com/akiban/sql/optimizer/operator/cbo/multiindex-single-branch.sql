SELECT * FROM customers, addresses
    WHERE customers.cid = addresses.cid
          AND customers.name = 'Awwyqrezr'
          AND addresses.state = 'CA'