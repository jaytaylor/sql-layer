SELECT * FROM customers, addresses
    WHERE customers.cid = addresses.cid
          AND customers.name = 'Atzdz'
          AND addresses.state = 'CA'