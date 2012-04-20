SELECT * FROM customers
  LEFT JOIN addresses ON customers.cid = addresses.cid
        AND addresses.city <> customers.name
