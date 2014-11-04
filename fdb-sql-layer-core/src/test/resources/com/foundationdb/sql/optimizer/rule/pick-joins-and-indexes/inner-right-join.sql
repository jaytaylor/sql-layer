SELECT * FROM orders
  INNER JOIN items
    ON items.quan = 12
    AND items.oid = orders.oid
  RIGHT OUTER JOIN customers
    USING (cid)
  WHERE customers.name = 'smith'