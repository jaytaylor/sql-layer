SELECT order_date, child.name
       FROM (customers INNER JOIN orders ON customers.cid = orders.cid)
  LEFT JOIN (parent INNER JOIN child ON parent.id = child.pid)
         ON customers.name = parent.name
      WHERE parent.name = 'Smith' OR parent.name = 'Jones'
