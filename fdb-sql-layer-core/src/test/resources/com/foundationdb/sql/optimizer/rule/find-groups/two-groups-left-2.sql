SELECT * 
      FROM (parent INNER JOIN child ON parent.id = child.pid)
 LEFT JOIN (customers INNER JOIN orders ON customers.cid = orders.cid)
         ON customers.name = parent.name
