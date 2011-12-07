SELECT * 
       FROM (customers INNER JOIN orders ON customers.cid = orders.cid)
 RIGHT JOIN (parent INNER JOIN child ON parent.id = child.pid)
         ON customers.name = parent.name
