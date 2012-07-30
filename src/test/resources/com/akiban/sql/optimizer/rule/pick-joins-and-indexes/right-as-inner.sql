SELECT customers.cid FROM customers INNER JOIN addresses ON customers.cid = addresses.cid
 WHERE state = 'MA' AND name = 'Adams'