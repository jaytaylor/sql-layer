SELECT * FROM customers JOIN orders USING (cid) JOIN addresses ON (addresses.cid = orders.cid)
