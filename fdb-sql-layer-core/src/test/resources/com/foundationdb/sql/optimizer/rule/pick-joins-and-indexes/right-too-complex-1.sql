-- Note: the orders.order_date <> '2011-01-01' cannot be pushed down to the index.
-- For any row with order_date = '2011-01-01' this should return that row once, with null
-- for the customers part, regardless of the contents of the customers table. Pushing it
-- down to the scan would cause it to not be returned at all.
SELECT name, order_date
  FROM customers 
 RIGHT JOIN orders ON customers.cid = orders.cid AND orders.order_date <> '2011-01-01'