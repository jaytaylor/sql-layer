SELECT name, order_date FROM customers RIGHT JOIN orders USING (cid)
WHERE (order_date != '2011-03-02' AND order_date != '2011-02-28')