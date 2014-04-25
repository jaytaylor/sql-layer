INSERT INTO customers(cid, name)
SELECT c1, c2 FROM
  (SELECT oid AS c1, month(order_date) AS c2, cid AS c3 FROM orders
     UNION
   SELECT iid AS c1, quan AS c2, oid AS c3 FROM items) AS t