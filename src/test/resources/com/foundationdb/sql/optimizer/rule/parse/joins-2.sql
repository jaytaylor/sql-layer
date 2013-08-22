SELECT * FROM
 (SELECT name, MAX(price * quan) AS vmax
    FROM customers INNER JOIN orders ON customers.cid = orders.cid
                   INNER JOIN items ON orders.oid = items.oid
    GROUP BY name) m1
RIGHT JOIN
 (SELECT name, MIN(price * quan) AS vmin
    FROM customers INNER JOIN orders ON customers.cid = orders.cid
                   INNER JOIN items ON orders.oid = items.oid
    GROUP BY name) m2
ON vmax = vmin
