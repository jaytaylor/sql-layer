SELECT
    (SELECT order_date FROM customers c_inner, orders WHERE orders.cid = parent.id AND c_inner.cid = c_outer.cid LIMIT 1)
FROM customers c_outer, parent WHERE c_outer.cid = parent.id