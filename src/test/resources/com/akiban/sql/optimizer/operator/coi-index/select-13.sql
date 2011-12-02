SELECT o1.order_date, o1.oid, o2.oid
  FROM orders o1 LEFT JOIN orders o2 
    ON o1.order_date = o2.order_date AND o1.oid <> o2.oid