SELECT name FROM customers, orders o1, orders o2
 WHERE customers.cid = o1.cid AND customers.cid = o2.cid
   AND o1.order_date = '2011-03-01' AND o2.order_date = '2011-03-02'