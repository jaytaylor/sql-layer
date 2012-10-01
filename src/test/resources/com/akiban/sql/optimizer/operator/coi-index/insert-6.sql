insert  into orders (oid, cid, order_date) select cid+1000, cid, now() date_ordered 
from customers 
returning oid + 1000, date_add(order_date, INTERVAL 1 day)   