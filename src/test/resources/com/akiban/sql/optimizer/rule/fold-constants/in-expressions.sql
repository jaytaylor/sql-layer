SELECT name FROM customers
 WHERE cid IN (extract(minute from now()), extract(second from now()),?)