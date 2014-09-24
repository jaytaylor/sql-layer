select cid, name from customers
UNION
select cid, NULL as name from orders