select cid, null as name from customers
UNION
select cid, cast (order_date as VARCHAR(32)) as name from orders