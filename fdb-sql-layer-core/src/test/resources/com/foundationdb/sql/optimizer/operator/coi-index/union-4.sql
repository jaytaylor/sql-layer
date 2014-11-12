select cid, name from customers
UNION
select cid, cast (order_date as VARCHAR(32)) from orders