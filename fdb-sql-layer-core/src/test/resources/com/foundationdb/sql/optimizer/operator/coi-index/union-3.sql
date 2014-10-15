select cid, name from customers
UNION
select cat, sku from categories
UNION
select iid, sku from items